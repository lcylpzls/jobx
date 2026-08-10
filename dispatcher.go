package jobx

import (
	"container/heap"
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lcylpzls/cryptox"
	"github.com/lcylpzls/errx"
	"github.com/lcylpzls/logx"
)

const (
	maxNameLength  = 128 // 任务名称长度上限
	idBytes        = 16  // 任务 ID 随机字节数（hex 32 字符）
	maxStatusCount = 10000
	fieldJobID     = "jobx_id"
	fieldJobName   = "jobx_name"
	fieldAttempt   = "jobx_attempt"
	fieldDuration  = "jobx_duration"
	fieldRetryAt   = "jobx_retry_at"
)

// randRead 可替换的随机源，便于测试注入失败场景；
// 由 randMu 保护，避免并发测试读写竞争。
var (
	randMu   sync.RWMutex
	randRead = cryptox.RandomBytes
)

// Status 任务状态。
type Status uint8

// 任务状态枚举。
const (
	StatusQueued Status = iota
	StatusDelayed
	StatusRunning
	StatusSucceeded
	StatusFailed
	StatusCancelled
)

// jobEntry 内部任务条目。
type jobEntry struct {
	job Job
}

// delayHeap 按 RunAt 排序的最小堆。
type delayHeap []*jobEntry

func (h delayHeap) Len() int           { return len(h) }
func (h delayHeap) Less(i, j int) bool { return h[i].job.RunAt.Before(h[j].job.RunAt) }
func (h delayHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *delayHeap) Push(x any)        { *h = append(*h, x.(*jobEntry)) }
func (h *delayHeap) Pop() any          { old := *h; n := len(old); e := old[n-1]; *h = old[:n-1]; return e }

// Dispatcher 任务执行器：就绪队列 + worker 池 + 延迟队列。
// 所有方法并发安全。
type Dispatcher struct {
	cfg          config
	handlers     sync.Map // name → Handler
	ready        *readyQueue
	stopCh       chan struct{}
	signal       chan struct{}
	shutdownOnce sync.Once
	shutdownErr  error
	wg           sync.WaitGroup
	running      atomic.Bool
	executing    sync.Map // jobID → context.CancelFunc
	cancelled    sync.Map // jobID → struct{}（排队中取消标记）
	statusMu     sync.Mutex
	statuses     map[string]Status
	statusOrder  []string
	inFlightMu   sync.Mutex
	inFlight     map[string]map[string]struct{}
	jobNames     sync.Map // jobID → name（供取消时释放在途集合）
	delayMu      sync.Mutex
	delayHeap    delayHeap
}

// NewDispatcher 构造执行器并启动 worker 池与延迟调度。
func NewDispatcher(opts ...Option) (*Dispatcher, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}
	d := &Dispatcher{
		cfg:      cfg,
		ready:    newReadyQueue(cfg.queueSize),
		stopCh:   make(chan struct{}),
		signal:   make(chan struct{}, 1),
		statuses: make(map[string]Status),
		inFlight: make(map[string]map[string]struct{}),
	}
	d.running.Store(true)
	for i := 0; i < cfg.workers; i++ {
		d.wg.Add(1)
		go d.worker()
	}
	d.wg.Add(1)
	go d.delayLoop()
	return d, nil
}

// Handle 注册任务处理器；空名/超长名返回 ErrJobInvalid，
// 重复注册返回 ErrHandlerConflict。
func (d *Dispatcher) Handle(name string, h Handler) error {
	if err := validateName(name); err != nil {
		return err
	}
	if h == nil {
		return ErrJobInvalid
	}
	if _, loaded := d.handlers.LoadOrStore(name, h); loaded {
		return ErrHandlerConflict
	}
	return nil
}

// Submit 提交任务（立即执行），返回任务 ID。
func (d *Dispatcher) Submit(ctx context.Context, name string, payload []byte, opts ...SubmitOption) (string, error) {
	return d.SubmitWithOptions(ctx, name, payload, opts...)
}

// SubmitAt 提交指定时刻执行的任务。
func (d *Dispatcher) SubmitAt(ctx context.Context, name string, payload []byte, at time.Time, opts ...SubmitOption) (string, error) {
	opts = append(opts, WithRunAt(at))
	return d.SubmitWithOptions(ctx, name, payload, opts...)
}

// SubmitAfter 提交延时执行的任务（delay 必须非负）。
func (d *Dispatcher) SubmitAfter(ctx context.Context, name string, payload []byte, delay time.Duration, opts ...SubmitOption) (string, error) {
	if delay < 0 {
		return "", errJobInvalid("延迟必须非负")
	}
	opts = append(opts, WithRunAt(d.cfg.now().Add(delay)))
	return d.SubmitWithOptions(ctx, name, payload, opts...)
}

// SubmitWithOptions 提交任务并应用全部选项。
func (d *Dispatcher) SubmitWithOptions(ctx context.Context, name string, payload []byte, opts ...SubmitOption) (string, error) {
	spec := jobSpec{}
	for _, opt := range opts {
		if err := opt(&spec); err != nil {
			return "", err
		}
	}
	entry, err := d.buildEntry(name, payload, spec)
	if err != nil {
		return "", err
	}
	if err := d.applyConflict(entry); err != nil {
		return "", err
	}
	d.jobNames.Store(entry.job.ID, entry.job.Name)
	if !d.running.Load() {
		d.releaseJob(entry.job.Name, entry.job.ID)
		return "", ErrShuttingDown
	}
	if spec.runAt.IsZero() || !spec.runAt.After(d.cfg.now()) {
		if err := d.persist(ctx, entry.job); err != nil {
			d.releaseJob(entry.job.Name, entry.job.ID)
			return "", err
		}
		if err := d.enqueueReady(ctx, entry); err != nil {
			if d.cfg.store != nil {
				_ = d.cfg.store.Delete(ctx, entry.job.ID)
			}
			d.releaseJob(entry.job.Name, entry.job.ID)
			return "", err
		}
		d.markStatus(entry.job.ID, StatusQueued)
		d.metricQueued(entry.job.Name, 1)
		d.logSubmit(entry.job)
		return entry.job.ID, nil
	}
	if err := d.persist(ctx, entry.job); err != nil {
		d.releaseJob(entry.job.Name, entry.job.ID)
		return "", err
	}
	d.pushDelayed(entry)
	d.markStatus(entry.job.ID, StatusDelayed)
	d.logSubmit(entry.job)
	return entry.job.ID, nil
}

// Restore 从持久化存储恢复未完成任务（进程重启后调用），返回恢复数量。
// 恢复绕过冲突检查但重建在途集合；缺失处理器的任务被删除并跳过。
func (d *Dispatcher) Restore(ctx context.Context) (int, error) {
	if d.cfg.store == nil {
		return 0, nil
	}
	jobs, err := d.cfg.store.List(ctx)
	if err != nil {
		return 0, errx.WrapCode(err, CodeStoreInvalid, "任务恢复读取失败")
	}
	restored := 0
	for _, job := range jobs {
		if _, ok := d.handlers.Load(job.Name); !ok {
			_ = d.cfg.store.Delete(ctx, job.ID)
			d.logStore("jobx：恢复时跳过缺失处理器的任务", job)
			continue
		}
		entry := &jobEntry{job: job}
		d.jobNames.Store(job.ID, job.Name)
		d.addInFlightForRestore(job.Name, job.ID)
		if job.RunAt.After(d.cfg.now()) {
			d.pushDelayed(entry)
			d.markStatus(job.ID, StatusDelayed)
		} else {
			if err := d.ready.push(ctx, entry, false); err != nil {
				return restored, err
			}
			d.markStatus(job.ID, StatusQueued)
			d.metricQueued(job.Name, 1)
		}
		restored++
	}
	return restored, nil
}

// JobStatus 查询任务状态；未知 ID 返回 ErrJobNotFound。
func (d *Dispatcher) JobStatus(id string) (Status, error) {
	d.statusMu.Lock()
	defer d.statusMu.Unlock()
	s, ok := d.statuses[id]
	if !ok {
		return 0, ErrJobNotFound
	}
	return s, nil
}

// Cancel 取消任务：延迟/排队中的直接取消，执行中的通过 context 协作。
func (d *Dispatcher) Cancel(id string) error {
	if _, err := d.JobStatus(id); err != nil {
		return err
	}
	d.cancelByID(id)
	return nil
}

// Shutdown 优雅关闭：拒绝新提交、丢弃延迟任务、等待存量执行完毕；
// ctx 超时则取消执行中的任务并返回 ctx 错误。幂等。
func (d *Dispatcher) Shutdown(ctx context.Context) error {
	d.shutdownOnce.Do(func() {
		d.running.Store(false)
		close(d.stopCh)
		d.ready.close()
		d.shutdownErr = d.waitWorkers(ctx)
		d.logShutdown()
	})
	return d.shutdownErr
}

// buildEntry 构造任务条目并做参数校验。
func (d *Dispatcher) buildEntry(name string, payload []byte, spec jobSpec) (*jobEntry, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}
	if len(payload) > d.cfg.maxPayload {
		return nil, errJobInvalid("任务载荷超出上限")
	}
	if _, ok := d.handlers.Load(name); !ok {
		return nil, ErrHandlerNotFound
	}
	id, err := newJobID()
	if err != nil {
		return nil, errx.WrapCode(err, CodeIDGenerateFailed, "任务 ID 生成失败")
	}
	return &jobEntry{job: Job{
		ID:         id,
		Name:       name,
		Payload:    append([]byte(nil), payload...),
		CreatedAt:  d.cfg.now(),
		RunAt:      spec.runAt,
		MaxRetries: spec.maxRetries,
		RetryDelay: spec.retryDelay,
		Timeout:    spec.timeout,
	}}, nil
}

// applyConflict 按冲突策略处理同名在途任务。
func (d *Dispatcher) applyConflict(entry *jobEntry) error {
	if d.cfg.conflict == ConflictAllow {
		return nil
	}
	var replaced []string
	d.inFlightMu.Lock()
	set := d.inFlight[entry.job.Name]
	switch d.cfg.conflict {
	case ConflictReplace:
		if len(set) > 0 {
			for id := range set {
				replaced = append(replaced, id)
			}
			delete(d.inFlight, entry.job.Name)
		}
		d.addInFlightLocked(entry.job.Name, entry.job.ID)
	case ConflictSkip:
		if len(set) > 0 {
			d.metricSkipped(entry.job.Name)
			d.logSkipped(entry.job)
			d.inFlightMu.Unlock()
			return ErrSkipped
		}
		d.addInFlightLocked(entry.job.Name, entry.job.ID)
	}
	d.inFlightMu.Unlock()
	for _, id := range replaced {
		d.cancelByIDLocked(id)
	}
	if len(replaced) > 0 {
		d.metricReplaced(entry.job.Name)
		d.logReplaced(entry.job)
	}
	return nil
}

// addInFlightLocked 加入在途集合（调用方持锁）。
func (d *Dispatcher) addInFlightLocked(name, id string) {
	set := d.inFlight[name]
	if set == nil {
		set = make(map[string]struct{})
		d.inFlight[name] = set
	}
	set[id] = struct{}{}
}

// releaseInFlight 终态移除在途集合。
func (d *Dispatcher) releaseInFlight(name, id string) {
	if d.cfg.conflict == ConflictAllow {
		return
	}
	d.inFlightMu.Lock()
	defer d.inFlightMu.Unlock()
	set := d.inFlight[name]
	if set == nil {
		return
	}
	delete(set, id)
	if len(set) == 0 {
		delete(d.inFlight, name)
	}
}

// releaseJob 终态清理：释放在途集合并删除任务名映射。
func (d *Dispatcher) releaseJob(name, id string) {
	d.releaseInFlight(name, id)
	d.jobNames.Delete(id)
}

// persist 同步持久化任务（未启用存储时直接成功）。
func (d *Dispatcher) persist(ctx context.Context, job Job) error {
	if d.cfg.store == nil {
		return nil
	}
	if err := d.cfg.store.Save(ctx, job); err != nil {
		return errx.WrapCode(err, CodeStoreInvalid, "任务持久化失败")
	}
	return nil
}

// addInFlightForRestore 恢复时重建在途集合（Allow 策略不维护）。
func (d *Dispatcher) addInFlightForRestore(name, id string) {
	if d.cfg.conflict == ConflictAllow {
		return
	}
	d.inFlightMu.Lock()
	d.addInFlightLocked(name, id)
	d.inFlightMu.Unlock()
}

// enqueueReady 将条目送入就绪队列。
func (d *Dispatcher) enqueueReady(ctx context.Context, entry *jobEntry) error {
	if err := d.ready.push(ctx, entry, d.cfg.queuePolicy == QueueFullDrop); err != nil {
		if d.cfg.queuePolicy == QueueFullDrop {
			d.metricDropped(entry.job.Name)
		}
		return err
	}
	return nil
}

// pushDelayed 将条目送入延迟堆并唤醒调度 goroutine。
func (d *Dispatcher) pushDelayed(entry *jobEntry) {
	d.delayMu.Lock()
	heap.Push(&d.delayHeap, entry)
	d.delayMu.Unlock()
	select {
	case d.signal <- struct{}{}:
	default:
	}
}

// delayLoop 延迟队列调度：到期移入就绪队列，关闭时丢弃。
func (d *Dispatcher) delayLoop() {
	defer d.wg.Done()
	var timer *time.Timer
	var timerC <-chan time.Time
	for {
		d.delayMu.Lock()
		if d.delayHeap.Len() == 0 {
			d.delayMu.Unlock()
			select {
			case <-d.signal:
				continue
			case <-d.stopCh:
				d.drainDelayed()
				return
			}
		}
		next := d.delayHeap[0].job.RunAt
		d.delayMu.Unlock()
		wait := time.Until(next)
		if wait < 0 {
			wait = 0
		}
		if timer == nil {
			timer = time.NewTimer(wait)
		} else {
			timer.Reset(wait)
		}
		timerC = timer.C
		select {
		case <-timerC:
			d.popDue()
		case <-d.signal:
			timer.Stop() // Go 1.23+ 同步通道：Stop 后无残留值。
		case <-d.stopCh:
			timer.Stop()
			d.drainDelayed()
			return
		}
	}
}

// popDue 将所有到期条目移入就绪队列。
func (d *Dispatcher) popDue() {
	now := time.Now()
	for {
		d.delayMu.Lock()
		if d.delayHeap.Len() == 0 {
			d.delayMu.Unlock()
			return
		}
		if d.delayHeap[0].job.RunAt.After(now) {
			d.delayMu.Unlock()
			return
		}
		entry := heap.Pop(&d.delayHeap).(*jobEntry)
		d.delayMu.Unlock()
		if d.isCancelled(entry.job.ID) {
			d.markStatus(entry.job.ID, StatusCancelled)
			d.releaseJob(entry.job.Name, entry.job.ID)
			continue
		}
		_ = d.ready.push(context.Background(), entry, false) // 到期任务不丢，按 Block 语义等待空间。
		d.markStatus(entry.job.ID, StatusQueued)
		d.metricQueued(entry.job.Name, 1)
	}
}

// drainDelayed 关闭时丢弃全部延迟任务。
func (d *Dispatcher) drainDelayed() {
	for {
		d.delayMu.Lock()
		if d.delayHeap.Len() == 0 {
			d.delayMu.Unlock()
			return
		}
		entry := heap.Pop(&d.delayHeap).(*jobEntry)
		d.delayMu.Unlock()
		if d.cfg.store != nil {
			_ = d.cfg.store.Delete(context.Background(), entry.job.ID)
		}
		d.metricDropped(entry.job.Name)
		d.markStatus(entry.job.ID, StatusCancelled)
		d.releaseJob(entry.job.Name, entry.job.ID)
	}
}

// worker 消费就绪队列。
func (d *Dispatcher) worker() {
	defer d.wg.Done()
	for {
		entry, ok := d.ready.pop()
		if !ok {
			return
		}
		d.runEntry(entry)
	}
}

// runEntry 执行单个任务：取消标记检查、超时 context、重试、日志与指标。
func (d *Dispatcher) runEntry(entry *jobEntry) {
	job := entry.job
	d.metricQueued(job.Name, -1)
	if d.isCancelled(job.ID) {
		d.markStatus(job.ID, StatusCancelled)
		d.releaseJob(job.Name, job.ID)
		return
	}
	d.markStatus(job.ID, StatusRunning)
	d.metricRunning(job.Name, 1)
	ctx, cancel := context.WithCancel(context.Background())
	if job.Timeout > 0 {
		var timeoutCtx context.Context
		timeoutCtx, cancel = context.WithTimeout(ctx, job.Timeout)
		ctx = timeoutCtx
	}
	d.executing.Store(job.ID, cancel)
	defer func() {
		d.executing.Delete(job.ID)
		cancel()
		d.metricRunning(job.Name, -1)
		d.releaseJob(job.Name, job.ID)
		if d.cfg.store != nil {
			_ = d.cfg.store.Delete(context.Background(), job.ID)
		}
	}()
	h, ok := d.handlers.Load(job.Name)
	if !ok {
		d.markStatus(job.ID, StatusFailed)
		d.metricFailed(job.Name, errx.WrapCode(ErrHandlerNotFound, CodeHandlerNotFound, "处理器缺失"))
		d.logError(job, ErrHandlerNotFound)
		return
	}
	start := d.cfg.now()
	d.logStart(job)
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = errx.WrapCode(fmt.Errorf("处理器 panic：%v", r),
					CodeExecutionFailed, "处理器执行失败")
				if d.cfg.logger != nil {
					d.cfg.logger.Error("jobx：处理器 panic", logx.Fields(
						logx.String(fieldJobID, job.ID),
						logx.String(fieldJobName, job.Name),
						logx.Any("panic", r),
					))
				}
			}
		}()
		spanCtx, end := d.traceStart(ctx, job)
		err = h.(Handler)(spanCtx, job)
		end(err)
	}()
	if d.isCancelled(job.ID) {
		d.markStatus(job.ID, StatusCancelled)
		d.logError(job, errx.NewCode(CodeJobCancelled, "任务已取消"))
		return
	}
	if err != nil {
		if job.MaxRetries > 0 && job.Attempt < job.MaxRetries {
			d.scheduleRetry(entry, err)
			return
		}
		d.markStatus(job.ID, StatusFailed)
		d.metricFailed(job.Name, err)
		d.logError(job, err)
		return
	}
	d.markStatus(job.ID, StatusSucceeded)
	d.metricCompleted(job.Name, d.cfg.now().Sub(start))
	d.logCompleted(job, d.cfg.now().Sub(start))
}

// traceStart 开始任务执行链路（无钩子时 no-op）。
func (d *Dispatcher) traceStart(ctx context.Context, job Job) (context.Context, func(error)) {
	if d.cfg.traceHook == nil {
		return ctx, func(error) {}
	}
	return d.cfg.traceHook.Start(ctx, "jobx.execute",
		TraceAttr{Key: "jobx.job_name", Value: job.Name},
		TraceAttr{Key: "jobx.job_id", Value: job.ID},
		TraceAttr{Key: "jobx.attempt", Value: strconv.Itoa(job.Attempt)},
	)
}

// scheduleRetry 安排重试：按指数退避进入延迟堆。
func (d *Dispatcher) scheduleRetry(entry *jobEntry, cause error) {
	job := entry.job
	delay := job.RetryDelay << job.Attempt
	next := *entry
	next.job.Attempt++
	next.job.RunAt = d.cfg.now().Add(delay)
	if d.cfg.store != nil {
		_ = d.cfg.store.Save(context.Background(), next.job)
	}
	d.pushDelayed(&next)
	d.markStatus(next.job.ID, StatusDelayed)
	d.metricRetried(job.Name, next.job.Attempt)
	d.logRetry(job, next.job.RunAt, cause)
}

// logStore 记录存储相关日志。
func (d *Dispatcher) logStore(msg string, job Job) {
	if d.cfg.logger == nil {
		return
	}
	d.cfg.logger.Warn(msg, logx.Fields(
		logx.String(fieldJobID, job.ID),
		logx.String(fieldJobName, job.Name),
	))
}

// waitWorkers 等待 worker 与延迟调度退出，ctx 超时则取消执行中任务。
func (d *Dispatcher) waitWorkers(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		d.executing.Range(func(_, v any) bool {
			if cancel, ok := v.(context.CancelFunc); ok {
				cancel()
			}
			return true
		})
		return ctx.Err()
	}
}

// cancelByID 取消任务（假定调用方已确认存在）。
func (d *Dispatcher) cancelByID(id string) {
	d.cancelByIDLocked(id)
}

// cancelByIDLocked 取消任务的具体实现；可持 inFlightMu 调用。
func (d *Dispatcher) cancelByIDLocked(id string) {
	d.delayMu.Lock()
	for i, e := range d.delayHeap {
		if e.job.ID == id {
			heap.Remove(&d.delayHeap, i)
			break
		}
	}
	d.delayMu.Unlock()
	d.ready.remove(id)
	d.cancelled.Store(id, struct{}{})
	if n, ok := d.jobNames.Load(id); ok {
		d.releaseJob(n.(string), id)
	}
	if d.cfg.store != nil {
		_ = d.cfg.store.Delete(context.Background(), id)
	}
	if v, ok := d.executing.Load(id); ok {
		if cancel, ok := v.(context.CancelFunc); ok {
			cancel()
		}
	}
	d.markStatus(id, StatusCancelled)
}

// isCancelled 判断任务是否已被取消。
func (d *Dispatcher) isCancelled(id string) bool {
	_, ok := d.cancelled.Load(id)
	return ok
}

// markStatus 记录任务状态（终态表带容量上限）。
func (d *Dispatcher) markStatus(id string, s Status) {
	d.statusMu.Lock()
	defer d.statusMu.Unlock()
	if _, ok := d.statuses[id]; !ok {
		d.statusOrder = append(d.statusOrder, id)
		if len(d.statusOrder) > maxStatusCount {
			oldest := d.statusOrder[0]
			d.statusOrder = d.statusOrder[1:]
			delete(d.statuses, oldest)
		}
	}
	d.statuses[id] = s
}

// validateName 校验任务名称。
func validateName(name string) error {
	if name == "" || len(name) > maxNameLength {
		return ErrJobInvalid
	}
	return nil
}

// newJobID 生成 32 位十六进制随机任务 ID。
func newJobID() (string, error) {
	randMu.RLock()
	read := randRead
	randMu.RUnlock()
	b, err := read(idBytes)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// errInvalidConfig 构造配置错误。
func errInvalidConfig(msg string) error {
	return errx.NewCode(CodeInvalidConfig, msg)
}

// errJobInvalid 构造任务参数错误。
func errJobInvalid(msg string) error {
	return errx.NewCode(CodeJobInvalid, msg)
}

// metricQueued 记录队列入队/出队。
func (d *Dispatcher) metricQueued(name string, delta int) {
	if d.cfg.metrics.Queued != nil {
		d.cfg.metrics.Queued(name, delta)
	}
	d.emitTaskEvent("queued", name, 0, nil)
}

// metricRunning 记录执行中增减。
func (d *Dispatcher) metricRunning(name string, delta int) {
	if d.cfg.metrics.Running != nil {
		d.cfg.metrics.Running(name, delta)
	}
	d.emitTaskEvent("running", name, 0, nil)
}

// metricCompleted 记录成功完成。
func (d *Dispatcher) metricCompleted(name string, dur time.Duration) {
	if d.cfg.metrics.Completed != nil {
		d.cfg.metrics.Completed(name, dur)
	}
	d.emitTaskEvent("completed", name, 0, nil)
}

// metricFailed 记录最终失败。
func (d *Dispatcher) metricFailed(name string, err error) {
	if d.cfg.metrics.Failed != nil {
		d.cfg.metrics.Failed(name, err)
	}
	d.emitTaskEvent("failed", name, 0, err)
}

// metricRetried 记录重试安排。
func (d *Dispatcher) metricRetried(name string, attempt int) {
	if d.cfg.metrics.Retried != nil {
		d.cfg.metrics.Retried(name, attempt)
	}
	d.emitTaskEvent("retried", name, attempt, nil)
}

// metricDropped 记录丢弃。
func (d *Dispatcher) metricDropped(name string) {
	if d.cfg.metrics.Dropped != nil {
		d.cfg.metrics.Dropped(name)
	}
	d.emitTaskEvent("dropped", name, 0, nil)
}

// metricSkipped 记录跳过。
func (d *Dispatcher) metricSkipped(name string) {
	if d.cfg.metrics.Skipped != nil {
		d.cfg.metrics.Skipped(name)
	}
	d.emitTaskEvent("skipped", name, 0, nil)
}

// metricReplaced 记录替换。
func (d *Dispatcher) metricReplaced(name string) {
	if d.cfg.metrics.Replaced != nil {
		d.cfg.metrics.Replaced(name)
	}
	d.emitTaskEvent("replaced", name, 0, nil)
}

// emitTaskEvent 发送任务事件（无钩子时 no-op）。
func (d *Dispatcher) emitTaskEvent(action, name string, attempt int, err error) {
	if d.cfg.eventHook == nil {
		return
	}
	d.cfg.eventHook.OnTaskEvent(context.Background(), TaskEvent{
		Action:  action,
		Name:    name,
		Attempt: attempt,
		Err:     err,
	})
}

// logSubmit 记录任务提交日志。
func (d *Dispatcher) logSubmit(job Job) {
	if d.cfg.logger == nil {
		return
	}
	d.cfg.logger.Info("jobx：任务已提交", logx.Fields(
		logx.String(fieldJobID, job.ID),
		logx.String(fieldJobName, job.Name),
		logx.String("jobx_run_at", job.RunAt.Format(time.RFC3339Nano)),
	))
}

// logStart 记录任务开始日志。
func (d *Dispatcher) logStart(job Job) {
	if d.cfg.logger == nil {
		return
	}
	d.cfg.logger.Info("jobx：任务开始执行", logx.Fields(
		logx.String(fieldJobID, job.ID),
		logx.String(fieldJobName, job.Name),
		logx.Int(fieldAttempt, job.Attempt),
	))
}

// logCompleted 记录任务完成日志。
func (d *Dispatcher) logCompleted(job Job, duration time.Duration) {
	if d.cfg.logger == nil {
		return
	}
	d.cfg.logger.Info("jobx：任务执行完成", logx.Fields(
		logx.String(fieldJobID, job.ID),
		logx.String(fieldJobName, job.Name),
		logx.String(fieldDuration, duration.String()),
	))
}

// logRetry 记录重试调度日志。
func (d *Dispatcher) logRetry(job Job, retryAt time.Time, cause error) {
	if d.cfg.logger == nil {
		return
	}
	d.cfg.logger.Warn("jobx：任务失败，安排重试", logx.Fields(
		logx.String(fieldJobID, job.ID),
		logx.String(fieldJobName, job.Name),
		logx.Int(fieldAttempt, job.Attempt+1),
		logx.String(fieldRetryAt, retryAt.Format(time.RFC3339Nano)),
		logx.String("error", cause.Error()),
	))
}

// logSkipped 记录任务跳过日志。
func (d *Dispatcher) logSkipped(job Job) {
	if d.cfg.logger == nil {
		return
	}
	d.cfg.logger.Warn("jobx：同名任务在途，本次提交被跳过", logx.Fields(
		logx.String(fieldJobID, job.ID),
		logx.String(fieldJobName, job.Name),
	))
}

// logReplaced 记录任务替换日志。
func (d *Dispatcher) logReplaced(job Job) {
	if d.cfg.logger == nil {
		return
	}
	d.cfg.logger.Warn("jobx：同名旧任务已被替换", logx.Fields(
		logx.String(fieldJobID, job.ID),
		logx.String(fieldJobName, job.Name),
	))
}

// logShutdown 记录关闭完成日志（含仍执行中的任务数）。
func (d *Dispatcher) logShutdown() {
	if d.cfg.logger == nil {
		return
	}
	remaining := 0
	d.executing.Range(func(_, _ any) bool {
		remaining++
		return true
	})
	d.cfg.logger.Info("jobx：执行器已关闭", logx.Fields(
		logx.Int("jobx_remaining", remaining),
	))
}

// logError 记录任务失败日志。
func (d *Dispatcher) logError(job Job, err error) {
	if d.cfg.logger == nil {
		return
	}
	d.cfg.logger.Error("jobx：任务执行失败", logx.Fields(
		logx.String(fieldJobID, job.ID),
		logx.String(fieldJobName, job.Name),
		logx.Int(fieldAttempt, job.Attempt),
		logx.String("error", err.Error()),
	))
}
