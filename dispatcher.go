package jobx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lcylpzls/errx"
	"github.com/lcylpzls/logx"
)

const (
	maxNameLength = 128 // 任务名称长度上限
	idBytes       = 16  // 任务 ID 随机字节数（hex 32 字符）
	fieldJobID    = "jobx_id"
	fieldJobName  = "jobx_name"
	fieldAttempt  = "jobx_attempt"
	fieldDuration = "jobx_duration"
)

// randRead 可替换的随机源，便于测试注入失败场景。
var randRead = rand.Read

// jobEntry 内部任务条目。
type jobEntry struct {
	job Job
}

// Dispatcher 任务执行器：就绪队列 + worker 池。
// 所有方法并发安全。
type Dispatcher struct {
	cfg          config
	handlers     sync.Map // name → Handler
	ready        chan *jobEntry
	stopCh       chan struct{}
	shutdownOnce sync.Once
	shutdownErr  error
	wg           sync.WaitGroup
	running      atomic.Bool
	executing    sync.Map // jobID → context.CancelFunc
}

// NewDispatcher 构造执行器并启动 worker 池。
func NewDispatcher(opts ...Option) (*Dispatcher, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}
	d := &Dispatcher{
		cfg:    cfg,
		ready:  make(chan *jobEntry, cfg.queueSize),
		stopCh: make(chan struct{}),
	}
	d.running.Store(true)
	for i := 0; i < cfg.workers; i++ {
		d.wg.Add(1)
		go d.worker()
	}
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

// Submit 提交立即执行的任务，返回任务 ID。
func (d *Dispatcher) Submit(ctx context.Context, name string, payload []byte, opts ...SubmitOption) (string, error) {
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
	if err := d.enqueue(ctx, entry); err != nil {
		return "", err
	}
	d.logSubmit(entry.job)
	return entry.job.ID, nil
}

// Shutdown 优雅关闭：拒绝新提交、等待存量执行完毕；ctx 超时则
// 取消执行中的任务并返回 ctx 错误。幂等。
func (d *Dispatcher) Shutdown(ctx context.Context) error {
	d.shutdownOnce.Do(func() {
		d.running.Store(false)
		close(d.stopCh)
		d.shutdownErr = d.waitWorkers(ctx)
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
		return nil, errx.Wrap(err, errx.KindUnavailable, CodeJobInvalid, "任务 ID 生成失败")
	}
	return &jobEntry{job: Job{
		ID:        id,
		Name:      name,
		Payload:   append([]byte(nil), payload...),
		CreatedAt: d.cfg.now(),
		RunAt:     d.cfg.now(),
		Timeout:   spec.timeout,
	}}, nil
}

// enqueue 按策略入队。
func (d *Dispatcher) enqueue(ctx context.Context, entry *jobEntry) error {
	if !d.running.Load() {
		return ErrShuttingDown
	}
	switch d.cfg.queuePolicy {
	case QueueFullDrop:
		select {
		case d.ready <- entry:
			return nil
		default:
			return ErrQueueFull
		}
	default:
		select {
		case d.ready <- entry:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// worker 消费就绪队列。
func (d *Dispatcher) worker() {
	defer d.wg.Done()
	for {
		select {
		case entry := <-d.ready:
			d.runEntry(entry)
		case <-d.stopCh:
			// 关闭后排空剩余存量。
			for {
				select {
				case entry := <-d.ready:
					d.runEntry(entry)
				default:
					return
				}
			}
		}
	}
}

// runEntry 执行单个任务：超时 context、panic 恢复、日志。
func (d *Dispatcher) runEntry(entry *jobEntry) {
	job := entry.job
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
	}()
	h, ok := d.handlers.Load(job.Name)
	if !ok {
		d.logError(job, errx.Wrap(ErrHandlerNotFound, errx.KindNotFound, CodeHandlerNotFound, "处理器缺失"))
		return
	}
	start := d.cfg.now()
	d.logStart(job)
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = errx.Wrap(fmt.Errorf("处理器 panic：%v", r), errx.KindInternal,
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
		err = h.(Handler)(ctx, job)
	}()
	if err != nil {
		d.logError(job, err)
		return
	}
	d.logCompleted(job, d.cfg.now().Sub(start))
}

// waitWorkers 等待 worker 退出，ctx 超时则取消执行中任务。
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

// validateName 校验任务名称。
func validateName(name string) error {
	if name == "" || len(name) > maxNameLength {
		return ErrJobInvalid
	}
	return nil
}

// newJobID 生成 32 位十六进制随机任务 ID。
func newJobID() (string, error) {
	b := make([]byte, idBytes)
	if _, err := randRead(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// errInvalidConfig 构造配置错误。
func errInvalidConfig(msg string) error {
	return errx.New(errx.KindInvalid, CodeInvalidConfig, msg)
}

// errJobInvalid 构造任务参数错误。
func errJobInvalid(msg string) error {
	return errx.New(errx.KindInvalid, CodeJobInvalid, msg)
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
