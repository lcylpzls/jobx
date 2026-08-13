package core

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lcylpzls/errx"
	"github.com/lcylpzls/jobx/cron"
	"github.com/lcylpzls/logx"
)

// Scheduler 定时调度器：周期、一次性与 cron 表达式调度。
// 触发后提交给 Dispatcher 执行；所有方法并发安全。
type Scheduler struct {
	dispatcher *Dispatcher
	loc        *time.Location
	logger     logx.Logger
	mu         sync.Mutex
	entries    map[string]*scheduleEntry
	stopped    bool
	wg         sync.WaitGroup
}

// scheduleEntry 调度条目内部状态。
type scheduleEntry struct {
	mu   sync.Mutex
	id   string
	name string
	next time.Time
	stop chan struct{}
	done chan struct{}
}

// Schedule 调度条目快照。
type Schedule struct {
	ID    string
	Name  string
	Next  time.Time
	sched *Scheduler
}

// Stop 停止该调度条目（幂等）。
func (s *Schedule) Stop() {
	if s.sched != nil {
		_ = s.sched.Stop(s.ID)
	}
}

// schedulerConfig 调度器配置。
type schedulerConfig struct {
	loc    *time.Location
	logger logx.Logger
}

// SchedulerOption 调度器配置项。
type SchedulerOption func(*schedulerConfig) error

// WithLocation 设置调度时区（默认本地时区）。
func WithLocation(loc *time.Location) SchedulerOption {
	return func(c *schedulerConfig) error {
		if loc == nil {
			return errInvalidConfig("时区不能为空")
		}
		c.loc = loc
		return nil
	}
}

// WithSchedulerLogger 注入调度日志器（nil 表示不记录）。
func WithSchedulerLogger(logger logx.Logger) SchedulerOption {
	return func(c *schedulerConfig) error {
		c.logger = logger
		return nil
	}
}

// NewScheduler 构造调度器并绑定执行器。
func NewScheduler(dispatcher *Dispatcher, opts ...SchedulerOption) (*Scheduler, error) {
	if dispatcher == nil {
		return nil, errInvalidConfig("执行器不能为空")
	}
	cfg := schedulerConfig{loc: time.Local}
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}
	cfg.logger = normalizeLogger(cfg.logger)
	return &Scheduler{
		dispatcher: dispatcher,
		loc:        cfg.loc,
		logger:     cfg.logger,
		entries:    make(map[string]*scheduleEntry),
	}, nil
}

// Every 注册周期调度，首次触发在第一个周期后。
func (s *Scheduler) Every(interval time.Duration, name string) (*Schedule, error) {
	if interval < time.Second {
		return nil, errJobInvalid("周期必须至少 1 秒")
	}
	if err := s.checkHandler(name); err != nil {
		return nil, err
	}
	entry, err := s.newEntry(name)
	if err != nil {
		return nil, err
	}
	if err := s.register(entry); err != nil {
		return nil, err
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(entry.done)
		defer s.remove(entry.id)
		timer := time.NewTimer(interval)
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				entry.setNext(time.Now().Add(interval))
				if !s.fire(name) {
					return
				}
				timer.Reset(interval)
			case <-entry.stop:
				return
			}
		}
	}()
	return s.snapshot(entry), nil
}

// Cron 注册 cron 表达式调度；表达式非法返回 ErrCronInvalid。
func (s *Scheduler) Cron(expr, name string) (*Schedule, error) {
	parsed, err := cron.Parse(expr)
	if err != nil {
		return nil, err
	}
	if err := s.checkHandler(name); err != nil {
		return nil, err
	}
	entry, err := s.newEntry(name)
	if err != nil {
		return nil, err
	}
	if err := s.register(entry); err != nil {
		return nil, err
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(entry.done)
		defer s.remove(entry.id)
		for {
			next, err := parsed.Next(time.Now().In(s.loc))
			if err != nil {
				s.logWarn("jobx：调度表达式无解，条目停止", name, err)
				return
			}
			entry.setNext(next)
			timer := time.NewTimer(time.Until(next))
			select {
			case <-timer.C:
				timer.Stop()
				if !s.fire(name) {
					return
				}
			case <-entry.stop:
				timer.Stop()
				return
			}
		}
	}()
	return s.snapshot(entry), nil
}

// OneShot 注册一次性调度，触发后条目自动失效。
func (s *Scheduler) OneShot(at time.Time, name string) (*Schedule, error) {
	if !at.After(time.Now()) {
		return nil, errJobInvalid("一次性调度时刻必须晚于当前时间")
	}
	if err := s.checkHandler(name); err != nil {
		return nil, err
	}
	entry, err := s.newEntry(name)
	if err != nil {
		return nil, err
	}
	if err := s.register(entry); err != nil {
		return nil, err
	}
	entry.setNext(at)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(entry.done)
		defer s.remove(entry.id)
		timer := time.NewTimer(time.Until(at))
		defer timer.Stop()
		select {
		case <-timer.C:
			_ = s.fire(name)
		case <-entry.stop:
		}
	}()
	return s.snapshot(entry), nil
}

// EveryMinuteAt 每分钟第 second 秒触发（等价 6 字段 cron 包装）。
func (s *Scheduler) EveryMinuteAt(second int, name string) (*Schedule, error) {
	return s.Cron(fmt.Sprintf("%d * * * * *", second), name)
}

// EveryHourAt 每小时第 minute 分 0 秒触发（等价 cron 包装）。
func (s *Scheduler) EveryHourAt(minute int, name string) (*Schedule, error) {
	return s.Cron(fmt.Sprintf("0 %d * * * *", minute), name)
}

// DailyAt 每天 hour:minute:second 触发（等价 cron 包装）。
func (s *Scheduler) DailyAt(hour, minute, second int, name string) (*Schedule, error) {
	return s.Cron(fmt.Sprintf("%d %d %d * * *", second, minute, hour), name)
}

// WeeklyAt 每周 weekday（0=周日）的 hour:minute:second 触发。
func (s *Scheduler) WeeklyAt(weekday, hour, minute, second int, name string) (*Schedule, error) {
	return s.Cron(fmt.Sprintf("%d %d %d * * %d", second, minute, hour, weekday), name)
}

// List 返回全部调度条目快照。
func (s *Scheduler) List() []Schedule {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Schedule, 0, len(s.entries))
	for _, e := range s.entries {
		out = append(out, Schedule{ID: e.id, Name: e.name, Next: e.nextTime(), sched: s})
	}
	return out
}

// Stop 停止并移除调度条目；不存在返回 ErrJobNotFound。
func (s *Scheduler) Stop(id string) error {
	s.mu.Lock()
	entry, ok := s.entries[id]
	if !ok {
		s.mu.Unlock()
		return ErrJobNotFound
	}
	delete(s.entries, id)
	s.mu.Unlock()
	close(entry.stop)
	<-entry.done
	return nil
}

// Shutdown 停止全部条目并等待退出；Dispatcher 由调用方关闭。
func (s *Scheduler) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.stopped = true
	entries := make([]*scheduleEntry, 0, len(s.entries))
	for _, e := range s.entries {
		entries = append(entries, e)
	}
	s.entries = make(map[string]*scheduleEntry)
	s.mu.Unlock()
	for _, e := range entries {
		close(e.stop)
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// checkHandler 校验任务名与处理器注册。
func (s *Scheduler) checkHandler(name string) error {
	if err := validateName(name); err != nil {
		return err
	}
	if _, ok := s.dispatcher.handlers.Load(name); !ok {
		return ErrHandlerNotFound
	}
	return nil
}

// register 注册条目（停止后拒绝）。
func (s *Scheduler) register(entry *scheduleEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return ErrSchedulerStopped
	}
	s.entries[entry.id] = entry
	return nil
}

// newEntry 构造调度条目。
func (s *Scheduler) newEntry(name string) (*scheduleEntry, error) {
	id, err := newJobID()
	if err != nil {
		return nil, errx.WrapCode(err, CodeIDGenerateFailed, "调度条目 ID 生成失败")
	}
	return &scheduleEntry{
		id:   id,
		name: name,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}, nil
}

// remove 从条目表移除（不等待 goroutine 退出）。
func (s *Scheduler) remove(id string) {
	s.mu.Lock()
	delete(s.entries, id)
	s.mu.Unlock()
}

// fire 触发提交；失败（如执行器关闭）返回 false 并记录日志。
func (s *Scheduler) fire(name string) bool {
	if _, err := s.dispatcher.Submit(context.Background(), name, nil); err != nil {
		s.logWarn("jobx：调度触发提交失败，条目停止", name, err)
		return false
	}
	return true
}

// snapshot 构造对外快照。
func (s *Scheduler) snapshot(entry *scheduleEntry) *Schedule {
	return &Schedule{ID: entry.id, Name: entry.name, Next: entry.nextTime(), sched: s}
}

// setNext 更新下次触发时间。
func (e *scheduleEntry) setNext(t time.Time) {
	e.mu.Lock()
	e.next = t
	e.mu.Unlock()
}

// nextTime 读取下次触发时间。
func (e *scheduleEntry) nextTime() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.next
}

// logWarn 记录调度告警。
func (s *Scheduler) logWarn(msg, name string, err error) {
	s.logger.Warn(msg, logx.Fields(
		logx.String(fieldJobName, name),
		logx.String("error", err.Error()),
	))
}
