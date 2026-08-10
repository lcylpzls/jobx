package jobx

import (
	"time"

	"github.com/lcylpzls/logx"
)

// QueueFullPolicy 就绪队列满时的提交策略。
type QueueFullPolicy uint8

const (
	// QueueFullBlock 阻塞提交直到队列有空间（默认，保证不丢任务）。
	QueueFullBlock QueueFullPolicy = iota
	// QueueFullDrop 丢弃新任务并返回 ErrQueueFull（不阻塞业务）。
	QueueFullDrop
)

// ConflictPolicy 同名任务在途时的处理策略。
type ConflictPolicy uint8

const (
	// ConflictSkip 跳过新任务，返回 ErrSkipped（默认）。
	ConflictSkip ConflictPolicy = iota
	// ConflictReplace 取消同名旧任务并执行新任务（尽力取消）。
	ConflictReplace
	// ConflictAllow 允许同名任务并发执行。
	ConflictAllow
)

// Metrics 外部注入的任务指标回调（全部可选，nil 跳过）。
type Metrics struct {
	// Queued 就绪队列入队 +1 / 出队 -1。
	Queued func(name string, delta int)
	// Running 执行中 +1 / -1。
	Running func(name string, delta int)
	// Completed 任务成功完成。
	Completed func(name string, duration time.Duration)
	// Failed 任务最终失败。
	Failed func(name string, err error)
	// Retried 安排重试（attempt 为即将执行的第 N 次）。
	Retried func(name string, attempt int)
	// Dropped 关闭/队列满等策略丢弃。
	Dropped func(name string)
	// Skipped ConflictSkip 跳过。
	Skipped func(name string)
	// Replaced ConflictReplace 替换旧任务。
	Replaced func(name string)
}

// config Dispatcher 配置。
type config struct {
	workers     int
	queueSize   int
	queuePolicy QueueFullPolicy
	conflict    ConflictPolicy
	maxPayload  int
	logger      logx.Logger
	now         func() time.Time
	metrics     Metrics
	store       Store
	traceHook   TraceHook
	eventHook   EventHook
}

// defaultConfig 返回默认配置。
func defaultConfig() config {
	return config{
		workers:     4,
		queueSize:   1024,
		queuePolicy: QueueFullBlock,
		conflict:    ConflictSkip,
		maxPayload:  1 << 20,
		now:         time.Now,
	}
}

// Option Dispatcher 配置项。
type Option func(*config) error

// WithWorkers 设置 worker 数量（必须为正）。
func WithWorkers(n int) Option {
	return func(c *config) error {
		if n <= 0 {
			return errInvalidConfig("worker 数量必须为正")
		}
		c.workers = n
		return nil
	}
}

// WithQueueSize 设置就绪队列容量（必须为正）。
func WithQueueSize(n int) Option {
	return func(c *config) error {
		if n <= 0 {
			return errInvalidConfig("队列容量必须为正")
		}
		c.queueSize = n
		return nil
	}
}

// WithQueueFullPolicy 设置队列满时的提交策略。
func WithQueueFullPolicy(p QueueFullPolicy) Option {
	return func(c *config) error {
		if p != QueueFullBlock && p != QueueFullDrop {
			return errInvalidConfig("非法队列满策略")
		}
		c.queuePolicy = p
		return nil
	}
}

// WithConflictPolicy 设置同名任务在途时的处理策略。
func WithConflictPolicy(p ConflictPolicy) Option {
	return func(c *config) error {
		if p != ConflictSkip && p != ConflictReplace && p != ConflictAllow {
			return errInvalidConfig("非法冲突策略")
		}
		c.conflict = p
		return nil
	}
}

// WithMaxPayloadBytes 设置任务载荷长度上限（必须为正）。
func WithMaxPayloadBytes(n int) Option {
	return func(c *config) error {
		if n <= 0 {
			return errInvalidConfig("载荷上限必须为正")
		}
		c.maxPayload = n
		return nil
	}
}

// WithLogger 注入结构化日志器（nil 表示不记录）。
func WithLogger(logger logx.Logger) Option {
	return func(c *config) error {
		c.logger = logger
		return nil
	}
}

// WithMetrics 注入任务指标回调（全部可选）。
func WithMetrics(m Metrics) Option {
	return func(c *config) error {
		c.metrics = m
		return nil
	}
}

// WithStore 启用任务持久化（提交/延迟同步写入，终态删除）。
func WithStore(store Store) Option {
	return func(c *config) error {
		if store == nil {
			return errInvalidConfig("任务存储不能为空")
		}
		c.store = store
		return nil
	}
}

// WithClock 注入时间源（测试用）。
func WithClock(now func() time.Time) Option {
	return func(c *config) error {
		if now == nil {
			return errInvalidConfig("时间源不能为空")
		}
		c.now = now
		return nil
	}
}

// WithTraceHook 设置任务执行链路追踪钩子。
func WithTraceHook(h TraceHook) Option {
	return func(c *config) error {
		c.traceHook = h
		return nil
	}
}

// WithEventHook 设置任务事件钩子；不设置时 no-op。
func WithEventHook(h EventHook) Option {
	return func(c *config) error {
		c.eventHook = h
		return nil
	}
}
