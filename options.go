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

// config Dispatcher 配置。
type config struct {
	workers     int
	queueSize   int
	queuePolicy QueueFullPolicy
	maxPayload  int
	logger      logx.Logger
	now         func() time.Time
}

// defaultConfig 返回默认配置。
func defaultConfig() config {
	return config{
		workers:     4,
		queueSize:   1024,
		queuePolicy: QueueFullBlock,
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
