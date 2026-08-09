package jobx

import (
	"context"
	"time"
)

// Job 任务单元。
type Job struct {
	// ID 全局唯一标识（32 位十六进制随机串）。
	ID string
	// Name 处理器路由名（非空且长度 ≤ 128）。
	Name string
	// Payload 载荷（业务自行编码，如 JSON）。
	Payload []byte
	// CreatedAt 创建时间。
	CreatedAt time.Time
	// RunAt 计划执行时间（零值表示立即执行）。
	RunAt time.Time
	// MaxRetries 失败重试次数上限（0 表示不重试）。
	MaxRetries int
	// RetryDelay 首次重试延迟（后续指数 ×2）。
	RetryDelay time.Duration
	// Timeout 单次执行超时（0 表示不限制）。
	Timeout time.Duration
	// Attempt 已尝试次数（框架维护，业务只读）。
	Attempt int
}

// Handler 任务处理器：返回 nil 视为成功；返回错误触发重试策略；
// panic 由框架 recover 并按失败处理。
type Handler func(ctx context.Context, job Job) error

// jobSpec 提交时的任务规格。
type jobSpec struct {
	timeout time.Duration
}

// SubmitOption 配置单次提交的任务。
type SubmitOption func(*jobSpec) error

// WithTimeout 设置单次执行超时（必须非负）。
func WithTimeout(timeout time.Duration) SubmitOption {
	return func(s *jobSpec) error {
		if timeout < 0 {
			return errJobInvalid("任务超时必须非负")
		}
		s.timeout = timeout
		return nil
	}
}
