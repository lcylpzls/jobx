package core

import "context"

// TaskEvent 描述一次任务生命周期事件。
type TaskEvent struct {
	// Action 事件类型：queued / running / completed / failed / retried /
	// dropped / skipped / replaced。
	Action string
	// Name 任务名。
	Name string
	// Attempt 重试时的即将执行次数（仅 retried）。
	Attempt int
	// Err 失败原因（仅 failed）。
	Err error
}

// EventHook 是可选事件钩子（默认 no-op），由 eventx 等外部适配器接入。
type EventHook interface {
	// OnTaskEvent 在任务生命周期节点触发。
	OnTaskEvent(ctx context.Context, e TaskEvent)
}
