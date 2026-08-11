package core

import (
	"context"
)

// Store 任务持久化接口（可选）：将排队/延迟任务落库，进程重启后可恢复。
// 默认不启用（进程内任务随进程退出丢失）；启用后提交与延迟任务同步写入，
// 终态/取消同步删除。适配 Redis/数据库等外部实现由业务自行提供。
type Store interface {
	// Save 保存任务（新建或状态更新，如重试后的 Attempt/RunAt）。
	Save(ctx context.Context, job Job) error
	// Delete 删除任务（终态、取消或关闭丢弃）。
	Delete(ctx context.Context, id string) error
	// List 列出全部未完成任务（排队/延迟），供 Restore 恢复。
	List(ctx context.Context) ([]Job, error)
}
