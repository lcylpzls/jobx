package jobx

import "github.com/lcylpzls/errx"

// registerCodes 在错误值初始化前完成注册，保证 NewCode 自动分类生效
// （包级变量初始化先于 init 执行，故不用 init 注册）。
var _ = registerCodes()

func registerCodes() bool {
	errx.RegisterCode(CodeInvalidConfig, "配置非法")
	errx.RegisterCodeKind(CodeInvalidConfig, errx.KindInvalid)
	errx.RegisterCode(CodeHandlerNotFound, "未注册处理器")
	errx.RegisterCodeKind(CodeHandlerNotFound, errx.KindNotFound)
	errx.RegisterCode(CodeHandlerConflict, "处理器重复注册")
	errx.RegisterCodeKind(CodeHandlerConflict, errx.KindAlreadyExists)
	errx.RegisterCode(CodeJobInvalid, "任务参数非法")
	errx.RegisterCodeKind(CodeJobInvalid, errx.KindInvalid)
	errx.RegisterCode(CodeJobNotFound, "任务或调度条目不存在")
	errx.RegisterCodeKind(CodeJobNotFound, errx.KindNotFound)
	errx.RegisterCode(CodeQueueFull, "就绪队列已满")
	errx.RegisterCodeKind(CodeQueueFull, errx.KindRateLimited)
	errx.RegisterCode(CodeShuttingDown, "关闭中拒绝新任务")
	errx.RegisterCodeKind(CodeShuttingDown, errx.KindUnavailable)
	errx.RegisterCode(CodeTimeout, "单任务执行超时")
	errx.RegisterCodeKind(CodeTimeout, errx.KindTimeout)
	errx.RegisterCode(CodeRetryExhausted, "重试耗尽")
	errx.RegisterCodeKind(CodeRetryExhausted, errx.KindInternal)
	errx.RegisterCode(CodeExecutionFailed, "处理器执行失败")
	errx.RegisterCodeKind(CodeExecutionFailed, errx.KindInternal)
	errx.RegisterCode(CodeSkipped, "同名任务在途,本次提交被跳过")
	errx.RegisterCodeKind(CodeSkipped, errx.KindAlreadyExists)
	errx.RegisterCode(CodeReplaced, "同名旧任务已被替换取消")
	errx.RegisterCodeKind(CodeReplaced, errx.KindConflict)
	errx.RegisterCode(CodeCronInvalid, "cron 表达式非法")
	errx.RegisterCodeKind(CodeCronInvalid, errx.KindInvalid)
	errx.RegisterCode(CodeSchedulerStopped, "调度器已停止")
	errx.RegisterCodeKind(CodeSchedulerStopped, errx.KindUnavailable)
	errx.RegisterCode(CodeStoreInvalid, "任务存储读写失败")
	errx.RegisterCodeKind(CodeStoreInvalid, errx.KindUnavailable)
	errx.RegisterCode(CodeIDGenerateFailed, "任务 ID 生成失败")
	errx.RegisterCodeKind(CodeIDGenerateFailed, errx.KindUnavailable)
	errx.RegisterCode(CodeJobCancelled, "任务已取消")
	errx.RegisterCodeKind(CodeJobCancelled, errx.KindCancelled)
	return true
}

// 错误码统一以 jobx_ 为前缀。
const (
	// CodeInvalidConfig 配置非法。
	CodeInvalidConfig errx.Code = "jobx_invalid_config"
	// CodeHandlerNotFound 未注册处理器。
	CodeHandlerNotFound errx.Code = "jobx_handler_not_found"
	// CodeHandlerConflict 处理器重复注册。
	CodeHandlerConflict errx.Code = "jobx_handler_conflict"
	// CodeJobInvalid 任务参数非法。
	CodeJobInvalid errx.Code = "jobx_job_invalid"
	// CodeJobNotFound 任务或调度条目不存在。
	CodeJobNotFound errx.Code = "jobx_job_not_found"
	// CodeQueueFull 就绪队列已满（丢弃策略）。
	CodeQueueFull errx.Code = "jobx_queue_full"
	// CodeShuttingDown 关闭中拒绝新任务。
	CodeShuttingDown errx.Code = "jobx_shutting_down"
	// CodeTimeout 单任务执行超时。
	CodeTimeout errx.Code = "jobx_timeout"
	// CodeRetryExhausted 重试耗尽。
	CodeRetryExhausted errx.Code = "jobx_retry_exhausted"
	// CodeExecutionFailed 处理器执行失败（含 panic）。
	CodeExecutionFailed errx.Code = "jobx_execution_failed"
	// CodeSkipped 同名任务在途，本次提交被跳过。
	CodeSkipped errx.Code = "jobx_skipped"
	// CodeReplaced 同名旧任务已被替换取消。
	CodeReplaced errx.Code = "jobx_replaced"
	// CodeCronInvalid cron 表达式非法。
	CodeCronInvalid errx.Code = "jobx_cron_invalid"
	// CodeSchedulerStopped 调度器已停止。
	CodeSchedulerStopped errx.Code = "jobx_scheduler_stopped"
	// CodeStoreInvalid 任务存储读写失败。
	CodeStoreInvalid errx.Code = "jobx_store_invalid"
	// CodeIDGenerateFailed 任务 ID 生成失败。
	CodeIDGenerateFailed errx.Code = "jobx_id_generate_failed"
	// CodeJobCancelled 任务已取消。
	CodeJobCancelled errx.Code = "jobx_job_cancelled"
)

// 预定义错误值，可用 errx.Is / errors.Is 判断。
var (
	// ErrInvalidConfig 配置非法。
	ErrInvalidConfig = errx.NewCode(CodeInvalidConfig, "配置非法")
	// ErrHandlerNotFound 处理器未注册。
	ErrHandlerNotFound = errx.NewCode(CodeHandlerNotFound, "处理器未注册")
	// ErrHandlerConflict 处理器重复注册。
	ErrHandlerConflict = errx.NewCode(CodeHandlerConflict, "处理器重复注册")
	// ErrJobInvalid 任务参数非法。
	ErrJobInvalid = errx.NewCode(CodeJobInvalid, "任务参数非法")
	// ErrJobNotFound 任务或调度条目不存在。
	ErrJobNotFound = errx.NewCode(CodeJobNotFound, "任务或调度条目不存在")
	// ErrQueueFull 任务队列已满。
	ErrQueueFull = errx.NewCode(CodeQueueFull, "任务队列已满")
	// ErrShuttingDown 执行器关闭中。
	ErrShuttingDown = errx.NewCode(CodeShuttingDown, "执行器关闭中")
	// ErrTimeout 任务执行超时。
	ErrTimeout = errx.NewCode(CodeTimeout, "任务执行超时")
	// ErrRetryExhausted 任务重试耗尽。
	ErrRetryExhausted = errx.NewCode(CodeRetryExhausted, "任务重试耗尽")
	// ErrExecutionFailed 处理器执行失败（含 panic）。
	ErrExecutionFailed = errx.NewCode(CodeExecutionFailed, "处理器执行失败")
	// ErrSkipped 同名任务在途，本次提交被跳过。
	ErrSkipped = errx.NewCode(CodeSkipped, "同名任务在途，本次提交被跳过")
	// ErrReplaced 同名旧任务已被替换取消。
	ErrReplaced = errx.NewCode(CodeReplaced, "同名旧任务已被替换取消")
	// ErrCronInvalid cron 表达式非法。
	ErrCronInvalid = errx.NewCode(CodeCronInvalid, "cron 表达式非法")
	// ErrSchedulerStopped 调度器已停止。
	ErrSchedulerStopped = errx.NewCode(CodeSchedulerStopped, "调度器已停止")
	// ErrStoreInvalid 任务存储读写失败。
	ErrStoreInvalid = errx.NewCode(CodeStoreInvalid, "任务存储读写失败")
)
