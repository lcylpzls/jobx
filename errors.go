package jobx

import "github.com/lcylpzls/errx"

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
)

// 预定义错误值，可用 errx.Is / errors.Is 判断。
var (
	// ErrInvalidConfig 配置非法。
	ErrInvalidConfig = errx.New(errx.KindInvalid, CodeInvalidConfig, "配置非法")
	// ErrHandlerNotFound 处理器未注册。
	ErrHandlerNotFound = errx.New(errx.KindNotFound, CodeHandlerNotFound, "处理器未注册")
	// ErrHandlerConflict 处理器重复注册。
	ErrHandlerConflict = errx.New(errx.KindAlreadyExists, CodeHandlerConflict, "处理器重复注册")
	// ErrJobInvalid 任务参数非法。
	ErrJobInvalid = errx.New(errx.KindInvalid, CodeJobInvalid, "任务参数非法")
	// ErrJobNotFound 任务或调度条目不存在。
	ErrJobNotFound = errx.New(errx.KindNotFound, CodeJobNotFound, "任务或调度条目不存在")
	// ErrQueueFull 任务队列已满。
	ErrQueueFull = errx.New(errx.KindRateLimited, CodeQueueFull, "任务队列已满")
	// ErrShuttingDown 执行器关闭中。
	ErrShuttingDown = errx.New(errx.KindUnavailable, CodeShuttingDown, "执行器关闭中")
	// ErrTimeout 任务执行超时。
	ErrTimeout = errx.New(errx.KindTimeout, CodeTimeout, "任务执行超时")
	// ErrRetryExhausted 任务重试耗尽。
	ErrRetryExhausted = errx.New(errx.KindInternal, CodeRetryExhausted, "任务重试耗尽")
	// ErrExecutionFailed 处理器执行失败（含 panic）。
	ErrExecutionFailed = errx.New(errx.KindInternal, CodeExecutionFailed, "处理器执行失败")
	// ErrSkipped 同名任务在途，本次提交被跳过。
	ErrSkipped = errx.New(errx.KindAlreadyExists, CodeSkipped, "同名任务在途，本次提交被跳过")
	// ErrReplaced 同名旧任务已被替换取消。
	ErrReplaced = errx.New(errx.KindConflict, CodeReplaced, "同名旧任务已被替换取消")
	// ErrCronInvalid cron 表达式非法。
	ErrCronInvalid = errx.New(errx.KindInvalid, CodeCronInvalid, "cron 表达式非法")
	// ErrSchedulerStopped 调度器已停止。
	ErrSchedulerStopped = errx.New(errx.KindUnavailable, CodeSchedulerStopped, "调度器已停止")
)
