package jobx

import (
	"testing"

	"github.com/lcylpzls/errx"
	"github.com/lcylpzls/testx"
)

// TestErrVarKinds 保证预定义错误值通过 NewCode 构造后分类正确
// （注册必须先于包级变量初始化）。
func TestErrVarKinds(t *testing.T) {
	cases := map[string]struct {
		err  error
		kind errx.Kind
	}{
		"配置非法":    {ErrInvalidConfig, errx.KindInvalid},
		"处理器缺失":   {ErrHandlerNotFound, errx.KindNotFound},
		"处理器冲突":   {ErrHandlerConflict, errx.KindAlreadyExists},
		"任务参数非法":  {ErrJobInvalid, errx.KindInvalid},
		"任务不存在":   {ErrJobNotFound, errx.KindNotFound},
		"队列已满":    {ErrQueueFull, errx.KindRateLimited},
		"关闭中":     {ErrShuttingDown, errx.KindUnavailable},
		"超时":      {ErrTimeout, errx.KindTimeout},
		"重试耗尽":    {ErrRetryExhausted, errx.KindInternal},
		"执行失败":    {ErrExecutionFailed, errx.KindInternal},
		"跳过":      {ErrSkipped, errx.KindAlreadyExists},
		"替换":      {ErrReplaced, errx.KindConflict},
		"cron 非法": {ErrCronInvalid, errx.KindInvalid},
		"调度停止":    {ErrSchedulerStopped, errx.KindUnavailable},
		"存储失败":    {ErrStoreInvalid, errx.KindUnavailable},
	}
	for _, tc := range cases {
		testx.RequireEqual(t, errx.KindOf(tc.err), tc.kind)
	}
}
