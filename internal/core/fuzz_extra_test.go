package core

import (
	"context"
	testx "github.com/lcylpzls/testx"
	"testing"
	"time"
)

// FuzzScheduler 模糊测试调度器注册路径，确保任意输入不 panic。
func FuzzScheduler(f *testing.F) {
	f.Add("* * * * *", "task", int64(60))
	f.Add("", "", int64(-1))
	f.Fuzz(func(t *testing.T, expr, name string, intervalSec int64) {
		if len(expr) > 128 || len(name) > 300 {
			t.Skip("输入过大")
		}
		d, err := NewDispatcher(WithWorkers(1), WithQueueSize(4))
		testx.RequireNoError(t, err)

		defer d.Shutdown(context.Background())
		_ = d.Handle("task", func(context.Context, Job) error { return nil })
		s, err := NewScheduler(d)
		testx.RequireNoError(t, err)

		defer s.Shutdown(context.Background())
		_, _ = s.Cron(expr, "task")
		if intervalSec >= 1 && intervalSec <= 86400 {
			_, _ = s.Every(time.Duration(intervalSec)*time.Second, "task")
		}
		_, _ = s.EveryMinuteAt(int(intervalSec%60), "task")
		_ = s.List()
	})
}

// FuzzOptions 模糊测试构造选项边界，确保任意输入不 panic。
func FuzzOptions(f *testing.F) {
	f.Add(0, 0, 0, uint8(0))
	f.Add(-1, 1, 1, uint8(99))
	f.Fuzz(func(t *testing.T, workers, queueSize, payload int, mode uint8) {
		// 限制规模并立即关闭，避免海量 worker goroutine 泄漏压垮 fuzz 进程。
		w := workers%64 + 1
		q := queueSize%4096 + 1
		p := payload%(1<<20) + 1
		d, err := NewDispatcher(
			WithWorkers(w),
			WithQueueSize(q),
			WithMaxPayloadBytes(p),
			WithQueueFullPolicy(QueueFullPolicy(mode%4)),
			WithConflictPolicy(ConflictPolicy(mode%4)),
		)
		if err != nil {
			return
		}
		_ = d.Shutdown(context.Background())
	})
}
