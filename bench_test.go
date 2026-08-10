package jobx

import (
	"context"
	"errors"
	testx "github.com/lcylpzls/testx"
	"testing"
	"time"
)

// BenchmarkSubmit 测量立即任务提交吞吐（丢弃策略下无阻塞）。
func BenchmarkSubmit(b *testing.B) {
	d, err := NewDispatcher(WithWorkers(4), WithQueueSize(1<<16),
		WithQueueFullPolicy(QueueFullDrop), WithConflictPolicy(ConflictAllow))
	testx.RequireNoError(b, err)

	defer d.Shutdown(context.Background())
	_ = d.Handle("task", func(context.Context, Job) error { return nil })
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = d.Submit(ctx, "task", nil)
	}
}

// BenchmarkSubmitDelayed 测量延迟任务提交吞吐。
func BenchmarkSubmitDelayed(b *testing.B) {
	d, err := NewDispatcher(WithWorkers(4), WithConflictPolicy(ConflictAllow))
	testx.RequireNoError(b, err)

	defer d.Shutdown(context.Background())
	_ = d.Handle("task", func(context.Context, Job) error { return nil })
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = d.SubmitAfter(ctx, "task", nil, time.Hour)
	}
}

// BenchmarkSubmitConflict 测量默认冲突策略下同名提交路径。
func BenchmarkSubmitConflict(b *testing.B) {
	d, err := NewDispatcher(WithWorkers(4), WithQueueFullPolicy(QueueFullDrop))
	testx.RequireNoError(b, err)

	defer d.Shutdown(context.Background())
	_ = d.Handle("task", func(context.Context, Job) error { return nil })
	ctx := context.Background()
	_, _ = d.Submit(ctx, "task", nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = d.Submit(ctx, "task", nil)
	}
}

// BenchmarkDelayPrecision 测量 10ms 延迟任务的调度精度。
func BenchmarkDelayPrecision(b *testing.B) {
	d, err := NewDispatcher(WithWorkers(1))
	testx.RequireNoError(b, err)

	defer d.Shutdown(context.Background())
	done := make(chan time.Time, 1)
	_ = d.Handle("task", func(_ context.Context, _ Job) error {
		done <- time.Now()
		return nil
	})
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		if _, err := d.SubmitAfter(ctx, "task", nil, 10*time.Millisecond); err != nil {
			b.Fatal(err)
		}
		<-done
		b.ReportMetric(float64(time.Since(start)-10*time.Millisecond), "偏差ns/op")
	}
}

// BenchmarkRetrySubmit 测量带重试配置的任务提交路径。
func BenchmarkRetrySubmit(b *testing.B) {
	d, err := NewDispatcher(WithWorkers(4), WithQueueFullPolicy(QueueFullDrop),
		WithConflictPolicy(ConflictAllow))
	testx.RequireNoError(b, err)

	defer d.Shutdown(context.Background())
	_ = d.Handle("task", func(context.Context, Job) error { return errors.New("失败") })
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = d.Submit(ctx, "task", nil, WithRetry(2, time.Millisecond))
	}
}
