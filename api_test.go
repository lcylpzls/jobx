package jobx_test

import (
	"context"
	"testing"
	"time"

	"github.com/lcylpzls/jobx"
)

// nopStore 是冒烟测试用的空任务存储实现。
type nopStore struct{}

func (nopStore) Save(context.Context, jobx.Job) error     { return nil }
func (nopStore) Delete(context.Context, string) error     { return nil }
func (nopStore) List(context.Context) ([]jobx.Job, error) { return nil, nil }

// TestPublicAPI 黑盒冒烟测试：覆盖根包全部转发函数、类型别名与常量。
func TestPublicAPI(t *testing.T) {
	d, err := jobx.NewDispatcher(
		jobx.WithWorkers(2),
		jobx.WithQueueSize(16),
		jobx.WithQueueFullPolicy(jobx.QueueFullBlock),
		jobx.WithConflictPolicy(jobx.ConflictSkip),
		jobx.WithMaxPayloadBytes(1024),
		jobx.WithLogger(nil),
		jobx.WithMetrics(jobx.Metrics{}),
		jobx.WithStore(nopStore{}),
		jobx.WithClock(time.Now),
		jobx.WithTraceHook(nil),
		jobx.WithEventHook(nil),
	)
	if err != nil || d == nil {
		t.Fatalf("NewDispatcher 失败：%v", err)
	}
	defer d.Shutdown(context.Background())

	if err := d.Handle("smoke", func(context.Context, jobx.Job) error { return nil }); err != nil {
		t.Fatalf("Handle 失败：%v", err)
	}
	_, err = d.Submit(context.Background(), "smoke", []byte("data"),
		jobx.WithTimeout(time.Second),
		jobx.WithRunAt(time.Now()),
		jobx.WithRetry(2, time.Millisecond),
	)
	if err != nil {
		t.Fatalf("Submit 失败：%v", err)
	}

	s, err := jobx.NewScheduler(d,
		jobx.WithLocation(time.UTC),
		jobx.WithSchedulerLogger(nil),
	)
	if err != nil || s == nil {
		t.Fatalf("NewScheduler 失败：%v", err)
	}
	defer s.Shutdown(context.Background())

	_ = jobx.QueueFullDrop
	_ = jobx.ConflictReplace
	_ = jobx.ConflictAllow
	_ = jobx.StatusQueued
	_ = jobx.StatusDelayed
	_ = jobx.StatusRunning
	_ = jobx.StatusSucceeded
	_ = jobx.StatusFailed
	_ = jobx.StatusCancelled
	_ = jobx.CodeInvalidConfig
	_ = jobx.CodeHandlerNotFound
	_ = jobx.CodeHandlerConflict
	_ = jobx.CodeJobInvalid
	_ = jobx.CodeJobNotFound
	_ = jobx.CodeQueueFull
	_ = jobx.CodeShuttingDown
	_ = jobx.CodeTimeout
	_ = jobx.CodeRetryExhausted
	_ = jobx.CodeExecutionFailed
	_ = jobx.CodeSkipped
	_ = jobx.CodeReplaced
	_ = jobx.CodeCronInvalid
	_ = jobx.CodeSchedulerStopped
	_ = jobx.CodeStoreInvalid
	_ = jobx.CodeIDGenerateFailed
	_ = jobx.CodeJobCancelled

	var _ jobx.QueueFullPolicy
	var _ jobx.ConflictPolicy
	var _ jobx.Metrics
	var _ jobx.Option
	var _ jobx.Job
	var _ jobx.Handler
	var _ jobx.SubmitOption
	var _ jobx.Status
	var _ jobx.TaskEvent
	var _ jobx.EventHook
	var _ jobx.TraceAttr
	var _ jobx.TraceHook
	var _ jobx.Store
	var _ jobx.Schedule
	var _ jobx.SchedulerOption
}
