package core

import (
	"context"
	"errors"
	"fmt"
	testx "github.com/lcylpzls/testx"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lcylpzls/errx"
)

// TestConflictPolicyInvalid 覆盖非法冲突策略。
func TestConflictPolicyInvalid(t *testing.T) {
	if _, err := NewDispatcher(WithConflictPolicy(ConflictPolicy(99))); err == nil ||
		!errx.Is(err, CodeInvalidConfig) {
		t.Fatalf("非法冲突策略应报错，实际：%v", err)
	}
}

// TestConflictSkip 覆盖默认跳过策略。
func TestConflictSkip(t *testing.T) {
	var skipped atomic.Int32
	d, err := NewDispatcher(WithWorkers(1), WithMetrics(Metrics{
		Skipped: func(string) { skipped.Add(1) },
	}))
	testx.RequireNoError(t, err)

	defer d.Shutdown(context.Background())
	release := make(chan struct{})
	started := make(chan struct{})
	_ = d.Handle("dup", blockHandler(release, started))
	id1, err := d.Submit(context.Background(), "dup", nil)
	testx.RequireNoError(t, err)

	<-started
	if _, err := d.Submit(context.Background(), "dup", nil); !errors.Is(err, ErrSkipped) {
		t.Fatalf("同名在途应跳过，实际：%v", err)
	}
	if skipped.Load() != 1 {
		t.Fatalf("跳过指标应为 1，实际 %d", skipped.Load())
	}
	if s, _ := d.JobStatus(id1); s != StatusRunning {
		t.Fatalf("旧任务应不受影响：%v", s)
	}
	close(release)
}

// TestConflictReplaceDelayed 覆盖替换延迟任务。
func TestConflictReplaceDelayed(t *testing.T) {
	var replaced atomic.Int32
	d, err := NewDispatcher(WithConflictPolicy(ConflictReplace), WithMetrics(Metrics{
		Replaced: func(string) { replaced.Add(1) },
	}))
	testx.RequireNoError(t, err)

	defer d.Shutdown(context.Background())
	executed := make(chan string, 2)
	_ = d.Handle("refresh", func(_ context.Context, job Job) error {
		executed <- job.ID
		return nil
	})
	oldID, err := d.SubmitAfter(context.Background(), "refresh", nil, time.Hour)
	testx.RequireNoError(t, err)

	if s, _ := d.JobStatus(oldID); s != StatusDelayed {
		t.Fatalf("旧任务应处于延迟：%v", s)
	}
	newID, err := d.Submit(context.Background(), "refresh", nil)
	testx.RequireNoError(t, err)

	if replaced.Load() != 1 {
		t.Fatalf("替换指标应为 1，实际 %d", replaced.Load())
	}
	if s, _ := d.JobStatus(oldID); s != StatusCancelled {
		t.Fatalf("旧任务应已取消：%v", s)
	}
	select {
	case id := <-executed:
		testx.RequireEqual(t, id, newID)

	case <-time.After(2 * time.Second):
		t.Fatal("新任务未执行")
	}
}

// TestConflictReplaceQueued 覆盖替换排队任务。
func TestConflictReplaceQueued(t *testing.T) {
	d, err := NewDispatcher(WithWorkers(1), WithQueueSize(1), WithConflictPolicy(ConflictReplace))
	testx.RequireNoError(t, err)

	defer d.Shutdown(context.Background())
	release := make(chan struct{})
	started := make(chan struct{})
	_ = d.Handle("refresh", blockHandler(release, started))
	ctx := context.Background()
	first, err := d.Submit(ctx, "refresh", nil)
	testx.RequireNoError(t, err)

	<-started
	queued, err := d.Submit(ctx, "refresh", nil)
	testx.RequireNoError(t, err)

	replaced, err := d.Submit(ctx, "refresh", nil)
	testx.RequireNoError(t, err)

	if s, _ := d.JobStatus(queued); s != StatusCancelled {
		t.Fatalf("排队旧任务应取消：%v", s)
	}
	if s, _ := d.JobStatus(first); s != StatusCancelled {
		t.Fatalf("执行中旧任务应标记取消：%v", s)
	}
	_ = replaced
	close(release)
}

// TestConflictReplaceRunning 覆盖替换执行中任务（ctx 协作取消）。
func TestConflictReplaceRunning(t *testing.T) {
	d, err := NewDispatcher(WithWorkers(1), WithConflictPolicy(ConflictReplace))
	testx.RequireNoError(t, err)

	defer d.Shutdown(context.Background())
	cancelled := make(chan struct{})
	done := make(chan struct{})
	_ = d.Handle("refresh", func(ctx context.Context, _ Job) error {
		select {
		case <-ctx.Done():
			close(cancelled)
			return ctx.Err()
		case <-done:
			return nil
		}
	})
	first, err := d.Submit(context.Background(), "refresh", nil)
	testx.RequireNoError(t, err)

	time.Sleep(50 * time.Millisecond)
	if _, err := d.Submit(context.Background(), "refresh", nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("执行中任务应被取消")
	}
	if s, _ := d.JobStatus(first); s != StatusCancelled {
		t.Fatalf("旧任务终态应为取消：%v", s)
	}
	close(done)
}

// TestConflictAllow 覆盖允许并发。
func TestConflictAllow(t *testing.T) {
	d, err := NewDispatcher(WithWorkers(2), WithConflictPolicy(ConflictAllow))
	testx.RequireNoError(t, err)

	defer d.Shutdown(context.Background())
	var running atomic.Int32
	var maxConcurrent atomic.Int32
	release := make(chan struct{})
	_ = d.Handle("parallel", func(context.Context, Job) error {
		cur := running.Add(1)
		for {
			old := maxConcurrent.Load()
			if cur <= old || maxConcurrent.CompareAndSwap(old, cur) {
				break
			}
		}
		<-release
		running.Add(-1)
		return nil
	})
	if _, err := d.Submit(context.Background(), "parallel", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Submit(context.Background(), "parallel", nil); err != nil {
		t.Fatalf("Allow 策略不应跳过：%v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for maxConcurrent.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if maxConcurrent.Load() < 2 {
		t.Fatalf("应并发执行 2 个任务：%d", maxConcurrent.Load())
	}
	close(release)
}

// TestSubmitAtPast 覆盖过去时刻立即执行。
func TestSubmitAtPast(t *testing.T) {
	d, err := NewDispatcher()
	testx.RequireNoError(t, err)

	defer d.Shutdown(context.Background())
	got := make(chan struct{})
	_ = d.Handle("task", func(context.Context, Job) error { close(got); return nil })
	_, err = d.SubmitAt(context.Background(), "task", nil, time.Now().Add(-time.Hour))
	testx.RequireNoError(t, err)

	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("过去时刻应立即执行")
	}
}

// TestSubmitAfter 覆盖延迟执行与参数校验。
func TestSubmitAfter(t *testing.T) {
	d, err := NewDispatcher()
	testx.RequireNoError(t, err)

	defer d.Shutdown(context.Background())
	if _, err := d.SubmitAfter(context.Background(), "task", nil, -time.Second); err == nil ||
		!errx.Is(err, CodeJobInvalid) {
		t.Fatalf("负延迟应报错，实际：%v", err)
	}
	got := make(chan struct{})
	_ = d.Handle("task", func(context.Context, Job) error { close(got); return nil })
	id, err := d.SubmitAfter(context.Background(), "task", nil, 20*time.Millisecond)
	testx.RequireNoError(t, err)

	if s, _ := d.JobStatus(id); s != StatusDelayed {
		t.Fatalf("提交后应处于延迟：%v", s)
	}
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("延迟任务未执行")
	}
	if s, _ := d.JobStatus(id); s != StatusSucceeded {
		t.Fatalf("终态应为成功：%v", s)
	}
}

// TestSubmitOptionsInvalid 覆盖新选项校验。
func TestSubmitOptionsInvalid(t *testing.T) {
	d, err := NewDispatcher()
	testx.RequireNoError(t, err)

	defer d.Shutdown(context.Background())
	_ = d.Handle("task", func(context.Context, Job) error { return nil })
	ctx := context.Background()
	if _, err := d.Submit(ctx, "task", nil, WithRunAt(time.Time{})); err == nil ||
		!errx.Is(err, CodeJobInvalid) {
		t.Fatalf("零值执行时刻应报错，实际：%v", err)
	}
	if _, err := d.Submit(ctx, "task", nil, WithRetry(-1, time.Second)); err == nil ||
		!errx.Is(err, CodeJobInvalid) {
		t.Fatalf("负重试次数应报错，实际：%v", err)
	}
	if _, err := d.Submit(ctx, "task", nil, WithRetry(101, time.Second)); err == nil ||
		!errx.Is(err, CodeJobInvalid) {
		t.Fatalf("超上限重试次数应报错，实际：%v", err)
	}
	if _, err := d.Submit(ctx, "task", nil, WithRetry(1, -time.Second)); err == nil ||
		!errx.Is(err, CodeJobInvalid) {
		t.Fatalf("负重试延迟应报错，实际：%v", err)
	}
}

// TestRetrySuccess 覆盖失败重试后成功。
func TestRetrySuccess(t *testing.T) {
	var calls atomic.Int32
	var retried atomic.Int32
	d, err := NewDispatcher(WithLogger(testLogger()), WithMetrics(Metrics{
		Retried: func(_ string, attempt int) { retried.Store(int32(attempt)) },
	}))
	testx.RequireNoError(t, err)

	defer d.Shutdown(context.Background())
	_ = d.Handle("flaky", func(_ context.Context, job Job) error {
		n := calls.Add(1)
		if n < 3 {
			return errors.New("暂时失败")
		}
		return nil
	})
	id, err := d.Submit(context.Background(), "flaky", nil,
		WithRetry(2, 5*time.Millisecond))
	testx.RequireNoError(t, err)

	deadline := time.Now().Add(3 * time.Second)
	for {
		s, _ := d.JobStatus(id)
		if s == StatusSucceeded {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("重试后应成功，当前状态：%v", s)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if calls.Load() != 3 {
		t.Fatalf("应执行 3 次：%d", calls.Load())
	}
	if retried.Load() != 2 {
		t.Fatalf("应重试 2 次：%d", retried.Load())
	}
}

// TestRetryExhausted 覆盖重试耗尽最终失败。
func TestRetryExhausted(t *testing.T) {
	var calls atomic.Int32
	var failed atomic.Int32
	d, err := NewDispatcher(WithMetrics(Metrics{
		Failed: func(_ string, _ error) { failed.Add(1) },
	}))
	testx.RequireNoError(t, err)

	defer d.Shutdown(context.Background())
	_ = d.Handle("always", func(context.Context, Job) error { calls.Add(1); return errors.New("始终失败") })
	id, err := d.Submit(context.Background(), "always", nil,
		WithRetry(1, time.Millisecond))
	testx.RequireNoError(t, err)

	deadline := time.Now().Add(3 * time.Second)
	for {
		s, _ := d.JobStatus(id)
		if s == StatusFailed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("重试耗尽应失败，当前状态：%v", s)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if calls.Load() != 2 {
		t.Fatalf("应执行 2 次（1 次 + 1 次重试）：%d", calls.Load())
	}
	if failed.Load() != 1 {
		t.Fatalf("失败指标应为 1：%d", failed.Load())
	}
}

// TestCancel 覆盖三类任务取消。
func TestCancel(t *testing.T) {
	d, err := NewDispatcher(WithWorkers(1), WithQueueSize(1), WithConflictPolicy(ConflictAllow))
	testx.RequireNoError(t, err)

	defer d.Shutdown(context.Background())
	// 延迟任务取消。
	_ = d.Handle("delayed", func(context.Context, Job) error { return nil })
	delayedID, err := d.SubmitAfter(context.Background(), "delayed", nil, time.Hour)
	testx.RequireNoError(t, err)

	if err := d.Cancel(delayedID); err != nil {
		t.Fatal(err)
	}
	if s, _ := d.JobStatus(delayedID); s != StatusCancelled {
		t.Fatalf("延迟任务应取消：%v", s)
	}
	if err := d.Cancel("missing"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("未知 ID 应报不存在：%v", err)
	}
	// 排队任务取消。
	release := make(chan struct{})
	started := make(chan struct{})
	_ = d.Handle("block", blockHandler(release, started))
	if _, err := d.Submit(context.Background(), "block", nil); err != nil {
		t.Fatal(err)
	}
	<-started
	queuedID, err := d.Submit(context.Background(), "block", nil)
	testx.RequireNoError(t, err)

	if err := d.Cancel(queuedID); err != nil {
		t.Fatal(err)
	}
	// 执行中任务取消。
	runningID, err := d.Submit(context.Background(), "block", nil)
	testx.RequireNoError(t, err)

	if err := d.Cancel(runningID); err != nil {
		t.Fatal(err)
	}
	close(release)
}

// TestShutdownDropDelayed 覆盖关闭时丢弃延迟任务。
func TestShutdownDropDelayed(t *testing.T) {
	var dropped atomic.Int32
	var executed atomic.Int32
	d, err := NewDispatcher(WithMetrics(Metrics{
		Dropped: func(string) { dropped.Add(1) },
	}))
	testx.RequireNoError(t, err)

	_ = d.Handle("later", func(context.Context, Job) error { executed.Add(1); return nil })
	if _, err := d.SubmitAfter(context.Background(), "later", nil, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := d.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if dropped.Load() != 1 {
		t.Fatalf("关闭应丢弃延迟任务：%d", dropped.Load())
	}
	if executed.Load() != 0 {
		t.Fatalf("延迟任务不应执行：%d", executed.Load())
	}
}

// TestStatusLimit 覆盖状态表容量上限逐出。
func TestStatusLimit(t *testing.T) {
	d, err := NewDispatcher()
	testx.RequireNoError(t, err)

	defer d.Shutdown(context.Background())
	for i := 0; i < maxStatusCount+1; i++ {
		d.markStatus(fmt.Sprintf("id-%d", i), StatusQueued)
	}
	if _, err := d.JobStatus("id-0"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("最旧状态应被逐出，实际：%v", err)
	}
	if s, err := d.JobStatus(fmt.Sprintf("id-%d", maxStatusCount)); err != nil || s != StatusQueued {
		t.Fatalf("最新状态应保留：%v %v", s, err)
	}
}

// TestInFlightRelease 覆盖终态释放后同名可再次提交。
func TestInFlightRelease(t *testing.T) {
	d, err := NewDispatcher()
	testx.RequireNoError(t, err)

	defer d.Shutdown(context.Background())
	got := make(chan struct{}, 2)
	_ = d.Handle("task", func(context.Context, Job) error { got <- struct{}{}; return nil })
	id1, err := d.Submit(context.Background(), "task", nil)
	testx.RequireNoError(t, err)

	<-got
	// CI 环境（尤其 Windows）调度偶有延迟，超时放宽到 10 秒避免偶发失败。
	deadline := time.Now().Add(10 * time.Second)
	for {
		s, _ := d.JobStatus(id1)
		if s == StatusSucceeded {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("任务未完成")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := d.Submit(context.Background(), "task", nil); err != nil {
		t.Fatalf("完成后应可再次提交：%v", err)
	}
	<-got
}
