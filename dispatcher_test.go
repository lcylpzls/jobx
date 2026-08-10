package jobx

import (
	"context"
	"errors"
	testx "github.com/lcylpzls/testx"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lcylpzls/errx"
	"github.com/lcylpzls/logx"
)

// testLogger 构造写入丢弃目标的日志器。
func testLogger() logx.Logger {
	logger, err := logx.NewBuilder().EnableWriter(io.Discard, logx.InfoLevel).Build()
	if err != nil {
		panic(err)
	}
	return logger
}

// TestHandle 覆盖处理器注册分支。
func TestHandle(t *testing.T) {
	d, err := NewDispatcher()
	testx.RequireNoError(t, err)

	defer d.Shutdown(context.Background())
	if err := d.Handle("", func(context.Context, Job) error { return nil }); err == nil ||
		!errx.Is(err, CodeJobInvalid) {
		t.Fatalf("空名应报错，实际：%v", err)
	}
	if err := d.Handle(strings.Repeat("x", maxNameLength+1), func(context.Context, Job) error { return nil }); err == nil ||
		!errx.Is(err, CodeJobInvalid) {
		t.Fatalf("超长名应报错，实际：%v", err)
	}
	if err := d.Handle("task", nil); err == nil || !errx.Is(err, CodeJobInvalid) {
		t.Fatalf("空处理器应报错，实际：%v", err)
	}
	if err := d.Handle("task", func(context.Context, Job) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := d.Handle("task", func(context.Context, Job) error { return nil }); err == nil ||
		!errx.Is(err, CodeHandlerConflict) {
		t.Fatalf("重复注册应报错，实际：%v", err)
	}
}

// TestSubmitErrors 覆盖提交参数校验分支。
func TestSubmitErrors(t *testing.T) {
	d, err := NewDispatcher(WithMaxPayloadBytes(8))
	testx.RequireNoError(t, err)

	defer d.Shutdown(context.Background())
	_ = d.Handle("task", func(context.Context, Job) error { return nil })
	if _, err := d.Submit(context.Background(), "task", []byte("123456789")); err == nil ||
		!errx.Is(err, CodeJobInvalid) {
		t.Fatalf("自定义载荷上限应生效，实际：%v", err)
	}
	if err := d.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	d2, err := NewDispatcher(WithClock(func() time.Time {
		return time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)
	}))
	testx.RequireNoError(t, err)

	defer d2.Shutdown(context.Background())
	fixed := time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)
	created := make(chan time.Time, 1)
	_ = d2.Handle("task", func(_ context.Context, job Job) error {
		created <- job.CreatedAt
		return nil
	})
	if _, err := d2.Submit(context.Background(), "task", nil); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-created:
		if !got.Equal(fixed) {
			t.Fatalf("注入时钟应生效：%v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("任务未执行")
	}
	ctx := context.Background()
	if _, err := d.Submit(ctx, "", nil); err == nil || !errx.Is(err, CodeJobInvalid) {
		t.Fatalf("空名应报错，实际：%v", err)
	}
	if _, err := d.Submit(ctx, strings.Repeat("x", maxNameLength+1), nil); err == nil ||
		!errx.Is(err, CodeJobInvalid) {
		t.Fatalf("超长名应报错，实际：%v", err)
	}
	if _, err := d.Submit(ctx, "task", make([]byte, 1<<20+1)); err == nil ||
		!errx.Is(err, CodeJobInvalid) {
		t.Fatalf("超长载荷应报错，实际：%v", err)
	}
	if _, err := d.Submit(ctx, "missing", nil); err == nil || !errx.Is(err, CodeHandlerNotFound) {
		t.Fatalf("未注册处理器应报错，实际：%v", err)
	}
	if _, err := d.Submit(ctx, "task", nil, WithTimeout(-time.Second)); err == nil ||
		!errx.Is(err, CodeJobInvalid) {
		t.Fatalf("负超时应报错，实际：%v", err)
	}
}

// TestSubmitRandFailure 覆盖随机源故障分支。
func TestSubmitRandFailure(t *testing.T) {
	d, err := NewDispatcher()
	testx.RequireNoError(t, err)

	defer d.Shutdown(context.Background())
	_ = d.Handle("task", func(context.Context, Job) error { return nil })
	orig := randRead
	randMu.Lock()
	randRead = func(n int) (string, error) { return "", errors.New("随机源故障") }
	randMu.Unlock()
	defer func() {
		randMu.Lock()
		randRead = orig
		randMu.Unlock()
	}()
	if _, err := d.Submit(context.Background(), "task", nil); err == nil ||
		!errx.Is(err, CodeIDGenerateFailed) {
		t.Fatalf("随机源故障应报错，实际：%v", err)
	}
}

// TestSubmitSuccess 覆盖提交与执行主流程。
func TestSubmitSuccess(t *testing.T) {
	d, err := NewDispatcher(WithWorkers(2))
	testx.RequireNoError(t, err)

	defer d.Shutdown(context.Background())
	got := make(chan Job, 1)
	_ = d.Handle("task", func(_ context.Context, job Job) error {
		got <- job
		return nil
	})
	payload := []byte("original")
	id, err := d.Submit(context.Background(), "task", payload)
	testx.RequireNoError(t, err)

	if len(id) != 32 {
		t.Fatalf("任务 ID 应为 32 位十六进制：%q", id)
	}
	payload[0] = 'X' // 外部修改不应影响已提交载荷。
	select {
	case job := <-got:
		if job.ID != id || job.Name != "task" || string(job.Payload) != "original" {
			t.Fatalf("任务字段不符：%+v", job)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("任务未执行")
	}
}

// TestWorkerPanicRecover 覆盖处理器 panic 恢复后 worker 仍可用。
func TestWorkerPanicRecover(t *testing.T) {
	d, err := NewDispatcher(WithWorkers(1), WithLogger(testLogger()))
	testx.RequireNoError(t, err)

	defer d.Shutdown(context.Background())
	_ = d.Handle("panic", func(context.Context, Job) error { panic("业务故障") })
	_ = d.Handle("ok", func(context.Context, Job) error { return nil })
	if _, err := d.Submit(context.Background(), "panic", nil); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	_ = d.Handle("after", func(context.Context, Job) error { close(done); return nil })
	_, _ = d.Submit(context.Background(), "after", nil)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("panic 后 worker 应继续工作")
	}
}

// TestTimeout 覆盖单任务超时。
func TestTimeout(t *testing.T) {
	d, err := NewDispatcher()
	testx.RequireNoError(t, err)

	defer d.Shutdown(context.Background())
	got := make(chan error, 1)
	_ = d.Handle("slow", func(ctx context.Context, job Job) error {
		<-ctx.Done()
		got <- ctx.Err()
		return ctx.Err()
	})
	if _, err := d.Submit(context.Background(), "slow", nil, WithTimeout(50*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-got:
		testx.RequireErrorIs(t, err, context.DeadlineExceeded)

	case <-time.After(2 * time.Second):
		t.Fatal("任务未超时")
	}
}

// blockHandler 通知 started 后阻塞直至 release 关闭。
func blockHandler(release, started chan struct{}) Handler {
	return func(_ context.Context, _ Job) error {
		close(started)
		<-release
		return nil
	}
}

// TestQueueFullBlock 覆盖阻塞策略。
func TestQueueFullBlock(t *testing.T) {
	d, err := NewDispatcher(WithWorkers(1), WithQueueSize(1), WithConflictPolicy(ConflictAllow))
	testx.RequireNoError(t, err)

	defer d.Shutdown(context.Background())
	release := make(chan struct{})
	started := make(chan struct{})
	_ = d.Handle("block", blockHandler(release, started))
	ctx := context.Background()
	if _, err := d.Submit(ctx, "block", nil); err != nil {
		t.Fatal(err)
	}
	<-started
	if _, err := d.Submit(ctx, "block", nil); err != nil {
		t.Fatal(err)
	}
	blocked := make(chan error, 1)
	go func() {
		_, err := d.Submit(ctx, "block", nil)
		blocked <- err
	}()
	select {
	case <-blocked:
		t.Fatal("队列满时 Block 策略应阻塞提交")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-blocked:
		testx.RequireNoError(t, err)

	case <-time.After(2 * time.Second):
		t.Fatal("阻塞未解除")
	}
}

// TestQueueFullDrop 覆盖丢弃策略。
func TestQueueFullDrop(t *testing.T) {
	d, err := NewDispatcher(WithWorkers(1), WithQueueSize(1), WithQueueFullPolicy(QueueFullDrop),
		WithConflictPolicy(ConflictAllow))
	testx.RequireNoError(t, err)

	defer d.Shutdown(context.Background())
	release := make(chan struct{})
	started := make(chan struct{})
	_ = d.Handle("block", blockHandler(release, started))
	ctx := context.Background()
	if _, err := d.Submit(ctx, "block", nil); err != nil {
		t.Fatal(err)
	}
	<-started
	if _, err := d.Submit(ctx, "block", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Submit(ctx, "block", nil); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("队列满应丢弃并报错，实际：%v", err)
	}
	close(release)
}

// TestSubmitCtxCancel 覆盖阻塞提交随 ctx 取消。
func TestSubmitCtxCancel(t *testing.T) {
	d, err := NewDispatcher(WithWorkers(1), WithQueueSize(1), WithConflictPolicy(ConflictAllow))
	testx.RequireNoError(t, err)

	defer d.Shutdown(context.Background())
	release := make(chan struct{})
	started := make(chan struct{})
	_ = d.Handle("block", blockHandler(release, started))
	if _, err := d.Submit(context.Background(), "block", nil); err != nil {
		t.Fatal(err)
	}
	<-started
	if _, err := d.Submit(context.Background(), "block", nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := d.Submit(ctx, "block", nil)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		testx.RequireErrorIs(t, err, context.Canceled)

	case <-time.After(2 * time.Second):
		t.Fatal("ctx 取消未生效")
	}
	close(release)
}

// TestShutdownDrain 覆盖关闭时排空存量。
func TestShutdownDrain(t *testing.T) {
	d, err := NewDispatcher(WithWorkers(2), WithConflictPolicy(ConflictAllow))
	testx.RequireNoError(t, err)

	var count atomic.Int32
	_ = d.Handle("task", func(context.Context, Job) error { count.Add(1); return nil })
	for i := 0; i < 10; i++ {
		if _, err := d.Submit(context.Background(), "task", nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := count.Load(); got != 10 {
		t.Fatalf("关闭前提交的任务应全部执行：%d", got)
	}
	if err := d.Shutdown(context.Background()); err != nil {
		t.Fatalf("重复关闭应返回首次结果（nil）：%v", err)
	}
}

// TestShutdownRejects 覆盖关闭后拒绝新提交。
func TestShutdownRejects(t *testing.T) {
	d, err := NewDispatcher()
	testx.RequireNoError(t, err)

	_ = d.Handle("task", func(context.Context, Job) error { return nil })
	if err := d.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Submit(context.Background(), "task", nil); !errors.Is(err, ErrShuttingDown) {
		t.Fatalf("关闭后提交应拒绝，实际：%v", err)
	}
}

// TestShutdownTimeout 覆盖关闭超时取消执行中任务。
func TestShutdownTimeout(t *testing.T) {
	d, err := NewDispatcher(WithWorkers(1), WithLogger(testLogger()))
	testx.RequireNoError(t, err)

	release := make(chan struct{})
	started := make(chan struct{})
	_ = d.Handle("block", blockHandler(release, started))
	if _, err := d.Submit(context.Background(), "block", nil, WithTimeout(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	<-started
	time.Sleep(50 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := d.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("关闭超时应返回 ctx 错误，实际：%v", err)
	}
	close(release)
}

// TestHandlerMissingDefensive 白盒覆盖执行时处理器缺失的防御分支。
func TestHandlerMissingDefensive(t *testing.T) {
	d, err := NewDispatcher(WithWorkers(1), WithLogger(testLogger()))
	testx.RequireNoError(t, err)

	defer d.Shutdown(context.Background())
	_ = d.ready.push(context.Background(), &jobEntry{job: Job{ID: "x", Name: "missing"}}, false)
	// worker 消费后不会 panic；关闭等待其退出。
}

// TestPayloadCopy 覆盖载荷深拷贝。
func TestPayloadCopy(t *testing.T) {
	d, err := NewDispatcher()
	testx.RequireNoError(t, err)

	defer d.Shutdown(context.Background())
	got := make(chan []byte, 1)
	_ = d.Handle("task", func(_ context.Context, job Job) error {
		got <- job.Payload
		return nil
	})
	payload := []byte("abc")
	if _, err := d.Submit(context.Background(), "task", payload); err != nil {
		t.Fatal(err)
	}
	payload[0] = 'z'
	select {
	case p := <-got:
		if string(p) != "abc" {
			t.Fatalf("载荷应深拷贝：%q", p)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("任务未执行")
	}
}

// TestLoggerEvents 覆盖日志输出路径（丢弃目标，验证不 panic）。
func TestLoggerEvents(t *testing.T) {
	d, err := NewDispatcher(WithLogger(testLogger()))
	testx.RequireNoError(t, err)

	defer d.Shutdown(context.Background())
	_ = d.Handle("ok", func(context.Context, Job) error { return nil })
	_ = d.Handle("fail", func(context.Context, Job) error { return errors.New("业务失败") })
	if _, err := d.Submit(context.Background(), "ok", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Submit(context.Background(), "fail", nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
}
