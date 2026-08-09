package jobx

import (
	"container/heap"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestDrainTimer 覆盖定时器清理的两个分支。
func TestDrainTimer(t *testing.T) {
	fired := time.NewTimer(20 * time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	drainTimer(fired) // 已触发：丢弃 C 中的事件。
	pending := time.NewTimer(time.Hour)
	drainTimer(pending) // 未触发：直接停止。
}

// TestDelayHeapOrder 覆盖最小堆排序比较。
func TestDelayHeapOrder(t *testing.T) {
	d, err := NewDispatcher(WithWorkers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Shutdown(context.Background())
	t1 := time.Now().Add(time.Hour)
	t2 := time.Now().Add(2 * time.Hour)
	d.delayMu.Lock()
	heap.Push(&d.delayHeap, &jobEntry{job: Job{ID: "a", RunAt: t2}})
	heap.Push(&d.delayHeap, &jobEntry{job: Job{ID: "b", RunAt: t1}})
	if d.delayHeap[0].job.ID != "b" {
		d.delayMu.Unlock()
		t.Fatal("最小堆应按 RunAt 升序")
	}
	d.delayMu.Unlock()
}

// TestPushDelayedSignalFull 覆盖信号通道已满时的非阻塞发送。
func TestPushDelayedSignalFull(t *testing.T) {
	d, err := NewDispatcher(WithWorkers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Shutdown(context.Background())
	d.signal <- struct{}{} // 手动填满信号通道。
	d.pushDelayed(&jobEntry{job: Job{ID: "x", RunAt: time.Now().Add(time.Hour)}})
}

// TestDelayLoopWaitNegative 覆盖过期条目的零等待。
func TestDelayLoopWaitNegative(t *testing.T) {
	d, err := NewDispatcher(WithWorkers(1))
	if err != nil {
		t.Fatal(err)
	}
	_ = d.Handle("task", func(context.Context, Job) error { return nil })
	d.pushDelayed(&jobEntry{job: Job{ID: "past", Name: "task",
		RunAt: time.Now().Add(-time.Second)}})
	if err := d.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestPopDueNotDue 覆盖堆顶未到期时直接返回。
func TestPopDueNotDue(t *testing.T) {
	d, err := NewDispatcher(WithWorkers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Shutdown(context.Background())
	d.pushDelayed(&jobEntry{job: Job{ID: "future", RunAt: time.Now().Add(time.Hour)}})
	d.popDue()
}

// TestPopDueCancelled 覆盖到期条目已被取消的跳过分支。
func TestPopDueCancelled(t *testing.T) {
	d, err := NewDispatcher(WithWorkers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Shutdown(context.Background())
	d.cancelled.Store("cancelled-id", struct{}{})
	d.pushDelayed(&jobEntry{job: Job{ID: "cancelled-id", Name: "task",
		RunAt: time.Now().Add(-time.Second)}})
	d.popDue()
}

// TestCancelledQueuedRace 覆盖排队任务出队时已被取消的跳过分支。
func TestCancelledQueuedRace(t *testing.T) {
	d, err := NewDispatcher(WithWorkers(1))
	if err != nil {
		t.Fatal(err)
	}
	_ = d.Handle("task", func(context.Context, Job) error { return nil })
	d.cancelled.Store("cancelled-id", struct{}{})
	_ = d.ready.push(context.Background(),
		&jobEntry{job: Job{ID: "cancelled-id", Name: "task"}}, false)
	time.Sleep(50 * time.Millisecond)
	if err := d.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestReadyQueueClose 覆盖队列关闭的各分支。
func TestReadyQueueClose(t *testing.T) {
	ctx := context.Background()
	q := newReadyQueue(1)
	if err := q.push(ctx, &jobEntry{job: Job{ID: "a"}}, false); err != nil {
		t.Fatal(err)
	}
	blocked := make(chan error, 1)
	go func() { blocked <- q.push(ctx, &jobEntry{job: Job{ID: "b"}}, false) }()
	time.Sleep(20 * time.Millisecond)
	q.close()
	select {
	case err := <-blocked:
		if !errors.Is(err, ErrShuttingDown) {
			t.Fatalf("关闭应唤醒阻塞入队：%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("阻塞入队未被唤醒")
	}
	if err := q.push(ctx, &jobEntry{job: Job{ID: "c"}}, false); !errors.Is(err, ErrShuttingDown) {
		t.Fatalf("关闭后入队应拒绝，实际：%v", err)
	}
	if e, ok := q.pop(); !ok || e.job.ID != "a" {
		t.Fatalf("关闭后应可取出存量：%v %v", e, ok)
	}
	if _, ok := q.pop(); ok {
		t.Fatal("空队列应返回 false")
	}
	if q.remove("a") {
		t.Fatal("不存在的条目移除应返回 false")
	}
}

// TestCancelReleasesInFlight 覆盖取消后释放在途集合。
func TestCancelReleasesInFlight(t *testing.T) {
	d, err := NewDispatcher()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Shutdown(context.Background())
	_ = d.Handle("task", func(context.Context, Job) error { return nil })
	id, err := d.SubmitAfter(context.Background(), "task", nil, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Cancel(id); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Submit(context.Background(), "task", nil); err != nil {
		t.Fatalf("取消后应可再次提交同名任务：%v", err)
	}
}

// TestMetricsAll 覆盖全部指标回调。
func TestMetricsAll(t *testing.T) {
	var queued, running, completed, failed, retried, dropped, skipped, replaced atomic.Int32
	m := Metrics{
		Queued:    func(string, int) { queued.Add(1) },
		Running:   func(string, int) { running.Add(1) },
		Completed: func(string, time.Duration) { completed.Add(1) },
		Failed:    func(string, error) { failed.Add(1) },
		Retried:   func(string, int) { retried.Add(1) },
		Dropped:   func(string) { dropped.Add(1) },
		Skipped:   func(string) { skipped.Add(1) },
		Replaced:  func(string) { replaced.Add(1) },
	}
	// 执行/重试/失败/丢弃。
	d, err := NewDispatcher(WithWorkers(2), WithQueueSize(4),
		WithConflictPolicy(ConflictReplace), WithMetrics(m))
	if err != nil {
		t.Fatal(err)
	}
	_ = d.Handle("ok", func(context.Context, Job) error { return nil })
	var calls atomic.Int32
	_ = d.Handle("fail", func(_ context.Context, job Job) error {
		calls.Add(1)
		if calls.Load() < 2 {
			return errors.New("失败")
		}
		return errors.New("仍失败")
	})
	if _, err := d.Submit(context.Background(), "ok", nil); err != nil {
		t.Fatal(err)
	}
	failID, err := d.Submit(context.Background(), "fail", nil, WithRetry(1, time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		s, _ := d.JobStatus(failID)
		if s == StatusFailed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("失败任务应终态失败：%v", s)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := d.SubmitAfter(context.Background(), "ok", nil, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := d.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if queued.Load() == 0 || running.Load() == 0 || completed.Load() == 0 ||
		failed.Load() == 0 || retried.Load() == 0 || dropped.Load() == 0 {
		t.Fatalf("核心指标缺失：q=%d r=%d c=%d f=%d rt=%d d=%d",
			queued.Load(), running.Load(), completed.Load(), failed.Load(), retried.Load(), dropped.Load())
	}
	// 跳过与替换。
	d2, err := NewDispatcher(WithWorkers(1), WithMetrics(m))
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	started := make(chan struct{})
	_ = d2.Handle("dup", blockHandler(release, started))
	defer d2.Shutdown(context.Background())
	if _, err := d2.Submit(context.Background(), "dup", nil); err != nil {
		t.Fatal(err)
	}
	<-started
	if _, err := d2.Submit(context.Background(), "dup", nil); !errors.Is(err, ErrSkipped) {
		t.Fatalf("应跳过：%v", err)
	}
	close(release)
	d2.Shutdown(context.Background())
	d3, err := NewDispatcher(WithWorkers(1), WithConflictPolicy(ConflictReplace), WithMetrics(m))
	if err != nil {
		t.Fatal(err)
	}
	_ = d3.Handle("r", func(context.Context, Job) error { return nil })
	defer d3.Shutdown(context.Background())
	if _, err := d3.SubmitAfter(context.Background(), "r", nil, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := d3.Submit(context.Background(), "r", nil); err != nil {
		t.Fatal(err)
	}
	d3.Shutdown(context.Background())
	if skipped.Load() == 0 || replaced.Load() == 0 {
		t.Fatalf("冲突指标缺失：s=%d r=%d", skipped.Load(), replaced.Load())
	}
}

// TestDelayLoopTimerCleanup 覆盖 timer 过期后信号/关闭分支的清理路径。
func TestDelayLoopTimerCleanup(t *testing.T) {
	for i := 0; i < 40; i++ {
		d, err := NewDispatcher(WithWorkers(1))
		if err != nil {
			t.Fatal(err)
		}
		_ = d.Handle("task", func(context.Context, Job) error { return nil })
		d.pushDelayed(&jobEntry{job: Job{ID: "near", Name: "task",
			RunAt: time.Now().Add(time.Millisecond)}})
		time.Sleep(3 * time.Millisecond) // 让 timer 先触发。
		_, _ = d.SubmitAfter(context.Background(), "task", nil, time.Hour)
		if err := d.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}
