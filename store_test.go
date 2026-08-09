package jobx

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lcylpzls/errx"
)

// stubStore 内存测试存储。
type stubStore struct {
	mu        sync.Mutex
	items     map[string]Job
	saveErr   error
	listErr   error
	deleteErr error
}

func newStubStore() *stubStore {
	return &stubStore{items: make(map[string]Job)}
}

func (s *stubStore) Save(_ context.Context, job Job) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[job.ID] = job
	return nil
}

func (s *stubStore) Delete(_ context.Context, id string) error {
	if s.deleteErr != nil {
		return s.deleteErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, id)
	return nil
}

func (s *stubStore) List(context.Context) ([]Job, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Job, 0, len(s.items))
	for _, job := range s.items {
		out = append(out, job)
	}
	return out, nil
}

func (s *stubStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.items)
}

func (s *stubStore) has(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.items[id]
	return ok
}

// TestStoreOptionInvalid 覆盖空存储选项。
func TestStoreOptionInvalid(t *testing.T) {
	if _, err := NewDispatcher(WithStore(nil)); err == nil || !errx.Is(err, CodeInvalidConfig) {
		t.Fatalf("空存储应报错，实际：%v", err)
	}
}

// TestStoreLifecycle 覆盖存储全生命周期。
func TestStoreLifecycle(t *testing.T) {
	store := newStubStore()
	d, err := NewDispatcher(WithStore(store), WithConflictPolicy(ConflictAllow))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Shutdown(context.Background())
	done := make(chan struct{}, 2)
	_ = d.Handle("task", func(_ context.Context, job Job) error {
		done <- struct{}{}
		return nil
	})
	ctx := context.Background()
	// 立即任务：保存 → 执行 → 删除。
	id, err := d.Submit(ctx, "task", nil)
	if err != nil {
		t.Fatal(err)
	}
	<-done
	deadline := time.Now().Add(2 * time.Second)
	for store.count() > 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if store.count() != 0 {
		t.Fatalf("完成后应删除持久化条目：%d", store.count())
	}
	// 延迟任务：保存 → 关闭丢弃 → 删除。
	_, err = d.SubmitAfter(ctx, "task", nil, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if store.count() != 1 {
		t.Fatalf("延迟任务应已保存：%d", store.count())
	}
	// 取消 → 删除。
	cancelID, err := d.SubmitAfter(ctx, "task", nil, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Cancel(cancelID); err != nil {
		t.Fatal(err)
	}
	if store.has(cancelID) {
		t.Fatal("取消任务应删除持久化条目")
	}
	_ = d.Shutdown(ctx)
	if store.count() != 0 {
		t.Fatalf("关闭丢弃应删除持久化条目：%d", store.count())
	}
	_ = id
}

// TestStoreSaveFailure 覆盖保存失败回滚。
func TestStoreSaveFailure(t *testing.T) {
	store := newStubStore()
	store.saveErr = errors.New("存储故障")
	d, err := NewDispatcher(WithStore(store))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Shutdown(context.Background())
	executed := make(chan struct{}, 1)
	_ = d.Handle("task", func(context.Context, Job) error {
		executed <- struct{}{}
		return nil
	})
	ctx := context.Background()
	if _, err := d.Submit(ctx, "task", nil); err == nil || !errx.Is(err, CodeStoreInvalid) {
		t.Fatalf("保存失败应报错，实际：%v", err)
	}
	if _, err := d.SubmitAfter(ctx, "task", nil, time.Hour); err == nil ||
		!errx.Is(err, CodeStoreInvalid) {
		t.Fatalf("延迟保存失败应报错，实际：%v", err)
	}
	select {
	case <-executed:
		t.Fatal("保存失败的任务不应执行")
	default:
	}
	// 修复存储后同名任务可再次提交（inFlight 已回滚）。
	store.saveErr = nil
	if _, err := d.Submit(ctx, "task", nil); err != nil {
		t.Fatalf("修复后应可提交：%v", err)
	}
	select {
	case <-executed:
	case <-time.After(2 * time.Second):
		t.Fatal("任务未执行")
	}
}

// TestRestore 覆盖任务恢复。
func TestRestore(t *testing.T) {
	store := newStubStore()
	now := time.Now()
	store.items["immediate"] = Job{ID: "immediate", Name: "task",
		CreatedAt: now, RunAt: now.Add(-time.Minute)}
	store.items["delayed"] = Job{ID: "delayed", Name: "task",
		CreatedAt: now, RunAt: now.Add(time.Hour)}
	store.items["orphan"] = Job{ID: "orphan", Name: "missing",
		CreatedAt: now, RunAt: now.Add(-time.Minute)}
	d, err := NewDispatcher(WithStore(store))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Shutdown(context.Background())
	executed := make(chan struct{}, 1)
	_ = d.Handle("task", func(context.Context, Job) error {
		executed <- struct{}{}
		return nil
	})
	n, err := d.Restore(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("应恢复 2 个任务：%d", n)
	}
	if store.has("orphan") {
		t.Fatalf("缺失处理器的任务应被删除：%d", store.count())
	}
	select {
	case <-executed:
	case <-time.After(2 * time.Second):
		t.Fatal("恢复的立即任务未执行")
	}
	if s, _ := d.JobStatus("delayed"); s != StatusDelayed {
		t.Fatalf("恢复的延迟任务应处于延迟：%v", s)
	}
}

// TestRestoreNoStore 覆盖未启用存储时的恢复。
func TestRestoreNoStore(t *testing.T) {
	d, err := NewDispatcher()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Shutdown(context.Background())
	n, err := d.Restore(context.Background())
	if err != nil || n != 0 {
		t.Fatalf("未启用存储应返回 0：n=%d err=%v", n, err)
	}
}

// TestStoreErrors 覆盖列表与删除失败路径。
func TestStoreErrors(t *testing.T) {
	store := newStubStore()
	store.listErr = errors.New("列表故障")
	d, err := NewDispatcher(WithStore(store))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Shutdown(context.Background())
	if _, err := d.Restore(context.Background()); err == nil || !errx.Is(err, CodeStoreInvalid) {
		t.Fatalf("列表失败应报错，实际：%v", err)
	}
	// 删除失败不影响执行。
	store.listErr = nil
	store.deleteErr = errors.New("删除故障")
	done := make(chan struct{}, 1)
	_ = d.Handle("task", func(context.Context, Job) error {
		done <- struct{}{}
		return nil
	})
	if _, err := d.Submit(context.Background(), "task", nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("删除失败不应阻塞执行")
	}
}

// TestRestoreRetryPersist 覆盖重试时存储更新。
func TestRestoreRetryPersist(t *testing.T) {
	store := newStubStore()
	d, err := NewDispatcher(WithStore(store))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Shutdown(context.Background())
	var calls int
	_ = d.Handle("flaky", func(_ context.Context, job Job) error {
		calls++
		if calls < 2 {
			return errors.New("失败")
		}
		return nil
	})
	id, err := d.Submit(context.Background(), "flaky", nil, WithRetry(1, 5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		s, _ := d.JobStatus(id)
		if s == StatusSucceeded {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("重试后应成功")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if store.count() != 0 {
		t.Fatalf("重试任务终态应删除：%d", store.count())
	}
}

// TestStoreQueueFullDrop 覆盖队列满丢弃时的存储回滚。
func TestStoreQueueFullDrop(t *testing.T) {
	store := newStubStore()
	d, err := NewDispatcher(WithWorkers(1), WithQueueSize(1),
		WithQueueFullPolicy(QueueFullDrop), WithConflictPolicy(ConflictAllow),
		WithStore(store), WithLogger(testLogger()))
	if err != nil {
		t.Fatal(err)
	}
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
		t.Fatalf("队列满应丢弃，实际：%v", err)
	}
	if store.count() != 2 {
		t.Fatalf("丢弃任务不应残留存储：%d", store.count())
	}
	close(release)
}

// TestRestoreCancelledCtx 覆盖恢复时 ctx 取消。
func TestRestoreCancelledCtx(t *testing.T) {
	store := newStubStore()
	now := time.Now()
	store.items["immediate"] = Job{ID: "immediate", Name: "task",
		CreatedAt: now, RunAt: now.Add(-time.Minute)}
	d, err := NewDispatcher(WithWorkers(1), WithQueueSize(1),
		WithStore(store), WithConflictPolicy(ConflictAllow))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Shutdown(context.Background())
	release := make(chan struct{})
	started := make(chan struct{})
	_ = d.Handle("task", blockHandler(release, started))
	// 占满队列：首个任务执行中，第二个任务占满队列。
	if _, err := d.Submit(context.Background(), "task", nil); err != nil {
		t.Fatal(err)
	}
	<-started
	if _, err := d.Submit(context.Background(), "task", nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := d.Restore(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("ctx 取消应中断恢复，实际：%v", err)
	}
	close(release)
}

// TestRestoreAllow 覆盖 Allow 策略下的恢复。
func TestRestoreAllow(t *testing.T) {
	store := newStubStore()
	now := time.Now()
	store.items["immediate"] = Job{ID: "immediate", Name: "task",
		CreatedAt: now, RunAt: now.Add(-time.Minute)}
	d, err := NewDispatcher(WithStore(store), WithConflictPolicy(ConflictAllow),
		WithLogger(testLogger()))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Shutdown(context.Background())
	done := make(chan struct{}, 1)
	_ = d.Handle("task", func(context.Context, Job) error {
		done <- struct{}{}
		return nil
	})
	// 含缺失处理器的任务以覆盖 logStore 日志路径。
	store.items["orphan"] = Job{ID: "orphan", Name: "missing",
		CreatedAt: now, RunAt: now.Add(-time.Minute)}
	n, err := d.Restore(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("应恢复 1 个任务：%d", n)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("恢复任务未执行")
	}
}
