package jobx

import (
	"context"
	"errors"
	testx "github.com/lcylpzls/testx"
	"sync"
	"testing"
	"time"
)

// traceCall 记录一次追踪调用。
type traceCall struct {
	name  string
	attrs map[string]string
	err   error
	ended bool
}

// fakeTraceHook 内存追踪钩子。
type fakeTraceHook struct {
	mu    sync.Mutex
	calls []traceCall
}

func (h *fakeTraceHook) Start(ctx context.Context, name string, attrs ...TraceAttr) (context.Context, func(error)) {
	h.mu.Lock()
	h.calls = append(h.calls, traceCall{name: name, attrs: map[string]string{}})
	for _, a := range attrs {
		h.calls[len(h.calls)-1].attrs[a.Key] = a.Value
	}
	h.mu.Unlock()
	return ctx, func(err error) {
		h.mu.Lock()
		h.calls[len(h.calls)-1].err = err
		h.calls[len(h.calls)-1].ended = true
		h.mu.Unlock()
	}
}

func (h *fakeTraceHook) snapshot() []traceCall {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]traceCall, len(h.calls))
	copy(out, h.calls)
	return out
}

// waitEnded 等待指定数量的结束回调。
func waitEnded(t *testing.T, h *fakeTraceHook, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ended := 0
		for _, c := range h.snapshot() {
			if c.ended {
				ended++
			}
		}
		if ended >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("等待追踪结束回调超时：%+v", h.snapshot())
}

// TestTraceHook 覆盖任务执行追踪埋点。
func TestTraceHook(t *testing.T) {
	hook := &fakeTraceHook{}
	d, err := NewDispatcher(WithWorkers(1), WithTraceHook(hook))
	testx.RequireNoError(t, err)

	defer d.Shutdown(context.Background())

	got := make(chan Job, 1)
	_ = d.Handle("task", func(_ context.Context, job Job) error {
		got <- job
		return nil
	})
	_ = d.Handle("fail", func(context.Context, Job) error { return errors.New("业务失败") })
	if _, err := d.Submit(context.Background(), "task", nil); err != nil {
		t.Fatal(err)
	}
	<-got
	if _, err := d.Submit(context.Background(), "fail", nil); err != nil {
		t.Fatal(err)
	}
	waitEnded(t, hook, 2)

	calls := hook.snapshot()
	if len(calls) != 2 {
		t.Fatalf("应调用 2 次追踪钩子，实际：%d", len(calls))
	}
	for _, c := range calls {
		if c.name != "jobx.execute" || c.attrs["jobx.job_name"] == "" || c.attrs["jobx.job_id"] == "" {
			t.Fatalf("追踪调用不符：%+v", c)
		}
	}
	if calls[1].err == nil {
		t.Fatal("失败任务应记录错误")
	}
}
