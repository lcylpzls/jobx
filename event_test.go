package jobx

import (
	"context"
	"errors"
	"github.com/lcylpzls/testx"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestEventHookAllActions(t *testing.T) {
	hook := &fakeEventHook{}
	d, err := NewDispatcher(
		WithWorkers(2),
		WithQueueSize(4),
		WithEventHook(hook),
	)
	testx.RequireNoError(t, err)
	defer d.Shutdown(context.Background())

	// 直接驱动指标辅助函数，覆盖全部事件类型。
	d.metricQueued("task", 1)
	d.metricRunning("task", 1)
	d.metricCompleted("task", time.Millisecond)
	d.metricFailed("task", errors.New("失败"))
	d.metricRetried("task", 2)
	d.metricDropped("task")
	d.metricSkipped("task")
	d.metricReplaced("task")

	events := hook.snapshot()
	if len(events) != 8 {
		t.Fatalf("期望 8 个事件，得到 %d：%+v", len(events), events)
	}
	var actions []string
	for _, e := range events {
		actions = append(actions, e.Action)
	}
	want := "queued,running,completed,failed,retried,dropped,skipped,replaced"
	if strings.Join(actions, ",") != want {
		t.Fatalf("事件序列不匹配：%v", actions)
	}
	if events[4].Attempt != 2 {
		t.Fatalf("retried 应携带 attempt：%+v", events[4])
	}
	if events[3].Err == nil {
		t.Fatal("failed 应携带错误")
	}
}

func TestEventHookRealRun(t *testing.T) {
	hook := &fakeEventHook{}
	d, err := NewDispatcher(
		WithWorkers(1),
		WithEventHook(hook),
	)
	testx.RequireNoError(t, err)
	defer d.Shutdown(context.Background())

	if err := d.Handle("ok", func(ctx context.Context, job Job) error { return nil }); err != nil {
		t.Fatalf("Handle 失败：%v", err)
	}
	if _, err := d.Submit(context.Background(), "ok", nil); err != nil {
		t.Fatalf("Submit 失败：%v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if hook.has("completed") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !hook.has("completed") {
		t.Fatal("任务完成事件未触发")
	}
}

func TestNoEventHook(t *testing.T) {
	d, err := NewDispatcher()
	testx.RequireNoError(t, err)
	defer d.Shutdown(context.Background())
	d.metricCompleted("task", time.Millisecond)
}

type fakeEventHook struct {
	mu   sync.Mutex
	list []TaskEvent
}

func (h *fakeEventHook) OnTaskEvent(_ context.Context, e TaskEvent) {
	h.mu.Lock()
	h.list = append(h.list, e)
	h.mu.Unlock()
}

func (h *fakeEventHook) snapshot() []TaskEvent {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]TaskEvent(nil), h.list...)
}

func (h *fakeEventHook) has(action string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, e := range h.list {
		if e.Action == action {
			return true
		}
	}
	return false
}
