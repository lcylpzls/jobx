package core

import (
	"context"
	testx "github.com/lcylpzls/testx"
	"sync"
	"testing"
	"time"
)

// TestConcurrentSubmitCancel 并发提交/取消/状态查询压力测试（配合 race）。
func TestConcurrentSubmitCancel(t *testing.T) {
	d, err := NewDispatcher(WithWorkers(4), WithQueueSize(64),
		WithConflictPolicy(ConflictAllow))
	testx.RequireNoError(t, err)

	defer d.Shutdown(context.Background())
	_ = d.Handle("task", func(ctx context.Context, _ Job) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
			return nil
		}
	})
	ctx := context.Background()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				id, err := d.Submit(ctx, "task", nil, WithTimeout(200*time.Millisecond))
				if err != nil {
					continue
				}
				if i%2 == 0 {
					_ = d.Cancel(id)
				} else {
					_, _ = d.JobStatus(id)
				}
			}
		}()
	}
	wg.Wait()
}
