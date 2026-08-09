// Package main 演示 jobx 高级用法：持久化、重启恢复与冲突策略。
package main

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/lcylpzls/jobx"
	"github.com/lcylpzls/logx"
)

// memoryStore 示例内存持久化实现（生产可基于 dbx/Redis）。
type memoryStore struct {
	mu    sync.Mutex
	items map[string]jobx.Job
}

func newMemoryStore() *memoryStore {
	return &memoryStore{items: make(map[string]jobx.Job)}
}

func (s *memoryStore) Save(_ context.Context, job jobx.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[job.ID] = job
	return nil
}

func (s *memoryStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, id)
	return nil
}

func (s *memoryStore) List(context.Context) ([]jobx.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]jobx.Job, 0, len(s.items))
	for _, job := range s.items {
		out = append(out, job)
	}
	return out, nil
}

// run 演示：提交任务 → 模拟进程重启 → Restore 恢复执行。
func run() error {
	logger, err := logx.NewBuilder().EnableWriter(io.Discard, logx.InfoLevel).Build()
	if err != nil {
		return err
	}
	defer logger.Close()
	ctx := context.Background()
	store := newMemoryStore()

	// 第一段进程：提交一个延迟任务后“崩溃”（不优雅关闭，任务留在存储）。
	d1, err := jobx.NewDispatcher(jobx.WithWorkers(2), jobx.WithStore(store),
		jobx.WithLogger(logger))
	if err != nil {
		return err
	}
	_ = d1.Handle("report", func(_ context.Context, job jobx.Job) error {
		fmt.Printf("执行恢复任务：%s\n", job.ID)
		return nil
	})
	if _, err := d1.SubmitAfter(ctx, "report", []byte(`{"type":"daily"}`),
		10*time.Millisecond); err != nil {
		return err
	}
	// 模拟崩溃：不调用 Shutdown，直接丢弃实例（延迟任务仍在存储中）。

	// 第二段进程：新执行器 + 同一存储，恢复未完成任务。
	d2, err := jobx.NewDispatcher(jobx.WithWorkers(2), jobx.WithStore(store),
		jobx.WithLogger(logger), jobx.WithConflictPolicy(jobx.ConflictAllow))
	if err != nil {
		return err
	}
	defer d2.Shutdown(ctx)
	_ = d2.Handle("report", func(_ context.Context, job jobx.Job) error {
		fmt.Printf("执行恢复任务：%s\n", job.ID)
		return nil
	})
	n, err := d2.Restore(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("恢复任务数：%d\n", n)
	time.Sleep(50 * time.Millisecond)
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Println("示例失败：", err)
	}
}
