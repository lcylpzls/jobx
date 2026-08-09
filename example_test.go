package jobx_test

import (
	"context"
	"fmt"
	"time"

	"github.com/lcylpzls/jobx"
)

// ExampleDispatcher 演示任务提交、执行与关闭。
func ExampleDispatcher() {
	d, err := jobx.NewDispatcher(jobx.WithWorkers(2))
	if err != nil {
		panic(err)
	}
	defer d.Shutdown(context.Background())
	_ = d.Handle("greet", func(_ context.Context, job jobx.Job) error {
		fmt.Printf("收到：%s\n", job.Payload)
		return nil
	})
	id, err := d.Submit(context.Background(), "greet", []byte("你好"))
	if err != nil {
		panic(err)
	}
	// 等待任务完成（演示用途；生产建议通过回调/指标观察）。
	for {
		s, _ := d.JobStatus(id)
		if s == jobx.StatusSucceeded {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Output: 收到：你好
}

// ExampleScheduler 演示定时调度注册。
func ExampleScheduler() {
	d, err := jobx.NewDispatcher(jobx.WithWorkers(1))
	if err != nil {
		panic(err)
	}
	defer d.Shutdown(context.Background())
	_ = d.Handle("task", func(context.Context, jobx.Job) error { return nil })
	s, err := jobx.NewScheduler(d)
	if err != nil {
		panic(err)
	}
	defer s.Shutdown(context.Background())
	sch, err := s.Cron("0 0 3 * * *", "task")
	if err != nil {
		panic(err)
	}
	fmt.Printf("已注册：%s\n", sch.Name)
	sch.Stop()
	// Output: 已注册：task
}
