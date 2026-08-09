// Package main 演示 jobx 的基础用法：异步任务、延迟任务与定时调度。
package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/lcylpzls/jobx"
	"github.com/lcylpzls/logx"
)

// run 组装并演示基础用法（不启动常驻服务）。
func run() error {
	logger, err := logx.NewBuilder().EnableWriter(io.Discard, logx.InfoLevel).Build()
	if err != nil {
		return err
	}
	defer logger.Close()

	d, err := jobx.NewDispatcher(jobx.WithWorkers(4), jobx.WithQueueSize(1024),
		jobx.WithLogger(logger))
	if err != nil {
		return err
	}
	defer d.Shutdown(context.Background())

	_ = d.Handle("greet", func(_ context.Context, job jobx.Job) error {
		fmt.Printf("执行任务 %s：%s\n", job.Name, job.Payload)
		return nil
	})
	_ = d.Handle("greet-later", func(_ context.Context, job jobx.Job) error {
		fmt.Printf("执行任务 %s：%s\n", job.Name, job.Payload)
		return nil
	})

	id, err := d.Submit(context.Background(), "greet", []byte("立即执行"))
	if err != nil {
		return err
	}
	fmt.Printf("已提交立即任务：%s\n", id)

	if _, err := d.SubmitAfter(context.Background(), "greet-later", []byte("延迟执行"),
		50*time.Millisecond); err != nil {
		return err
	}

	sched, err := jobx.NewScheduler(d)
	if err != nil {
		return err
	}
	defer sched.Shutdown(context.Background())
	every, err := sched.Every(time.Second, "greet")
	if err != nil {
		return err
	}
	every.Stop()
	if _, err := sched.DailyAt(3, 0, 0, "greet"); err != nil {
		return err
	}
	fmt.Printf("已注册 %d 条调度\n", len(sched.List()))
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Println("示例失败：", err)
	}
}
