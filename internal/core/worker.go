package core

import (
	"context"
	"fmt"
	"strconv"

	"github.com/lcylpzls/errx"
	"github.com/lcylpzls/logx"
)

func (d *Dispatcher) worker() {
	defer d.wg.Done()
	for {
		entry, ok := d.ready.pop()
		if !ok {
			return
		}
		d.runEntry(entry)
	}
}

// runEntry 执行单个任务：取消标记检查、超时 context、重试、日志与指标。
func (d *Dispatcher) runEntry(entry *jobEntry) {
	job := entry.job
	d.metricQueued(job.Name, -1)
	if d.isCancelled(job.ID) {
		d.markStatus(job.ID, StatusCancelled)
		d.releaseJob(job.Name, job.ID)
		return
	}
	d.markStatus(job.ID, StatusRunning)
	d.metricRunning(job.Name, 1)
	ctx, cancel := context.WithCancel(context.Background())
	if job.Timeout > 0 {
		var timeoutCtx context.Context
		timeoutCtx, cancel = context.WithTimeout(ctx, job.Timeout)
		ctx = timeoutCtx
	}
	d.executing.Store(job.ID, cancel)
	defer func() {
		d.executing.Delete(job.ID)
		cancel()
		d.metricRunning(job.Name, -1)
		d.releaseJob(job.Name, job.ID)
		if d.cfg.store != nil {
			_ = d.cfg.store.Delete(context.Background(), job.ID)
		}
	}()
	h, ok := d.handlers.Load(job.Name)
	if !ok {
		d.markStatus(job.ID, StatusFailed)
		d.metricFailed(job.Name, errx.WrapCode(ErrHandlerNotFound, CodeHandlerNotFound, "处理器缺失"))
		d.logError(job, ErrHandlerNotFound)
		return
	}
	start := d.cfg.now()
	d.logStart(job)
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = errx.WrapCode(fmt.Errorf("处理器 panic：%v", r),
					CodeExecutionFailed, "处理器执行失败")
				if d.cfg.logger != nil {
					d.cfg.logger.Error("jobx：处理器 panic", logx.Fields(
						logx.String(fieldJobID, job.ID),
						logx.String(fieldJobName, job.Name),
						logx.Any("panic", r),
					))
				}
			}
		}()
		spanCtx, end := d.traceStart(ctx, job)
		err = h.(Handler)(spanCtx, job)
		end(err)
	}()
	if d.isCancelled(job.ID) {
		d.markStatus(job.ID, StatusCancelled)
		d.logError(job, errx.NewCode(CodeJobCancelled, "任务已取消"))
		return
	}
	if err != nil {
		if job.MaxRetries > 0 && job.Attempt < job.MaxRetries {
			d.scheduleRetry(entry, err)
			return
		}
		d.markStatus(job.ID, StatusFailed)
		d.metricFailed(job.Name, err)
		d.logError(job, err)
		return
	}
	d.markStatus(job.ID, StatusSucceeded)
	d.metricCompleted(job.Name, d.cfg.now().Sub(start))
	d.logCompleted(job, d.cfg.now().Sub(start))
}

// traceStart 开始任务执行链路（无钩子时 no-op）。
func (d *Dispatcher) traceStart(ctx context.Context, job Job) (context.Context, func(error)) {
	if d.cfg.traceHook == nil {
		return ctx, func(error) {}
	}
	return d.cfg.traceHook.Start(ctx, "jobx.execute",
		TraceAttr{Key: "jobx.job_name", Value: job.Name},
		TraceAttr{Key: "jobx.job_id", Value: job.ID},
		TraceAttr{Key: "jobx.attempt", Value: strconv.Itoa(job.Attempt)},
	)
}

// scheduleRetry 安排重试：按指数退避进入延迟堆。
func (d *Dispatcher) scheduleRetry(entry *jobEntry, cause error) {
	job := entry.job
	delay := job.RetryDelay << job.Attempt
	next := *entry
	next.job.Attempt++
	next.job.RunAt = d.cfg.now().Add(delay)
	if d.cfg.store != nil {
		_ = d.cfg.store.Save(context.Background(), next.job)
	}
	d.pushDelayed(&next)
	d.markStatus(next.job.ID, StatusDelayed)
	d.metricRetried(job.Name, next.job.Attempt)
	d.logRetry(job, next.job.RunAt, cause)
}

// logStore 记录存储相关日志。
func (d *Dispatcher) logStore(msg string, job Job) {
	if d.cfg.logger == nil {
		return
	}
	d.cfg.logger.Warn(msg, logx.Fields(
		logx.String(fieldJobID, job.ID),
		logx.String(fieldJobName, job.Name),
	))
}

// waitWorkers 等待 worker 与延迟调度退出，ctx 超时则取消执行中任务。
func (d *Dispatcher) waitWorkers(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		d.executing.Range(func(_, v any) bool {
			if cancel, ok := v.(context.CancelFunc); ok {
				cancel()
			}
			return true
		})
		return ctx.Err()
	}
}

// cancelByID 取消任务（假定调用方已确认存在）。
