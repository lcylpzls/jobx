package jobx

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lcylpzls/errx"
)

// TestNewSchedulerErrors 覆盖调度器构造校验。
func TestNewSchedulerErrors(t *testing.T) {
	if _, err := NewScheduler(nil); err == nil || !errx.Is(err, CodeInvalidConfig) {
		t.Fatalf("空执行器应报错，实际：%v", err)
	}
	d, err := NewDispatcher()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Shutdown(context.Background())
	if _, err := NewScheduler(d, WithLocation(nil)); err == nil || !errx.Is(err, CodeInvalidConfig) {
		t.Fatalf("空时区应报错，实际：%v", err)
	}
}

// TestEvery 覆盖周期调度。
func TestEvery(t *testing.T) {
	d, err := NewDispatcher()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Shutdown(context.Background())
	var count atomic.Int32
	_ = d.Handle("tick", func(context.Context, Job) error { count.Add(1); return nil })
	s, err := NewScheduler(d)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(context.Background())
	if _, err := s.Every(100*time.Millisecond, "tick"); err == nil ||
		!errx.Is(err, CodeJobInvalid) {
		t.Fatalf("低于 1 秒周期应报错，实际：%v", err)
	}
	sch, err := s.Every(time.Second, "tick")
	if err != nil {
		t.Fatal(err)
	}
	if sch.ID == "" || sch.Name != "tick" {
		t.Fatalf("条目快照不符：%+v", sch)
	}
	deadline := time.Now().Add(2500 * time.Millisecond)
	for count.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if count.Load() < 2 {
		t.Fatalf("周期调度应至少触发 2 次：%d", count.Load())
	}
}

// TestCron 覆盖表达式调度。
func TestCron(t *testing.T) {
	d, err := NewDispatcher()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Shutdown(context.Background())
	var count atomic.Int32
	_ = d.Handle("every-sec", func(context.Context, Job) error { count.Add(1); return nil })
	s, err := NewScheduler(d)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(context.Background())
	if _, err := s.Cron("bad expr", "every-sec"); err == nil ||
		!errx.Is(err, CodeCronInvalid) {
		t.Fatalf("非法表达式应报错，实际：%v", err)
	}
	if _, err := s.Cron("* * * * * *", "missing"); !errors.Is(err, ErrHandlerNotFound) {
		t.Fatalf("未注册处理器应报错，实际：%v", err)
	}
	if _, err := s.Cron("* * * * * *", "every-sec"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2500 * time.Millisecond)
	for count.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if count.Load() < 2 {
		t.Fatalf("cron 调度应至少触发 2 次：%d", count.Load())
	}
}

// TestCronNoSolution 覆盖无解表达式自动停止。
func TestCronNoSolution(t *testing.T) {
	d, err := NewDispatcher()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Shutdown(context.Background())
	_ = d.Handle("never", func(context.Context, Job) error { return nil })
	s, err := NewScheduler(d, WithSchedulerLogger(testLogger()))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(context.Background())
	sch, err := s.Cron("0 0 30 2 *", "never")
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if len(s.List()) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("无解条目应自动移除")
		}
		time.Sleep(20 * time.Millisecond)
	}
	_ = sch
}

// TestOneShot 覆盖一次性调度与自动失效。
func TestOneShot(t *testing.T) {
	d, err := NewDispatcher()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Shutdown(context.Background())
	got := make(chan struct{}, 1)
	_ = d.Handle("once", func(context.Context, Job) error { got <- struct{}{}; return nil })
	s, err := NewScheduler(d)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(context.Background())
	if _, err := s.OneShot(time.Now().Add(-time.Second), "once"); err == nil ||
		!errx.Is(err, CodeJobInvalid) {
		t.Fatalf("过去时刻应报错，实际：%v", err)
	}
	if _, err := s.OneShot(time.Now().Add(50*time.Millisecond), "once"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("一次性调度未触发")
	}
	deadline := time.Now().Add(time.Second)
	for len(s.List()) > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if len(s.List()) != 0 {
		t.Fatal("触发后条目应自动移除")
	}
}

// TestStop 覆盖条目停止。
func TestStop(t *testing.T) {
	d, err := NewDispatcher()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Shutdown(context.Background())
	var count atomic.Int32
	_ = d.Handle("tick", func(context.Context, Job) error { count.Add(1); return nil })
	s, err := NewScheduler(d)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(context.Background())
	sch, err := s.Every(time.Second, "tick")
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(1500 * time.Millisecond)
	for count.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	sch.Stop()
	after := count.Load()
	time.Sleep(1200 * time.Millisecond)
	if count.Load() != after {
		t.Fatalf("停止后不应再触发：%d -> %d", after, count.Load())
	}
	if err := s.Stop(sch.ID); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("已停止条目应报不存在，实际：%v", err)
	}
}

// TestList 覆盖条目快照。
func TestList(t *testing.T) {
	d, err := NewDispatcher()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Shutdown(context.Background())
	_ = d.Handle("a", func(context.Context, Job) error { return nil })
	_ = d.Handle("b", func(context.Context, Job) error { return nil })
	s, err := NewScheduler(d)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(context.Background())
	if _, err := s.Every(time.Hour, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Every(2*time.Hour, "b"); err != nil {
		t.Fatal(err)
	}
	if got := len(s.List()); got != 2 {
		t.Fatalf("应有 2 个条目：%d", got)
	}
}

// TestShutdown 覆盖调度器关闭。
func TestShutdown(t *testing.T) {
	d, err := NewDispatcher()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Shutdown(context.Background())
	_ = d.Handle("tick", func(context.Context, Job) error { return nil })
	s, err := NewScheduler(d)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Every(time.Hour, "tick"); err != nil {
		t.Fatal(err)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatalf("重复关闭应返回 nil：%v", err)
	}
	if _, err := s.Every(time.Hour, "tick"); !errors.Is(err, ErrSchedulerStopped) {
		t.Fatalf("关闭后注册应拒绝，实际：%v", err)
	}
}

// TestScheduleIDFailure 覆盖调度条目 ID 生成失败。
func TestScheduleIDFailure(t *testing.T) {
	d, err := NewDispatcher()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Shutdown(context.Background())
	_ = d.Handle("tick", func(context.Context, Job) error { return nil })
	s, err := NewScheduler(d)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(context.Background())
	orig := randRead
	randRead = func(b []byte) (int, error) { return 0, errors.New("随机源故障") }
	defer func() { randRead = orig }()
	if _, err := s.Every(time.Hour, "tick"); err == nil || !errx.Is(err, CodeIDGenerateFailed) {
		t.Fatalf("ID 生成失败应报错，实际：%v", err)
	}
}

// TestFireFailure 覆盖调度触发提交失败自动停止。
func TestFireFailure(t *testing.T) {
	d, err := NewDispatcher()
	if err != nil {
		t.Fatal(err)
	}
	_ = d.Handle("tick", func(context.Context, Job) error { return nil })
	s, err := NewScheduler(d, WithSchedulerLogger(testLogger()))
	if err != nil {
		t.Fatal(err)
	}
	sch, err := s.Every(time.Second, "tick")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2500 * time.Millisecond)
	for {
		if len(s.List()) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("触发失败后条目应自动移除")
		}
		time.Sleep(20 * time.Millisecond)
	}
	_ = sch
}

// TestSchedulerShutdownTimeout 覆盖调度器关闭超时。
func TestSchedulerShutdownTimeout(t *testing.T) {
	d, err := NewDispatcher(WithWorkers(1), WithQueueSize(1), WithConflictPolicy(ConflictAllow))
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	started := make(chan struct{})
	_ = d.Handle("block", blockHandler(release, started))
	s, err := NewScheduler(d)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Every(time.Second, "block")
	if err != nil {
		t.Fatal(err)
	}
	// 等待首个任务执行阻塞并让调度触发积压。
	time.Sleep(3500 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := s.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("关闭应超时，实际：%v", err)
	}
	close(release)
}

// TestSimpleSchedulers 覆盖简易调度方法与参数越界。
func TestSimpleSchedulers(t *testing.T) {
	d, err := NewDispatcher()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Shutdown(context.Background())
	_ = d.Handle("task", func(context.Context, Job) error { return nil })
	s, err := NewScheduler(d)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(context.Background())
	for name, fn := range map[string]func() (*Schedule, error){
		"每分钟": func() (*Schedule, error) { return s.EveryMinuteAt(30, "task") },
		"每小时": func() (*Schedule, error) { return s.EveryHourAt(15, "task") },
		"每天":  func() (*Schedule, error) { return s.DailyAt(3, 0, 0, "task") },
		"每周":  func() (*Schedule, error) { return s.WeeklyAt(1, 3, 0, 0, "task") },
	} {
		sch, err := fn()
		if err != nil {
			t.Fatalf("%s 应注册成功：%v", name, err)
		}
		sch.Stop()
	}
	if _, err := s.EveryMinuteAt(60, "task"); err == nil || !errx.Is(err, CodeCronInvalid) {
		t.Fatalf("秒越界应报错，实际：%v", err)
	}
	if _, err := s.EveryHourAt(60, "task"); err == nil || !errx.Is(err, CodeCronInvalid) {
		t.Fatalf("分越界应报错，实际：%v", err)
	}
	if _, err := s.DailyAt(24, 0, 0, "task"); err == nil || !errx.Is(err, CodeCronInvalid) {
		t.Fatalf("时越界应报错，实际：%v", err)
	}
	if _, err := s.WeeklyAt(7, 0, 0, 0, "task"); err == nil || !errx.Is(err, CodeCronInvalid) {
		t.Fatalf("周越界应报错，实际：%v", err)
	}
}

// TestSchedulerEdgeCases 覆盖剩余注册与停止分支。
func TestSchedulerEdgeCases(t *testing.T) {
	d, err := NewDispatcher()
	if err != nil {
		t.Fatal(err)
	}
	_ = d.Handle("task", func(context.Context, Job) error { return nil })
	s, err := NewScheduler(d)
	if err != nil {
		t.Fatal(err)
	}
	// 空名与未注册处理器。
	if _, err := s.Every(time.Hour, ""); err == nil || !errx.Is(err, CodeJobInvalid) {
		t.Fatalf("空名应报错，实际：%v", err)
	}
	if _, err := s.Every(time.Hour, "missing"); !errors.Is(err, ErrHandlerNotFound) {
		t.Fatalf("未注册处理器应报错，实际：%v", err)
	}
	if _, err := s.OneShot(time.Now().Add(time.Hour), "missing"); !errors.Is(err, ErrHandlerNotFound) {
		t.Fatalf("OneShot 未注册处理器应报错，实际：%v", err)
	}
	// OneShot 停止分支。
	one, err := s.OneShot(time.Now().Add(time.Hour), "task")
	if err != nil {
		t.Fatal(err)
	}
	one.Stop()
	// Cron 注册后调度器关闭 → register 拒绝。
	if _, err := s.Cron("* * * * * *", "task"); err != nil {
		t.Fatal(err)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Cron("* * * * * *", "task"); !errors.Is(err, ErrSchedulerStopped) {
		t.Fatalf("关闭后 Cron 应拒绝，实际：%v", err)
	}
	if _, err := s.OneShot(time.Now().Add(time.Hour), "task"); !errors.Is(err, ErrSchedulerStopped) {
		t.Fatalf("关闭后 OneShot 应拒绝，实际：%v", err)
	}
}

// TestSchedulerRandFailure 覆盖各注册入口的 ID 生成失败。
func TestSchedulerRandFailure(t *testing.T) {
	d, err := NewDispatcher()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Shutdown(context.Background())
	_ = d.Handle("task", func(context.Context, Job) error { return nil })
	s, err := NewScheduler(d)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(context.Background())
	orig := randRead
	randRead = func(b []byte) (int, error) { return 0, errors.New("随机源故障") }
	defer func() { randRead = orig }()
	if _, err := s.Cron("* * * * * *", "task"); err == nil || !errx.Is(err, CodeIDGenerateFailed) {
		t.Fatalf("Cron ID 失败应报错，实际：%v", err)
	}
	if _, err := s.OneShot(time.Now().Add(time.Hour), "task"); err == nil ||
		!errx.Is(err, CodeIDGenerateFailed) {
		t.Fatalf("OneShot ID 失败应报错，实际：%v", err)
	}
}

// TestCronFireFailure 覆盖 cron 触发提交失败自动停止（无日志器路径）。
func TestCronFireFailure(t *testing.T) {
	d, err := NewDispatcher()
	if err != nil {
		t.Fatal(err)
	}
	_ = d.Handle("sec", func(context.Context, Job) error { return nil })
	s, err := NewScheduler(d)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Cron("* * * * * *", "sec")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2500 * time.Millisecond)
	for len(s.List()) > 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if len(s.List()) != 0 {
		t.Fatal("cron 触发失败后条目应自动移除")
	}
}

// TestSchedulerLocation 覆盖时区选项成功路径。
func TestSchedulerLocation(t *testing.T) {
	d, err := NewDispatcher()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Shutdown(context.Background())
	_ = d.Handle("task", func(context.Context, Job) error { return nil })
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewScheduler(d, WithLocation(loc))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(context.Background())
	if s.loc != loc {
		t.Fatal("时区应生效")
	}
}
