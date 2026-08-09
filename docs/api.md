# jobx API 定版

> 版本：v0.0.0（规划定稿） · 以下签名在实现阶段按本文执行；
> v0.1.0 前允许微调，v0.1.0 起冻结核心公开面。

## 1. 包结构

```go
jobx            // 根包：Job/Handler/Dispatcher/Scheduler/选项/错误
jobx/cron       // cron 表达式解析与下次触发计算
```

## 2. 核心类型

### 2.1 Job

```go
type Job struct {
	ID         string
	Name       string
	Payload    []byte
	CreatedAt  time.Time
	RunAt      time.Time
	MaxRetries int
	RetryDelay time.Duration
	Timeout    time.Duration
	Attempt    int
}
```

约束见 [design.md §5.1](design.md)。`Attempt` 由框架维护，业务只读。

### 2.2 Handler

```go
type Handler func(ctx context.Context, job Job) error
```

- 返回 nil 视为成功；
- 返回错误触发重试（若 `Attempt < MaxRetries`，语义见
  [design.md §6.6](design.md)）；
- panic 视为失败（框架 recover）。

## 3. Dispatcher

### 3.1 构造

```go
func NewDispatcher(opts ...Option) (*Dispatcher, error)
```

默认配置：

| 项 | 默认值 |
| --- | --- |
| Workers | 4 |
| QueueSize | 1024 |
| QueueFullPolicy | Block |
| ConflictPolicy | ConflictSkip（同名在途时跳过新任务） |
| MaxPayloadBytes | 1 MiB |
| Logger | 无（不记录） |
| Clock | time.Now |

### 3.2 选项

```go
func WithWorkers(n int) Option            // 必须为正
func WithQueueSize(n int) Option          // 必须为正
func WithQueueFullPolicy(p QueueFullPolicy) Option
func WithConflictPolicy(p ConflictPolicy) Option
func WithMaxPayloadBytes(n int) Option    // 必须为正
func WithLogger(logger logx.Logger) Option
func WithMetrics(m Metrics) Option
func WithClock(now func() time.Time) Option
```

`QueueFullPolicy`：

```go
type QueueFullPolicy uint8

const (
	QueueFullBlock QueueFullPolicy = iota // 默认：阻塞提交
	QueueFullDrop                        // 丢弃并返回 ErrQueueFull
)
```

非法选项返回 `ErrInvalidConfig`（构造时校验，不 panic）。

`ConflictPolicy`（同名任务在途时的处理策略）：

```go
type ConflictPolicy uint8

const (
	ConflictSkip    ConflictPolicy = iota // 默认：跳过新任务，返回 ErrSkipped
	ConflictReplace                       // 取消旧任务并执行新任务
	ConflictAllow                         // 允许同名并发执行
)
```

语义细节见 [design.md §6.5](design.md)。

`Metrics`：

```go
type Metrics struct {
	Queued    func(name string, delta int)      // 入队 +1 / 出队 -1
	Running   func(name string, delta int)      // 执行中 +1 / -1
	Completed func(name string, duration time.Duration)
	Failed    func(name string, err error)
	Retried   func(name string, attempt int)
	Dropped   func(name string)                 // 关闭/策略丢弃
	Skipped   func(name string)                 // ConflictSkip 跳过
	Replaced  func(name string)                 // ConflictReplace 替换旧任务
}
```

全部回调可选（nil 跳过）；`WithMetrics` 可随时注入覆盖。

### 3.3 处理器注册

```go
func (d *Dispatcher) Handle(name string, h Handler) error
```

- `name` 非空且 ≤128，重复注册返回 `ErrHandlerConflict`；
- 空名/超长返回 `ErrJobInvalid`；
- 允许运行中注册，仅拒绝重复名。

### 3.4 提交

```go
func (d *Dispatcher) Submit(ctx context.Context, name string, payload []byte) (string, error)
func (d *Dispatcher) SubmitAt(ctx context.Context, name string, payload []byte, at time.Time) (string, error)
func (d *Dispatcher) SubmitAfter(ctx context.Context, name string, payload []byte, delay time.Duration) (string, error)
func (d *Dispatcher) SubmitWithOptions(ctx context.Context, name string, payload []byte, opts ...SubmitOption) (string, error)
```

`SubmitOption`：

```go
func WithRunAt(at time.Time) SubmitOption
func WithRetry(maxRetries int, delay time.Duration) SubmitOption // v0.2.0 生效
func WithTimeout(timeout time.Duration) SubmitOption
```

语义：

- 返回任务 ID（32 位十六进制随机串）与错误；
- 空 `name` 返回 `ErrJobInvalid`；未注册 Handler 返回 `ErrHandlerNotFound`；
- `ctx` 取消时提交中断（返回 ctx 错误），已入队不受影响；
- 队列满且策略为 Drop 时返回 `ErrQueueFull`；
- 关闭中返回 `ErrShuttingDown`；
- 同名任务在途时按 `ConflictPolicy` 处理：
  - `ConflictSkip`：返回 `("", ErrSkipped)`（新任务不入队）；
  - `ConflictReplace`：取消同名旧任务后正常入队，返回新任务 ID；
  - `ConflictAllow`：正常入队（允许并发）。

### 3.5 查询与取消

```go
func (d *Dispatcher) JobStatus(id string) (Status, error)
func (d *Dispatcher) Cancel(id string) error
```

`Status`：

```go
type Status uint8

const (
	StatusQueued Status = iota
	StatusDelayed
	StatusRunning
	StatusSucceeded
	StatusFailed
	StatusCancelled
)
```

> 版本标注：v0.1.0 **不提供** `JobStatus`/`Cancel` 公开 API；
> v0.2.0 提供 `Status` 查询与“排队中/延迟中”取消；
> 执行中任务取消通过任务 context 协作实现。

### 3.6 关闭

```go
func (d *Dispatcher) Shutdown(ctx context.Context) error
```

语义见 [design.md §6.7](design.md)。幂等。

## 4. Scheduler

### 4.1 构造与选项

```go
func NewScheduler(dispatcher *Dispatcher, opts ...SchedulerOption) (*Scheduler, error)

func WithLocation(loc *time.Location) SchedulerOption
func WithSchedulerLogger(logger logx.Logger) SchedulerOption
```

`dispatcher` 必须非 nil；`loc` 默认为 `time.Local`。

### 4.2 调度注册

```go
func (s *Scheduler) Every(interval time.Duration, name string) (*Schedule, error)
func (s *Scheduler) Cron(expr string, name string) (*Schedule, error)
func (s *Scheduler) OneShot(at time.Time, name string) (*Schedule, error)
func (s *Scheduler) EveryMinuteAt(second int, name string) (*Schedule, error)
func (s *Scheduler) EveryHourAt(minute int, name string) (*Schedule, error)
func (s *Scheduler) DailyAt(hour, minute, second int, name string) (*Schedule, error)
func (s *Scheduler) WeeklyAt(weekday, hour, minute, second int, name string) (*Schedule, error)
```

`Schedule`：

```go
type Schedule struct {
	ID   string
	Name string
	Next time.Time
}

func (s *Schedule) Stop()
```

语义：

- `Every`：interval 必须 ≥1s（防止高频自锁）；首次触发在第一个周期后；
- `Cron`：表达式非法返回 `ErrCronInvalid`；触发后自动计算下一次；
- 简易方法（`EveryMinuteAt`/`EveryHourAt`/`DailyAt`/`WeeklyAt`）：
  内部等价构造 cron 表达式，参数越界返回 `ErrCronInvalid`
  （秒 0-59、分 0-59、时 0-23、周 0-6）；
- `OneShot`：`at` 必须晚于当前时刻（否则返回 `ErrJobInvalid`）；
  触发后条目自动失效（`Stop` 幂等）；
- 触发提交使用默认 Block 策略；关闭中触发失败则条目自动停止并记日志。
- `Shutdown` 后注册新调度返回 `ErrSchedulerStopped`。

### 4.3 条目管理

```go
func (s *Scheduler) List() []Schedule
func (s *Scheduler) Stop(id string) error
func (s *Scheduler) Shutdown(ctx context.Context) error
```

- `Stop`：停止并移除条目；不存在返回 `ErrJobNotFound`；
- `Shutdown`：停止全部条目并等待 goroutine 退出；不关闭 Dispatcher。

## 5. cron 子包

```go
package cron

type Expr struct{ /* 内部字段 */ }

func Parse(expr string) (*Expr, error)                    // 5/6 字段
func (e *Expr) Next(after time.Time) (time.Time, error)   // 严格晚于 after
func (e *Expr) NextN(after time.Time, n int) ([]time.Time, error)
func (e *Expr) String() string                            // 规范化输出
```

- `Parse` 错误均为 `errx` 语义 `jobx_cron_invalid`，消息含字段定位；
- `Next` 在最坏情况扫描上限（如 5 年）内寻找；无解返回错误；
- `NextN`：n 必须为正。

## 6. 错误值清单

根包导出：

```go
var (
	ErrInvalidConfig    = errx.New(errx.KindInvalid, CodeInvalidConfig, "配置非法")
	ErrHandlerNotFound  = errx.New(errx.KindNotFound, CodeHandlerNotFound, "处理器未注册")
	ErrHandlerConflict  = errx.New(errx.KindAlreadyExists, CodeHandlerConflict, "处理器重复注册")
	ErrJobInvalid       = errx.New(errx.KindInvalid, CodeJobInvalid, "任务参数非法")
	ErrJobNotFound      = errx.New(errx.KindNotFound, CodeJobNotFound, "任务或调度条目不存在")
	ErrQueueFull        = errx.New(errx.KindRateLimited, CodeQueueFull, "任务队列已满")
	ErrShuttingDown     = errx.New(errx.KindUnavailable, CodeShuttingDown, "执行器关闭中")
	ErrTimeout          = errx.New(errx.KindTimeout, CodeTimeout, "任务执行超时")
	ErrRetryExhausted   = errx.New(errx.KindInternal, CodeRetryExhausted, "任务重试耗尽")
	ErrExecutionFailed  = errx.New(errx.KindInternal, CodeExecutionFailed, "处理器执行失败")
	ErrSkipped          = errx.New(errx.KindAlreadyExists, CodeSkipped, "同名任务在途，本次提交被跳过")
	ErrReplaced         = errx.New(errx.KindConflict, CodeReplaced, "同名旧任务已被替换取消")
	ErrCronInvalid      = errx.New(errx.KindInvalid, CodeCronInvalid, "cron 表达式非法")
	ErrSchedulerStopped = errx.New(errx.KindUnavailable, CodeSchedulerStopped, "调度器已停止")
	ErrStoreInvalid     = errx.New(errx.KindUnavailable, CodeStoreInvalid, "任务存储读写失败")
)
```

对应 `CodeXxx` 常量（`jobx_*` 前缀），完整对照见
[design.md §9](design.md)。

> `ErrTimeout`/`ErrRetryExhausted` 为**任务执行结果语义**（供
> Handler 返回与日志/Metrics 使用），不通过 `Submit` 返回；
> `ErrReplaced` 为内部标记（日志/Metrics），不通过 `Submit` 返回。

## 7. 完整示例（规划）

```go
ctx := context.Background()

d, _ := jobx.NewDispatcher(
	jobx.WithWorkers(8),
	jobx.WithQueueSize(2048),
	jobx.WithLogger(logger),
	jobx.WithMetrics(metrics),
)
defer d.Shutdown(ctx)

_ = d.Handle("order_timeout", func(ctx context.Context, job jobx.Job) error {
	return nil
})

// 立即异步。
_, _ = d.Submit(ctx, "order_timeout", []byte(`{"order_id":"o-1"}`))

// 5 分钟后，失败重试 3 次，单次超时 30s。
_, _ = d.SubmitAfter(ctx, "order_timeout", []byte(`{"order_id":"o-2"}`), 5*time.Minute,
	jobx.WithRetry(3, time.Second), jobx.WithTimeout(30*time.Second))

sched, _ := jobx.NewScheduler(d)
_, _ = sched.Every(10*time.Minute, "cleanup")
_, _ = sched.Cron("0 3 * * *", "daily_report")
_, _ = sched.EveryHourAt(15, "hourly_sync")   // 每小时第 15 分
_, _ = sched.DailyAt(3, 0, 0, "daily_report") // 每天 03:00:00
```
