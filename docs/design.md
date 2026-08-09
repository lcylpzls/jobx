# jobx 设计定版

> 版本：v0.0.0（规划定稿） · 状态：文档已定版，代码未开始

## 1. 定位

jobx 是**单进程内**的任务执行与调度库，解决自用项目中每个业务都要重复的
三件事：

1. **异步执行**：提交任务后立即返回，后台 worker 池并发消费；
2. **延迟执行**：指定时刻或延时后执行；
3. **定时执行**：周期、一次性、cron 表达式调度。

配套提供任务生命周期能力：单任务超时、失败重试、同名任务冲突策略
（跳过/替换/允许并发）、
优雅关闭，以及 logx 结构化日志与外部注入 Metrics。

## 2. 范围边界（明确不做）

以下内容**不属于** jobx，文档阶段即定界，避免实现期蔓延：

| 不做 | 原因与替代 |
| --- | --- |
| 跨进程分布式队列 | 需要 Redis Stream / Kafka / NSQ 时，由业务引入中间件适配器；jobx 提供稳定接口便于适配 |
| 任务持久化 | 默认内存实现，进程重启丢失排队中任务；v0.5 起提供可选持久化存储接口 |
| 任务编排 / DAG 依赖 | 自用规模过度设计；业务可在 Handler 内编排 |
| 分布式锁 / 分布式限流 | 属于基础设施，由业务或后续库解决 |
| 与 webx 中间件耦合 | webx 服务内嵌 jobx 由业务在启动流程中组装 |
| 抢占式调度 / 任务迁移 | 单进程模型无此需求 |

## 3. 术语

| 术语 | 含义 |
| --- | --- |
| Job | 任务单元：ID、名称、载荷、执行元数据 |
| Handler | 任务处理器，按任务名称路由 |
| Dispatcher | 执行器：队列 + worker 池 + 延迟队列 + 重试 + 冲突策略 + 关闭 |
| Scheduler | 调度器：周期/一次性/cron 触发，触发后提交给 Dispatcher |
| Schedule | 调度条目：由 Scheduler 管理，可停止 |

## 4. 总体架构

```
业务代码
  │ Submit / SubmitAt / SubmitAfter
  ▼
Dispatcher
  ├─ 就绪队列   readyQueue（chan，容量可配）
  ├─ 延迟堆     min-heap + timer（到期移入就绪队列）
  ├─ worker 池  N 个 goroutine 消费就绪队列
  │    ├─ 单任务超时 context
  │    ├─ 失败重试（指数退避，进入延迟堆）
  │    └─ 同名防重入（运行中状态表）
  └─ Shutdown   停止接收 → 排空延迟堆 → 等待 worker / 超时取消

Scheduler
  ├─ Every(interval)      周期调度
  ├─ Cron(expr)           表达式调度（自研解析器）
  ├─ OneShot(at)          一次性调度
  └─ Stop(id) / List()    条目管理
```

数据流：`Submit → 就绪队列 → worker 执行`；
`延迟任务 → 延迟堆 → 到期 → 就绪队列 → worker 执行`；
`Scheduler 触发 → 提交（等价 Submit）`。

## 5. 数据模型

### 5.1 Job

```go
type Job struct {
	ID         string        // 全局唯一（crypto/rand 十六进制）
	Name       string        // 处理器路由名
	Payload    []byte        // 载荷（业务自行编码，如 JSON）
	CreatedAt  time.Time     // 创建时间
	RunAt      time.Time     // 计划执行时间（零值=立即）
	MaxRetries int           // 失败重试次数上限（0=不重试）
	RetryDelay time.Duration // 首次重试延迟（后续指数 ×2）
	Timeout    time.Duration // 单次执行超时（0=无超时）
	Attempt    int           // 已尝试次数（内部维护，对外只读）
}
```

约束：

- `Name` 非空且长度 ≤ 128；
- `Payload` 长度 ≤ 1 MiB（默认上限，可配置）；
- `MaxRetries` 0-100；
- `RetryDelay` ≥ 0；
- `Timeout` ≥ 0（0 表示不限制，建议业务始终设置）。

### 5.2 Handler

```go
type Handler func(ctx context.Context, job Job) error
```

- 处理器内 panic 由框架 recover，记录日志并按失败处理（触发重试）；
- 返回错误触发重试策略；ctx 取消（超时/关闭）时不应继续工作。

## 6. Dispatcher 并发模型

### 6.1 状态机

Dispatcher 生命周期：`new → running → shutting_down → stopped`。

- `running`：可提交、可执行；
- `shutting_down`：`Shutdown` 已调用，拒绝新提交，继续消费存量；
- `stopped`：全部 worker 退出。

状态切换由 `sync.RWMutex` 保护，提交路径用原子读判断关闭状态
（热路径无锁竞争）。

### 6.2 就绪队列

- 实现：`chan *jobEntry`，容量 `WithQueueSize`（默认 1024，必须为正）；
- 提交策略 `WithQueueFullPolicy`：
  - `Block`（默认）：队列满时阻塞提交，保证不丢任务；
  - `QueueFullDrop`：队列满时丢弃新任务并返回 `ErrQueueFull`（不阻塞业务）。

### 6.3 延迟队列

- 实现：`container/heap` 最小堆（按 `RunAt`）+ 单一调度 goroutine；
- 调度 goroutine 持有堆顶 timer：到期弹出并移入就绪队列；
- 新延迟任务入堆后通过信号通道唤醒调度 goroutine 重置 timer；
- 关闭时排空堆：延迟任务全部丢弃并计入 Metrics（`Dropped`）。

### 6.4 worker 池

- 数量 `WithWorkers`（默认 4，必须为正），启动时固定；
- 每个 worker 循环消费就绪队列；无任务时阻塞在 channel；
- 单任务执行：
  1. 构造超时 context（`WithTimeout`）；
  2. 调用 Handler；
  3. 成功 → 完成回调、在途计数 -1；
  4. 失败 → 按重试策略入延迟堆（在途计数不变）或最终失败（在途计数 -1）。
- handler panic：recover 后按失败处理，worker 不退出。

### 6.5 同名任务冲突策略

提交新任务时，若同名任务仍**在途**（排队/延迟/执行中），按
`WithConflictPolicy` 配置处理：

- `ConflictSkip`（默认）：跳过本次提交，返回 `ErrSkipped`；
  新任务不入队，旧任务不受影响；Metrics `Skipped` 计数；
- `ConflictReplace`：取消全部同名在途任务并执行新任务；
  排队/延迟中的旧任务直接移除，执行中的旧任务通过任务 context
  协作退出（尽力取消，不保证严格串行）；旧任务终态为
  `StatusCancelled`；Metrics `Replaced` 计数；
- `ConflictAllow`：允许同名任务并发执行（在途集合 +1）。

实现：在途集合（`sync.Map`：name → 任务 ID 集合），
**排队、延迟、执行中均计入**；提交时按策略处理，
终态（成功/最终失败/取消）从集合移除自身 ID，重试中间态不增减。

`ConflictReplace` 的取消语义：执行中任务依赖 Handler 监听
`ctx.Done()`；不协作的 Handler 只能等其自然结束，新任务正常
入队执行，可能出现短暂并发（文档明确“尽力取消”）。

### 6.6 重试

- 条件：`err != nil && Attempt < MaxRetries`（`MaxRetries` 表示最多
  重试次数，0 表示不重试；总执行次数 = `MaxRetries + 1`）；
- 延迟：`RetryDelay × 2^(Attempt-1)`，不引入随机抖动（可复现、可测试）；
- 重试任务进入延迟堆，`Attempt` 递增；
- 重试耗尽 → 最终失败回调 + 结构化错误日志（含 `attempt`）。

### 6.7 优雅关闭

```go
func (d *Dispatcher) Shutdown(ctx context.Context) error
```

- 幂等；重复调用返回首次调用的结果；
- 流程：标记 shutting_down → 拒绝新提交（`ErrShuttingDown`）→
  排空延迟堆 → 关闭就绪队列 → 等待 worker 消费完存量 →
  ctx 超时则取消执行中的任务 context 并强制退出；
- 返回 nil（正常完成）或 ctx 错误（超时强制退出）。

## 7. Scheduler 并发模型

- `NewScheduler(dispatcher)`：复用 Dispatcher，不持有独立队列；
- 每个 Schedule 一个 goroutine：
  - `Every`：`time.Ticker`；
  - `OneShot`：`time.Timer`，触发后条目自动失效；
  - `Cron`：每次触发后计算下一次时间（自研解析器，见 §8）；
- 触发动作 = `Submit`（默认策略 Block），提交失败（如关闭）记日志并停止条目；
- `Stop(id)`：取消条目 goroutine（context cancel + close done chan）；
- `Shutdown(ctx)`：停止全部条目并等待退出；Dispatcher 由调用方负责关闭。

### 7.1 简易调度方法

对高频 cron 场景提供包装，内部等价构造 cron 表达式，
语义（时区、下次触发计算）与 `Cron` 完全一致：

- `EveryMinuteAt(second int, name string)`：每分钟第 `second` 秒；
  等价 6 字段 `second * * * * *`；
- `EveryHourAt(minute int, name string)`：每小时第 `minute` 分 0 秒；
  等价 `0 minute * * * *`；
- `DailyAt(hour, minute, second int, name string)`：每天
  `hour:minute:second`；等价 `second minute hour * * *`；
- `WeeklyAt(weekday, hour, minute, second int, name string)`：
  每周 `weekday`（0=周日）的 `hour:minute:second`；等价
  `second minute hour * * weekday`。

参数越界返回 `ErrCronInvalid`（秒 0-59、分 0-59、时 0-23、周 0-6）。
这些方法避免业务手写易错表达式，同时保留 `Cron` 作为完整能力出口。

## 8. Cron 表达式（自研解析器）

- 支持 5 字段：`分 时 日 月 周`，及 6 字段（前导秒）；
- 字段语法：数字、`*`、`*/n`、`a-b`、`a,b,c`、`?`（等价 `*`）；
- 合法范围：秒 0-59、分 0-59、时 0-23、日 1-31、月 1-12、周 0-6（0=周日）；
- 语义：基于本地时区（或注入时区）计算下一个触发时刻；
- 错误：`ErrCronInvalid`（字段数、范围、语法错误，含字段定位信息）；
- 不做：`L`、`W`、`#`、年字段、`@daily` 别名（可后续扩展，接口预留）。

解析器放 `jobx/cron` 子包，独立可测试、可 fuzz。

## 9. 错误码（errx）

| 错误码 | 含义 | Kind | 建议 HTTP |
| --- | --- | --- | --- |
| `jobx_invalid_config` | 配置非法 | invalid_argument | 400 |
| `jobx_handler_not_found` | 未注册处理器 | not_found | 404 |
| `jobx_handler_conflict` | 处理器重复注册 | already_exists | 409 |
| `jobx_job_invalid` | 任务参数非法 | invalid_argument | 400 |
| `jobx_job_not_found` | 任务/调度条目不存在 | not_found | 404 |
| `jobx_queue_full` | 就绪队列已满（丢弃策略） | rate_limited | 429 |
| `jobx_shutting_down` | 关闭中拒绝新任务 | unavailable | 503 |
| `jobx_timeout` | 单任务执行超时 | timeout | 504 |
| `jobx_retry_exhausted` | 重试耗尽 | internal | 500 |
| `jobx_execution_failed` | 处理器执行失败（含 panic） | internal | 500 |
| `jobx_skipped` | 同名任务在途，本次提交被跳过 | already_exists | 409 |
| `jobx_replaced` | 同名任务在途，旧任务被替换取消 | conflict | 409 |
| `jobx_cron_invalid` | cron 表达式非法 | invalid_argument | 400 |
| `jobx_scheduler_stopped` | 调度器已停止 | unavailable | 503 |

预定义错误值（`ErrQueueFull` 等）支持 `errors.Is`；`errx.Is` 支持按码判断。

## 10. 可观测性

### 10.1 日志（logx）

结构化字段统一前缀 `jobx_`，事件：

| 事件 | 字段 |
| --- | --- |
| 任务提交 | `jobx_id`、`jobx_name`、`jobx_run_at` |
| 任务开始 | `jobx_id`、`jobx_name`、`jobx_attempt` |
| 任务完成 | `jobx_id`、`jobx_name`、`jobx_duration` |
| 任务失败 | `jobx_id`、`jobx_name`、`jobx_attempt`、`error` |
| 重试调度 | `jobx_id`、`jobx_name`、`jobx_attempt`、`jobx_retry_at` |
| 处理器 panic | `jobx_id`、`jobx_name`、`panic` |
| 任务跳过 | `jobx_id`、`jobx_name`（ConflictSkip 触发） |
| 任务替换 | `jobx_name`、`jobx_old_id`、`jobx_new_id`（ConflictReplace 触发） |
| 关闭完成 | `jobx_remaining` |

### 10.2 Metrics（外部注入）

```go
type Metrics struct {
	Queued    func(name string, delta int)      // 入队/出队
	Running   func(name string, delta int)      // 执行中增减
	Completed func(name string, duration time.Duration)
	Failed    func(name string, err error)
	Retried   func(name string, attempt int)
	Dropped   func(name string)                 // 关闭/策略丢弃
	Skipped   func(name string)                 // ConflictSkip 跳过
	Replaced  func(name string)                 // ConflictReplace 替换旧任务
}
```

全部回调可选（nil 跳过），不引入第三方指标依赖；
Prometheus 适配由业务或后续 metricsx 完成。

## 11. 安全与健壮性

- 处理器 panic 恢复，worker 永不因业务 panic 退出；
- 所有配置项有上限校验（队列、worker、重试、载荷长度）；
- 任务 ID 使用 `crypto/rand`，失败返回错误，不回退弱随机；
- 提交热路径避免锁竞争（原子状态 + channel）；
- 时钟可注入（测试与边界验证）；
- 关闭流程不泄漏 goroutine（所有 goroutine 有明确退出路径）。

## 12. 性能目标

- 就绪任务提交/消费为无锁热路径（channel 语义），worker 数内吞吐线性；
- 延迟任务入堆 O(log n)，堆顶 timer 单 goroutine；
- 基准测试：提交吞吐、消费吞吐、延迟任务调度精度、并发重试；
- 参考基线（本机 benchmark 数据将在 v0.1.0 记录）：
  - 提交（空载荷）：≥ 1M ops/s；
  - worker 消费：≥ 500K ops/s（8 worker，空 Handler）。

> 以上为**目标基线**，v0.1.0 以本机实测校准后写入 README。

## 13. 测试与质量门禁

沿用各底座统一标准：

- 核心包语句覆盖率 100%（含 cron 子包）；
- `-race` 全绿、连续多轮无偶发竞态；
- fuzz：cron 解析器、Job 元数据构造；
- 三平台 CI（ubuntu/windows/macos）+ govulncheck + Release 自动发布；
- 基准测试纳入仓库（`bench_test.go`）；
- 文档、错误码、示例在每版本同步更新。

## 14. 依赖

```go
require (
	github.com/lcylpzls/errx v1.2.0
	github.com/lcylpzls/logx v1.0.0
)
```

除自身生态外 **0 第三方依赖**。

## 15. 开放问题（定稿结论）

| 问题 | 结论 |
| --- | --- |
| 队列满默认策略 | 默认 Block（不丢任务），提供 QueueFullDrop 选项 |
| 同名任务冲突默认策略 | 默认 ConflictSkip（不报错打扰调用方，日志与 Metrics 可感知） |
| ConflictReplace 的取消保证 | 尽力取消：执行中任务依赖 Handler 协作 ctx.Done() |
| 关闭时延迟任务 | 丢弃并计数，不做持久化补偿 |
| cron 时区 | 默认本地时区，支持 `WithLocation` 注入 |
| 持久化接口 | v0.5.0 引入可选 Store 接口，默认内存，不阻塞 v0.1-0.4 |
