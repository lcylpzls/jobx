# Changelog

本项目遵循语义化版本（SemVer）。v1.0.0 之前允许破坏性变更。

## [v0.3.0] - 2026-08-09

### 新增

- `jobx/cron` 自研 cron 解析器：
  - 5/6 字段（秒 分 时 日 月 周），支持 `*`、`*/n`、`a-b`、`a,b,c`、`?`；
  - `Next`（严格晚于，5 年扫描上限）与 `NextN`，时区保留；
  - 日与周字段取并集，非法表达式返回 `ErrCronInvalid`（含字段定位）；
  - fuzz 目标 `FuzzCron`；
- Scheduler 调度器：
  - `Every` / `Cron` / `OneShot`（触发后自动失效）；
  - 简易调度包装：`EveryMinuteAt` / `EveryHourAt` / `DailyAt` /
    `WeeklyAt`（内部等价 cron 表达式，参数越界报 `ErrCronInvalid`）；
  - 条目管理：`List` / `Stop` / `Shutdown`（幂等、超时支持）；
  - `WithLocation` 时区注入、`WithSchedulerLogger` 日志注入；
  - 无解表达式、触发提交失败自动移除条目并记录日志。

## [v0.2.0] - 2026-08-09

### 新增

- 延迟队列（最小堆 + 单 timer）：
  - `SubmitAt` / `SubmitAfter` / `WithRunAt`；
  - 到期自动移入就绪队列，关闭时延迟任务丢弃并计数；
- 失败重试：`WithRetry`（指数退避，`RetryDelay × 2^Attempt`）；
- 同名任务冲突策略：`WithConflictPolicy`——
  `ConflictSkip`（默认，返回 `ErrSkipped`）/
  `ConflictReplace`（尽力取消旧任务并执行新任务）/
  `ConflictAllow`（允许并发）；
- 任务状态与取消：`JobStatus`（六态 + 容量上限）、`Cancel`
  （延迟/排队物理移除，执行中 ctx 协作）；
- Metrics 全量注入：Queued/Running/Completed/Failed/Retried/
  Dropped/Skipped/Replaced。

### 架构改进

- 就绪队列由 channel 改为自实现队列：支持按 ID 物理移除被取消的
  排队任务，修复 ConflictReplace 在队列满时被占位条目阻塞的问题；
- 修复 Cancel 后未释放在途集合导致同名任务被误拦截的问题；
- timer 清理提取为幂等函数，行为等价且可测试。

## [v0.1.0] - 2026-08-09

### 新增

- Dispatcher 执行器核心：
  - 就绪队列（容量可配）+ 固定 worker 池；
  - `Handle` 处理器注册（重名冲突、空名/超长名校验）；
  - `Submit` 立即任务提交（载荷上限、深拷贝、crypto/rand 任务 ID）；
  - 单任务超时（`WithTimeout`）与处理器 panic 恢复；
  - 队列满策略：`QueueFullBlock`（默认，阻塞不丢任务）与
    `QueueFullDrop`（丢弃并返回 `ErrQueueFull`）；
  - 优雅关闭：拒绝新提交、排空存量、ctx 超时取消执行中任务、幂等；
- errx 错误码骨架（`jobx_*` 全集，按能力分版启用）；
- logx 结构化日志（提交/开始/完成/失败/panic 事件）；
- fuzz 目标 `FuzzSubmit`；CI：三平台 + race + fuzz + govulncheck + Release。

### 设计定稿

- 规划文档（design/api/research/roadmap）同步 v0.1.0 范围；
- 处理器执行失败错误码 `jobx_execution_failed` 补充进文档。
