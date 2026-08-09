# jobx 版本路线

> 目标：v0.1.0 起每版完成即全自动 CI + Release，全部通过后进入下一版；
> 定版级质量贯穿全程（100% 覆盖、race、fuzz、三平台 CI、govulncheck）。

## v0.1.0 — 执行器核心（已发布）

- Dispatcher：就绪队列 + worker 池 + 提交（立即）；
- Handler 注册与路由；任务超时；处理器 panic 恢复；
- 配置校验、队列满策略（Block/QueueFullDrop）；
- 优雅关闭（排空 + 等待 + 超时取消）；
- errx 错误码骨架（按能力分版启用）、logx 结构化日志；
- 交付：`Submit`、`Handle`、`Shutdown`、选项、错误值。

## v0.2.0 — 延迟与生命周期（已发布）

- 延迟队列（最小堆 + 单 timer）：`SubmitAt` / `SubmitAfter`；
- 失败重试（指数退避 + 重试次数上限）；
- 同名任务冲突策略（`WithConflictPolicy`）：
  - `ConflictSkip`（默认）：在途时跳过新任务并返回 `ErrSkipped`；
  - `ConflictReplace`：取消旧任务（尽力取消）并执行新任务；
  - `ConflictAllow`：允许同名并发；
- 任务状态查询与取消（`JobStatus` / `Cancel`）；
- Metrics 注入（Queued/Running/Completed/Failed/Retried/Dropped/Skipped/Replaced）。

## v0.3.0 — 调度器与 cron（已发布）

- `jobx/cron` 解析器（5/6 字段，`Next`/`NextN`，fuzz 覆盖）；
- Scheduler：`Every` / `Cron` / `OneShot`；
- 简易调度包装：`EveryMinuteAt` / `EveryHourAt` / `DailyAt` / `WeeklyAt`
  （内部等价构造 cron 表达式，参数越界返回 `ErrCronInvalid`）；
- 调度条目管理：`Stop` / `List` / `Shutdown`；
- 时区注入（`WithLocation`）。

## v0.4.0 — 可观测与性能打磨（已发布）

- 日志事件补齐（提交/开始/完成/失败/重试/panic/关闭）；
- 基准测试：提交、消费、延迟精度、重试；
- 性能优化（热路径零分配审查）；
- TTL/边界测试矩阵（延迟到期瞬间、重试边界、关闭竞态、
  冲突策略切换与替换竞态）。

## v0.5.0 — 持久化接口与适配（已发布）

- 可选 Store 接口（排队/延迟任务落库与恢复），默认内存实现；
- 提供基于 dbx 的适配示例（文档级，不引第三方）；
- 关闭时延迟任务丢弃策略文档化与 Metrics 计数。

## v0.6.0 — 发布前终审

- 依赖整理、govulncheck、静态检查全量；
- 并发/泄漏/死锁/阻塞终审（连续多轮测试）；
- README / ERRORS.md / 示例定稿；
- 收口于 v0.6.0（v1.0.0 之前允许破坏性变更，按需继续迭代）。

## 质量门禁（每版）

```powershell
go test -count=1 ./...                    # 全量
go test -count=1 -coverprofile=coverage.out ./...  # 核心包 100%
go test -race -count=1 ./...              # race
go vet ./... && staticcheck ./...          # 静态检查
go test -run '^$' -fuzz '^FuzzCron$' -fuzztime=10s ./cron/   # fuzz
govulncheck ./...                         # 依赖漏洞
```

CI：ubuntu/windows/macos 三平台 + fuzz job + govulncheck job +
Release（tag 触发）。
