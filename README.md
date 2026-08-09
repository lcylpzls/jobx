# jobx

自研单进程任务执行与调度库：异步任务、延迟任务、定时任务，
与 errx / logx 生态打通。

> 当前状态：**v0.10.0 已发布**。竞品对比与压测报告完成
> （见 [docs/benchmark.md](docs/benchmark.md)）。

## 定位

jobx **不是消息队列**，不解决跨进程消息传递；它解决单进程内每个业务
都要重复的部分：

- 异步任务：提交后进入队列，worker 池并发消费；
- 延迟任务：指定时间或延时后执行；
- 定时任务：周期调度、一次性调度、cron 表达式；
- 简易调度：每小时某分、每分钟某秒、每天/每周定点（cron 包装）；
- 生命周期：单任务超时、失败重试、同名冲突策略
  （跳过/替换/允许并发）、优雅关闭；
- 可观测性：logx 结构化日志、外部注入 Metrics；
- 错误语义：统一 errx 错误码。

所有组件并发安全，可在多个 goroutine 间共享。

## 目录

```
jobx/
├── CHANGELOG.md          # 变更记录
├── docs/
│   ├── README.md          # 文档索引
│   ├── design.md          # 设计定版（范围/架构/并发模型/错误码）
│   ├── api.md             # API 定版（完整签名与语义）
│   ├── research.md        # 领域调研与设计取舍
│   └── roadmap.md         # 版本路线
└── README.md
```

## 快速上手（规划草案）

```go
import (
	"context"
	"time"

	"github.com/lcylpzls/jobx"
)

dispatcher, err := jobx.NewDispatcher(jobx.WithWorkers(8), jobx.WithQueueSize(1024))
defer dispatcher.Shutdown(context.Background())

_ = dispatcher.Handle("send_mail", func(ctx context.Context, job jobx.Job) error {
	return nil
})

_, err = dispatcher.Submit(context.Background(), "send_mail", []byte(`{"to":"a@b.c"}`))

scheduler, err := jobx.NewScheduler(dispatcher)
_ = scheduler.Every(5*time.Minute, "cleanup")
_ = scheduler.Cron("0 3 * * *", "daily_report")
_ = scheduler.EveryHourAt(15, "hourly_sync")
_ = scheduler.DailyAt(3, 0, 0, "daily_report")
```

`logx.Logger` 通过 `jobx.WithLogger` 注入；完整 API 见
[docs/api.md](docs/api.md)。

完整错误码清单见 [ERRORS.md](ERRORS.md)。

## 持久化（可选）

```go
// 实现 jobx.Store 接口（Save/Delete/List），如基于 dbx：
store := &myDBStore{}

d, _ := jobx.NewDispatcher(jobx.WithStore(store))

// 进程重启后恢复未完成任务。
n, err := d.Restore(ctx)
```

提交与延迟任务先落库再入队，终态/取消同步删除；
未启用存储时行为与旧版本完全一致。

## 性能基准（本机参考）

以下为 Windows 12 核 / Go 1.26 实测数据（`go test -bench .`），
非跨平台承诺：

| 场景 | 耗时/op | 分配 |
| --- | --- | --- |
| 立即任务提交 | ~1.6µs | 12 allocs |
| 延迟任务提交 | ~1.0µs | 8 allocs |
| 冲突跳过路径 | ~472ns | 4 allocs |
| 10ms 延迟调度偏差 | ~34µs | — |

提交热路径无锁竞争（原子状态 + 队列），延迟任务入堆 O(log n)。

> 当前快速上手为**目标形态**：`NewDispatcher` / `Handle` / `Submit` /
> `SubmitAt` / `SubmitAfter` / `JobStatus` / `Cancel` / `Shutdown`
> 与 Scheduler（Every/Cron/OneShot/简易方法）均已落地。

## License

MIT © [lcylpzls](https://github.com/lcylpzls)
