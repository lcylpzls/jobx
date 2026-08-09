# jobx

自研单进程任务执行与调度库：异步任务、延迟任务、定时任务，
与 errx / logx 生态打通。

> 当前状态：**v0.1.0 已发布**。执行器核心可用，延迟/调度等能力按
> [docs/roadmap.md](docs/roadmap.md) 持续迭代。

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

> 当前快速上手为**目标形态**：Scheduler 与简易调度方法将在
> v0.3.0 落地，v0.1.0 已可用能力为 `NewDispatcher` / `Handle` /
> `Submit` / `Shutdown` 及选项与错误值。

## License

MIT © [lcylpzls](https://github.com/lcylpzls)
