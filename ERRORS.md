# 错误码清单

所有错误统一使用 errx 语义（`errx.Is(err, Code)` 判断）。

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
| `jobx_replaced` | 同名旧任务已被替换取消 | conflict | 409 |
| `jobx_cron_invalid` | cron 表达式非法 | invalid_argument | 400 |
| `jobx_scheduler_stopped` | 调度器已停止 | unavailable | 503 |
| `jobx_store_invalid` | 任务存储读写失败 | unavailable | 503 |

> `jobx_timeout` / `jobx_retry_exhausted` / `jobx_execution_failed`
> 为任务执行结果语义（日志/Metrics/Handler 返回），不通过 `Submit` 返回。
