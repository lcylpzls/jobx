# Changelog

本项目遵循语义化版本（SemVer）。v1.0.0 之前允许破坏性变更。

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
