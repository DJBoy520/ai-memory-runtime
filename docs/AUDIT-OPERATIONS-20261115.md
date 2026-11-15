# AMR 运行与 Unraid 迁移审计（2026-11-15）

范围：当前 Ubuntu VM 上的运行日志、配置、源码及 SQLite/Qdrant 只读统计。目标拓扑为客户端继续位于 Ubuntu VM，AMR 与 Tesla P4 未来迁到 Unraid Docker。本次未修改生产数据、配置或部署；统计是不同时间取得的快照，并非跨库原子快照。

## 结论与优先级

### P0：真实使用链路与数据隔离

1. **OpenClaw hook 被阻止**：11 月 14 日 22:34 的 gateway 日志显示 `before_prompt_build` 与 `agent_end` 均被阻止，并提示配置 `plugins.entries.openclaw-amr.hooks.allowConversationAccess=true`。当前插件已启用但该配置缺失，故不能以插件初始化成功证明自动检索注入与会话归档工作。启用会授予插件会话访问，需要核实权限与重载后的真实行为。
2. **测试数据曾进入生产库**：`plugins/openclaw-amr/tests/test_agent_end_reliability.js` 的部分测试使用模拟 socket 但未隔离持久化目录；插件默认使用用户目录。真实 spool 中存在测试样例，11 月 12 日网关日志记录重放成功，生产会话表和摄入日志存在同名条目。不能据此认定由哪次测试创建，也不要直接删除。先隔离测试目录及网络，再确定污染范围、备份和清理策略。
3. **提炼闭环没有得到验证**：过去七天蒸馏日志 730 次带结果统计的调用均为新增候选 0，其中 638 次扫描会话数为 0；11 月 13 日曾有 `ModelResponseParseError` 和 `IntegrityError`。最新一次有记录的会话摄入为 11 月 13 日 12:42；近期仍有多智能体记忆写入和成功投影。需要使用隔离的可控会话检查摄入、候选、审核与召回，不应把成功退出等同有效产出。
4. **延迟入库可能被遗漏**：`src/cognitive/scheduler.py:67-101` 默认按会话或消息事件时间扫描近 24 小时，而非入库时间；延迟补传且事件时间均在窗口外的会话可能永远不被常规扫描选中。历史消息存在超过 24 小时才入库的情况，但不能据此认定全部漏处理。应测试并设计基于入库水位或可靠待处理队列的扫描。
5. **OpenClaw 故障重放并非默认自动**：`plugins/openclaw-amr/index.js:76-103` 只有显式配置目录、环境变量或开关才默认启用启动重放；当前配置不满足，代码未见周期重放。当前队列没有待处理项，但未来失败需要恢复机制。
6. **Codex 来源及离线队列**：当前 Codex MCP 配置直接调用 Python bridge、未设置 `AMR_SPOOL_DIR`，并将来源设为 `claude_code`。`src/interfaces/mcp/bridge.py:79-94,265-269` 显示来源被强制写入，且未设置目录不启用队列。需要单独验证真实 Codex 来源、白名单和重连行为。

### P1：对账和可观测性不可信

7. **583 项不等于 583 项待删除**：11 月 15 日对账 dry-run 报告 583 项异常。后续快照核对为 522 个 SQLite 与 Qdrant 均为 HISTORICAL 的点、61 个 SQLite 无对应记录且 Qdrant 标记 DELETED 的旧点。`src/reconciliation/engine.py:284-288` 将历史点视作异常，而 `src/service/outbox_worker.py:121` 只物理删除 DELETED，`src/service/memory_service.py:279-282` 支持历史检索，口径冲突。61 项须结合历史、引用和备份审查，不自动删除。
8. **对账门禁是假阳性**：`src/reconciliation/engine.py:315-325` 直接将 `correctness_gate_passed` 设为 true；所谓活跃点数量是未按状态过滤的总数。Phase 4 只报告差异并更新时间，不自动修复。实际 systemd reconciliation 带 `--dry-run`，不创建备份也不修正。
9. **体检指标映射错误**：`scripts/amr_doctor.py:239-246` 从顶层读取计数，而 `src/service/metrics.py:56-62` 返回 `counters/gauges/summary`，worker 使用 `outbox_pending_processed` 等名称；未提供值被显示为零。请求/错误总数未见对应采集。不能从体检零值推断没有流量。
10. **检索异常可能伪装为空结果**：`src/service/memory_service.py:411-417` 捕获单集合搜索异常后返回空列表，调用方难以区分无结果和后端故障；需考虑部分成功、结构化错误及指标。检索质量和近期调用量目前没有足够证据。
11. **宿主 I/O 压力**：检查期间曾有高 I/O pressure、AMR 进程等待页 I/O，管理体检超时；压力下降后同一体检恢复 healthy。根因未确认，不能归咎 AMR 或断言损坏。需排查宿主/虚拟化存储、内核日志及监控，复现时关联延迟。

### P1：Unraid 迁移阻碍

12. **跨 VM 接入**：当前业务及管理接口是 Unix socket，Ubuntu 客户端无法直接访问 Unraid 容器的 socket。可评估 Ubuntu 本地 socket 经 SSH Unix socket 转发到 Unraid 业务 socket；或设计有认证、加密、限流的远程网关。两者均未实测。管理接口必须独立保护，不能直接裸露网络。
13. **身份边界**：业务 `agent_id` 是本机请求字段白名单，不是远程身份认证；拥有 socket 访问权的进程可冒充允许的身份。管理 socket 未见独立应用层认证。远程方案须有独立的身份、授权、连接限制及审计。
14. **切换单写者**：`src/service/outbox_worker.py:69-77` 使用本地运行目录或 `/tmp` 下的 flock；独立容器/主机默认不共享锁。切换必须确保旧写者停止、数据一致备份、验证和回滚，不能依赖各自的本地锁实现跨主机互斥。
15. **GPU 配置缺陷与验证**：`src/core/engine.py:325-332` 接受 `cuda:0`，但 `:442-447` 仅对 `cuda` 将模型移到 GPU，`cuda:0` 会走 CPU。CUDA 不可用会降级 CPU，READY 不证明 P4 可用。当前 Ubuntu P4 可见；Unraid 上的驱动、容器 GPU 映射和 PyTorch 构建尚未验证。
16. **部署组件缺失**：仓库没有 Dockerfile/Compose；现有蒸馏和对账靠 systemd 定时。迁移需明确 SQLite/WAL 一致备份及本地持久存储、模型、配置、Qdrant、客户端 spool、独立定时任务、日志、健康检查和恢复演练。依赖未锁定，目标 CUDA/P4 组合必须实测。
17. **明文 Qdrant 连接**：当前启动日志提示 API key 经不安全连接发送。迁移跨宿主后应评估 TLS 或可信隔离网络与密钥管理。

## 建议执行顺序

1. 保护生产数据：修正测试隔离，盘点测试污染，验证备份与恢复；不自动清理旧点位。
2. 恢复和验证真实客户端链路：OpenClaw 会话权限、Codex 身份和 spool、离线重放、延迟会话扫描；以隔离数据做端到端验证。
3. 修复对账分类和真实门禁、体检指标及检索错误传播；验证历史记忆不会被误清理。
4. 设计 Ubuntu 到 Unraid 的连接与安全边界，再准备容器、定时任务、GPU 和可回滚的迁移演练。迁移前确认客户端断连、重试、单写者和数据恢复。

## 验证范围和限制

本机 Python 测试结果 439 passed、5 skipped；该结果不证明生产链路或测试隔离有效。AMR doctor 复测 healthy，仅证明当时探针成功；实际使用和数据质量见上述证据。Gemini/DeepSeek 经 Claude 路由的检查尝试未返回有效结果，未将其当作审计证据。未在 Unraid 上执行部署或 GPU 测试。
