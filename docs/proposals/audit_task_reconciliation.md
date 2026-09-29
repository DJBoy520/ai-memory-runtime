# 任务书：AMR 多 Agent 记忆一致性对账（daily_memory_reconciliation）架构审计

- **委托方**: Hermes (方案架构组)
- **受托方**: OpenClaw (独立代码与安全审计专家)
- **待审文档**: `/home/dj/WorkSpaces/ai-memory-runtime/docs/proposals/RFC-003_daily_memory_reconciliation_spec.md`
- **审计模式**: 严格只读（禁止修改任何业务代码与配置，只输出专业审计报告）

---

## 一、 审计背景与核心定位

AI Memory Runtime (AMR) 目前为 Hermes、OpenClaw、OpenCode、DSH 提供跨 Agent 的长期记忆共享中枢。当前存在各 Agent 日志级交互、系统中断、超时报错产生噪音，以及 Qdrant 检索索引与本地 SQLite 存在漂移的隐患。

本方案旨在设计并落地每日凌晨 02:30 的一致性校准任务 `daily_memory_reconciliation`。
其核心定位为：**“根据 SQLite SSOT 校准/重建 Qdrant 检索投影，SQLite 是唯一事实来源，Qdrant 仅为只读检索投影”**。

---

## 二、 重点审计要点与核查项

请 OpenClaw 依据架构设计规范与工业级生产标准，对该 RFC-003 提案执行全面、深度的技术审查：

### 1. 七条一致性不变量（Invariants 01~07）完备性
- 检查 Invariants 01~07 是否存在逻辑自相矛盾或无法落实的技术盲区。
- 特别核验：当 Qdrant 发生完全损坏丢失时，从 SQLite 重建检索索引的语义是否严密（如模型版本升级、距离度量兼容性等）。

### 2. Policy Gate 状态机设计安全性
- 检查 `DETECTED → PROPOSED → POLICY_CHECK → APPROVED → APPLIED` 流转路径。
- 确认规则引擎（Rule Engine）是否被彻底剥离了写正式库的权限，是否能杜绝误伤正常记忆。
- 检查 `curation_candidates` 隔离表的设计是否有效防止 SSOT 污染。

### 3. SQLite DDL 与性能评估
- 审查 `memories` 增量字段（`curation_status`, `content_hash`, `curation_batch_id`, `is_tombstone`）和索引设计。
- 评估在单表几十万甚至数百万条数据规模下，凌晨对账是否有引发 SQLite 锁表（`database is locked`）导致在线 Agent UDS 请求超时的风险，以及对 WAL Checkpoint / Online Backup 的并发影响。

### 4. 复合去重键与生命周期状态矩阵
- 审查 `tenant + agent + project + role + normalized_content_hash` 去重策略。
- 审查 `ACTIVE`、`STALE`、`ARCHIVED`、`DELETED` 四阶状态矩阵在 SQLite 与 Qdrant 之间的一致性定义，确认对“历史失败经验”的保护是否周全。

### 5. Canary 测试门禁与恢复演练（Restore Drill）
- 审查正向召回、负向拒识（已删除记忆排斥、旧状态抑制）及 P95 延迟的门禁阈值设计。
- 评估定期“灾备恢复演练（Restore Drill）”在实际运维环境中的可落地性与风险。

---

## 三、 输出要求
请直接给出明确的审计结论：
1. **综合评级**：【通过】/【有条件通过】/【打回修订】；
2. **逐项审计意见与潜在隐患识别**（包括任何边缘场景 Edge Case）；
3. **改进与落地加固建议**。
