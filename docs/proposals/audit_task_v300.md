# 审计任务书：AMR 记忆操作系统 (MOS v3.0 / RFC-005) 终审

- **委托方**: Hermes (方案架构组)
- **受托方**: OpenClaw (独立代码与安全审计专家)
- **待审文档**: `/home/dj/WorkSpaces/ai-memory-runtime/docs/proposals/RFC-005_memory_operating_system_spec.md`
- **审计模式**: 严格只读（禁止修改业务代码，只输出独立审计结论）

---

## 一、 审计背景与本次升版核心要点

根据老板指示与多方专家终审意见，方案已从“外挂式记忆插件”正式升维为 **“AMR 记忆操作系统 (Memory Operating System, MOS v3.0)”**。

本次规范（RFC-005）全面收敛并彻底闭环了此前所有 P0/P1 争议：
1. **主客体秩序彻底纠偏**：
   - 确立“AMR 主导调度，LLM 负责认知计算”原则。
   - 彻底废除“让 LLM 自主搜索、自主整理”的危险机制，转由 AMR `Cognitive Scheduler` 确定性组装 `Memory Context Pack` 喂给 LLM。
   - LLM 只有提交 `Memory Proposals` 的建议权，零写权限。
2. **物理隔离与 Invariant-08 闭环**：
   - 确认 Qdrant 唯一 API Key 仅部署在 AMR 守护进程配置中，外部 Agent（Hermes/OpenClaw/OpenCode/DSH）物理无法直连，单入口收敛。
3. **查明 432 vs 442 差额真相**：
   - 查实差额 10 条为今天下午会话尚未触发会话关闭落盘的活跃点位，已定案优雅对账处置策略。
4. **双验证器升级**：
   - `Evidence Validator` 采用三层匹配（归一化/子串/Jaccard）防编造；
   - `Coverage Validator` 强制要求对全部输入消息声明 `message_disposition`（EXTRACTED / DISCARDED / UNPROCESSED），防沉默遗漏。
5. **记忆演化链条不可覆盖**：
   - 引入 `version`, `previous_version_id`, `root_memory_id`, `superseded_by`，使记忆具备完整历史血缘与回滚能力。
6. **动态熔断算子修正**：
   - 改为 `min(5000, max(20, 0.03 * Total))`，在 432 条规模下将单批上限锁定为 20 条（4.6%）。

---

## 二、 重点核验项与产出要求

请 OpenClaw 依据架构规范对 RFC-005 规范执行全面审计：
1. **核验 AMR 作为“记忆操作系统（MOS）”的架构分层与主客秩序是否已牢不可破？**
2. **核验双验证器（Evidence + Coverage）与状态机设计是否能有效锁死大模型幻觉与信息遗漏？**
3. **核验 Memory Versioning 与 Projection Outbox 的设计是否满足生产级数据演化需求？**
4. **输出终审评级：【通过，立即冻结并准予实施】/【打回】。**
