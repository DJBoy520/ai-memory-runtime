# 终审任务书：RFC-003 (v1.0.1 终审冻结版) 多 Agent 记忆对账中枢架构审计

- **委托方**: Hermes (方案架构组)
- **受托方**: OpenClaw (独立代码与安全审计专家)
- **待审文档**: `/home/dj/WorkSpaces/ai-memory-runtime/docs/proposals/RFC-003_daily_memory_reconciliation_spec.md`
- **审计模式**: 严格只读（禁止修改任何业务代码与配置，只输出专业审计报告）

---

## 一、 审计背景与本次升版（v1.0.1）闭环清单

本次升版彻底停止脑暴规则，直接基于生产真实状态（SQLite 356 条 `memories`，全部 `status='active'`；Qdrant 427 点位，包含 71 条 OpenClaw 直连写入的孤儿点位）完成 **5 项 P0 阻断性缺陷** 与 **6 项 P1 关键工程加固** 的全面闭环：

1. **[P0-1 闭环] 消除 Invariant-02 与 stale 矛盾**：明确只有 `active` 与 `stale` 允许存在于主检索投影，`archived` 与 `deleted` 严禁存在。
2. **[P0-2 闭环] 验证真实 status 语义并归一**：实测确认全部为 `'active'`，废弃 `curation_status`，废弃冗余 `is_tombstone`，统一为单值状态机。
3. **[P0-3 闭环] 确立 Projection Lag 机制**：Qdrant 失败严禁反向回滚已提交的 SQLite 事务，由 Outbox 指数退避重试收敛。
4. **[P0-4 闭环] 查明并定案 356 vs 427 差额**：查实差额 71 条为历史孤儿点位，制定第一阶段候选保护与反向补登契约，严禁误删。
5. **[P0-5 闭环] 灾备重建显式过滤**：重建必须跳过 `deleted` 与 `archived`，固化 BGE-M3 向量元数据 Provenance。
6. **[P1 闭环] 批次 ID 与扫描时间解耦**（`curation_batch_id` vs `last_reconciled_at`）。
7. **[P1 闭环] 评分公式修正**：加入 `max(RecencyFactor, 0.2)` 保底，防止长周期重要知识被归零。
8. **[P1 闭环] 熔断机制加装绝对上下限**：`max(50, min(0.05 * total, 5000))`。
9. **[P1 闭环] 抽象升级 Projection Outbox**：解耦 Qdrant，支持多投影扩展，带唯一幂等键约束。

---

## 二、 重点核验项与产出要求

请 OpenClaw 进行最终复核并出具终审结论：
1. **核验当前 v1.0.1 的 7 条不变量与数据模型是否已具备绝对确定性，是否可以立即冻结？**
2. **核验 71 条孤儿点位的对账与反向补登设计是否稳妥？**
3. **输出终审评级：【通过，立即冻结并准予编码实施】/【打回】。**
