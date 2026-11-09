# AMR v5 数据库迁移预演与对账报告

> 本报告记录在**生产数据库只读快照副本**上执行 `scripts/migrate_projection_queue.py --rehearse` 的真实结果。
> 源生产库全程未改动；迁移仅在临时副本上进行。

---

## 1. 预演环境与基本信息

- **预演执行时间**: 2026-11-10 00:47 CST（2026-11-09 16:47 UTC）
- **执行方式**: 编排方自动预演（Gemini 子 agent 编码 + 独立验证）
- **源数据库路径**: `/tmp/amr-rehearsal/sessions_prod_snapshot.db`
  （由生产 `data/sessions.db` 的 sqlite3 backup API 生成，1176 条 memories）
- **临时副本路径**: `/tmp/amr_rehearse_ru8v7_gp/rehearsal_replica.db`
- **演练代码版本**: `f7049ad`（feat/amr-reliability-v5 集成分支）
- **迁移结果状态**: `COMPLETED` → **GO**
- **源库完整性**: 迁移前后 `sha256` 一致（脚本只读复制，源库 schema 仍为 legacy_old，无 `_amr_migrations` 表）

---

## 2. 模式与资产盘点前后对账表

| 维度 | 盘点项 | 迁移前 (Before) | 迁移后 (After) | 差异 | 结论 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Schema** | `memories.projection_revision` | 无 | 有 | +1 列 | ✅ |
| | `memories.collection_name` | 无 | 有 | +1 列 | ✅ |
| | `memories.projection_class` | 无 | 有 | +1 列 | ✅ |
| | `memory_projection_outbox` | legacy_old | v5_new | 换表 | ✅ |
| | `_amr_migrations` | 无 | 2 条记录 | +2 | ✅ |
| | `idempotency_keys` | 无 | 有 | 新表 | ✅ |
| **memories** | v3 记忆行数 | 0（未分类） | **1173** | +1173 | ✅ |
| | legacy 记忆行数 | 0（未分类） | **3** | +3 | ✅ |
| | memories 总行数 | **1176** | **1176** | **0** | ✅ 无事实行丢失 |
| | 状态分布（迁移后） | — | ACTIVE 623 / HISTORICAL 522 / DELETED 27 / PENDING_VERIFY 1（v3）；DELETED 3（legacy） | — | ✅ |
| **新投影队列** | pending / done / dead_letter | 0 / 0 / 0 | **0 / 0 / 0** | 0 | ✅ |
| | 可转换旧任务 | 0 | 0 | 0 | ✅ |
| **旧同步队列** | qdrant_sync_queue 总数 | **3610** | **3610** | **0** | ✅ 冻结未删 |
| | 旧队列状态 | 全部 synced | 全部 synced | 0 | ✅ |
| **隔离区** | `quarantine_count` | 0 | **0** | **0** | ✅ 上线门禁通过 |

**分类依据**（一次性 `_amr_migrations` 门控）：
- 存在 `memory_revisions` 行 ∨ status ∈ 大写规范集 ∨ `source_refs` 非空 → `projection_class='v3'`；
- 其余保持 `'legacy'`。本次仅 3 条 2026-09-27 的 legacy DELETED 行（source_refs 为空、无 revisions）保持 legacy。

---

## 3. 隔离区详细清单

```json
[]
```

> `quarantine_count = 0`，无需人工审批。

---

## 4. 迁移脚本执行输出原始摘要

```json
{
  "action": "rehearse",
  "status": "completed",
  "source_db": "/tmp/amr-rehearsal/sessions_prod_snapshot.db",
  "replica_db": "/tmp/amr_rehearse_ru8v7_gp/rehearsal_replica.db",
  "before": {
    "schema_status": {"memory_projection_outbox": {"schema_type": "legacy_old"}},
    "memories_classification": {"total": 1176, "by_projection_class": {"unclassified": 1176}},
    "old_queue": {"total": 3610, "by_status": {"synced": 3610}}
  },
  "after": {
    "schema_status": {"memory_projection_outbox": {"schema_type": "v5_new"}},
    "memories_classification": {"total": 1176, "by_projection_class": {"legacy": 3, "v3": 1173}},
    "new_queue": {"exists": true, "total": 0, "by_status": {}}
  },
  "quarantine_count": 0
}
```

二次初始化验证：`_amr_migrations` 两条标记不变、分类计数不变（幂等 no-op）；副本 `PRAGMA integrity_check = ok`。

---

## 5. 结论与上线决策

- [x] **准予上线 (GO)**：quarantine=0，模式加列完整，事实行 0 丢失，旧队列原样冻结，二次初始化幂等。
- [ ] **中止上线 (NO-GO)**

上线顺序与回退操作见 `docs/ROLLOUT-AMR-RELIABILITY.md`。
