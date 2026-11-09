# AMR 阶段 A 数据库迁移预演与对账报告模板

> **使用说明**：
> 本报告由 `scripts/migrate_projection_queue.py --rehearse --report docs/MIGRATION-REHEARSAL-REPORT.md` 自动生成或辅助填充。
> 在生产上线窗口执行前，运维人员必须在生产环境备份的数据库副本上完整演练一次，并核准本报告中的对账数据。

---

## 1. 预演环境与基本信息

- **预演执行时间**: YYYY-MM-DD HH:MM:SS UTC
- **执行人**: [运维工程师/开发负责人]
- **源数据库路径**: `/data/amr/storage/amr.db`
- **临时副本路径**: `/tmp/amr_rehearse_xxxx/rehearsal_replica.db`
- **演练版本 Git SHA**: `[commit sha]`
- **迁移结果状态**: `[ COMPLETED | BLOCKED ]`

---

## 2. 模式与资产盘点前后对账表

| 维度 | 盘点项 | 迁移前 (Before) | 迁移后 (After) | 差异 (Diff) | 预期基线 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Schema 状态** | `memories.projection_revision` | [有/无] | 有 | +1 列 | 必须存在 |
| | `memories.collection_name` | [有/无] | 有 | +1 列 | 必须存在 |
| | `memories.projection_class` | [有/无] | 有 | +1 列 | 必须存在 |
| | `memory_projection_outbox` 结构 | [旧/无/新] | 新 (v5) | 升级换表 | 无快照/point列，含revision |
| | `_amr_migrations` 表 | [有/无] | 有 | +2 记录 | 包含回填与重建标记 |
| **memories 事实表** | v3 记忆行数 | 0 | [数字] | 增加 | 包含 revisions/大写态的记忆 |
| | legacy 记忆行数 | [数字] | [数字] | 减少 | 存量扁平旧记忆 |
| | memories 总行数 | [数字] | [数字] | 0 (不变) | 严禁事实行丢失 |
| **新投影队列** | pending 待投影任务数 | 0 | [数字] | 增加 | 旧表合法任务已平移 |
| | done 已完成任务数 | 0 | 0 | 0 | 新建表初始无 done |
| | dead_letter 死信任务数 | 0 | 0 | 0 | 必须为 0 |
| **旧同步队列** | 旧队列行总数 (qdrant_sync_queue) | [数字] | [数字] | 0 (不变) | 冻结旧表，不可删除 |
| | 旧队列中 pending 任务数 | [数字] | [数字] | 0 (不变) | 待后续 Worker 启动时排水 |
| **隔离区 (Quarantine)**| 隔离任务总数 (`quarantine_count`) | 0 | **0** | **0** | **必须为 0，否则阻断上线** |

---

## 3. 隔离区详细清单 (若非空)

若存在无法转换的孤儿任务（即 memories 主表中已不存在对应行或点位缺失），清单记录如下：

```json
[
  /*
  示例：
  {
    "id": 1024,
    "memory_id": "mem_missing_001",
    "reason": "memory row missing (orphan)"
  }
  */
]
```

> **审批声明**：
> 若隔离区包含任何任务，必须在此填写研发负责人审批签字：
> - 阻断原因确认人：________________
> - 处理方案（确认丢弃/人工补齐）：________________
> - 批准上线签字：________________  日期：________

---

## 4. 迁移脚本执行输出原始摘要

```json
{
  "action": "rehearse",
  "status": "completed",
  "source_db": "/data/amr/storage/amr.db",
  "replica_db": "/tmp/amr_rehearse_demo/rehearsal_replica.db",
  "quarantine_count": 0,
  "timestamp": 1731234567
}
```

---

## 5. 结论与上线决策

- [ ] **准予上线 (GO)**：所有对账数据平稳，quarantine 隔离区为 0，模式加列完整。
- [ ] **中止上线 (NO-GO)**：存在孤儿任务阻断，需退回研发分支修复数据。
