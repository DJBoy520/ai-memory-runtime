# Task 01 任务书：SQLite SSOT 核心数据表与 DDL 迁移落地

## 一、 执行者与工作区
- **执行者**：OpenCode
- **工作区**：`/home/dj/WorkSpaces/ai-memory-runtime`
- **当前 Git 分支**：`feat/amr-engine-upgrade-v2`
- **规范基准**：`docs/proposals/AMR认知记忆引擎升级与全量离线提纯实施规格_v2.2终版.md` (第二节与第四节)

---

## 二、 目标与交付清单

扩展 `src/core/session_store.py`（或建立专有存储层），实现符合 v2.2 规格的完整 SQLite 表结构生命周期管理：

1. **核心数据表（4 张业务表 + 2 张治理表）**：
   - `raw_sessions`：原始会话表；
   - `raw_messages`：消息表，增加 `source_type`（枚举：`user_message` / `assistant_message` / `tool_output` / `system` / `legacy_memory` / `imported`）与 `is_synthetic` 字段；
   - `memories`（知识主表 SSOT）：
     - `memory_id` (TEXT PRIMARY KEY)
     - `qdrant_point_id` (TEXT NOT NULL UNIQUE)
     - `type` (`fact`, `preference`, `decision`, `task`, `episode`, `relation`)
     - `conflict_policy` (`overwrite`, `coexist`, `state_machine`, `immutable`)
     - `subject`, `predicate`, `object`, `content`
     - `valid_from`, `valid_to`, `validity_type` (`open_ended`, `bounded`, `unknown`)
     - `confidence`, `importance`, `mention_count`
     - `status` (`candidate`, `active`, `superseded`, `archived`, `deleted`)
     - `superseded_by`, `project_id`, `scope`, `source_agent`, `version`
     - `deleted_at`, `deleted_by`, `deletion_reason`
     - `created_at`, `updated_at`
   - `memory_evidence`：多对多关联表，包含 `evidence_strength`；
   - `qdrant_sync_queue`：Transactional Outbox 队列表，包含 `op_type`, `payload_snapshot`, `status`, `retry_count`, `last_error`；
   - `memory_audit_log`：治理审计表，包含 `action`, `operator`, `detail`, `timestamp`。

2. **方法与契约实现**：
   - 增加对上述表的初始化及幂等增量迁移支持（若旧表存在则平滑添加新列，若不存在则创建完整 DDL）；
   - 提供底层存储方法：
     - `create_memory(...)`：原子写入 `memories`、`memory_evidence`，并**在同一事务中**写入 `qdrant_sync_queue`（保证 Outbox 事务原子性）；
     - `get_memory(memory_id)`：读取完整记忆实体；
     - `update_memory_status(memory_id, status, ...)`：更新状态、`superseded_by` 并追加 Outbox 任务与 audit log；
     - `fetch_pending_sync_tasks(limit=50)`：拉取待同步发件箱队列；
     - `mark_sync_task_done(task_id)` / `mark_sync_task_failed(task_id, error)`。

3. **单测保障**：
   - 在 `tests/test_v2_storage.py` 中编写完备单元测试，覆盖全表创建、字段写入、外键关系、唯一约束、事务原子性与并发 `busy_timeout`。

---

## 三、 质量与审计门禁
1. **严格类型校验**：使用 Python 3.11/3.12 兼容强类型；
2. **零现有测试破坏**：运行 `pytest tests/` 原有 167 个测试用例必须 100% 保持通过；
3. **完成标准**：`pytest tests/test_v2_storage.py` 全部 PASS，代码整洁无 lint 错误。
