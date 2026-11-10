# AMR v5 迁移与存储缺陷修复报告 · Track R3 & Track R2

- **工作目录**: `/home/dj/WorkSpaces/ai-memory-runtime-wt/fx-store`（分支 `fix/audit-store`，基线 `main @ 1e30119` v5.0.0）
- **修复日期**: 2026-11-10
- **基准生产快照**: `/tmp/amr-rehearsal/sessions_prod_snapshot.db`（sha256: `1ea25dfc7de8dc58737d062615fee490f01dec1b77e266f5da8cc07d0695316b`，全程严格只读，0 字节改动）

---

## 1. 修复总结

本次修复针对 Track R3 审计报告中发现的 7 项迁移与预演缺陷，以及 Track R2 报告中追加的 6 项核心存储与事务缺陷进行了完整修复。修复后：
- `tests/audit_r3_*.py`: **43 passed / 0 failed**（原有 7 个失败用例全部转绿）
- `tests/audit_r2_*.py`: **36 passed / 0 failed**（原有 35 项保持全绿，部分 defect 测试强化为断言修复后正确契约，并补充白名单测试）
- 全量测试 `tests/ -q`: **336 passed, 6 skipped, 0 failed**

---

## 2. R3 迁移核心缺陷修复详解

### 1. 旧 outbox 同 (collection, memory_id) 多条任务导致唯一索引 IntegrityError
- **改动文件**: `src/core/session_store.py` (`_migrate_schema_v5`)
- **根因**: 旧 `memory_projection_outbox` 允许同一记忆积压多条任务；迁移代码直接逐行插入 `memory_projection_outbox_new`，随后创建部分唯一索引 `ux_outbox_pending_key (target_collection, memory_id) WHERE status='pending'` 时触发 `sqlite3.IntegrityError: UNIQUE constraint failed`，导致整个迁移事务崩溃并全部回滚。
- **修复方式**: 在插入新表前对 `convertible` 任务按 `(target_collection, memory_id)` 进行去重字典映射，保留最新一条记录（比较排序 key 优先取 `projection_revision` / `version`，次选 `created_at`，最后按 `id` 取大），确保写入新表时不产生待处理任务冲突，顺利建立唯一索引。
- **验收测试**: `tests/audit_r3_schema_variants.py::test_old_outbox_duplicate_tasks_integrity_error` (PASSED)

### 2. 旧 outbox 缺少 `id` 列时报告组装触发 KeyError
- **改动文件**: `src/core/session_store.py` (`_migrate_schema_v5`)
- **根因**: 当旧表以 `memory_id` 为主键或为复合主键且未包含 `id` 列时，代码向隔离表插入时安全使用了 `r_dict.get("id")`，但在组装 `report_data["quarantine"]` 时硬编码了 `r["id"]` 索引访问，触发 `KeyError: 'id'` 并进入外层 `except Exception` 整体回滚，导致隔离记录未能提交且未正常 raise `MigrationBlocked`。
- **修复方式**: 报告组装统一使用 `r.get("id")` 与 `r.get("memory_id")`；并在 raise `MigrationBlocked` 前维持先独立 `COMMIT` 隔离区记录的设计契约。
- **验收测试**: `tests/audit_r3_schema_variants.py::test_old_outbox_missing_id_column_quarantine` (PASSED)

### 3. 极老 memories 表缺少 `status` 或 `qdrant_point_id` 导致 OperationalError
- **改动文件**: `src/core/session_store.py` (`_migrate_schema_v2`, `_migrate_schema_v5`)
- **根因**: `_migrate_schema_v2` 中的 `v2_mem_columns` 列补全字典未包含 `status` 和 `qdrant_point_id`（假设旧表必定具备此两列），导致在老库上执行后续 `CREATE INDEX idx_memories_status` 或 `CREATE INDEX idx_mem_point` 时抛出 `OperationalError: no such column`。
- **修复方式**:
  1. 在 `_migrate_schema_v2` 的 `v2_mem_columns` 字典中增加兜底列定义：
     `"status": "TEXT NOT NULL DEFAULT 'ACTIVE'"` 与 `"qdrant_point_id": "TEXT"`；
  2. 若探测到历史行缺失 `qdrant_point_id`，立即回填为 `memory_id`；缺失 `status` 兜底回填为 `'ACTIVE'`；
  3. 在 `_migrate_schema_v5` 的 Step 0 PRAGMA 探测中同步增加防御性补列与回填。
- **验收测试**:
  - `tests/audit_r3_schema_variants.py::test_old_schema_missing_status_graceful_migration` (PASSED)
  - `tests/audit_r3_schema_variants.py::test_old_schema_missing_point_id_graceful_migration` (PASSED)

### 4. 旧 outbox 非 pending 任务被错误复活为 pending
- **改动文件**: `src/core/session_store.py` (`_migrate_schema_v5`)
- **根因**: 旧 outbox 表中已处理完成（`done`/`completed`/`synced`）或死信（`dead_letter`）的任务，在插入 `memory_projection_outbox_new` 时硬编码了字面量 `'pending'` 与重试次数 `0`，导致历史任务被错误复活并重新向 Qdrant 投递。
- **修复方式**: 实现标准状态映射函数 `_map_outbox_status`：
  - `completed` / `done` / `synced` → `done`（保持 `projected_revision = projection_revision`）
  - `dead_letter` → `dead_letter`（保留原 `dead_letter_at`，若无则取 `now`）
  - `pending` / `failed` → `pending`（保留原 `retry_count` 与合法 `next_retry_at`）
  - 严禁将已完成/死信任务重新投递。
- **验收测试**: `tests/audit_r3_schema_variants.py::test_old_outbox_non_pending_status_not_revived` (PASSED)

### 5. `ambiguous_count` 硬编码为 0 修复
- **改动文件**: `src/core/session_store.py` (`_migrate_schema_v5`)
- **根因**: 代码未实现歧义行探测 SQL，在存量分类报告与 `_amr_migrations` 表详情中均硬编码 `"ambiguous_count": 0`。
- **修复方式**: 在存量分类阶段执行真实歧义特征探测 SQL，统计以下存量歧义行：
  1. `status` 为 NULL 或不属于规范 6 态且非规范 legacy 态（如 `'candidate'` 等非标中间状态）；
  2. `source_refs` 存在空白字符或非标准 JSON 数组；
  3. `status = 'DELETED'` 且无任何 `memory_revisions` 快照记录（存量分类归属分歧风险）；
  将统计值真实写入 `migration_report.json`（各分支）以及 `_amr_migrations` 的 `details` JSON 中。
- **验收测试**: `tests/audit_r3_classification_heuristics.py::test_ambiguous_count_not_hardcoded_zero` (PASSED)

### 6. `--rehearse` 临时副本目录泄漏修复
- **改动文件**: `scripts/migrate_projection_queue.py` (`run_rehearse_action`)
- **根因**: 每次 `--rehearse` 均使用 `tempfile.mkdtemp(prefix="amr_rehearse_")` 创建临时目录，但在演练完成后未在 `finally` 中清理，导致 `/tmp` 产生大量残留。
- **修复方式**:
  1. 将整个演练流程包裹在 `try...finally` 块中，在 `finally` 内执行 `shutil.rmtree(temp_dir, ignore_errors=True)`；
  2. 新增可选命令行参数 `--keep-replica`，便于需要离线深度排错时保留副本。
- **验收测试**: `tests/audit_r3_scripts_and_rehearsal.py::test_script_rehearse_temp_dir_leak` (PASSED)

---

## 3. R2 报告追加真实缺陷修复与断言强化清单

根据任务书要求，修复了 R2 报告中的 6 项真实缺陷，并将对应的“defect”测试断言强化为**修复后的正确契约**：

### F-R2-01: Legacy 记忆 DELETED 终态防护
- **代码修复**: 在 `src/core/session_store.py` 的 `apply_status_change` 头部增加终态检查：
  ```python
  for r in rows:
      curr = normalize_status(r["status"] if "status" in r.keys() else "ACTIVE")
      if curr == "DELETED" and canonical != "DELETED":
          raise ValueError("Illegal status transition: DELETED state is terminal and cannot be reverted.")
  ```
- **测试断言改动** (`tests/audit_r2_state_family.py::test_legacy_deleted_status_reversion_defect`):
  - 原测试断言错误行为: `res = store.change_memory_status(mem_id, "ACTIVE"); assert res["current_status"] == "ACTIVE"`
  - 改为断言正确契约:
    ```python
    with pytest.raises(ValueError) as exc:
        store.change_memory_status(mem_id, "ACTIVE")
    assert "DELETED state is terminal and cannot be reverted" in str(exc.value)
    assert store.get_memory(mem_id)["status"] == "DELETED"
    ```

### F-R2-02: `update_memory_v3` 校验 `superseded_by`
- **代码修复**: 在 `src/core/session_store.py` 的 `update_memory_v3` 状态更新分支增加：
  ```python
  if superseded_by:
      self._validate_superseded_target(cursor, [row], superseded_by)
  ```
- **测试断言改动** (`tests/audit_r2_state_family.py::test_update_memory_v3_self_referencing_superseded_by_defect`):
  - 原测试断言错误行为: `store.update_memory_v3(...); assert m["superseded_by"] == mem_id`
  - 改为断言正确契约:
    ```python
    with pytest.raises(ValueError) as exc_self:
        store.update_memory_v3(memory_id=mem_id, status="HISTORICAL", superseded_by=mem_id)
    assert "must not reference the same memory or its shard family" in str(exc_self.value)

    with pytest.raises(ValueError) as exc_ghost:
        store.update_memory_v3(memory_id=mem_id, status="HISTORICAL", superseded_by="mem_ghost_nonexistent")
    assert "not found" in str(exc_ghost.value)
    ```

### F-R2-03: `execute_idempotent` 重放 None / 非 dict 返回值
- **代码修复**: 在 `src/core/session_store.py` 的 `execute_idempotent` 重放分支中增加类型分支：
  ```python
  resp_raw = json.loads(row["response_json"]) if row["response_json"] else None
  if isinstance(resp_raw, dict):
      return {**resp_raw, "idempotent_replay": True}
  return {"result": resp_raw, "idempotent_replay": True}
  ```
- **测试断言改动** (`tests/audit_r2_transactions.py::test_idempotent_non_dict_response_replay_defect`):
  - 原测试断言崩溃错误: `with pytest.raises(TypeError): store.execute_idempotent(...)`
  - 改为断言正确契约:
    ```python
    res_replay = store.execute_idempotent("custom_op", idem_key, digest, lambda cur: None)
    assert res_replay == {"result": None, "idempotent_replay": True}
    ```

### F-R2-04: `locked_write_transaction` 事务重入与 SAVEPOINT 支持
- **代码修复**: 在 `src/core/session_store.py` 中维护 `_tx_depth` 深度计数。最外层（depth 0）执行 `BEGIN IMMEDIATE / COMMIT / ROLLBACK`；内层嵌套事务使用 `SAVEPOINT / RELEASE SAVEPOINT / ROLLBACK TO SAVEPOINT`，避免内层提前 COMMIT 破坏原子性。
- **测试断言改动** (`tests/audit_r2_transactions.py::test_locked_write_transaction_nesting_defect`):
  - 原测试断言崩溃错误: `with pytest.raises(sqlite3.OperationalError): assert "cannot commit - no transaction is active"`
  - 改为断言正确契约:
    ```python
    with store.locked_write_transaction() as conn1:
        conn1.execute("INSERT INTO sessions (session_id, agent_id, started_at) VALUES ('s1', 'a1', 100);")
        with store.locked_write_transaction() as conn2:
            conn2.execute("INSERT INTO sessions (session_id, agent_id, started_at) VALUES ('s2', 'a2', 200);")

    conn = store.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT session_id FROM sessions WHERE session_id IN ('s1', 's2') ORDER BY session_id;")
    assert [r[0] for r in cur.fetchall()] == ["s1", "s2"]
    ```

### F-R2-05: 目标行 `conflicts_with` 为合法非列表 JSON 容错
- **代码修复**: 在 `create_memory_v3`、`update_memory_v3` 以及 `get_memory_v3` 解析 `conflicts_with` 和 `source_refs` 时增加 `isinstance(..., list)` 守卫，遇到非列表 JSON（如 int/dict）时安全降级为 `[]`。
- **测试断言改动** (`tests/audit_r2_integrity_capacity.py::test_json_fields_error_tolerance_and_non_list_crash_defect`):
  - 原测试断言崩溃错误: `with pytest.raises(TypeError): ...` 以及 `with pytest.raises(AttributeError): ...`
  - 改为断言正确契约:
    ```python
    store.create_memory_v3(memory_id="mem_attacker_int", content="...", conflicts_with=[mem_id])
    assert "mem_attacker_int" in store.get_memory_v3(mem_id)["conflicts_with"]

    store.create_memory_v3(memory_id="mem_attacker_dict", content="...", conflicts_with=[mem_id])
    assert "mem_attacker_dict" in store.get_memory_v3(mem_id)["conflicts_with"]
    ```

### F-R2-06: `checkpoint(mode)` 参数白名单校验
- **代码修复**: 在 `src/core/session_store.py` 的 `checkpoint` 接口增加参数校验：
  ```python
  valid_modes = {"PASSIVE", "FULL", "RESTART", "TRUNCATE"}
  mode_clean = mode.upper().strip() if mode else ""
  if mode_clean not in valid_modes:
      raise ValueError(f"Invalid checkpoint mode: '{mode}'. Must be one of {valid_modes}")
  ```
- **新增测试** (`tests/audit_r2_transactions.py::test_checkpoint_mode_whitelist`):
  - 验证 `PASSIVE`、`FULL`、`RESTART`、`TRUNCATE`（含大小写）均可正常执行；
  - 验证非法字符串、SQL 注入尝试、空字符串均抛出 `ValueError("Invalid checkpoint mode...")`。

---

## 4. 修改文件清单

1. `src/core/session_store.py`:
   - `_migrate_schema_v2`: 增加 `status` 与 `qdrant_point_id` 列补全及兜底回填；
   - `_migrate_schema_v5`: Step 0 探测兜底、`ambiguous_count` 真实统计、隔离区报告使用 `.get("id")`、旧 outbox 状态映射与去重；
   - `checkpoint`: 增加模式白名单校验；
   - `locked_write_transaction`: 事务嵌套深度计数器与 `SAVEPOINT` 机制；
   - `execute_idempotent`: 重放分支非 dict 包装为字典；
   - `apply_status_change`: legacy 行 DELETED 终态不可逆状态机校验；
   - `update_memory_v3`: 增加 `superseded_by` 合法性与自指校验；
   - `create_memory_v3`, `update_memory_v3`, `get_memory_v3`: `conflicts_with` / `source_refs` 非列表 JSON 降级防护。
2. `scripts/migrate_projection_queue.py`:
   - `run_rehearse_action`: 增加 `finally` 块清理临时目录 `shutil.rmtree(temp_dir)`；
   - 增加 `--keep-replica` 命令行选项。
3. `tests/audit_r2_state_family.py`:
   - 将 `test_legacy_deleted_status_reversion_defect` 与 `test_update_memory_v3_self_referencing_superseded_by_defect` 强化为断言修复后契约。
4. `tests/audit_r2_transactions.py`:
   - 将 `test_idempotent_non_dict_response_replay_defect` 与 `test_locked_write_transaction_nesting_defect` 强化为断言修复后契约；
   - 新增 `test_checkpoint_mode_whitelist` 测试白名单。
5. `tests/audit_r2_integrity_capacity.py`:
   - 将 `test_json_fields_error_tolerance_and_non_list_crash_defect` 强化为断言修复后契约。

---

## 5. 测试验收执行输出

### 5.1 `pytest tests/audit_r3_*.py -q`
```
...........................................                              [100%]
43 passed in 9.97s
```

### 5.2 `pytest tests/audit_r2_*.py -q`
```
....................................                                     [100%]
36 passed, 1 warning in 30.87s
```

### 5.3 全量回归 `pytest tests/ -q`
```
........................................................................ [ 21%]
........................................................................ [ 42%]
........................................................................ [ 63%]
...................................................................ssss. [ 84%]
..........s...............s...........................                   [100%]
336 passed, 6 skipped, 7 warnings in 46.10s
```

---

## 6. 遗留项

当前所有在审计报告中复现的失败用例与真实缺陷均已得到修复，全量 0 失败，生产快照副本演练完全一致（sha256 0 改动，0 隔离任务，0 数据丢失）。无遗留未解决项。
