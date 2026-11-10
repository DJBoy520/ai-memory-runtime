# AMR v5 独立红队审计报告 · Track R3（v5 迁移 / 存量分类 / 隔离区 / 迁移脚本）

- **审计工作目录**: `/home/dj/WorkSpaces/ai-memory-runtime-wt/r3-migrate`（分支 `audit/r3-migrate`，基于 `main @ 1e30119` v5.0.0）
- **审计日期**: 2026-11-10
- **基准生产快照**: `/tmp/amr-rehearsal/sessions_prod_snapshot.db`（只读复制到 `tmp_path` 演练，源库 sha256: `1ea25dfc7de8dc58737d062615fee490f01dec1b77e266f5da8cc07d0695316b`，全程未修改）

---

## 1. 结论

- **总体可靠性/鲁棒性评分**: **4.0 / 5.0**
- **一句话风险判断**:
  > 针对当前特定生产快照（1176 条 memories、3610 条 synced 旧队列、0 条旧 outbox 积压），在副本上演练可平滑升级且 0 任务丢失；但在面对含并发积压重复任务、历史已完成/死信任务、或缺少 `id` 主键列的非标旧库时，迁移存在唯一约束冲突崩溃、任务错误复活以及隔离记录丢失的脆弱性。
- **演练决策**: **准予上线 (GO)**（仅基于生产快照副本演练结果）。
  > **判据依据**: 当前真实生产快照中 `memory_projection_outbox` 为 0 行，未命中旧 outbox 转换段中的脆弱路径；分类回填准确（1173 v3 / 3 legacy）、隔离区为 0、耗时 0.35s 远优于 2.0s 预算、二次初始化幂等稳定、`PRAGMA integrity_check = ok`。但强烈建议研发将下述发现纳入后续修复补丁。

---

## 2. 发现清单

### 【HIGH】1. 旧 outbox 存在同一记忆的多条待处理任务时触发唯一约束冲突导致迁移崩溃回滚
- **where**: `src/core/session_store.py:745-788`
- **what**: 当旧 outbox 存在同一 `(target_collection, memory_id)` 的多条待投递任务时，代码盲目逐行 INSERT 为 pending，在随后建立 `ux_outbox_pending_key` 唯一索引时触发 `sqlite3.IntegrityError: UNIQUE constraint failed`，导致整个迁移崩溃并全部回滚。
- **repro**: `pytest tests/audit_r3_schema_variants.py::test_old_outbox_duplicate_tasks_integrity_error`
- **observed**: 抛出 `sqlite3.IntegrityError: UNIQUE constraint failed: memory_projection_outbox.target_collection, memory_projection_outbox.memory_id`，迁移终止。
- **expected**: 按照 RFC v5.1 §3.1 设计规格，同一记忆在旧 outbox 的多条任务应合并/去重为单个 pending 任务（或带 `ON CONFLICT DO NOTHING`），平滑建表与建立唯一索引。
- **root_cause**: 第 742-759 行遍历 `convertible` 时直接执行无冲突处理的 `INSERT INTO memory_projection_outbox_new`，而第 783 行无条件创建了部分唯一索引 `ux_outbox_pending_key`。
- **status**: 已复现（测试失败）
- **proposed_fix**:
  ```python
  # 建议在写入 memory_projection_outbox_new 前对 (target_collection, memory_id) 做去重保留最新版本，
  # 或在 memory_projection_outbox_new 建表时定义 UNIQUE(target_collection, memory_id) 并使用 INSERT OR IGNORE / ON CONFLICT DO UPDATE
  seen_keys = set()
  for r_dict, m_dict in convertible:
      col = m_dict.get("collection_name") or "ai_memory"
      mid = r_dict["memory_id"]
      if (col, mid) in seen_keys:
          continue
      seen_keys.add((col, mid))
      # 执行写入...
  ```

---

### 【HIGH】2. 旧 outbox 表缺少 `id` 主键列时报告组装触发 `KeyError` 导致隔离记录未能提交
- **where**: `src/core/session_store.py:708`
- **what**: 当旧 `memory_projection_outbox` 表缺少 `id` 列（例如复合主键或自定义主键）时，隔离区报告生成代码 `{"id": r["id"], ...}` 抛出未捕获的 `KeyError: 'id'`，导致在 `except Exception` 处被整体 `ROLLBACK`，隔离记录未能独立提交，且未正常 raise `MigrationBlocked`。
- **repro**: `pytest tests/audit_r3_schema_variants.py::test_old_outbox_missing_id_column_quarantine`
- **observed**: 抛出 `KeyError: 'id'` 并在 except Exception 中执行 `ROLLBACK`，未抛出预期的 `MigrationBlocked`，隔离记录未提交到隔离表。
- **expected**: 即使旧表没有 `id` 列，也能安全落入隔离区，先独立 COMMIT 隔离记录，生成报告，并抛出 `MigrationBlocked`。
- **root_cause**: 第 686 行在向隔离表插入时安全地使用了 `r_dict.get("id")`，但在第 708 行组装 `report_data["quarantine"]` 时硬编码使用了索引访问 `r["id"]`。
- **status**: 已复现（测试失败）
- **proposed_fix**:
  ```python
  # 将 src/core/session_store.py:708 修改为使用 .get("id")
  "quarantine": [
      {"id": r.get("id"), "memory_id": r.get("memory_id"), "reason": reason}
      for r, reason in quarantine
  ],
  ```

---

### 【MEDIUM】3. 极老 memories 表缺少 `status` 或 `qdrant_point_id` 时建索引抛出 `OperationalError`
- **where**: `src/core/session_store.py:440` 及 `src/core/session_store.py:153`
- **what**: 早期老旧库的 `memories` 表缺少 `status` 列或 `qdrant_point_id` 列时，`_migrate_schema_v2` 列补全字典未包含这两列，在执行 `CREATE INDEX idx_memories_status` 或 `CREATE INDEX idx_mem_point` 时抛出 `sqlite3.OperationalError: no such column`。
- **repro**:
  - `pytest tests/audit_r3_schema_variants.py::test_old_schema_missing_status_graceful_migration`
  - `pytest tests/audit_r3_schema_variants.py::test_old_schema_missing_point_id_graceful_migration`
- **observed**: 抛出 `sqlite3.OperationalError: no such column: status` 或 `sqlite3.OperationalError: no such column: qdrant_point_id`。
- **expected**: 迁移前置应当对极老表做完整 PRAGMA 列探测，若缺少 `status` 或 `qdrant_point_id` 自动补全默认值或给出明确诊断提示。
- **root_cause**: `_migrate_schema_v2` 的 `v2_mem_columns` 补充了 30 余个列，但假定旧表必定含有 `status` 和 `qdrant_point_id`，因此未定义这两列的补全语句，但后续代码直接对这两列创建了索引。
- **status**: 已复现（测试失败）
- **proposed_fix**:
  ```python
  # 在 src/core/session_store.py 的 v2_mem_columns 中增加兜底定义：
  "status": "TEXT NOT NULL DEFAULT 'ACTIVE'",
  "qdrant_point_id": "TEXT",
  ```

---

### 【MEDIUM】4. 旧 outbox 中已完成/死信任务在重建时被强制重写为 `pending` 状态复活
- **where**: `src/core/session_store.py:749`
- **what**: 旧 outbox 表中已处理完成（`status='done'`）或已死信（`status='dead_letter'`）的历史记录，在迁移到新 outbox 表时，`status` 被强制写死为 `'pending'`，导致历史任务被错误复活并重新向 Qdrant 投递。
- **repro**: `pytest tests/audit_r3_schema_variants.py::test_old_outbox_non_pending_status_not_revived`
- **observed**: 原状态为 `done` 的历史旧任务在新表中状态变为 `pending`，`retry_count` 被清零。
- **expected**: 已完成（`done`）的任务应保持完成状态或不应作为 `pending` 重新投递；只有原状态为 `pending`（或可重试状态）的任务才应作为 `pending` 迁移。
- **root_cause**: 第 749 行 `INSERT INTO memory_projection_outbox_new` 语句中，硬编码了字面量 `'pending'` 与 `0` 重试次数，没有按旧任务原状态进行映射或过滤。
- **status**: 已复现（测试失败）
- **proposed_fix**:
  ```python
  # 仅迁移旧任务中 status 为 'pending' 的行，或根据 r_dict.get('status') 保留状态：
  old_status = r_dict.get("status") or "pending"
  if old_status not in ("pending", "done", "dead_letter"):
      old_status = "pending"
  ```

---

### 【LOW】5. 迁移报告中的 `ambiguous_count` 字段硬编码为 0
- **where**: `src/core/session_store.py:604, 706, 770`
- **what**: 迁移报告文件（`migration_report.json`）中的 `ambiguous_count` 字段在代码各分支中硬编码为 `0`，未对具备歧义特征（如空白字符 `source_refs`、非合规 JSON 数组、无修订的大写 `DELETED`）的数据进行计数暴露。
- **repro**: `pytest tests/audit_r3_classification_heuristics.py::test_ambiguous_count_not_hardcoded_zero`
- **observed**: 即使库中构造了多条歧义数据，报告中的 `ambiguous_count` 仍始终为 `0`。
- **expected**: 按照设计规范，应对可能存在分类歧义的数据行进行探测与真实计数，供人工审核。
- **root_cause**: 代码中未实现歧义行探测 SQL，直接在字典中字面量赋值 `"ambiguous_count": 0`。
- **status**: 已复现（测试失败）
- **proposed_fix**:
  在存量分类前，执行探测 SQL（如 `SELECT COUNT(*) FROM memories WHERE (source_refs NOT IN ('', '[]') AND NOT (source_refs LIKE '[%]' ...))`）并将计数值填入 `report_data["ambiguous_count"]`。

---

### 【LOW】6. `--rehearse` 创建的临时副本目录未提供自动清理
- **where**: `scripts/migrate_projection_queue.py:273-286`
- **what**: `migrate_projection_queue.py --rehearse` 每次执行都会调用 `tempfile.mkdtemp(prefix="amr_rehearse_")` 创建临时目录并复制几十 MB 的副本数据库，执行完成后未清理该目录，多次演练会导致 `/tmp` 磁盘空间持续增长。
- **repro**: `pytest tests/audit_r3_scripts_and_rehearsal.py::test_script_rehearse_temp_dir_leak`
- **observed**: 演练结束后 `/tmp/amr_rehearse_*` 目录和副本文件依然残留。
- **expected**: 演练完成后可自动清理临时目录（或提供 `--keep-replica` 选项供需要审计时保留）。
- **root_cause**: `run_rehearse_action` 未在 `finally` 块中执行清理逻辑。
- **status**: 已复现（测试失败）
- **proposed_fix**:
  ```python
  # 在 run_rehearse_action 结束时增加自动清理：
  finally:
      if not keep_replica and replica_path.parent.exists():
          shutil.rmtree(replica_path.parent, ignore_errors=True)
  ```

---

## 3. 已验证的攻面（通过的测试覆盖了哪些风险）

本轮审计通过合成数据库与生产快照副本，共编写并通过了 **36 项** 专项红队用例，覆盖以下关键安全攻面：

1. **真实生产快照副本无损演练（1176 条记忆对账）**:
   - 生产快照副本（1176 memories / 3610 synced 旧队列）执行 `--apply` 后：
     - `v3` 分类: **1173** 行
     - `legacy` 分类: **3** 行（严格保留 2026-09-27 无 revisions 且无 source_refs 的历史 DELETED 数据）
     - `qdrant_sync_queue`: 3610 条历史任务原样冻结，0 条丢失
     - `memory_projection_outbox_quarantine`: 0 条
     - 全程迁移耗时 **0.35s**（远低于 2.0s 预算）
     - 迁移后副本 `PRAGMA integrity_check = ok`
     - **源生产快照文件 sha256 绝对一致**（全程只读，0 字节改动）。

2. **迁移顺序与显式事务完整性**:
   - 证明 `ALTER TABLE` 增量加列严格执行于存量数据分类之前，且包裹在单一显式 `BEGIN IMMEDIATE ... COMMIT` 事务中；
   - 证明在迁移中途发生非 `MigrationBlocked` 异常时，`ROLLBACK` 能完整回滚，旧表结构与旧数据完全未被破坏；
   - 证明当发现隔离任务时，先独立 `COMMIT` 隔离表数据，再 raise `MigrationBlocked`，旧 outbox 表保持原样未被删除；
   - 证明隔离任务发生后，运维或守护进程多次重复启动时，`INSERT OR IGNORE` 保证隔离表不发生主键冲突或无限膨胀。

3. **`_amr_migrations` 一次性门控与防重入**:
   - 证明 `v5_projection_class_backfill` 与 `v5_outbox_rebuild` 标记表机制能有效阻断二次重复初始化；
   - 证明在部分标记已存在（如回填已完成但 rebuild 曾被隔离阻断）的场景下，回填逻辑不会重复执行，在修复数据后能平滑接续完成 rebuild。

4. **存量分类核心启发式与边界**:
   - 证明 `memory_revisions > 0` 优先判定为 `v3`（即使 status 为小写或未入队）；
   - 证明规范大写 5 态（`ACTIVE`, `HISTORICAL`, `PENDING_VERIFY`, `CONFLICT`, `TEMPORARY`）无 revisions 仍能准确认定为 `v3`；
   - 证明非空合规 `source_refs` 能够准确识别为 `v3`，空数组 `'[]'`、空串 `''` 和 `NULL` 准确识别为 `legacy`；
   - 证明规范大写 `DELETED` 状态的精确边界：无 revisions 且无 source_refs 时严格保留为 `legacy`，有 revisions 或 source_refs 时升级为 `v3`。

5. **迁移脚本行为严格性（`scripts/migrate_projection_queue.py`）**:
   - 证明 `--inventory` 采用 `file:...?mode=ro` 只读连接，源库文件的 sha256 与 `os.path.getmtime` 严格不变；
   - 证明 `--rehearse` 采用 `sqlite3.connect.backup()` 仅在临时副本演练，绝不修改源库；
   - 证明隔离区非空时，`--inventory`、`--rehearse` 与 `--apply` 均返回非 0 退出码（`exit code 1`）并输出标准告警；
   - 证明全新库（fresh DB）与已迁移库（already-new）执行脚本均为 safe no-op，退出码为 0。

6. **迁移后系统兼容性与运行时契约**:
   - 证明迁移后新生成的 v3 投影任务可被 `OutboxWorker` 平滑消费，完成向量化并执行 CAS 更新为 `done`；
   - 证明新表的 `ux_outbox_pending_key` 约束在常规运行时能正常拦截重复 pending 任务；
   - 证明受控 legacy 路径（`apply_status_change`）在迁移后库上可生成正确的 `amr_delta_v1` delta 信封并入队 `qdrant_sync_queue`，且对 legacy 记忆调用 v3 入队会被严格拒绝（`LegacyPathForbidden`）；
   - 证明 `idempotency_keys` 幂等控制表在单事务写入中正常工作，同 key 重复请求被安全拦截；
   - **证明 C2 状态枚举迁移前置语义**: 迁移后即使将 legacy 行的 status 更新为大写 `ACTIVE`，二次启动 `SessionStore` 不会因状态大写化而将 legacy 数据错误重新分类为 v3。

7. **损坏与恶意库防御**:
   - 截断/损坏 SQLite 文件（truncated DB）安全捕获 `sqlite3.DatabaseError`，无段错误或卡死；
   - 只读文件权限（chmod 0400）在 `--inventory` 下正常工作，在 `--apply` 下安全报错并终止；
   - 并发独占锁（`BEGIN EXCLUSIVE`）在超时后正常抛出 `sqlite3.OperationalError: database is locked`；
   - 100KB+ 超长 `memory_id` 与 1MB+ 巨大 payload 正常存储与迁移，无崩溃无内存泄露；
   - SQL 注入测试：在 `memory_id`、`status`、`source_refs`、`content` 中注入 `'; DROP TABLE memories; --` 等恶意字符，证明内部执行全参数化绑定，表结构与数据完好无损。

---

## 4. 未覆盖 / 无法验证

1. **多进程并发竞争迁移**: 当前迁移设计假定为单实例独占执行（上线维护期停止服务后执行脚本）。未测试多个进程同时对同一个未迁移数据库并发调用 `--apply` 的极端写锁竞争（生产上线由 systemd 停止原服务后人工或单脚本执行，实际不允许多实例并发迁移）。
2. **底层硬件位翻转损坏**: 仅合成了文件截断与随机字节损坏，未模拟文件系统介质级的静默位翻转（Silent Data Corruption），该类故障依赖 SQLite 底层 checksum / OS 级 ECC 内存与 RAID 校验。
3. **真实生产 Qdrant 节点直连**: 审计全程严格遵守纪律，使用 FakeQdrant / In-memory Qdrant，未向生产 192.168.30.161 发送网络流量。

---

## 5. 测试文件清单与运行结果

### 5.1 新增审计测试文件清单
- `tests/audit_r3_schema_variants.py`: 旧 schema 变体、极端缺失列、旧 outbox 各种任务形态与冲突
- `tests/audit_r3_tx_and_quarantine.py`: 迁移执行顺序、事务原子性、ROLLBACK 恢复、隔离区提交与幂等重试
- `tests/audit_r3_classification_heuristics.py`: 存量记忆分类启发式算法、DELETED 边界、歧义行审计
- `tests/audit_r3_scripts_and_rehearsal.py`: 迁移脚本 `--inventory`/`--rehearse`/`--apply` 只读性、退出码与生产快照真实演练
- `tests/audit_r3_post_migration_and_compatibility.py`: 迁移后队列排水、v3 投影 CAS、legacy delta、幂等表与 C2 状态前置
- `tests/audit_r3_malicious_and_corruption.py`: 截断库、只读文件、busy 锁超时、超长负载、SQL 注入攻击防御

### 5.2 审计测试套件运行结果
执行命令:
```bash
PYTHONPATH=$PWD /home/dj/WorkSpaces/ai-memory-runtime/.venv/bin/python -m pytest tests/audit_r3_*.py -q
```
运行输出尾部:
```text
....F...........FF........FFF..F...........                              [100%]
=========================== short test summary info ============================
FAILED tests/audit_r3_classification_heuristics.py::test_ambiguous_count_not_hardcoded_zero - assert 0 > 0
FAILED tests/audit_r3_schema_variants.py::test_old_schema_missing_status_graceful_migration - sqlite3.OperationalError: no such column: status
FAILED tests/audit_r3_schema_variants.py::test_old_schema_missing_point_id_graceful_migration - sqlite3.OperationalError: no such column: qdrant_point_id
FAILED tests/audit_r3_schema_variants.py::test_old_outbox_duplicate_tasks_integrity_error - sqlite3.IntegrityError: UNIQUE constraint failed: memory_projection_outbox.target_collection, memory_projection_outbox.memory_id
FAILED tests/audit_r3_schema_variants.py::test_old_outbox_missing_id_column_quarantine - KeyError: 'id'
FAILED tests/audit_r3_schema_variants.py::test_old_outbox_non_pending_status_not_revived - assert 'pending' == 'done'
FAILED tests/audit_r3_scripts_and_rehearsal.py::test_script_rehearse_temp_dir_leak - AssertionError: assert not True
7 failed, 36 passed in 10.45s
```
*注：7 项失败用例严格用于可复现地证明上述第 2 节报告中的 6 项产品设计/代码缺陷。*

### 5.3 既有全量回归测试套件运行结果
执行命令:
```bash
PYTHONPATH=$PWD /home/dj/WorkSpaces/ai-memory-runtime/.venv/bin/python -m pytest tests/ -q
```
运行输出尾部:
```text
........................................................................ [ 21%]
........................................................................ [ 42%]
........................................................................ [ 63%]
...................................................................ssss. [ 84%]
..........s...............s...........................                   [100%]
336 passed, 6 skipped, 7 warnings in 46.88s
```
既有测试 100% 通过（336 passed, 6 skipped），无任何断言退化或既有用例破坏。
