# AMR v5 独立红队审计报告 · R2 存储 / 投影队列 / Worker / 并发与崩溃恢复

## 1. 结论

- **总体可靠性/鲁棒性评价**：**4.2 / 5.0**
- **一句话风险判断**：AMR v5 的核心写入承诺（“提交即成功 + 单写者 flock + CAS 乐观版本控制 + 崩溃幂等自愈”）在主干路径上表现坚固且经过充分验证；但在**遗留 legacy 状态机终态防御**、**更新路径自指校验一致性**、**重入事务管理**及**畸形/非标 JSON 类型守卫**上存在可被利用或引发服务崩溃的确定性缺陷。

---

## 2. 发现清单 (Findings)

### [High] F-R2-01: Legacy 记忆在 DELETED 终态下允许通过 `change_memory_status` 逆转回 ACTIVE

- **where**: `src/core/session_store.py:1801-1850` (`apply_status_change`) 与 `src/core/session_store.py:2020-2038` (`change_memory_status`)
- **what**: legacy 记忆在 DELETED 终态下缺失不可逆状态机校验，允许被直接变更为 ACTIVE 重新复活，推翻终态不可逆契约。
- **repro**: `tests/audit_r2_state_family.py::test_legacy_deleted_status_reversion_defect`
- **observed vs expected**:
  - *Observed*: 对已标记 `status='DELETED'` 的 legacy 记忆调用 `store.change_memory_status(legacy_id, "ACTIVE")`，函数无报错成功执行，SQLite 中行状态被改写回 `'ACTIVE'` 并生成 promote 审计日志。
  - *Expected*: 必须与 v3 记忆行为一致，抛出 `ValueError("Illegal status transition: DELETED state is terminal and cannot be reverted.")`，拦截对已删除实体的逆向流转。
- **root_cause**: `change_memory_status` 在 `if has_v3:` 分支内部显式执行了 `if curr_status == "DELETED" and canonical != "DELETED": raise ValueError(...)` 终态检查；但对于全 legacy 行族，直接交由 `apply_status_change` 纯函数处理，而 `apply_status_change` 内部直接构造 `UPDATE memories SET status = ?`，没有任何状态机合法性或终态检查。
- **status**: 已复现（真实 Bug）
- **proposed_fix**:
  在 `src/core/session_store.py` 的 `apply_status_change` 中增加对已有状态的终态校验：
  ```python
  for r in rows:
      curr_status = normalize_status(r["status"] if "status" in r.keys() else "ACTIVE")
      if curr_status == "DELETED" and canonical != "DELETED":
          raise ValueError("Illegal status transition: DELETED state is terminal and cannot be reverted.")
  ```

---

### [High] F-R2-02: `update_memory_v3` 缺失 `_validate_superseded_target` 校验，允许自指 (superseded_by=self) 导致重定向死循环

- **where**: `src/core/session_store.py:2587-2603` (`update_memory_v3`)
- **what**: `update_memory_v3` 处理状态更新时未调用 `_validate_superseded_target`，允许将 `superseded_by` 设为自身或无效目标。
- **repro**: `tests/audit_r2_state_family.py::test_update_memory_v3_self_referencing_superseded_by_defect`
- **observed vs expected**:
  - *Observed*: 调用 `store.update_memory_v3(mem_id, status="HISTORICAL", superseded_by=mem_id)` 成功写入，SQLite 中 `superseded_by` 为 `mem_id`。当后续语义检索 `memory_search` 命中该记忆时，沿着 `superseded_by` 指针无限循环重定向。
  - *Expected*: 抛出 `ValueError("INVALID_ARGUMENT: superseded_by must not reference the same memory or its shard family")`。
- **root_cause**: `change_memory_status` 与 `apply_status_change` 均显式调用了 `self._validate_superseded_target(cursor, rows, superseded_by)`；但在 `update_memory_v3` 的状态更新分支中，仅做 `sup_by = superseded_by or row["superseded_by"]` 后直接写入 SQL。由于 SQLite 外键约束 `REFERENCES memories(memory_id)` 在自身行存在时能够自满足，导致自指绕过了所有上层业务校验。
- **status**: 已复现（真实 Bug）
- **proposed_fix**:
  在 `src/core/session_store.py` 的 `update_memory_v3` 中补齐校验：
  ```python
  if new_status == "HISTORICAL" and superseded_by:
      self._validate_superseded_target(cursor, [row], superseded_by)
  ```

---

### [Medium] F-R2-03: `execute_idempotent` 对非字典/None 返回值的重放抛出 `TypeError` 崩溃

- **where**: `src/core/session_store.py:1606-1607` (`execute_idempotent`)
- **what**: 被包装业务函数返回 `None` 或基础数据类型时，第二次使用同 key 同 digest 重放时直接引发 `TypeError`。
- **repro**: `tests/audit_r2_transactions.py::test_idempotent_non_dict_response_replay_defect`
- **observed vs expected**:
  - *Observed*: 首次执行成功返回 `None`，底表写入 `response_json='null'`；重放时读出 `row["response_json"]`，执行 `{**None, "idempotent_replay": True}`，触发 `TypeError: 'NoneType' object is not a mapping` 崩溃。
  - *Expected*: 重放应当正常返回原响应（或包装成字典）并附带 `idempotent_replay=True`，绝不因解包操作崩溃。
- **root_cause**: 代码假定 `row["response_json"]` 反序列化后一定是一个字典对象，使用 `{**resp_data, "idempotent_replay": True}` 进行解包合并。而当业务逻辑返回 `None` 时，`json.loads('null')` 为 `None`，对非映射对象解包在 Python 中抛出 `TypeError`。
- **status**: 已复现（真实 Bug）
- **proposed_fix**:
  在 `src/core/session_store.py` 的 `execute_idempotent` 重放分支中增加类型判断：
  ```python
  resp_raw = json.loads(row["response_json"]) if row["response_json"] else {}
  if isinstance(resp_raw, dict):
      return {**resp_raw, "idempotent_replay": True}
  return {"result": resp_raw, "idempotent_replay": True}
  ```

---

### [Medium] F-R2-04: `locked_write_transaction` 缺乏 SQLite 嵌套重入计数，同线程嵌套导致外层报错与半写

- **where**: `src/core/session_store.py:1523-1546` (`locked_write_transaction`)
- **what**: 同一线程内嵌套调用 `locked_write_transaction` 时，内层上下文退出时提前 `COMMIT`，导致外层事务退出时抛出 `cannot commit - no transaction is active`。
- **repro**: `tests/audit_r2_transactions.py::test_locked_write_transaction_nesting_defect`
- **observed vs expected**:
  - *Observed*: 外层事务退出抛出 `sqlite3.OperationalError: cannot commit - no transaction is active`；若内层退出后外层继续执行了其他 SQL，那些语句将处于 autocommit 状态，一旦后续异常将无法回滚内层语句，原子性遭到破坏。
  - *Expected*: 上下文管理器应支持重入深度计数（或 SQLite `SAVEPOINT`），内层进入为 no-op，仅在最外层退出时执行真正 `COMMIT`。
- **root_cause**: 线程级互斥锁 `self._lock` 使用了 `threading.RLock()` 允许线程重入；但底层连接并未维护事务深度计数器，内层无条件执行了 `conn.execute("COMMIT")`。
- **status**: 已复现（真实 Bug / 架构缺陷）
- **proposed_fix**:
  维护连接/实例的嵌套事务深度 `_tx_depth`：
  ```python
  @contextmanager
  def locked_write_transaction(self):
      with self._lock:
          conn = self._get_connection()
          if not hasattr(self, "_tx_depth"):
              self._tx_depth = 0
          is_root = (self._tx_depth == 0)
          self._tx_depth += 1
          old_isolation = conn.isolation_level
          try:
              if is_root:
                  conn.isolation_level = None
                  conn.execute("BEGIN IMMEDIATE")
              yield conn
              if is_root:
                  conn.execute("COMMIT")
          except Exception:
              if is_root:
                  try:
                      conn.execute("ROLLBACK")
                  except Exception:
                      pass
              raise
          finally:
              self._tx_depth -= 1
              if is_root:
                  conn.isolation_level = old_isolation
  ```

---

### [Low] F-R2-05: 目标记忆 `conflicts_with` 为合法非列表 JSON（如 int/dict）时导致声明方创建/更新崩溃

- **where**: `src/core/session_store.py:2360-2365` (`create_memory_v3`) 与 `src/core/session_store.py:2619-2624` (`update_memory_v3`)
- **what**: 解析目标记忆的 `conflicts_with` 字段时缺少 `isinstance(t_conflicts, list)` 守卫，遇到非列表 JSON（如 `'123'` 或 `"{}"`）时抛出 `TypeError` 或 `AttributeError` 中断写入。
- **repro**: `tests/audit_r2_integrity_capacity.py::test_json_fields_error_tolerance_and_non_list_crash_defect`
- **observed vs expected**:
  - *Observed*: 目标行 `conflicts_with='123'` 时，调用 `create_memory_v3` 抛出 `TypeError: argument of type 'int' is not iterable`；存入 `"{}"` 时抛出 `AttributeError: 'dict' object has no attribute 'append'`。
  - *Expected*: 解析结果非列表时自动安全降级为初始空列表 `[]`，容错完成双向关系打标。
- **root_cause**: 代码仅通过 `try: t_conflicts = json.loads(...) except Exception: t_conflicts = []` 防御格式语法错误，未防御 JSON 数据类型不匹配。
- **status**: 已复现（真实 Bug）
- **proposed_fix**:
  ```python
  try:
      t_conflicts = json.loads(t_row["conflicts_with"] or "[]")
      if not isinstance(t_conflicts, list):
          t_conflicts = []
  except Exception:
      t_conflicts = []
  ```

---

### [Low] F-R2-06: `wal_checkpoint(mode)` 缺少参数白名单校验

- **where**: `src/core/session_store.py:1521` (`checkpoint`)
- **what**: `checkpoint` 接口直接使用 f-string 插值 `mode` 执行 PRAGMA 命令，缺乏白名单校验。
- **repro**: `store.checkpoint("UNKNOWN_MODE")`
- **observed vs expected**:
  - *Observed*: 允许任意字符串传入 `PRAGMA wal_checkpoint(...)`。
  - *Expected*: 校验 `mode.upper() in {"PASSIVE", "FULL", "RESTART", "TRUNCATE"}`，非法时抛出 `ValueError`。
- **root_cause**: `cursor.execute(f"PRAGMA wal_checkpoint({mode});")` 未校验入参。
- **status**: 已复现（轻微隐患）
- **proposed_fix**:
  ```python
  valid_modes = {"PASSIVE", "FULL", "RESTART", "TRUNCATE"}
  if mode.upper() not in valid_modes:
      raise ValueError(f"Invalid checkpoint mode: '{mode}'. Must be one of {valid_modes}")
  ```

---

## 3. 测试假设偏差与说明 (Not a Product Bug)

在审计用例开发过程中，记录了如下 1 处“测试假设错误与行为澄清”：

- **Worker 单轮指标采样时机 (Outbox Backlog Gauge Timing)**：
  - *测试初设假设*：认为 `await worker.run_once()` 返回后，`METRICS.snapshot()["gauges"]["outbox_backlog"]` 会立即反映当前轮次处理 10 条之后的剩余队列长度（即从 200 变为 190）。
  - *实际产品行为*：`OutboxWorker.run_once()` 在每轮开始时最先调用 `count_projection_backlog()` 采样指标，随后才拉取任务进行处理。因此在第一轮中，Gauge 记录的是该轮开始时的 200.0；当第二轮执行时，采样指标立即刷新为 190.0。
  - *结论*：符合设计，非产品缺陷。测试已按实际采样周期调整断言。

---

## 4. 已验证的攻击面与契约

本轮红队测试共构建 **35 项高强度独立测试**，覆盖全部关键承诺：

1. **事务原子性与故障回滚 (Atomicity)**：
   - 创建/更新/删除中途注入异常（Outbox 入队失败、审计日志表写入失败、触发器注入异常）→ 主表 `memories`、版本快照 `memory_revisions`、审计日志 `memory_audit_log`、发件箱 `memory_projection_outbox` 全部干净回滚，0 半写、0 孤儿数据。
   - 幂等业务函数失败 → 事务回滚带动 `idempotency_keys` 记录一并回滚，不烧 key，支持客户端直接用原 key 干净重试。
   - 多线程并发写操作通过 `locked_write_transaction`（排他锁 + `BEGIN IMMEDIATE`）严格串行化，无死锁。

2. **版本号单调性与 CAS 乐观锁 (CAS & Concurrency)**：
   - 验证所有写路径（`create_memory_v3`、正文变更、类型变更、状态变更、软删除 `DELETED`、双向冲突联动）版本号严格单调 +1。
   - Worker 投影处理窗口内模拟外部并发新写入：
     - Worker 提交 `mark_projection_done(claim=1)` 时 CAS 命中 0 行，任务保持 `pending` 并保留新版本 `projection_revision=2`，在下一轮自动按最新态重投，更新不丢失。
   - 失败分支 CAS 隔离：Worker 报告旧版本失败时，`mark_projection_failed(claim=1)` 返回 0 行，新版本的 `retry_count` 与 `next_retry_at` 未受污染。
   - 死信流转阈值：重试未满 8 次前拒绝进入死信；满 8 次后成功转入 `dead_letter`。
   - 死信 replay 语义：以当前最新版本重新入队生成/合并 `pending` 任务，原死信行标记 `status='done', last_error='absorbed by pending#...'`。
   - 部分唯一索引 `ux_outbox_pending_key`：同一集合同 ID 仅存在一条 pending 任务（UPSERT 自动合并）；跨 collection 互不干扰。

3. **子进程物理崩溃与恢复 (Crash Recovery)**：
   - 子进程在 `BEGIN IMMEDIATE` 后写入半截数据并使用 `os._exit(42)` 强退（模拟 SIGKILL）：主进程重连后 WAL 自动丢弃未提交事务，`integrity_check` 为 `'ok'`，0 半写。
   - Qdrant 写入成功后、SQLite `mark_projection_done` 前子进程强退：重启 Worker 后重试该任务，Qdrant 点位幂等覆盖，SQLite 成功标记 done，达成最终一致。
   - 故障发生后、写回 `mark_projection_failed` 前子进程强退：任务保留在 `pending` 状态，下一次安全重试。

4. **Worker 故障与队列排水 (Worker Fault Tolerance)**：
   - `engine.embed` 抛异常/超时：Worker 安全捕获不崩溃主循环，记录 `outbox_failures` 指标，自动设置指数退避。
   - Qdrant upsert / delete 抛异常：自动进入重试退避。
   - 持续故障下由于 `next_retry_at > now`，fetch 过滤生效，不发生紧凑死循环热轮询。
   - 旧队列排水状态机：遗留在旧队列中的 v3 记忆任务自动补投新队列；delta patch/delete 正常执行；遇到 Qdrant point missing 时标记 synced 容错归档，不卡队列；被取消 (synced) 任务绝不复活；failed 任务合法重试。
   - 单执行者 flock 排他性：两个独立 Worker 进程竞争同一锁文件，第二进程以退出码 `3` 退出并输出关键日志；持有锁进程释放后后续进程可正常启动。

5. **数据完整性与容量上限 (Integrity & Capacity)**：
   - 恶意 SQL 注入片段、单双引号、分号、NUL 字节 `\x00`、多字节 UTF-8 emoji 完整参数化存储回读无损。
   - 正文长度精确卡位：4 字符拒绝、16,000 字符成功、16,001 字符拒绝。
   - 非默认 collection 全链路路由：SQLite memories、outbox 目标集合及 Qdrant 写入严格对齐。
   - 同一实体批量 200 次更新：版本号升至 201，outbox 队列合并为 1 条任务，Worker 1 次同步追平。
   - 200 独立实体批量入队：Worker 单轮处理严格限制在 `batch_size=10`，剩余 190 条留在队列；`outbox_lag_seconds` 随时间推进准确反映最老积压年龄。

---

## 5. 未覆盖 / 无法验证范围

1. **真实 GPU / BGE-M3 模型推理崩溃**：根据红队审计通用规则，环境严禁加载真实模型与占用 GPU，所有向量生成由 FakeEngine / 规则向量替代；未测试真实 PyTorch/CUDA OOM 信号。
2. **生产环境远端 Qdrant 网络断开 (TCP RST / Keepalive Timeout)**：测试基于本地磁盘与内存 Qdrant 客户端，远端网络抖动通过 mock/monkeypatch 异常注入模拟。
3. **百万级大表 SQLite 写入延迟与 WAL 文件膨胀极限**：测试验证至 200 批量并发与合并容量，更长时间的 WAL 自动 checkpoint 压力测试受本地测试时间约束未长期挂机。

---

## 6. 测试文件清单与运行结果

### 审计测试文件清单
- `tests/audit_r2_transactions.py` (7 tests)
- `tests/audit_r2_revision_cas.py` (6 tests)
- `tests/audit_r2_crash_recovery.py` (3 tests)
- `tests/audit_r2_worker_faults.py` (7 tests)
- `tests/audit_r2_state_family.py` (6 tests)
- `tests/audit_r2_integrity_capacity.py` (6 tests)

### 运行结果统计

1. **独立运行红队测试套件**：
   ```bash
   PYTHONPATH=$PWD /home/dj/WorkSpaces/ai-memory-runtime/.venv/bin/python -m pytest tests/audit_r2_*.py -q
   ```
   **结果**：`35 passed, 1 warning in 31.42s`

2. **全量运行回归测试套件（含原有用例）**：
   ```bash
   PYTHONPATH=$PWD /home/dj/WorkSpaces/ai-memory-runtime/.venv/bin/python -m pytest tests/audit_r2_*.py tests/test_*.py -q
   ```
   **结果**：`371 passed, 6 skipped, 7 warnings in 74.09s`
   （原 336 用例无一修改且全部保持 PASS，新设 35 项红队测试全部 PASS）
