# RFC v5.1 增量修订（回应外部审计第二轮：4 阻塞项 + 3 修订项 + 2 处证据修正）

- 状态: v5.1（2026-11-08，仅覆盖本文件所列问题；RFC v5 其余内容全部维持，冲突处以本文件为准）
- 规范输入: `docs/rfcs/GPT-AUDIT-RELIABILITY.md`（不变）+ 外部审计第二轮意见（本文件即为回应）
- 用户指令: 先补 v5.1 回答四个问题（伪代码 + 故障测试），再按阶段实施

---

## 1. 问题一：如何识别 legacy 记忆，哪些路径允许修改它，如何防止两队列操作同一点位？

### 1.1 识别：落库能力类型，不靠调用入口推断

GPT 指出正确事实：`memory_record`（legacy，`memory_service.py:353-470`）**会把分片写入 memories 主表并保存 point id**，因此 legacy 记忆同样有 v3 生命周期入口（状态更新、治理流程），"按调用方判断归属"不成立。

**方案：`memories` 增加能力类型列，创建时落库、此后不可变**：

```sql
ALTER TABLE memories ADD COLUMN projection_class TEXT NOT NULL DEFAULT 'legacy'
    CHECK (projection_class IN ('v3', 'legacy'));
```

- `memory_create`（v3）是**唯一**写入 `'v3'` 的入口；
- `memory_record` 与全部存量行默认 `'legacy'`（保守：未知即 legacy）；
- 任何写路径的归属判断**只读该列**，与调用方无关。

### 1.2 路径治理：v3 链路对 legacy 行只读或拒绝；legacy 行的元数据变更走"受控 legacy 更新路径"

```python
def v3_write_guard(row):
    """memory_update / memory_delete / 统一入队 helper 的第一道校验"""
    if row["projection_class"] != "v3":
        raise LegacyPathForbidden(
            memory_id=row["memory_id"],
            projection_class=row["projection_class"],
            hint="legacy 记忆的元数据变更请走受控 legacy 路径；正文变更不支持",
        )   # IPC 映射 -32602 INVALID_ARGUMENT，data 携带 projection_class 与 hint（可诊断）

def dispatch_status_change(memory_id, new_status, actor):
    row = SELECT * FROM memories WHERE memory_id=?          # 必须存在
    if row["projection_class"] == "v3":
        store.update_memory_status(...)                      # SQLite 写 + 统一入队（新队列）
    else:
        legacy_controlled_status_patch(row, new_status, actor)

def legacy_controlled_status_patch(row, new_status, actor):
    """legacy 行的唯一安全变更路径：补丁既有 payload，绝不重建"""
    BEGIN IMMEDIATE
      payload = qdrant.retrieve_point(row["qdrant_point_id"]).payload   # 读失败→回滚并报错，不猜
      payload["status"]    = normalize_status(new_status)
      payload["updated_at"] = now
      new_id = INSERT INTO qdrant_sync_queue (..., payload_snapshot=json(payload), status='pending', ...)
      # 取消同点位更老的 pending 旧任务：延迟重试不可能再覆盖本次补丁
      UPDATE qdrant_sync_queue SET status='synced',
             last_error='superseded by task #' || :new_id
       WHERE memory_id=? AND status='pending' AND id < :new_id
      qdrant.upsert(row["qdrant_point_id"], payload)                   # 保持 legacy 直写语义
      UPDATE memories SET status=payload["status"], updated_at=:now WHERE memory_id=?
    COMMIT
```

- **正文变更（memory_update content）对 legacy 行：一律拒绝**（LegacyPathForbidden）——payload 重建不安全，不提供受控路径；
- `reconciliation`、`cognitive forget` 等治理路径统一走 `dispatch_status_change` 分派；
- 两队列点位隔离由此闭合：新队列 helper 内断言 `projection_class=='v3'`（同事务），legacy 链路永不入新队列，legacy 行的旧队列延迟重试被受控路径的取消语义封死。

### 1.3 故障测试（对应 GPT 验收场景"legacy 创建 → v3 状态更新或删除 → 旧队列延迟重试"）

| 测试 | 断言 |
| --- | --- |
| `test_legacy_v3_update_rejected` | memory_record 创建 → `memory_update(content)` → 抛 LegacyPathForbidden；memories 行与 Qdrant payload 零变化 |
| `test_legacy_v3_delete_rejected` | 对 legacy 行 `memory_delete` → 同样拒绝 |
| `test_legacy_stale_retry_cannot_overwrite` | legacy 创建（含 tags/chunk_index）→ 旧队列先有 pending 老快照任务 → 受控路径改状态（取消老任务）→ Worker 排水 → 最终 payload 状态为新值且 legacy 字段完整 |
| `test_legacy_delayed_retry_after_rejected_v3` | legacy 创建 → 尝试 v3 更新（被拒）→ 旧队列延迟重试原快照 → 最终状态与全部 legacy 字段不变 |

---

## 2. 问题二：如何强制单投影执行者？成功与失败分支如何分别做 revision CAS？

### 2.1 单执行者强制（OS 级，非文档约定）

```python
# daemon 启动（outbox worker 初始化）与独立脚本共用同一锁文件
lock_fd = open(XDG_RUNTIME_DIR / "amr-outbox.lock", "w")
try:
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError:
    logger.critical("another outbox worker holds amr-outbox.lock; refusing to start")
    raise SystemExit(3)          # daemon 直接启动失败（systemd Restart 会重试）；脚本拒绝执行
```

- 覆盖：daemon、手动脚本、重启交叠（systemd `RestartSec=3s` 先停后起 + flock 兜底，即使交叠第二个也拒绝）；
- 明确边界：本方案只覆盖**单机**（当前部署形态）；多机/多实例需租约+fencing，明确不在本次范围（RFC v5 已声明）。

### 2.2 全分支 revision CAS（成功、失败、退避、死信都以 claim revision 记账）

```python
def process_v3_task(task):
    claim = read_state_and_revision(task.memory_id)      # 同一读事务，claim.revision 取自 memories

    try:
        project(claim.state)                             # embed + qdrant upsert / delete
    except Exception as e:
        # 失败分支 CAS：新 revision 已被合并刷新时（0 行），绝不给它记账
        cur = execute("""
            UPDATE memory_projection_outbox
               SET retry_count = retry_count + 1, next_retry_at = :backoff,
                   last_error = :err, updated_at = :now
             WHERE id = :id AND status = 'pending' AND projection_revision = :claim""", ...)
        if cur.rowcount == 1:
            rc = read_retry_count(task.id)
            if rc >= 8:
                execute("""UPDATE memory_projection_outbox
                              SET status='dead_letter', dead_letter_at=:now
                            WHERE id=:id AND status='pending' AND projection_revision=:claim""", ...)
        # cur.rowcount == 0 → 合并 helper 已把该行刷成新 revision（retry_count=0, next_retry_at=NULL）
        return

    cur = execute("""
        UPDATE memory_projection_outbox
           SET status='done', projected_revision=:claim, updated_at=:now
         WHERE id=:id AND status='pending' AND projection_revision=:claim""", ...)
    if cur.rowcount == 0:
        pass   # 处理期间发生新写入：任务保持 pending，下轮以最新态重投（不丢更新）
    recheck_and_reenqueue_if_needed(claim)               # 兜底复查
```

- 失败记账永不落到比 claim 新的 revision 上（合并 helper 已把新 revision 的 retry_count 归零、退避清空）；
- **一致性边界（如实声明，写进接口文档与上线手册）**：最终一致依赖三条件——写入最终停止、Worker 持续运行、下游恢复；持续高频写入允许投影暂时落后（观测 `outbox_lag_seconds`），持续故障进入**可见死信**（可查询/可重放），不承诺无条件最终一致。

### 2.3 故障测试

| 测试 | 断言 |
| --- | --- |
| `test_failure_cas_spares_merged_revision` | v=5 处理失败瞬间合并刷新至 v=6 → 该行 retry_count 未增、next_retry_at 仍 NULL |
| `test_dead_letter_never_for_stale_revision` | v=5 连续失败逼近 8 次，第 7 次失败后合并刷新 → 不置死信，行保持 pending 且计数已重置 |
| `test_second_worker_refused` | 两进程抢 `amr-outbox.lock` → 第二个拒绝启动（exit 3） |
| `test_lag_visible_under_sustained_failure` | 持续高频写 + Qdrant 故障 → outbox_lag_seconds 单调可见增长，最终进入死信且可列出 |

---

## 3. 问题三：旧投影表存在非空任务时，哪些能迁移、哪些必须阻止上线？

### 3.1 分类迁移门（重建前的强制分类，"0 行"只是当前生产快照、不是算法前提）

```python
def _migrate_outbox_schema(cur):            # _migrate_schema 内，幂等
    rows = cur.execute("SELECT * FROM memory_projection_outbox").fetchall()   # 旧表
    convertible, quarantine = [], []
    for r in rows:
        m = cur.execute("SELECT * FROM memories WHERE memory_id=?", (r["memory_id"],)).fetchone()
        if m is not None and m["qdrant_point_id"]:
            # 可证明能从 SQLite 完整重建：行存在 + point id 已知 → 转 v3 合并任务
            convertible.append((m["collection_name"], r["memory_id"], m["projection_revision"]))
        else:
            # 行缺失 / point 未知 / 语义不可推导 → 不可自动转换
            quarantine.append((r, reason))
    if quarantine:
        create_quarantine_table_and_copy(cur, quarantine)      # memory_projection_outbox_quarantine（全列+reason+observed_at）
        write_rebuild_report(convertible, quarantine)
        raise MigrationBlocked(len(quarantine), report_path)   # 安全失败：拒绝自动切换，绝不吞任务
    # 全部可转换（或空表）才允许切换，且同事务完成：
    cur.execute("BEGIN")
    cur.execute("CREATE TABLE memory_projection_outbox_new (...)")   # v5 schema
    for c in convertible:
        cur.execute("INSERT INTO memory_projection_outbox_new ... ON CONFLICT(...) DO NOTHING", c)
    cur.execute("DROP TABLE memory_projection_outbox")
    cur.execute("ALTER TABLE memory_projection_outbox_new RENAME TO memory_projection_outbox")
    cur.execute("CREATE UNIQUE INDEX ..."); cur.execute("CREATE INDEX ...")
    cur.execute("COMMIT")                   # 任何异常→整体回滚，旧表原样
    write_rebuild_report(convertible, [])
```

- 重复执行：以 schema 状态判定，已迁移则 no-op；回滚路径 = 事务回滚后旧表完好（预演验证）；
- 上线手册写明：迁移报告必须人工确认（尤其 quarantine 非空时）才可继续部署；
- 当前生产快照 0 行 → convertible=[] → 直接重建；算法对一般情形成立，副本演练覆盖。

### 3.2 故障测试

| 测试 | 断言 |
| --- | --- |
| `test_rebuild_with_orphan_task_blocks` | 旧表插入 memories 无行任务 → MigrationBlocked，旧表原样，隔离表有行+reason，报告生成 |
| `test_rebuild_converts_convertible_task` | 旧表任务对应 v3 行 → 转为新队列 pending（revision=当前值），报告记录转换清单 |
| `test_rebuild_idempotent_and_rollback` | 二次执行 no-op；注入故障回滚 → 旧表结构与数据完好 |

---

## 4. 问题四：幂等仲裁、业务变更、响应快照的事务顺序是什么？

### 4.1 单事务顺序（仲裁最先，业务失败连 key 一起回滚）

```python
def write_with_idempotency(con, op, key, digest, business_fn):
    con.execute("BEGIN IMMEDIATE")                       # 写事务以仲裁开始，串行化并发写者
    cur = con.execute("""
        INSERT INTO idempotency_keys (agent_id_raw, op, idem_key, request_digest, memory_id, response_json, created_at)
        VALUES (?,?,?,?,?,?,?)
        ON CONFLICT(agent_id_raw, op, idem_key) DO UPDATE
           SET request_digest=excluded.request_digest, memory_id=excluded.memory_id,
               response_json=excluded.response_json, created_at=excluded.created_at
         WHERE created_at < :cutoff""", ..., cutoff=now - retention)   # 过期键原子覆写
    if cur.rowcount == 0:
        # 未过期 key 已存在：本事务零业务写入，回读仲裁
        row = con.execute("SELECT request_digest, response_json, memory_id FROM idempotency_keys WHERE ...").fetchone()
        con.execute("ROLLBACK")
        if row["request_digest"] == digest:
            return {**json.loads(row["response_json"]), "idempotent_replay": True}
        raise IdempotencyConflict(op=op, idem_key=key, existing_memory_id=row["memory_id"])   # -32006
    # rowcount == 1：新占位或过期覆写，本事务独占该 key
    try:
        result = business_fn(con)                        # 业务变更 + revisions + 审计 + 入队（同一事务）
        con.execute("UPDATE idempotency_keys SET response_json=:snap WHERE agent_id_raw=? AND op=? AND idem_key=?",
                    json.dumps(full_result_with_memory_id_and_projection_status))
        con.execute("COMMIT")                            # key + 业务 + 队列 + 快照 原子提交
        return result
    except:
        con.execute("ROLLBACK")                          # 业务失败→key 一并回滚，重试可干净重执行
        raise
```

- 关键性质：**先仲裁、再业务、后快照、最后 COMMIT**；仲裁失败分支零写入；业务异常连 key 一起回滚（业务失败不烧 key，客户端修正参数后可用同 key 重试）；
- 并发：SQLite 单写锁 + `BEGIN IMMEDIATE` 串行化，两个连接同 key 同摘要→恰一次提交+另一方重放；异摘要→后者 -32006；
- 首次事务提交前崩溃→整体回滚，无 key 行、无业务行、无队列行；
- 接口文档明确：保留期届满同 key 重新执行；重放快照中的 `pending` 指首次提交时点，当前投影状态用独立查询获取。

### 4.2 故障测试

| 测试 | 断言 |
| --- | --- |
| `test_idempotent_business_failure_rolls_back_key` | 业务抛 VERSION_CONFLICT → key 行/业务行/队列行全部不存在；修参后同 key 重试完整重执行 |
| `test_concurrent_same_key_serialized` | 两连接同 key：同摘要→恰一次业务提交+一次重放；异摘要→一次提交+一次 -32006 |
| `test_expired_key_reexecutes` | key 过期后同 key 新请求 → 原子覆写占位并完整重执行（不报冲突、不重放旧响应） |
| `test_cleanup_concurrent_with_arbitration` | 清理 DELETE 与仲裁并发 → 无 IntegrityError、语义不变 |

---

## 5. 三项修订

### 5.1 Watchdog 门控与循环重启告警（修正 v5 的 os._exit 表述）

- `os._exit(1)` 仅作最后恢复手段，且**仅在 daemon（systemd 托管）模式启用**：新配置 `model.watchdog_exit_enabled`（默认 **false**；生产 `config.yaml` 显式 true 并注释适用条件）；
- 退出函数注入（`exit_fn`），单测不真退测试进程；受控退出路径用**子进程测试**验证；
- 循环重启告警：启动时检查 `XDG_RUNTIME_DIR/amr.crash.count`（时间戳窗口），10 分钟内 ≥3 次 → CRITICAL "repeated watchdog exits"；上线手册建议 systemd 单元补 `StartLimitIntervalSec=300` / `StartLimitBurst=5`；
- 声明修正：watchdog **不保证**下游可用或系统整体高可用，只保证卡死可见与受控恢复。

### 5.2 memory_get 富化工程约束

- 富化查询跑在**专用** `ThreadPoolExecutor(max_workers=2)` + `asyncio.wait_for(..., 0.2s)`：Qdrant 阻塞不堆积（最多 2 线程在途，超出直接跳过富化），不用默认 executor；
- 响应显式新增 `enrichment: "ok" | "degraded"` 字段，degraded 时缺失字段置 null——调用方可判断哪些字段缺；
- 富化只填充 SQLite 不存在的 legacy 键，**绝不以 payload 值覆盖 SQLite 权威字段**；
- 措辞修正：v5"memory_get 永远成功"改为——**SSOT 路径对 Qdrant 故障免疫**；SQLite 自身故障或无行时如实走对应路径/报错，不宣称无条件成功。

### 5.3 测试统一防线（不逐个补丁，入口设防）

- `tests/conftest.py` 会话级守卫（A 写轨交付）：
  1. 默认注入 `AMR_CONFIG_PATH` → 自动生成的 tmp 配置（tmp sqlite + qdrant 指向 `127.0.0.1:1` 关闭端口）；
  2. socket 连接守卫：对 `/run/user/1000/*.sock` 的 connect 直接 raise（防触碰运行中 daemon）；
  3. `test_isolation_guard.py`：校验守卫生效（默认配置不在生产路径、生产 socket 连接被拒）；
- live 用例双重门控：`AMR_TEST_LIVE_DAEMON=1` **且** `AMR_TEST_DAEMON_SOCKET` 显式指向隔离 socket 才允许连接；仅设 env 不自动授权连生产；
- 已核实 `settings.load_config`（`config/settings.py:125-131`）真实读取 `AMR_CONFIG_PATH`，非测试一厢情愿。

---

## 6. 证据表述修正（审计文档两处）

1. 原"模型冷启动故障占比 >80%"缺分母与统计方法 → 修正为可数事实："近 7 天 journald 中 `ModelLoadingTimeoutError` 7 次、`Client disconnected before response could be sent` 40 次"；
2. 原"VERSION_CONFLICT 即假失败"过度推断 → 修正为："该日志证明发生了版本冲突；'假失败'的判定依据是源码链路分析（`memory_service.py:728-769` 先提交后同步 embed），日志本身不构成独立证明"。

---

## 7. 验收补充（并入 §5 映射）

GPT 指定的验收场景 ↔ 测试：legacy 双链路（§1.3 四用例）、CAS 全分支（§2.3 四用例）、迁移分类门（§3.2 三用例）、幂等事务序（§4.2 四用例）、watchdog 子进程验证、富化降级字段、conftest 防线自检。

## 8. 表述纪律

后续所有文档与报告不得把"彻底切断""绝对杜绝"作为已证明结论使用；一律以"在本方案边界内（单机、单 Worker、v3/legacy 能力类型不可变）成立"的方式表述，并列出边界外未覆盖项。
