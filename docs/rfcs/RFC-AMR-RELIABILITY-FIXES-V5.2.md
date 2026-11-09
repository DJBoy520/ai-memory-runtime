# RFC v5.2 增量修订（回应第四轮评审 20 条；v5/v5.1 其余内容维持，冲突以本文件为准）

- 状态: v5.2（2026-11-08；第四轮 blocker 含多项 SQLite 实测复现，编排方抽查 4 条全部属实）
- 规范输入: RFC v5 + V5.1 + `docs/rfcs/GPT-AUDIT-RELIABILITY.md`；本轮新增外部审计第二轮四问的**闭环核查**发现

## 0. v5.2 决策表（20 条 → 处置）

| # | 发现（证据） | v5.2 处置 |
| --- | --- | --- |
| F1/F11 | 幂等占位 INSERT 先于业务，`memory_id/response_json` NOT NULL 实测炸；create 的 memory_id 业务内才生成，最终 UPDATE 漏写；store 各方法自带 commit，包装器需事务感知+持锁 | §2：DDL 改 `memory_id TEXT`、`response_json TEXT` **可空**（占位阶段为 NULL），最终 UPDATE **同时写 memory_id 与 response_json**；包装器拥有 BEGIN IMMEDIATE/COMMIT，business_fn 只拿 cursor 不自提交；整个事务持 store 现有 `_lock`；并发测试改经 store API（线程级，覆盖共享单连接） |
| F2 | 迁移分类在 ADD COLUMN 前读新列 | §3 步骤顺序固化：PRAGMA 探测 → 先 ALTER memories 两列+projection_class 回填 → 再分类旧表 |
| F3 | legacy 受控路径单 point 操作，多分片查空/无法级联 | §4：patch 解析**分片族**（root_memory_id 归组，无根则单行），delta 任务携带全族 (collection, point_id, memory_id) 列表，Worker 级联应用 |
| F4 | `DEFAULT 'legacy'` 误分类存量 v3 行（522 HISTORICAL 只能是 v3 产物：legacy 合法状态仅小写 4 态、v3 禁止直建 HISTORICAL） | §3 **存量分类回填**：有 memory_revisions 行 ∨ status 属大写规范集 ∨ source_refs 非空 → 'v3'；其余保持 'legacy' 并出 inventory 报告；歧义行不猜，人工确认入口留出 |
| F5 | 受控 patch 丢 superseded_by/superseded_at/deleted_*/审计；dispatch 无 superseded_by 参 | §4：patch 携带**完整字段集**（status+superseded_by+superseded_at+deleted_at/deleted_by/deletion_reason）写 memories 行 + memory_audit_log；dispatch_status_change 签名带 superseded_by/reason |
| F6 | 取消老任务只覆盖 pending；failed(retry<5) 仍会被取、in-flight 不受影响；直写后老快照仍可回写 | §4：受控路径**改为"仅入队"**（零 Qdrant I/O）：取消 `status IN ('pending','failed')`；Worker 写 Qdrant 前重查任务仍为 pending（被取消即跳过）；mark done 加 `AND status='pending'` |
| F7 | legacy 非默认 collection 未落库；旧队列无 collection 维度；patch 按 point id 无 collection | §4：memory_record 创建时落库 `collection_name`；delta 任务携带 collection；存量 legacy 行 collection 不可考→默认 ai_memory 并在 inventory 报告标注 |
| F8 | 旧队列也含 **v3 行任务**（create/update-DELETED 写旧队列）；旧 worker 重放会覆盖新投影/复活删除点；D3 论证不成立 | §4：旧队列排水**按 projection_class 路由**：v3 行任务→标 synced+注记 "superseded by v3 single-writer"，不重放；legacy 行任务→原语义；迁移门对旧队列同样盘点 v3 行任务数量 |
| F9 | outbox 迁移分类未查 projection_class，legacy 行任务会被转成 v3 丢字段 | §3：可转换判定加 `projection_class=='v3'`；legacy 行任务→隔离表（reason 注明） |
| F10 | quarantine 无独立提交即 raise 会随回滚丢失（实测 in_transaction 行为）；嵌套 BEGIN 报错；fresh-DB 无旧表 | §3：quarantine **独立事务先 COMMIT 再 raise**；INSERT OR IGNORE 按 (旧表行 id) 幂等；迁移前置 schema 状态探测（新 schema 直接 no-op，防 fresh-DB no such column） |
| F12 | reconciliation 已有 chunk 事务；dispatch 内嵌 BEGIN + Qdrant I/O 不可组合；Qdrant 故障阻塞写事务 | §4：拆出**纯函数 `apply_status_change(cursor, ...)`**（无 BEGIN/无 Qdrant/无 commit，可在调用方事务内组合）；受控路径仅入队（无 Qdrant I/O）后天然可组合；test_reconciliation 250 行 legacy→走旧队列（§7 改断言目标） |
| F13 | AMR_CONFIG_PATH 对 AMRApplication 无效（main.py 硬编码 config.yaml）；test_v3_e2e_integration 未列 §7；conftest 晚于模块级 load_config | §5：main.py 默认路径改为**尊重 AMR_CONFIG_PATH**（显式传参仍优先）；AMRApplication 增可选 DI（engine/qdrant_manager/session_store）供测试注入 fake；conftest 在**模块顶层**设 env（先于测试模块导入）；test_v3_e2e_integration.py 入 §7（live 双门控 + 弱一致性断言适配） |
| F14 | §7 漏 test_cognitive_distill:82-92、test_v2_engine:409,420-421、test_reconciliation:157 目标队列 | §6 补齐精确清单 |
| F15 | cognitive_engine/两脚本是隐藏生产者；v5"只盘点不改"与 v5.1"统一走 dispatch"冲突 | §5 裁决：`cognitive_engine.py`、`scripts/reconcile_ssot_drift.py`、`scripts/normalize_status_schema.py` **纳入 A 写轨所有权**（merge/forget 走 apply_status_change 分派；两脚本改走 store helper 或加废弃横幅）；read-your-writes 由 memory_get SSOT 承担，search 弱一致文档化 |
| F16 | conflicts_with 目标直写不递增/不入队（legacy 目标会让 v3 create 事务失败）；CAS 0 行时无法检测递增遗漏，任务永久 pending | §5：conflicts_with 目标走 dispatch 分派（v3 目标递增+新队列；legacy 目标 delta 任务）；Worker CAS 0 行时自愈：比较 outbox 行 revision 与 memories.projection_revision，不一致→WARN"疑似漏递增"并按当前值重新入队 |
| F17 | normalize 词表未定义；大小写敏感分支/phase_4/cognitive 比较会失效 | §5：memory_status.py 定义完整映射（active→ACTIVE、superseded→HISTORICAL、archived→HISTORICAL、deleted→DELETED）；`stale`/`candidate` 属 curation_candidates 词表（非 memories.status），核实后分词表管理；reconciliation/cognitive 的比较点在 A 轨内一并归一 |
| F18 | 故障矩阵缺崩溃类（仲裁后 COMMIT 前被杀）与成功 CAS/Qdrant 后崩溃等 | §7 测试矩阵补：子进程 kill（测试进程不真退）、成功 CAS 遇合并、Qdrant 成功后崩溃重做幂等、legacy failed/in-flight/非默认 collection/存量分类、v3-only 转换、fresh-DB、quarantine 幂等 |
| F19 | Executor(2)+wait_for 不能"饱和即跳过"（无界队列） | §5.2 改：非阻塞 `threading.Semaphore(2)` acquire(blocking=False)——拿不到立即 degraded（零排队）；degraded 判定=信号量不可得 ∨ wait_for 超时 ∨ 异常 |
| F20 | main.py:234 `except (KeyboardInterrupt, SystemExit): pass` 吞 SystemExit(3)，exit 0（实测确认） | §5：main.py 仅吞 KeyboardInterrupt，SystemExit 重抛（C 状态轨交付）；test_second_worker_refused 断言非零退出在 C 落地后生效 |

## 1. 幂等修正后 DDL 与事务（§2/§4 替换 v5.1 §4）

```sql
CREATE TABLE IF NOT EXISTS idempotency_keys (
    agent_id_raw TEXT NOT NULL, op TEXT NOT NULL, idem_key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    memory_id TEXT,              -- 占位阶段 NULL；最终 UPDATE 写入（create 由业务生成）
    response_json TEXT,          -- 占位阶段 NULL
    created_at INTEGER NOT NULL,
    PRIMARY KEY (agent_id_raw, op, idem_key)
);
```

```python
def write_with_idempotency(op, key, digest, business_fn):
    with store.locked_write_transaction() as con:        # BEGIN IMMEDIATE + 持 store._lock
        cur = con.execute(ARBITRATION_UPSERT, ..., cutoff=now - retention)
        if cur.rowcount == 0:
            row = con.execute("SELECT request_digest, response_json, memory_id FROM idempotency_keys WHERE ...").fetchone()
            if row["request_digest"] != digest:
                raise IdempotencyConflict(op, key, row["memory_id"])
            return {**json.loads(row["response_json"]), "idempotent_replay": True}   # 读路径无需 ROLLBACK（零写入）
        memory_id, result = business_fn(con)             # 业务变更+入队；con 内不自提交
        con.execute("UPDATE idempotency_keys SET memory_id=?, response_json=? WHERE agent_id_raw=? AND op=? AND idem_key=?",
                    (memory_id, json.dumps(result), ...))
        # with 退出时 COMMIT；异常时 ROLLBACK（key 一并回滚，业务失败不烧 key）
        return result
```
（读路径零写入时直接返回，不需显式 ROLLBACK 也能保持原子——统一由 locked_write_transaction 的退出语义保证。）

## 2. legacy 受控路径 v5.2（§4 替换 v5.1 §1 的 patch 段）

```python
def apply_status_change(cursor, row, new_status, superseded_by=None, reason=None):
    """纯函数：无 BEGIN / 无 Qdrant / 无 commit；可在调用方事务内组合（reconciliation chunk 事务兼容）"""
    canonical = normalize_status(new_status)
    fields = {"status": canonical, "updated_at": now}
    if canonical == "HISTORICAL" and superseded_by: fields["superseded_by"], fields["superseded_at"] = superseded_by, now
    if canonical == "DELETED":     fields.update(deleted_at=now, deleted_by=actor, deletion_reason=reason)
    UPDATE memories SET ... WHERE memory_id=?
    INSERT memory_audit_log (...)                                   # 审计不丢
    family = resolve_shard_family(row)                              # root_memory_id 归组，无根则单行
    for shard in family:                                            # 每个分片
        cancel: UPDATE qdrant_sync_queue SET status='synced', last_error='superseded by task #'||:new_id
                 WHERE memory_id=shard.memory_id AND status IN ('pending','failed') AND id < :new_id
        INSERT qdrant_sync_queue (..., op_type='update_payload',
               payload_snapshot=json({"patch": fields, "collection": shard.collection_name,
                                      "point_id": shard.qdrant_point_id, "memory_id": shard.memory_id}),
               status='pending', ...)

# dispatch_status_change(row, new_status, superseded_by=None, reason=None, cursor=None):
#   v3 行 → store 层 SQLite 写（同一 cursor 可组合）+ 新队列 helper
#   legacy 行 → apply_status_change（仅入队，零 Qdrant I/O）
```

Worker legacy 排水 v5.2：
```python
for task in fetch_pending_sync_tasks():
    row = recheck_task_row(task.id)                  # 写 Qdrant 前重查
    if row.status != 'pending': continue             # 已被取消/吸收 → 跳过
    m = memories_by_id(task.memory_id)
    if m and m.projection_class == 'v3':
        mark done WHERE id AND status='pending'      # CAS；注记 superseded by v3 single-writer；不重放
        continue
    if task.op_type == 'update_payload':             # delta 合并：读现 payload→补丁→upsert（collection 取任务携带值）
    else:                                            # 原快照重放（legacy）
```

## 3. 存量分类回填与迁移顺序（§3 替换 v5.1 §3）

```
_migrate_schema(cur):
  0. schema 探测：PRAGMA table_info(memory_projection_outbox)
     → 已是新 schema：no-op（fresh-DB 安全）
     → 无该表（全新库，CREATE_TABLES_SQL 已建新结构）：no-op
     → 旧 schema：继续
  1. ALTER memories ADD projection_revision / collection_name / projection_class（逐列判存在性，幂等）
  2. 分类回填：
     UPDATE memories SET projection_class='v3'
      WHERE (SELECT COUNT(*) FROM memory_revisions r WHERE r.memory_id = memories.memory_id) > 0
         OR status IN ('ACTIVE','HISTORICAL','PENDING_VERIFY','CONFLICT','TEMPORARY')
         OR (source_refs IS NOT NULL AND source_refs NOT IN ('', '[]'));
     -- 歧义行保持 'legacy'；inventory 报告输出各类行数，歧义行列表供人工确认（脚本 --inventory 可见）
  3. 旧 memory_projection_outbox 分类（含 projection_class=='v3' 门；legacy 行任务→quarantine）
  4. quarantine 独立事务 COMMIT 后再 raise MigrationBlocked（INSERT OR IGNORE 幂等）
  5. 同事务重建换表
```

## 4. 测试隔离与身份修正（§5）

- main.py：`config_path=None` 时尊重 AMR_CONFIG_PATH（显式传参优先不变）；`except` 只留 KeyboardInterrupt，SystemExit 重抛；AMRApplication 增可选 DI（engine/qdrant_manager/session_store）。
- conftest.py：模块**顶层**（先于测试模块导入）设 `os.environ["AMR_CONFIG_PATH"]=tmp_cfg` + XDG_RUNTIME_DIR→tmp + socket 守卫；tmp 配置的 qdrant 指向 127.0.0.1:1；AMRApplication 类测试用 DI 注入 fake（不经网络）。
- §6 测试适配清单补：`test_v3_contracts.py:214`（改 C 轨：DI 注入）、`test_v3_e2e_integration.py`（live 双门控+create→search 弱一致断言适配）、`test_cognitive_distill.py:82-92`、`test_v2_engine.py:409,420-421`、`test_reconciliation.py:157`（断言目标改旧队列/受控 delta 任务）。

## 5. 故障测试矩阵 v5.2 增补

子进程 kill（仲裁后 COMMIT 前；测试进程不真调 os._exit）｜成功 CAS 遇合并｜Qdrant 成功后崩溃重做幂等｜legacy failed/in-flight 延迟重试不覆盖｜legacy 非默认 collection 全链路｜存量分类回填（522 HISTORICAL→v3）｜v3-only 转换门｜fresh-DB 迁移 no-op｜quarantine 幂等+独立提交｜CAS 0 行自愈（漏递增检测）｜second_worker_refused 非零退出（C 落地后）。
