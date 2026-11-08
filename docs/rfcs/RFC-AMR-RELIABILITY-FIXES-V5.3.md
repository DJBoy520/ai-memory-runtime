# RFC v5.3 增量修订（回应第五轮评审 11 条；gemini 已 approve，本轮闭合 deepseek 发现）

- 状态: v5.3（2026-11-08；优先级 v5.3 > v5.2 > v5.1 > v5）
- 本轮 11 条全部集中在 v5.2 新引入的 legacy/旧队列细节，逐条处置如下

## 0. 决策表

| # | 发现（证据） | v5.3 处置 |
| --- | --- | --- |
| F1 | 取消语句无 op_type 条件，会把未排水的 create upsert 任务（旧队列唯一携带向量/正文的任务）一起标 synced → legacy 行状态变更后永久丢点位；test_v2_storage.py:260 的 3 条 pending 期望变 2/1 | §1 取消策略改为**分类处置**：同 memory 更老 pending/failed 任务（id < delta_id）——`upsert` → **原地合并补丁进其 payload_snapshot**（保向量/正文，应用新状态字段，不取消）；`update_payload` → 取消（superseded）；`delete` → 保留（删除意图优先，照常执行删除）。pending 计数期望不变 |
| F2 | 旧队列 v3 行任务被标 synced 后直接丢弃 → 切换时真实未完成投影永久丢失，违反审计 A3"排水不静默丢弃" | §2 v3 行任务排水 = **先经统一入队 helper 在新队列补一条等价重投影任务**（revision=memories 当前值），再标 synced + 注记 "migrated to outbox#id" |
| F3 | 排水状态机自相矛盾：fetch 会取 failed(retry<5) 但重查 `!= 'pending' → skip` 废掉重试；mark done 加 `AND status='pending'` 使 failed 成功后 0 行更新→死循环；mark_sync_task_failed 无状态条件→被取消的 in-flight 复活 | §2 状态机修正：重查仅跳过 `status='synced'`（被取消/吸收的行）；failed 是合法重试态不跳过；done/failed 终态更新条件统一 `AND status IN ('pending','failed')`（mark_sync_task_failed 同步加条件，被取消行不能复活）；单线程按 id 序排水 + delta 后置合并使 in-flight 竞态最终态正确 |
| F4 | delta 复用 op_type='update_payload' 与旧扁平行无法区分；"upsert 补丁"原语错误（delta 无向量） | §2 信封以**形态区分**：payload_snapshot 解析为含 `"amr_delta_v1"` 标记键的 JSON → delta 信封；否则旧扁平语义。写原语 = **set_payload**（部分字段，无需向量）；point 缺失 → done+注记 |
| F5 | 多分片 legacy 的 parent id 在 memories 无行（memory_record 生成 parent_chunk_i 行、root_memory_id=parent、返回 parent），dispatch "行必须存在" 在分片族解析前失败 | §3 分派入口先解析族：`WHERE memory_id=? OR root_memory_id=?`；命中分片行→按族 patch；两者皆空→NOT_FOUND（可诊断） |
| F6 | v3_write_guard 拒绝 legacy 的 memory_delete，但 memory.delete 是标准 MCP 工具且审计 A1 要求删除"提交即 pending"——兼容性回退 | §3 legacy 行 memory_delete **改走受控路径**（status=DELETED），delta 信封 op='delete' → Worker 物理删 point；`test_legacy_v3_delete_rejected` 更名为 `test_legacy_delete_via_controlled_path` |
| F7 | 迁移事务顺序不可实现：分类 DML 开隐式事务后显式 BEGIN 报错（实测）；步骤0只探测 outbox 表漏"有 memories 无 outbox"老库的加列；转换 INSERT 缺 NOT NULL 列（实测 created_at） | §4 迁移执行统一 `conn.isolation_level=None` + 显式 BEGIN…COMMIT 包全程；步骤0改为**逐表逐列探测**（memories 缺列即补，与 outbox 状态无关）；转换 INSERT 给**全列清单**（含 created_at/updated_at/task_class 等全部 NOT NULL 列） |
| F8 | "legacy 合法状态仅小写 4 态"不被代码保证：adopt_orphan_points.py:97 从 payload 原样取 status 直插 memories、reconcile_ssot_drift.py:80 写大写 ARCHIVED——分类回填会误判 v3；quarantine 阻塞时回填每次启动重跑，C2 大写化后全表误判；两迁移先后未规定 | §4/§5：回填改为**一次性迁移**，以 `_amr_migrations` 标记表（migration id）做幂等门（不再靠 schema 状态重入）；判定信号仅保留不随 C2 变化的证据（memory_revisions 存在 ∨ source_refs 非空 ∨ status∈大写规范集——标记门保证回填先于 C2 执行，上线手册固化顺序：outbox/分类迁移 → 状态枚举迁移）；`adopt_orphan_points.py`、`reconcile_ssot_drift.py` 纳入 A 轨所有权：adopt 时显式 stamp projection_class='legacy'，ARCHIVED 写入走 normalize |
| F9 | 比较点清点漏 src/cognitive/context_builder.py:72,86（`('active','candidate')`）、cognitive_engine.py:605,639,701、reconciliation:292,298 | §5：`src/cognitive/context_builder.py` 纳入 A 轨所有权；上述全部比较点经 normalize/大小写无关 helper 归一；对应测试断言入 §7 |
| F10 | bridge 压平 daemon error 丢 code/data（bridge.py:80-82,164-175），验收 #8 的 MCP 侧可区分业务码无法断言 | §5 C1 硬性化：bridge 透传 error.{code,message,data}（携带 data 的异常类型），往返测试断言 **VERSION_CONFLICT 的 data.current_version 经 MCP 端到端可见** |
| F11 | §7 与 v5.2 legacy 删除语义不一致（test_v2_engine:466-478 物理删除断言、test_v2_storage:237 op_type='delete'、215/230/260 未列）；且授权"改经 memory_get/断言目标改旧队列"超出用户四类适配白名单 | §6 §7 重写：legacy DELETED 语义随 F6 恢复（delete 任务物理删除，:466-478/:237 原样通过）；清单精确到 file:line 且**严格限定用户四类**（期望字符串/异常类型/夹具注入/隔离门控），撤回越权授权 |

## 1. 取消策略（替换 v5.2 §2 cancel 段）

```python
# 受控 legacy 状态变更：仅入队 delta（零 Qdrant I/O），同事务处理更老任务
new_id = INSERT INTO qdrant_sync_queue (..., op_type='update_payload',
           payload_snapshot=json({"amr_delta_v1": True, "op": "patch"|"delete",
                                  "fields": {...完整字段集...}, "collection": c,
                                  "targets": [(memory_id, point_id), ...]}), status='pending', ...)
for t in SELECT * FROM qdrant_sync_queue
          WHERE memory_id IN family_ids AND status IN ('pending','failed') AND id < :new_id:
    if t.op_type == 'upsert':
        snap = json.loads(t.payload_snapshot)             # 保向量/正文
        snap.update(delta.fields_for(snap))               # 就地应用新状态字段
        UPDATE qdrant_sync_queue SET payload_snapshot=json(snap), last_error='patched by #'||:new_id WHERE id=t.id
    elif t.op_type == 'delete':
        pass                                              # 保留：删除意图优先，照常物理删除
    else:                                                 # 旧扁平 update_payload
        UPDATE qdrant_sync_queue SET status='synced', last_error='superseded by #'||:new_id WHERE id=t.id
```

## 2. Worker legacy 排水 v5.3（替换 v5.2 §2 worker 段）

```python
for task in fetch_pending_sync_tasks():                  # pending + failed(retry<5)，按 id 序
    row = recheck(task.id)
    if row is None or row.status == 'synced': continue   # 仅跳过被取消/吸收的行；failed=合法重试
    m, family = resolve_memories(task.memory_id)         # memory_id=? OR root_memory_id=?
    if m and m.projection_class == 'v3':
        new_outbox_id = enqueue_new_queue(m)             # 等价重投影任务（不静默丢弃）
        mark_synced(task.id, note=f"migrated to outbox#{new_outbox_id}",
                    where_status_in=('pending','failed'))
        continue
    if is_delta_envelope(task.payload_snapshot):         # 含 amr_delta_v1 标记
        d = parse(task)
        for (mid, pid) in d.targets:
            if d.op == 'delete': qdrant.delete(pid, d.collection)
            else:
                try: qdrant.set_payload(pid, d.fields, d.collection)
                except PointMissing: note_done(...)
    else:                                                # 原快照重放（legacy 原语义）
        replay_legacy_snapshot(task)
    finalize: UPDATE ... SET status='done' WHERE id=:id AND status IN ('pending','failed')
    # 失败分支: mark_sync_task_failed 同加 WHERE status IN ('pending','failed')
```

## 3. 分派入口与 legacy 删除（替换 v5.1/v5.2 的 dispatch 前置校验）

```python
def dispatch_status_change(memory_id, new_status, superseded_by=None, reason=None, cursor=None):
    rows = cursor.execute("SELECT * FROM memories WHERE memory_id=? OR root_memory_id=?",
                          (memory_id, memory_id)).fetchall()
    if not rows:
        raise NotFound(memory_id)                        # 可诊断；parent-only id 也能命中族
    if any(r["projection_class"] == 'v3' for r in rows):
        # 混合族不允许：v3 行与 legacy 行不共享族；以首行类为准并校验全族一致，不一致报 INVALID_ARGUMENT
        v3_write_guard(rows); store_update_status(cursor, rows, ...)   # 新队列
    else:
        apply_status_change(cursor, rows, ...)           # §1 仅入队 delta；memory_delete 同通道（status=DELETED）
```

## 4. 迁移执行修正（替换 v5.2 §3 步骤 0/5 与转换 INSERT）

- 全迁移在 `conn.isolation_level=None` 下以单一显式 `BEGIN IMMEDIATE ... COMMIT` 包裹（分类 DML 不再依赖隐式事务）；任何异常 ROLLBACK 后旧表原样；
- 步骤 0：**逐表逐列探测**——memories 缺任一新列即补（与 outbox 表状态无关）；outbox 已是新 schema 或不存在→跳过重建段；
- 转换 INSERT 提供全列（target_collection/memory_id/projection_revision/request_id/task_class/status/retry_count/next_retry_at/last_error/dead_letter_at/created_at/updated_at）；
- `_amr_migrations` 标记表：记录已应用迁移 id，回填/重建一次性执行（幂等门），上线手册固化顺序（结构迁移→状态枚举迁移）。

## 5. 归属与错误契约补强

- A 轨新增所有权：`src/cognitive/context_builder.py`、`scripts/adopt_orphan_points.py`（adopt 时 stamp `projection_class='legacy'`）；全部状态比较点（context_builder.py:72,86；cognitive_engine.py:605,639,701；reconciliation/engine.py:292,298）经 normalize 归一；
- C1 硬性化：bridge 透传 error.{code,message,data}；`tests/test_rpc_roundtrip.py` 断言 VERSION_CONFLICT 的 data.current_version **经 MCP bridge 端到端可见**（验收 #8 的判据）。

## 6. §7 重写（严格限定用户四类：期望字符串 / 异常类型 / 夹具注入 / 隔离门控）

| 文件:位置 | 允许修改 | 类型 |
| --- | --- | --- |
| test_step3_engine.py::test_loading_timeout | 异常 → ModelLoadingPendingError | 异常类型 |
| test_step4_memory_service.py（159-164,186-193,231,238,280,319-332） | memory_get 合并形态字段期望；update_status 后改 memory_get 断言（search 弱一致） | 期望字符串/夹具 |
| test_step5_extra_contracts.py:165-180、test_step5_comprehensive.py:222-237 | 状态期望按 normalize 映射 | 期望字符串 |
| test_reconciliation.py:153,157 | 状态大写期望；:157 断言目标改旧队列 delta/upsert 语义 | 期望字符串 |
| test_v2_engine.py:409（forget 返回值）,413,420-421,466-478 | 状态大写期望；delete 任务断言随 F6 恢复应原样通过，若夹具路径变化仅允许夹具注入 | 期望字符串/夹具 |
| test_v2_storage.py:215,230,237,260 | 状态大写期望；:260 pending 计数随 F1 取消策略应原样通过，必要时夹具注入 | 期望字符串/夹具 |
| test_cognitive_distill.py:82-92 | 状态期望大写化 | 期望字符串 |
| test_retrieval_contract_fixes.py:128 | 应原样通过（legacy 冻结）；如夹具需要→夹具注入 | 夹具 |
| test_v3_contracts.py:214 起 | AMRApplication 用 DI 注入 fake（C 轨） | 夹具/隔离门控 |
| test_v2_end_to_end.py、test_v3_e2e_integration.py | AMR_TEST_LIVE_DAEMON=1 + AMR_TEST_DAEMON_SOCKET 双门控 | 隔离门控 |

撤回 v5.2 中"改经 memory_get / 断言目标改旧队列"的越权表述——语义断言一律不得改写，只能改期望值、异常类型、夹具与门控；无法在四类内适配的用例如实上报，不得改。
