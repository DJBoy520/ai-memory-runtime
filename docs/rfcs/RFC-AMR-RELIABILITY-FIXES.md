# RFC: AMR 可靠性修复（P0 写链路一致性 + 冷启动并发控制，P1 契约统一与可观测性）

- 状态: v2（2026-11-08 修订，吸收第一轮评审 22 条意见后重审）
- 作者: zcode（编排），基于 2026-11-07 用户复核结论与源码核验
- 影响面: 守护进程 `amr.service`，需重启后生效；含增量 DDL（幂等表、outbox 加列）与可选数据迁移

## v2 变更摘要（相对 v1）

1. **D1 推翻重写**：v1 用 `asyncio.Future` 做 single-flight——engine 实例被 main 事件循环与 Outbox Worker 的独立线程+事件循环共享（`src/main.py:64-69`、`src/service/outbox_worker.py:161-171`），跨 loop await Future 会挂起/报错，executor 线程 `set_result` 亦非线程安全。v2 改为**线程原语方案**（执行锁 + 状态轮询 + epoch 代次），完全 loop 无关。
2. **D2 纠正投影队列事实**：v1 误以 `memory_projection_outbox` 为在用队列——实测该表 0 行且**无消费者**（仅 `src/reconciliation/engine.py:230-245` 写入）；真实链路是 `qdrant_sync_queue`（3587 行，全 synced），且它**没有 version / next_retry_at / dead_letter_at 列**，`retry_count>=5` 的行永久不再拉取。v2 指定 `memory_projection_outbox` 为规范队列（该表本就具备 version/retry/死信字段，补 `target_collection` 列），Worker 切换消费者，`qdrant_sync_queue` 仅保留给 legacy `memory_record` 排水。
3. **D2 幂等键重设计**：PK 改为 `(agent_id_raw, op, idem_key)` 并校验 memory_id 一致（v1 的 `(agent_id, idem_key)` 会跨操作重放、且 agent_type 归一后同类型多实例共享键空间）。
4. **D2 明确删除 create 路径的 `engine.embed`**（v1 文字只写了删 Qdrant 写，`memory_service.py:628-630` 的同步向量计算被漏掉）。
5. **D2 新增**：`memory_get` 改 SQLite SSOT 优先（v1 未处理投影滞后窗口内 `memory_get=null` / `update_status NOT_FOUND`）；陈旧行跳过规则（v1 无，迟到旧行会用旧 payload 覆盖新版本）。
6. **D3/D6 兼容性修正**：映射链首位加 `RpcError` 直通分支（否则业务异常漏入 -32603）；`RpcError`/`VersionConflictError` 改继承 `ValueError`（现有 `tests/test_v3_contracts.py:91,226` 的 `pytest.raises(ValueError, ...)` 必须继续通过）；bridge 保留 daemon 错误的 `data`。
7. **D4 从"值改写"改为"映射层 + 条件修正"**：`memory_search` 的 superseded_by 重定向条件从 `status=="SUPERSEDED"` 改为按 `superseded_by` 字段判定（否则折叠成 HISTORICAL 会打断重定向）；真正写库的是 `session_store.update_memory_status`（仍写小写 superseded），接入点在那里；迁移脚本扩到 SQLite + Qdrant payload；越界文件（reconciliation/cognitive_engine）只清点报告不改。
8. **验收与测试可执行性**：single-flight 测试必须同时注入 Dummy 模型/tokenizer 并 mock `_infer_batch_sync`（照 `tests/test_step3_engine.py` 既有模式）；`idle_timeout_seconds<=0` 时 embed 的 finally 不再把 READY 降为 IDLE；admin metrics 测试禁止用生产 `AMRApplication()`。

## 0. 背景与已核验证据

近 7 天 journald：7 次推理等待超时、40 次客户端提前断开；单次冷启动超时窗口约 100s。

### 源码与数据库证据（2026-11-07/08 核验）

| 问题 | 位置 | 证据 |
| --- | --- | --- |
| 无 single-flight | `src/core/engine.py:209-232` | `load_model()` 对 `LOADING` 态无分支，并发第二个调用者会再次进入 `_load_model_sync` |
| 请求等待超时硬编码 25s | `src/core/engine.py:250,325` | `_wait_for_ready(timeout=25.0)`，实测冷启动 28~100s |
| `idle_timeout_seconds<=0` 危险 | `src/core/engine.py:145-158` | `threading.Timer(self.idle_timeout_seconds, ...)`，0 = 立即卸载 |
| 假失败链路 | `src/service/memory_service.py:728-769` | 先 `update_memory_v3`（SQLite 提交）→ 再 `engine.embed`(746) + `qdrant.upsert_points`(765)；embed 超时异常在提交后抛出 |
| 双写者 | `src/service/memory_service.py:666,765` | 请求路径直接写 Qdrant，同时队列行也会被 Worker 投影 |
| 错误全被包成 -32603 | `src/interfaces/ipc/server.py:84-86` | `except Exception → make_jsonrpc_error(-32603, ...)` |
| 队列表分裂 | `src/core/session_store.py:156-188,1410-1431,1627-1637`；`src/service/outbox_worker.py:63-141` | Worker 实际消费 `qdrant_sync_queue`（无 version/重试调度/死信列，retry_count>=5 永久停拉）；`memory_projection_outbox` 0 行、无消费者（仅 reconciliation 写入）。实测去重分析：按 (memory_id, op_type) 只剩 1657 组，同 version 重复行大量存在（metadata-only 更新不递增 version） |
| 状态枚举分裂 | `src/service/memory_service.py:205-216,231-234,535-581`；`src/core/session_store.py:1004-1103` | `memory_search` 仅在 `status=="SUPERSEDED"` 时按 superseded_by 重定向；`memory_service.memory_update_status` 只写 Qdrant payload，真正写库的 `session_store.update_memory_status` 仍落小写 `superseded`；`scripts/normalize_status_schema.py` 既有 superseded→SUPERSEDED（仅 Qdrant）脚本 |

DB 现状（2026-11-07 实测）：`memories.status` 分布 ACTIVE 605 / DELETED 28 / HISTORICAL 522，SQLite 无 legacy 值；Qdrant payload 中可能残留 SUPERSEDED/ARCHIVED（迁移脚本覆盖）。

## 1. 目标 / 非目标

**目标**
- G1 任意并发冷启动只发生一次模型加载；等待方共享同一次加载结果；跨线程/跨事件循环安全。
- G2 SQLite 是唯一同步提交点；Qdrant 唯一写者是 Outbox Worker；写响应可确定（projection_status）。
- G3 写接口幂等：同 (agent, op, idempotency_key) 重试不产生第二次版本递增/投影，且不跨操作/跨目标重放。
- G4 JSON-RPC 返回稳定业务错误码 + 结构化 data；**既有契约测试全部保持通过**。
- G5 状态枚举单一事实来源，legacy 值在兼容层映射，superseded_by 重定向行为保留。
- G6 request_id 贯穿日志；关键耗时进入内存指标，admin UDS 可读。
- G7 agent 身份拆分 agent_type（参与授权）与 instance_id（仅审计）。
- G8 `amr doctor` 一键诊断（含死信可见性）。

**非目标（本轮明确不做）**
- 不加文件日志/RotatingFileHandler（journald 已覆盖）。
- 不做白名单通配符；不改变"agent_id 客户端自声明"的信任模型（真正边界是 UDS 0600 同用户）。
- 不做内容语义自动合并；OCC 冲突如实返回 current_version。
- 不放宽插件 80–150ms 预取预算；插件侧零改动。
- 不动 `memory_record` legacy 路径及其 `qdrant_sync_queue` 排水（另行废弃排期）。
- 不引入 Prometheus；不改 `scripts/normalize_status_schema.py`（既有脚本保持原样，仅在新迁移脚本中说明关系）。
- 不重构 reconciliation / cognitive_engine（只清点其 legacy 状态引用并报告）。

## 2. 设计

### D1 引擎并发治理（W1，gemini）—— 线程原语方案

`src/core/engine.py`：

1. **single-flight 用执行锁，不用 asyncio.Future**（engine 被 main loop 与 Outbox Worker 的独立 loop 共享，Future 跨 loop 会挂起）：
   - 新增 `self._load_exec_lock = threading.Lock()`、`self._loader_active = False`、`self._load_epoch = 0`、`self._load_started_at: Optional[float]`，均受 `_state_lock` 保护。
   - **加载发起**（`_try_become_loader_locked()`，持 `_state_lock` 调用）：状态在 `UNLOADED/ERROR/UNLOADING`，或 `LOADING` 但 `_loader_active is False`（兼容既有测试手工置 LOADING 的路径，如 `tests/test_step3_engine.py` 的 `test_loading_spin_queue_success`）时：`_load_exec_lock.acquire(blocking=False)` 成功 → 置 `LOADING`、`_loader_active=True`、`_load_started_at=now`、`_load_epoch` 不变，向 executor 提交 `_load_and_finalize(epoch)`；acquire 失败 → 本调用方是等待者。
   - **加载执行**（`_load_and_finalize(epoch)`，跑在 executor 线程）：调用重构后的 `_load_model_sync()`（**改为返回 `(tokenizer, model)`，不直接挂到 self**，修掉超时后线程仍装 model 的竞态）；随后持 `_state_lock` 收尾：若 `epoch == self._load_epoch` → 赋值 `self.tokenizer/self.model`、状态 READY、观测 `model_load_duration_ms`/`model_loads_total`；若 epoch 已过期 → 丢弃加载产物（del + `torch.cuda.empty_cache()`）、不动状态。任何路径finally：`_loader_active=False`、释放 `_load_exec_lock`。异常 → 仅在 epoch 未过期时置 `ERROR`（`_last_error` 记录），等待者轮询可见。
   - **等待方**（async 协程，任意 loop 可用）：轮询 `while True: 持锁读状态；READY/IDLE→返回；ERROR→raise EngineError；LOADING→检查 (now - _load_started_at) > load_timeout_seconds → 若 epoch 未过期则 epoch+=1、置 ERROR("load timeout")（首个到达者执行，收敛点）；超出本请求 `request_load_wait_seconds` 预算 → raise ModelLoadingPendingError；否则 await asyncio.sleep(0.05)`。轮询周期 50ms，远小于加载耗时，无性能问题。
2. **`load_timeout_seconds` 收敛语义**：超时不试图取消不可取消的 executor 线程，而是"标记本次尝试失败 + epoch 递增"——完成线程发现 epoch 过期即丢弃产物；executor `max_workers=1` 保证同一时刻至多一个 `_load_model_sync` 在跑，新请求可发起新 epoch 的加载（排队在 executor 后面，仍是单飞）。
3. **`embed()` finally 与 idle 语义**（修 `engine.py:352-361`）：`remaining==0 且 state==READY` 时：`idle_timeout_seconds > 0` → 转 IDLE 并 `_schedule_idle_unload()`；`<= 0` → **保持 READY**（不降 IDLE、不定时器）。`_schedule_idle_unload` 入口对 `<= 0` 直接短路（双保险）。
4. `embed()` 开头调用上述等待逻辑（替代 `_wait_for_ready` 的 25s 硬编码）；预算超时抛 `core/errors.ModelLoadingPendingError`（映射 MODEL_LOADING，可重试），与加载失败（`EngineError`，映射 MODEL_ERROR）严格区分。
5. 新增 `async def preload(self) -> None`（幂等，READY 时 no-op，等待用 `load_timeout_seconds` 全额预算），供 main.py `create_task` 调用。
6. 队列满语义不变（`QueueFullError`）。

`config/settings.py` 的 `ModelConfig` 新增（W1）：

```python
preload_on_start: bool = False        # 启动时后台预热，不阻塞 UDS 就绪
load_timeout_seconds: int = 120       # 单次加载尝试的收敛上限（覆盖实测 100s 窗口）
request_load_wait_seconds: int = 30   # 单个请求愿意等加载的时间；超时→MODEL_LOADING
```

`idle_timeout_seconds` 语义修订：`<= 0` = 永不自动卸载。本机 `config/config.yaml`（git 忽略，直接改）与 `config/config.example.yaml`：

```yaml
model:
  preload_on_start: true
  load_timeout_seconds: 120
  request_load_wait_seconds: 30
  idle_timeout_seconds: 0   # 常驻显存；<=0 表示永不自动卸载
```

### D2 事务性 Outbox 单写者 + 幂等（W2，deepseek）

**第 0 步（强制）**：先 grep `session_store` 的全部入队 helper 与 `outbox_worker` 的 fetch/mark 函数，绘制真实 enqueue/consume 配对表，写进 deviations，再动手。

**响应契约变更**：`memory.create/update/delete` 在 SQLite 事务提交 + 规范队列行落库后**立即返回**，响应带 `projection_status: "pending"`。请求路径删除**全部** `engine.embed` 与 `qdrant.*` 调用（`_create_v3_memory` 的 628-630 同步向量计算一并删除；Qdrant 向量由 Worker 从 payload content 现算）。`memory_record` 旧路径本轮不动。

**规范队列 = `memory_projection_outbox`**（它已具备 version/op_type/retry_count/next_retry_at/last_error/dead_letter_at；v1 误认的 `qdrant_sync_queue` 无这些列）：

- 迁移（幂等 DDL，启动时执行）：`ALTER TABLE memory_projection_outbox ADD COLUMN target_collection TEXT`（已存在则跳过）；v3 写入路径入队时填 `target_collection`（消费 `collection_name` 参数——修复 v1 中 collection_name 变无人消费、非默认集合被静默错投的问题）。
- `src/service/outbox_worker.py` 改为消费 `memory_projection_outbox`；**同时保留**既有 `qdrant_sync_queue` 排水循环给 legacy `memory_record`（行为不变）。reconciliation 往规范队列写的行从此有了消费者（该表的设计意图）。
- **重试与死信**：规范队列行失败 → `next_retry_at = now + min(2^retry_count, 60)s`，`retry_count >= 8` → 置 `dead_letter_at`（不再重试，doctor 可见）；legacy 队列行为不变。
- **陈旧行跳过（单写者正确性的关键）**：Worker 每轮按 memory_id 分组，同组只应用 id 最大的 pending 行；被跳过的旧行标 done 并在 last_error 记 "superseded by newer projection"。附加版本护栏：`op=upsert 且 row.version < memories.version（SQLite 当前值）` → 跳过。杜绝迟到旧行用旧 payload 覆盖新版本。
- **不建** `(memory_id, version, op_type)` 唯一索引：metadata-only 更新不递增 version，同键重复行是合法业务形态（实测同 version 重复组大量存在），唯一约束反而制造 IntegrityError 新假失败。

**幂等（重设计）**：

- 表：`CREATE TABLE IF NOT EXISTS idempotency_keys (agent_id_raw TEXT NOT NULL, op TEXT NOT NULL, idem_key TEXT NOT NULL, memory_id TEXT NOT NULL, response_json TEXT NOT NULL, created_at INTEGER NOT NULL, PRIMARY KEY (agent_id_raw, op, idem_key))`。
- `op ∈ {create, update, delete}`，防跨操作重放；`agent_id_raw` 用**归一化前的原始调用方标识**（如 `zcode-win`），同类型多实例键空间隔离。
- 命中且 `memory_id` 一致 → 重放 `response_json`（附 `idempotent_replay: true`）；命中但 `memory_id` 不一致 → 抛 `RpcInvalidArgument`（"idempotency_key reused for different target"）。
- **快照闭环在 session_store 事务内**：完整响应（含 `projection_status`）在 `create_memory_v3/update_memory_v3` 事务内组装并写入 `response_json` 后才 COMMIT；service 层不得事后追加字段（否则重放响应缺字段）。
- 保留 7 天（`getattr(config.storage, "idempotency_retention_days", 7)`），Worker 顺带清理。

**其他**：

- `update_memory_v3` 的 VERSION_CONFLICT 改抛 `core/errors.VersionConflictError`（**继承 ValueError**，消息保留 `VERSION_CONFLICT: ...` 前缀，`tests/test_v3_contracts.py:91` 的 `pytest.raises(ValueError, match=...)` 无需改动即通过）。
- `memory_get` 改 **SQLite SSOT 优先**（`get_memory_v3` 命中即返回，Qdrant 仅作向量检索源）——消除投影滞后窗口内 `memory_get=null`、`memory_update_status NOT_FOUND` 的新故障面。
- `tools.py`：`memory_create/update/delete` 增加 `idempotency_key` 参数与 projection_status 契约说明。

### D3 稳定业务错误码（W3，gemini）

`src/interfaces/ipc/errors.py` 映射表（**`RpcError` 已改继承 `ValueError`**，编排方已更新，兼容既有 `raises(ValueError)` 契约）：

| code | error_code | data |
| --- | --- | --- |
| -32001 | AGENT_UNAUTHORIZED | `{agent_id}` |
| -32002 | VERSION_CONFLICT | `{memory_id, current_version, expected_version}` |
| -32003 | MODEL_LOADING | `{model_state, retry_after_ms}` |
| -32004 | QUEUE_FULL | `{active_requests, max_queue_size}` |
| -32005 | NOT_FOUND | `{memory_id}` |
| -32602 | INVALID_ARGUMENT | `{field?, reason}` |
| -32603 | INTERNAL | `{}`（兜底） |

`server.py` `_handle_client` 映射顺序（**第一条是直通**，否则业务异常漏入兜底）：

1. `isinstance(exc, RpcError)` → 原样 `to_jsonrpc_error(req_id)`；
2. `core.errors.VersionConflictError` → `RpcVersionConflict`（data 取异常字段）；
3. `core.errors.ModelLoadingPendingError` → `RpcModelLoading`；
4. `engine.QueueFullError` → `RpcQueueFull`；
5. `core.errors.InvalidStatusError` → `RpcInvalidArgument`；
6. `KeyError` → `RpcNotFound`；
7. 其余 `ValueError` 按消息前缀解析（`AGENT_UNAUTHORIZED:` / `VERSION_CONFLICT:` / `Invalid status:`，向后兼容未迁移路径）；
8. 兜底 INTERNAL。

`bridge.py`：`_call_daemon` 不再把 daemon 错误折叠成裸 `RuntimeError`——透传 `error.{code,message,data}`（新增异常类型携带 data），`handle_request` 的 isError 文本首行 `[error_code] message`，data JSON 附后（error_code 取 message `XXX:` 前缀或 `data.error_code`）。

### D4 状态枚举统一——映射层 + 条件修正（W2）

- `src/core/memory_status.py`（新建）：`VALID = {TEMPORARY, ACTIVE, CONFLICT, DELETED, HISTORICAL, PENDING_VERIFY}`；`LEGACY_MAP = {SUPERSEDED→HISTORICAL, ARCHIVED→HISTORICAL, 小写同映射}`；`normalize_status()`：upper → legacy 映射 → 校验，非法抛 `core/errors.InvalidStatusError`。
- **superseded_by 重定向保留**：`memory_search`（`memory_service.py:231-234,205-216`）的重定向条件从 `status == "SUPERSEDED"` 改为「`superseded_by` 非空即重定向（与 status 解耦）」——折叠枚举后重定向行为不变。
- **接入点修正**：真正写库的是 `session_store.update_memory_status`（仍落小写 superseded）→ 在此接 `normalize_status`（HISTORICAL + 保留 superseded_by）；`memory_service.memory_update_status`（只写 Qdrant payload 的那个）→ 补规范队列 `update_payload` 入队 + normalize，消除 SQLite/Qdrant 分叉。
- 迁移脚本 `scripts/migrate_status_enum.py`：SQLite 行 + **Qdrant payload 扫描修正**（默认 `--dry-run`）；文档注明与既有 `scripts/normalize_status_schema.py` 的关系（后者保持不动）。
- 边界纪律：reconciliation / cognitive_engine 等**不在所有权矩阵内**的 legacy 状态引用只 grep 清点并写进 deviations 报告，不修改——它们的兼容性由边界处的 normalize 与 `superseded_by` 解耦条件保证。

### D5 request_id + 指标 + doctor（W3 + 集成轨）

- `src/core/request_context.py`（已预置）：server.py 每请求 set/finally reset；id 取 JSON-RPC `id`（字符串化），缺失则 `new_request_id()`。
- `main.py`（集成轨）：`logging.Filter` 注入 `request_id`，格式串加 `%(request_id)s`。
- `src/service/metrics.py`（已预置）：server 观测 `request_duration_ms{method}` 与 error_code 计数；engine 观测 `model_load_duration_ms`/`model_loads_total`；worker 观测 embedding/qdrant 耗时与 `outbox_lag_seconds` gauge。
- `main.py` `dispatch_admin_rpc` 新增 `status.metrics`：`METRICS.snapshot()` + 引擎状态 + 规范队列积压/死信统计。
- `scripts/amr_doctor.py`：systemd 活性、UDS socket、admin `status.metrics`、**规范队列积压/死信/最老滞后**、SQLite quick_check、Qdrant 连通、journald 近 24h 错误分类；`--json`；对未升级 daemon 优雅降级。
- **测试隔离硬约束**：`test_admin_metrics.py` 禁止用生产 `AMRApplication()`（会开生产 `data/sessions.db` 并连真实 Qdrant）——用 tmp_path 内存库 + fake engine/qdrant 注入构造被测对象。

### D6 agent 身份拆分（W3 定义 + 集成轨接线）

`src/interfaces/ipc/auth.py`（新建）：

```python
def authorize_agent(raw_agent_id: str, allowed: set[str]) -> tuple[str, Optional[str]]:
    """1) 精确命中白名单 → (raw, None)
       2) raw 形如 <type>-<instance> 且 type 命中 → (type, instance)
       3) 否则抛 RpcAgentUnauthorized(data={"agent_id": raw})"""
```

`RpcAgentUnauthorized` 继承 `RpcError`（即 ValueError），`main.py` 换用它后既有 `pytest.raises(ValueError, match="AGENT_UNAUTHORIZED")` 兼容。service 收归一化 agent_type；instance_id 只进日志/审计；幂等键空间用归一化前 raw（见 D2）。真实安全边界仍是 UDS 0600 同用户。

### D7 凭据治理（最小落地）

`QdrantConfig` 支持 `api_key_file` 与环境变量 `AMR_QDRANT_API_KEY`（优先级 env > file > 明文）；`config.example.yaml` 注明并提示轮换（轮换由用户在 Qdrant 侧执行）。

## 3. 文件归属矩阵（并行编码的硬边界）

| 轨道 | 模型 | 拥有文件（不得越界） |
| --- | --- | --- |
| W1 引擎单飞 | gemini | `src/core/engine.py`、`config/settings.py`、`config/config.yaml`、`config/config.example.yaml`、`tests/test_engine_singleflight.py`、`tests/test_idle_timeout_disabled.py` |
| W2 写链路 | deepseek | `src/service/memory_service.py`、`src/core/session_store.py`、`src/service/outbox_worker.py`、`src/core/memory_status.py`（新建）、`src/interfaces/mcp/tools.py`、`scripts/migrate_status_enum.py`（新建）、`tests/test_outbox_single_writer.py`、`tests/test_idempotency.py`、`tests/test_status_unification.py`、`tests/test_v3_contracts.py`（仅当异常继承变更需要适配） |
| W3 IPC | gemini | `src/interfaces/ipc/server.py`、`src/interfaces/ipc/auth.py`（新建）、`src/interfaces/mcp/bridge.py`、`tests/test_rpc_errors.py`、`tests/test_auth_normalization.py` |
| 集成 | deepseek | `src/main.py`、`scripts/amr_doctor.py`（新建）、`tests/test_admin_metrics.py`（新建）、`src/core/qdrant.py`（仅 D7 凭据读取）、`config/settings.py`（仅 QdrantConfig 追加）、`config/config.example.yaml`（仅凭据注释） |
| 共享契约（编排方维护，编码轨**只读**） | — | `src/core/errors.py`、`src/core/request_context.py`、`src/service/metrics.py`、`src/interfaces/ipc/errors.py`（v2 已按评审改为 ValueError 继承） |

交叉审核：gemini 审 deepseek 系（W2+集成），deepseek 审 gemini 系（W1+W3）；修复由原轨作者执行。

## 4. 验收标准（对应测试）

1. 20 并发触发冷启动（注入 Dummy 模型/tokenizer + 慢 `_load_model_sync` + mock `_infer_batch_sync`，照 `tests/test_step3_engine.py` 模式）：加载恰好一次，全部请求成功 —— `tests/test_engine_singleflight.py`。
2. `idle_timeout_seconds=0` 与 `=-1`：请求完成后状态**保持 READY**（finally 不降 IDLE）、无卸载定时器 —— `tests/test_idle_timeout_disabled.py`。
3. SQLite 提交后模拟下游全故障：`memory_update` 响应成功 + `projection_status:"pending"`，version 递增恰好一次；同 `(agent, op, key)` 重试 → 同 version、无新 revision、无新队列行、`idempotent_replay:true`；同 key 换 memory_id → INVALID_ARGUMENT —— `tests/test_outbox_single_writer.py`、`tests/test_idempotency.py`。
4. 一次 create 全链路（含 Worker 投影）恰好一次 qdrant upsert；陈旧行（同 memory 更早 id / 旧 version）被跳过不覆盖 —— `tests/test_outbox_single_writer.py`。
5. 真 OCC 冲突返回 `VERSION_CONFLICT` + `data.current_version`；既有 `test_v3_contracts.py` 全部保持通过 —— `tests/test_rpc_errors.py`。
6. legacy `superseded/archived` 归一 HISTORICAL 且 `superseded_by` 重定向不断；非法值 INVALID_ARGUMENT —— `tests/test_status_unification.py`。
7. 投影滞后窗口内 `memory_get` 返回 SQLite SSOT 数据 —— `tests/test_outbox_single_writer.py`。
8. 请求日志可用 request_id 串联 —— `tests/test_rpc_errors.py`。
9. 插件 80–150ms 预取 fail-open 零改动（既有插件测试通过）。
10. `tests/` 全量 pytest 与基线一致（不新增失败；基线仅 `test_v2_end_to_end` E2E 延迟用例已知失败）。

## 5. 实施顺序与回滚

顺序：评审 v2 → 并行三路 → 集成 → 全量测试门 → 交叉审核 → 修复 → 终验。
回滚：`fix/amr-reliability` 分支单基线，修复改动集中在其上工作区 diff，`git checkout -- <file>` 或回退基线 commit 即整体撤销。守护进程需 `systemctl --user restart amr` 后生效（用户择机执行）。

## 6. 已知风险

- `memory_search` 对新卡片的可见性变为"投影后"（亚秒级）；`projection_status` 即提示；`memory_get` 已 SSOT 化不受影响。
- Worker 切换规范队列后，reconciliation 写入的行开始被消费——预期行为（该表设计意图），doctor 监控积压。
- `bridge.py` 错误文本格式变化对依赖旧文本的调用方可感知（首行保留 message，前缀新增）。
- 幂等键空间用 raw agent_id：同一物理 Agent 若每次会话随机化 instance 后缀，键不复用（安全方向的失败）。
