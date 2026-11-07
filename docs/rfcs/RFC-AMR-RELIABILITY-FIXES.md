# RFC: AMR 可靠性修复（P0 写链路一致性 + 冷启动并发控制，P1 契约统一与可观测性）

- 状态: 待评审（DeepSeek + Gemini 双向评审通过后实施）
- 作者: zcode（编排），基于 2026-11-07 用户复核结论与源码核验
- 影响面: 守护进程 `amr.service`，需重启后生效；不迁移数据（除幂等表/唯一索引的增量 DDL）

## 0. 背景与已核验证据

近 7 天 journald：7 次推理等待超时、40 次客户端提前断开；单次冷启动超时窗口约 100s。
用户复核结论（本 RFC 的出发点）：

1. 冷启动问题属实，但只把 25s 改 60s 兜不住（实测加载窗口可达 100s+）；
2. `memory_update` 存在"SQLite 已提交却返回失败"的假失败链路（源码已核实）；
3. 插件 80–150ms 预取是刻意的 fail-open 预算（DOC-AMR-06 §4.2），**不得放宽**；
4. 白名单报错属实，但 `agent_id` 是客户端自声明字符串，通配符不增强认证；
5. 状态枚举分裂：v3 接口拒绝 `SUPERSEDED`，legacy/搜索/旧文档仍用 superseded/archived；
6. systemd 已负责日志收集与轮转（`systemd/amr.service:18`），不再叠加文件日志。

### 源码证据（核验于 2026-11-07）

| 问题 | 位置 | 证据 |
| --- | --- | --- |
| 无 single-flight | `src/core/engine.py:209-232` | `load_model()` 对 `LOADING` 态无分支，并发第二个调用者会再次进入 `_load_model_sync` |
| 请求等待超时硬编码 25s | `src/core/engine.py:250,325` | `_wait_for_ready(timeout=25.0)`，实测冷启动 28~100s |
| `idle_timeout_seconds<=0` 危险 | `src/core/engine.py:145-158` | `threading.Timer(self.idle_timeout_seconds, ...)`，0 = 立即卸载 |
| 假失败链路 | `src/service/memory_service.py:728-769` | 先 `update_memory_v3`（SQLite 提交）→ 再 `engine.embed`(746) + `qdrant.upsert_points`(765)；embed 超时异常在提交后抛出 |
| 双写者 | `src/service/memory_service.py:666,765` | 请求路径直接写 Qdrant，同时 outbox 行也会被 Worker 投影 |
| 错误全被包成 -32603 | `src/interfaces/ipc/server.py:84-86` | `except Exception → make_jsonrpc_error(-32603, "Internal error: ...")` |
| 状态枚举分裂 | `src/core/session_store.py:1530` 附近 / 搜索过滤 | v3 只接受 6 态；legacy 接口与文档仍出现 superseded/archived |

DB 现状（2026-11-07 实测）：`memories.status` 分布 ACTIVE 605 / DELETED 28 / HISTORICAL 522，**无 legacy 值**；`memory_projection_outbox` 为空表。状态统一因此主要是代码层收敛 + 防御性迁移脚本。

## 1. 目标 / 非目标

**目标**
- G1 任意并发冷启动只发生一次模型加载；等待方共享同一 Future。
- G2 SQLite 是唯一同步提交点；Qdrant 唯一写者是 Outbox Worker；写响应可确定（projection_status）。
- G3 写接口幂等：同 idempotency_key 重试不产生第二次版本递增/投影。
- G4 JSON-RPC 返回稳定业务错误码 + 结构化 data（VERSION_CONFLICT 带 current_version）。
- G5 状态枚举单一事实来源，legacy 值在兼容层映射。
- G6 request_id 贯穿 IPC→service→engine/outbox 日志；关键耗时进入内存指标，admin UDS 可读。
- G7 agent 身份拆分 agent_type（参与授权）与 instance_id（仅审计）。
- G8 `amr doctor` 一键诊断。

**非目标（本轮明确不做）**
- 不加文件日志/RotatingFileHandler（journald 已覆盖）。
- 不做白名单通配符；不改变"agent_id 客户端自声明"的信任模型（真正边界是 UDS 0600 同用户）。
- 不做内容语义自动合并；OCC 冲突如实返回 current_version。
- 不放宽插件 80–150ms 预取预算；插件侧零改动。
- 不动 `memory_record` legacy 路径（已知不写底账，另行废弃排期）。
- 不引入 Prometheus。

## 2. 设计

### D1 引擎 single-flight + 配置语义（W1，gemini）

`src/core/engine.py`：

1. 新增 `self._load_future: Optional[asyncio.Future] = None`（受 `_state_lock` 保护）。
2. `load_model()` 重写状态分支：
   - `READY` → 直接返回；`IDLE` → 取消定时器、转 READY 返回；
   - `LOADING` 且 `_load_future` 存在 → **await 同一个 future**（锁外），绝不再次进入 `_load_model_sync`；
   - `UNLOADED/ERROR/UNLOADING` → 转 LOADING、创建共享 future，唯一的执行者在 executor 里跑 `_load_model_sync`，成功 `fut.set_result(None)` 并转 READY，失败 `fut.set_exception` 并转 ERROR（等待方随之醒来拿到异常）。
3. `embed()` 的等待改为共享 future + 单请求预算：
   ```python
   fut = <确保加载已启动，拿到共享 future>
   try:
       await asyncio.wait_for(asyncio.shield(fut), timeout=self.request_load_wait_seconds)
   except asyncio.TimeoutError:
       raise ModelLoadingPendingError(...)   # 绝不取消共享加载
   ```
   **`asyncio.shield` 是硬要求**：`wait_for` 超时会取消被等待对象，不 shield 会把共享加载一起取消。
4. 预算超时抛 `src/core/errors.ModelLoadingPendingError`（MAPPING：MODEL_LOADING，可重试），与"加载本身失败"（`EngineError`，映射 MODEL_ERROR）严格区分。
5. `embed()` finally 与 `_schedule_idle_unload()`：`idle_timeout_seconds <= 0` 一律短路（永不自动卸载）——修复 `Timer(0)` 立即卸载的坑。
6. 状态转移日志附带耗时，并向 `src/service/metrics.METRICS` 观测 `model_load_duration_ms`、`model_loads_total` 计数。
7. 提供公开预热入口 `async def preload(self) -> None`（幂等，READY 时 no-op），供 main.py 启动时 `create_task` 调用。

`config/settings.py` 的 `ModelConfig` 新增字段（含默认值）：

```python
preload_on_start: bool = False        # 启动时后台预热，不阻塞 UDS 就绪
load_timeout_seconds: int = 120       # 共享加载本身的上限（覆盖实测 100s 窗口）
request_load_wait_seconds: int = 30   # 单个请求愿意等加载的时间；超时→MODEL_LOADING
```

`idle_timeout_seconds` 语义修订：`<= 0` = 永不自动卸载。本机 `config/config.yaml` 与 `config/config.example.yaml` 同步更新：

```yaml
model:
  preload_on_start: true
  load_timeout_seconds: 120
  request_load_wait_seconds: 30
  idle_timeout_seconds: 0   # 常驻显存；<=0 表示永不自动卸载
```

### D2 事务性 Outbox 单写者 + 幂等（W2，deepseek）

**响应契约变更**：`memory.create/update/delete` 在 SQLite 事务提交 + outbox 行落库后**立即返回**，响应新增 `projection_status: "pending"`。请求路径内不再调用 `engine.embed` 与 `qdrant.upsert_points/update_payload/delete`。Qdrant 的唯一写者是 Outbox Worker。

`src/service/memory_service.py`：
- `_create_v3_memory`（约 640-672 行）：删除 payload 构造 + `qdrant.upsert_points` 段；`store_res` 附加 `projection_status: "pending"`。
- `memory_update`（706-784 行）：删除第 2 步全部 Qdrant 同步段；同样附加 `projection_status`。
- `memory_delete`（801 行）：维持走 `memory_update(status=DELETED)`，自动获得单写者语义。
- `memory_record`（353 行起）**本轮不动**。

`src/core/session_store.py`：
- 新增幂等表（启动时 `CREATE TABLE IF NOT EXISTS`）：
  ```sql
  CREATE TABLE IF NOT EXISTS idempotency_keys (
      agent_id      TEXT NOT NULL,
      idem_key      TEXT NOT NULL,
      memory_id     TEXT NOT NULL,
      response_json TEXT NOT NULL,
      created_at    INTEGER NOT NULL,
      PRIMARY KEY (agent_id, idem_key)
  );
  ```
- `create_memory_v3 / update_memory_v3`（及 delete 入口）接受可选 `idempotency_key`：
  事务内先查 `(agent_id, idem_key)`，命中则原样重放 `response_json`（解析后附加 `idempotent_replay: true`），不新增 revision、不新增 outbox 行；未命中则执行并写入响应快照。
- `update_memory_v3` 的 VERSION_CONFLICT 改抛 `src/core/errors.VersionConflictError`，携带 `memory_id / current_version / expected_version`（消息文本保留原格式以兼容日志检索）。
- outbox 表 `(memory_id, version, op_type)` 唯一索引（防重复投影；建索引前先去重，保留最大 id）。
- 幂等键清理：默认保留 7 天（`getattr(config.storage, "idempotency_retention_days", 7)`），由 Outbox Worker 每轮顺带 `DELETE` 过期行。

`src/service/outbox_worker.py`：
- 失败重试已有 `retry_count / next_retry_at / dead_letter_at` 机制，保持；
- 新增 METRICS：`outbox_lag_seconds` gauge（now − 最老 pending 的 created_at）、`embedding_duration_ms` / `qdrant_duration_ms` 观测、失败计数。

`src/interfaces/mcp/tools.py`：`memory_create/update/delete` 参数表增加 `idempotency_key`（string, optional）及响应契约说明（projection_status 语义）。

### D3 稳定业务错误码（W3，gemini）

`src/interfaces/ipc/errors.py`（已由编排方预置）定义映射：

| code | error_code | data |
| --- | --- | --- |
| -32001 | AGENT_UNAUTHORIZED | `{agent_id}` |
| -32002 | VERSION_CONFLICT | `{memory_id, current_version, expected_version}` |
| -32003 | MODEL_LOADING | `{model_state, retry_after_ms}` |
| -32004 | QUEUE_FULL | `{active_requests, max_queue_size}` |
| -32005 | NOT_FOUND | `{memory_id}` |
| -32602 | INVALID_ARGUMENT | `{field?, reason}` |
| -32603 | INTERNAL | `{}`（兜底，行为与现状一致） |

`src/interfaces/ipc/server.py` 的 `_handle_client` 异常映射顺序：
`core.errors.VersionConflictError` → `core.errors.ModelLoadingPendingError` → `core.errors.InvalidStatusError` → `engine.QueueFullError` → `KeyError` → `ValueError` 消息前缀解析（`AGENT_UNAUTHORIZED:` / `VERSION_CONFLICT:` / `Invalid status:`）→ 兜底 INTERNAL。

`src/interfaces/mcp/bridge.py`：`isError` 响应的文本首行改为 `[error_code] message`，data 以 JSON 附后——让 Agent 直接看到 `VERSION_CONFLICT (current_version=3)` 而不是裸 Internal error。

### D4 状态枚举统一（W2）

`src/core/memory_status.py`（新建）：

```python
VALID = {"TEMPORARY", "ACTIVE", "CONFLICT", "DELETED", "HISTORICAL", "PENDING_VERIFY"}
LEGACY_MAP = {"SUPERSEDED": "HISTORICAL", "ARCHIVED": "HISTORICAL", "archived": "HISTORICAL", "superseded": "HISTORICAL"}

def normalize_status(value) -> str:
    """upper → legacy 映射 → 校验；非法值抛 core.errors.InvalidStatusError（附 allowed）"""
```

接入点：`create_memory_v3` / `update_memory_v3` / `update_memory_status` 的写入口，以及检索的 `status` 过滤参数。全库 grep `superseded|archived`（不区分大小写）清点残留调用点并归一。
`scripts/migrate_status_enum.py`（新建）：防御性迁移，默认 `--dry-run`，输出将被改写的行；当前 DB 无 legacy 值，预期 0 行。

### D5 request_id + 指标 + doctor（W3 + 集成轨）

- `src/core/request_context.py`（已预置）：contextvar 承载 request_id。`server.py` 每请求 set/finally reset；id 取 JSON-RPC `id`（字符串化），缺失则 `new_request_id()`。
- `main.py`（集成轨）：安装 `logging.Filter` 把 `request_id` 注入全部日志记录，格式串加 `%(request_id)s`。
- `src/service/metrics.py`（已预置）：`METRICS` 单例，`incr / observe / set_gauge / snapshot`。
  归属：server.py 观测 `request_duration_ms{method}` 与错误计数（按 error_code）；engine 观测 `model_load_duration_ms`；outbox worker 观测 embedding/qdrant 耗时与 `outbox_lag_seconds`。
- `main.py` `dispatch_admin_rpc` 新增 `status.metrics`：返回 `METRICS.snapshot()` + 引擎状态（state、active_requests、total_inferences）+ outbox 积压/死信统计（查 SQLite）。
- `scripts/amr_doctor.py`（新建，集成轨）：体检项——systemd 活性、两个 UDS socket 存在性与权限、admin `status.metrics`、outbox 积压/死信/最老滞后、SQLite `PRAGMA quick_check` + WAL 体积、Qdrant 连通、journald 近 24h 错误按 error_code 分类计数。支持 `--json`；对未升级 daemon（无 status.metrics 方法）优雅降级并注明。

### D6 agent 身份拆分（W3 定义 + 集成轨接线）

`src/interfaces/ipc/auth.py`（新建）：

```python
def authorize_agent(raw_agent_id: str, allowed: set[str]) -> tuple[str, Optional[str]]:
    """1) 精确命中白名单 → (raw, None)
       2) raw 形如 <type>-<instance> 且 type 命中 → (type, instance)
       3) 否则抛 RpcAgentUnauthorized(data={"agent_id": raw})"""
```

`main.py` `dispatch_business_rpc` 改用 `authorize_agent`：service 收到归一化 `agent_type`（稳定、参与授权），`instance_id`（如 `zcode-win` / `zcode-overnight`）只进 request_context 与日志审计。`memories.created_by_agent` 继续落归一化后的 type（历史行不回改）。真实安全边界仍是 UDS 0600 同用户（文档注明；SO_PEERCRED 后续增强）。

### D7 凭据治理（最小落地）

`QdrantConfig` 支持 `api_key_file`（相对仓库根解析）与环境变量 `AMR_QDRANT_API_KEY` 覆盖，优先级：env > api_key_file > api_key。`config.example.yaml` 注明并提示轮换现有明文 key（轮换动作由用户在 Qdrant 侧执行）。

## 3. 文件归属矩阵（并行编码的硬边界）

| 轨道 | 模型 | 拥有文件（不得越界） |
| --- | --- | --- |
| W1 引擎单飞 | gemini | `src/core/engine.py`、`config/settings.py`、`config/config.yaml`、`config/config.example.yaml`、`tests/test_engine_singleflight.py`、`tests/test_idle_timeout_disabled.py` |
| W2 写链路 | deepseek | `src/service/memory_service.py`、`src/core/session_store.py`、`src/service/outbox_worker.py`、`src/core/memory_status.py`、`src/interfaces/mcp/tools.py`、`scripts/migrate_status_enum.py`、`tests/test_outbox_single_writer.py`、`tests/test_idempotency.py`、`tests/test_status_unification.py` |
| W3 IPC | gemini | `src/interfaces/ipc/server.py`、`src/interfaces/ipc/auth.py`、`src/interfaces/mcp/bridge.py`、`tests/test_rpc_errors.py`、`tests/test_auth_normalization.py` |
| 集成 | deepseek | `src/main.py`、`scripts/amr_doctor.py`、`tests/test_admin_metrics.py` |
| 共享契约（编排方已预置，任何人**只读不改**） | — | `src/core/errors.py`、`src/core/request_context.py`、`src/service/metrics.py`、`src/interfaces/ipc/errors.py` |

交叉审核：gemini 审 deepseek 系（W2+集成）的 diff，deepseek 审 gemini 系（W1+W3）的 diff；修复由原轨道作者执行。

## 4. 验收标准（对应测试）

1. 20 个并发请求触发冷启动（monkeypatch `_load_model_sync` 为 0.5s 假加载），`_load_model_sync` 只执行一次，全部请求成功 —— `tests/test_engine_singleflight.py`。
2. `idle_timeout_seconds=0`：请求完成后状态保持 READY、无卸载定时器；`= -1` 同 —— `tests/test_idle_timeout_disabled.py`。
3. SQLite 提交后模拟下游全故障：`memory_update` 响应仍为成功 + `projection_status:"pending"`，version 递增恰好一次；同 `idempotency_key` 重试 → 同 version、无新 revision、无新 outbox 行、`idempotent_replay:true` —— `tests/test_outbox_single_writer.py`、`tests/test_idempotency.py`。
4. 一次 create 全链路（含 outbox worker 投影）恰好一次 qdrant upsert 调用（mock 计数）。
5. 真 OCC 冲突返回 `VERSION_CONFLICT` + `data.current_version`，而非裸 Internal error —— `tests/test_rpc_errors.py`。
6. legacy `superseded/archived` 归一为 HISTORICAL；非法值抛 INVALID_ARGUMENT —— `tests/test_status_unification.py`。
7. 请求日志可用 request_id 串联（日志捕获断言）—— `tests/test_rpc_errors.py`。
8. 插件 80–150ms 预取 fail-open 行为零改动（既有插件测试保持通过）。
9. `tests/` 全量 pytest 与基线一致（不新增失败）。

## 5. 实施顺序与回滚

顺序：评审 → 并行三路（W1/W2/W3）→ 集成 → 全量测试门 → 交叉审核 → 修复 → 终验。
回滚：单分支单基线 commit，修复改动集中在其上的工作区 diff；`git checkout -- <file>` 或回退到基线 commit 即可整体撤销。守护进程需 `systemctl --user restart amr` 后新代码才生效（重启动作由用户择机执行，避免打断在线 Agent）。

## 6. 已知风险

- `memory_search` 对刚写入卡片的可见性从"同步"变为"outbox 投影后"（亚秒级）；`projection_status` 即为提示。检索语义契约不变。
- 全量 pytest 中若存在依赖真实 GPU/Qdrant/daemon 的既有用例，以基线为准判定"新增失败"，不强行修绿。
- `bridge.py` 错误文本格式变化对依赖旧文本的调用方是可感知变更（首行保留 message，前缀新增）。
