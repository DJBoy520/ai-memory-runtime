# AI Memory Runtime - 接口协议与契约规范 (API)

**文档标识**：`DOC-AMR-04-API`  
**版本号**：`v5.0.0`  
**传输规范**：本地 Unix Domain Socket (UDS) + JSON-RPC 2.0 (二进制 Length-Prefixed 帧，单包 ≤ 4MB)

---

## 1. 核心一致性与并发模型 (Consistency Model)

AMR v5 采用 **CQRS (命令查询职责分离) + 事务发件箱 (Transactional Outbox)** 架构：

```text
┌────────────────────────────────────────────────────────────────────────┐
│                          写路径 (Commit-then-Project)                   │
│                                                                        │
│   memory_create / update / delete                                      │
│         │                                                              │
│         ├──► [1. 本地写 SQLite SSOT] (memories + memory_projection_outbox) │
│         │    事务同步提交完成 (<5ms)                                   │
│         │    立即返回 projection_status="pending", version=N            │
│         ▼                                                              │
│   [2. 后台单写者 Worker 异步投递] (OutboxWorker)                          │
│         │                                                              │
│         └──► 计算 BGE-M3 向量 ──► Qdrant 写入 ──► 任务标记 done        │
└────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────┐
│                          读与检索路径 (双重一致性)                     │
│                                                                        │
│   • memory_get: 直读本地 SQLite SSOT ──► 【强一致】(Read-your-writes)  │
│   • memory_search: 检索 Qdrant 向量索引 ──► 【最终一致】(毫秒级延迟)   │
└────────────────────────────────────────────────────────────────────────┘
```

1. **写路径（Commit-then-Project）**：
   - 所有写入（`memory_create`, `memory_update`, `memory_delete`）均以单事务原子写入 SQLite 的 `memories` 表与 `memory_projection_outbox` 表，事务提交即对客户端返回。
   - 响应中携带 `projection_status="pending"` 与 `projection_revision`。写路径零阻塞、零外部网络 I/O。
2. **强一致读取（Strong Consistency）**：
   - `memory_get` 直接查询 SQLite SSOT，事务提交后立即可见最新版本、元数据与状态。
3. **最终一致检索（Eventual Consistency）**：
   - `memory_search` 依赖 `OutboxWorker` 单执行者线程异步计算 BGE-M3 嵌入向量并写入 Qdrant。新创建或更新的数据通常在数十至数百毫秒内就绪。
4. **单写者与锁边界**：
   - `OutboxWorker` 启动时抢占 `$XDG_RUNTIME_DIR/amr-outbox.lock`（OS 级 `fcntl.flock` 独占锁），保证同机单写者避免并发写 Qdrant 发生版本乱序；冲突实例将以退出码 `3` 退出并触发 systemd 重启退避。

---

## 2. 状态生命周期规范 (Status Lifecycle)

AMR 内部采用**全大写 6 态生命周期规范**，同时兼容旧版 4 态小写输入（自动做向后归一化映射）：

| 标准 6 态 (全大写) | 含义与流转规则 | Legacy 4 态兼容映射 |
| :--- | :--- | :--- |
| `ACTIVE` | 当前有效现役事实。`memory_search` 默认仅检索此状态。 | `active` |
| `PENDING_VERIFY` | 待验证事实（如由外部 Agent 采集但未经确认的推测）。 | - |
| `CONFLICT` | 冲突状态。当 `conflicts_with` 命中已有记录时，双方均自动转为此状态。 | - |
| `HISTORICAL` | 历史事实。经 CAS 更新产生新版本后，旧版本降级归档为此状态。 | `superseded` / `archived` |
| `TEMPORARY` | 临时记忆。具有生命周期限制。 | - |
| `DELETED` | 软删除。保留在 SQLite SSOT 中供审计与恢复，Outbox 异步从 Qdrant 物理删点。 | `deleted` |

---

## 3. 标准错误码体系 (Error Codes)

AMR 采用 JSON-RPC 2.0 扩展负数码，所有业务错误均携带机器可读的 `data` 结构：

| 错误码 | 枚举标识 | 触发场景 | `error.data` 负载字段 |
| :---: | :--- | :--- | :--- |
| `-32001` | `STORAGE_WRITE_FAILURE` | SQLite 写入失败、文件被锁或磁盘 I/O 异常 | `{"memory_id": "...", "reason": "..."}` |
| `-32002` | `VERSION_CONFLICT` | CAS 乐观锁冲突：传入的 `expected_version` 与当前版本不符 | `{"memory_id": "...", "current_version": N, "expected_version": M}` |
| `-32003` | `MODEL_UNAVAILABLE` | BGE-M3 模型尚未加载或处于加载中 | `{"model_state": "LOADING"}` |
| `-32004` | `VECTOR_INDEX_FAILURE` | Qdrant 连接超时或向量写入失败 | `{"collection": "...", "error": "..."}` |
| `-32005` | `AGENT_UNAUTHORIZED` | 传入的 `agent_id` 未在 `config.server.allowed_agents` 白名单中 | `{"agent_id": "..."}` |
| `-32006` | `IDEMPOTENCY_CONFLICT` | 传入已存在的 `idempotency_key` 但正文/项目等核心载荷不一致 | `{"existing_memory_id": "..."}` |
| `-32600` | `INVALID_REQUEST` | 非法 JSON-RPC 请求结构 | - |
| `-32601` | `METHOD_NOT_FOUND` | 未知的方法名 | `{"method": "..."}` |
| `-32602` | `INVALID_PARAMS` | 缺少必填参数或参数格式校验失败 | `{"field": "...", "reason": "..."}` |
| `-32603` | `INTERNAL_ERROR` | 服务端未捕获的内部异常 | `{"traceback": "..."}` |

---

## 4. 业务接口与 MCP 工具清单 (Business API)

业务 Socket 默认监听 `$XDG_RUNTIME_DIR/qdrant-bge.sock`（权限 `0600`）。对外提供 9 个标准 MCP 工具：

### 4.1 `memory_search` (记忆语义检索)
- **RPC 方法**：`memory.search` / `memory_search`
- **说明**：基于 Qdrant 向量检索。弱一致（若刚写入正在 pending 则暂未索引）。默认仅返回 `ACTIVE` 现役事实。
- **入参**：
```json
{
  "query": "AEP architecture consensus",
  "project_id": "aep-arch",
  "type": "decision/arch",
  "include_history": false,
  "status": "ACTIVE",
  "limit": 5,
  "score_threshold": 0.52
}
```
- **出参**：
```json
{
  "results": [
    {
      "memory_id": "mem_20261109_775697",
      "content": "AEP architecture consensus: single writer projection with CAS",
      "score": 0.8842,
      "project_id": "aep-arch",
      "type": "decision/arch",
      "status": "ACTIVE",
      "version": 1,
      "created_at": 1791456000
    }
  ],
  "total": 1
}
```

---

### 4.2 `memory_create` (创建记忆)
- **RPC 方法**：`memory.create` / `memory_create`
- **说明**：单事务写入 SQLite SSOT 并生成 Outbox 任务，返回 `projection_status="pending"`。支持 `idempotency_key`。
- **入参**：
```json
{
  "content": "SM4 GCM 模式下 IV 推荐为 12 字节，Tag 长度固定为 16 字节。",
  "project_id": "crypto-infrastructure",
  "type": "decision/crypto",
  "status": "ACTIVE",
  "source_refs": ["zcode:sess_1001:msg_001"],
  "conflicts_with": ["mem_old_001"],
  "idempotency_key": "uuid-v4-client-token"
}
```
- **出参**：
```json
{
  "memory_id": "mem_20261110_01a2b3",
  "version": 1,
  "status": "ACTIVE",
  "projection_status": "pending",
  "projection_revision": 1,
  "source_refs": ["zcode:sess_1001:msg_001"],
  "revisions": [
    {
      "version": 1,
      "content": "SM4 GCM 模式下 IV 推荐为 12 字节，Tag 长度固定为 16 字节。",
      "change_reason": "Initial creation"
    }
  ],
  "idempotent_replay": false
}
```
*注：若未传 `source_refs`，AMR 会自动打上 `"provenance-missing:<agent_id>:<utc-timestamp>"` 审计标签。同一 `idempotency_key` 重复调用时，相同内容返回 `"idempotent_replay": true`，冲突内容返回 `-32006`。*

---

### 4.3 `memory_get` (获取详情 - 强一致)
- **RPC 方法**：`memory.get` / `memory_get`
- **说明**：直读 SQLite SSOT，强一致返回完整数据（版本、正文、状态、变更历史、溯源信息）。
- **入参**：`{"memory_id": "mem_20261110_01a2b3"}`
- **出参**：返回该条记录的最新版本、状态及所有修订历史。

---

### 4.4 `memory_update` (CAS 乐观锁更新)
- **RPC 方法**：`memory.update` / `memory_update`
- **说明**：修改正文时**强制要求**传入 `expected_version` 和 `change_reason`。版本不符拒绝并返回 `-32002`，成功后版本递增并生成 Outbox 任务。
- **入参**：
```json
{
  "memory_id": "mem_20261110_01a2b3",
  "content": "SM4 GCM 模式下 IV 推荐 12 字节，Tag 必须固定 16 字节，且严禁重复 IV。",
  "expected_version": 1,
  "change_reason": "补充 IV 重复使用防范规则",
  "type": "decision/crypto",
  "status": "ACTIVE",
  "idempotency_key": "uuid-update-token"
}
```
- **出参**：
```json
{
  "memory_id": "mem_20261110_01a2b3",
  "version": 2,
  "status": "ACTIVE",
  "projection_status": "pending",
  "projection_revision": 2
}
```

---

### 4.5 `memory_history` (修订历史)
- **RPC 方法**：`memory.history` / `memory_history`
- **入参**：`{"memory_id": "mem_20261110_01a2b3"}`
- **出参**：按版本顺序返回完整修订记录数组（`revisions`）。

---

### 4.6 `memory_delete` (软删除)
- **RPC 方法**：`memory.delete` / `memory_delete`
- **说明**：SQLite 标记为 `DELETED`，Outbox 异步向 Qdrant 提交物理点位删除。保留本地记录供审计。
- **入参**：`{"memory_id": "mem_20261110_01a2b3", "reason": "过期废弃"}`
- **出参**：
```json
{
  "memory_id": "mem_20261110_01a2b3",
  "previous_status": "ACTIVE",
  "current_status": "DELETED",
  "projection_status": "pending",
  "projection_revision": 3
}
```

---

### 4.7 `memory_ingest_session` (会话归档)
- **RPC 方法**：`session.ingest` / `session_ingest`
- **说明**：批量归档对话历史至 SQLite `sessions` 与 `messages` 表，支持断点续传与溯源审计。
- **入参**：
```json
{
  "session_id": "sess_1001",
  "project_id": "crypto-infrastructure",
  "messages": [
    {
      "message_id": "msg_001",
      "role": "user",
      "content": "SM4 GCM 的 Tag 长度是多少？",
      "sequence": 1,
      "timestamp": 1791456000
    }
  ]
}
```
- **出参**：`{"inserted": 1, "ignored": 0, "status": "ok"}`

---

### 4.8 兼容工具 (Legacy Compat)
- `memory_record`：向后兼容旧版接口，自动映射至 SSOT 事务持久化与发件箱。
- `memory_update_status`：兼容旧版快速状态变更。

---

## 5. 管理接口清单 (Admin API)

管理 Socket 默认监听 `$XDG_RUNTIME_DIR/qdrant-bge-admin.sock`（权限 `0600`）。仅供运维诊断与 CLI 工具使用：

| RPC 方法 | 对应 CLI 命令 | 说明 | 响应内容关键字段 |
| :--- | :--- | :--- | :--- |
| `admin.status` | `cli.py status` | 查询模型状态与存储路径 | `{"engine": {...}, "collections": [...], "sqlite_path": "...", "status": "healthy"}` |
| `admin.collections` | `cli.py collections` | 查询 Qdrant 集合点位统计 | `{"ai_memory": {"points_count": 120}, ...}` |
| `admin.load` | `cli.py load` | 显式触发 BGE-M3 模型加载 | `{"status": "loaded", "state": "READY"}` |
| `admin.unload` | `cli.py unload` | 显式卸载模型以释放显存 | `{"status": "unloaded", "state": "UNLOADED"}` |
| `admin.snapshot` | `cli.py snapshot <col>` | 对指定集合创建快照 | `{"collection": "...", "snapshot_name": "..."}` |
| `status.metrics` | `cli.py metrics` | 查询内存性能与发件箱指标 | `{"requests_total": N, "errors_total": N, "outbox": {...}}` |
| `outbox.list_dead_letters` | `cli.py dead-letters` | 查看发件箱死信任务 | `{"dead_letters": [...], "total": N}` |
| `outbox.replay` | `cli.py replay` | 重放发件箱死信任务 | `{"replayed_count": N, "status": "ok"}` |
