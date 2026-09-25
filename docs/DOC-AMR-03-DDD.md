# AI Memory Runtime - 详细设计文档 (DDD)

**文档标识**：`DOC-AMR-03-DDD`  
**版本号**：`v1.0.0`  
**定案日期**：2026-09-26  
**编写方**：OpenClaw（架构团队）  
**审计方**：Hermes（架构专家）  
**执行方**：opencode（代码交付）

---

## 1. 核心数据模型与 Schema 规范

### 1.1 SQLite 会话与流水表结构 (`src/core/session_store.py`)
```sql
PRAGMA journal_mode = WAL;
PRAGMA busy_timeout = 5000;
PRAGMA foreign_keys = ON;

-- 1. 会话主表
CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    project_id TEXT,
    started_at INTEGER NOT NULL,
    ended_at INTEGER,
    message_count INTEGER DEFAULT 0,
    status TEXT DEFAULT 'active' -- active, closed, archived
);

-- 2. 消息明细表 (联合 content_hash 识别 revision)
CREATE TABLE IF NOT EXISTS messages (
    session_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    role TEXT NOT NULL,         -- user, assistant, system
    content TEXT NOT NULL,
    content_hash TEXT NOT NULL, -- SHA-256 (role + content)
    sequence INTEGER NOT NULL,
    timestamp INTEGER NOT NULL,
    PRIMARY KEY (session_id, message_id),
    FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
);

-- 3. 摄取审计与断点恢复表
CREATE TABLE IF NOT EXISTS ingest_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    processed_at INTEGER NOT NULL,
    status TEXT NOT NULL,       -- ingested, revision_updated, ignored, error
    error_msg TEXT
);

CREATE INDEX IF NOT EXISTS idx_messages_sess ON messages(session_id);
CREATE INDEX IF NOT EXISTS idx_ingest_sess ON ingest_log(session_id, message_id);
```

### 1.2 Qdrant 集合与 Payload Schema
- **集合配置（BGE-M3 1024 维 Cosine）**：
  - 默认集合：`ai_memory`、`crypto_standards`、`project_docs`。
- **Payload 严格结构定义**：
```json
{
  "memory_id": "mem_20260926_01a2b3",
  "parent_memory_id": null,
  "chunk_index": 0,
  "total_chunks": 1,
  "content": "SM4 GCM 模式下 IV 推荐为 12 字节，Tag 长度必须固定为 16 字节。",
  "memory_type": "decision",
  "status": "active",
  "superseded_by": null,
  "scope": "global",
  "project_id": "Reduction-Go",
  "source_agent": "opencode",
  "session_id": "sess_20260926_dj",
  "source_message_ids": ["msg_102", "msg_104"],
  "meta": {
    "tags": ["crypto", "sm4", "gcm"],
    "category": "standard"
  },
  "created_at": 1790352000,
  "updated_at": 1790352000
}
```

---

## 2. 状态机引擎与显存回收时序设计

### 2.1 状态转移矩阵与并发锁控制 (`src/core/engine.py`)
- 全局使用 `threading.Lock` 保护状态转移，结合 `asyncio.Event` 唤醒等待协程。
- 状态机 6 态流转：
  1. `UNLOADED`：显存释放完毕。新推理请求到达触发异步加载，状态转为 `LOADING`。
  2. `LOADING`：PyTorch 权重载入 GPU（耗时 3~5s）。新请求进入 `_wait_queue`，超时上限 25s。
  3. `READY`：模型载入完成，Worker 协程从 `asyncio.Queue` 消费批次推理。
  4. `IDLE`：推理队列为空，启动 `threading.Timer(300, trigger_unload)`。
  5. `UNLOADING`：300s 倒计时结束。先上排它锁确认队列无新入任务，注销 Timer，执行显存回收：
     ```python
     del self.model
     self.model = None
     gc.collect()
     torch.cuda.empty_cache()
     torch.cuda.ipc_collect()
     ```
  6. `ERROR`：发生未捕获的 CUDA 运行时异常，记录日志并重置为 `UNLOADED`。

---

## 3. 五层流控防御拦截链设计

```text
[UDS Socket 数据流]
        │
  [第1层: MAX_REQUEST_BYTES (4MB)] ──(超限)──> 立即切断连接, 返回 RequestEntityTooLarge
        │
  [第2层: MAX_INPUT_CHARS (32000)] ──(超限)──> 返回 400 Payload Too Large
        │
  [第3层: MAX_INPUT_TOKENS (8192)] ──(超限)──> 自动 Chunking 引擎 (Overlap 128 Tokens)
        │
  [第4层: MAX_BATCH (16)]          ──(超限)──> 切割为 batch_1, batch_2 串行执行
        │
  [第5层: MAX_QUEUE_SIZE (64)]     ──(超限)──> 触发背压, 立即返回 503 Service Unavailable
        ▼
[BGE-M3 GPU 推理核心 (FP16)]
```

---

## 4. UDS IPC 通信协议设计 (`src/interfaces/ipc/`)

### 4.1 帧封包协议
- 采用 **Length-Prefixed JSON-RPC 2.0** 格式：
  - 前 4 字节：Big-Endian 32-bit 无符号整数（`uint32`），声明后续 JSON Payload 字节长度。
  - Payload：UTF-8 编码的标准 JSON-RPC 请求或响应体。
  - 若前 4 字节声明长度 > 4,194,304 (4MB)，服务端立即报错断开。

### 4.2 双 Socket 职责划分
- **业务 Socket**：`/run/user/<uid>/qdrant-bge.sock`（权限 0600）
  - 仅处理 `memory_*` 命名空间下的 5 个标准方法。
- **管理 Socket**：`/run/user/<uid>/qdrant-bge-admin.sock`（权限 0600）
  - 仅处理 `admin_*` 运维方法及健康监控。
