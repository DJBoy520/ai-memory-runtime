# AI Memory Runtime - 详细设计与数据模型规范 (DDD)

**文档标识**：`DOC-AMR-03-DDD`  
**版本号**：`v2.2.0`  
**核心内容**：SQLite DDL、Qdrant Payload 规范、IPC 帧协议

---

## 1. 存储层 Schema 规范

### 1.1 SQLite 表结构 (`src/core/session_store.py`)
```sql
PRAGMA journal_mode = WAL;
PRAGMA busy_timeout = 5000;
PRAGMA foreign_keys = ON;

-- 1. 会话表
CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    project_id TEXT,
    started_at INTEGER NOT NULL,
    ended_at INTEGER,
    message_count INTEGER DEFAULT 0,
    status TEXT DEFAULT 'active'
);

-- 2. 消息明细表
CREATE TABLE IF NOT EXISTS messages (
    session_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    timestamp INTEGER NOT NULL,
    PRIMARY KEY (session_id, message_id),
    FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
);

-- 3. 记忆事实表 (SSOT)
CREATE TABLE IF NOT EXISTS memories (
    memory_id TEXT PRIMARY KEY,
    content TEXT NOT NULL,
    memory_type TEXT DEFAULT 'general',
    status TEXT DEFAULT 'active',
    project_id TEXT,
    created_by_agent TEXT,
    created_at INTEGER,
    updated_at INTEGER,
    version INTEGER DEFAULT 1
);

-- 4. 事务发件箱队列表
CREATE TABLE IF NOT EXISTS qdrant_sync_queue (
    task_id INTEGER PRIMARY KEY AUTOINCREMENT,
    memory_id TEXT NOT NULL,
    op_type TEXT NOT NULL, -- upsert / delete
    status TEXT DEFAULT 'pending', -- pending / processing / done / failed
    retry_count INTEGER DEFAULT 0,
    created_at REAL,
    updated_at REAL
);

CREATE INDEX IF NOT EXISTS idx_messages_sess ON messages(session_id);
CREATE INDEX IF NOT EXISTS idx_memories_status ON memories(status);
CREATE INDEX IF NOT EXISTS idx_sync_status ON qdrant_sync_queue(status);
```

### 1.2 Qdrant Payload 结构
```json
{
  "memory_id": "mem_20261008_01a2b3",
  "content": "SM4 GCM 模式下 IV 推荐为 12 字节，Tag 长度必须固定为 16 字节。",
  "memory_type": "decision",
  "status": "active",
  "project_id": "crypto-infrastructure",
  "source_agent": "hermes",
  "created_at": 1791456000,
  "updated_at": 1791456000
}
```

---

## 2. 通信协议规范 (`src/interfaces/ipc/`)

- **传输层**：本地 Unix Domain Socket (UDS)。
- **数据帧格式**：`Length-Prefixed JSON-RPC 2.0`
  - 前 4 字节：`uint32_be`（无符号 32 位大端整数），声明 Payload 字节长度；
  - 单包上限：`MAX_REQUEST_BYTES = 4,194,304` (4MB)。
- **Socket 端点**：
  - 业务接口：`$XDG_RUNTIME_DIR/qdrant-bge.sock`（默认权限 `0600`）
  - 管理接口：`$XDG_RUNTIME_DIR/qdrant-bge-admin.sock`（默认权限 `0600`）
