# AI Memory Runtime - 详细设计与数据模型规范 (DDD)

**文档标识**：`DOC-AMR-03-DDD`  
**基线版本**：`v2.2.0`（含 `v5.0.0` 变更摘要）  
**核心内容**：SQLite DDL、Qdrant Payload 规范、IPC 帧协议

---

## 0. v5 变更摘要与数据模型演进 (v5 Data Model Evolution)

AMR v5 在数据模型层进行了全面升级（注：下文 §1 的历史 DDL 仅供历史背景参考）：

1. **统一事务发件箱表 (`memory_projection_outbox`)**：
   - 替代历史版本的 `qdrant_sync_queue`；
   - 字段包含：`id`, `target_collection`, `memory_id`, `projection_revision`, `operation` (upsert/delete), `payload`, `status` (`pending` / `processing` / `done` / `dead_letter`), `retry_count`, `claimed_by`, `claimed_at`, `dead_letter_at`, `last_error`；
   - 支持 CAS 租约声明与超时回收，支持死信隔离（`dead_letter`）。
2. **幂等控制表 (`idempotency_keys`)**：
   - 记录 `key`, `agent_id`, `request_hash`, `memory_id`, `created_at`，默认保留 7 天；
   - 相同 key + 相同载荷安全重放（`idempotent_replay: true`），相同 key + 冲突载荷返回 `-32006`。
3. **版本演进与修订表 (`memory_revisions`)**：
   - 记录每次内容更新的 `memory_id`, `version`, `content`, `change_reason`, `created_at`, `created_by_agent`。
4. **状态与字段规范**：
   - 状态使用标准全大写 6 态：`ACTIVE`, `PENDING_VERIFY`, `CONFLICT`, `HISTORICAL`, `TEMPORARY`, `DELETED`；
   - 统一使用 `type` 替代旧版 `memory_type`（支持 `decision/arch` 命名空间通配）。

---

## 1. 存储层 Schema 规范（注：旧版 v2.2 背景参考）

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
    type TEXT DEFAULT 'general',
    status TEXT DEFAULT 'ACTIVE',
    project_id TEXT,
    created_by_agent TEXT,
    created_at INTEGER,
    updated_at INTEGER,
    version INTEGER DEFAULT 1
);

-- 4. 生产发件箱表 (v5 核心表，替代旧版 qdrant_sync_queue)
CREATE TABLE IF NOT EXISTS memory_projection_outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_collection TEXT NOT NULL,
    memory_id TEXT NOT NULL,
    projection_revision INTEGER NOT NULL,
    operation TEXT NOT NULL, -- 'upsert' | 'delete'
    payload TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending', -- 'pending' | 'processing' | 'done' | 'dead_letter'
    retry_count INTEGER NOT NULL DEFAULT 0,
    claimed_by TEXT,
    claimed_at REAL,
    last_error TEXT,
    dead_letter_at REAL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

-- 5. 幂等控制表
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    memory_id TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_messages_sess ON messages(session_id);
CREATE INDEX IF NOT EXISTS idx_memories_status ON memories(status);
CREATE INDEX IF NOT EXISTS idx_outbox_status ON memory_projection_outbox(status);
```

### 1.2 Qdrant Payload 结构 (v5 规范)
```json
{
  "memory_id": "mem_20261109_775697",
  "content": "AEP architecture consensus: single writer projection with CAS",
  "type": "decision/arch",
  "status": "ACTIVE",
  "project_id": "aep-arch",
  "source_agent": "openclaw",
  "version": 1,
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
