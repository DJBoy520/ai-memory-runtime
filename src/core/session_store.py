"""
AI Memory Runtime - Session Store & Idempotent Ingest Engine
实现 SQLite 会话管理、消息明细存储、幂等判定与审计日志
遵循 DOC-AMR-03-DDD 规范
"""

import hashlib
import json
import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from config.settings import AppConfig, StorageConfig, DatabaseConfig, load_config


CREATE_TABLES_SQL = """
-- 1. 兼容原版的 sessions 主表
CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    project_id TEXT,
    started_at INTEGER NOT NULL,
    ended_at INTEGER,
    message_count INTEGER DEFAULT 0,
    status TEXT DEFAULT 'active' -- active, closed, archived
);

-- 2. 兼容原版的消息明细表
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

-- 3. 兼容原版的摄取审计与断点恢复表
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

-- ================= v2.2 核心存储架构 (SSOT + Outbox + Audit) =================

-- 1. 原始会话表 (v2.2)
CREATE TABLE IF NOT EXISTS raw_sessions (
    session_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    project_id TEXT DEFAULT 'general',
    started_at INTEGER NOT NULL,
    ended_at INTEGER,
    status TEXT DEFAULT 'active'
);

-- 2. 原始消息证据表 (v2.2 支持证据分类与合成标记)
CREATE TABLE IF NOT EXISTS raw_messages (
    message_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES raw_sessions(session_id),
    role TEXT NOT NULL CHECK(role IN ('user', 'assistant', 'system')),
    content TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    source_type TEXT NOT NULL DEFAULT 'user_message' 
        CHECK(source_type IN ('user_message', 'assistant_message', 'tool_output', 'system', 'legacy_memory', 'imported')),
    is_synthetic INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_msg_session ON raw_messages(session_id);
CREATE INDEX IF NOT EXISTS idx_msg_hash ON raw_messages(content_hash);

-- 3. 记忆主表 (知识层 Memory SSOT - v3.0 多 Agent 共享记忆模型)
CREATE TABLE IF NOT EXISTS memories (
    memory_id TEXT PRIMARY KEY,
    qdrant_point_id TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL DEFAULT 'general',          -- 支持 namespace/name (如 decision/arch) 或扁平词
    conflict_policy TEXT NOT NULL DEFAULT 'coexist',
    subject TEXT DEFAULT '',
    predicate TEXT DEFAULT '',
    object TEXT,
    content TEXT NOT NULL,
    valid_from INTEGER NOT NULL DEFAULT 0,
    valid_to INTEGER,
    validity_type TEXT NOT NULL DEFAULT 'open_ended',
    confidence REAL NOT NULL DEFAULT 1.0,
    importance REAL NOT NULL DEFAULT 0.5,
    mention_count INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'ACTIVE',          -- 6态: ACTIVE, PENDING_VERIFY, CONFLICT, HISTORICAL, TEMPORARY, DELETED
    superseded_by TEXT REFERENCES memories(memory_id),
    superseded_at INTEGER,
    previous_version_id TEXT,
    root_memory_id TEXT,
    protection_level TEXT NOT NULL DEFAULT 'NONE',
    project_id TEXT NOT NULL DEFAULT 'global',
    scope TEXT NOT NULL DEFAULT 'global',
    source_agent TEXT NOT NULL DEFAULT 'system',
    created_by_agent TEXT DEFAULT 'system',
    updated_by_agent TEXT DEFAULT 'system',
    version INTEGER NOT NULL DEFAULT 1,
    content_hash TEXT,
    curation_batch_id TEXT,
    last_reconciled_at INTEGER,
    deleted_at INTEGER,
    deleted_by TEXT,
    deletion_reason TEXT,
    source_refs TEXT DEFAULT '[]',                 -- 弱引用 JSON 数组，记录关联会话与来源
    conflicts_with TEXT DEFAULT '[]',              -- 冲突对立记忆 ID 列表 (JSON)
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

-- 3.1 记忆修订历史快照表 (v3.0 Append-only 内容版本快照)
CREATE TABLE IF NOT EXISTS memory_revisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    memory_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    content TEXT NOT NULL,
    updated_by_agent TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    change_reason TEXT,
    FOREIGN KEY(memory_id) REFERENCES memories(memory_id)
);
CREATE INDEX IF NOT EXISTS idx_revisions_mem ON memory_revisions(memory_id);
CREATE INDEX IF NOT EXISTS idx_mem_lookup ON memories(project_id, type, status);
CREATE INDEX IF NOT EXISTS idx_mem_subject ON memories(subject, predicate);
CREATE INDEX IF NOT EXISTS idx_mem_point ON memories(qdrant_point_id);
CREATE INDEX IF NOT EXISTS idx_memories_status ON memories(status);
CREATE INDEX IF NOT EXISTS idx_memories_content_hash ON memories(content_hash);
CREATE INDEX IF NOT EXISTS idx_memories_reconciled ON memories(last_reconciled_at);

-- 4. 记忆与证据溯源多对多关联表
CREATE TABLE IF NOT EXISTS memory_evidence (
    memory_id TEXT NOT NULL REFERENCES memories(memory_id),
    message_id TEXT NOT NULL REFERENCES raw_messages(message_id),
    session_id TEXT NOT NULL,
    evidence_strength REAL NOT NULL DEFAULT 0.5,
    linked_at INTEGER NOT NULL,
    PRIMARY KEY (memory_id, message_id)
);

-- 5. Transactional Outbox 异步同步发件箱队列 (v2.2 原版保留兼容)
CREATE TABLE IF NOT EXISTS qdrant_sync_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    memory_id TEXT NOT NULL,
    qdrant_point_id TEXT NOT NULL,
    op_type TEXT NOT NULL CHECK(op_type IN ('upsert', 'delete', 'update_payload')),
    payload_snapshot TEXT,
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'synced', 'failed')),
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sync_status ON qdrant_sync_queue(status, retry_count);
CREATE INDEX IF NOT EXISTS idx_sync_order ON qdrant_sync_queue(memory_id, id);

-- 6. 多投影 Outbox 队列 (RFC-003: memory_projection_outbox)
CREATE TABLE IF NOT EXISTS memory_projection_outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    memory_id TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    projection_type TEXT NOT NULL DEFAULT 'qdrant_main',
    op_type TEXT NOT NULL, -- upsert, delete, update_payload
    payload_snapshot TEXT,
    status TEXT NOT NULL DEFAULT 'pending', -- pending, processing, completed, dead_letter
    retry_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at INTEGER DEFAULT 0,
    last_error TEXT,
    dead_letter_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(memory_id, version, projection_type, op_type)
);
CREATE INDEX IF NOT EXISTS idx_outbox_queue ON memory_projection_outbox(status, next_retry_at);

-- 7. 治理候选表 (RFC-003 & RFC-005: curation_candidates)
CREATE TABLE IF NOT EXISTS curation_candidates (
    candidate_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    memory_id TEXT,
    session_window_json TEXT,
    operation TEXT NOT NULL, -- EXTRACT, MERGE, UPDATE, SUPERSEDE, LINK, ARCHIVE
    current_status TEXT,
    proposed_status TEXT,
    matched_rule_id TEXT,
    reason TEXT,
    evidence_snapshot TEXT,
    subject TEXT,
    predicate TEXT,
    object TEXT,
    content TEXT,
    evidence_source_ids TEXT, -- JSON Array
    extracted_spans TEXT,      -- JSON Array
    message_disposition TEXT,  -- JSON Array
    rationale TEXT,
    proposed_relation_json TEXT,
    prompt_version TEXT,
    model_name TEXT,
    llm_confidence REAL,
    state TEXT NOT NULL DEFAULT 'PROPOSED', -- PROPOSED, APPROVED, REJECTED, APPLIED
    rejection_reason TEXT,
    created_at INTEGER NOT NULL,
    processed_at INTEGER,
    FOREIGN KEY(memory_id) REFERENCES memories(memory_id)
);
CREATE INDEX IF NOT EXISTS idx_curation_batch ON curation_candidates(batch_id, state);

-- 8. 对账检查点与元数据表 (RFC-003: reconciliation_checkpoints)
CREATE TABLE IF NOT EXISTS reconciliation_checkpoints (
    batch_id TEXT PRIMARY KEY,
    start_watermark_ts INTEGER NOT NULL,
    end_watermark_ts INTEGER,
    sqlite_memory_count INTEGER NOT NULL,
    qdrant_active_count INTEGER NOT NULL,
    proposed_count INTEGER DEFAULT 0,
    applied_count INTEGER DEFAULT 0,
    embedding_model TEXT NOT NULL DEFAULT 'BGE-M3',
    embedding_dimension INTEGER NOT NULL DEFAULT 1024,
    distance_metric TEXT NOT NULL DEFAULT 'Cosine',
    status TEXT NOT NULL, -- RUNNING, COMPLETED, FAILED
    error_message TEXT,
    created_at INTEGER NOT NULL,
    completed_at INTEGER
);

-- 9. 治理与生命周期审计日志表
CREATE TABLE IF NOT EXISTS memory_audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    memory_id TEXT NOT NULL,
    action TEXT NOT NULL CHECK(action IN ('create', 'promote', 'merge', 'update', 'supersede', 'archive', 'delete')),
    operator TEXT NOT NULL,
    detail TEXT,
    timestamp INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_mem_audit ON memory_audit_log(memory_id, timestamp);
"""


def compute_content_hash(role: str, content: str) -> str:
    """计算 content_hash = sha256(role + ':' + content)"""
    raw = f"{role}:{content}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


class SessionStore:
    """
    SessionStore 负责对 SQLite 进行会话流水及审计日志的读写与幂等控制。
    """

    def __init__(
        self,
        config: Optional[Union[AppConfig, StorageConfig, DatabaseConfig, str, Path]] = None,
        db_path: Optional[Union[str, Path]] = None,
        busy_timeout: Optional[int] = None,
        wal_enabled: Optional[bool] = None,
        wal_mode: Optional[bool] = None,
    ):
        # 兼容位置参数传入：SessionStore(config) 或 SessionStore(db_path)
        actual_config: Optional[Union[AppConfig, StorageConfig, DatabaseConfig]] = None
        actual_db_path: Optional[Union[str, Path]] = None

        if isinstance(config, (AppConfig, StorageConfig)):
            actual_config = config
            actual_db_path = db_path
        elif isinstance(config, (str, Path)):
            actual_db_path = config
            if isinstance(db_path, (AppConfig, StorageConfig)):
                actual_config = db_path
        else:
            actual_config = None
            actual_db_path = db_path

        if actual_config is None:
            app_cfg = load_config()
            storage_cfg = app_cfg.storage
        elif isinstance(actual_config, AppConfig):
            storage_cfg = actual_config.storage
        else:
            storage_cfg = actual_config

        self.db_path = Path(actual_db_path or storage_cfg.sqlite_path)
        self.busy_timeout = busy_timeout if busy_timeout is not None else storage_cfg.busy_timeout
        effective_wal = wal_enabled if wal_enabled is not None else wal_mode
        self.wal_enabled = effective_wal if effective_wal is not None else getattr(storage_cfg, "wal_enabled", getattr(storage_cfg, "wal_mode", True))
        self.wal_mode = self.wal_enabled

        # 确保父级目录存在
        if not self.db_path.parent.exists():
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._lock = threading.RLock()
        self._conn: Optional[sqlite3.Connection] = None
        self._init_db()

    def get_connection(self) -> sqlite3.Connection:
        """获取或创建当前连接（支持上下文管理器与直接调用）"""
        return self._get_connection()

    def _get_connection(self) -> sqlite3.Connection:
        """获取或创建当前长连接"""
        if self._conn is None:
            self._conn = sqlite3.connect(
                str(self.db_path),
                timeout=self.busy_timeout / 1000.0,
                check_same_thread=False,
            )
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def _init_db(self) -> None:
        """初始化 SQLite 数据库及 Schema，支持幂等增量迁移"""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            # PRAGMA 设置
            if self.wal_mode:
                cursor.execute("PRAGMA journal_mode = WAL;")
            cursor.execute(f"PRAGMA busy_timeout = {self.busy_timeout};")
            cursor.execute("PRAGMA foreign_keys = ON;")
            # 先跑增量迁移（若老库已存在 memories 表但缺少新列，避免 CREATE INDEX 直接报错）
            self._migrate_schema(cursor)
            cursor.executescript(CREATE_TABLES_SQL)
            self._migrate_schema(cursor)
            conn.commit()

    def _migrate_schema(self, cursor: sqlite3.Cursor) -> None:
        """
        幂等平滑表迁移：检查表结构字段并为旧数据库增量添加缺失列。
        """
        # 1. 检查 raw_messages 列 (如 source_type, is_synthetic)
        cursor.execute("PRAGMA table_info(raw_messages);")
        raw_msg_cols = {row["name"] for row in cursor.fetchall()}
        if raw_msg_cols:
            if "source_type" not in raw_msg_cols:
                cursor.execute(
                    "ALTER TABLE raw_messages ADD COLUMN source_type TEXT NOT NULL DEFAULT 'user_message';"
                )
            if "is_synthetic" not in raw_msg_cols:
                cursor.execute(
                    "ALTER TABLE raw_messages ADD COLUMN is_synthetic INTEGER NOT NULL DEFAULT 0;"
                )

        # 2. 检查 memories 列 (确保所有 v2.2 及 RFC-003, RFC-005 治理与版本化字段存在)
        cursor.execute("PRAGMA table_info(memories);")
        mem_cols = {row["name"] for row in cursor.fetchall()}
        if mem_cols:
            v2_mem_columns = {
                "conflict_policy": "TEXT NOT NULL DEFAULT 'coexist'",
                "validity_type": "TEXT NOT NULL DEFAULT 'open_ended'",
                "version": "INTEGER NOT NULL DEFAULT 1",
                "previous_version_id": "TEXT",
                "root_memory_id": "TEXT",
                "superseded_by": "TEXT",
                "superseded_at": "INTEGER",
                "protection_level": "TEXT NOT NULL DEFAULT 'NONE'",
                "content_hash": "TEXT",
                "curation_batch_id": "TEXT",
                "last_reconciled_at": "INTEGER",
                "deleted_at": "INTEGER",
                "deleted_by": "TEXT",
                "deletion_reason": "TEXT",
                "created_by_agent": "TEXT DEFAULT 'system'",
                "updated_by_agent": "TEXT DEFAULT 'system'",
                "source_refs": "TEXT DEFAULT '[]'",
                "conflicts_with": "TEXT DEFAULT '[]'",
            }
            for col_name, col_def in v2_mem_columns.items():
                if col_name not in mem_cols:
                    cursor.execute(f"ALTER TABLE memories ADD COLUMN {col_name} {col_def};")

            # 补充索引
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_memories_status ON memories(status);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_memories_content_hash ON memories(content_hash);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_memories_reconciled ON memories(last_reconciled_at);")

        # 2.1 确保 memory_revisions 快照表存在
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS memory_revisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                memory_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                content TEXT NOT NULL,
                updated_by_agent TEXT NOT NULL,
                updated_at INTEGER NOT NULL,
                change_reason TEXT,
                FOREIGN KEY(memory_id) REFERENCES memories(memory_id)
            );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_revisions_mem ON memory_revisions(memory_id);")

        # 3. 确保 RFC-003 / RFC-005 核心扩展表存在
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS memory_projection_outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                memory_id TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                projection_type TEXT NOT NULL DEFAULT 'qdrant_main',
                op_type TEXT NOT NULL,
                payload_snapshot TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                retry_count INTEGER NOT NULL DEFAULT 0,
                next_retry_at INTEGER DEFAULT 0,
                last_error TEXT,
                dead_letter_at INTEGER,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE(memory_id, version, projection_type, op_type)
            );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_outbox_queue ON memory_projection_outbox(status, next_retry_at);")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS curation_candidates (
                candidate_id TEXT PRIMARY KEY,
                batch_id TEXT NOT NULL,
                memory_id TEXT,
                session_window_json TEXT,
                operation TEXT NOT NULL,
                current_status TEXT,
                proposed_status TEXT,
                matched_rule_id TEXT,
                reason TEXT,
                evidence_snapshot TEXT,
                subject TEXT,
                predicate TEXT,
                object TEXT,
                content TEXT,
                evidence_source_ids TEXT,
                extracted_spans TEXT,
                message_disposition TEXT,
                rationale TEXT,
                proposed_relation_json TEXT,
                prompt_version TEXT,
                model_name TEXT,
                llm_confidence REAL,
                state TEXT NOT NULL DEFAULT 'PROPOSED',
                rejection_reason TEXT,
                created_at INTEGER NOT NULL,
                processed_at INTEGER,
                FOREIGN KEY(memory_id) REFERENCES memories(memory_id)
            );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_curation_batch ON curation_candidates(batch_id, state);")

        # 检查 curation_candidates 表是否缺失 RFC-005 字段（如果表是在 RFC-003 时已创建的旧结构）
        cursor.execute("PRAGMA table_info(curation_candidates);")
        cand_cols = {row["name"] for row in cursor.fetchall()}
        if cand_cols:
            cand_new_cols = {
                "session_window_json": "TEXT",
                "operation": "TEXT DEFAULT 'UPDATE'",
                "subject": "TEXT",
                "predicate": "TEXT",
                "object": "TEXT",
                "content": "TEXT",
                "evidence_source_ids": "TEXT",
                "extracted_spans": "TEXT",
                "message_disposition": "TEXT",
                "rationale": "TEXT",
                "proposed_relation_json": "TEXT",
                "prompt_version": "TEXT",
                "model_name": "TEXT",
                "llm_confidence": "REAL",
                "rejection_reason": "TEXT",
            }
            for c_name, c_def in cand_new_cols.items():
                if c_name not in cand_cols:
                    cursor.execute(f"ALTER TABLE curation_candidates ADD COLUMN {c_name} {c_def};")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS reconciliation_checkpoints (
                batch_id TEXT PRIMARY KEY,
                start_watermark_ts INTEGER NOT NULL,
                end_watermark_ts INTEGER,
                sqlite_memory_count INTEGER NOT NULL,
                qdrant_active_count INTEGER NOT NULL,
                proposed_count INTEGER DEFAULT 0,
                applied_count INTEGER DEFAULT 0,
                embedding_model TEXT NOT NULL DEFAULT 'BGE-M3',
                embedding_dimension INTEGER NOT NULL DEFAULT 1024,
                distance_metric TEXT NOT NULL DEFAULT 'Cosine',
                status TEXT NOT NULL,
                error_message TEXT,
                created_at INTEGER NOT NULL,
                completed_at INTEGER
            );
        """)

    def ingest_messages(
        self,
        session_id: str,
        agent_id: str,
        project_id: Optional[str],
        messages: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        幂等摄取会话消息。
        - 若不存在：插入 messages 表，ingest_log 记 status='ingested'，inserted + 1
        - 若存在且 content_hash 相同：忽略，ingest_log 记 status='ignored'，ignored + 1
        - 若存在但 content_hash 不同：判定为 revision，更新 messages 表，ingest_log 记 status='revision_updated'，revision_updated + 1
        """
        now = int(time.time())
        inserted = 0
        revision_updated = 0
        ignored = 0
        total_received = len(messages)

        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            try:
                # 1. 检查/创建 session 记录
                cursor.execute(
                    "SELECT session_id, started_at, ended_at, message_count FROM sessions WHERE session_id = ?",
                    (session_id,),
                )
                sess_row = cursor.fetchone()

                earliest_ts = now
                latest_ts = now
                if messages:
                    msg_timestamps = [m.get("timestamp", now) for m in messages if isinstance(m.get("timestamp"), (int, float))]
                    if msg_timestamps:
                        earliest_ts = int(min(msg_timestamps))
                        latest_ts = int(max(msg_timestamps))

                if sess_row is None:
                    cursor.execute(
                        """
                        INSERT INTO sessions (session_id, agent_id, project_id, started_at, ended_at, message_count, status)
                        VALUES (?, ?, ?, ?, ?, 0, 'active')
                        """,
                        (session_id, agent_id, project_id, earliest_ts, latest_ts),
                    )
                else:
                    # 如果会话已存在，若提供了更新的 project_id 或 agent_id 可同步，也可以拓展 ended_at
                    current_ended_at = sess_row["ended_at"] or 0
                    new_ended_at = max(current_ended_at, latest_ts)
                    cursor.execute(
                        """
                        UPDATE sessions
                        SET ended_at = ?, project_id = COALESCE(?, project_id)
                        WHERE session_id = ?
                        """,
                        (new_ended_at, project_id, session_id),
                    )

                # 2. 逐条处理消息
                for idx, msg in enumerate(messages):
                    msg_id = msg.get("message_id") or msg.get("id") or f"{session_id}_{idx+1}_{now}"
                    role = msg.get("role", "user")
                    content = msg.get("content", "")
                    if isinstance(content, list):
                        content = "\n".join(str(p.get("text", p) if isinstance(p, dict) else p) for p in content)
                    seq = msg.get("sequence", idx + 1)
                    ts = int(msg.get("timestamp", now))
                    c_hash = compute_content_hash(role, content)

                    # 查询已有消息
                    cursor.execute(
                        "SELECT content_hash FROM messages WHERE session_id = ? AND message_id = ?",
                        (session_id, msg_id),
                    )
                    existing = cursor.fetchone()

                    if existing is None:
                        # 新增插入
                        cursor.execute(
                            """
                            INSERT INTO messages (session_id, message_id, role, content, content_hash, sequence, timestamp)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            """,
                            (session_id, msg_id, role, content, c_hash, seq, ts),
                        )
                        cursor.execute(
                            """
                            INSERT INTO ingest_log (session_id, message_id, processed_at, status, error_msg)
                            VALUES (?, ?, ?, 'ingested', NULL)
                            """,
                            (session_id, msg_id, now),
                        )
                        inserted += 1
                    else:
                        if existing["content_hash"] == c_hash:
                            # 完全相同，忽略
                            cursor.execute(
                                """
                                INSERT INTO ingest_log (session_id, message_id, processed_at, status, error_msg)
                                VALUES (?, ?, ?, 'ignored', NULL)
                                """,
                                (session_id, msg_id, now),
                            )
                            ignored += 1
                        else:
                            # 发生 revision 更新
                            cursor.execute(
                                """
                                UPDATE messages
                                SET role = ?, content = ?, content_hash = ?, sequence = ?, timestamp = ?
                                WHERE session_id = ? AND message_id = ?
                                """,
                                (role, content, c_hash, seq, ts, session_id, msg_id),
                            )
                            cursor.execute(
                                """
                                INSERT INTO ingest_log (session_id, message_id, processed_at, status, error_msg)
                                VALUES (?, ?, ?, 'revision_updated', NULL)
                                """,
                                (session_id, msg_id, now),
                            )
                            revision_updated += 1

                # 3. 重新聚合更新 sessions 的 message_count 与 ended_at
                cursor.execute(
                    "SELECT COUNT(*) as cnt, MAX(timestamp) as max_ts FROM messages WHERE session_id = ?",
                    (session_id,),
                )
                stat_row = cursor.fetchone()
                total_cnt = stat_row["cnt"] if stat_row else 0
                max_ts = stat_row["max_ts"] if stat_row and stat_row["max_ts"] is not None else latest_ts

                cursor.execute(
                    """
                    UPDATE sessions
                    SET message_count = ?, ended_at = MAX(COALESCE(ended_at, 0), ?)
                    WHERE session_id = ?
                    """,
                    (total_cnt, max_ts, session_id),
                )

                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e

        return {
            "session_id": session_id,
            "total_received": total_received,
            "inserted": inserted,
            "revision_updated": revision_updated,
            "ignored": ignored,
            "status": "success",
        }

    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """获取单个会话主表信息"""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM sessions WHERE session_id = ?", (session_id,))
            row = cursor.fetchone()
            if row is None:
                return None
            return dict(row)

    def get_messages(
        self, session_id: str, message_ids: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        获取指定会话下的消息列表。
        若指定 message_ids，则仅返回过滤出的消息；否则按 sequence、timestamp 升序返回全量。
        """
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            if message_ids is not None:
                if not message_ids:
                    return []
                placeholders = ",".join("?" for _ in message_ids)
                query = f"""
                    SELECT message_id, role, content, content_hash, sequence, timestamp
                    FROM messages
                    WHERE session_id = ? AND message_id IN ({placeholders})
                    ORDER BY sequence ASC, timestamp ASC
                """
                cursor.execute(query, [session_id] + list(message_ids))
            else:
                cursor.execute(
                    """
                    SELECT message_id, role, content, content_hash, sequence, timestamp
                    FROM messages
                    WHERE session_id = ?
                    ORDER BY sequence ASC, timestamp ASC
                    """,
                    (session_id,),
                )
            rows = cursor.fetchall()
            return [dict(r) for r in rows]

    def get_ingest_logs(
        self, session_id: str, message_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """获取摄取审计日志（支持单消息过滤）"""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            if message_id:
                cursor.execute(
                    """
                    SELECT id, session_id, message_id, processed_at, status, error_msg
                    FROM ingest_log
                    WHERE session_id = ? AND message_id = ?
                    ORDER BY id ASC
                    """,
                    (session_id, message_id),
                )
            else:
                cursor.execute(
                    """
                    SELECT id, session_id, message_id, processed_at, status, error_msg
                    FROM ingest_log
                    WHERE session_id = ?
                    ORDER BY id ASC
                    """,
                    (session_id,),
                )
            rows = cursor.fetchall()
            return [dict(r) for r in rows]

    # =========================================================================
    # v2.2 SQLite SSOT & Outbox / Audit Storage Methods
    # =========================================================================

    def record_raw_session(
        self,
        session_id: str,
        agent_id: str,
        project_id: str = "general",
        started_at: Optional[int] = None,
        ended_at: Optional[int] = None,
        status: str = "active",
    ) -> Dict[str, Any]:
        """记录或更新 raw_sessions 表中的原始会话"""
        now = int(time.time())
        s_at = started_at if started_at is not None else now
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO raw_sessions (session_id, agent_id, project_id, started_at, ended_at, status)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    agent_id = excluded.agent_id,
                    project_id = excluded.project_id,
                    ended_at = COALESCE(excluded.ended_at, raw_sessions.ended_at),
                    status = excluded.status
                """,
                (session_id, agent_id, project_id, s_at, ended_at, status),
            )
            conn.commit()
            return {
                "session_id": session_id,
                "agent_id": agent_id,
                "project_id": project_id,
                "started_at": s_at,
                "ended_at": ended_at,
                "status": status,
            }

    def record_raw_message(
        self,
        message_id: str,
        session_id: str,
        role: str,
        content: str,
        sequence: int,
        source_type: str = "user_message",
        is_synthetic: int = 0,
        created_at: Optional[int] = None,
    ) -> Dict[str, Any]:
        """向 raw_messages 表写入一条原始消息证据"""
        now = int(time.time())
        c_at = created_at if created_at is not None else now
        c_hash = compute_content_hash(role, content)
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO raw_messages (
                    message_id, session_id, role, content, content_hash, sequence, source_type, is_synthetic, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(message_id) DO UPDATE SET
                    role = excluded.role,
                    content = excluded.content,
                    content_hash = excluded.content_hash,
                    sequence = excluded.sequence,
                    source_type = excluded.source_type,
                    is_synthetic = excluded.is_synthetic
                """,
                (message_id, session_id, role, content, c_hash, sequence, source_type, is_synthetic, c_at),
            )
            conn.commit()
            return {
                "message_id": message_id,
                "session_id": session_id,
                "role": role,
                "content_hash": c_hash,
                "sequence": sequence,
                "source_type": source_type,
                "is_synthetic": is_synthetic,
                "created_at": c_at,
            }

    def create_memory(
        self,
        memory_id: str,
        subject: str,
        predicate: str,
        content: str,
        type: str,
        conflict_policy: str = "coexist",
        object: Optional[str] = None,
        valid_from: Optional[int] = None,
        valid_to: Optional[int] = None,
        validity_type: str = "open_ended",
        confidence: float = 0.8,
        importance: float = 0.5,
        mention_count: int = 1,
        status: str = "candidate",
        superseded_by: Optional[str] = None,
        superseded_at: Optional[int] = None,
        previous_version_id: Optional[str] = None,
        root_memory_id: Optional[str] = None,
        protection_level: str = "NONE",
        project_id: str = "general",
        scope: str = "global",
        source_agent: str = "default_agent",
        version: int = 1,
        qdrant_point_id: Optional[str] = None,
        evidence: Optional[List[Dict[str, Any]]] = None,
        operator: str = "engine",
        audit_detail: Optional[Dict[str, Any]] = None,
        qdrant_payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        原子创建记忆实体：
        1. 写入 memories (知识主表 SSOT)
        2. 写入 memory_evidence (多对多关联)
        3. 同一事务写入 qdrant_sync_queue (Transactional Outbox)
        4. 同一事务写入 memory_audit_log (审计日志)
        保证数据一致性与发件箱原子性。

        qdrant_payload: 调用方已自行构造投影 payload（如 legacy memory.record 的
        带 scope/tags/分块溯源的 payload）时传入，Outbox 回放将原样使用该快照，
        避免 Worker 用默认 schema 覆盖调用方的投影。
        """
        now = int(time.time())
        v_from = valid_from if valid_from is not None else now
        point_id = qdrant_point_id or str(uuid.uuid5(uuid.NAMESPACE_URL, memory_id))
        c_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()

        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            try:
                # 1. 插入 memories
                cursor.execute(
                    """
                    INSERT INTO memories (
                        memory_id, qdrant_point_id, type, conflict_policy,
                        subject, predicate, object, content,
                        valid_from, valid_to, validity_type,
                        confidence, importance, mention_count,
                        status, superseded_by, superseded_at,
                        previous_version_id, root_memory_id, protection_level,
                        project_id, scope, source_agent, version,
                        content_hash, curation_batch_id, last_reconciled_at,
                        deleted_at, deleted_by, deletion_reason,
                        created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, ?, ?)
                    """,
                    (
                        memory_id, point_id, type, conflict_policy,
                        subject, predicate, object, content,
                        v_from, valid_to, validity_type,
                        confidence, importance, mention_count,
                        status, superseded_by, superseded_at,
                        previous_version_id, root_memory_id or memory_id, protection_level,
                        project_id, scope, source_agent, version,
                        c_hash,
                        now, now
                    ),
                )

                # 2. 插入 memory_evidence (多对多关联表)
                #    evidence 为弱引用：仅关联已摄取入库的原始消息，未入库的消息跳过（FK 约束要求 raw_messages 先存在）
                if evidence:
                    for ev in evidence:
                        cursor.execute(
                            "SELECT 1 FROM raw_messages WHERE message_id = ?",
                            (ev["message_id"],),
                        )
                        if cursor.fetchone() is None:
                            continue
                        cursor.execute(
                            """
                            INSERT INTO memory_evidence (
                                memory_id, message_id, session_id, evidence_strength, linked_at
                            )
                            VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT(memory_id, message_id) DO UPDATE SET
                                evidence_strength = excluded.evidence_strength,
                                linked_at = excluded.linked_at
                            """,
                            (
                                memory_id,
                                ev["message_id"],
                                ev["session_id"],
                                float(ev.get("evidence_strength", 0.5)),
                                int(ev.get("linked_at", now)),
                            ),
                        )

                # 3. 构造 payload 快照并写入 qdrant_sync_queue (Transactional Outbox)
                payload_snapshot = json.dumps(qdrant_payload or {
                    "memory_id": memory_id,
                    "point_id": point_id,
                    "type": type,
                    "subject": subject,
                    "predicate": predicate,
                    "object": object,
                    "content": content,
                    "project_id": project_id,
                    "scope": scope,
                    "status": status,
                    "confidence": confidence,
                    "importance": importance,
                    "version": version,
                }, ensure_ascii=False)

                cursor.execute(
                    """
                    INSERT INTO qdrant_sync_queue (
                        memory_id, qdrant_point_id, op_type, payload_snapshot,
                        status, retry_count, last_error, created_at, updated_at
                    )
                    VALUES (?, ?, 'upsert', ?, 'pending', 0, NULL, ?, ?)
                    """,
                    (memory_id, point_id, payload_snapshot, now, now),
                )

                # 4. 插入 memory_audit_log
                detail_str = json.dumps(audit_detail or {"action": "create_memory", "status": status}, ensure_ascii=False)
                cursor.execute(
                    """
                    INSERT INTO memory_audit_log (
                        memory_id, action, operator, detail, timestamp
                    )
                    VALUES (?, 'create', ?, ?, ?)
                    """,
                    (memory_id, operator, detail_str, now),
                )

                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e

        return self.get_memory(memory_id)  # type: ignore[return-value]

    def get_memory(self, memory_id: str) -> Optional[Dict[str, Any]]:
        """读取完整的记忆实体，包含关联的 evidence 列表"""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM memories WHERE memory_id = ?", (memory_id,))
            row = cursor.fetchone()
            if row is None:
                return None
            mem_dict = dict(row)

            # 查询 evidence 列表
            cursor.execute(
                """
                SELECT message_id, session_id, evidence_strength, linked_at
                FROM memory_evidence
                WHERE memory_id = ?
                ORDER BY linked_at ASC
                """,
                (memory_id,),
            )
            ev_rows = cursor.fetchall()
            mem_dict["evidence"] = [dict(ev) for ev in ev_rows]
            return mem_dict

    def update_memory_status(
        self,
        memory_id: str,
        status: str,
        superseded_by: Optional[str] = None,
        operator: str = "engine",
        detail: Optional[Dict[str, Any]] = None,
        deleted_by: Optional[str] = None,
        deletion_reason: Optional[str] = None,
        superseded_at: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        更新记忆状态、替换指向，并原子追加 Outbox 任务与 audit log。
        若状态变为 deleted，则向 Outbox 追加 op_type='delete'，否则追加 op_type='update_payload'。
        """
        now = int(time.time())
        action_map = {
            "active": "promote",
            "superseded": "supersede",
            "archived": "archive",
            "deleted": "delete",
            "candidate": "update",
        }
        action = action_map.get(status, "update")

        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM memories WHERE memory_id = ?", (memory_id,))
            row = cursor.fetchone()
            if row is None:
                return None

            point_id = row["qdrant_point_id"]
            d_at = now if status == "deleted" else row["deleted_at"]
            d_by = deleted_by if status == "deleted" else row["deleted_by"]
            d_reason = deletion_reason if status == "deleted" else row["deletion_reason"]
            sup_at = superseded_at if superseded_at is not None else (now if status == "superseded" or superseded_by else row["superseded_at"])

            try:
                # 1. 更新 memories 主表
                cursor.execute(
                    """
                    UPDATE memories
                    SET status = ?,
                        superseded_by = COALESCE(?, superseded_by),
                        superseded_at = COALESCE(?, superseded_at),
                        deleted_at = ?,
                        deleted_by = ?,
                        deletion_reason = ?,
                        updated_at = ?
                    WHERE memory_id = ?
                    """,
                    (status, superseded_by, sup_at, d_at, d_by, d_reason, now, memory_id),
                )

                # 2. 追加 qdrant_sync_queue 发件箱任务
                op_type = "delete" if status == "deleted" else "update_payload"
                payload_snapshot = json.dumps({
                    "memory_id": memory_id,
                    "status": status,
                    "superseded_by": superseded_by or row["superseded_by"],
                    "updated_at": now,
                }, ensure_ascii=False)

                cursor.execute(
                    """
                    INSERT INTO qdrant_sync_queue (
                        memory_id, qdrant_point_id, op_type, payload_snapshot,
                        status, retry_count, last_error, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, 'pending', 0, NULL, ?, ?)
                    """,
                    (memory_id, point_id, op_type, payload_snapshot, now, now),
                )

                # 3. 追加 memory_audit_log
                audit_dict = detail or {}
                audit_dict.update({
                    "old_status": row["status"],
                    "new_status": status,
                    "superseded_by": superseded_by,
                    "deletion_reason": d_reason,
                })
                cursor.execute(
                    """
                    INSERT INTO memory_audit_log (
                        memory_id, action, operator, detail, timestamp
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (memory_id, action, operator, json.dumps(audit_dict, ensure_ascii=False), now),
                )

                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e

        return self.get_memory(memory_id)

    def fetch_pending_sync_tasks(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        拉取待同步发件箱队列任务。
        按 memory_id 和 id 升序排列，保证单个 memory_id 上的操作严格保序。
        仅拉取 status IN ('pending', 'failed') 且 retry_count < 5 的任务。
        """
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, memory_id, qdrant_point_id, op_type, payload_snapshot,
                       status, retry_count, last_error, created_at, updated_at
                FROM qdrant_sync_queue
                WHERE status = 'pending' OR (status = 'failed' AND retry_count < 5)
                ORDER BY memory_id ASC, id ASC
                LIMIT ?
                """,
                (limit,),
            )
            rows = cursor.fetchall()
            return [dict(r) for r in rows]

    def mark_sync_task_done(self, task_id: int) -> bool:
        """标记同步任务已完成 (synced)"""
        now = int(time.time())
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE qdrant_sync_queue
                SET status = 'synced', updated_at = ?
                WHERE id = ?
                """,
                (now, task_id),
            )
            conn.commit()
            return cursor.rowcount > 0

    def mark_sync_task_failed(self, task_id: int, error: str) -> bool:
        """标记同步任务失败并增加重试计数"""
        now = int(time.time())
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE qdrant_sync_queue
                SET status = 'failed',
                    retry_count = retry_count + 1,
                    last_error = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (str(error), now, task_id),
            )
            conn.commit()
            return cursor.rowcount > 0

    def mark_pending_sync_tasks_done(self, memory_id: str) -> int:
        """
        将指定 memory_id 上所有 pending/failed 的同步任务直接置为 synced，返回受影响行数。

        用于调用方已把投影直接写入目标集合的场景（qdrant_sync_queue 不携带 collection 维度，
        Worker 只能推送默认集合 ai_memory）；若不关闭，同一点位会被重复推送到默认集合造成跨集合污染。
        """
        now = int(time.time())
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE qdrant_sync_queue
                SET status = 'synced', updated_at = ?
                WHERE memory_id = ? AND status IN ('pending', 'failed')
                """,
                (now, memory_id),
            )
            conn.commit()
            return cursor.rowcount

    def get_audit_logs(self, memory_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """获取记忆治理审计日志"""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            if memory_id:
                cursor.execute(
                    """
                    SELECT id, memory_id, action, operator, detail, timestamp
                    FROM memory_audit_log
                    WHERE memory_id = ?
                    ORDER BY id ASC
                    """,
                    (memory_id,),
                )
            else:
                cursor.execute(
                    """
                    SELECT id, memory_id, action, operator, detail, timestamp
                    FROM memory_audit_log
                    ORDER BY id ASC
                    """
                )
            rows = cursor.fetchall()
            return [dict(r) for r in rows]

    def checkpoint(self, mode: str = "PASSIVE") -> None:
        """
        强制触发 SQLite WAL 检查点，将 WAL 文件数据刷回主库。
        mode 可选: PASSIVE, FULL, RESTART, TRUNCATE
        """
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA wal_checkpoint({mode});")

    # =========================================================================
    # v3.0 AMR Core Methods (6-State, Revision History, Optimistic Lock, Outbox)
    # =========================================================================

    def record_memory_revision(
        self,
        memory_id: str,
        version: int,
        content: str,
        updated_by_agent: str,
        change_reason: Optional[str] = None,
        updated_at: Optional[int] = None,
    ) -> Dict[str, Any]:
        """记录一条不可变的记忆修订历史快照"""
        now = updated_at or int(time.time())
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO memory_revisions (
                    memory_id, version, content, updated_by_agent, updated_at, change_reason
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (memory_id, version, content, updated_by_agent, now, change_reason),
            )
            conn.commit()
            return {
                "id": cursor.lastrowid,
                "memory_id": memory_id,
                "version": version,
                "content": content,
                "updated_by_agent": updated_by_agent,
                "updated_at": now,
                "change_reason": change_reason,
            }

    def get_memory_revisions(self, memory_id: str) -> List[Dict[str, Any]]:
        """获取某条记忆的所有历史修订版本，按版本号升序排列"""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, memory_id, version, content, updated_by_agent, updated_at, change_reason
                FROM memory_revisions
                WHERE memory_id = ?
                ORDER BY version ASC
                """,
                (memory_id,),
            )
            rows = cursor.fetchall()
            return [dict(r) for r in rows]

    def create_memory_v3(
        self,
        memory_id: str,
        content: str,
        project_id: str = "global",
        type: str = "general",
        status: str = "ACTIVE",
        created_by_agent: str = "system",
        source_refs: Optional[List[str]] = None,
        conflicts_with: Optional[List[str]] = None,
        qdrant_point_id: Optional[str] = None,
        operator: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        v3.0 标准记忆创建：
        - 验证 6 态合法性与非空规则
        - 初始版本固定为 version=1
        - 写入 memories 主表与 revisions 表 (v1 快照)
        - 写入 qdrant_sync_queue (Transactional Outbox)
        - 若声明 conflicts_with，原子双向标记目标记忆为 CONFLICT
        """
        valid_statuses = {"ACTIVE", "PENDING_VERIFY", "CONFLICT", "HISTORICAL", "TEMPORARY", "DELETED"}
        status_norm = status.upper() if status else "ACTIVE"
        if status_norm not in valid_statuses:
            raise ValueError(f"Invalid memory status: '{status}'. Must be one of {valid_statuses}")
        if status_norm == "HISTORICAL":
            raise ValueError("Cannot directly create memory in HISTORICAL state. HISTORICAL is for superseded records.")

        if not content or not content.strip():
            raise ValueError("Memory content cannot be empty")
        if len(content.strip()) < 5:
            raise ValueError("Memory content too short (min 5 chars)")
        if len(content) > 16000:
            raise ValueError("Memory content too long (max 16000 chars)")

        now = int(time.time())
        point_id = qdrant_point_id or str(uuid.uuid5(uuid.NAMESPACE_URL, memory_id))
        c_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
        refs_json = json.dumps(source_refs or [], ensure_ascii=False)
        conflicts_list = conflicts_with or []
        conflicts_json = json.dumps(conflicts_list, ensure_ascii=False)
        agent = created_by_agent or "system"
        op = operator or agent

        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            try:
                # 1. 插入 memories 主表
                cursor.execute(
                    """
                    INSERT INTO memories (
                        memory_id, qdrant_point_id, type, conflict_policy,
                        subject, predicate, object, content,
                        valid_from, valid_to, validity_type,
                        confidence, importance, mention_count,
                        status, superseded_by, superseded_at,
                        previous_version_id, root_memory_id, protection_level,
                        project_id, scope, source_agent, created_by_agent, updated_by_agent, version,
                        content_hash, curation_batch_id, last_reconciled_at,
                        deleted_at, deleted_by, deletion_reason,
                        source_refs, conflicts_with,
                        created_at, updated_at
                    )
                    VALUES (?, ?, ?, 'coexist', '', '', NULL, ?, 0, NULL, 'open_ended', 1.0, 0.5, 1,
                            ?, NULL, NULL, NULL, ?, 'NONE', ?, 'global', ?, ?, ?, 1,
                            ?, NULL, NULL, NULL, NULL, NULL, ?, ?, ?, ?)
                    """,
                    (
                        memory_id, point_id, type, content,
                        status_norm, memory_id, project_id, agent, agent, agent,
                        c_hash, refs_json, conflicts_json, now, now
                    ),
                )

                # 2. 写入 memory_revisions (v1 初始快照)
                cursor.execute(
                    """
                    INSERT INTO memory_revisions (
                        memory_id, version, content, updated_by_agent, updated_at, change_reason
                    )
                    VALUES (?, 1, ?, ?, ?, 'Initial creation')
                    """,
                    (memory_id, content, agent, now),
                )

                # 3. 若有冲突目标，进行双向原子打标
                if conflicts_list:
                    for target_id in conflicts_list:
                        cursor.execute("SELECT conflicts_with, status FROM memories WHERE memory_id = ?", (target_id,))
                        t_row = cursor.fetchone()
                        if t_row:
                            try:
                                t_conflicts = json.loads(t_row["conflicts_with"] or "[]")
                            except Exception:
                                t_conflicts = []
                            if memory_id not in t_conflicts:
                                t_conflicts.append(memory_id)
                            cursor.execute(
                                """
                                UPDATE memories
                                SET status = 'CONFLICT',
                                    conflicts_with = ?,
                                    updated_at = ?
                                WHERE memory_id = ?
                                """,
                                (json.dumps(t_conflicts, ensure_ascii=False), now, target_id),
                            )
                            # 同步入发件箱更新 target 状态
                            target_payload = json.dumps({
                                "memory_id": target_id,
                                "status": "CONFLICT",
                                "updated_at": now,
                            }, ensure_ascii=False)
                            cursor.execute(
                                """
                                INSERT INTO qdrant_sync_queue (
                                    memory_id, qdrant_point_id, op_type, payload_snapshot,
                                    status, retry_count, last_error, created_at, updated_at
                                )
                                VALUES (?, ?, 'update_payload', ?, 'pending', 0, NULL, ?, ?)
                                """,
                                (target_id, target_id, target_payload, now, now),
                            )

                # 4. 构造 Qdrant 9 字段 Payload 并入发件箱
                payload_snapshot = json.dumps({
                    "memory_id": memory_id,
                    "version": 1,
                    "content": content,
                    "project_id": project_id,
                    "type": type,
                    "status": status_norm,
                    "created_by_agent": agent,
                    "updated_by_agent": agent,
                    "updated_at": now,
                }, ensure_ascii=False)

                cursor.execute(
                    """
                    INSERT INTO qdrant_sync_queue (
                        memory_id, qdrant_point_id, op_type, payload_snapshot,
                        status, retry_count, last_error, created_at, updated_at
                    )
                    VALUES (?, ?, 'upsert', ?, 'pending', 0, NULL, ?, ?)
                    """,
                    (memory_id, point_id, payload_snapshot, now, now),
                )

                # 5. 记录审计日志
                audit_dict = {
                    "action": "create_memory_v3",
                    "status": status_norm,
                    "type": type,
                    "project_id": project_id,
                    "conflicts_with": conflicts_list,
                }
                cursor.execute(
                    """
                    INSERT INTO memory_audit_log (
                        memory_id, action, operator, detail, timestamp
                    )
                    VALUES (?, 'create', ?, ?, ?)
                    """,
                    (memory_id, op, json.dumps(audit_dict, ensure_ascii=False), now),
                )

                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e

        return self.get_memory_v3(memory_id)

    def get_memory_v3(self, memory_id: str) -> Optional[Dict[str, Any]]:
        """获取 v3.0 记忆实体详情及历史修订版本列表"""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM memories WHERE memory_id = ?", (memory_id,))
            row = cursor.fetchone()
            if row is None:
                return None
            res = dict(row)
            try:
                res["source_refs"] = json.loads(res.get("source_refs") or "[]")
            except Exception:
                res["source_refs"] = []
            try:
                res["conflicts_with"] = json.loads(res.get("conflicts_with") or "[]")
            except Exception:
                res["conflicts_with"] = []

            cursor.execute(
                """
                SELECT version, content, updated_by_agent, updated_at, change_reason
                FROM memory_revisions
                WHERE memory_id = ?
                ORDER BY version ASC
                """,
                (memory_id,),
            )
            res["revisions"] = [dict(r) for r in cursor.fetchall()]
            return res

    def update_memory_v3(
        self,
        memory_id: str,
        content: Optional[str] = None,
        type: Optional[str] = None,
        status: Optional[str] = None,
        expected_version: Optional[int] = None,
        change_reason: Optional[str] = None,
        agent_id: str = "system",
        conflicts_with: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        v3.0 记忆修改：
        - 仅修改 type / status：元数据就地更新，不递增版本，不写 revisions 表，不重算向量。
        - 修改 content：
            - 必须校验 expected_version（乐观锁防止并发踩踏），不匹配抛 ValueError("VERSION_CONFLICT")
            - 必须提供 change_reason 说明
            - version ++
            - 写入 memory_revisions 快照
            - 入发件箱触发向量重算 (op_type='upsert')
        - 状态流转合法性校验：严禁从 DELETED 逆向流转为其他状态
        """
        valid_statuses = {"ACTIVE", "PENDING_VERIFY", "CONFLICT", "HISTORICAL", "TEMPORARY", "DELETED"}
        now = int(time.time())

        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM memories WHERE memory_id = ?", (memory_id,))
            row = cursor.fetchone()
            if row is None:
                raise KeyError(f"Memory '{memory_id}' not found")

            curr_status = (row["status"] or "ACTIVE").upper()
            curr_version = row["version"] or 1
            point_id = row["qdrant_point_id"]

            # 状态流转守卫
            new_status = status.upper() if status else curr_status
            if new_status not in valid_statuses:
                raise ValueError(f"Invalid status: '{status}'. Must be one of {valid_statuses}")
            if curr_status == "DELETED" and new_status != "DELETED":
                raise ValueError("Illegal status transition: DELETED state is terminal and cannot be reverted.")

            new_type = type if type is not None else (row["type"] or "general")
            content_changed = content is not None and content.strip() != row["content"].strip()

            new_version = curr_version
            op_type = "update_payload"

            try:
                if content_changed:
                    # 乐观锁验证
                    if expected_version is None:
                        raise ValueError("Content modification requires 'expected_version' for optimistic concurrency control.")
                    if expected_version != curr_version:
                        raise ValueError(f"VERSION_CONFLICT: Expected version {expected_version}, but current version is {curr_version}.")
                    if not change_reason or not change_reason.strip():
                        raise ValueError("Content modification requires a non-empty 'change_reason'.")

                    clean_content = content.strip()
                    if len(clean_content) < 5:
                        raise ValueError("Memory content too short (min 5 chars)")
                    if len(clean_content) > 16000:
                        raise ValueError("Memory content too long (max 16000 chars)")

                    new_version = curr_version + 1
                    c_hash = hashlib.sha256(clean_content.encode("utf-8")).hexdigest()

                    # 1. 插入新 revision
                    cursor.execute(
                        """
                        INSERT INTO memory_revisions (
                            memory_id, version, content, updated_by_agent, updated_at, change_reason
                        )
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (memory_id, new_version, clean_content, agent_id, now, change_reason),
                    )

                    # 2. 更新 memories 主表
                    cursor.execute(
                        """
                        UPDATE memories
                        SET content = ?,
                            content_hash = ?,
                            version = ?,
                            type = ?,
                            status = ?,
                            updated_by_agent = ?,
                            updated_at = ?
                        WHERE memory_id = ?
                        """,
                        (clean_content, c_hash, new_version, new_type, new_status, agent_id, now, memory_id),
                    )
                    op_type = "upsert"
                    final_content = clean_content
                else:
                    # 仅元数据更新
                    final_content = row["content"]
                    deleted_at = now if new_status == "DELETED" else row["deleted_at"]
                    deleted_by = agent_id if new_status == "DELETED" else row["deleted_by"]
                    cursor.execute(
                        """
                        UPDATE memories
                        SET type = ?,
                            status = ?,
                            updated_by_agent = ?,
                            updated_at = ?,
                            deleted_at = ?,
                            deleted_by = ?
                        WHERE memory_id = ?
                        """,
                        (new_type, new_status, agent_id, now, deleted_at, deleted_by, memory_id),
                    )
                    op_type = "delete" if new_status == "DELETED" else "update_payload"

                # 3. 处理冲突
                if conflicts_with is not None:
                    conflicts_json = json.dumps(conflicts_with, ensure_ascii=False)
                    cursor.execute(
                        "UPDATE memories SET conflicts_with = ? WHERE memory_id = ?",
                        (conflicts_json, memory_id),
                    )

                # 4. Outbox 同步
                payload_snapshot = json.dumps({
                    "memory_id": memory_id,
                    "version": new_version,
                    "content": final_content,
                    "project_id": row["project_id"] or "global",
                    "type": new_type,
                    "status": new_status,
                    "created_by_agent": row["created_by_agent"] or row["source_agent"] or "system",
                    "updated_by_agent": agent_id,
                    "updated_at": now,
                }, ensure_ascii=False)

                cursor.execute(
                    """
                    INSERT INTO qdrant_sync_queue (
                        memory_id, qdrant_point_id, op_type, payload_snapshot,
                        status, retry_count, last_error, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, 'pending', 0, NULL, ?, ?)
                    """,
                    (memory_id, point_id, op_type, payload_snapshot, now, now),
                )

                # 5. 审计记录
                audit_dict = {
                    "action": "update_memory_v3",
                    "content_changed": content_changed,
                    "old_version": curr_version,
                    "new_version": new_version,
                    "status": new_status,
                    "type": new_type,
                    "change_reason": change_reason,
                }
                cursor.execute(
                    """
                    INSERT INTO memory_audit_log (
                        memory_id, action, operator, detail, timestamp
                    )
                    VALUES (?, 'update', ?, ?, ?)
                    """,
                    (memory_id, agent_id, json.dumps(audit_dict, ensure_ascii=False), now),
                )

                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e

        return self.get_memory_v3(memory_id)

    def close(self) -> None:
        """关闭数据库长连接"""
        with self._lock:
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:
                    pass
                self._conn = None
