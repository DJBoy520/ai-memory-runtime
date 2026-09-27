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

-- 3. 记忆主表 (知识层 Memory SSOT)
CREATE TABLE IF NOT EXISTS memories (
    memory_id TEXT PRIMARY KEY,
    qdrant_point_id TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL CHECK(type IN ('fact', 'preference', 'decision', 'task', 'episode', 'relation')),
    conflict_policy TEXT NOT NULL DEFAULT 'coexist' 
        CHECK(conflict_policy IN ('overwrite', 'coexist', 'state_machine', 'immutable')),
    subject TEXT NOT NULL,
    predicate TEXT NOT NULL,
    object TEXT,
    content TEXT NOT NULL,
    valid_from INTEGER NOT NULL,
    valid_to INTEGER,
    validity_type TEXT NOT NULL DEFAULT 'open_ended' 
        CHECK(validity_type IN ('open_ended', 'bounded', 'unknown')),
    confidence REAL NOT NULL DEFAULT 0.8,
    importance REAL NOT NULL DEFAULT 0.5,
    mention_count INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'candidate' 
        CHECK(status IN ('candidate', 'active', 'superseded', 'archived', 'deleted')),
    superseded_by TEXT REFERENCES memories(memory_id),
    project_id TEXT NOT NULL DEFAULT 'general',
    scope TEXT NOT NULL DEFAULT 'global' CHECK(scope IN ('global', 'project', 'agent', 'session')),
    source_agent TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    deleted_at INTEGER,
    deleted_by TEXT,
    deletion_reason TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_mem_lookup ON memories(project_id, type, status);
CREATE INDEX IF NOT EXISTS idx_mem_subject ON memories(subject, predicate);
CREATE INDEX IF NOT EXISTS idx_mem_point ON memories(qdrant_point_id);

-- 4. 记忆与证据溯源多对多关联表
CREATE TABLE IF NOT EXISTS memory_evidence (
    memory_id TEXT NOT NULL REFERENCES memories(memory_id),
    message_id TEXT NOT NULL REFERENCES raw_messages(message_id),
    session_id TEXT NOT NULL,
    evidence_strength REAL NOT NULL DEFAULT 0.5,
    linked_at INTEGER NOT NULL,
    PRIMARY KEY (memory_id, message_id)
);

-- 5. Transactional Outbox 异步同步发件箱队列
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

-- 6. 治理与生命周期审计日志表
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

        # 2. 检查 memories 列 (确保所有 v2.2 字段存在)
        cursor.execute("PRAGMA table_info(memories);")
        mem_cols = {row["name"] for row in cursor.fetchall()}
        if mem_cols:
            v2_mem_columns = {
                "conflict_policy": "TEXT NOT NULL DEFAULT 'coexist'",
                "validity_type": "TEXT NOT NULL DEFAULT 'open_ended'",
                "version": "INTEGER NOT NULL DEFAULT 1",
                "deleted_at": "INTEGER",
                "deleted_by": "TEXT",
                "deletion_reason": "TEXT",
            }
            for col_name, col_def in v2_mem_columns.items():
                if col_name not in mem_cols:
                    cursor.execute(f"ALTER TABLE memories ADD COLUMN {col_name} {col_def};")

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
        project_id: str = "general",
        scope: str = "global",
        source_agent: str = "default_agent",
        version: int = 1,
        qdrant_point_id: Optional[str] = None,
        evidence: Optional[List[Dict[str, Any]]] = None,
        operator: str = "engine",
        audit_detail: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        原子创建记忆实体：
        1. 写入 memories (知识主表 SSOT)
        2. 写入 memory_evidence (多对多关联)
        3. 同一事务写入 qdrant_sync_queue (Transactional Outbox)
        4. 同一事务写入 memory_audit_log (审计日志)
        保证数据一致性与发件箱原子性。
        """
        now = int(time.time())
        v_from = valid_from if valid_from is not None else now
        point_id = qdrant_point_id or str(uuid.uuid5(uuid.NAMESPACE_URL, memory_id))

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
                        status, superseded_by,
                        project_id, scope, source_agent, version,
                        deleted_at, deleted_by, deletion_reason,
                        created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?)
                    """,
                    (
                        memory_id, point_id, type, conflict_policy,
                        subject, predicate, object, content,
                        v_from, valid_to, validity_type,
                        confidence, importance, mention_count,
                        status, superseded_by,
                        project_id, scope, source_agent, version,
                        now, now
                    ),
                )

                # 2. 插入 memory_evidence (多对多关联表)
                if evidence:
                    for ev in evidence:
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
                payload_snapshot = json.dumps({
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

            try:
                # 1. 更新 memories 主表
                cursor.execute(
                    """
                    UPDATE memories
                    SET status = ?,
                        superseded_by = COALESCE(?, superseded_by),
                        deleted_at = ?,
                        deleted_by = ?,
                        deletion_reason = ?,
                        updated_at = ?
                    WHERE memory_id = ?
                    """,
                    (status, superseded_by, d_at, d_by, d_reason, now, memory_id),
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

    def close(self) -> None:
        """关闭数据库长连接"""
        with self._lock:
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:
                    pass
                self._conn = None
