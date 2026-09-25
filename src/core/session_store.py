"""
AI Memory Runtime - Session Store & Idempotent Ingest Engine
实现 SQLite 会话管理、消息明细存储、幂等判定与审计日志
遵循 DOC-AMR-03-DDD 规范
"""

import hashlib
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from config.settings import AppConfig, StorageConfig, DatabaseConfig, load_config


CREATE_TABLES_SQL = """
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
        """初始化 SQLite 数据库及 Schema"""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            # PRAGMA 设置
            if self.wal_mode:
                cursor.execute("PRAGMA journal_mode = WAL;")
            cursor.execute(f"PRAGMA busy_timeout = {self.busy_timeout};")
            cursor.execute("PRAGMA foreign_keys = ON;")
            cursor.executescript(CREATE_TABLES_SQL)
            conn.commit()

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
                for msg in messages:
                    msg_id = msg["message_id"]
                    role = msg["role"]
                    content = msg["content"]
                    seq = msg.get("sequence", 0)
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
