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
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from config.settings import AppConfig, StorageConfig, DatabaseConfig, load_config
from src.core.errors import (
    IdempotencyConflict,
    InvalidStatusError,
    LegacyPathForbidden,
    MigrationBlocked,
    VersionConflictError,
)
from src.core.idempotency import canonical_json, compute_request_digest
from src.service.memory_status import CANONICAL_STATUSES, is_status, normalize_status


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
    projection_revision INTEGER NOT NULL DEFAULT 0,
    collection_name TEXT NOT NULL DEFAULT 'ai_memory',
    projection_class TEXT NOT NULL DEFAULT 'legacy' CHECK (projection_class IN ('v3', 'legacy')),
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

-- 6. 统一投影 Outbox 队列 (RFC v5: memory_projection_outbox)
CREATE TABLE IF NOT EXISTS memory_projection_outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_collection TEXT NOT NULL DEFAULT 'ai_memory',
    memory_id TEXT NOT NULL,
    projection_revision INTEGER NOT NULL DEFAULT 0,
    request_id TEXT,
    projected_revision INTEGER,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'done', 'dead_letter')),
    retry_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at INTEGER,
    last_error TEXT,
    dead_letter_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

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

-- 10. 幂等控制表 (RFC v5.2 §1: idempotency_keys)
CREATE TABLE IF NOT EXISTS idempotency_keys (
    agent_id_raw TEXT NOT NULL,
    op TEXT NOT NULL,
    idem_key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    memory_id TEXT,              -- 占位阶段 NULL；最终 UPDATE 写入（create 由业务生成）
    response_json TEXT,          -- 占位阶段 NULL
    created_at INTEGER NOT NULL,
    PRIMARY KEY (agent_id_raw, op, idem_key)
);

-- 11. 模式迁移幂等标记表 (RFC v5.3 §4: _amr_migrations)
CREATE TABLE IF NOT EXISTS _amr_migrations (
    migration_id TEXT PRIMARY KEY,
    applied_at INTEGER NOT NULL,
    details TEXT
);
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
            # 先跑 v2 增量列迁移 -> executescript(CREATE_TABLES_SQL) -> 跑 v5 迁移（保持兼容旧库）
            self._migrate_schema_v2(cursor)
            conn.commit()
            cursor.executescript(CREATE_TABLES_SQL)
            conn.commit()
            self._migrate_schema_v5(cursor)

    def _migrate_schema(self, cursor: sqlite3.Cursor) -> None:
        """平滑表迁移：兼容旧调用，执行完整迁移流程。"""
        self._migrate_schema_v2(cursor)
        cursor.connection.commit()
        cursor.executescript(CREATE_TABLES_SQL)
        cursor.connection.commit()
        self._migrate_schema_v5(cursor)

    def _migrate_schema_v2(self, cursor: sqlite3.Cursor) -> None:
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
                "type": "TEXT NOT NULL DEFAULT 'general'",
                "subject": "TEXT DEFAULT ''",
                "predicate": "TEXT DEFAULT ''",
                "object": "TEXT",
                "valid_from": "INTEGER NOT NULL DEFAULT 0",
                "valid_to": "INTEGER",
                "confidence": "REAL NOT NULL DEFAULT 1.0",
                "importance": "REAL NOT NULL DEFAULT 0.5",
                "mention_count": "INTEGER NOT NULL DEFAULT 1",
                "project_id": "TEXT NOT NULL DEFAULT 'global'",
                "scope": "TEXT NOT NULL DEFAULT 'global'",
                "source_agent": "TEXT NOT NULL DEFAULT 'system'",
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
                "projection_revision": "INTEGER NOT NULL DEFAULT 0",
                "collection_name": "TEXT NOT NULL DEFAULT 'ai_memory'",
                "projection_class": "TEXT NOT NULL DEFAULT 'legacy'",
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

    def _migrate_schema_v5(self, cursor: sqlite3.Cursor) -> None:
        """
        v5 增量迁移：
        - 显式事务 BEGIN IMMEDIATE ... COMMIT
        - Step 0: PRAGMA 逐表逐列探测 memories 新列补齐
        - _amr_migrations 幂等门
        - v5_projection_class_backfill: 存量分类回填
        - v5_outbox_rebuild: 旧 outbox 重建或隔离
        """
        conn = cursor.connection
        old_isolation = conn.isolation_level
        try:
            conn.isolation_level = None
            cursor.execute("BEGIN IMMEDIATE;")

            # 确保 _amr_migrations 表存在
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS _amr_migrations (
                    migration_id TEXT PRIMARY KEY,
                    applied_at INTEGER NOT NULL,
                    details TEXT
                );
            """)
            cursor.execute("SELECT migration_id FROM _amr_migrations;")
            applied_migrations = {row[0] for row in cursor.fetchall()}
            now = int(time.time())
            report_path = self.db_path.parent / "migration_report.json"

            # Step 0: 逐表逐列 PRAGMA 探测 memories
            cursor.execute("PRAGMA table_info(memories);")
            mem_cols = {row["name"] for row in cursor.fetchall()}
            if mem_cols:
                if "projection_revision" not in mem_cols:
                    cursor.execute("ALTER TABLE memories ADD COLUMN projection_revision INTEGER NOT NULL DEFAULT 0;")
                if "collection_name" not in mem_cols:
                    cursor.execute("ALTER TABLE memories ADD COLUMN collection_name TEXT NOT NULL DEFAULT 'ai_memory';")
                if "projection_class" not in mem_cols:
                    cursor.execute("ALTER TABLE memories ADD COLUMN projection_class TEXT NOT NULL DEFAULT 'legacy';")

            v3_count = 0
            legacy_count = 0
            if mem_cols:
                cursor.execute("SELECT COUNT(*) FROM memories WHERE projection_class = 'v3';")
                v3_count = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM memories WHERE projection_class = 'legacy';")
                legacy_count = cursor.fetchone()[0]

            # 1. 存量分类回填 (一次性门控)
            if "v5_projection_class_backfill" not in applied_migrations:
                if mem_cols:
                    cursor.execute("""
                        UPDATE memories SET projection_class = 'v3'
                        WHERE (SELECT COUNT(*) FROM memory_revisions r WHERE r.memory_id = memories.memory_id) > 0
                           OR status IN ('ACTIVE', 'HISTORICAL', 'PENDING_VERIFY', 'CONFLICT', 'TEMPORARY')
                           OR (source_refs IS NOT NULL AND source_refs NOT IN ('', '[]'));
                    """)
                    cursor.execute("SELECT COUNT(*) FROM memories WHERE projection_class = 'v3';")
                    v3_count = cursor.fetchone()[0]
                    cursor.execute("SELECT COUNT(*) FROM memories WHERE projection_class = 'legacy';")
                    legacy_count = cursor.fetchone()[0]

                cursor.execute(
                    "INSERT INTO _amr_migrations (migration_id, applied_at, details) VALUES (?, ?, ?);",
                    ("v5_projection_class_backfill", now, json.dumps({"v3_count": v3_count, "legacy_count": legacy_count})),
                )
                report_data = {
                    "v3_count": v3_count,
                    "legacy_count": legacy_count,
                    "ambiguous_count": 0,
                    "quarantine": [],
                    "status": "in_progress",
                    "updated_at": now,
                }
                with open(report_path, "w", encoding="utf-8") as f:
                    json.dump(report_data, f, ensure_ascii=False, indent=2)

            # 2. 旧 outbox 重建 (一次性门控)
            if "v5_outbox_rebuild" not in applied_migrations:
                cursor.execute("PRAGMA table_info(memory_projection_outbox);")
                outbox_cols = {row["name"] for row in cursor.fetchall()}
                if not outbox_cols:
                    # fresh DB / 表尚未创建
                    cursor.execute(
                        "INSERT INTO _amr_migrations (migration_id, applied_at, details) VALUES (?, ?, ?);",
                        ("v5_outbox_rebuild", now, "fresh_db_noop"),
                    )
                elif "target_collection" in outbox_cols and "version" not in outbox_cols:
                    # 已经是新 schema
                    cursor.execute(
                        "INSERT INTO _amr_migrations (migration_id, applied_at, details) VALUES (?, ?, ?);",
                        ("v5_outbox_rebuild", now, "already_new_schema"),
                    )
                else:
                    # 旧 schema：逐行判定 convertible vs quarantine
                    cursor.execute("SELECT * FROM memory_projection_outbox;")
                    old_rows = cursor.fetchall()
                    convertible = []
                    quarantine = []
                    for r in old_rows:
                        r_dict = dict(r)
                        mid = r_dict["memory_id"]
                        cursor.execute(
                            "SELECT memory_id, qdrant_point_id, projection_class, collection_name, projection_revision FROM memories WHERE memory_id = ?;",
                            (mid,),
                        )
                        m_row = cursor.fetchone()
                        if m_row is not None and m_row["qdrant_point_id"] and m_row["projection_class"] == "v3":
                            convertible.append((r_dict, dict(m_row)))
                        else:
                            reasons = []
                            if m_row is None:
                                reasons.append("memory row missing (orphan)")
                            else:
                                if not m_row["qdrant_point_id"]:
                                    reasons.append("qdrant_point_id missing")
                                if m_row["projection_class"] != "v3":
                                    reasons.append(f"projection_class is '{m_row['projection_class']}', expected 'v3'")
                            quarantine.append((r_dict, "; ".join(reasons)))

                    if quarantine:
                        cursor.execute("""
                            CREATE TABLE IF NOT EXISTS memory_projection_outbox_quarantine (
                                id INTEGER PRIMARY KEY,
                                memory_id TEXT,
                                version INTEGER,
                                projection_type TEXT,
                                op_type TEXT,
                                payload_snapshot TEXT,
                                status TEXT,
                                retry_count INTEGER,
                                next_retry_at INTEGER,
                                last_error TEXT,
                                dead_letter_at INTEGER,
                                created_at INTEGER,
                                updated_at INTEGER,
                                reason TEXT,
                                observed_at INTEGER
                            );
                        """)
                        for r_dict, q_reason in quarantine:
                            cursor.execute(
                                """
                                INSERT OR IGNORE INTO memory_projection_outbox_quarantine (
                                    id, memory_id, version, projection_type, op_type,
                                    payload_snapshot, status, retry_count, next_retry_at,
                                    last_error, dead_letter_at, created_at, updated_at,
                                    reason, observed_at
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                                """,
                                (
                                    r_dict.get("id"),
                                    r_dict.get("memory_id"),
                                    r_dict.get("version"),
                                    r_dict.get("projection_type"),
                                    r_dict.get("op_type"),
                                    r_dict.get("payload_snapshot"),
                                    r_dict.get("status"),
                                    r_dict.get("retry_count"),
                                    r_dict.get("next_retry_at"),
                                    r_dict.get("last_error"),
                                    r_dict.get("dead_letter_at"),
                                    r_dict.get("created_at"),
                                    r_dict.get("updated_at"),
                                    q_reason,
                                    now,
                                ),
                            )
                        report_data = {
                            "v3_count": v3_count,
                            "legacy_count": legacy_count,
                            "ambiguous_count": 0,
                            "quarantine": [
                                {"id": r["id"], "memory_id": r["memory_id"], "reason": reason}
                                for r, reason in quarantine
                            ],
                            "status": "blocked",
                            "updated_at": now,
                        }
                        with open(report_path, "w", encoding="utf-8") as f:
                            json.dump(report_data, f, ensure_ascii=False, indent=2)

                        # 先独立 COMMIT 再 raise MigrationBlocked（旧表完好）
                        cursor.execute("COMMIT;")
                        raise MigrationBlocked(
                            quarantine_count=len(quarantine),
                            report_path=str(report_path),
                        )

                    # 全部可转换或空表：同事务建 _new、全列 INSERT、换表
                    cursor.execute("""
                        CREATE TABLE memory_projection_outbox_new (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            target_collection TEXT NOT NULL DEFAULT 'ai_memory',
                            memory_id TEXT NOT NULL,
                            projection_revision INTEGER NOT NULL DEFAULT 0,
                            request_id TEXT,
                            projected_revision INTEGER,
                            status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','done','dead_letter')),
                            retry_count INTEGER NOT NULL DEFAULT 0,
                            next_retry_at INTEGER,
                            last_error TEXT,
                            dead_letter_at INTEGER,
                            created_at INTEGER NOT NULL,
                            updated_at INTEGER NOT NULL
                        );
                    """)
                    for r_dict, m_dict in convertible:
                        cursor.execute(
                            """
                            INSERT INTO memory_projection_outbox_new (
                                target_collection, memory_id, projection_revision, request_id,
                                projected_revision, status, retry_count, next_retry_at,
                                last_error, dead_letter_at, created_at, updated_at
                            ) VALUES (?, ?, ?, ?, NULL, 'pending', 0, NULL, NULL, NULL, ?, ?);
                            """,
                            (
                                m_dict.get("collection_name") or "ai_memory",
                                r_dict["memory_id"],
                                m_dict.get("projection_revision") or 0,
                                r_dict.get("request_id"),
                                r_dict.get("created_at") or now,
                                now,
                            ),
                        )
                    cursor.execute("DROP TABLE memory_projection_outbox;")
                    cursor.execute("ALTER TABLE memory_projection_outbox_new RENAME TO memory_projection_outbox;")

                    cursor.execute(
                        "INSERT INTO _amr_migrations (migration_id, applied_at, details) VALUES (?, ?, ?);",
                        ("v5_outbox_rebuild", now, f"converted_{len(convertible)}_tasks"),
                    )
                    report_data = {
                        "v3_count": v3_count,
                        "legacy_count": legacy_count,
                        "ambiguous_count": 0,
                        "quarantine": [],
                        "status": "completed",
                        "converted_count": len(convertible),
                        "updated_at": now,
                    }
                    with open(report_path, "w", encoding="utf-8") as f:
                        json.dump(report_data, f, ensure_ascii=False, indent=2)

            # 确认 outbox 表已经是新 schema（非 quarantine 阻断）之后统一建立索引
            cursor.execute("PRAGMA table_info(memory_projection_outbox);")
            cur_outbox_cols = {row["name"] for row in cursor.fetchall()}
            if "target_collection" in cur_outbox_cols:
                cursor.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS ux_outbox_pending_key ON memory_projection_outbox(target_collection, memory_id) WHERE status='pending';"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS ix_outbox_status ON memory_projection_outbox(status, next_retry_at);"
                )

            cursor.execute("COMMIT;")
        except MigrationBlocked:
            raise
        except Exception:
            try:
                cursor.execute("ROLLBACK;")
            except Exception:
                pass
            raise
        finally:
            conn.isolation_level = old_isolation

        # 确保 reconciliation_checkpoints 表存在（保持原 v2 行为）
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
        collection_name: str = "ai_memory",
        cursor: Optional[sqlite3.Cursor] = None,
    ) -> Dict[str, Any]:
        """
        原子创建记忆实体 (Legacy 路径)：
        1. 写入 memories (显式 projection_class='legacy', collection_name, projection_revision=0)
        2. 写入 memory_evidence (多对多关联)
        3. 同一事务写入 qdrant_sync_queue (Transactional Outbox)
        4. 同一事务写入 memory_audit_log (审计日志)
        """
        if cursor is None:
            with self.locked_write_transaction() as conn:
                return self.create_memory(
                    memory_id=memory_id,
                    subject=subject,
                    predicate=predicate,
                    content=content,
                    type=type,
                    conflict_policy=conflict_policy,
                    object=object,
                    valid_from=valid_from,
                    valid_to=valid_to,
                    validity_type=validity_type,
                    confidence=confidence,
                    importance=importance,
                    mention_count=mention_count,
                    status=status,
                    superseded_by=superseded_by,
                    superseded_at=superseded_at,
                    previous_version_id=previous_version_id,
                    root_memory_id=root_memory_id,
                    protection_level=protection_level,
                    project_id=project_id,
                    scope=scope,
                    source_agent=source_agent,
                    version=version,
                    qdrant_point_id=qdrant_point_id,
                    evidence=evidence,
                    operator=operator,
                    audit_detail=audit_detail,
                    qdrant_payload=qdrant_payload,
                    collection_name=collection_name,
                    cursor=conn.cursor(),
                )

        now = int(time.time())
        v_from = valid_from if valid_from is not None else now
        point_id = qdrant_point_id or str(uuid.uuid5(uuid.NAMESPACE_URL, memory_id))
        c_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()

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
                projection_revision, collection_name, projection_class,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, 0, ?, 'legacy', ?, ?)
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
                collection_name,
                now, now
            ),
        )

        # 2. 插入 memory_evidence (多对多关联表)
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

        return self.get_memory(memory_id, cursor=cursor)  # type: ignore[return-value]

    def get_memory(self, memory_id: str, cursor: Optional[sqlite3.Cursor] = None) -> Optional[Dict[str, Any]]:
        """读取完整的记忆实体，包含关联的 evidence 列表"""
        if cursor is not None:
            cursor.execute("SELECT * FROM memories WHERE memory_id = ?", (memory_id,))
            row = cursor.fetchone()
            if row is None:
                return None
            mem_dict = dict(row)
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

        with self._lock:
            conn = self._get_connection()
            return self.get_memory(memory_id, cursor=conn.cursor())

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
        cursor: Optional[sqlite3.Cursor] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        更新记忆状态、替换指向，并原子追加 Outbox 任务与 audit log。
        委托给 change_memory_status 实现，统一状态大写规范化。
        """
        norm_status = normalize_status(status)
        try:
            self.change_memory_status(
                memory_id,
                norm_status,
                actor=deleted_by or operator,
                superseded_by=superseded_by,
                reason=deletion_reason,
                cursor=cursor,
            )
        except KeyError:
            return None
        return self.get_memory(memory_id, cursor=cursor)

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

    def mark_sync_task_done(self, task_id: int, note: Optional[str] = None) -> bool:
        """标记同步任务已完成 (synced)。支持传入 note 注记 last_error。"""
        now = int(time.time())
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            if note is not None:
                cursor.execute(
                    """
                    UPDATE qdrant_sync_queue
                    SET status = 'synced', last_error = ?, updated_at = ?
                    WHERE id = ? AND status IN ('pending', 'failed')
                    """,
                    (note, now, task_id),
                )
            else:
                cursor.execute(
                    """
                    UPDATE qdrant_sync_queue
                    SET status = 'synced', updated_at = ?
                    WHERE id = ? AND status IN ('pending', 'failed')
                    """,
                    (now, task_id),
                )
            conn.commit()
            return cursor.rowcount > 0

    def mark_sync_task_failed(self, task_id: int, error: str) -> bool:
        """标记同步任务失败并增加重试计数。仅更新 pending/failed 状态的任务。"""
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
                WHERE id = ? AND status IN ('pending', 'failed')
                """,
                (error, now, task_id),
            )
            conn.commit()
            return cursor.rowcount > 0
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

    @contextmanager
    def locked_write_transaction(self):
        """
        排他写事务上下文管理器：
        持 self._lock（RLock）；conn.isolation_level=None；显式 BEGIN IMMEDIATE；
        正常退出 COMMIT；异常 ROLLBACK 后原样抛出；finally 恢复原 isolation_level。
        yield sqlite3.Connection（调用方用 .cursor()）
        """
        with self._lock:
            conn = self._get_connection()
            old_isolation = conn.isolation_level
            try:
                conn.isolation_level = None
                conn.execute("BEGIN IMMEDIATE")
                yield conn
                conn.execute("COMMIT")
            except Exception:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                raise
            finally:
                conn.isolation_level = old_isolation

    def execute_idempotent(
        self,
        op: str,
        idem_key: Optional[str],
        digest: str,
        business_fn,
        retention_days: int = 7,
        agent_id_raw: str = "default",
    ) -> Dict[str, Any]:
        """
        执行带幂等仲裁与事务隔离的写操作（RFC v5.2 §1/§2）。
        - idem_key 为空：单事务执行 business_fn(cursor) 并返回结果。
        - idem_key 非空：在 locked_write_transaction 内先执行仲裁 UPSERT：
            INSERT ... ON CONFLICT(agent_id_raw, op, idem_key) DO UPDATE SET ... WHERE created_at < cutoff
            rowcount==0 -> 回读；digest 相同 -> 返回 {**json.loads(response_json), "idempotent_replay": True}
                          digest 不同 -> raise IdempotencyConflict(op=..., idem_key=..., existing_memory_id=...)
            rowcount==1 -> business_fn(cursor)；再 UPDATE 写入 memory_id/response_json；返回 result
            business_fn 抛异常 -> 事务自动回滚（key 一并回滚，不烧 key）
        """
        if not idem_key:
            with self.locked_write_transaction() as conn:
                res = business_fn(conn.cursor())
                if isinstance(res, tuple) and len(res) == 2:
                    return res[1]
                return res

        now = int(time.time())
        cutoff = now - (retention_days * 86400)

        with self.locked_write_transaction() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO idempotency_keys (
                    agent_id_raw, op, idem_key, request_digest, memory_id, response_json, created_at
                ) VALUES (?, ?, ?, ?, NULL, NULL, ?)
                ON CONFLICT(agent_id_raw, op, idem_key) DO UPDATE
                  SET request_digest = excluded.request_digest,
                      memory_id = NULL,
                      response_json = NULL,
                      created_at = excluded.created_at
                  WHERE idempotency_keys.created_at < ?;
                """,
                (agent_id_raw, op, idem_key, digest, now, cutoff),
            )
            if cur.rowcount == 0:
                cur.execute(
                    "SELECT request_digest, response_json, memory_id FROM idempotency_keys WHERE agent_id_raw = ? AND op = ? AND idem_key = ?;",
                    (agent_id_raw, op, idem_key),
                )
                row = cur.fetchone()
                if row is None or row["request_digest"] != digest:
                    existing_mid = row["memory_id"] if row else None
                    raise IdempotencyConflict(
                        op=op,
                        idem_key=idem_key,
                        existing_memory_id=existing_mid,
                    )
                resp_data = json.loads(row["response_json"]) if row["response_json"] else {}
                return {**resp_data, "idempotent_replay": True}

            business_res = business_fn(cur)
            if isinstance(business_res, tuple) and len(business_res) == 2:
                memory_id, result = business_res
            elif isinstance(business_res, dict) and "memory_id" in business_res:
                memory_id = business_res["memory_id"]
                result = business_res
            else:
                memory_id = None
                result = business_res

            cur.execute(
                """
                UPDATE idempotency_keys
                SET memory_id = ?, response_json = ?
                WHERE agent_id_raw = ? AND op = ? AND idem_key = ?;
                """,
                (memory_id, json.dumps(result, ensure_ascii=False), agent_id_raw, op, idem_key),
            )
            return result

    def cleanup_expired_idempotency(self, retention_days: int = 7) -> int:
        """清理已过期的幂等记录"""
        cutoff = int(time.time()) - (retention_days * 86400)
        with self.locked_write_transaction() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM idempotency_keys WHERE created_at < ?;", (cutoff,))
            return cursor.rowcount

    def get_idempotency_record(
        self, agent_id_raw: str, op: str, idem_key: str
    ) -> Optional[Dict[str, Any]]:
        """获取幂等记录（用于测试与诊断）"""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT agent_id_raw, op, idem_key, request_digest, memory_id, response_json, created_at
                FROM idempotency_keys
                WHERE agent_id_raw = ? AND op = ? AND idem_key = ?;
                """,
                (agent_id_raw, op, idem_key),
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def bump_projection_revision(self, cursor: sqlite3.Cursor, memory_id: str) -> int:
        """递增记忆的 projection_revision 并返回新值"""
        try:
            cursor.execute(
                "UPDATE memories SET projection_revision = projection_revision + 1 WHERE memory_id = ? RETURNING projection_revision;",
                (memory_id,),
            )
            row = cursor.fetchone()
            if row is not None:
                return int(row[0])
        except Exception:
            pass
        cursor.execute(
            "UPDATE memories SET projection_revision = projection_revision + 1 WHERE memory_id = ?;",
            (memory_id,),
        )
        cursor.execute(
            "SELECT projection_revision FROM memories WHERE memory_id = ?;",
            (memory_id,),
        )
        row = cursor.fetchone()
        if row is None:
            raise KeyError(f"Memory '{memory_id}' not found")
        return int(row[0])

    def enqueue_projection_task(
        self,
        cursor: sqlite3.Cursor,
        *,
        memory_id: str,
        projection_revision: int,
        target_collection: str = "ai_memory",
        request_id: Optional[str] = None,
    ) -> int:
        """
        向统一投影队列入队（RFC v5 §A2.2）。
        同事务断言该行 projection_class=='v3'，否则抛出 LegacyPathForbidden。
        UPSERT 合并待处理任务，重置 retry_count/next_retry_at/projected_revision。
        返回 outbox id。
        """
        cursor.execute("SELECT projection_class, collection_name FROM memories WHERE memory_id = ?;", (memory_id,))
        row = cursor.fetchone()
        if row is None:
            raise KeyError(f"Memory '{memory_id}' not found")
        if row["projection_class"] != "v3":
            raise LegacyPathForbidden(
                memory_id=memory_id,
                projection_class=row["projection_class"],
                hint="legacy 记忆不可入统一投影队列",
            )
        col = target_collection or row["collection_name"] or "ai_memory"
        now = int(time.time())
        try:
            cursor.execute(
                """
                INSERT INTO memory_projection_outbox (
                    target_collection, memory_id, projection_revision, request_id,
                    projected_revision, status, retry_count, next_retry_at,
                    last_error, dead_letter_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, NULL, 'pending', 0, NULL, NULL, NULL, ?, ?)
                ON CONFLICT(target_collection, memory_id) WHERE status = 'pending' DO UPDATE
                  SET projection_revision = excluded.projection_revision,
                      request_id = excluded.request_id,
                      retry_count = 0,
                      next_retry_at = NULL,
                      projected_revision = NULL,
                      updated_at = excluded.updated_at
                RETURNING id;
                """,
                (col, memory_id, projection_revision, request_id, now, now),
            )
            ret = cursor.fetchone()
            if ret is not None:
                return int(ret[0])
        except Exception:
            pass

        cursor.execute(
            """
            INSERT INTO memory_projection_outbox (
                target_collection, memory_id, projection_revision, request_id,
                projected_revision, status, retry_count, next_retry_at,
                last_error, dead_letter_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, NULL, 'pending', 0, NULL, NULL, NULL, ?, ?)
            ON CONFLICT(target_collection, memory_id) WHERE status = 'pending' DO UPDATE
              SET projection_revision = excluded.projection_revision,
                  request_id = excluded.request_id,
                  retry_count = 0,
                  next_retry_at = NULL,
                  projected_revision = NULL,
                  updated_at = excluded.updated_at;
            """,
            (col, memory_id, projection_revision, request_id, now, now),
        )
        cursor.execute(
            "SELECT id FROM memory_projection_outbox WHERE target_collection = ? AND memory_id = ? AND status = 'pending';",
            (col, memory_id),
        )
        row_id = cursor.fetchone()
        return int(row_id[0]) if row_id else cursor.lastrowid

    def resolve_memory_family(self, cursor: sqlite3.Cursor, memory_id: str) -> List[sqlite3.Row]:
        """解析记忆族（多分片 legacy 行）与独立实体/版本行。

        root_memory_id 同时被多分片（parent_chunk_N）与版本链（v2.root=v1）复用，
        因此不能无条件按 root 归组，否则状态变更会误伤同一根下的其它版本行：
        - parent-only id（memories 无对应行）-> root 下的全部分片；
        - 分片行（memory_id == f"{root}_chunk_N"）-> root 下的全部分片；
        - 其它（含版本行）-> 仅该行。
        """
        cursor.execute("SELECT * FROM memories WHERE memory_id = ?;", (memory_id,))
        row = cursor.fetchone()
        if row is None:
            cursor.execute(
                "SELECT * FROM memories WHERE root_memory_id = ? ORDER BY created_at ASC, memory_id ASC;",
                (memory_id,),
            )
            return list(cursor.fetchall())

        root_id = row["root_memory_id"]
        if root_id and root_id != memory_id:
            # root 行是否存在是区分“分片族”与“版本链”的可靠信号：
            # memory_record 多分片的 parent 不落 memories；版本链的 v1 是真实行。
            cursor.execute("SELECT 1 FROM memories WHERE memory_id = ?;", (root_id,))
            root_exists = cursor.fetchone() is not None
            if not root_exists:
                cursor.execute(
                    "SELECT * FROM memories WHERE memory_id = ? OR root_memory_id = ? ORDER BY created_at ASC, memory_id ASC;",
                    (root_id, root_id),
                )
                return list(cursor.fetchall())

        return [row]

    def _validate_superseded_target(
        self, cursor: sqlite3.Cursor, rows: List[sqlite3.Row], superseded_by: str
    ) -> None:
        """校验 superseded_by 目标存在且不指向自身/同族，避免底层 FK 错误不可诊断。"""
        if any(r["memory_id"] == superseded_by for r in rows):
            raise ValueError(
                "INVALID_ARGUMENT: superseded_by must not reference the same memory or its shard family"
            )
        cursor.execute("SELECT 1 FROM memories WHERE memory_id = ?;", (superseded_by,))
        if cursor.fetchone() is None:
            raise ValueError(f"INVALID_ARGUMENT: superseded_by '{superseded_by}' not found")

    def apply_status_change(
        self,
        cursor: sqlite3.Cursor,
        rows: List[sqlite3.Row],
        *,
        new_status: str,
        actor: str = "system",
        superseded_by: Optional[str] = None,
        reason: Optional[str] = None,
        extra_fields: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        纯函数：仅用于 legacy 行的状态变更及 delta 信封入队（RFC v5.2 §4 + RFC v5.3 §1）。
        无 BEGIN/COMMIT，无 Qdrant I/O，可直接嵌入调用方 chunk 事务。
        """
        canonical = normalize_status(new_status)
        now = int(time.time())
        if canonical == "HISTORICAL" and superseded_by:
            self._validate_superseded_target(cursor, rows, superseded_by)
        fields: Dict[str, Any] = {"status": canonical, "updated_at": now}
        if canonical == "HISTORICAL" and superseded_by:
            fields["superseded_by"] = superseded_by
            fields["superseded_at"] = now
        if canonical == "DELETED":
            fields["deleted_at"] = now
            fields["deleted_by"] = actor
            fields["deletion_reason"] = reason
        if extra_fields:
            fields.update(extra_fields)

        # 1. 更新 memories 行与写入审计
        for r in rows:
            set_clauses = [f"{k} = ?" for k in fields.keys()]
            cursor.execute(
                f"UPDATE memories SET {', '.join(set_clauses)} WHERE memory_id = ?;",
                (*fields.values(), r["memory_id"]),
            )
            action = "delete" if canonical == "DELETED" else ("supersede" if canonical == "HISTORICAL" and superseded_by else ("promote" if canonical == "ACTIVE" else "update"))
            audit_dict = {
                "old_status": r["status"],
                "new_status": canonical,
                "superseded_by": superseded_by,
                "deletion_reason": reason,
            }
            if extra_fields:
                audit_dict["extra_fields"] = extra_fields
            cursor.execute(
                "INSERT INTO memory_audit_log (memory_id, action, operator, detail, timestamp) VALUES (?, ?, ?, ?, ?);",
                (r["memory_id"], action, actor, json.dumps(audit_dict, ensure_ascii=False), now),
            )

        # 2. 按 collection 分组生成 delta 任务并写入 qdrant_sync_queue
        collections_map: Dict[str, List[sqlite3.Row]] = {}
        for r in rows:
            c = r["collection_name"] if "collection_name" in r.keys() and r["collection_name"] else "ai_memory"
            collections_map.setdefault(c, []).append(r)

        all_targets: List[Tuple[str, str]] = []
        for c, col_rows in collections_map.items():
            targets = [(r["memory_id"], r["qdrant_point_id"]) for r in col_rows]
            all_targets.extend(targets)
            op = "delete" if canonical == "DELETED" else "patch"
            envelope = {
                "amr_delta_v1": True,
                "op": op,
                "fields": fields,
                "collection": c,
                "targets": targets,
            }
            op_type = "delete" if canonical == "DELETED" else "update_payload"
            first_r = col_rows[0]
            cursor.execute(
                """
                INSERT INTO qdrant_sync_queue (
                    memory_id, qdrant_point_id, op_type, payload_snapshot,
                    status, retry_count, last_error, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'pending', 0, NULL, ?, ?);
                """,
                (first_r["memory_id"], first_r["qdrant_point_id"], op_type, json.dumps(envelope, ensure_ascii=False), now, now),
            )
            new_id = cursor.lastrowid

            # 3. v5.3 §1 取消策略
            family_ids = [t[0] for t in targets]
            placeholders = ",".join("?" for _ in family_ids)
            cursor.execute(
                f"""
                SELECT id, memory_id, op_type, payload_snapshot
                FROM qdrant_sync_queue
                WHERE memory_id IN ({placeholders})
                  AND status IN ('pending', 'failed')
                  AND id < ?;
                """,
                (*family_ids, new_id),
            )
            older_tasks = cursor.fetchall()
            for t in older_tasks:
                t_op = t["op_type"]
                t_id = t["id"]
                if t_op == "upsert":
                    try:
                        snap = json.loads(t["payload_snapshot"]) if t["payload_snapshot"] else {}
                    except Exception:
                        snap = {}
                    snap.update(fields)
                    cursor.execute(
                        "UPDATE qdrant_sync_queue SET payload_snapshot = ?, last_error = ? WHERE id = ?;",
                        (json.dumps(snap, ensure_ascii=False), f"patched by #{new_id}", t_id),
                    )
                elif t_op == "delete":
                    pass
                else:
                    cursor.execute(
                        "UPDATE qdrant_sync_queue SET status = 'synced', last_error = ? WHERE id = ?;",
                        (f"superseded by #{new_id}", t_id),
                    )

        return {
            "new_status": canonical,
            "targets": all_targets,
        }

    def change_memory_status(
        self,
        memory_id: str,
        new_status: str,
        *,
        actor: str = "system",
        superseded_by: Optional[str] = None,
        reason: Optional[str] = None,
        cursor: Optional[sqlite3.Cursor] = None,
        extra_fields: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        统一状态流转分派入口（RFC v5.3 §3）。
        先解析记忆族；族内含 v3 行则走统一投影队列（断言全族一致）；否则走受控 legacy 路径。
        """
        if cursor is None:
            with self.locked_write_transaction() as conn:
                return self.change_memory_status(
                    memory_id,
                    new_status,
                    actor=actor,
                    superseded_by=superseded_by,
                    reason=reason,
                    cursor=conn.cursor(),
                    extra_fields=extra_fields,
                )

        rows = self.resolve_memory_family(cursor, memory_id)
        if not rows:
            raise KeyError(f"Memory '{memory_id}' not found")

        has_v3 = any(r["projection_class"] == "v3" for r in rows)
        if has_v3:
            if not all(r["projection_class"] == "v3" for r in rows):
                raise ValueError(f"INVALID_ARGUMENT: Mixed projection_class in memory family for {memory_id}")

            canonical = normalize_status(new_status)
            if canonical not in CANONICAL_STATUSES:
                raise ValueError(f"Invalid memory status: '{new_status}'. Must be one of {CANONICAL_STATUSES}")
            if canonical == "HISTORICAL" and superseded_by:
                self._validate_superseded_target(cursor, rows, superseded_by)

            now = int(time.time())
            fields: Dict[str, Any] = {"status": canonical, "updated_at": now}
            if canonical == "HISTORICAL" and superseded_by:
                fields["superseded_by"] = superseded_by
                fields["superseded_at"] = now
            if canonical == "DELETED":
                fields["deleted_at"] = now
                fields["deleted_by"] = actor
                fields["deletion_reason"] = reason
            if extra_fields:
                fields.update(extra_fields)

            new_rev = 0
            for r in rows:
                curr_status = (r["status"] or "ACTIVE").upper()
                if curr_status == "DELETED" and canonical != "DELETED":
                    raise ValueError("Illegal status transition: DELETED state is terminal and cannot be reverted.")

                set_clauses = [f"{k} = ?" for k in fields.keys()]
                cursor.execute(
                    f"UPDATE memories SET {', '.join(set_clauses)} WHERE memory_id = ?;",
                    (*fields.values(), r["memory_id"]),
                )
                action = "delete" if canonical == "DELETED" else ("supersede" if canonical == "HISTORICAL" and superseded_by else ("promote" if canonical == "ACTIVE" else "update"))
                audit_dict = {
                    "old_status": r["status"],
                    "new_status": canonical,
                    "superseded_by": superseded_by,
                    "deletion_reason": reason,
                }
                if extra_fields:
                    audit_dict["extra_fields"] = extra_fields
                cursor.execute(
                    "INSERT INTO memory_audit_log (memory_id, action, operator, detail, timestamp) VALUES (?, ?, ?, ?, ?);",
                    (r["memory_id"], action, actor, json.dumps(audit_dict, ensure_ascii=False), now),
                )
                new_rev = self.bump_projection_revision(cursor, r["memory_id"])
                col = r["collection_name"] if "collection_name" in r.keys() and r["collection_name"] else "ai_memory"
                self.enqueue_projection_task(
                    cursor,
                    memory_id=r["memory_id"],
                    projection_revision=new_rev,
                    target_collection=col,
                )

            target_row = next((r for r in rows if r["memory_id"] == memory_id), rows[0])
            return {
                "memory_id": memory_id,
                "previous_status": target_row["status"],
                "current_status": canonical,
                "updated_at": now,
                "projection_status": "pending",
                "projection_revision": new_rev,
            }

        # 全部为 legacy 行
        target_row = next((r for r in rows if r["memory_id"] == memory_id), rows[0])
        prev_status = target_row["status"]
        res = self.apply_status_change(
            cursor,
            rows,
            new_status=new_status,
            actor=actor,
            superseded_by=superseded_by,
            reason=reason,
            extra_fields=extra_fields,
        )
        return {
            "memory_id": memory_id,
            "previous_status": prev_status,
            "current_status": res["new_status"],
            "updated_at": int(time.time()),
            "targets": res.get("targets", []),
        }

    def fetch_pending_projection_tasks(
        self, limit: int = 10, now: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """拉取待处理的统一投影任务 (按 id 升序，支持 next_retry_at 退避时间过滤)"""
        ts = now if now is not None else int(time.time())
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, target_collection, memory_id, projection_revision, request_id,
                       projected_revision, status, retry_count, next_retry_at,
                       last_error, dead_letter_at, created_at, updated_at
                FROM memory_projection_outbox
                WHERE status = 'pending' AND (next_retry_at IS NULL OR next_retry_at <= ?)
                ORDER BY id ASC
                LIMIT ?;
                """,
                (ts, limit),
            )
            rows = cursor.fetchall()
            return [dict(r) for r in rows]

    def get_memory_projection_snapshot(self, memory_id: str) -> Optional[Dict[str, Any]]:
        """单条 SELECT 查询记忆完整状态，返回构造 11 字段投影 payload 所需的全部列"""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM memories WHERE memory_id = ?;", (memory_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def mark_projection_done(
        self, task_id: int, claim_revision: int, projected_revision: Optional[int] = None
    ) -> bool:
        """CAS 标记投影任务已完成 (status='done')。若处理期间被合并改写则更新 0 行保留 pending。"""
        proj_rev = projected_revision if projected_revision is not None else claim_revision
        now = int(time.time())
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE memory_projection_outbox
                SET status = 'done',
                    projected_revision = ?,
                    updated_at = ?
                WHERE id = ? AND status = 'pending' AND projection_revision = ?;
                """,
                (proj_rev, now, task_id, claim_revision),
            )
            conn.commit()
            return cursor.rowcount > 0

    def mark_projection_failed(
        self, task_id: int, claim_revision: int, error: str, next_retry_at: int
    ) -> int:
        """CAS 标记任务失败并设置下一次重试时间。返回受影响行数（0 表示被合并改写，禁止记账）。"""
        now = int(time.time())
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE memory_projection_outbox
                SET retry_count = retry_count + 1,
                    next_retry_at = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE id = ? AND status = 'pending' AND projection_revision = ?;
                """,
                (next_retry_at, error, now, task_id, claim_revision),
            )
            conn.commit()
            return cursor.rowcount

    def mark_projection_dead_letter(self, task_id: int, claim_revision: int) -> bool:
        """CAS 标记任务进入死信状态 (retry_count >= 8)"""
        now = int(time.time())
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE memory_projection_outbox
                SET status = 'dead_letter',
                    dead_letter_at = ?,
                    updated_at = ?
                WHERE id = ? AND status = 'pending' AND projection_revision = ? AND retry_count >= 8;
                """,
                (now, now, task_id, claim_revision),
            )
            conn.commit()
            return cursor.rowcount > 0

    def reenqueue_projection_if_newer(
        self, memory_id: str, claim_revision: int
    ) -> Optional[int]:
        """检查记忆当前 projection_revision；若大于 claim_revision 则用最新值重新入队"""
        with self.locked_write_transaction() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT projection_revision, collection_name, projection_class FROM memories WHERE memory_id = ?;",
                (memory_id,),
            )
            row = cursor.fetchone()
            if not row or row["projection_class"] != "v3":
                return None
            current_rev = row["projection_revision"]
            if current_rev > claim_revision:
                target_col = row["collection_name"] or "ai_memory"
                return self.enqueue_projection_task(
                    cursor,
                    memory_id=memory_id,
                    projection_revision=current_rev,
                    target_collection=target_col,
                )
            return None

    def count_projection_backlog(self) -> Dict[str, Any]:
        """统计统一投影队列积压情况"""
        now = int(time.time())
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM memory_projection_outbox WHERE status = 'pending';")
            pending_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM memory_projection_outbox WHERE status = 'dead_letter';")
            dead_letter_count = cursor.fetchone()[0]

            cursor.execute("SELECT MIN(created_at) FROM memory_projection_outbox WHERE status = 'pending';")
            oldest_row = cursor.fetchone()
            oldest_ts = oldest_row[0] if oldest_row and oldest_row[0] is not None else None
            oldest_age = (now - oldest_ts) if oldest_ts is not None else 0

            return {
                "pending": pending_count,
                "dead_letter": dead_letter_count,
                "oldest_pending_age_seconds": max(0, oldest_age),
            }

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
        collection_name: str = "ai_memory",
        request_id: Optional[str] = None,
        cursor: Optional[sqlite3.Cursor] = None,
    ) -> Dict[str, Any]:
        """
        v3.0 标准记忆创建：
        - 验证 6 态合法性与非空规则
        - 初始版本固定为 version=1
        - 写入 memories 主表 (显式 projection_class='v3', collection_name, projection_revision=1)
        - 写入 revisions 表 (v1 快照)
        - 同步写入统一投影队列 memory_projection_outbox (不再写 qdrant_sync_queue)
        - 若声明 conflicts_with：
            - target 为 v3：UPDATE 目标 status='CONFLICT' + conflicts_with + bump revision + 新队列入队
            - target 为 legacy：走 legacy 受控路径的 delta 入队（不改目标为 v3，不写新队列）
            - 目标不存在：忽略
        - 支持 cursor 传入组合事务
        """
        if cursor is None:
            with self.locked_write_transaction() as conn:
                return self.create_memory_v3(
                    memory_id=memory_id,
                    content=content,
                    project_id=project_id,
                    type=type,
                    status=status,
                    created_by_agent=created_by_agent,
                    source_refs=source_refs,
                    conflicts_with=conflicts_with,
                    qdrant_point_id=qdrant_point_id,
                    operator=operator,
                    collection_name=collection_name,
                    request_id=request_id,
                    cursor=conn.cursor(),
                )

        status_norm = normalize_status(status)
        if status_norm not in CANONICAL_STATUSES:
            raise ValueError(f"Invalid memory status: '{status}'. Must be one of {CANONICAL_STATUSES}")
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
        agent = created_by_agent or "system"
        # 记忆治理宪法第 3 条「任何卡片可溯源」：客户端未带溯源时不留空，
        # 打诚实占位标记（可 grep 审计、可批量追补），而非伪装成已溯源
        refs = [r for r in (source_refs or []) if isinstance(r, str) and r.strip()]
        if not refs:
            refs = ["provenance-missing:%s:%s"
                    % (agent, time.strftime("%Y%m%d_%H%M%S", time.gmtime()))]
        refs_json = json.dumps(refs, ensure_ascii=False)
        conflicts_list = conflicts_with or []
        conflicts_json = json.dumps(conflicts_list, ensure_ascii=False)
        op = operator or agent

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
                projection_revision, collection_name, projection_class,
                created_at, updated_at
            )
            VALUES (?, ?, ?, 'coexist', '', '', NULL, ?, 0, NULL, 'open_ended', 1.0, 0.5, 1,
                    ?, NULL, NULL, NULL, ?, 'NONE', ?, 'global', ?, ?, ?, 1,
                    ?, NULL, NULL, NULL, NULL, NULL, ?, ?, 1, ?, 'v3', ?, ?)
            """,
            (
                memory_id, point_id, type, content,
                status_norm, memory_id, project_id, agent, agent, agent,
                c_hash, refs_json, conflicts_json, collection_name, now, now
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
                cursor.execute(
                    "SELECT memory_id, qdrant_point_id, conflicts_with, status, projection_class, collection_name FROM memories WHERE memory_id = ?;",
                    (target_id,),
                )
                t_row = cursor.fetchone()
                if t_row:
                    try:
                        t_conflicts = json.loads(t_row["conflicts_with"] or "[]")
                    except Exception:
                        t_conflicts = []
                    if memory_id not in t_conflicts:
                        t_conflicts.append(memory_id)
                    t_conflicts_json = json.dumps(t_conflicts, ensure_ascii=False)

                    if t_row["projection_class"] == "v3":
                        cursor.execute(
                            """
                            UPDATE memories
                            SET status = 'CONFLICT',
                                conflicts_with = ?,
                                updated_at = ?
                            WHERE memory_id = ?;
                            """,
                            (t_conflicts_json, now, target_id),
                        )
                        target_rev = self.bump_projection_revision(cursor, target_id)
                        t_col = t_row["collection_name"] or "ai_memory"
                        self.enqueue_projection_task(
                            cursor,
                            memory_id=target_id,
                            projection_revision=target_rev,
                            target_collection=t_col,
                            request_id=request_id,
                        )
                    else:
                        cursor.execute(
                            "UPDATE memories SET conflicts_with = ? WHERE memory_id = ?;",
                            (t_conflicts_json, target_id),
                        )
                        t_family = self.resolve_memory_family(cursor, target_id)
                        if t_family:
                            self.apply_status_change(
                                cursor,
                                t_family,
                                new_status="CONFLICT",
                                actor=agent,
                            )

        # 4. 统一投影队列入队 (不再写 qdrant_sync_queue)
        self.enqueue_projection_task(
            cursor,
            memory_id=memory_id,
            projection_revision=1,
            target_collection=collection_name,
            request_id=request_id,
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

        res = self.get_memory_v3(memory_id, cursor=cursor)
        if res:
            res["projection_status"] = "pending"
            res["projection_revision"] = 1
            res["collection_name"] = collection_name
        return res

    def get_memory_v3(self, memory_id: str, cursor: Optional[sqlite3.Cursor] = None) -> Optional[Dict[str, Any]]:
        """获取 v3.0 记忆实体详情及历史修订版本列表"""
        if cursor is not None:
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

        with self._lock:
            conn = self._get_connection()
            return self.get_memory_v3(memory_id, cursor=conn.cursor())

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
        cursor: Optional[sqlite3.Cursor] = None,
        superseded_by: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        v3.0 记忆修改：
        - 不写 qdrant_sync_queue；变更均 bump_projection_revision 并 enqueue_projection_task
        - 仅修改 type / status：元数据就地更新，不递增版本，不写 revisions 表
        - 修改 content：
            - 必须校验 expected_version（乐观锁），不匹配抛 VersionConflictError
            - 必须提供 change_reason 说明
            - version ++，写入 memory_revisions 快照
        - 状态流转守卫：严禁从 DELETED 逆向流转
        - 冲突目标处理：v3 目标递增+新队列，legacy 目标 delta 任务
        - 支持 cursor 传入组合事务
        """
        if cursor is None:
            with self.locked_write_transaction() as conn:
                return self.update_memory_v3(
                    memory_id=memory_id,
                    content=content,
                    type=type,
                    status=status,
                    expected_version=expected_version,
                    change_reason=change_reason,
                    agent_id=agent_id,
                    conflicts_with=conflicts_with,
                    cursor=conn.cursor(),
                    superseded_by=superseded_by,
                )

        now = int(time.time())
        cursor.execute("SELECT * FROM memories WHERE memory_id = ?;", (memory_id,))
        row = cursor.fetchone()
        if row is None:
            raise KeyError(f"Memory '{memory_id}' not found")

        if row["projection_class"] != "v3":
            raise LegacyPathForbidden(
                memory_id=memory_id,
                projection_class=row["projection_class"],
                hint="legacy 记忆请走受控 legacy 路径；正文变更不支持",
            )

        curr_status = normalize_status(row["status"])
        curr_version = row["version"] or 1

        new_status = normalize_status(status) if status is not None else curr_status
        if new_status not in CANONICAL_STATUSES:
            raise ValueError(f"Invalid status: '{status}'. Must be one of {CANONICAL_STATUSES}")
        if curr_status == "DELETED" and new_status != "DELETED":
            raise ValueError("Illegal status transition: DELETED state is terminal and cannot be reverted.")

        new_type = type if type is not None else (row["type"] or "general")
        content_changed = content is not None and content.strip() != row["content"].strip()
        new_version = curr_version

        if content_changed:
            if expected_version is None:
                raise ValueError("Content modification requires 'expected_version' for optimistic concurrency control.")
            if expected_version != curr_version:
                raise VersionConflictError(
                    f"VERSION_CONFLICT: Expected version {expected_version}, but current version is {curr_version}.",
                    memory_id=memory_id,
                    current_version=curr_version,
                    expected_version=expected_version,
                )
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
        else:
            # 仅元数据 / 状态更新
            deleted_at = now if new_status == "DELETED" else row["deleted_at"]
            deleted_by = agent_id if new_status == "DELETED" else row["deleted_by"]
            sup_by = superseded_by or row["superseded_by"]
            sup_at = now if (new_status == "HISTORICAL" and sup_by) else row["superseded_at"]
            cursor.execute(
                """
                UPDATE memories
                SET type = ?,
                    status = ?,
                    updated_by_agent = ?,
                    updated_at = ?,
                    deleted_at = ?,
                    deleted_by = ?,
                    superseded_by = ?,
                    superseded_at = ?
                WHERE memory_id = ?
                """,
                (new_type, new_status, agent_id, now, deleted_at, deleted_by, sup_by, sup_at, memory_id),
            )

        # 3. 处理冲突
        if conflicts_with is not None:
            conflicts_json = json.dumps(conflicts_with, ensure_ascii=False)
            cursor.execute(
                "UPDATE memories SET conflicts_with = ? WHERE memory_id = ?",
                (conflicts_json, memory_id),
            )
            for target_id in conflicts_with:
                cursor.execute(
                    "SELECT memory_id, qdrant_point_id, conflicts_with, status, projection_class, collection_name FROM memories WHERE memory_id = ?;",
                    (target_id,),
                )
                t_row = cursor.fetchone()
                if t_row:
                    try:
                        t_conflicts = json.loads(t_row["conflicts_with"] or "[]")
                    except Exception:
                        t_conflicts = []
                    if memory_id not in t_conflicts:
                        t_conflicts.append(memory_id)
                    t_conflicts_json = json.dumps(t_conflicts, ensure_ascii=False)

                    if t_row["projection_class"] == "v3":
                        cursor.execute(
                            "UPDATE memories SET status = 'CONFLICT', conflicts_with = ?, updated_at = ? WHERE memory_id = ?;",
                            (t_conflicts_json, now, target_id),
                        )
                        target_rev = self.bump_projection_revision(cursor, target_id)
                        t_col = t_row["collection_name"] or "ai_memory"
                        self.enqueue_projection_task(
                            cursor,
                            memory_id=target_id,
                            projection_revision=target_rev,
                            target_collection=t_col,
                        )
                    else:
                        cursor.execute(
                            "UPDATE memories SET conflicts_with = ? WHERE memory_id = ?;",
                            (t_conflicts_json, target_id),
                        )
                        t_family = self.resolve_memory_family(cursor, target_id)
                        if t_family:
                            self.apply_status_change(
                                cursor,
                                t_family,
                                new_status="CONFLICT",
                                actor=agent_id,
                            )

        # 4. bump projection revision 并入统一投影队列 (不再写 qdrant_sync_queue)
        new_rev = self.bump_projection_revision(cursor, memory_id)
        target_collection = row["collection_name"] or "ai_memory"
        self.enqueue_projection_task(
            cursor,
            memory_id=memory_id,
            projection_revision=new_rev,
            target_collection=target_collection,
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
            "projection_revision": new_rev,
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

        res = self.get_memory_v3(memory_id, cursor=cursor)
        if res:
            res["projection_status"] = "pending"
            res["projection_revision"] = new_rev
        return res

    def close(self) -> None:
        """关闭数据库长连接"""
        with self._lock:
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:
                    pass
                self._conn = None
