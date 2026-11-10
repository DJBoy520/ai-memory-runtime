"""
Audit R3 Track: Schema Variants & Outbox Structure Tests
测试重点攻击面 1:
1. 极老 memories（缺 status / qdrant_point_id / project_id / type 等列）
2. 表组合形态（有 memories 无 outbox、有旧 outbox 无 memories、空库、仅 sqlite_sequence）
3. 旧 outbox 任务形态（可转换 v3、legacy 隔离、orphan 隔离、mixed、重复任务冲突、缺失 id 列、非 pending 状态复活）
"""

import json
from pathlib import Path
import sqlite3
import time
import pytest

from src.core.errors import MigrationBlocked
from src.core.session_store import SessionStore


def test_old_schema_missing_status_graceful_migration(tmp_path: Path):
    """
    【发现复现 / 缺陷证明 - MEDIUM】
    极老 memories 表缺少 status 列时，迁移应平滑补全 status 列并完成迁移。
    当前代码由于 _migrate_schema_v2 列补全字典未包含 status，
    在尝试建立 idx_memories_status 索引时直接崩溃：OperationalError: no such column: status。
    """
    db_file = tmp_path / "missing_status.db"
    conn = sqlite3.connect(db_file)
    conn.execute(
        """
        CREATE TABLE memories (
            memory_id TEXT PRIMARY KEY,
            qdrant_point_id TEXT,
            content TEXT
        );
        """
    )
    conn.commit()
    conn.close()

    # 预期契约：SessionStore 应当自动完成 schema 升级并补齐 status 默认值
    store = SessionStore(db_path=db_file)
    store.close()

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(memories);")
    cols = {r[1] for r in cursor.fetchall()}
    assert "status" in cols
    conn.close()


def test_old_schema_missing_point_id_graceful_migration(tmp_path: Path):
    """
    【发现复现 / 缺陷证明 - MEDIUM】
    极老 memories 表缺少 qdrant_point_id 列时，v2 列补全字典未包含该列，
    在 CREATE_TABLES_SQL 阶段建立 idx_mem_point 索引时抛出 sqlite3.OperationalError: no such column: qdrant_point_id。
    预期契约应平滑补齐或诊断提示，而非未捕获的 OperationalError 崩溃。
    """
    db_file = tmp_path / "missing_point_id.db"
    conn = sqlite3.connect(db_file)
    conn.execute(
        """
        CREATE TABLE memories (
            memory_id TEXT PRIMARY KEY,
            content TEXT,
            status TEXT
        );
        """
    )
    conn.commit()
    conn.close()

    # 预期契约：极老库初始化成功补齐列
    store = SessionStore(db_path=db_file)
    store.close()

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(memories);")
    cols = {r[1] for r in cursor.fetchall()}
    assert "qdrant_point_id" in cols
    conn.close()


def test_old_schema_missing_project_id_and_type_handled(tmp_path: Path):
    """
    验证 memories 表缺少 project_id, type, source_refs 等字段时，
    _migrate_schema_v2 能成功通过 ALTER TABLE 补全默认值。
    """
    db_file = tmp_path / "missing_project_id.db"
    conn = sqlite3.connect(db_file)
    conn.execute(
        """
        CREATE TABLE memories (
            memory_id TEXT PRIMARY KEY,
            qdrant_point_id TEXT,
            content TEXT,
            status TEXT
        );
        """
    )
    conn.execute("INSERT INTO memories VALUES ('mem_1', 'pt_1', 'hello', 'ACTIVE');")
    conn.commit()
    conn.close()

    store = SessionStore(db_path=db_file)
    store.close()

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(memories);")
    cols = {row[1]: row[2] for row in cursor.fetchall()}
    assert "project_id" in cols
    assert "type" in cols
    assert "source_refs" in cols
    assert "projection_class" in cols

    cursor.execute("SELECT project_id, type, projection_class FROM memories WHERE memory_id = 'mem_1';")
    row = cursor.fetchone()
    assert row[0] == "global"
    assert row[1] == "general"
    assert row[2] == "v3"  # status 为 ACTIVE 故被分类为 v3
    conn.close()


def test_memories_present_outbox_missing(tmp_path: Path):
    """
    验证：有 memories 且无 memory_projection_outbox 的旧库（常见于早期生产环境），
    在新初始化时平滑建表，outbox 重建段标记为 already_new_schema，不报错。
    """
    db_file = tmp_path / "memories_no_outbox.db"
    conn = sqlite3.connect(db_file)
    conn.execute(
        """
        CREATE TABLE memories (
            memory_id TEXT PRIMARY KEY,
            qdrant_point_id TEXT,
            content TEXT,
            status TEXT,
            source_refs TEXT
        );
        """
    )
    conn.execute("INSERT INTO memories VALUES ('mem_exist', 'pt_exist', 'data', 'ACTIVE', '[]');")
    conn.commit()
    conn.close()

    store = SessionStore(db_path=db_file)
    store.close()

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(memory_projection_outbox);")
    outbox_cols = {row[1] for row in cursor.fetchall()}
    assert "target_collection" in outbox_cols
    assert "projection_revision" in outbox_cols

    cursor.execute("SELECT migration_id, details FROM _amr_migrations WHERE migration_id = 'v5_outbox_rebuild';")
    mig_row = cursor.fetchone()
    assert mig_row is not None
    assert mig_row[1] == "already_new_schema"
    conn.close()


def test_old_outbox_present_memories_missing(tmp_path: Path):
    """
    验证：有旧 outbox 但 memories 表不存在（空主表），
    旧 outbox 内全部任务判定为 orphan 孤儿任务，隔离并抛出 MigrationBlocked。
    """
    db_file = tmp_path / "outbox_no_memories.db"
    conn = sqlite3.connect(db_file)
    conn.execute(
        """
        CREATE TABLE memory_projection_outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            memory_id TEXT NOT NULL,
            version INTEGER NOT NULL DEFAULT 1,
            projection_type TEXT NOT NULL DEFAULT 'qdrant_main',
            op_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        """
    )
    now = int(time.time())
    conn.execute(
        "INSERT INTO memory_projection_outbox (id, memory_id, version, projection_type, op_type, status, created_at, updated_at) "
        "VALUES (1, 'ghost_mem', 1, 'qdrant_main', 'upsert', 'pending', ?, ?);",
        (now, now),
    )
    conn.commit()
    conn.close()

    with pytest.raises(MigrationBlocked) as exc_info:
        SessionStore(db_path=db_file)
    assert exc_info.value.quarantine_count == 1

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("SELECT id, memory_id, reason FROM memory_projection_outbox_quarantine;")
    q_row = cursor.fetchone()
    assert q_row[0] == 1
    assert q_row[1] == "ghost_mem"
    assert "memory row missing (orphan)" in q_row[2]
    conn.close()


def test_fresh_database_0_bytes(tmp_path: Path):
    """验证全新空库（0 字节文件）初始化正常，两个迁移标记均成功落库。"""
    db_file = tmp_path / "fresh_0byte.db"
    db_file.touch()

    store = SessionStore(db_path=db_file)
    store.close()

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("SELECT migration_id FROM _amr_migrations ORDER BY migration_id;")
    migs = [r[0] for r in cursor.fetchall()]
    assert migs == ["v5_outbox_rebuild", "v5_projection_class_backfill"]
    conn.close()


def test_sqlite_sequence_only_database(tmp_path: Path):
    """验证仅有 sqlite_sequence 伪表的库能正常被 SessionStore 初始化。"""
    db_file = tmp_path / "sequence_only.db"
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE dummy (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);")
    conn.execute("INSERT INTO dummy (name) VALUES ('x');")
    conn.execute("DROP TABLE dummy;")
    # 此时仅剩下 sqlite_sequence
    conn.commit()
    conn.close()

    store = SessionStore(db_path=db_file)
    store.close()

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(memories);")
    assert len(cursor.fetchall()) > 0
    conn.close()


def test_old_outbox_convertible_v3_task(tmp_path: Path):
    """验证合法 v3 行关联的任务平滑迁移到新 memory_projection_outbox。"""
    db_file = tmp_path / "convertible.db"
    conn = sqlite3.connect(db_file)
    conn.execute(
        """
        CREATE TABLE memories (
            memory_id TEXT PRIMARY KEY,
            qdrant_point_id TEXT,
            content TEXT,
            status TEXT,
            source_refs TEXT,
            created_at INTEGER,
            updated_at INTEGER
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE memory_projection_outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            memory_id TEXT NOT NULL,
            version INTEGER NOT NULL DEFAULT 1,
            projection_type TEXT NOT NULL DEFAULT 'qdrant_main',
            op_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            retry_count INTEGER NOT NULL DEFAULT 0,
            next_retry_at INTEGER,
            last_error TEXT,
            dead_letter_at INTEGER,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        """
    )
    now = int(time.time())
    conn.execute("INSERT INTO memories VALUES ('mem_v3_ok', 'pt_1', 'hello', 'ACTIVE', '[]', ?, ?);", (now, now))
    conn.execute(
        "INSERT INTO memory_projection_outbox (id, memory_id, version, projection_type, op_type, status, retry_count, next_retry_at, created_at, updated_at) "
        "VALUES (101, 'mem_v3_ok', 1, 'qdrant_main', 'upsert', 'pending', 0, NULL, ?, ?);",
        (now, now),
    )
    conn.commit()
    conn.close()

    store = SessionStore(db_path=db_file)
    store.close()

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("SELECT memory_id, target_collection, projection_revision, status FROM memory_projection_outbox;")
    tasks = cursor.fetchall()
    assert len(tasks) == 1
    assert tasks[0][0] == "mem_v3_ok"
    assert tasks[0][1] == "ai_memory"
    assert tasks[0][2] == 0
    assert tasks[0][3] == "pending"
    conn.close()


def test_old_outbox_legacy_row_task_quarantined(tmp_path: Path):
    """验证 legacy 记忆关联的任务必须被隔离并阻断。"""
    db_file = tmp_path / "legacy_quarantine.db"
    conn = sqlite3.connect(db_file)
    conn.execute(
        """
        CREATE TABLE memories (
            memory_id TEXT PRIMARY KEY,
            qdrant_point_id TEXT,
            content TEXT,
            status TEXT,
            source_refs TEXT,
            created_at INTEGER,
            updated_at INTEGER
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE memory_projection_outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            memory_id TEXT NOT NULL,
            version INTEGER NOT NULL DEFAULT 1,
            projection_type TEXT NOT NULL DEFAULT 'qdrant_main',
            op_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        """
    )
    now = int(time.time())
    # status 为小写 active 且无 revisions 无 source_refs，判定为 legacy
    conn.execute("INSERT INTO memories VALUES ('mem_leg', 'pt_leg', 'old', 'active', '[]', ?, ?);", (now, now))
    conn.execute(
        "INSERT INTO memory_projection_outbox (id, memory_id, version, projection_type, op_type, status, created_at, updated_at) "
        "VALUES (1, 'mem_leg', 1, 'qdrant_main', 'upsert', 'pending', ?, ?);",
        (now, now),
    )
    conn.commit()
    conn.close()

    with pytest.raises(MigrationBlocked) as exc_info:
        SessionStore(db_path=db_file)
    assert exc_info.value.quarantine_count == 1

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("SELECT memory_id, reason FROM memory_projection_outbox_quarantine;")
    q_row = cursor.fetchone()
    assert q_row[0] == "mem_leg"
    assert "projection_class is 'legacy', expected 'v3'" in q_row[1]
    conn.close()


def test_old_outbox_mixed_tasks_all_quarantined_safely(tmp_path: Path):
    """
    验证混合任务（1 个合法 v3、1 个 legacy、1 个 orphan）：
    由于存在隔离任务，整个迁移被阻断，旧 outbox 表保持完好，仅非法任务写入隔离表。
    """
    db_file = tmp_path / "mixed_outbox.db"
    conn = sqlite3.connect(db_file)
    conn.execute(
        """
        CREATE TABLE memories (
            memory_id TEXT PRIMARY KEY,
            qdrant_point_id TEXT,
            content TEXT,
            status TEXT,
            source_refs TEXT,
            created_at INTEGER,
            updated_at INTEGER
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE memory_projection_outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            memory_id TEXT NOT NULL,
            version INTEGER NOT NULL DEFAULT 1,
            projection_type TEXT NOT NULL DEFAULT 'qdrant_main',
            op_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        """
    )
    now = int(time.time())
    conn.execute("INSERT INTO memories VALUES ('mem_v3', 'pt_v3', 'data', 'ACTIVE', '[]', ?, ?);", (now, now))
    conn.execute("INSERT INTO memories VALUES ('mem_leg', 'pt_leg', 'data', 'active', '[]', ?, ?);", (now, now))

    # 3 个任务：1 个 v3, 1 个 legacy, 1 个 orphan (mem_ghost)
    conn.execute("INSERT INTO memory_projection_outbox VALUES (1, 'mem_v3', 1, 'qdrant_main', 'upsert', 'pending', ?, ?);", (now, now))
    conn.execute("INSERT INTO memory_projection_outbox VALUES (2, 'mem_leg', 1, 'qdrant_main', 'upsert', 'pending', ?, ?);", (now, now))
    conn.execute("INSERT INTO memory_projection_outbox VALUES (3, 'mem_ghost', 1, 'qdrant_main', 'upsert', 'pending', ?, ?);", (now, now))
    conn.commit()
    conn.close()

    with pytest.raises(MigrationBlocked) as exc_info:
        SessionStore(db_path=db_file)
    assert exc_info.value.quarantine_count == 2  # legacy 与 orphan 共 2 条

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    # 验证旧表结构与 3 条数据完整保留
    cursor.execute("PRAGMA table_info(memory_projection_outbox);")
    cols = {r[1] for r in cursor.fetchall()}
    assert "version" in cols  # 仍为旧 schema
    cursor.execute("SELECT COUNT(*) FROM memory_projection_outbox;")
    assert cursor.fetchone()[0] == 3

    # 验证隔离区仅记录 2 条任务
    cursor.execute("SELECT memory_id, reason FROM memory_projection_outbox_quarantine ORDER BY id;")
    q_rows = cursor.fetchall()
    assert len(q_rows) == 2
    assert q_rows[0][0] == "mem_leg"
    assert q_rows[1][0] == "mem_ghost"
    conn.close()


def test_old_outbox_duplicate_tasks_integrity_error(tmp_path: Path):
    """
    【发现复现 / 缺陷证明 - HIGH】
    旧 outbox 表允许多条指向同一 memory_id 的任务（如并发多次更新）。
    RFC v5.1 §3.1 规格要求将其合并/去重为单个 pending 任务。
    当前代码盲目逐行 INSERT，随后建立 ux_outbox_pending_key 唯一索引时
    抛出 sqlite3.IntegrityError: UNIQUE constraint failed 崩溃！
    """
    db_file = tmp_path / "dup_tasks.db"
    conn = sqlite3.connect(db_file)
    conn.execute(
        """
        CREATE TABLE memories (
            memory_id TEXT PRIMARY KEY,
            qdrant_point_id TEXT,
            content TEXT,
            status TEXT,
            source_refs TEXT,
            created_at INTEGER,
            updated_at INTEGER
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE memory_projection_outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            memory_id TEXT NOT NULL,
            version INTEGER NOT NULL DEFAULT 1,
            projection_type TEXT NOT NULL DEFAULT 'qdrant_main',
            op_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        """
    )
    now = int(time.time())
    conn.execute("INSERT INTO memories VALUES ('mem_dup_target', 'pt_1', 'content', 'ACTIVE', '[]', ?, ?);", (now, now))
    # 同一 memory 插入两条 pending 任务
    conn.execute("INSERT INTO memory_projection_outbox VALUES (1, 'mem_dup_target', 1, 'qdrant_main', 'upsert', 'pending', ?, ?);", (now, now))
    conn.execute("INSERT INTO memory_projection_outbox VALUES (2, 'mem_dup_target', 2, 'qdrant_main', 'upsert', 'pending', ?, ?);", (now, now))
    conn.commit()
    conn.close()

    # 预期：迁移应自动去重/合并为 1 条 pending 任务，顺利完成建表
    store = SessionStore(db_path=db_file)
    store.close()

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM memory_projection_outbox WHERE memory_id = 'mem_dup_target' AND status = 'pending';")
    assert cursor.fetchone()[0] == 1
    conn.close()


def test_old_outbox_missing_id_column_quarantine(tmp_path: Path):
    """
    【发现复现 / 缺陷证明 - HIGH】
    当旧 memory_projection_outbox 表缺少 id 列时，line 708
    {"id": r["id"], ...} 触发 KeyError: 'id'，导致迁移未捕获异常并整体 ROLLBACK，
    隔离记录未能持久化提交，且未正常 raise MigrationBlocked。
    """
    db_file = tmp_path / "missing_outbox_id.db"
    conn = sqlite3.connect(db_file)
    conn.execute(
        """
        CREATE TABLE memories (
            memory_id TEXT PRIMARY KEY,
            qdrant_point_id TEXT,
            content TEXT,
            status TEXT,
            source_refs TEXT,
            created_at INTEGER,
            updated_at INTEGER
        );
        """
    )
    # 旧表以 memory_id 为主键，没有名为 id 的整型列
    conn.execute(
        """
        CREATE TABLE memory_projection_outbox (
            memory_id TEXT PRIMARY KEY,
            version INTEGER NOT NULL DEFAULT 1,
            projection_type TEXT NOT NULL DEFAULT 'qdrant_main',
            op_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        """
    )
    now = int(time.time())
    # 插入一个孤儿任务
    conn.execute("INSERT INTO memory_projection_outbox VALUES ('orphan_without_id', 1, 'qdrant_main', 'upsert', 'pending', ?, ?);", (now, now))
    conn.commit()
    conn.close()

    # 预期契约：应安全落入隔离区并抛出 MigrationBlocked
    with pytest.raises(MigrationBlocked) as exc_info:
        SessionStore(db_path=db_file)
    assert exc_info.value.quarantine_count == 1


def test_old_outbox_non_pending_status_not_revived(tmp_path: Path):
    """
    【发现复现 / 缺陷证明 - MEDIUM】
    旧 outbox 表中已完成 (status='done') 的历史任务，
    在迁移时应保持其完成态或不应重新入队 pending。
    当前代码强制 status='pending'，将历史已完成任务错误“复活”。
    """
    db_file = tmp_path / "revived_tasks.db"
    conn = sqlite3.connect(db_file)
    conn.execute(
        """
        CREATE TABLE memories (
            memory_id TEXT PRIMARY KEY,
            qdrant_point_id TEXT,
            content TEXT,
            status TEXT,
            source_refs TEXT,
            created_at INTEGER,
            updated_at INTEGER
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE memory_projection_outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            memory_id TEXT NOT NULL,
            version INTEGER NOT NULL DEFAULT 1,
            projection_type TEXT NOT NULL DEFAULT 'qdrant_main',
            op_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        """
    )
    now = int(time.time())
    conn.execute("INSERT INTO memories VALUES ('mem_done', 'pt_done', 'content', 'ACTIVE', '[]', ?, ?);", (now, now))
    # 插入已完成的任务
    conn.execute("INSERT INTO memory_projection_outbox VALUES (1, 'mem_done', 1, 'qdrant_main', 'upsert', 'done', ?, ?);", (now, now))
    conn.commit()
    conn.close()

    store = SessionStore(db_path=db_file)
    store.close()

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("SELECT memory_id, status FROM memory_projection_outbox WHERE memory_id = 'mem_done';")
    row = cursor.fetchone()
    # 预期：已完成的任务状态不应被重置为 pending
    assert row[1] == "done"
    conn.close()
