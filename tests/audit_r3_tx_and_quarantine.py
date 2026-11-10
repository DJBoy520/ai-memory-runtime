"""
Audit R3 Track: Transaction Ordering & Quarantine Robustness Tests
测试重点攻击面 2:
1. ALTER 加列在存量分类之前
2. isolation_level=None + 显式 BEGIN IMMEDIATE；失败 ROLLBACK 后旧表与数据完好
3. quarantine：先独立 COMMIT 再 raise MigrationBlocked；重复启动 INSERT OR IGNORE 幂等；报告文件内容与路径
4. _amr_migrations 一次性门控：回填/重建只执行一次；第二次初始化 no-op；部分标记已存在时的行为
"""

import json
from pathlib import Path
import sqlite3
import time
from unittest.mock import patch
import pytest

from src.core.errors import MigrationBlocked
from src.core.session_store import SessionStore


def test_alter_columns_before_classification_in_transaction(tmp_path: Path):
    """
    验证：ALTER TABLE 添加 projection_class/projection_revision/collection_name
    在 UPDATE memories 存量分类之前执行，且在同一事务内。
    若旧库只有基础列，分类 SQL 引用 projection_class 不会因列不存在报错。
    """
    db_file = tmp_path / "alter_before_classify.db"
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
    now = int(time.time())
    conn.execute(
        "INSERT INTO memories (memory_id, qdrant_point_id, content, status, source_refs, created_at, updated_at) "
        "VALUES ('mem_test_order', 'pt_1', 'abc', 'ACTIVE', '[]', ?, ?);",
        (now, now),
    )
    conn.commit()
    conn.close()

    store = SessionStore(db_path=db_file)
    store.close()

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("SELECT projection_class, projection_revision, collection_name FROM memories WHERE memory_id = 'mem_test_order';")
    row = cursor.fetchone()
    assert row[0] == "v3"
    assert row[1] == 0
    assert row[2] == "ai_memory"
    conn.close()


def test_rollback_on_migration_failure_preserves_old_db(tmp_path: Path):
    """
    验证：全流程 isolation_level=None + 显式 BEGIN IMMEDIATE ... COMMIT。
    若在迁移过程抛出非 MigrationBlocked 异常，触发 ROLLBACK，旧表结构与数据保持完好。
    """
    db_file = tmp_path / "rollback_test.db"
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
            id INTEGER PRIMARY KEY,
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
        "INSERT INTO memories (memory_id, qdrant_point_id, content, status, source_refs, created_at, updated_at) "
        "VALUES ('mem_rollback_1', 'pt_1', 'data', 'ACTIVE', '[]', ?, ?);",
        (now, now),
    )
    conn.execute(
        "INSERT INTO memory_projection_outbox (id, memory_id, version, projection_type, op_type, status, created_at, updated_at) "
        "VALUES (1, 'mem_rollback_1', 1, 'qdrant_main', 'upsert', 'pending', ?, ?);",
        (now, now),
    )
    conn.commit()
    conn.close()

    # 通过包一层包装 cursor 模拟在 _migrate_schema_v5 中执行建 new 表时发生意外故障
    real_connect = sqlite3.connect

    class FaultyCursor:
        def __init__(self, real_cursor):
            self._real = real_cursor

        def execute(self, sql, *args, **kwargs):
            if "CREATE TABLE memory_projection_outbox_new" in sql:
                raise RuntimeError("Simulated crash during table creation")
            return self._real.execute(sql, *args, **kwargs)

        @property
        def connection(self):
            return self._real.connection

        def __getattr__(self, name):
            return getattr(self._real, name)

    class FaultyConnection:
        def __init__(self, real_conn):
            self._real = real_conn

        @property
        def row_factory(self):
            return self._real.row_factory

        @row_factory.setter
        def row_factory(self, val):
            self._real.row_factory = val

        def cursor(self):
            return FaultyCursor(self._real.cursor())

        def __getattr__(self, name):
            return getattr(self._real, name)

    def faulty_connect(*args, **kwargs):
        conn = real_connect(*args, **kwargs)
        return FaultyConnection(conn)

    with patch("sqlite3.connect", faulty_connect):
        with pytest.raises(RuntimeError, match="Simulated crash"):
            SessionStore(db_path=db_file)

    # 验证 ROLLBACK 后：旧表完好，旧数据完好，_amr_migrations 无未完成标记
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(memory_projection_outbox);")
    cols = {r[1] for r in cursor.fetchall()}
    assert "version" in cols  # 仍保留旧表结构，未换表
    assert "target_collection" not in cols

    cursor.execute("SELECT COUNT(*) FROM memory_projection_outbox;")
    assert cursor.fetchone()[0] == 1

    # _amr_migrations 中不应有已提交的记录
    cursor.execute("SELECT COUNT(*) FROM _amr_migrations;")
    assert cursor.fetchone()[0] == 0
    conn.close()


def test_quarantine_commit_and_migration_blocked(tmp_path: Path):
    """
    验证 quarantine 关键契约：
    1. 先独立 COMMIT 隔离表数据
    2. 再抛出 MigrationBlocked 异常
    3. 旧 outbox 表依然保持旧结构，数据未丢失
    4. migration_report.json 状态为 'blocked'
    """
    db_file = tmp_path / "quarantine_commit.db"
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
            id INTEGER PRIMARY KEY,
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
    # 插入一个孤儿任务
    conn.execute(
        "INSERT INTO memory_projection_outbox (id, memory_id, version, projection_type, op_type, status, created_at, updated_at) "
        "VALUES (999, 'orphan_x', 1, 'qdrant_main', 'upsert', 'pending', ?, ?);",
        (now, now),
    )
    conn.commit()
    conn.close()

    report_path = tmp_path / "migration_report.json"
    with pytest.raises(MigrationBlocked) as exc_info:
        SessionStore(db_path=db_file)

    assert exc_info.value.quarantine_count == 1
    assert str(report_path) in exc_info.value.report_path

    # 验证旧表数据未丢失
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("SELECT memory_id FROM memory_projection_outbox WHERE id = 999;")
    assert cursor.fetchone()[0] == "orphan_x"

    # 验证隔离表已独立 COMMIT
    cursor.execute("SELECT id, memory_id, reason FROM memory_projection_outbox_quarantine WHERE id = 999;")
    q_row = cursor.fetchone()
    assert q_row is not None
    assert q_row[0] == 999
    assert q_row[1] == "orphan_x"
    assert "memory row missing (orphan)" in q_row[2]
    conn.close()

    # 验证报告文件
    assert report_path.exists()
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "blocked"
    assert len(report["quarantine"]) == 1
    assert report["quarantine"][0]["memory_id"] == "orphan_x"


def test_quarantine_idempotent_restart_no_duplicates(tmp_path: Path):
    """
    验证：当旧库由于隔离任务导致 MigrationBlocked 时，
    若运维或应用再次重启尝试初始化，
    INSERT OR IGNORE 保证不会向 memory_projection_outbox_quarantine 插入重复主键，且不会报错。
    """
    db_file = tmp_path / "quarantine_restart.db"
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
            id INTEGER PRIMARY KEY,
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
        "VALUES (50, 'orphan_restart', 1, 'qdrant_main', 'upsert', 'pending', ?, ?);",
        (now, now),
    )
    conn.commit()
    conn.close()

    # 第一次启动：阻断
    with pytest.raises(MigrationBlocked):
        SessionStore(db_path=db_file)

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM memory_projection_outbox_quarantine;")
    assert c.fetchone()[0] == 1
    conn.close()

    # 第二次启动：依然阻断，但隔离表行数依然为 1（INSERT OR IGNORE 幂等生效）
    with pytest.raises(MigrationBlocked):
        SessionStore(db_path=db_file)

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM memory_projection_outbox_quarantine;")
    assert c.fetchone()[0] == 1
    conn.close()


def test_amr_migrations_gate_idempotency(tmp_path: Path):
    """
    验证：_amr_migrations 记录已应用迁移。
    多次 SessionStore 初始化为 no-op，不会重复执行分类回填与 outbox 重建，
    _amr_migrations 内 applied_at 时间戳保持不变。
    """
    db_file = tmp_path / "idempotent_gate.db"
    store1 = SessionStore(db_path=db_file)
    store1.close()

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("SELECT migration_id, applied_at, details FROM _amr_migrations ORDER BY migration_id;")
    first_records = c.fetchall()
    conn.close()

    assert len(first_records) == 2
    mig_ids = [r[0] for r in first_records]
    assert mig_ids == ["v5_outbox_rebuild", "v5_projection_class_backfill"]

    time.sleep(0.01)

    store2 = SessionStore(db_path=db_file)
    store2.close()

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("SELECT migration_id, applied_at, details FROM _amr_migrations ORDER BY migration_id;")
    second_records = c.fetchall()
    conn.close()

    # 两次时间戳与详情完全一致，表明二次初始化完全为 no-op
    assert first_records == second_records


def test_partial_migration_mark_behavior(tmp_path: Path):
    """
    验证部分标记已存在时的行为：
    若库中已有 v5_projection_class_backfill 标记（例如隔离阻断后回填已提交），
    再次运行时回填逻辑被门控跳过；
    若随后修复了 outbox 孤儿任务并重跑，v5_outbox_rebuild 成功完成并写入第二条标记。
    """
    db_file = tmp_path / "partial_mark.db"
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
            id INTEGER PRIMARY KEY,
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
        "VALUES (1, 'fixable_mem', 1, 'qdrant_main', 'upsert', 'pending', ?, ?);",
        (now, now),
    )
    conn.commit()
    conn.close()

    # 第一次运行：因 memories 中无 fixable_mem 导致阻断
    with pytest.raises(MigrationBlocked):
        SessionStore(db_path=db_file)

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("SELECT migration_id FROM _amr_migrations;")
    migs = {r[0] for r in c.fetchall()}
    assert "v5_projection_class_backfill" in migs
    assert "v5_outbox_rebuild" not in migs

    # 人工修复：补齐 memories 表中的行，使得任务变为可转换
    c.execute(
        "INSERT INTO memories (memory_id, qdrant_point_id, content, status, source_refs, created_at, updated_at, projection_class) "
        "VALUES ('fixable_mem', 'pt_fix', 'content', 'ACTIVE', '[]', ?, ?, 'v3');",
        (now, now),
    )
    conn.commit()
    conn.close()

    # 第二次运行：回填被跳过，rebuild 成功完成
    store = SessionStore(db_path=db_file)
    store.close()

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("SELECT migration_id FROM _amr_migrations;")
    migs_after = {r[0] for r in c.fetchall()}
    assert migs_after == {"v5_projection_class_backfill", "v5_outbox_rebuild"}

    # 验证新 outbox 表成功换表且任务完成转换
    c.execute("PRAGMA table_info(memory_projection_outbox);")
    cols = {r[1] for r in c.fetchall()}
    assert "target_collection" in cols
    assert "version" not in cols
    conn.close()
