"""测试阶段 A 第一层：SQLite Schema、增量迁移、分类回填与 Outbox 重建。"""
import json
import sqlite3
import pytest

from src.core.errors import MigrationBlocked
from src.core.session_store import SessionStore


def test_fresh_db_schema_and_indices(tmp_path):
    """验证全新数据库表结构、列与索引完整性"""
    db_file = str(tmp_path / "fresh.db")
    store = SessionStore(db_path=db_file)

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # 1. 验证 memories 表新列
    cursor.execute("PRAGMA table_info(memories);")
    cols = {row[1]: row[2] for row in cursor.fetchall()}
    assert "projection_revision" in cols
    assert "collection_name" in cols
    assert "projection_class" in cols

    # 2. 验证 memory_projection_outbox 表新结构
    cursor.execute("PRAGMA table_info(memory_projection_outbox);")
    outbox_cols = {row[1] for row in cursor.fetchall()}
    assert "target_collection" in outbox_cols
    assert "memory_id" in outbox_cols
    assert "projection_revision" in outbox_cols
    assert "projected_revision" in outbox_cols
    assert "request_id" in outbox_cols
    assert "status" in outbox_cols
    # 确保旧字段不再存在
    assert "version" not in outbox_cols
    assert "projection_type" not in outbox_cols
    assert "op_type" not in outbox_cols

    # 3. 验证 idempotency_keys 表
    cursor.execute("PRAGMA table_info(idempotency_keys);")
    idem_cols = {row[1] for row in cursor.fetchall()}
    assert "agent_id_raw" in idem_cols
    assert "op" in idem_cols
    assert "idem_key" in idem_cols
    assert "request_digest" in idem_cols
    assert "memory_id" in idem_cols
    assert "response_json" in idem_cols

    # 4. 验证 _amr_migrations 表
    cursor.execute("PRAGMA table_info(_amr_migrations);")
    mig_cols = {row[1] for row in cursor.fetchall()}
    assert "migration_id" in mig_cols
    assert "applied_at" in mig_cols

    # 5. 验证 outbox 索引存在
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='memory_projection_outbox';")
    indices = {row[0] for row in cursor.fetchall()}
    assert "ux_outbox_pending_key" in indices
    assert "ix_outbox_status" in indices

    # 6. 验证已标记的 migration
    cursor.execute("SELECT migration_id FROM _amr_migrations;")
    migs = {row[0] for row in cursor.fetchall()}
    assert "v5_projection_class_backfill" in migs
    assert "v5_outbox_rebuild" in migs

    store.close()
    conn.close()


def test_second_init_idempotent_noop(tmp_path):
    """验证二次初始化为幂等 no-op"""
    db_file = str(tmp_path / "second_init.db")
    store1 = SessionStore(db_path=db_file)
    store1.close()

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("SELECT migration_id, applied_at FROM _amr_migrations ORDER BY migration_id;")
    first_migs = cursor.fetchall()
    conn.close()

    store2 = SessionStore(db_path=db_file)
    store2.close()

    conn2 = sqlite3.connect(db_file)
    cursor2 = conn2.cursor()
    cursor2.execute("SELECT migration_id, applied_at FROM _amr_migrations ORDER BY migration_id;")
    second_migs = cursor2.fetchall()
    conn2.close()

    assert first_migs == second_migs


def test_classification_backfill_logic(tmp_path):
    """验证存量记忆分类回填逻辑（revisions/大写状态/source_refs -> v3，其余 -> legacy）"""
    db_file = str(tmp_path / "backfill.db")
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # 构造旧版表结构
    cursor.execute("""
        CREATE TABLE memories (
            memory_id TEXT PRIMARY KEY,
            qdrant_point_id TEXT,
            content TEXT,
            status TEXT,
            source_refs TEXT,
            created_at INTEGER,
            updated_at INTEGER
        );
    """)
    cursor.execute("""
        CREATE TABLE memory_revisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            memory_id TEXT,
            version INTEGER,
            content TEXT,
            updated_by_agent TEXT,
            updated_at INTEGER,
            change_reason TEXT
        );
    """)

    # 插入待分类数据
    # row1: active, 无 revision, 无 refs -> legacy
    cursor.execute("INSERT INTO memories VALUES ('mem_1', 'pt_1', 'content 1', 'active', '[]', 100, 100);")
    # row2: ACTIVE (大写) -> v3
    cursor.execute("INSERT INTO memories VALUES ('mem_2', 'pt_2', 'content 2', 'ACTIVE', '[]', 100, 100);")
    # row3: HISTORICAL (大写) -> v3
    cursor.execute("INSERT INTO memories VALUES ('mem_3', 'pt_3', 'content 3', 'HISTORICAL', '[]', 100, 100);")
    # row4: candidate 但有 revision -> v3
    cursor.execute("INSERT INTO memories VALUES ('mem_4', 'pt_4', 'content 4', 'candidate', '[]', 100, 100);")
    cursor.execute("INSERT INTO memory_revisions (memory_id, version, content, updated_by_agent, updated_at) VALUES ('mem_4', 1, 'content 4', 'sys', 100);")
    # row5: candidate 但有 source_refs -> v3
    cursor.execute("INSERT INTO memories VALUES ('mem_5', 'pt_5', 'content 5', 'candidate', '[\"ref1\"]', 100, 100);")
    # row6: candidate, 无 revision, source_refs='' -> legacy
    cursor.execute("INSERT INTO memories VALUES ('mem_6', 'pt_6', 'content 6', 'candidate', '', 100, 100);")

    conn.commit()
    conn.close()

    # 启动 SessionStore 触发迁移
    store = SessionStore(db_path=db_file)
    store.close()

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("SELECT memory_id, projection_class FROM memories ORDER BY memory_id;")
    classes = dict(cursor.fetchall())
    conn.close()

    assert classes["mem_1"] == "legacy"
    assert classes["mem_2"] == "v3"
    assert classes["mem_3"] == "v3"
    assert classes["mem_4"] == "v3"
    assert classes["mem_5"] == "v3"
    assert classes["mem_6"] == "legacy"

    # 检查 migration_report.json 生成
    report_file = tmp_path / "migration_report.json"
    assert report_file.exists()
    report = json.loads(report_file.read_text(encoding="utf-8"))
    assert report["v3_count"] == 4
    assert report["legacy_count"] == 2


def test_outbox_rebuild_convertible_tasks(tmp_path):
    """验证可转换任务从旧 outbox 平滑重建为新 outbox schema"""
    db_file = str(tmp_path / "rebuild_ok.db")
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # 建立旧表结构
    cursor.execute("""
        CREATE TABLE memories (
            memory_id TEXT PRIMARY KEY,
            qdrant_point_id TEXT,
            content TEXT,
            status TEXT,
            source_refs TEXT,
            created_at INTEGER,
            updated_at INTEGER
        );
    """)
    cursor.execute("""
        CREATE TABLE memory_projection_outbox (
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
            updated_at INTEGER NOT NULL
        );
    """)

    # 插入一条 v3 记忆（ACTIVE）及对应的旧任务
    cursor.execute("INSERT INTO memories VALUES ('mem_v3_1', 'pt_1', 'hello', 'ACTIVE', '[]', 1000, 1000);")
    cursor.execute("""
        INSERT INTO memory_projection_outbox (
            id, memory_id, version, projection_type, op_type, payload_snapshot, status, retry_count, next_retry_at, created_at, updated_at
        ) VALUES (10, 'mem_v3_1', 1, 'qdrant_main', 'upsert', '{"data":1}', 'pending', 0, 0, 1000, 1000);
    """)
    conn.commit()
    conn.close()

    # 启动触发迁移
    store = SessionStore(db_path=db_file)
    store.close()

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("SELECT target_collection, memory_id, projection_revision, status, retry_count, next_retry_at, projected_revision FROM memory_projection_outbox;")
    tasks = cursor.fetchall()
    conn.close()

    assert len(tasks) == 1
    t = tasks[0]
    assert t[0] == "ai_memory"
    assert t[1] == "mem_v3_1"
    assert t[2] == 0  # memories.projection_revision 默认值 0
    assert t[3] == "pending"
    assert t[4] == 0
    assert t[5] is None
    assert t[6] is None


def test_outbox_rebuild_orphan_task_blocks_and_quarantines(tmp_path):
    """验证包含孤儿任务的旧 outbox 抛出 MigrationBlocked，保留旧表且隔离记录提交"""
    db_file = str(tmp_path / "orphan_block.db")
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE memories (
            memory_id TEXT PRIMARY KEY,
            qdrant_point_id TEXT,
            content TEXT,
            status TEXT,
            source_refs TEXT,
            created_at INTEGER,
            updated_at INTEGER
        );
    """)
    cursor.execute("""
        CREATE TABLE memory_projection_outbox (
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
            updated_at INTEGER NOT NULL
        );
    """)

    # 仅插入旧 outbox 孤儿任务，memories 中无此行
    cursor.execute("""
        INSERT INTO memory_projection_outbox (
            id, memory_id, version, projection_type, op_type, payload_snapshot, status, retry_count, next_retry_at, created_at, updated_at
        ) VALUES (99, 'orphan_mem', 1, 'qdrant_main', 'upsert', '{}', 'pending', 0, 0, 2000, 2000);
    """)
    conn.commit()
    conn.close()

    # 启动必须抛出 MigrationBlocked
    with pytest.raises(MigrationBlocked) as exc_info:
        SessionStore(db_path=db_file)

    assert exc_info.value.quarantine_count == 1
    report_file = tmp_path / "migration_report.json"
    assert str(report_file) in exc_info.value.report_path

    # 验证旧表完好存在且数据未丢失
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(memory_projection_outbox);")
    old_cols = {row[1] for row in cursor.fetchall()}
    assert "version" in old_cols  # 旧表结构原样保留

    cursor.execute("SELECT memory_id FROM memory_projection_outbox WHERE id=99;")
    assert cursor.fetchone()[0] == "orphan_mem"

    # 验证隔离表已独立 COMMIT 写入
    cursor.execute("SELECT id, memory_id, reason FROM memory_projection_outbox_quarantine;")
    q_rows = cursor.fetchall()
    assert len(q_rows) == 1
    assert q_rows[0][0] == 99
    assert q_rows[0][1] == "orphan_mem"
    assert "orphan" in q_rows[0][2]

    # 验证迁移报告
    report = json.loads(report_file.read_text(encoding="utf-8"))
    assert report["status"] == "blocked"
    assert len(report["quarantine"]) == 1
    conn.close()


def test_outbox_rebuild_legacy_task_quarantined(tmp_path):
    """验证 legacy 行的任务不会被转换到 v3 新队列，而是被隔离"""
    db_file = str(tmp_path / "legacy_quarantine.db")
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE memories (
            memory_id TEXT PRIMARY KEY,
            qdrant_point_id TEXT,
            content TEXT,
            status TEXT,
            source_refs TEXT,
            created_at INTEGER,
            updated_at INTEGER
        );
    """)
    cursor.execute("""
        CREATE TABLE memory_projection_outbox (
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
            updated_at INTEGER NOT NULL
        );
    """)

    # memories 行是 legacy (status='active', 无 revision, 无 refs)
    cursor.execute("INSERT INTO memories VALUES ('legacy_mem', 'pt_leg', 'text', 'active', '[]', 500, 500);")
    cursor.execute("""
        INSERT INTO memory_projection_outbox (
            id, memory_id, version, projection_type, op_type, payload_snapshot, status, retry_count, next_retry_at, created_at, updated_at
        ) VALUES (55, 'legacy_mem', 1, 'qdrant_main', 'upsert', '{}', 'pending', 0, 0, 500, 500);
    """)
    conn.commit()
    conn.close()

    with pytest.raises(MigrationBlocked) as exc_info:
        SessionStore(db_path=db_file)

    assert exc_info.value.quarantine_count == 1
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("SELECT id, memory_id, reason FROM memory_projection_outbox_quarantine;")
    q_rows = cursor.fetchall()
    assert len(q_rows) == 1
    assert "legacy" in q_rows[0][2]
    conn.close()
