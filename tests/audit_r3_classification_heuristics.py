"""
Audit R3 Track: Classification Heuristics & Boundary Tests
测试重点攻击面 3:
1. revisions 存在 / 大写规范 status / source_refs 非空 → v3；其余 legacy
2. 大写 DELETED 且无 revisions/source_refs 的边界；source_refs 各类取值 ('[]', ' ', NULL, 非法 JSON)
3. 歧义行计数 (ambiguous_count) 真实性审计
4. 分类后 v3 行 projection_revision/collection_name 默认值；旧 outbox 转换任务的 revision/target_collection 映射
"""

import json
from pathlib import Path
import sqlite3
import time
import pytest

from src.core.session_store import SessionStore


def test_classify_v3_revisions_present(tmp_path: Path):
    """验证：只要存在 memory_revisions 快照行，无论 status 为小写或 source_refs 为空，均判定为 v3。"""
    db_file = tmp_path / "rev_present.db"
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
        CREATE TABLE memory_revisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            memory_id TEXT NOT NULL,
            version INTEGER NOT NULL,
            content TEXT NOT NULL,
            updated_by_agent TEXT NOT NULL,
            updated_at INTEGER NOT NULL,
            change_reason TEXT
        );
        """
    )
    now = int(time.time())
    # status 为小写 'candidate'，source_refs 为空数组
    conn.execute("INSERT INTO memories VALUES ('mem_rev_v3', 'pt_1', 'rev test', 'candidate', '[]', ?, ?);", (now, now))
    conn.execute("INSERT INTO memory_revisions VALUES (1, 'mem_rev_v3', 1, 'rev test', 'agent_1', ?, 'init');", (now,))
    conn.commit()
    conn.close()

    store = SessionStore(db_path=db_file)
    store.close()

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("SELECT projection_class FROM memories WHERE memory_id = 'mem_rev_v3';")
    assert c.fetchone()[0] == "v3"
    conn.close()


def test_classify_v3_uppercase_canonical_statuses(tmp_path: Path):
    """
    验证规范大写状态集合：
    ACTIVE, HISTORICAL, PENDING_VERIFY, CONFLICT, TEMPORARY
    在无 revisions 且 source_refs='[]' 时全部判定为 v3。
    """
    db_file = tmp_path / "upper_status.db"
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
        CREATE TABLE memory_revisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            memory_id TEXT NOT NULL,
            version INTEGER NOT NULL,
            content TEXT NOT NULL,
            updated_by_agent TEXT NOT NULL,
            updated_at INTEGER NOT NULL,
            change_reason TEXT
        );
        """
    )
    now = int(time.time())
    statuses = ["ACTIVE", "HISTORICAL", "PENDING_VERIFY", "CONFLICT", "TEMPORARY"]
    for idx, s in enumerate(statuses):
        conn.execute(f"INSERT INTO memories VALUES ('mem_{idx}', 'pt_{idx}', 'text', '{s}', '[]', ?, ?);", (now, now))
    conn.commit()
    conn.close()

    store = SessionStore(db_path=db_file)
    store.close()

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    for idx in range(len(statuses)):
        c.execute(f"SELECT projection_class FROM memories WHERE memory_id = 'mem_{idx}';")
        assert c.fetchone()[0] == "v3"
    conn.close()


def test_classify_source_refs_variations(tmp_path: Path):
    """
    测试 source_refs 各种取值下的分类行为：
    - '["session:1"]' -> v3
    - '[]' -> legacy
    - '' -> legacy
    - NULL -> legacy
    - ' ' (纯空白字符) -> 边界特例：由于 SQL 为 NOT IN ('', '[]')，被字符串判定为 v3
    - 'bad-json' -> 边界特例：因未做 JSON 解析，被判定为 v3
    """
    db_file = tmp_path / "refs_variation.db"
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
        CREATE TABLE memory_revisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            memory_id TEXT NOT NULL,
            version INTEGER NOT NULL,
            content TEXT NOT NULL,
            updated_by_agent TEXT NOT NULL,
            updated_at INTEGER NOT NULL,
            change_reason TEXT
        );
        """
    )
    now = int(time.time())
    cases = [
        ("mem_json_array", '["session:1"]'),
        ("mem_empty_array", "[]"),
        ("mem_empty_str", ""),
        ("mem_null", None),
        ("mem_spaces", "   "),
        ("mem_bad_json", "{malformed json]"),
    ]
    for mid, ref in cases:
        conn.execute("INSERT INTO memories VALUES (?, 'pt', 'content', 'archived', ?, ?, ?);", (mid, ref, now, now))
    conn.commit()
    conn.close()

    store = SessionStore(db_path=db_file)
    store.close()

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    results = dict(c.execute("SELECT memory_id, projection_class FROM memories;").fetchall())
    conn.close()

    assert results["mem_json_array"] == "v3"
    assert results["mem_empty_array"] == "legacy"
    assert results["mem_empty_str"] == "legacy"
    assert results["mem_null"] == "legacy"
    # 纯空白字符与非规范 JSON 字符串因 SQL NOT IN ('', '[]') 被归为 v3
    assert results["mem_spaces"] == "v3"
    assert results["mem_bad_json"] == "v3"


def test_classify_deleted_status_boundary(tmp_path: Path):
    """
    【核心边界测试】
    大写 DELETED 状态的存量分类边界：
    1. status='DELETED', revisions=0, source_refs='[]' -> 保持 legacy！
       （这正是生产快照中 3 条 2026-09-27 的旧数据的真实形态，必须保持 legacy）
    2. status='DELETED', revisions>0, source_refs='[]' -> 判定为 v3（生命周期证据）
    3. status='DELETED', revisions=0, source_refs='["s1"]' -> 判定为 v3（溯源证据）
    """
    db_file = tmp_path / "deleted_boundary.db"
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
        CREATE TABLE memory_revisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            memory_id TEXT NOT NULL,
            version INTEGER NOT NULL,
            content TEXT NOT NULL,
            updated_by_agent TEXT NOT NULL,
            updated_at INTEGER NOT NULL,
            change_reason TEXT
        );
        """
    )
    now = int(time.time())
    # 1. 对应生产 3 条旧数据形态
    conn.execute("INSERT INTO memories VALUES ('prod_legacy_del', 'pt_1', 'old del', 'DELETED', '[]', ?, ?);", (now, now))
    # 2. 有 revision 的 DELETED
    conn.execute("INSERT INTO memories VALUES ('v3_del_with_rev', 'pt_2', 'v3 del', 'DELETED', '[]', ?, ?);", (now, now))
    conn.execute("INSERT INTO memory_revisions VALUES (1, 'v3_del_with_rev', 1, 'prev', 'sys', ?, 'c');", (now,))
    # 3. 有 source_refs 的 DELETED
    conn.execute("INSERT INTO memories VALUES ('v3_del_with_refs', 'pt_3', 'v3 del 2', 'DELETED', '[\"session:abc\"]', ?, ?);", (now, now))
    conn.commit()
    conn.close()

    store = SessionStore(db_path=db_file)
    store.close()

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    classes = dict(c.execute("SELECT memory_id, projection_class FROM memories;").fetchall())
    conn.close()

    assert classes["prod_legacy_del"] == "legacy"
    assert classes["v3_del_with_rev"] == "v3"
    assert classes["v3_del_with_refs"] == "v3"


def test_ambiguous_count_not_hardcoded_zero(tmp_path: Path):
    """
    【发现复现 / 缺陷证明 - LOW】
    当数据库中存在歧义行（如 source_refs 为 '   '、非合法 JSON，或 status 为 DELETED 且无 revisions 等）时，
    迁移报告中的 ambiguous_count 应当统计歧义行数量，以便人工审计。
    当前代码在 SessionStore 任意路径下均硬编码为 "ambiguous_count": 0。
    """
    db_file = tmp_path / "ambiguous_report.db"
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
        CREATE TABLE memory_revisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            memory_id TEXT NOT NULL,
            version INTEGER NOT NULL,
            content TEXT NOT NULL,
            updated_by_agent TEXT NOT NULL,
            updated_at INTEGER NOT NULL,
            change_reason TEXT
        );
        """
    )
    now = int(time.time())
    # 插入多条特征歧义数据（source_refs 是空白字符、畸形 JSON、DELETED 无 revisions）
    conn.execute("INSERT INTO memories VALUES ('ambig_1', 'pt_1', 'text', 'candidate', '   ', ?, ?);", (now, now))
    conn.execute("INSERT INTO memories VALUES ('ambig_2', 'pt_2', 'text', 'DELETED', '[]', ?, ?);", (now, now))
    conn.commit()
    conn.close()

    store = SessionStore(db_path=db_file)
    store.close()

    report_file = tmp_path / "migration_report.json"
    assert report_file.exists()
    report = json.loads(report_file.read_text(encoding="utf-8"))

    # 预期契约：应探测到歧义行并统计 > 0
    # 当前缺陷：硬编码为 0
    assert report["ambiguous_count"] > 0


def test_projection_defaults_and_outbox_mapping(tmp_path: Path):
    """
    验证：分类后 v3 记忆行的 projection_revision 默认为 0，collection_name 默认为 'ai_memory'；
    转换旧 outbox 任务时，新任务的 projection_revision 和 target_collection
    必须精确映射 memories 表中对应行的 projection_revision 与 collection_name。
    """
    db_file = tmp_path / "defaults_mapping.db"
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
            version INTEGER NOT NULL DEFAULT 9,
            projection_type TEXT NOT NULL DEFAULT 'qdrant_main',
            op_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        """
    )
    now = int(time.time())
    conn.execute("INSERT INTO memories VALUES ('mem_map_test', 'pt_map', 'content', 'ACTIVE', '[]', ?, ?);", (now, now))
    conn.execute("INSERT INTO memory_projection_outbox VALUES (1, 'mem_map_test', 9, 'qdrant_main', 'upsert', 'pending', ?, ?);", (now, now))
    conn.commit()
    conn.close()

    store = SessionStore(db_path=db_file)
    store.close()

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("SELECT projection_revision, collection_name, projection_class FROM memories WHERE memory_id = 'mem_map_test';")
    m_row = c.fetchone()
    assert m_row[0] == 0
    assert m_row[1] == "ai_memory"
    assert m_row[2] == "v3"

    c.execute("SELECT target_collection, projection_revision, memory_id FROM memory_projection_outbox WHERE memory_id = 'mem_map_test';")
    t_row = c.fetchone()
    assert t_row[0] == "ai_memory"
    assert t_row[1] == 0  # 对应 memories 的 projection_revision
    assert t_row[2] == "mem_map_test"
    conn.close()
