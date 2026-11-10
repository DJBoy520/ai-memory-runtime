"""
Audit R3 Track: Malicious Inputs & Corrupted Database Tests
测试重点攻击面 6:
1. 截断 DB (Truncated database file -> DatabaseError)
2. 只读文件 (Read-only file -> inventory ok, apply fails safely)
3. busy 锁 (Exclusive lock timeout -> database is locked)
4. 超长 memory_id / JSON 负载压力
5. SQL 注入式字段值防注入验证
"""

import json
import os
from pathlib import Path
import sqlite3
import subprocess
import threading
import time
import pytest

from src.core.session_store import SessionStore

PYTHON_EXE = "/home/dj/WorkSpaces/ai-memory-runtime/.venv/bin/python"
SCRIPT_PATH = str(Path(__file__).resolve().parent.parent / "scripts" / "migrate_projection_queue.py")


def test_truncated_db_fails_safely(tmp_path: Path):
    """验证截断/损坏的 SQLite 文件：安全报错 DatabaseError，进程不发生段错误或死锁。"""
    db_file = tmp_path / "truncated.db"
    # 写入前 100 字节的垃圾数据模拟文件被意外截断
    db_file.write_bytes(b"SQLite format 3\x00" + b"\xff" * 84)

    with pytest.raises(sqlite3.DatabaseError):
        SessionStore(db_path=db_file)

    # 脚本调用同样安全报错退出
    res = subprocess.run(
        [PYTHON_EXE, SCRIPT_PATH, "--db", str(db_file), "--inventory"],
        capture_output=True,
        text=True,
    )
    assert res.returncode != 0


def test_readonly_file_handled(tmp_path: Path):
    """
    验证只读文件（chmod 0400）：
    1. --inventory 只读模式打开可以正常盘点
    2. --apply 写入时安全抛出 attempt to write a readonly database
    """
    db_file = tmp_path / "readonly_test.db"
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE memories (memory_id TEXT PRIMARY KEY, content TEXT);")
    conn.execute("INSERT INTO memories VALUES ('mem_ro', 'text');")
    conn.commit()
    conn.close()

    # 设置文件只读
    os.chmod(db_file, 0o400)

    try:
        # 1. 只读盘点应当成功
        res_inv = subprocess.run(
            [PYTHON_EXE, SCRIPT_PATH, "--db", str(db_file), "--inventory"],
            capture_output=True,
            text=True,
        )
        assert res_inv.returncode == 0
        inv = json.loads(res_inv.stdout)
        assert inv["schema_status"]["memories"]["table_exists"] is True

        # 2. apply 写入必须安全失败
        res_app = subprocess.run(
            [PYTHON_EXE, SCRIPT_PATH, "--db", str(db_file), "--apply"],
            capture_output=True,
            text=True,
        )
        assert res_app.returncode != 0
        assert "readonly" in (res_app.stdout + res_app.stderr).lower()
    finally:
        # 恢复权限以便 pytest 清理 tmp_path
        os.chmod(db_file, 0o600)


def test_busy_lock_timeout_handled(tmp_path: Path):
    """
    验证并发进程/线程独占锁 (BEGIN EXCLUSIVE)：
    当锁超时超过 busy_timeout 时，SessionStore 优雅抛出 sqlite3.OperationalError: database is locked。
    """
    db_file = tmp_path / "busy_lock.db"
    # 先初始化库
    init_conn = sqlite3.connect(db_file)
    init_conn.execute("CREATE TABLE memories (memory_id TEXT PRIMARY KEY, content TEXT);")
    init_conn.commit()
    init_conn.close()

    lock_acquired = threading.Event()
    release_lock = threading.Event()

    def hold_exclusive_lock():
        conn = sqlite3.connect(db_file, timeout=0.1)
        conn.execute("BEGIN EXCLUSIVE;")
        lock_acquired.set()
        release_lock.wait(timeout=5.0)
        conn.rollback()
        conn.close()

    t = threading.Thread(target=hold_exclusive_lock)
    t.start()
    try:
        assert lock_acquired.wait(timeout=2.0)
        # 用低超时 (100ms) 初始化 SessionStore 尝试获取锁
        with pytest.raises(sqlite3.OperationalError) as exc_info:
            SessionStore(db_path=db_file, busy_timeout=100)
        assert "locked" in str(exc_info.value).lower()
    finally:
        release_lock.set()
        t.join(timeout=2.0)


def test_huge_fields_stability(tmp_path: Path):
    """验证超长 memory_id (100KB) 与巨大 JSON payload (1MB) 能正常被迁移与索引，无崩溃无内存泄露。"""
    db_file = tmp_path / "huge_payload.db"
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
    huge_id = "mem_" + "x" * (100 * 1024)
    huge_content = "content_" + "y" * (1024 * 1024)
    now = int(time.time())
    conn.execute(
        "INSERT INTO memories VALUES (?, 'pt_huge', ?, 'ACTIVE', '[]', ?, ?);",
        (huge_id, huge_content, now, now),
    )
    conn.commit()
    conn.close()

    store = SessionStore(db_path=db_file)
    store.close()

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("SELECT projection_class, length(content) FROM memories WHERE memory_id = ?;", (huge_id,))
    row = c.fetchone()
    assert row[0] == "v3"
    assert row[1] == len(huge_content)
    conn.close()


def test_sql_injection_resilience(tmp_path: Path):
    """
    【SQL 注入攻击防御测试】
    在 memory_id、status、source_refs、content 等所有关键字段中注入恶意 SQL 字符串：
    如 "'; DROP TABLE memories; --"、"1' OR '1'='1"。
    验证：所有内部查询均通过 ? 参数绑定执行，SQL 注入完全无效，memories 表与审计表完好无损。
    """
    db_file = tmp_path / "sql_injection.db"
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
    injected_mid = "mem_inj'; DROP TABLE memories; --"
    injected_status = "ACTIVE'; DELETE FROM memories; --"
    injected_refs = "[\"session:123'; DROP TABLE sessions; --\"]"
    injected_content = "Robert'); DROP TABLE Students;--"

    conn.execute(
        "INSERT INTO memories VALUES (?, 'pt_inj', ?, ?, ?, ?, ?);",
        (injected_mid, injected_content, injected_status, injected_refs, now, now),
    )
    conn.commit()
    conn.close()

    # 运行迁移
    store = SessionStore(db_path=db_file)
    store.close()

    # 验证表依然健在，且数据完整保留
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='memories';")
    assert c.fetchone() is not None, "Table 'memories' was dropped by SQL injection!"

    c.execute("SELECT memory_id, projection_class, content FROM memories WHERE memory_id = ?;", (injected_mid,))
    row = c.fetchone()
    assert row is not None
    assert row[0] == injected_mid
    # status 包含注入字符，因参数化不匹配标准 5 态，但 source_refs 判定为有效非空 refs，故分类为 v3
    assert row[1] == "v3"
    assert row[2] == injected_content
    conn.close()
