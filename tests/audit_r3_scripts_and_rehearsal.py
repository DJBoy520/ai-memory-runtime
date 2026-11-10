"""
Audit R3 Track: Migration Script (scripts/migrate_projection_queue.py) & Rehearsal Tests
测试重点攻击面 4:
1. --inventory 只读（源库 hash / mtime 严格不变）
2. --rehearse 仅在副本执行（源库 hash 不变，输出规范对账 JSON）
3. --apply 原地迁移与前后对账
4. --report 输出文件路径与内容完整性
5. 隔离区非空时退出码非 0 且提示人工确认
6. fresh DB / already-new schema 的 no-op
7. 生产快照副本真实演练（1176 行对账、0 隔离、耗时上界、无损 integrity_check）
"""

import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import time
import pytest

from src.core.session_store import SessionStore

PYTHON_EXE = "/home/dj/WorkSpaces/ai-memory-runtime/.venv/bin/python"
SCRIPT_PATH = str(Path(__file__).resolve().parent.parent / "scripts" / "migrate_projection_queue.py")
PROD_SNAPSHOT_PATH = Path("/tmp/amr-rehearsal/sessions_prod_snapshot.db")


def compute_sha256(file_path: Path) -> str:
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def test_script_inventory_strictly_readonly(tmp_path: Path):
    """验证 --inventory 只读属性：目标库的 sha256 和 mtime 严格不变。"""
    db_file = tmp_path / "test_inventory.db"
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE memories (memory_id TEXT PRIMARY KEY, content TEXT);")
    conn.execute("INSERT INTO memories VALUES ('m1', 'hello');")
    conn.commit()
    conn.close()

    sha_before = compute_sha256(db_file)
    mtime_before = os.path.getmtime(db_file)

    res = subprocess.run(
        [PYTHON_EXE, SCRIPT_PATH, "--db", str(db_file), "--inventory"],
        capture_output=True,
        text=True,
        check=True,
    )
    sha_after = compute_sha256(db_file)
    mtime_after = os.path.getmtime(db_file)

    assert sha_before == sha_after
    assert mtime_before == mtime_after

    data = json.loads(res.stdout)
    assert data["schema_status"]["memories"]["table_exists"] is True
    assert data["quarantine"]["count"] == 0


def test_script_rehearse_isolates_replica(tmp_path: Path):
    """验证 --rehearse 仅在临时副本演练，源库 sha256 绝对保持不变。"""
    db_file = tmp_path / "test_rehearse_source.db"
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE memories (memory_id TEXT PRIMARY KEY, qdrant_point_id TEXT, content TEXT, status TEXT);")
    conn.execute("INSERT INTO memories VALUES ('m1', 'pt1', 'hello', 'ACTIVE');")
    conn.commit()
    conn.close()

    sha_before = compute_sha256(db_file)

    res = subprocess.run(
        [PYTHON_EXE, SCRIPT_PATH, "--db", str(db_file), "--rehearse"],
        capture_output=True,
        text=True,
        check=True,
    )
    sha_after = compute_sha256(db_file)
    assert sha_before == sha_after

    data = json.loads(res.stdout)
    assert data["action"] == "rehearse"
    assert data["status"] == "completed"
    assert data["source_db"] == str(db_file.resolve())
    assert "rehearsal_replica.db" in data["replica_db"]


def test_script_rehearse_temp_dir_leak(tmp_path: Path):
    """
    【发现复现 / 缺陷证明 - LOW】
    --rehearse 每次执行均使用 tempfile.mkdtemp 建立临时目录，
    但脚本在完成演练后没有清理临时目录，导致 /tmp 下堆积 amr_rehearse_* 目录与副本数据库文件。
    """
    db_file = tmp_path / "leak_test.db"
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE memories (memory_id TEXT PRIMARY KEY, qdrant_point_id TEXT, content TEXT, status TEXT);")
    conn.commit()
    conn.close()

    res = subprocess.run(
        [PYTHON_EXE, SCRIPT_PATH, "--db", str(db_file), "--rehearse"],
        capture_output=True,
        text=True,
        check=True,
    )
    data = json.loads(res.stdout)
    replica_dir = Path(data["replica_db"]).parent

    # 预期：执行完成后临时副本目录应自动清理（或者提供清理机制）
    # 当前缺陷：replica_dir 依旧存在未删除
    try:
        assert not replica_dir.exists()
    finally:
        if replica_dir.exists():
            shutil.rmtree(replica_dir, ignore_errors=True)


def test_script_quarantine_nonzero_exit_and_alert(tmp_path: Path):
    """
    验证隔离区非空时：
    --inventory, --rehearse, --apply 均返回退出码 1，并在 stderr 输出明确的人工核准警告。
    """
    db_file = tmp_path / "quarantine_script.db"
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
    conn.execute(
        """
        CREATE TABLE memory_projection_outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            memory_id TEXT,
            version INTEGER,
            projection_type TEXT,
            op_type TEXT,
            status TEXT,
            created_at INTEGER,
            updated_at INTEGER
        );
        """
    )
    now = int(time.time())
    conn.execute("INSERT INTO memory_projection_outbox (id, memory_id, version, projection_type, op_type, status, created_at, updated_at) VALUES (1, 'orphan_script', 1, 'qdrant_main', 'upsert', 'pending', ?, ?);", (now, now))
    conn.commit()
    conn.close()

    # 1. 测试 --rehearse 非 0 退出
    res_reh = subprocess.run(
        [PYTHON_EXE, SCRIPT_PATH, "--db", str(db_file), "--rehearse"],
        capture_output=True,
        text=True,
    )
    assert res_reh.returncode == 1
    assert "必须人工确认后才可上线" in (res_reh.stdout + res_reh.stderr)
    reh_json = json.loads(res_reh.stdout)
    assert reh_json["status"] == "blocked"
    assert reh_json["quarantine_count"] == 1

    # 2. 测试 --apply 非 0 退出
    res_app = subprocess.run(
        [PYTHON_EXE, SCRIPT_PATH, "--db", str(db_file), "--apply"],
        capture_output=True,
        text=True,
    )
    assert res_app.returncode == 1
    assert "必须人工确认后才可上线" in (res_app.stdout + res_app.stderr)

    # 3. 测试 --inventory 在已有隔离表时的非 0 退出
    res_inv = subprocess.run(
        [PYTHON_EXE, SCRIPT_PATH, "--db", str(db_file), "--inventory"],
        capture_output=True,
        text=True,
    )
    assert res_inv.returncode == 1
    assert "隔离区存在 1 条任务，必须人工确认后才可上线" in (res_inv.stdout + res_inv.stderr)


def test_script_apply_modifies_target_in_place(tmp_path: Path):
    """验证 --apply 对目标库原地执行模式迁移，返回 0 退出码，各表结构完成升级。"""
    db_file = tmp_path / "apply_target.db"
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
    conn.execute("INSERT INTO memories VALUES ('mem_apply_1', 'pt_1', 'hello', 'ACTIVE', '[]', ?, ?);", (now, now))
    conn.commit()
    conn.close()

    res = subprocess.run(
        [PYTHON_EXE, SCRIPT_PATH, "--db", str(db_file), "--apply"],
        capture_output=True,
        text=True,
        check=True,
    )
    data = json.loads(res.stdout)
    assert data["status"] == "completed"
    assert data["quarantine_count"] == 0

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("PRAGMA table_info(memory_projection_outbox);")
    cols = {r[1] for r in c.fetchall()}
    assert "target_collection" in cols
    c.execute("SELECT projection_class FROM memories WHERE memory_id = 'mem_apply_1';")
    assert c.fetchone()[0] == "v3"
    conn.close()


def test_script_report_file_output(tmp_path: Path):
    """验证 --report 参数将对账 JSON 正确落盘到指定路径。"""
    db_file = tmp_path / "report_arg_test.db"
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE memories (memory_id TEXT PRIMARY KEY, content TEXT);")
    conn.commit()
    conn.close()

    report_out = tmp_path / "custom_dir" / "my_report.json"
    subprocess.run(
        [PYTHON_EXE, SCRIPT_PATH, "--db", str(db_file), "--inventory", "--report", str(report_out)],
        capture_output=True,
        text=True,
        check=True,
    )
    assert report_out.exists()
    file_data = json.loads(report_out.read_text(encoding="utf-8"))
    assert file_data["schema_status"]["memories"]["table_exists"] is True


def test_script_fresh_and_already_new_noop(tmp_path: Path):
    """验证全新空库与已完成迁移的库执行脚本均为 safe no-op。"""
    fresh_db = tmp_path / "fresh_noop.db"
    store = SessionStore(db_path=fresh_db)
    store.close()

    res1 = subprocess.run(
        [PYTHON_EXE, SCRIPT_PATH, "--db", str(fresh_db), "--apply"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert res1.returncode == 0
    data1 = json.loads(res1.stdout)
    assert data1["status"] == "completed"

    # 第二次执行：already_new_schema
    res2 = subprocess.run(
        [PYTHON_EXE, SCRIPT_PATH, "--db", str(fresh_db), "--apply"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert res2.returncode == 0
    data2 = json.loads(res2.stdout)
    assert data2["status"] == "completed"


def test_production_snapshot_rehearsal_full_reproduction(tmp_path: Path):
    """
    【生产快照副本真实演练】
    复制生产快照到 tmp_path 副本，源库 /tmp/amr-rehearsal/sessions_prod_snapshot.db 绝不修改。
    全量验证：
    1. 源库在演练前后 sha256 绝对一致
    2. 副本演练前后对账：1176 总行、1173 v3、3 legacy、3610 旧队列不变、0 隔离
    3. 耗时小于 2.0s
    4. 迁移后副本 PRAGMA integrity_check 为 ok
    """
    assert PROD_SNAPSHOT_PATH.exists(), f"Snapshot not found: {PROD_SNAPSHOT_PATH}"

    sha_source_before = compute_sha256(PROD_SNAPSHOT_PATH)

    # 严格在 tmp_path 中复制副本
    replica_copy = tmp_path / "prod_snapshot_copy.db"
    shutil.copyfile(PROD_SNAPSHOT_PATH, replica_copy)

    # 1. 运行 --inventory
    res_inv = subprocess.run(
        [PYTHON_EXE, SCRIPT_PATH, "--db", str(replica_copy), "--inventory"],
        capture_output=True,
        text=True,
        check=True,
    )
    inv = json.loads(res_inv.stdout)
    assert inv["memories_classification"]["total"] == 1176
    assert inv["old_queue"]["total"] == 3610
    assert inv["old_queue"]["by_status"]["synced"] == 3610
    assert inv["quarantine"]["count"] == 0

    # 2. 测量在副本上执行 --apply 的耗时与结果
    t0 = time.time()
    res_app = subprocess.run(
        [PYTHON_EXE, SCRIPT_PATH, "--db", str(replica_copy), "--apply"],
        capture_output=True,
        text=True,
        check=True,
    )
    duration = time.time() - t0
    assert duration < 2.0, f"Migration took too long: {duration:.3f}s (budget: 2.0s)"

    app_data = json.loads(res_app.stdout)
    assert app_data["status"] == "completed"
    assert app_data["quarantine_count"] == 0
    after_mem = app_data["after"]["memories_classification"]
    assert after_mem["total"] == 1176
    assert after_mem["by_projection_class"]["v3"] == 1173
    assert after_mem["by_projection_class"]["legacy"] == 3
    assert app_data["after"]["old_queue"]["total"] == 3610

    # 3. 验证 SQLite 完整性
    conn = sqlite3.connect(replica_copy)
    c = conn.cursor()
    c.execute("PRAGMA integrity_check;")
    assert c.fetchone()[0] == "ok"
    conn.close()

    # 4. 最终确认源生产快照 sha256 完好无损
    sha_source_after = compute_sha256(PROD_SNAPSHOT_PATH)
    assert sha_source_before == sha_source_after
