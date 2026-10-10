import concurrent.futures
import json
import os
import shutil
import sqlite3
import tempfile
import time
import uuid
import pytest

from config.settings import AppConfig
from src.core.session_store import SessionStore


@pytest.fixture
def temp_v2_store():
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test_v2_sessions.db")
    config = AppConfig()
    config.storage.sqlite_path = db_path
    config.storage.wal_enabled = True
    config.storage.busy_timeout = 5000

    store = SessionStore(config)
    yield store
    store.close()
    shutil.rmtree(temp_dir, ignore_errors=True)


def test_v2_tables_initialization(temp_v2_store):
    """验证 4 张业务表 + 2 张治理表在初始化时完整创建"""
    with temp_v2_store.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = {row[0] for row in cursor.fetchall()}

        expected_tables = {
            "raw_sessions",
            "raw_messages",
            "memories",
            "memory_evidence",
            "qdrant_sync_queue",
            "memory_audit_log",
        }
        for t in expected_tables:
            assert t in tables, f"Expected table {t} to exist in DB"


def test_raw_sessions_and_messages(temp_v2_store):
    """验证 raw_sessions 和 raw_messages 的写入与约束支持 (source_type, is_synthetic)"""
    sess = temp_v2_store.record_raw_session(
        session_id="sess_v2_001",
        agent_id="test_agent",
        project_id="test_project",
    )
    assert sess["session_id"] == "sess_v2_001"
    assert sess["project_id"] == "test_project"

    # 写入带有 source_type 与 is_synthetic 的消息
    msg = temp_v2_store.record_raw_message(
        message_id="msg_v2_001",
        session_id="sess_v2_001",
        role="user",
        content="AMR engine upgrade v2.2 test",
        sequence=1,
        source_type="user_message",
        is_synthetic=0,
    )
    assert msg["message_id"] == "msg_v2_001"
    assert msg["source_type"] == "user_message"
    assert msg["is_synthetic"] == 0

    # 写入合成证据消息 (例如 legacy_memory 导入)
    msg_syn = temp_v2_store.record_raw_message(
        message_id="msg_v2_synthetic",
        session_id="sess_v2_001",
        role="system",
        content="Imported historical memory fact",
        sequence=2,
        source_type="legacy_memory",
        is_synthetic=1,
    )
    assert msg_syn["source_type"] == "legacy_memory"
    assert msg_syn["is_synthetic"] == 1


def test_create_memory_with_evidence_and_outbox_atomicity(temp_v2_store):
    """验证 create_memory 原子性写入 memories, evidence, outbox 队列与 audit_log"""
    temp_v2_store.record_raw_session("sess_100", "agent_coder", "proj_x")
    temp_v2_store.record_raw_message("msg_101", "sess_100", "user", "User prefers Python", 1)

    evidence_list = [
        {"message_id": "msg_101", "session_id": "sess_100", "evidence_strength": 0.75}
    ]

    mem = temp_v2_store.create_memory(
        memory_id="mem_pref_001",
        subject="user",
        predicate="prefers",
        object="python",
        content="User prefers Python for data and backend systems",
        type="preference",
        conflict_policy="coexist",
        confidence=0.85,
        importance=0.6,
        project_id="proj_x",
        scope="project",
        source_agent="agent_coder",
        evidence=evidence_list,
        operator="engine",
        audit_detail={"trigger": "user_statement"},
    )

    assert mem["memory_id"] == "mem_pref_001"
    assert mem["subject"] == "user"
    assert mem["predicate"] == "prefers"
    assert mem["object"] == "python"
    assert mem["status"] == "candidate"
    assert mem["conflict_policy"] == "coexist"
    assert len(mem["evidence"]) == 1
    assert mem["evidence"][0]["message_id"] == "msg_101"
    assert mem["evidence"][0]["evidence_strength"] == 0.75

    # 检查 point_id 是否为标准的 UUIDv5 形式
    expected_point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, "mem_pref_001"))
    assert mem["qdrant_point_id"] == expected_point_id

    # 检查 Outbox 队列
    tasks = temp_v2_store.fetch_pending_sync_tasks(limit=10)
    assert len(tasks) == 1
    task = tasks[0]
    assert task["memory_id"] == "mem_pref_001"
    assert task["qdrant_point_id"] == expected_point_id
    assert task["op_type"] == "upsert"
    assert task["status"] == "pending"
    payload = json.loads(task["payload_snapshot"])
    assert payload["subject"] == "user"
    assert payload["type"] == "preference"

    # 检查审计日志
    audit_logs = temp_v2_store.get_audit_logs("mem_pref_001")
    assert len(audit_logs) == 1
    assert audit_logs[0]["action"] == "create"
    assert audit_logs[0]["operator"] == "engine"


def test_foreign_key_and_check_constraints(temp_v2_store):
    """验证外键约束和 CHECK 约束的有效性"""
    # 1. 尝试向不存在的 session 插入 raw_messages 应该触发 FOREIGN KEY 约束失败
    with pytest.raises(sqlite3.IntegrityError):
        with temp_v2_store.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO raw_messages (
                    message_id, session_id, role, content, content_hash, sequence, source_type, is_synthetic, created_at
                )
                VALUES ('msg_orphan', 'non_existing_session', 'user', 'hi', 'hash1', 1, 'user_message', 0, 1000)
                """
            )

    # 2. 检查 CHECK 约束：非法 role in raw_messages
    temp_v2_store.record_raw_session("sess_for_check", "agent")
    with pytest.raises(sqlite3.IntegrityError):
        with temp_v2_store.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO raw_messages (
                    message_id, session_id, role, content, content_hash, sequence, source_type, is_synthetic, created_at
                )
                VALUES ('msg_bad_role', 'sess_for_check', 'invalid_role', 'hi', 'hash1', 1, 'user_message', 0, 1000)
                """
            )

    # 3. 检查 CHECK 约束：非法 source_type
    with pytest.raises(sqlite3.IntegrityError):
        temp_v2_store.record_raw_session("sess_valid", "agent")
        with temp_v2_store.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO raw_messages (
                    message_id, session_id, role, content, content_hash, sequence, source_type, is_synthetic, created_at
                )
                VALUES ('msg_bad_type', 'sess_valid', 'user', 'hi', 'hash1', 1, 'invalid_source_type', 0, 1000)
                """
            )


def test_update_memory_status_and_outbox(temp_v2_store):
    """验证 update_memory_status 更新状态、变更指向，并写入 Outbox 队列和审计日志"""
    temp_v2_store.create_memory(
        memory_id="mem_state_001",
        subject="service",
        predicate="port",
        object="8080",
        content="Service port is 8080",
        type="fact",
        status="candidate",
    )

    # 消耗掉初始 upsert 任务
    tasks = temp_v2_store.fetch_pending_sync_tasks()
    for t in tasks:
        temp_v2_store.mark_sync_task_done(t["id"])

    # 1. 晋升为 active
    updated = temp_v2_store.update_memory_status(
        memory_id="mem_state_001",
        status="active",
        operator="council",
        detail={"reason": "confidence threshold reached"},
    )
    assert updated["status"] == "active"

    tasks = temp_v2_store.fetch_pending_sync_tasks()
    assert len(tasks) == 1
    assert tasks[0]["op_type"] == "update_payload"
    assert tasks[0]["status"] == "pending"
    temp_v2_store.mark_sync_task_done(tasks[0]["id"])

    # 2. 软删除/标记删除为 deleted
    deleted = temp_v2_store.update_memory_status(
        memory_id="mem_state_001",
        status="deleted",
        deleted_by="admin_user",
        deletion_reason="superseded by dynamic discovery",
    )
    assert deleted["status"] == "deleted"
    assert deleted["deleted_by"] == "admin_user"
    assert deleted["deletion_reason"] == "superseded by dynamic discovery"
    assert deleted["deleted_at"] is not None

    tasks = temp_v2_store.fetch_pending_sync_tasks()
    assert len(tasks) == 1
    assert tasks[0]["op_type"] == "delete"

    # 检查审计流
    audits = temp_v2_store.get_audit_logs("mem_state_001")
    actions = [a["action"] for a in audits]
    assert "create" in actions
    assert "promote" in actions
    assert "delete" in actions


def test_outbox_queue_retry_and_ordering(temp_v2_store):
    """验证 Outbox 发件箱任务重试失败记录、重试次数上限过滤与保序性"""
    temp_v2_store.create_memory(
        memory_id="mem_seq_001",
        subject="s",
        predicate="p",
        content="c1",
        type="fact",
    )
    temp_v2_store.update_memory_status("mem_seq_001", "active")
    temp_v2_store.update_memory_status("mem_seq_001", "archived")

    tasks = temp_v2_store.fetch_pending_sync_tasks()
    assert len(tasks) == 3
    # 验证按自增 id 严格保序
    assert tasks[0]["op_type"] == "upsert"
    assert tasks[1]["op_type"] == "update_payload"
    assert tasks[2]["op_type"] == "update_payload"

    # 测试任务重试标记
    t0_id = tasks[0]["id"]
    temp_v2_store.mark_sync_task_failed(t0_id, "Qdrant connection timeout")
    
    # 再次拉取，应该包含该 failed 任务
    retry_tasks = temp_v2_store.fetch_pending_sync_tasks()
    t0_retried = [t for t in retry_tasks if t["id"] == t0_id][0]
    assert t0_retried["status"] == "failed"
    assert t0_retried["retry_count"] == 1
    assert t0_retried["last_error"] == "Qdrant connection timeout"

    # 连续失败直到达到重试上限 5 次
    for _ in range(4):
        temp_v2_store.mark_sync_task_failed(t0_id, "Qdrant timeout again")

    # 此时 retry_count == 5，应当不再被 fetch_pending_sync_tasks 拉取 (转入死信状态)
    active_tasks = temp_v2_store.fetch_pending_sync_tasks()
    assert t0_id not in [t["id"] for t in active_tasks]


def test_migration_from_legacy_db():
    """验证从缺少 v2 列的旧版数据库平滑增量迁移"""
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "legacy.db")

    # 手动建立一个不带 v2 新列的旧版表结构
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE raw_sessions (
            session_id TEXT PRIMARY KEY,
            agent_id TEXT NOT NULL,
            started_at INTEGER NOT NULL
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE raw_messages (
            message_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            created_at INTEGER NOT NULL
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE memories (
            memory_id TEXT PRIMARY KEY,
            qdrant_point_id TEXT NOT NULL UNIQUE,
            type TEXT NOT NULL,
            subject TEXT NOT NULL,
            predicate TEXT NOT NULL,
            content TEXT NOT NULL,
            valid_from INTEGER NOT NULL,
            confidence REAL NOT NULL DEFAULT 0.8,
            importance REAL NOT NULL DEFAULT 0.5,
            mention_count INTEGER NOT NULL DEFAULT 1,
            status TEXT NOT NULL DEFAULT 'candidate',
            project_id TEXT NOT NULL DEFAULT 'general',
            scope TEXT NOT NULL DEFAULT 'global',
            source_agent TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        """
    )
    conn.commit()
    conn.close()

    # 使用 SessionStore 挂载该旧库，触发增量迁移
    store = SessionStore(db_path=db_path)
    
    # 验证新列是否自动迁移补充
    with store.get_connection() as c:
        cur = c.cursor()
        cur.execute("PRAGMA table_info(raw_messages);")
        msg_cols = {row["name"] for row in cur.fetchall()}
        assert "source_type" in msg_cols
        assert "is_synthetic" in msg_cols

        cur.execute("PRAGMA table_info(memories);")
        mem_cols = {row["name"] for row in cur.fetchall()}
        assert "conflict_policy" in mem_cols
        assert "validity_type" in mem_cols
        assert "version" in mem_cols
        assert "deleted_at" in mem_cols

    store.close()
    shutil.rmtree(temp_dir, ignore_errors=True)


def test_concurrent_writes_and_busy_timeout(temp_v2_store):
    """验证多线程并发写入时的线程安全与 busy_timeout / WAL 机制"""
    num_threads = 8
    items_per_thread = 5

    def worker(worker_id: int):
        for i in range(items_per_thread):
            mem_id = f"mem_thread_{worker_id}_{i}"
            temp_v2_store.create_memory(
                memory_id=mem_id,
                subject=f"subject_{worker_id}",
                predicate="related_to",
                content=f"Content from thread {worker_id} item {i}",
                type="fact",
            )
            time.sleep(0.01)

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker, tid) for tid in range(num_threads)]
        for f in concurrent.futures.as_completed(futures):
            f.result()

    # 验证总共写入量
    with temp_v2_store.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM memories WHERE memory_id LIKE 'mem_thread_%';")
        count = cursor.fetchone()[0]
        assert count == num_threads * items_per_thread
