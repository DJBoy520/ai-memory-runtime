"""测试受控 legacy 状态变更路径与 delta 任务入队/取消策略 (RFC v5.2 §4 + RFC v5.3 §1)。"""
import json
import sqlite3
import pytest

from src.core.session_store import SessionStore


@pytest.fixture
def store(tmp_path):
    db_file = str(tmp_path / "test_legacy_controlled.db")
    st = SessionStore(db_path=db_file)
    yield st
    st.close()


def test_apply_status_change_envelope_patch_and_delete(store):
    """验证 apply_status_change 生成 amr_delta_v1 信封，patch 产生 update_payload，DELETED 产生 delete"""
    # 建立一条 legacy 记忆
    mem = store.create_memory(
        memory_id="mem_leg_env",
        subject="sub",
        predicate="pred",
        content="legacy text",
        type="fact",
        status="candidate",
    )
    with store.locked_write_transaction() as conn:
        cur = conn.cursor()
        rows = store.resolve_memory_family(cur, "mem_leg_env")
        # 1. 状态变更为 ACTIVE (patch)
        res_patch = store.apply_status_change(cur, rows, new_status="ACTIVE")
        assert res_patch["new_status"] == "ACTIVE"

    tasks = store.fetch_pending_sync_tasks()
    # 包含了 create_memory 的 upsert 和本次 patch
    patch_tasks = [t for t in tasks if t["op_type"] == "update_payload"]
    assert len(patch_tasks) == 1
    snap = json.loads(patch_tasks[0]["payload_snapshot"])
    assert snap["amr_delta_v1"] is True
    assert snap["op"] == "patch"
    assert snap["fields"]["status"] == "ACTIVE"
    assert snap["collection"] == "ai_memory"
    assert len(snap["targets"]) == 1

    # 2. 状态变更为 DELETED (delete)
    with store.locked_write_transaction() as conn:
        cur = conn.cursor()
        rows = store.resolve_memory_family(cur, "mem_leg_env")
        res_del = store.apply_status_change(cur, rows, new_status="DELETED", reason="test delete")
        assert res_del["new_status"] == "DELETED"

    del_tasks = [t for t in store.fetch_pending_sync_tasks() if t["op_type"] == "delete"]
    assert len(del_tasks) == 1
    del_snap = json.loads(del_tasks[0]["payload_snapshot"])
    assert del_snap["amr_delta_v1"] is True
    assert del_snap["op"] == "delete"
    assert del_snap["fields"]["status"] == "DELETED"
    assert del_snap["fields"]["deletion_reason"] == "test delete"


def test_older_tasks_cancellation_policy(store):
    """验证 v5.3 §1 取消策略：老 upsert 原地 patch、老 update_payload 标 synced、老 delete 保留"""
    mem = store.create_memory(
        memory_id="mem_cancel_test",
        subject="sub",
        predicate="pred",
        content="preserve vector content",
        type="fact",
        status="candidate",
    )
    conn = store.get_connection()
    cur = conn.cursor()

    # 手工在 qdrant_sync_queue 插入一条老 update_payload 和一条老 delete
    cur.execute("""
        INSERT INTO qdrant_sync_queue (
            memory_id, qdrant_point_id, op_type, payload_snapshot, status, retry_count, last_error, created_at, updated_at
        ) VALUES ('mem_cancel_test', 'pt_1', 'update_payload', '{"old":1}', 'pending', 0, NULL, 100, 100);
    """)
    old_update_id = cur.lastrowid

    cur.execute("""
        INSERT INTO qdrant_sync_queue (
            memory_id, qdrant_point_id, op_type, payload_snapshot, status, retry_count, last_error, created_at, updated_at
        ) VALUES ('mem_cancel_test', 'pt_1', 'delete', '{"del":1}', 'pending', 0, NULL, 101, 101);
    """)
    old_delete_id = cur.lastrowid
    conn.commit()

    # 创建目标记忆以满足 FK 约束
    store.create_memory(
        memory_id="mem_new_target",
        subject="new",
        predicate="new",
        content="new target memory content",
        type="fact",
    )

    # 执行状态变更为 HISTORICAL (superseded)
    store.change_memory_status("mem_cancel_test", "HISTORICAL", superseded_by="mem_new_target")

    # 验证处理结果：
    # 1. 初始 upsert (id=1): 应该保持 pending，payload_snapshot 就地应用了 status='HISTORICAL'
    cur.execute("SELECT status, payload_snapshot, last_error FROM qdrant_sync_queue WHERE id = 1;")
    upsert_row = cur.fetchone()
    assert upsert_row["status"] == "pending"
    upsert_snap = json.loads(upsert_row["payload_snapshot"])
    assert upsert_snap["status"] == "HISTORICAL"
    assert upsert_snap["superseded_by"] == "mem_new_target"
    assert "content" in upsert_snap  # 正文未丢失
    assert "patched by #" in upsert_row["last_error"]

    # 2. 老 update_payload (old_update_id): 标为 synced，注记 superseded
    cur.execute("SELECT status, last_error FROM qdrant_sync_queue WHERE id = ?;", (old_update_id,))
    update_row = cur.fetchone()
    assert update_row["status"] == "synced"
    assert "superseded by #" in update_row["last_error"]

    # 3. 老 delete (old_delete_id): 保留不动 (status='pending')
    cur.execute("SELECT status FROM qdrant_sync_queue WHERE id = ?;", (old_delete_id,))
    delete_row = cur.fetchone()
    assert delete_row["status"] == "pending"


def test_resolve_shard_family_with_root_and_parent_only(store):
    """验证分片族解析：通过 root_memory_id 聚合并支持 parent-only id 命中"""
    conn = store.get_connection()
    cur = conn.cursor()

    # 模拟多分片 legacy 记录：parent id 本身在 memories 无行，有 shard_0 与 shard_1
    cur.execute("""
        INSERT INTO memories (
            memory_id, qdrant_point_id, type, subject, predicate, content,
            status, root_memory_id, projection_class, created_at, updated_at
        ) VALUES
        ('parent_chunk_0', 'pt_c0', 'fact', 'sub', 'pred', 'chunk 0 text', 'active', 'parent_doc', 'legacy', 100, 100),
        ('parent_chunk_1', 'pt_c1', 'fact', 'sub', 'pred', 'chunk 1 text', 'active', 'parent_doc', 'legacy', 101, 101);
    """)
    conn.commit()

    # 1. 用 parent id 解析 -> 命中全族 2 个分片
    family1 = store.resolve_memory_family(cur, "parent_doc")
    assert len(family1) == 2
    ids1 = [r["memory_id"] for r in family1]
    assert ids1 == ["parent_chunk_0", "parent_chunk_1"]

    # 2. 用具体分片 id 解析 -> 同样命中全族
    family2 = store.resolve_memory_family(cur, "parent_chunk_0")
    assert len(family2) == 2

    # 3. 对 parent id 执行 change_memory_status
    res = store.change_memory_status("parent_doc", "DELETED", reason="GDPR purge")
    assert res["current_status"] == "DELETED"
    assert len(res["targets"]) == 2

    # 验证两个分片的状态都变成了 DELETED
    cur.execute("SELECT memory_id, status, deletion_reason FROM memories WHERE root_memory_id = 'parent_doc';")
    shards = cur.fetchall()
    for s in shards:
        assert s["status"] == "DELETED"
        assert s["deletion_reason"] == "GDPR purge"


def test_change_memory_status_v3_path(store):
    """验证 v3 记忆走统一投影队列，projection_revision 递增，不写 qdrant_sync_queue"""
    mem = store.create_memory_v3(
        memory_id="mem_v3_status",
        content="v3 status flow content",
        status="ACTIVE",
    )
    # create 时 revision=1，outbox 任务 revision=1
    outbox1 = store.fetch_pending_projection_tasks()
    assert len(outbox1) == 1
    assert outbox1[0]["projection_revision"] == 1

    # 变更状态为 PENDING_VERIFY
    res = store.change_memory_status("mem_v3_status", "PENDING_VERIFY")
    assert res["current_status"] == "PENDING_VERIFY"
    assert res["projection_revision"] == 2

    # 验证 memories 主表与 outbox 队列
    outbox2 = store.fetch_pending_projection_tasks()
    assert len(outbox2) == 1
    assert outbox2[0]["projection_revision"] == 2

    # 验证旧队列 qdrant_sync_queue 零写入
    old_tasks = store.fetch_pending_sync_tasks()
    assert len([t for t in old_tasks if t["memory_id"] == "mem_v3_status"]) == 0


def test_mixed_projection_class_forbidden(store):
    """验证同一记忆族内若出现混合 projection_class 则抛出 INVALID_ARGUMENT ValueError"""
    conn = store.get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO memories (
            memory_id, qdrant_point_id, type, subject, predicate, content,
            status, root_memory_id, projection_class, created_at, updated_at
        ) VALUES
        ('mix_chunk_0', 'pt_m0', 'fact', 'sub', 'pred', 'c0', 'active', 'mix_root', 'legacy', 100, 100),
        ('mix_chunk_1', 'pt_m1', 'fact', 'sub', 'pred', 'c1', 'ACTIVE', 'mix_root', 'v3', 101, 101);
    """)
    conn.commit()

    with pytest.raises(ValueError, match="INVALID_ARGUMENT"):
        store.change_memory_status("mix_root", "DELETED")


def test_change_memory_status_extra_fields(store):
    """验证 change_memory_status 支持 extra_fields 写入 memories（如 cognitive merge 的 confidence/mention_count）"""
    mem = store.create_memory(
        memory_id="mem_extra_fields",
        subject="s",
        predicate="p",
        content="c",
        type="fact",
        confidence=0.5,
        mention_count=1,
    )
    store.change_memory_status(
        "mem_extra_fields",
        "ACTIVE",
        extra_fields={"confidence": 0.95, "mention_count": 5},
    )
    updated = store.get_memory("mem_extra_fields")
    assert updated["confidence"] == 0.95
    assert updated["mention_count"] == 5
    assert updated["status"] == "ACTIVE"

    # 验证 v3 记忆也支持 extra_fields
    store.create_memory_v3("mem_v3_extra", content="v3 extra test")
    store.change_memory_status(
        "mem_v3_extra",
        "ACTIVE",
        extra_fields={"confidence": 0.99, "mention_count": 3},
    )
    v3_up = store.get_memory_v3("mem_v3_extra")
    assert v3_up["confidence"] == 0.99
    assert v3_up["mention_count"] == 3
