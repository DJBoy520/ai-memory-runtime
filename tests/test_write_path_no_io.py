"""测试写路径零 I/O 契约：
- fake engine.embed 与 fake qdrant 任何方法调用即 AssertionError；memory_create/update/delete 全流程仍成功返回 pending；
- 断言 memories.projection_revision 递增值（create=1, update +1, delete +1）；
- 断言 memory_projection_outbox 有 pending 行且 target_collection 正确；
- legacy 行 memory_update(content) 抛 LegacyPathForbidden；legacy delete 生成旧队列 amr_delta_v1 op=delete（不写新队列）。
"""
import json
import pytest

from src.core.errors import LegacyPathForbidden
from src.core.session_store import SessionStore
from src.service.memory_service import MemoryService


class ExplodingEngine:
    """任何方法调用即炸的 Fake Engine"""
    async def embed(self, *args, **kwargs):
        raise AssertionError("engine.embed called on write path!")

    def __getattr__(self, name):
        def _boom(*args, **kwargs):
            raise AssertionError(f"engine.{name} called on write path!")
        return _boom


class ExplodingQdrant:
    """任何方法调用即炸的 Fake Qdrant Manager"""
    def __getattr__(self, name):
        def _boom(*args, **kwargs):
            raise AssertionError(f"qdrant.{name} called on write path!")
        return _boom


@pytest.fixture
def store(tmp_path):
    db_file = str(tmp_path / "test_write_no_io.db")
    st = SessionStore(db_path=db_file)
    yield st
    st.close()


@pytest.fixture
def service(store):
    return MemoryService(
        engine=ExplodingEngine(),
        qdrant_manager=ExplodingQdrant(),
        session_store=store,
    )


@pytest.mark.asyncio
async def test_v3_write_path_zero_io_and_revisions(service, store):
    """验证 v3 记忆 create/update/delete 路径零 embed、零 Qdrant I/O，revision 严格递增"""
    # 1. memory_create (revision=1)
    create_res = await service.memory_create(
        content="Substantive v3 knowledge content text",
        project_id="aep-core",
        type="decision/arch",
        collection_name="custom_vault",
    )
    assert create_res["projection_status"] == "pending"
    assert create_res["projection_revision"] == 1
    mid = create_res["memory_id"]

    # 验证 SQLite 主表与新队列 outbox
    with store._lock:
        conn = store._get_connection()
        cur = conn.cursor()
        cur.execute("SELECT projection_revision, projection_class, collection_name FROM memories WHERE memory_id = ?;", (mid,))
        row = cur.fetchone()
        assert row is not None
        assert row["projection_revision"] == 1
        assert row["projection_class"] == "v3"
        assert row["collection_name"] == "custom_vault"

        cur.execute("SELECT status, projection_revision, target_collection FROM memory_projection_outbox WHERE memory_id = ?;", (mid,))
        outbox_row = cur.fetchone()
        assert outbox_row is not None
        assert outbox_row["status"] == "pending"
        assert outbox_row["projection_revision"] == 1
        assert outbox_row["target_collection"] == "custom_vault"

    # 2. memory_update 内容变更 (revision=2)
    up_res = await service.memory_update(
        memory_id=mid,
        content="Revised substantive v3 knowledge content text",
        expected_version=1,
        change_reason="Add clarity to architecture",
    )
    assert up_res["projection_status"] == "pending"
    assert up_res["projection_revision"] == 2
    assert up_res["version"] == 2

    with store._lock:
        conn = store._get_connection()
        cur = conn.cursor()
        cur.execute("SELECT projection_revision, version FROM memories WHERE memory_id = ?;", (mid,))
        row = cur.fetchone()
        assert row["projection_revision"] == 2
        assert row["version"] == 2

        cur.execute("SELECT status, projection_revision FROM memory_projection_outbox WHERE memory_id = ?;", (mid,))
        outbox_row = cur.fetchone()
        assert outbox_row["status"] == "pending"
        assert outbox_row["projection_revision"] == 2

    # 3. memory_delete 软删除 (revision=3)
    del_res = await service.memory_delete(
        memory_id=mid,
        reason="Retire obsolete architecture rule",
    )
    assert del_res["projection_status"] == "pending"

    with store._lock:
        conn = store._get_connection()
        cur = conn.cursor()
        cur.execute("SELECT status, projection_revision FROM memories WHERE memory_id = ?;", (mid,))
        row = cur.fetchone()
        assert row["status"] == "DELETED"
        assert row["projection_revision"] == 3

        cur.execute("SELECT status, projection_revision FROM memory_projection_outbox WHERE memory_id = ?;", (mid,))
        outbox_row = cur.fetchone()
        assert outbox_row["status"] == "pending"
        assert outbox_row["projection_revision"] == 3


@pytest.mark.asyncio
async def test_legacy_write_path_guard_and_delta_outbox(service, store):
    """验证 legacy 记忆：正文修改抛 LegacyPathForbidden；删除走受控路径写旧队列 amr_delta_v1 不写新队列"""
    leg_id = "mem_leg_12345"
    store.create_memory(
        memory_id=leg_id,
        subject="",
        predicate="",
        content="Original legacy memory content",
        type="fact",
        collection_name="legacy_col",
    )

    # 1. 尝试修改 legacy 记忆内容 -> 抛出 LegacyPathForbidden
    with pytest.raises(LegacyPathForbidden) as excinfo:
        await service.memory_update(
            memory_id=leg_id,
            content="Attempted update to legacy content",
            expected_version=1,
            change_reason="Not allowed",
        )
    assert excinfo.value.memory_id == leg_id

    # 2. 删除 legacy 记忆 -> 成功返回 pending，并写入旧队列 delta 信封
    del_res = await service.memory_delete(
        memory_id=leg_id,
        reason="Legacy deprecation",
    )
    assert del_res["projection_status"] == "pending"

    with store._lock:
        conn = store._get_connection()
        cur = conn.cursor()

        # 断言未写入新队列
        cur.execute("SELECT * FROM memory_projection_outbox WHERE memory_id = ?;", (leg_id,))
        assert cur.fetchone() is None

        # 断言写入旧队列 qdrant_sync_queue，携带 amr_delta_v1 op='delete'
        cur.execute("SELECT op_type, payload_snapshot FROM qdrant_sync_queue WHERE memory_id = ? ORDER BY id DESC;", (leg_id,))
        queue_row = cur.fetchone()
        assert queue_row is not None
        assert queue_row["op_type"] == "delete"

        snap = json.loads(queue_row["payload_snapshot"])
        assert snap["amr_delta_v1"] is True
        assert snap["op"] == "delete"
        assert snap["collection"] == "legacy_col"
        assert snap["fields"]["status"] == "DELETED"
