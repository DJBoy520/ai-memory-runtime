import asyncio
import os
import pytest
import uuid
import time
from src.core.session_store import SessionStore
from src.interfaces.mcp.bridge import MCPBridge

@pytest.fixture
def store(tmp_path):
    db_file = str(tmp_path / "test_sessions.db")
    st = SessionStore(db_path=db_file)
    yield st
    st.close()

def test_v3_create_and_revisions(store):
    mem_id = f"mem_test_{uuid.uuid4().hex[:6]}"
    res = store.create_memory_v3(
        memory_id=mem_id,
        content="AEP-Chain 强制采用 SM2 128-hex 双层签名架构",
        project_id="aep-chain",
        type="decision/arch",
        status="ACTIVE",
        created_by_agent="hermes",
    )
    assert res is not None
    assert res["memory_id"] == mem_id
    assert res["version"] == 1
    assert res["status"] == "ACTIVE"
    assert res["type"] == "decision/arch"
    assert res["created_by_agent"] == "hermes"
    assert len(res["revisions"]) == 1
    assert res["revisions"][0]["version"] == 1
    assert res["revisions"][0]["content"] == "AEP-Chain 强制采用 SM2 128-hex 双层签名架构"

def test_v3_optimistic_lock_and_version_increment(store):
    mem_id = f"mem_test_{uuid.uuid4().hex[:6]}"
    store.create_memory_v3(
        memory_id=mem_id,
        content="最初考虑采用 PostgreSQL 存储事实",
        project_id="aep-core",
        type="decision/db",
        status="ACTIVE",
        created_by_agent="hermes",
    )

    # 1. 尝试用错误的版本号修改 -> 应抛出 VERSION_CONFLICT
    with pytest.raises(ValueError, match="VERSION_CONFLICT"):
        store.update_memory_v3(
            memory_id=mem_id,
            content="改为采用 SQLite 作为 SSOT 事实源",
            expected_version=99,
            change_reason="PostgreSQL 部署过重",
            agent_id="opencode",
        )

    # 2. 尝试无 change_reason 修改内容 -> 报错拒绝
    with pytest.raises(ValueError, match="change_reason"):
        store.update_memory_v3(
            memory_id=mem_id,
            content="改为采用 SQLite 作为 SSOT 事实源",
            expected_version=1,
            change_reason="",
            agent_id="opencode",
        )

    # 3. 正常修改内容 -> 版本自增至 2
    up_res = store.update_memory_v3(
        memory_id=mem_id,
        content="改为采用 SQLite 作为 SSOT 事实源",
        expected_version=1,
        change_reason="PostgreSQL 部署过重，SQLite 更轻量自闭环",
        agent_id="opencode",
    )
    assert up_res["version"] == 2
    assert up_res["content"] == "改为采用 SQLite 作为 SSOT 事实源"
    assert up_res["updated_by_agent"] == "opencode"
    assert len(up_res["revisions"]) == 2
    assert up_res["revisions"][1]["version"] == 2
    assert up_res["revisions"][1]["change_reason"] == "PostgreSQL 部署过重，SQLite 更轻量自闭环"

def test_v3_pure_metadata_update_no_version_bump(store):
    mem_id = f"mem_test_{uuid.uuid4().hex[:6]}"
    store.create_memory_v3(
        memory_id=mem_id,
        content="计划采用 SM2 协同签名方案",
        project_id="aep-chain",
        type="general",
        status="PENDING_VERIFY",
        created_by_agent="hermes",
    )

    # 仅更新 type 和 status
    up_res = store.update_memory_v3(
        memory_id=mem_id,
        type="decision/crypto",
        status="ACTIVE",
        agent_id="openclaw",
    )
    # 版本号不应递增，revisions 表不应增加
    assert up_res["version"] == 1
    assert up_res["status"] == "ACTIVE"
    assert up_res["type"] == "decision/crypto"
    assert len(up_res["revisions"]) == 1

def test_v3_conflict_mutual_flagging(store):
    mem_a = f"mem_a_{uuid.uuid4().hex[:6]}"
    store.create_memory_v3(
        memory_id=mem_a,
        content="方案 A: 采用同步 HTTP 轮询架构",
        project_id="aep-net",
        type="decision/network",
        status="ACTIVE",
        created_by_agent="hermes",
    )

    mem_b = f"mem_b_{uuid.uuid4().hex[:6]}"
    store.create_memory_v3(
        memory_id=mem_b,
        content="方案 B: 采用异步 WebSocket 长连接事件流",
        project_id="aep-net",
        type="decision/network",
        status="CONFLICT",
        created_by_agent="opencode",
        conflicts_with=[mem_a],
    )

    # 检查双方是否都被原子标记为 CONFLICT
    item_a = store.get_memory_v3(mem_a)
    item_b = store.get_memory_v3(mem_b)
    assert item_a["status"] == "CONFLICT"
    assert mem_b in item_a["conflicts_with"]
    assert item_b["status"] == "CONFLICT"
    assert mem_a in item_b["conflicts_with"]

def test_v3_status_transition_guard(store):
    mem_id = f"mem_del_{uuid.uuid4().hex[:6]}"
    store.create_memory_v3(
        memory_id=mem_id,
        content="一条废弃的临时草案内容",
        project_id="global",
        type="general",
        status="ACTIVE",
        created_by_agent="system",
    )

    # 软删除
    store.update_memory_v3(
        memory_id=mem_id,
        status="DELETED",
        change_reason="废弃清理",
    )

    # 严禁 DELETED -> ACTIVE 逆流
    with pytest.raises(ValueError, match="DELETED state is terminal"):
        store.update_memory_v3(
            memory_id=mem_id,
            status="ACTIVE",
            change_reason="尝试恢复",
        )
