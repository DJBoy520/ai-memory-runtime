"""
Red-Team Audit Track R2: State Machine, Memory Family & Legacy Mutation Guard Tests
验证 AMR v5 的状态机与记忆族语义：
1. v3 DELETED 终态不可逆（禁止转回 ACTIVE 等状态）
2. [缺陷验证] legacy 行在 DELETED 终态下被成功逆转回 ACTIVE（契约漏洞）
3. HISTORICAL 与 superseded_by 校验：合法流转、目标不存在、自指环路防护
4. [缺陷验证] update_memory_v3 遗漏 _validate_superseded_target 校验，允许自指 (superseded_by=self)
5. 记忆族解析语义：分片族 (parent 缺省) 全族归组 vs 版本链 (root 实存) 独立演进
6. Legacy 变更保护守卫：正文与类型变更拒绝抛出 LegacyPathForbidden，删除转入 delta delete
"""

import json
import pytest

from src.core.errors import LegacyPathForbidden
from src.core.session_store import SessionStore
from src.service.memory_service import MemoryService


@pytest.fixture
def store(tmp_path):
    db_file = tmp_path / "audit_state.db"
    st = SessionStore(db_path=db_file)
    yield st
    st.close()


def test_v3_deleted_status_terminal_and_irreversible(store):
    """
    攻击面 5.1: v3 记忆 DELETED 终态严格不可逆
    无论是通过 change_memory_status 还是 update_memory_v3，均必须拦截并抛出 ValueError。
    """
    mem_id = "mem_v3_term_del"
    store.create_memory_v3(
        memory_id=mem_id,
        content="Permanent deletion content v3",
    )
    # 删除
    store.change_memory_status(mem_id, "DELETED", reason="Terminal test")
    m = store.get_memory_v3(mem_id)
    assert m["status"] == "DELETED"

    # 1. 尝试通过 change_memory_status 逆转回 ACTIVE
    with pytest.raises(ValueError) as exc1:
        store.change_memory_status(mem_id, "ACTIVE")
    assert "DELETED state is terminal and cannot be reverted" in str(exc1.value)

    # 2. 尝试通过 update_memory_v3 逆转回 ACTIVE
    with pytest.raises(ValueError) as exc2:
        store.update_memory_v3(mem_id, status="ACTIVE")
    assert "DELETED state is terminal and cannot be reverted" in str(exc2.value)

    # 3. 尝试逆转为 HISTORICAL
    with pytest.raises(ValueError) as exc3:
        store.change_memory_status(mem_id, "HISTORICAL")
    assert "DELETED state is terminal and cannot be reverted" in str(exc3.value)


def test_legacy_deleted_status_reversion_defect(store):
    """
    攻击面 5.2: legacy 记忆 DELETED 终态防护
    apply_status_change 校验 curr_status == 'DELETED'，
    已被软删除的 legacy 记忆严禁通过 change_memory_status 被逆转复活成 ACTIVE。
    """
    mem_id = "mem_leg_del_defect"
    store.create_memory(
        memory_id=mem_id,
        subject="sub",
        predicate="pred",
        content="Legacy memory destined to delete",
        type="fact",
        status="active",
    )
    # 软删除
    store.change_memory_status(mem_id, "DELETED", reason="Soft delete")
    m_del = store.get_memory(mem_id)
    assert m_del["status"] == "DELETED"

    # 尝试非法复活：断言抛出 ValueError 拦截对 DELETED 终态的逆转
    with pytest.raises(ValueError) as exc:
        store.change_memory_status(mem_id, "ACTIVE")
    assert "DELETED state is terminal and cannot be reverted" in str(exc.value)

    m_still_del = store.get_memory(mem_id)
    assert m_still_del["status"] == "DELETED"


def test_historical_superseded_by_validation_in_change_status(store):
    """
    攻击面 5.3: change_memory_status 中对 HISTORICAL 与 superseded_by 的校验
    1. 目标不存在 -> ValueError: superseded_by '...' not found
    2. 自指 (superseded_by == self) -> ValueError: superseded_by must not reference the same memory
    3. 目标存在 -> 成功持久化 superseded_by 与 superseded_at，并写入审计日志
    """
    mem_id = "mem_sup_val_001"
    store.create_memory_v3(
        memory_id=mem_id,
        content="Content to be superseded",
    )

    # 1. 目标不存在
    with pytest.raises(ValueError) as exc_none:
        store.change_memory_status(mem_id, "HISTORICAL", superseded_by="mem_ghost_target")
    assert "superseded_by 'mem_ghost_target' not found" in str(exc_none.value)

    # 2. 自指
    with pytest.raises(ValueError) as exc_self:
        store.change_memory_status(mem_id, "HISTORICAL", superseded_by=mem_id)
    assert "must not reference the same memory or its shard family" in str(exc_self.value)

    # 3. 合法替换
    target_id = "mem_target_successor"
    store.create_memory_v3(
        memory_id=target_id,
        content="Successor valid content",
    )
    res = store.change_memory_status(mem_id, "HISTORICAL", superseded_by=target_id)
    assert res["current_status"] == "HISTORICAL"

    m = store.get_memory_v3(mem_id)
    assert m["status"] == "HISTORICAL"
    assert m["superseded_by"] == target_id
    assert m["superseded_at"] is not None


def test_update_memory_v3_self_referencing_superseded_by_defect(store):
    """
    攻击面 5.4: update_memory_v3 调用 _validate_superseded_target 校验
    拦截调用方通过 update_memory_v3 设置 superseded_by=memory_id（自指环路）或不存在的目标。
    """
    mem_id = "mem_self_loop_defect"
    store.create_memory_v3(
        memory_id=mem_id,
        content="Content tested for update_memory_v3 self-reference defect",
    )

    # 通过 update_memory_v3 传入自身 ID 作为 superseded_by，断言抛出 ValueError 拦截自指
    with pytest.raises(ValueError) as exc_self:
        store.update_memory_v3(
            memory_id=mem_id,
            status="HISTORICAL",
            superseded_by=mem_id,
        )
    assert "must not reference the same memory or its shard family" in str(exc_self.value)

    # 验证目标不存在也会被拦截
    with pytest.raises(ValueError) as exc_ghost:
        store.update_memory_v3(
            memory_id=mem_id,
            status="HISTORICAL",
            superseded_by="mem_ghost_nonexistent",
        )
    assert "not found" in str(exc_ghost.value)


def test_resolve_memory_family_shards_vs_versions(store):
    """
    攻击面 5.5: 记忆族归组区分分片族 (shard family) 与版本链 (version chain)
    1. 分片族：由 memory_record 切片产生，parent_memory_id 行在 memories 表中不存在，
       因此按 root 查出全部分片；
    2. 版本链：v1 实体在 memories 中真实存在，v2 记录 root_memory_id=v1，
       此时 root 存在，resolve_memory_family 绝不跨版本把 v1 归入 v2。
    """
    conn = store.get_connection()
    cur = conn.cursor()

    # 1. 模拟多分片族（parent 行缺省，chunks 行存在）
    parent_id = "mem_shard_parent_999"
    chunk_0 = f"{parent_id}_chunk_0"
    chunk_1 = f"{parent_id}_chunk_1"

    store.create_memory(
        memory_id=chunk_0,
        subject="", predicate="", content="Chunk 0", type="fact",
        root_memory_id=parent_id,
    )
    store.create_memory(
        memory_id=chunk_1,
        subject="", predicate="", content="Chunk 1", type="fact",
        root_memory_id=parent_id,
    )

    # 传入 parent_id 查询族 -> 包含全部 2 个分片
    f_from_parent = store.resolve_memory_family(cur, parent_id)
    assert len(f_from_parent) == 2
    assert {r["memory_id"] for r in f_from_parent} == {chunk_0, chunk_1}

    # 传入任意 chunk_id 查询族 -> 包含全部 2 个分片
    f_from_chunk = store.resolve_memory_family(cur, chunk_0)
    assert len(f_from_chunk) == 2
    assert {r["memory_id"] for r in f_from_chunk} == {chunk_0, chunk_1}

    # 2. 模拟版本链（v1 与 v2 均为 memories 表真实行）
    v1_id = "mem_ver_root_v1"
    v2_id = "mem_ver_child_v2"
    store.create_memory(
        memory_id=v1_id,
        subject="", predicate="", content="Version 1", type="fact",
        root_memory_id=v1_id,
    )
    store.create_memory(
        memory_id=v2_id,
        subject="", predicate="", content="Version 2", type="fact",
        root_memory_id=v1_id,
    )

    # 查询 v2 族 -> 仅包含 [v2]，不能误伤或联立 v1
    f_v2 = store.resolve_memory_family(cur, v2_id)
    assert len(f_v2) == 1
    assert f_v2[0]["memory_id"] == v2_id


@pytest.mark.asyncio
async def test_legacy_mutation_guards_in_service(store):
    """
    攻击面 5.6: MemoryService 中对 legacy 记忆的变更防护与删除 delta 映射
    - legacy content 修改 -> LegacyPathForbidden
    - legacy type 修改 -> LegacyPathForbidden
    - legacy delete -> 走 delta delete 逻辑
    """
    mem_id = "mem_legacy_guard_001"
    store.create_memory(
        memory_id=mem_id,
        subject="", predicate="", content="Legacy protected content", type="fact",
    )

    service = MemoryService(session_store=store)

    # 1. 修改正文必须被拒绝
    with pytest.raises(LegacyPathForbidden) as exc_content:
        await service.memory_update(memory_id=mem_id, content="Illegal content modification")
    assert exc_content.value.memory_id == mem_id
    assert "legacy 记忆不支持正文变更" in str(exc_content.value.hint)

    # 2. 修改类型必须被拒绝
    with pytest.raises(LegacyPathForbidden) as exc_type:
        await service.memory_update(memory_id=mem_id, type="new_type")
    assert exc_type.value.memory_id == mem_id
    assert "legacy 记忆不支持类型变更" in str(exc_type.value.hint)

    # 3. 删除操作必须成功流转为 DELETED 且入队 delta 任务
    del_res = await service.memory_delete(memory_id=mem_id, reason="Audit delete legacy")
    assert del_res["projection_status"] == "pending"

    m = store.get_memory(mem_id)
    assert m["status"] == "DELETED"
    assert m["deletion_reason"] == "Audit delete legacy"

    tasks = store.fetch_pending_sync_tasks()
    del_tasks = [t for t in tasks if t["op_type"] == "delete"]
    assert len(del_tasks) == 1
    snap = json.loads(del_tasks[0]["payload_snapshot"])
    assert snap["amr_delta_v1"] is True
    assert snap["op"] == "delete"
