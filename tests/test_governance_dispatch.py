"""
tests/test_governance_dispatch.py
验证 AMR 治理链路统一走状态分派（reconciliation / cognitive / context_builder / 维护脚本）
严格遵循 RFC v5 §A2(7)/§C2、V5.1 §1、V5.2 F12/F15/F16/F17、V5.3 F8/F9/§5
"""

import asyncio
import json
import sqlite3
import time
from unittest.mock import MagicMock

import pytest

from src.cognitive.context_builder import ContextBuilder
from src.core.session_store import SessionStore
from src.reconciliation.engine import ReconciliationEngine
from src.service.cognitive_engine import CognitiveEngine
from scripts.adopt_orphan_points import adopt_orphans
from scripts.normalize_status_schema import get_normalized_status, run_inventory, run_rehearse


def _create_test_store(tmp_path, db_name="test.db") -> SessionStore:
    db_file = tmp_path / db_name
    return SessionStore(db_path=str(db_file))


def test_reconciliation_phase_3_dispatch_v3_and_legacy(tmp_path):
    """
    reconciliation phase_3:
    - v3 候选 -> 新队列 pending + revision+1（不写旧队列）
    - legacy 候选 -> 旧队列 amr_delta_v1（不写新队列）
    """
    store = _create_test_store(tmp_path, "recon_dispatch.db")
    conn = store.get_connection()
    cursor = conn.cursor()
    now = int(time.time())

    # 1. 插入一条 v3 活跃记忆
    cursor.execute(
        """
        INSERT INTO memories (
            memory_id, qdrant_point_id, type, subject, predicate, content,
            valid_from, confidence, importance, mention_count, status,
            project_id, scope, source_agent, version, created_at, updated_at,
            projection_class, collection_name, projection_revision
        ) VALUES ('mem_v3_cand', 'pt_v3_cand', 'fact', 'subj_v3', 'pred', 'content v3',
                  ?, 0.8, 0.5, 1, 'ACTIVE', 'test_proj', 'global', 'agent_a', 1, ?, ?,
                  'v3', 'ai_memory', 1)
        """,
        (now, now, now),
    )

    # 2. 插入一条 legacy 活跃记忆
    cursor.execute(
        """
        INSERT INTO memories (
            memory_id, qdrant_point_id, type, subject, predicate, content,
            valid_from, confidence, importance, mention_count, status,
            project_id, scope, source_agent, version, created_at, updated_at,
            projection_class, collection_name, projection_revision
        ) VALUES ('mem_leg_cand', 'pt_leg_cand', 'fact', 'subj_leg', 'pred', 'content leg',
                  ?, 0.7, 0.4, 1, 'active', 'test_proj', 'global', 'agent_b', 1, ?, ?,
                  'legacy', 'ai_memory', 0)
        """,
        (now, now, now),
    )
    conn.commit()

    engine = ReconciliationEngine(
        dry_run=False,
        db_path=str(tmp_path / "recon_dispatch.db"),
        batch_id="batch_recon_test_1",
    )

    # 伪造 Policy Gate 审核通过的两条候选
    approved = [
        {
            "candidate_id": "cand_v3_1",
            "memory_id": "mem_v3_cand",
            "proposed_status": "deleted",
            "reason": "v3 lifecycle test",
        },
        {
            "candidate_id": "cand_leg_1",
            "memory_id": "mem_leg_cand",
            "proposed_status": "archived",
            "reason": "legacy lifecycle test",
        },
    ]

    # 插入 curation_candidates 初始记录
    cursor.execute(
        """
        INSERT INTO curation_candidates (candidate_id, batch_id, memory_id, operation, current_status, proposed_status, matched_rule_id, reason, state, created_at)
        VALUES ('cand_v3_1', 'batch_recon_test_1', 'mem_v3_cand', 'DELETE', 'ACTIVE', 'DELETED', 'RULE_1', 'v3 lifecycle test', 'APPROVED', ?),
               ('cand_leg_1', 'batch_recon_test_1', 'mem_leg_cand', 'ARCHIVE', 'active', 'HISTORICAL', 'RULE_2', 'legacy lifecycle test', 'APPROVED', ?)
        """,
        (now, now),
    )
    conn.commit()

    # 执行阶段 3
    applied_count = engine.phase_3_lifecycle_transition(approved)
    assert applied_count == 2

    # 验证 v3 记忆：状态 DELETED, revision 递增为 2, 新队列产生 pending 任务, 旧队列无任务
    cursor.execute(
        "SELECT status, projection_revision, deleted_at, deleted_by, deletion_reason FROM memories WHERE memory_id = 'mem_v3_cand'"
    )
    v3_row = cursor.fetchone()
    assert v3_row["status"] == "DELETED"
    assert v3_row["projection_revision"] == 2
    assert v3_row["deleted_by"] == "reconciliation_engine"
    assert v3_row["deletion_reason"] == "v3 lifecycle test"

    cursor.execute(
        "SELECT memory_id, projection_revision, status FROM memory_projection_outbox WHERE memory_id = 'mem_v3_cand'"
    )
    v3_outbox_rows = cursor.fetchall()
    assert len(v3_outbox_rows) == 1
    assert v3_outbox_rows[0]["projection_revision"] == 2
    assert v3_outbox_rows[0]["status"] == "pending"

    cursor.execute("SELECT count(*) FROM qdrant_sync_queue WHERE memory_id = 'mem_v3_cand'")
    assert cursor.fetchone()[0] == 0

    # 验证 legacy 记忆：状态 HISTORICAL, 旧队列产生 amr_delta_v1 任务, 新队列无任务
    cursor.execute("SELECT status FROM memories WHERE memory_id = 'mem_leg_cand'")
    leg_row = cursor.fetchone()
    assert leg_row["status"] == "HISTORICAL"

    cursor.execute("SELECT count(*) FROM memory_projection_outbox WHERE memory_id = 'mem_leg_cand'")
    assert cursor.fetchone()[0] == 0

    cursor.execute(
        "SELECT op_type, payload_snapshot, status FROM qdrant_sync_queue WHERE memory_id = 'mem_leg_cand'"
    )
    leg_sync_rows = cursor.fetchall()
    assert len(leg_sync_rows) == 1
    assert leg_sync_rows[0]["status"] == "pending"
    envelope = json.loads(leg_sync_rows[0]["payload_snapshot"])
    assert envelope.get("amr_delta_v1") is True
    assert envelope.get("op") == "patch"
    assert envelope.get("fields", {}).get("status") == "HISTORICAL"

    # 验证 curation_candidates 表状态已更新为 APPLIED
    cursor.execute("SELECT state FROM curation_candidates WHERE candidate_id IN ('cand_v3_1', 'cand_leg_1')")
    states = [r[0] for r in cursor.fetchall()]
    assert states == ["APPLIED", "APPLIED"]


def test_chunk_transaction_composition_no_nested_begin(tmp_path):
    """
    测试 chunk 事务组合：
    外部显式事务中调用 change_memory_status(..., cursor=cursor) 不触发嵌套 BEGIN，能原子提交。
    """
    store = _create_test_store(tmp_path, "chunk_tx.db")
    conn = store.get_connection()
    cursor = conn.cursor()
    now = int(time.time())

    # 插入 5 条记忆
    mem_ids = [f"mem_chunk_{i}" for i in range(5)]
    for mid in mem_ids:
        cursor.execute(
            """
            INSERT INTO memories (
                memory_id, qdrant_point_id, type, subject, predicate, content,
                valid_from, confidence, importance, mention_count, status,
                project_id, scope, source_agent, version, created_at, updated_at,
                projection_class, collection_name
            ) VALUES (?, ?, 'fact', 'test', 'pred', 'content', ?, 0.8, 0.5, 1, 'active',
                      'test_proj', 'global', 'agent', 1, ?, ?, 'legacy', 'ai_memory')
            """,
            (mid, f"pt_{mid}", now, now, now),
        )
    conn.commit()

    # 模拟 chunk 外部事务：逐条 change_memory_status 传入 cursor，不嵌套 BEGIN
    for mid in mem_ids:
        store.change_memory_status(
            mid,
            "HISTORICAL",
            actor="chunk_tester",
            reason="chunky commit test",
            cursor=cursor,
        )
    conn.commit()

    cursor.execute("SELECT status FROM memories WHERE memory_id IN ('mem_chunk_0', 'mem_chunk_1', 'mem_chunk_2', 'mem_chunk_3', 'mem_chunk_4')")
    statuses = [r[0] for r in cursor.fetchall()]
    assert all(s == "HISTORICAL" for s in statuses)

    # 验证审计日志
    cursor.execute("SELECT count(*) FROM memory_audit_log WHERE operator = 'chunk_tester'")
    assert cursor.fetchone()[0] == 5


def test_cognitive_forget_v3_and_legacy_dispatch(tmp_path):
    """
    测试 cognitive forget -> DELETED 分派：
    - v3 记忆: memories 标记 DELETED，进入 memory_projection_outbox
    - legacy 记忆: memories 标记 DELETED，进入 qdrant_sync_queue (amr_delta_v1 op='delete')
    - 不存在的记忆: 返回 False
    """
    store = _create_test_store(tmp_path, "forget_dispatch.db")
    cog = CognitiveEngine(session_store=store)
    conn = store.get_connection()
    cursor = conn.cursor()
    now = int(time.time())

    # 1. 创建 v3 记忆
    cursor.execute(
        """
        INSERT INTO memories (
            memory_id, qdrant_point_id, type, subject, predicate, content,
            valid_from, confidence, importance, mention_count, status,
            project_id, scope, source_agent, version, created_at, updated_at,
            projection_class, collection_name, projection_revision
        ) VALUES ('mem_forget_v3', 'pt_fv3', 'fact', 's', 'p', 'content',
                  ?, 0.9, 0.8, 1, 'ACTIVE', 'p1', 'global', 'agent', 1, ?, ?,
                  'v3', 'ai_memory', 1)
        """,
        (now, now, now),
    )

    # 2. 创建 legacy 记忆
    cursor.execute(
        """
        INSERT INTO memories (
            memory_id, qdrant_point_id, type, subject, predicate, content,
            valid_from, confidence, importance, mention_count, status,
            project_id, scope, source_agent, version, created_at, updated_at,
            projection_class, collection_name, projection_revision
        ) VALUES ('mem_forget_leg', 'pt_fleg', 'fact', 's', 'p', 'content',
                  ?, 0.9, 0.8, 1, 'active', 'p1', 'global', 'agent', 1, ?, ?,
                  'legacy', 'ai_memory', 0)
        """,
        (now, now, now),
    )
    conn.commit()

    # 遗忘 v3 记忆
    res_v3 = cog.forget("mem_forget_v3", deleted_by="sec_v3", reason="gdpr_v3")
    assert res_v3 is True
    mem_v3 = store.get_memory("mem_forget_v3")
    assert mem_v3["status"] == "DELETED"
    assert mem_v3["deleted_by"] == "sec_v3"
    assert mem_v3["deletion_reason"] == "gdpr_v3"

    cursor.execute("SELECT status, projection_revision FROM memory_projection_outbox WHERE memory_id = 'mem_forget_v3'")
    out_row = cursor.fetchone()
    assert out_row is not None
    assert out_row["status"] == "pending"

    # 遗忘 legacy 记忆
    res_leg = cog.forget("mem_forget_leg", deleted_by="sec_leg", reason="gdpr_leg")
    assert res_leg is True
    mem_leg = store.get_memory("mem_forget_leg")
    assert mem_leg["status"] == "DELETED"
    assert mem_leg["deleted_by"] == "sec_leg"
    assert mem_leg["deletion_reason"] == "gdpr_leg"

    cursor.execute("SELECT op_type, payload_snapshot FROM qdrant_sync_queue WHERE memory_id = 'mem_forget_leg'")
    sync_row = cursor.fetchone()
    assert sync_row is not None
    assert sync_row["op_type"] == "delete"
    env = json.loads(sync_row["payload_snapshot"])
    assert env.get("amr_delta_v1") is True
    assert env.get("op") == "delete"

    # 遗忘不存在的记忆
    assert cog.forget("mem_non_existent") is False


def test_merge_and_update_superseded_persisted(tmp_path):
    """
    测试 update_with_conflict_policy:
    - 旧记忆打上 HISTORICAL 状态
    - superseded_by 正确持久化到 memories 表及审计日志
    """
    store = _create_test_store(tmp_path, "supersede.db")
    cog = CognitiveEngine(session_store=store)

    # 1. 状态机迁移
    store.create_memory(
        memory_id="task_sm_1",
        subject="deploy_job",
        predicate="status_is",
        object="pending",
        content="Deploy job pending",
        type="task",
        conflict_policy="state_machine",
    )

    res_sm = cog.update_with_conflict_policy(
        existing_memory_id="task_sm_1",
        new_content="Deploy job running",
        new_task_status="in_progress",
    )
    assert res_sm["success"] is True
    new_mem_id = res_sm["new_memory"]["memory_id"]

    old_task = store.get_memory("task_sm_1")
    assert old_task["status"] == "HISTORICAL"
    assert old_task["superseded_by"] == new_mem_id
    assert old_task["superseded_at"] is not None

    # 2. overwrite 覆盖
    store.create_memory(
        memory_id="dec_ow_1",
        subject="framework_choice",
        predicate="selected",
        content="Chosen PyTorch",
        type="decision",
        conflict_policy="overwrite",
        version=1,
    )

    res_ow = cog.update_with_conflict_policy(
        existing_memory_id="dec_ow_1",
        new_content="Chosen JAX instead",
        new_object="jax",
    )
    assert res_ow["success"] is True
    new_dec_id = res_ow["new_memory"]["memory_id"]

    old_dec = store.get_memory("dec_ow_1")
    assert old_dec["status"] == "HISTORICAL"
    assert old_dec["superseded_by"] == new_dec_id
    assert old_dec["superseded_at"] is not None


@pytest.mark.asyncio
async def test_context_builder_mixed_case_recall(tmp_path):
    """
    测试 ContextBuilder 对大小写混合状态 ('active', 'ACTIVE', 'candidate', 'CANDIDATE') 均能正常召回，
    而 DELETED/HISTORICAL 等终态/归档态被排除。
    """
    store = _create_test_store(tmp_path, "context_builder.db")
    conn = store.get_connection()
    cursor = conn.cursor()
    now = int(time.time())
    old_ts = now - (10 * 86400)

    items = [
        ("mem_cb_1", "active", "db_entity", "content active lowercase", now),
        ("mem_cb_2", "ACTIVE", "db_entity", "content ACTIVE uppercase", now),
        ("mem_cb_3", "candidate", "cache_entity", "content candidate lowercase", now),
        ("mem_cb_4", "CANDIDATE", "cache_entity", "content CANDIDATE uppercase", now),
        ("mem_cb_5", "DELETED", "db_entity", "content deleted", old_ts),
        ("mem_cb_6", "HISTORICAL", "db_entity", "content historical", old_ts),
    ]

    for mid, st, subj, cnt, u_ts in items:
        cursor.execute(
            """
            INSERT INTO memories (
                memory_id, qdrant_point_id, type, subject, predicate, content,
                valid_from, confidence, importance, mention_count, status,
                project_id, scope, source_agent, version, created_at, updated_at,
                projection_class, collection_name
            ) VALUES (?, ?, 'fact', ?, 'is', ?, ?, 0.8, 0.5, 1, ?, 'my_project',
                      'global', 'agent', 1, ?, ?, 'legacy', 'ai_memory')
            """,
            (mid, f"pt_{mid}", subj, cnt, now, st, now, u_ts),
        )
    conn.commit()

    builder = ContextBuilder(session_store=store)

    # 1. 按 project_id 活跃记忆召回
    pack = await builder.build_context_pack(
        project_id="my_project",
        recent_messages=[],
        entities=None,
    )
    recalled_ids = {m["memory_id"] for m in pack}
    assert "mem_cb_1" in recalled_ids
    assert "mem_cb_2" in recalled_ids
    assert "mem_cb_3" in recalled_ids
    assert "mem_cb_4" in recalled_ids
    assert "mem_cb_5" not in recalled_ids
    assert "mem_cb_6" not in recalled_ids

    # 2. 按 entity 匹配召回
    pack_entity = await builder.build_context_pack(
        project_id="other_project",
        recent_messages=[],
        entities=["db_entity", "cache_entity"],
    )
    recalled_entity_ids = {m["memory_id"] for m in pack_entity}
    assert "mem_cb_1" in recalled_entity_ids
    assert "mem_cb_2" in recalled_entity_ids
    assert "mem_cb_3" in recalled_entity_ids
    assert "mem_cb_4" in recalled_entity_ids
    assert "mem_cb_5" not in recalled_entity_ids


def test_adopt_orphan_points_sets_legacy_and_collection(tmp_path):
    """
    测试 adopt_orphan_points 脚本：
    - INSERT 时落 projection_class='legacy'
    - collection_name 取点位所在集合
    - status 经 normalize_status 规范化
    - --dry-run 模式不写库
    """
    db_file = tmp_path / "adopt_test.db"
    store = SessionStore(db_path=str(db_file))

    # 构造 mock QdrantClient
    mock_client = MagicMock()
    fake_point = MagicMock()
    fake_point.id = "qdrant_pt_orphan_99"
    fake_point.payload = {
        "memory_id": "mem_orphan_99",
        "content": "Orphan memory content from vector index",
        "status": "active",  # lowercase -> 应规范化为 ACTIVE
        "created_at": 1700000000,
        "memory_type": "decision",
        "subject": "infra",
        "predicate": "selected",
        "object": "k8s",
    }
    mock_client.scroll.return_value = ([fake_point], None)

    # 1. 验证 dry-run 行为
    count_dry = adopt_orphans(
        db_path=str(db_file),
        collection_name="tenant_col_x",
        dry_run=True,
        client=mock_client,
    )
    assert count_dry == 0
    conn = store.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM memories WHERE memory_id = 'mem_orphan_99'")
    assert cur.fetchone()[0] == 0

    # 2. 验证实际 apply 补登
    count_applied = adopt_orphans(
        db_path=str(db_file),
        collection_name="tenant_col_x",
        dry_run=False,
        client=mock_client,
    )
    assert count_applied == 1

    cur.execute(
        "SELECT memory_id, projection_class, collection_name, status, content_hash FROM memories WHERE memory_id = 'mem_orphan_99'"
    )
    row = cur.fetchone()
    assert row is not None
    assert row["memory_id"] == "mem_orphan_99"
    assert row["projection_class"] == "legacy"
    assert row["collection_name"] == "tenant_col_x"
    assert row["status"] == "ACTIVE"
    assert row["content_hash"] is not None


def test_normalize_status_schema_inventory_and_rehearse(tmp_path):
    """
    测试 normalize_status_schema 脚本：
    - STATUS_MAP 与 normalize_status 对齐 (archived/superseded -> HISTORICAL, HISTORICAL -> HISTORICAL)
    - --inventory 只读统计各状态计数与歧义清单
    - --rehearse 复制到临时副本演练迁移，绝不改源库，输出前后对账 JSON
    """
    # 验证状态映射表对齐
    assert get_normalized_status("active") == "ACTIVE"
    assert get_normalized_status("archived") == "HISTORICAL"
    assert get_normalized_status("superseded") == "HISTORICAL"
    assert get_normalized_status("historical") == "HISTORICAL"
    assert get_normalized_status("HISTORICAL") == "HISTORICAL"
    assert get_normalized_status("deleted") == "DELETED"

    db_file = tmp_path / "schema_test.db"
    store = SessionStore(db_path=str(db_file))
    conn = store.get_connection()
    cur = conn.cursor()
    now = int(time.time())

    # 插入多状态数据（含小写、大写、歧义态）
    test_rows = [
        ("m_norm_1", "active"),
        ("m_norm_2", "archived"),
        ("m_norm_3", "superseded"),
        ("m_norm_4", "ACTIVE"),
        ("m_norm_5", "HISTORICAL"),
        ("m_norm_6", "deleted"),
        ("m_norm_7", "UNKNOWN_XYZ"),
    ]
    for mid, st in test_rows:
        cur.execute(
            """
            INSERT INTO memories (
                memory_id, qdrant_point_id, type, subject, predicate, content,
                valid_from, confidence, importance, mention_count, status,
                project_id, scope, source_agent, version, created_at, updated_at,
                projection_class, collection_name
            ) VALUES (?, ?, 'fact', 'subj', 'pred', 'content', ?, 0.8, 0.5, 1, ?,
                      'p_test', 'global', 'agent', 1, ?, ?, 'legacy', 'ai_memory')
            """,
            (mid, f"pt_{mid}", now, st, now, now),
        )
    conn.commit()

    # 1. 验证 inventory (只读统计)
    inv = run_inventory(str(db_file), client=None)
    assert inv["sqlite_total"] == 7
    assert inv["sqlite_status_counts"]["active"] == 1
    assert inv["sqlite_status_counts"]["archived"] == 1
    assert inv["sqlite_status_counts"]["superseded"] == 1
    assert inv["sqlite_status_counts"]["deleted"] == 1
    assert inv["sqlite_will_change_count"] == 4  # active, archived, superseded, deleted 需要变更
    # UNKNOWN_XYZ 在 CANONICAL_STATUSES 外，应进入歧义清单
    ambiguous_statuses = [a["raw_status"] for a in inv["sqlite_ambiguities"]]
    assert "UNKNOWN_XYZ" in ambiguous_statuses

    # 2. 验证 rehearse (演练模式)
    rehearsal = run_rehearse(str(db_file))
    assert rehearsal["status"] == "rehearsal_completed"
    assert rehearsal["source_untouched"] is True
    assert "先 outbox/分类迁移，再状态枚举迁移" in rehearsal["order_explanation"]
    assert rehearsal["before_inventory"]["active"] == 1
    assert rehearsal["before_inventory"]["archived"] == 1
    assert rehearsal["modified_count"] == 4

    # 核心安全断言：源库绝对未被修改
    cur.execute("SELECT memory_id, status FROM memories WHERE memory_id = 'm_norm_2'")
    row_archived = cur.fetchone()
    assert row_archived["status"] == "archived"  # 源库仍然是小写 archived，未被触碰！

