"""
tests/test_reconciliation.py
测试 AMR 记忆对账与生命周期治理 (RFC-003)
"""

import tempfile
import time
import pytest
from unittest.mock import MagicMock, patch

from src.core.session_store import SessionStore
from src.reconciliation.policy_gate import PolicyGate, CandidateState
from src.reconciliation.rules import RuleEngine
from src.reconciliation.engine import ReconciliationEngine


def test_schema_migration_creates_rfc003_tables_and_columns():
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        store = SessionStore(db_path=tmp.name)
        conn = store.get_connection()
        cursor = conn.cursor()

        # 检查 memories 表新字段
        cursor.execute("PRAGMA table_info(memories)")
        cols = {row["name"] for row in cursor.fetchall()}
        assert "content_hash" in cols
        assert "curation_batch_id" in cols
        assert "last_reconciled_at" in cols

        # 检查新增表
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row["name"] for row in cursor.fetchall()}
        assert "memory_projection_outbox" in tables
        assert "curation_candidates" in tables
        assert "reconciliation_checkpoints" in tables


def test_policy_gate_circuit_breaker():
    gate = PolicyGate(dry_run=True)
    # total_points = 100 -> max_allowed = max(50, min(5, 5000)) = 50
    candidates = [{"memory_id": f"m_{i}", "proposed_status": "deleted"} for i in range(60)]
    approved, rejected, summary = gate.evaluate(candidates, total_points=100)
    assert summary["circuit_breaker_tripped"] is True
    assert len(approved) == 0
    assert len(rejected) == 60


def test_policy_gate_whitelist_protection():
    gate = PolicyGate(dry_run=True)
    candidates = [
        {
            "candidate_id": "c1",
            "memory_id": "m1",
            "content": "核心经验总结 [LESSON_LEARNED] 密码学关键流程",
            "proposed_status": "deleted",
            "confidence": 0.8,
            "importance": 0.5,
        },
        {
            "candidate_id": "c2",
            "memory_id": "m2",
            "content": "高重要度决策",
            "proposed_status": "deleted",
            "confidence": 0.95,
            "importance": 0.85,
        },
        {
            "candidate_id": "c3",
            "memory_id": "m3",
            "content": "普通噪音 ping",
            "proposed_status": "deleted",
            "confidence": 0.5,
            "importance": 0.2,
        },
    ]
    approved, rejected, summary = gate.evaluate(candidates, total_points=1000)
    assert summary["whitelist_blocked"] == 1
    assert summary["high_value_blocked"] == 1
    assert len(approved) == 1
    assert approved[0]["memory_id"] == "m3"


def test_rule_engine_detection():
    engine = RuleEngine()
    # 心跳探测
    cand1 = engine.scan_memory({"memory_id": "m1", "content": "ping", "status": "active"})
    assert cand1 is not None
    assert cand1["matched_rule_id"] == "RULE_HEARTBEAT_NOISE"

    # 工具循环报错
    cand2 = engine.scan_memory({
        "memory_id": "m2",
        "content": "error: timeout error: timeout error: timeout",
        "status": "active",
    })
    assert cand2 is not None
    assert cand2["matched_rule_id"] == "RULE_TOOL_LOOP_ERROR"

    # 临时协议注入
    cand3 = engine.scan_memory({
        "memory_id": "m3",
        "content": "<!--TEMP_AMR_SYNC_MARKER--> test injection",
        "status": "active",
    })
    assert cand3 is not None
    assert cand3["matched_rule_id"] == "RULE_TEMP_PROTOCOL_INJECT"

    # 正常技术记忆不命中
    cand4 = engine.scan_memory({
        "memory_id": "m4",
        "content": "SM2 签名算法遵循 GM/T 0009 标准规范",
        "status": "active",
    })
    assert cand4 is None


def test_reconciliation_engine_lifecycle_chunky_commit():
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        store = SessionStore(db_path=tmp.name)
        # 预先插入 250 条待治理测试记忆
        conn = store.get_connection()
        cursor = conn.cursor()
        now = int(time.time())
        for i in range(250):
            cursor.execute(
                """
                INSERT INTO memories (
                    memory_id, qdrant_point_id, type, subject, predicate, content,
                    valid_from, confidence, importance, mention_count, status,
                    project_id, scope, source_agent, version, created_at, updated_at
                ) VALUES (?, ?, 'fact', 'test', 'ping', 'ping', ?, 0.5, 0.2, 1, 'active', 'test', 'global', 'agent', 1, ?, ?)
                """,
                (f"test_mem_{i}", f"test_pt_{i}", now, now, now),
            )
        conn.commit()

        engine = ReconciliationEngine(dry_run=False, db_path=tmp.name, batch_id="test_batch_1")
        # 伪造 policy_gate 审核通过 250 条
        approved = [
            {
                "candidate_id": f"cand_{i}",
                "memory_id": f"test_mem_{i}",
                "proposed_status": "deleted",
                "reason": "心跳测试",
            }
            for i in range(250)
        ]

        applied = engine.phase_3_lifecycle_transition(approved)
        assert applied == 250

        # 检查 SQLite 状态已流转
        cursor.execute("SELECT count(*) FROM memories WHERE status = 'deleted'")
        assert cursor.fetchone()[0] == 250

        # 检查 memory_projection_outbox 已记录
        cursor.execute("SELECT count(*) FROM memory_projection_outbox WHERE op_type = 'delete'")
        assert cursor.fetchone()[0] == 250
