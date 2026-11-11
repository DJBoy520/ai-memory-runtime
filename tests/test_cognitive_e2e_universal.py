"""
tests/test_cognitive_e2e_universal.py
端到端集成测试：AMR 现有 src/cognitive 提炼流程与通用会话输入接通
覆盖需求：
1. 验证 MCP session.ingest 写入 messages/sessions 表后可供 CognitiveScheduler 增量扫描并提炼
2. 保留来源与证据校验：防编造/防幻觉，严格校验 extracted_spans 与 evidence_source_ids
3. 铁律保护：不允许未经验证的事实自动生效（仅作为 PROPOSED 写入 curation_candidates，绝不直接写 memories SSOT）
4. 假模型（Mock LLM Worker）端到端闭环，具备失败自动重试能力
5. 包含临时 SQLite 隔离环境与批量跑批审计测试
"""

import asyncio
import json
import tempfile
import time
from typing import Any, Dict, List
import pytest

from src.core.session_store import SessionStore
from src.cognitive.scheduler import CognitiveScheduler
from src.cognitive.evidence_validator import EvidenceValidator
from src.cognitive.coverage_validator import CoverageValidator
from src.cognitive.context_builder import ContextBuilder
from src.cognitive.prompt_manager import PromptManager


@pytest.fixture
def temp_store():
    """提供临时独立的 SQLite 存储环境，测试结束后自动销毁"""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        store = SessionStore(db_path=tmp.name)
        yield store


@pytest.mark.asyncio
async def test_mcp_session_ingest_to_scheduler_distillation_e2e(temp_store):
    """
    核心闭环 E2E 测试：
    1. 模拟 MCP session.ingest（SessionStore.ingest_messages）写入真实多轮技术对话；
    2. CognitiveScheduler 扫描增量会话，验证通用会话输入能够被正确捕获；
    3. Mock LLM Worker 提炼，验证来源与证据三层匹配；
    4. 铁律验证：通过的提纯提案进入 curation_candidates (PROPOSED)，memories SSOT 零写权限。
    """
    now = int(time.time())
    session_id = "mcp_sess_prod_001"
    agent_id = "mcp_openclaw_agent"
    project_id = "project_titan"

    # 1. 模拟 MCP session.ingest 写入对话
    messages = [
        {
            "message_id": "msg_001",
            "role": "user",
            "content": "我们确定生产环境数据库为 PostgreSQL 16，主库 IP 10.0.10.5，监听端口 5432，WAL 日志归档路径设为 /data/pg_wal。",
            "sequence": 1,
            "timestamp": now - 120,
        },
        {
            "message_id": "msg_002",
            "role": "assistant",
            "content": "收到，已固化数据库规格：PostgreSQL 16，地址 10.0.10.5:5432，开启 WAL 归档到 /data/pg_wal，最大连接数限制为 500。",
            "sequence": 2,
            "timestamp": now - 100,
        },
        {
            "message_id": "msg_003",
            "role": "user",
            "content": "好的，辛苦了，今天先聊到这里。",
            "sequence": 3,
            "timestamp": now - 80,
        },
    ]

    ingest_result = temp_store.ingest_messages(
        session_id=session_id,
        agent_id=agent_id,
        project_id=project_id,
        messages=messages,
    )
    assert ingest_result["inserted"] == 3
    assert ingest_result["session_id"] == session_id

    # 验证 sessions 与 messages 表已正确入库
    conn = temp_store.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM sessions WHERE session_id = ?", (session_id,))
    sess_row = cur.fetchone()
    assert sess_row is not None
    assert sess_row["project_id"] == project_id
    assert sess_row["agent_id"] == agent_id

    # 2. 调度器执行增量扫描，验证能捕获 MCP ingest 的通用会话
    scheduler = CognitiveScheduler(session_store=temp_store)
    scanned = scheduler.scan_incremental_sessions(lookback_seconds=3600, limit=10)
    assert len(scanned) >= 1
    target_scan = next((s for s in scanned if s["session_id"] == session_id), None)
    assert target_scan is not None
    assert target_scan["project_id"] == project_id

    # 3. 构造 Mock LLM Worker（包含合法提案与编造提案）
    def mock_model(system_prompt: str, user_content: str) -> str:
        assert project_id in system_prompt or "titan" in system_prompt or "Memory Context Pack" in system_prompt
        user_msgs = json.loads(user_content)
        assert len(user_msgs) == 3

        return json.dumps({
            "prompt_version": "v3.0.0-mos",
            "session_id": session_id,
            "candidates": [
                {
                    "proposal_id": "prop_valid_pg",
                    "operation": "EXTRACT",
                    "subject": "PostgreSQL_Database",
                    "predicate": "deployed_at",
                    "object": "10.0.10.5:5432",
                    "content": "生产环境数据库为 PostgreSQL 16，主库 IP 10.0.10.5，监听端口 5432，WAL 日志归档路径设为 /data/pg_wal。",
                    "evidence_source_ids": ["msg_001", "msg_002"],
                    "extracted_spans": [
                        {"message_id": "msg_001", "span": "生产环境数据库为 PostgreSQL 16，主库 IP 10.0.10.5，监听端口 5432，WAL 日志归档路径设为 /data/pg_wal。"},
                        {"message_id": "msg_002", "span": "最大连接数限制为 500"}
                    ],
                    "rationale": "生产环境数据库核心配置与连接池参数",
                    "self_assessed_confidence": 0.98,
                },
                {
                    "proposal_id": "prop_hallucinated",
                    "operation": "EXTRACT",
                    "subject": "Cache_Layer",
                    "predicate": "uses",
                    "object": "Redis_Cluster",
                    "content": "系统采用 Redis 集群部署在 10.0.10.99:6379",
                    "evidence_source_ids": ["msg_001"],
                    "extracted_spans": [
                        {"message_id": "msg_001", "span": "系统采用 Redis 集群部署在 10.0.10.99:6379"}
                    ],
                    "rationale": "虚构编造的缓存配置",
                    "self_assessed_confidence": 0.90,
                }
            ],
            "message_disposition": [
                {"message_id": "msg_001", "disposition": "EXTRACTED"},
                {"message_id": "msg_002", "disposition": "EXTRACTED"},
                {"message_id": "msg_003", "disposition": "DISCARDED", "reason": "客套礼貌用语"},
            ]
        }, ensure_ascii=False)

    scheduler.llm_worker = mock_model

    # 4. 执行提炼流水线
    batch_id = "batch_mcp_e2e_001"
    pipeline_res = await scheduler.run_pipeline_for_session(
        session_id=session_id,
        batch_id=batch_id,
    )

    assert pipeline_res["status"] == "SUCCESS"
    assert pipeline_res["project_id"] == project_id
    assert pipeline_res["total_candidates"] == 2
    assert pipeline_res["valid_candidates"] == 1
    assert pipeline_res["invalid_candidates"] == 1
    assert pipeline_res["written_candidates"] == 1

    # 5. 校验候选表 (curation_candidates) 与未验证事实隔离
    cur.execute("SELECT * FROM curation_candidates WHERE batch_id = ?", (batch_id,))
    rows = cur.fetchall()
    assert len(rows) == 1
    cand = dict(rows[0])
    assert cand["candidate_id"] == "prop_valid_pg"
    assert cand["state"] == "PROPOSED"
    assert "10.0.10.5" in cand["content"]

    # 6. 铁律检查：memories SSOT 绝不能有任何直接写入（未经验证的事实绝不生效）
    cur.execute("SELECT COUNT(*) as cnt FROM memories WHERE subject = 'PostgreSQL_Database'")
    mem_cnt = cur.fetchone()["cnt"]
    assert mem_cnt == 0

    cur.execute("SELECT COUNT(*) as cnt FROM memories WHERE content LIKE '%10.0.10.5%'")
    assert cur.fetchone()["cnt"] == 0


@pytest.mark.asyncio
async def test_scheduler_mock_model_transient_failure_and_retry(temp_store):
    """
    测试假模型临时失败与自动重试机制：
    1. 模拟 LLM Worker 前 2 次调用抛出瞬时超时异常或返回畸形文本；
    2. 第 3 次成功返回合规 JSON；
    3. Scheduler 应自动重试并最终成功完成流水线；
    4. 验证当重试次数耗尽时的优雅降级响应。
    """
    now = int(time.time())
    session_id = "retry_sess_001"
    temp_store.ingest_messages(
        session_id=session_id,
        agent_id="test_agent",
        project_id="proj_retry",
        messages=[
            {"message_id": "m1", "role": "user", "content": "认证服务统一使用 RS256 算法签名 JWT。", "sequence": 1, "timestamp": now},
            {"message_id": "m2", "role": "assistant", "content": "确认，公钥路径为 /etc/auth/jwt.pub。", "sequence": 2, "timestamp": now + 1},
        ],
    )

    call_count = 0

    def flaky_model(prompt: str, user_content: str) -> str:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ConnectionError("504 Gateway Timeout from LLM Provider")
        elif call_count == 2:
            return "Malformed non-JSON output from LLM worker..."
        else:
            return json.dumps({
                "prompt_version": "v3.0.0-mos",
                "session_id": session_id,
                "candidates": [
                    {
                        "proposal_id": "prop_jwt_01",
                        "operation": "EXTRACT",
                        "subject": "AuthService",
                        "predicate": "signs_with",
                        "object": "RS256",
                        "content": "认证服务统一使用 RS256 算法签名 JWT，公钥路径为 /etc/auth/jwt.pub。",
                        "evidence_source_ids": ["m1", "m2"],
                        "extracted_spans": [
                            {"message_id": "m1", "span": "认证服务统一使用 RS256 算法签名 JWT"},
                            {"message_id": "m2", "span": "公钥路径为 /etc/auth/jwt.pub"}
                        ],
                        "rationale": "认证签名规范与公钥路径",
                        "self_assessed_confidence": 0.95
                    }
                ],
                "message_disposition": [
                    {"message_id": "m1", "disposition": "EXTRACTED"},
                    {"message_id": "m2", "disposition": "EXTRACTED"}
                ]
            }, ensure_ascii=False)

    scheduler = CognitiveScheduler(session_store=temp_store, llm_worker=flaky_model, default_max_retries=3)
    res = await scheduler.run_pipeline_for_session(session_id=session_id, batch_id="batch_retry_success")

    assert call_count == 3
    assert res["status"] == "SUCCESS"
    assert res["written_candidates"] == 1

    # 测试重试次数耗尽时的优雅处理
    always_fail_count = 0

    def dead_model(prompt: str, user_content: str) -> str:
        nonlocal always_fail_count
        always_fail_count += 1
        raise RuntimeError("Persistent Upstream Outage")

    scheduler.llm_worker = dead_model
    fail_res = await scheduler.run_pipeline_for_session(
        session_id=session_id,
        batch_id="batch_retry_exhausted",
        max_retries=2,
    )
    assert always_fail_count == 2
    assert fail_res["status"] == "LLM_ERROR"
    assert "Persistent Upstream Outage" in fail_res["error"]
    assert fail_res["written_candidates"] == 0


@pytest.mark.asyncio
async def test_evidence_validator_strictly_rejects_hallucinations_and_tampered_sources(temp_store):
    """
    测试证据链与来源校验防御深度：
    1. 候选提案引用不存在的 message_id 时拦截；
    2. 候选提案 extracted_spans 指向不存在的消息时拦截；
    3. 语义相反或篡改原文事实（伪造 span）时拦截；
    4. 验证未通过提案不得写入 curation_candidates。
    """
    now = int(time.time())
    session_id = "evidence_tamper_sess"
    temp_store.ingest_messages(
        session_id=session_id,
        agent_id="test_agent",
        project_id="proj_tamper",
        messages=[
            {"message_id": "msg_real_1", "role": "user", "content": "我们暂不开启自动扩缩容，维持固定 3 个 Pod 运行。", "sequence": 1, "timestamp": now},
            {"message_id": "msg_real_2", "role": "assistant", "content": "好的，已保持静态副本数为 3。", "sequence": 2, "timestamp": now + 2},
        ],
    )

    # 构造多种造假提案
    tampered_llm_result = {
        "prompt_version": "v3.0.0-mos",
        "session_id": session_id,
        "candidates": [
            # 造假 1：虚造不存在的 evidence_source_ids
            {
                "proposal_id": "prop_fake_source",
                "operation": "EXTRACT",
                "subject": "Cluster",
                "predicate": "scales",
                "object": "Autoscaling",
                "content": "集群开启自动扩容",
                "evidence_source_ids": ["msg_non_existent_999"],
                "extracted_spans": [{"span": "维持固定 3 个 Pod 运行"}],
                "self_assessed_confidence": 0.8
            },
            # 造假 2：extracted_spans 指定不存在的 message_id
            {
                "proposal_id": "prop_fake_span_msg_id",
                "operation": "EXTRACT",
                "subject": "Cluster",
                "predicate": "pod_count",
                "object": "3",
                "content": "静态副本数为 3",
                "evidence_source_ids": ["msg_real_1"],
                "extracted_spans": [{"message_id": "msg_ghost_404", "span": "静态副本数为 3"}],
                "self_assessed_confidence": 0.9
            },
            # 造假 3：颠倒黑白，篡改原文事实（原文是'暂不开启自动扩缩容'，篡改为'全面开启自动扩容'）
            {
                "proposal_id": "prop_opposite_fact",
                "operation": "EXTRACT",
                "subject": "Cluster",
                "predicate": "status",
                "object": "Autoscaling_Enabled",
                "content": "系统全面开启自动扩容模式",
                "evidence_source_ids": ["msg_real_1"],
                "extracted_spans": [{"message_id": "msg_real_1", "span": "全面开启自动扩容"}],
                "self_assessed_confidence": 0.95
            },
            # 真实合法提案
            {
                "proposal_id": "prop_legit",
                "operation": "EXTRACT",
                "subject": "Cluster_Replicas",
                "predicate": "fixed_at",
                "object": "3",
                "content": "系统维持固定 3 个 Pod 运行，保持静态副本数为 3。",
                "evidence_source_ids": ["msg_real_1", "msg_real_2"],
                "extracted_spans": [
                    {"message_id": "msg_real_1", "span": "维持固定 3 个 Pod 运行"},
                    {"message_id": "msg_real_2", "span": "保持静态副本数为 3"}
                ],
                "self_assessed_confidence": 0.99
            }
        ],
        "message_disposition": [
            {"message_id": "msg_real_1", "disposition": "EXTRACTED"},
            {"message_id": "msg_real_2", "disposition": "EXTRACTED"},
        ]
    }

    scheduler = CognitiveScheduler(session_store=temp_store)
    res = await scheduler.run_pipeline_for_session(
        session_id=session_id,
        batch_id="batch_tamper_audit",
        mock_llm_result=tampered_llm_result,
    )

    assert res["status"] == "SUCCESS"
    assert res["total_candidates"] == 4
    assert res["valid_candidates"] == 1
    assert res["invalid_candidates"] == 3
    assert res["written_candidates"] == 1

    # 验证只有 prop_legit 写入
    conn = temp_store.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT candidate_id FROM curation_candidates WHERE batch_id = 'batch_tamper_audit'")
    written_ids = [row["candidate_id"] for row in cur.fetchall()]
    assert written_ids == ["prop_legit"]


@pytest.mark.asyncio
async def test_scheduler_run_batch_across_multiple_ingested_sessions(temp_store):
    """
    测试批量驱动流水线 (run_batch)：
    1. 写入多个来自不同 agent / project 的通用会话；
    2. 执行批量认知提纯；
    3. 验证幂等性：重复执行不发生主键冲突或污染。
    """
    now = int(time.time())

    # 会话 A
    temp_store.ingest_messages(
        session_id="batch_sess_A",
        agent_id="agent_alpha",
        project_id="proj_alpha",
        messages=[
            {"message_id": "a1", "role": "user", "content": "Alpha 节点绑定端口 8001。", "sequence": 1, "timestamp": now - 50},
            {"message_id": "a2", "role": "assistant", "content": "Alpha 端口 8001 设置成功。", "sequence": 2, "timestamp": now - 40},
        ],
    )

    # 会话 B
    temp_store.ingest_messages(
        session_id="batch_sess_B",
        agent_id="agent_beta",
        project_id="proj_beta",
        messages=[
            {"message_id": "b1", "role": "user", "content": "Beta 服务超时阈值配置为 30 秒。", "sequence": 1, "timestamp": now - 30},
            {"message_id": "b2", "role": "assistant", "content": "Beta 超时 30 秒已生效。", "sequence": 2, "timestamp": now - 20},
        ],
    )

    def simple_mock_worker(prompt: str, user_content: str) -> str:
        msgs = json.loads(user_content)
        sess_id = msgs[0]["message_id"][0]  # 'a' or 'b'
        if sess_id == "a":
            return json.dumps({
                "prompt_version": "v3.0.0-mos",
                "session_id": "batch_sess_A",
                "candidates": [{
                    "proposal_id": "prop_a",
                    "operation": "EXTRACT",
                    "subject": "Alpha_Node",
                    "predicate": "listens_on",
                    "object": "8001",
                    "content": "Alpha 节点绑定端口 8001",
                    "evidence_source_ids": ["a1"],
                    "extracted_spans": [{"message_id": "a1", "span": "Alpha 节点绑定端口 8001"}],
                }],
                "message_disposition": [
                    {"message_id": "a1", "disposition": "EXTRACTED"},
                    {"message_id": "a2", "disposition": "UNPROCESSED"},
                ]
            }, ensure_ascii=False)
        else:
            return json.dumps({
                "prompt_version": "v3.0.0-mos",
                "session_id": "batch_sess_B",
                "candidates": [{
                    "proposal_id": "prop_b",
                    "operation": "EXTRACT",
                    "subject": "Beta_Service",
                    "predicate": "timeout",
                    "object": "30s",
                    "content": "Beta 服务超时阈值配置为 30 秒",
                    "evidence_source_ids": ["b1"],
                    "extracted_spans": [{"message_id": "b1", "span": "Beta 服务超时阈值配置为 30 秒"}],
                }],
                "message_disposition": [
                    {"message_id": "b1", "disposition": "EXTRACTED"},
                    {"message_id": "b2", "disposition": "UNPROCESSED"},
                ]
            }, ensure_ascii=False)

    scheduler = CognitiveScheduler(session_store=temp_store, llm_worker=simple_mock_worker)

    # 首次批量执行
    batch_report_1 = await scheduler.run_batch(lookback_seconds=3600, batch_id="batch_run_01")
    assert batch_report_1["sessions_scanned"] >= 2
    assert batch_report_1["sessions_succeeded"] >= 2
    assert batch_report_1["total_written_candidates"] == 2

    # 验证幂等重跑
    batch_report_2 = await scheduler.run_batch(lookback_seconds=3600, batch_id="batch_run_01")
    assert batch_report_2["sessions_succeeded"] >= 2

    conn = temp_store.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) as cnt FROM curation_candidates WHERE batch_id = 'batch_run_01'")
    assert cur.fetchone()["cnt"] == 2
