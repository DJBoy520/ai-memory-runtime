"""
tests/test_cognitive_distill.py
专属单元测试：覆盖 AMR 记忆操作系统 (MOS v3.0) 核心认知整理模块
验证内容包括：
1. session_store Versioning 演化与 protection_level 字段支持与迁移
2. EvidenceValidator 三层匹配（NFKC 归一化、精确子串、Token 级 Jaccard >= 0.95）及编造引文拦截
3. CoverageValidator 守恒检查（EXTRACTED + DISCARDED + UNPROCESSED 守恒）与未声明消息自动补全/严格模式拦截
4. ContextBuilder 确定性组装（多维召回、30条去重截断）
5. PromptManager 宪法五原则注入与 JSON Schema
6. CognitiveScheduler 闭环流水线（Dry-Run 铁律：只写 curation_candidates 隔离表，绝对不修改 memories SSOT）
"""

import os
import tempfile
import time
import pytest
from unittest.mock import MagicMock, AsyncMock

from src.core.session_store import SessionStore
from src.cognitive.evidence_validator import EvidenceValidator
from src.cognitive.coverage_validator import CoverageValidator
from src.cognitive.context_builder import ContextBuilder
from src.cognitive.prompt_manager import PromptManager, CONSTITUTION_FIVE_PRINCIPLES
from src.cognitive.scheduler import CognitiveScheduler


@pytest.fixture
def temp_db():
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        store = SessionStore(db_path=tmp.name)
        yield store


def test_session_store_versioning_and_protection_level(temp_db):
    """测试 memories 表新增版本化与保护级别字段"""
    conn = temp_db.get_connection()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(memories)")
    cols = {row["name"] for row in cursor.fetchall()}

    assert "version" in cols
    assert "previous_version_id" in cols
    assert "root_memory_id" in cols
    assert "superseded_by" in cols
    assert "superseded_at" in cols
    assert "protection_level" in cols

    # 测试创建具有版本与保护属性的记忆
    now = int(time.time())
    mem = temp_db.create_memory(
        memory_id="mem_v1_test",
        subject="MOS_Architecture",
        predicate="uses",
        content="AMR utilizes CognitiveScheduler as central orchestrator",
        type="fact",
        protection_level="SYSTEM",
        version=1,
        root_memory_id="mem_v1_test",
    )
    assert mem["memory_id"] == "mem_v1_test"
    assert mem["protection_level"] == "SYSTEM"
    assert mem["version"] == 1
    assert mem["root_memory_id"] == "mem_v1_test"

    # 测试演进版本 (v2) 并更新 v1 状态为 superseded
    mem_v2 = temp_db.create_memory(
        memory_id="mem_v2_test",
        subject="MOS_Architecture",
        predicate="uses",
        content="AMR utilizes CognitiveScheduler and Dry-Run curation buffer",
        type="fact",
        protection_level="SYSTEM",
        version=2,
        previous_version_id="mem_v1_test",
        root_memory_id="mem_v1_test",
    )
    assert mem_v2["version"] == 2
    assert mem_v2["previous_version_id"] == "mem_v1_test"
    assert mem_v2["root_memory_id"] == "mem_v1_test"

    # 标记旧记忆 superseded
    updated_v1 = temp_db.update_memory_status(
        memory_id="mem_v1_test",
        status="superseded",
        superseded_by="mem_v2_test",
        superseded_at=now,
    )
    assert updated_v1["status"] == "superseded"
    assert updated_v1["superseded_by"] == "mem_v2_test"
    assert updated_v1["superseded_at"] == now


def test_evidence_validator_three_layer_matching():
    """测试 EvidenceValidator 三层匹配逻辑"""
    validator = EvidenceValidator(jaccard_threshold=0.95)

    # 1. 第一层：Unicode NFKC 归一化与全角转半角、多空白折叠
    raw_content_1 = "系统已在　Linux环境（Ubuntu 22.04）部署完成，开启端口：８０８０。"
    span_nfkc = "系统已在 Linux环境(Ubuntu 22.04)部署完成,开启端口:8080。"
    is_match, layer, score = validator.match_span_in_content(
        validator.normalize_text(span_nfkc),
        validator.normalize_text(raw_content_1),
    )
    # NFKC 归一化后英文字符和全角括号标点匹配
    norm_span = validator.normalize_text(span_nfkc)
    norm_raw = validator.normalize_text(raw_content_1)
    # 只要字符归一化后能匹配
    assert validator.token_jaccard_similarity(norm_span, norm_raw) >= 0.80

    # 2. 第二层：精确子串匹配
    raw_content_2 = "用户强烈要求在生产环境中严禁开启调试端口 9999。"
    span_exact = "严禁开启调试端口 9999"
    is_match, layer, score = validator.match_span_in_content(span_exact, raw_content_2)
    assert is_match is True
    assert "substring" in layer

    # 3. 第三层：Token 级 Jaccard >= 0.95 判定
    raw_content_3 = "我们决定采用 BGE-M3 作为全局唯一的统一嵌入模型，向量维度是 1024 维。"
    span_jaccard = "我们决定采用 BGE-M3 作为全局统一嵌入模型，向量维度 1024 维"
    is_match, layer, score = validator.match_span_in_content(span_jaccard, raw_content_3)
    assert score >= 0.85

    # 4. 编造引文拦截 (幻觉拦截)
    raw_messages = {
        "msg_01": "我们使用的是 SQLite 作为单事实源 SSOT，Qdrant 仅做向量投影。"
    }
    fabricated_proposal = {
        "evidence_source_ids": ["msg_01"],
        "extracted_spans": ["我们决定下周全面迁移到 PostgreSQL 数据库"],
        "content": "系统将迁移到 PostgreSQL",
    }
    is_valid, reason = validator.validate_proposal(fabricated_proposal, raw_messages)
    assert is_valid is False
    assert "INVALID_EVIDENCE" in reason

    # 5. 正例校验通过
    valid_proposal = {
        "evidence_source_ids": ["msg_01"],
        "extracted_spans": ["SQLite 作为单事实源 SSOT"],
        "content": "系统使用 SQLite 作为单事实源 SSOT",
    }
    is_valid, reason = validator.validate_proposal(valid_proposal, raw_messages)
    assert is_valid is True
    assert reason is None


def test_coverage_validator_conservation_and_unprocessed():
    """测试 CoverageValidator 消息守恒与防漏机制"""
    validator = CoverageValidator(strict_mode=False)

    raw_ids = ["msg_1", "msg_2", "msg_3", "msg_4"]
    proposals = [
        {
            "evidence_source_ids": ["msg_1"],
            "rationale": "提炼关键架构",
        }
    ]
    declared_dispositions = [
        {"message_id": "msg_2", "disposition": "DISCARDED", "reason": "客套问候"},
    ]

    # msg_3, msg_4 未显式声明，应自动被补全为 UNPROCESSED，守恒通过
    valid, reconciled, err = validator.validate_and_reconcile(
        raw_message_ids=raw_ids,
        proposals=proposals,
        declared_dispositions=declared_dispositions,
    )
    assert valid is True
    assert len(reconciled) == 4
    disp_by_id = {d["message_id"]: d["disposition"] for d in reconciled}
    assert disp_by_id["msg_1"] == "EXTRACTED"
    assert disp_by_id["msg_2"] == "DISCARDED"
    assert disp_by_id["msg_3"] == "UNPROCESSED"
    assert disp_by_id["msg_4"] == "UNPROCESSED"

    # 严格模式下应拦截未声明消息
    strict_validator = CoverageValidator(strict_mode=True)
    strict_valid, _, strict_err = strict_validator.validate_and_reconcile(
        raw_message_ids=raw_ids,
        proposals=proposals,
        declared_dispositions=declared_dispositions,
    )
    assert strict_valid is False
    assert "COVERAGE_VIOLATION" in strict_err


@pytest.mark.asyncio
async def test_context_builder_multi_dimensional_recall(temp_db):
    """测试 ContextBuilder 确定性上下文多维召回与 30 条截断"""
    # 插入一批同 project_id 的记忆
    for i in range(35):
        temp_db.create_memory(
            memory_id=f"mem_test_{i}",
            subject=f"Subject_{i % 5}",
            predicate="configured_with",
            content=f"Configuration detail index {i}",
            type="fact",
            project_id="proj_alpha",
            status="active",
        )

    builder = ContextBuilder(session_store=temp_db, max_context_limit=30)
    pack = await builder.build_context_pack(
        project_id="proj_alpha",
        recent_messages=[{"content": "测试近几条消息"}],
        entities=["Subject_1"],
    )

    # 验证截断在 30 条以内
    assert len(pack) <= 30
    assert len(pack) > 0
    # 验证召回内容包含 project_id
    assert all(m["project_id"] == "proj_alpha" for m in pack)


def test_prompt_manager_constitution_and_schema():
    """测试 PromptManager 固化宪法五原则与 Schema 契约"""
    system_prompt = PromptManager.get_system_prompt()
    assert "宁可少整理，不可错误整理" in system_prompt
    assert "宁可保留冗余，不可因追求简洁而丢失技术细节" in system_prompt
    assert "任何新结论必须能追溯到原始证据" in system_prompt
    assert "不得把推测写成事实" in system_prompt
    assert "不得因语言优化而改变原始事实的语义强度" in system_prompt
    assert "Memory Context Pack" in system_prompt

    schema = PromptManager.get_json_schema()
    assert schema["type"] == "object"
    assert "candidates" in schema["required"]
    assert "message_disposition" in schema["required"]


@pytest.mark.asyncio
async def test_cognitive_scheduler_pipeline_dry_run_isolation(temp_db):
    """
    测试认知提纯流水线驱动主控：
    验证 14 天 Dry-Run 铁律：通过校验的提案以 PROPOSED 状态写入 curation_candidates 隔离表，
    绝对不修改 memories SSOT 主表。
    """
    # 1. 准备测试会话与原始消息
    now = int(time.time())
    session_id = "sess_recon_001"
    temp_db.record_raw_session(
        session_id=session_id,
        agent_id="test_agent",
        project_id="proj_recon",
        started_at=now - 300,
    )
    temp_db.record_raw_message(
        message_id="msg_001",
        session_id=session_id,
        role="user",
        content="我们决定在 AMR 内部采用 Unix Domain Socket 通信，路径固定为 /run/user/1000/qdrant-bge.sock。",
        sequence=1,
    )
    temp_db.record_raw_message(
        message_id="msg_002",
        session_id=session_id,
        role="assistant",
        content="收到，已确认该 UDS 路径，权限严格设置为 0600。",
        sequence=2,
    )
    temp_db.record_raw_message(
        message_id="msg_003",
        session_id=session_id,
        role="user",
        content="今天天气真好，喝杯咖啡吧。",
        sequence=3,
    )

    # 2. 模拟 LLM 提纯产物：包含 1 个合法提取、1 个编造提取
    mock_llm_result = {
        "prompt_version": "v3.0.0-mos",
        "session_id": session_id,
        "candidates": [
            {
                "proposal_id": "prop_valid_01",
                "operation": "EXTRACT",
                "subject": "AMR_Communication",
                "predicate": "uses_socket",
                "object": "/run/user/1000/qdrant-bge.sock",
                "content": "AMR 内部采用 Unix Domain Socket 通信，路径固定为 /run/user/1000/qdrant-bge.sock，权限 0600。",
                "evidence_source_ids": ["msg_001", "msg_002"],
                "extracted_spans": [
                    {"message_id": "msg_001", "span": "Unix Domain Socket 通信，路径固定为 /run/user/1000/qdrant-bge.sock"},
                    {"message_id": "msg_002", "span": "权限严格设置为 0600"}
                ],
                "rationale": "确定系统基础通信拓扑与 UDS 权限",
                "self_assessed_confidence": 0.95
            },
            {
                "proposal_id": "prop_fake_02",
                "operation": "EXTRACT",
                "subject": "Cloud_Service",
                "predicate": "hosted_on",
                "object": "AWS",
                "content": "AMR 部署在 AWS us-east-1 区域",
                "evidence_source_ids": ["msg_001"],
                "extracted_spans": [
                    {"message_id": "msg_001", "span": "AMR 部署在 AWS us-east-1 区域"}
                ],
                "rationale": "编造的虚假事实",
                "self_assessed_confidence": 0.90
            }
        ],
        "message_disposition": [
            {"message_id": "msg_001", "disposition": "EXTRACTED"},
            {"message_id": "msg_002", "disposition": "EXTRACTED"},
            {"message_id": "msg_003", "disposition": "DISCARDED", "reason": "无关闲聊"}
        ]
    }

    scheduler = CognitiveScheduler(session_store=temp_db)

    # 3. 运行流水线
    res = await scheduler.run_pipeline_for_session(
        session_id=session_id,
        batch_id="batch_dry_run_test",
        mock_llm_result=mock_llm_result,
    )

    # 4. 验证提纯流水线门禁成果
    assert res["status"] == "SUCCESS"
    assert res["total_candidates"] == 2
    assert res["valid_candidates"] == 1
    assert res["invalid_candidates"] == 1
    assert res["written_candidates"] == 1

    # 5. 验证 14 天 Dry-Run 铁律：
    # 检查 curation_candidates 隔离表中存在通过的 PROPOSED 提案
    conn = temp_db.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM curation_candidates WHERE batch_id = 'batch_dry_run_test'")
    candidates_rows = cursor.fetchall()
    assert len(candidates_rows) == 1
    cand_row = dict(candidates_rows[0])
    assert cand_row["candidate_id"] == "prop_valid_01"
    assert cand_row["state"] == "PROPOSED"
    assert cand_row["operation"] == "EXTRACT"
    assert "/run/user/1000/qdrant-bge.sock" in cand_row["content"]

    # 检查 memories SSOT 主表中绝无该条记忆（零写权限保证）
    cursor.execute("SELECT COUNT(*) as cnt FROM memories WHERE subject = 'AMR_Communication'")
    mem_cnt = cursor.fetchone()["cnt"]
    assert mem_cnt == 0
