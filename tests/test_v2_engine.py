"""
AMR v2.2 认知记忆引擎单元测试套件
验证 Task 02 任务书全部要求：
1. 实体归一化服务（同义词、大小写、连字符、三元组）；
2. 五大核心 API 契约（extract, merge, update, retrieve, forget）；
3. 三级混合合并防语义漂移（三元组强仲裁、反义/否定词对抗拦截）；
4. 跨 Session 贝叶斯证据累加数学结果；
5. 状态机 DAG 拓扑流转与 conflict_policy 解耦；
6. 写后读一致性 (SQLite 60s pending 记忆合并) 与复合重排打分；
7. Transactional Outbox Worker 异步消费与同步至 Qdrant。
"""

import asyncio
import os
import shutil
import tempfile
import time
from unittest.mock import AsyncMock, MagicMock
import pytest
from qdrant_client import QdrantClient

from config.settings import AppConfig
from src.core.qdrant import QdrantManager
from src.core.session_store import SessionStore
from src.service.cognitive_engine import CognitiveEngine
from src.service.entity_normalizer import EntityNormalizer
from src.service.outbox_worker import OutboxWorker


@pytest.fixture
def test_env():
    """准备单测沙盒环境 (内存 Qdrant + 临时 SQLite + Mock 向量引擎)"""
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test_v2_engine.db")

    config = AppConfig()
    config.storage.sqlite_path = db_path
    config.storage.wal_enabled = True
    config.storage.busy_timeout = 5000

    # 1. 内存 QdrantClient
    memory_qdrant_client = QdrantClient(":memory:")
    qdrant_manager = QdrantManager(config=config, client=memory_qdrant_client, auto_init_collections=True)

    # 2. SQLite SessionStore
    store = SessionStore(config=config)

    # 3. Mock 向量推理引擎 (避免跑完整真实模型加速单测)
    mock_engine = MagicMock()
    # 模拟 1024 维向量
    mock_vector = [0.05] * 1024
    mock_engine.embed = AsyncMock(side_effect=lambda texts: [mock_vector for _ in texts])

    # 4. 实体归一化服务
    normalizer = EntityNormalizer()

    # 5. 认知治理引擎
    cognitive_engine = CognitiveEngine(
        session_store=store,
        qdrant_manager=qdrant_manager,
        engine=mock_engine,
        normalizer=normalizer,
        config=config,
    )

    # 6. Outbox Worker
    worker = OutboxWorker(
        session_store=store,
        qdrant_manager=qdrant_manager,
        engine=mock_engine,
        config=config,
    )

    yield {
        "store": store,
        "qdrant": qdrant_manager,
        "engine": mock_engine,
        "normalizer": normalizer,
        "cognitive": cognitive_engine,
        "worker": worker,
        "db_path": db_path,
    }

    store.close()
    shutil.rmtree(temp_dir, ignore_errors=True)


# =========================================================================
# 1. 实体归一化服务测试
# =========================================================================
def test_entity_normalizer():
    normalizer = EntityNormalizer()

    # 1. 别名归一化
    assert normalizer.normalize_entity("美式") == "americano_coffee"
    assert normalizer.normalize_entity("Americano") == "americano_coffee"
    assert normalizer.normalize_entity("美式咖啡") == "americano_coffee"
    assert normalizer.normalize_entity("AEP-TSA") == "aep_tsa"
    assert normalizer.normalize_entity("aep_tsa") == "aep_tsa"

    # 2. 谓词归一化
    assert normalizer.normalize_predicate("喜欢") == "prefers"
    assert normalizer.normalize_predicate("love") == "prefers"
    assert normalizer.normalize_predicate("绑定端口") == "binds_port"

    # 3. 动态注册别名
    normalizer.register_alias("拿铁咖啡", "latte_coffee")
    assert normalizer.normalize_entity("拿铁咖啡") == "latte_coffee"

    # 4. 三元组归一化
    norm_triple = normalizer.normalize_triple(
        subject=" 用户 ",
        predicate=" 偏好 ",
        object_=" 美式 ",
    )
    assert norm_triple == {
        "subject": "user",
        "predicate": "prefers",
        "object": "americano_coffee",
    }


# =========================================================================
# 2. extract: 候选抽取与晋升门槛
# =========================================================================
def test_extract_and_promotion_threshold(test_env):
    cog = test_env["cognitive"]
    store = test_env["store"]

    # 预先录入 raw_session 和 raw_messages 满足外键要求
    store.record_raw_session("s1", "agent1", "general")
    store.record_raw_session("s2", "agent1", "general")
    store.record_raw_message("m1", "s1", "user", "msg1", 1)
    store.record_raw_message("m2", "s1", "user", "msg2", 2)
    store.record_raw_message("m3", "s2", "user", "msg3", 1)

    # 1. 未达到门槛：confidence < 0.90 且 mention_count < 2 -> status 必须为 candidate
    mem1 = cog.extract_candidate(
        subject="用户",
        predicate="偏好",
        object_="美式",
        content="用户平时喜欢喝美式咖啡",
        type="preference",
        confidence=0.85,
        evidence=[{"message_id": "m1", "session_id": "s1", "evidence_strength": 0.5}],
    )
    assert mem1["status"] == "candidate"
    assert mem1["subject"] == "user"
    assert mem1["predicate"] == "prefers"
    assert mem1["object"] == "americano_coffee"

    # 2. 达到晋升门槛：confidence >= 0.90 AND mention_count >= 2 -> status 自动晋升 active
    mem2 = cog.extract_candidate(
        subject="aep-tsa",
        predicate="绑定端口",
        object_="8080",
        content="AEP-TSA服务绑定8080端口",
        type="fact",
        confidence=0.95,
        evidence=[
            {"message_id": "m2", "session_id": "s1", "evidence_strength": 0.8},
            {"message_id": "m3", "session_id": "s2", "evidence_strength": 0.8},
        ],
    )
    assert mem2["status"] == "active"
    assert mem2["subject"] == "aep_tsa"
    assert mem2["predicate"] == "binds_port"

    # 3. 强制晋升 force_promote=True -> 哪怕 confidence 较低也进入 active
    mem3 = cog.extract_candidate(
        subject="user",
        predicate="prefers",
        object_="python",
        content="请务必记住我用Python",
        type="preference",
        confidence=0.7,
        force_promote=True,
    )
    assert mem3["status"] == "active"


# =========================================================================
# 3. merge: 三级混合防语义漂移与贝叶斯累加
# =========================================================================
def test_merge_multi_level_arbitration_and_bayes(test_env):
    cog = test_env["cognitive"]
    store = test_env["store"]

    # 预先录入 raw session 和 messages
    store.record_raw_session("sess_A", "agent1", "general")
    store.record_raw_session("sess_B", "agent2", "general")
    store.record_raw_message("msg_001", "sess_A", "user", "msg1", 1)
    store.record_raw_message("msg_new_sess", "sess_B", "user", "msg2", 1)
    store.record_raw_message("msg_repeat", "sess_B", "user", "msg3", 2)

    # 先创建一个基础记忆
    base_mem = store.create_memory(
        memory_id="mem_coffee_base",
        subject="user",
        predicate="prefers",
        object="americano_coffee",
        content="用户偏好美式咖啡",
        type="preference",
        confidence=0.80,
        mention_count=1,
        status="candidate",
        evidence=[{"message_id": "msg_001", "session_id": "sess_A", "evidence_strength": 0.6}],
    )

    # 1. Level 1 测试：主语/谓词/宾语互斥禁止合并
    res_l1 = cog.merge_memory(
        existing_memory_id="mem_coffee_base",
        new_subject="user",
        new_predicate="prefers",
        new_object="latte_coffee",  # 宾语冲突 (拿铁 vs 美式)
        new_content="用户偏好拿铁咖啡",
    )
    assert res_l1["merged"] is False
    assert "Level 1" in res_l1["reason"]

    # 2. Level 2 测试：否定词对抗拦截
    res_l2 = cog.merge_memory(
        existing_memory_id="mem_coffee_base",
        new_subject="用户",
        new_predicate="喜欢",
        new_object="美式",
        new_content="用户不再偏好美式咖啡，严禁推荐美式",  # 含有否定词与严禁
    )
    assert res_l2["merged"] is False
    assert "Level 2" in res_l2["reason"]

    # 3. Level 3 测试：独立会话跨 Session 贝叶斯证据累加
    # 原 confidence = 0.80, evidence_strength = 0.50
    # 预期: confidence_new = 1 - (1 - 0.80) * (1 - 0.50) = 1 - 0.20 * 0.50 = 0.90
    res_l3 = cog.merge_memory(
        existing_memory_id="mem_coffee_base",
        new_subject="用户",
        new_predicate="偏好",
        new_object="美式咖啡",
        new_content="用户确实喜欢美式咖啡",
        evidence={"message_id": "msg_new_sess", "session_id": "sess_B"},
        evidence_strength=0.50,
    )
    assert res_l3["merged"] is True
    updated = res_l3["memory"]
    assert updated["confidence"] == 0.90
    assert updated["mention_count"] == 2
    # 达到 0.90 且 mention_count == 2，原 candidate 应当已晋升为 active
    assert updated["status"] == "active"

    # 4. 同 Session 重复提及不叠加贝叶斯置信度（防刷），仅累加 mention_count
    res_same = cog.merge_memory(
        existing_memory_id="mem_coffee_base",
        new_subject="user",
        new_predicate="prefers",
        new_object="americano_coffee",
        new_content="用户再次提及喜欢美式咖啡",
        evidence={"message_id": "msg_repeat", "session_id": "sess_B"},  # 同为 sess_B
        evidence_strength=0.60,
    )
    assert res_same["merged"] is True
    updated_same = res_same["memory"]
    assert updated_same["confidence"] == 0.90  # 保持 0.90，未被刷高
    assert updated_same["mention_count"] == 3


# =========================================================================
# 4. update: 状态机 DAG 与 conflict_policy 正交解耦
# =========================================================================
def test_update_with_conflict_policy(test_env):
    cog = test_env["cognitive"]
    store = test_env["store"]

    # 1. task 状态机流转 (pending -> in_progress 合法, pending -> completed 非法)
    task_mem = store.create_memory(
        memory_id="task_001",
        subject="etl_job",
        predicate="status_is",
        object="pending",
        content="ETL Job is pending",
        type="task",
        conflict_policy="state_machine",
    )

    # 尝试非法流转: pending -> completed
    res_illegal = cog.update_with_conflict_policy(
        existing_memory_id="task_001",
        new_content="ETL Job completed",
        new_task_status="completed",
    )
    assert res_illegal["success"] is False
    assert "State machine violation" in res_illegal["reason"]

    # 合法流转: pending -> in_progress
    res_legal = cog.update_with_conflict_policy(
        existing_memory_id="task_001",
        new_content="ETL Job is now running",
        new_task_status="in_progress",
    )
    assert res_legal["success"] is True
    new_task = res_legal["new_memory"]
    assert new_task["object"] == "in_progress"
    assert new_task["version"] == 2
    # 原 task 应该已变为 superseded
    old_task = store.get_memory("task_001")
    assert old_task["status"] == "superseded"
    assert old_task["superseded_by"] == new_task["memory_id"]

    # 2. episode 记忆不可篡改 (immutable)
    ep_mem = store.create_memory(
        memory_id="ep_001",
        subject="meeting_2026",
        predicate="occurred_at",
        content="Important architectural review meeting occurred",
        type="episode",
        conflict_policy="immutable",
    )
    res_ep = cog.update_with_conflict_policy(
        existing_memory_id="ep_001",
        new_content="Modified meeting record",
    )
    assert res_ep["success"] is False
    assert "Policy 'immutable'" in res_ep["reason"]

    # 3. decision overwrite 策略：版本递增与 supersede
    dec_mem = store.create_memory(
        memory_id="dec_001",
        subject="db_choice",
        predicate="selected",
        content="Selected PostgreSQL",
        type="decision",
        conflict_policy="overwrite",
        version=1,
    )
    res_dec = cog.update_with_conflict_policy(
        existing_memory_id="dec_001",
        new_content="Selected SQLite SSOT instead",
        new_object="sqlite",
    )
    assert res_dec["success"] is True
    new_dec = res_dec["new_memory"]
    assert new_dec["version"] == 2
    assert new_dec["content"] == "Selected SQLite SSOT instead"


# =========================================================================
# 5. retrieve: 服务端硬过滤、写后读一致性与复合重排
# =========================================================================
@pytest.mark.asyncio
async def test_retrieve_ryow_and_composite_ranking(test_env):
    cog = test_env["cognitive"]
    store = test_env["store"]

    # 1. 模拟在 SQLite 写入了一条 active 记忆（此时尚未同步至 Qdrant，qdrant_sync_queue 处于 pending）
    mem_pending = store.create_memory(
        memory_id="mem_ryow_001",
        subject="service_endpoint",
        predicate="url",
        object="https://amr.local",
        content="AMR service runs on https://amr.local",
        type="fact",
        importance=0.9,
        confidence=0.95,
        status="active",
        project_id="general",
    )

    # 2. 检索：即使 Qdrant 里还没有该点位，由于写后读一致性（Read-Your-Own-Writes），它应当从 SQLite pending 队列被合并召回
    results = await cog.retrieve(
        query="Where does AMR service run?",
        project_id="general",
        limit=5,
    )

    assert len(results) >= 1
    found = [r for r in results if r["memory_id"] == "mem_ryow_001"]
    assert len(found) == 1
    hit = found[0]

    # 验证响应体同时返回 final_score 与 vector_score (契约兼容)
    assert "final_score" in hit
    assert "vector_score" in hit
    assert hit["final_score"] > 0
    assert hit["status"] == "active"


# =========================================================================
# 6. forget: 合规安全擦除与审计
# =========================================================================
def test_forget_and_audit(test_env):
    cog = test_env["cognitive"]
    store = test_env["store"]

    mem = store.create_memory(
        memory_id="mem_forget_me",
        subject="user_token",
        predicate="secret_key",
        content="sensitive auth secret",
        type="fact",
        status="active",
    )

    # 执行 forget
    success = cog.forget(
        memory_id="mem_forget_me",
        deleted_by="security_officer",
        reason="privacy compliance GDPR",
    )
    assert success is True

    # 验证 SQLite 状态更新
    forgotten = store.get_memory("mem_forget_me")
    assert forgotten["status"] == "deleted"
    assert forgotten["deleted_by"] == "security_officer"
    assert forgotten["deletion_reason"] == "privacy compliance GDPR"
    assert forgotten["deleted_at"] is not None

    # 验证 Outbox 产生 delete 任务
    tasks = store.fetch_pending_sync_tasks()
    delete_tasks = [t for t in tasks if t["memory_id"] == "mem_forget_me" and t["op_type"] == "delete"]
    assert len(delete_tasks) == 1

    # 验证审计日志记录
    audits = store.get_audit_logs("mem_forget_me")
    actions = [a["action"] for a in audits]
    assert "delete" in actions


# =========================================================================
# 7. Transactional Outbox Worker 异步同步守护测试
# =========================================================================
@pytest.mark.asyncio
async def test_outbox_worker_sync_cycle(test_env):
    worker = test_env["worker"]
    store = test_env["store"]
    qdrant = test_env["qdrant"]

    # 创建一条待同步记忆
    mem = store.create_memory(
        memory_id="mem_sync_test",
        subject="worker_test",
        predicate="status",
        object="testing",
        content="Testing Outbox Worker synchronization",
        type="fact",
        status="active",
    )

    pending_tasks = store.fetch_pending_sync_tasks()
    assert len(pending_tasks) >= 1

    # 执行一次单轮同步
    processed = await worker.run_once()
    assert processed >= 1

    # 验证发件箱任务已标记为 synced (不会再被 fetch_pending_sync_tasks 拉取)
    remaining_tasks = store.fetch_pending_sync_tasks()
    assert not any(t["memory_id"] == "mem_sync_test" for t in remaining_tasks)

    # 验证 Qdrant 中已插入点位
    point_rec = qdrant.get_point_by_memory_id("mem_sync_test")
    assert point_rec is not None
    assert point_rec["payload"]["subject"] == "worker_test"

    # 测试物理删除同步
    store.update_memory_status(
        memory_id="mem_sync_test",
        status="deleted",
        deleted_by="tester",
        deletion_reason="cleanup",
    )
    # 处理 delete 任务
    processed_del = await worker.run_once()
    assert processed_del >= 1

    # 验证 Qdrant 中点位已被物理删除
    point_after_del = qdrant.get_point_by_memory_id("mem_sync_test")
    assert point_after_del is None


# =========================================================================
# 8. Outbox Worker 失败重试上限与网络自愈
# =========================================================================
@pytest.mark.asyncio
async def test_outbox_worker_retry_and_dead_letter(test_env):
    worker = test_env["worker"]
    store = test_env["store"]
    qdrant = test_env["qdrant"]

    mem = store.create_memory(
        memory_id="mem_retry_test",
        subject="network",
        predicate="timeout",
        content="Testing retry failure and dead-letter",
        type="fact",
        status="active",
    )

    # 模拟 Qdrant 发生网络异常
    orig_upsert = qdrant.upsert_points
    try:
        qdrant.upsert_points = MagicMock(side_effect=RuntimeError("Simulated network timeout"))

        # 第 1 次处理应该失败并记录 retry_count=1
        processed = await worker.run_once()
        assert processed == 0

        pending = store.fetch_pending_sync_tasks()
        task = [t for t in pending if t["memory_id"] == "mem_retry_test"][0]
        assert task["status"] == "failed"
        assert task["retry_count"] == 1
        assert "Simulated network timeout" in task["last_error"]

        # 连续失败直到上限 5 次
        for _ in range(4):
            await worker.run_once()

        # 此时重试达到 5 次，转入死信状态，不再被拉取
        pending_after = store.fetch_pending_sync_tasks()
        assert not any(t["memory_id"] == "mem_retry_test" for t in pending_after)

    finally:
        qdrant.upsert_points = orig_upsert


# =========================================================================
# 9. Outbox Worker 启动与停止生命周期
# =========================================================================
def test_outbox_worker_start_stop(test_env):
    worker = test_env["worker"]
    worker.start()
    assert worker._running is True
    assert worker._thread is not None
    assert worker._thread.is_alive()

    worker.stop(timeout=1.0)
    assert worker._running is False

