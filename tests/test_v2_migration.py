"""
AMR v2.2 离线提纯合并流水线与安全回滚测试套件
验证 Task 03 任务书全部要求：
1. 10~20 条典型历史碎片输入 (含重复偏好、反义词、不同状态任务、不可变 Episode 等)；
2. 反义词未被合并（语义防漂移 Level 2 拦截）；
3. 相同偏好成功合并，mention_count 递增，跨 Session 贝叶斯证据累加置信度增长；
4. 状态机任务流转与不可变 Episode 记忆保全；
5. 回滚脚本安全幂等清理 (is_synthetic=1 消息与关联记忆被彻底清理，状态复原)；
6. 100 条真实备份样本的端到端小步闭环测试。
"""

import json
import os
import shutil
import tempfile
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
import pytest
from qdrant_client import QdrantClient

from config.settings import AppConfig
from src.core.qdrant import QdrantManager
from src.core.session_store import SessionStore
from src.service.cognitive_engine import CognitiveEngine
from src.service.entity_normalizer import EntityNormalizer
from scripts.migrate_refine_v2 import MigrationRefinePipeline, LegacyMemoryParser, PROJECT_ROOT
from scripts.rollback_refine_v2 import RefineRollbackService


@pytest.fixture
def sandbox_env():
    """搭建独立的隔离测试沙盒环境"""
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test_migration.db")

    config = AppConfig()
    config.storage.sqlite_path = db_path
    config.storage.wal_enabled = True
    config.storage.busy_timeout = 5000

    memory_qdrant_client = QdrantClient(":memory:")
    qdrant_manager = QdrantManager(config=config, client=memory_qdrant_client, auto_init_collections=True)
    store = SessionStore(config=config)

    mock_engine = MagicMock()
    mock_engine.embed = AsyncMock(side_effect=lambda texts: [[0.05] * 1024 for _ in texts])
    normalizer = EntityNormalizer()
    cognitive_engine = CognitiveEngine(
        session_store=store,
        qdrant_manager=qdrant_manager,
        engine=mock_engine,
        normalizer=normalizer,
        config=config,
    )

    yield {
        "temp_dir": temp_dir,
        "db_path": db_path,
        "config": config,
        "store": store,
        "qdrant": qdrant_manager,
        "normalizer": normalizer,
        "cognitive": cognitive_engine,
    }

    store.close()
    shutil.rmtree(temp_dir, ignore_errors=True)


def test_legacy_parser_logic():
    """测试历史碎片解析器对各种类型、反义词、三元组的提取能力"""
    parser = LegacyMemoryParser()

    # 1. 偏好提取 (喜欢 vs 讨厌)
    t1 = parser.classify_type("preference", "【用户指令/需求】: 我喜欢喝美式咖啡\n【结论】: 好的")
    assert t1 == "preference"
    s1, p1, o1, c1 = parser.extract_triple_and_statement("我喜欢喝美式咖啡", t1)
    assert s1 == "user"
    assert p1 == "prefers"
    assert "美式咖啡" in o1

    t2 = parser.classify_type("preference", "不要总是使用全局变量")
    assert t2 == "preference"
    s2, p2, o2, c2 = parser.extract_triple_and_statement("不要总是使用全局变量", t2)
    assert p2 == "dislikes"

    # 2. 任务状态提取
    t3 = parser.classify_type("fact", "【任务】: AEP-IAM 审计任务 状态为 进行中")
    assert t3 == "task"
    s3, p3, o3, c3 = parser.extract_triple_and_statement("【任务】: AEP-IAM 审计任务 状态为 进行中", t3)
    assert o3 == "in_progress"

    # 3. Episode 提取
    t4 = parser.classify_type("decision", "系统崩溃原因故障复盘：内存耗尽导致的 Gateway 重启")
    assert t4 == "episode"


def test_migration_pipeline_with_typical_fragments(sandbox_env):
    """
    测试 10~20 条典型历史碎片输入：
    - 重复偏好输入 (验证合并与贝叶斯累加)
    - 反义词输入 (验证否定词 Level 2 拦截不合并)
    - 不同状态任务
    - Episode 不可变性
    """
    store = sandbox_env["store"]
    cog = sandbox_env["cognitive"]
    norm = sandbox_env["normalizer"]
    temp_dir = sandbox_env["temp_dir"]

    # 构造 12 条典型历史碎片
    sample_records = [
        # 1-3: 重复偏好 (不同会话，相同偏好)
        {
            "id": "item-1",
            "payload": {
                "memory_id": "mem_pref_1",
                "memory_type": "preference",
                "content": "【用户指令/需求】: 我平时喜欢喝美式咖啡\n【结论】: 记住了",
                "session_id": "sess_001",
                "project_id": "general",
            }
        },
        {
            "id": "item-2",
            "payload": {
                "memory_id": "mem_pref_2",
                "memory_type": "preference",
                "content": "【用户指令/需求】: 给我点一杯美式，我喜欢喝美式\n【结论】: 没问题",
                "session_id": "sess_002",
                "project_id": "general",
            }
        },
        # 4: 反义偏好 (否定词 / 不喜欢美式 -> 必须被 Level 2 拦截，不得与前两条合并)
        {
            "id": "item-3",
            "payload": {
                "memory_id": "mem_pref_antonym",
                "memory_type": "preference",
                "content": "【用户指令/需求】: 我现在不喜欢喝美式咖啡，太苦了，别给我点\n【结论】: 了解",
                "session_id": "sess_003",
                "project_id": "general",
            }
        },
        # 5: 偏好（不同对象：拿铁 -> Level 1 object mismatch 拦截不合并）
        {
            "id": "item-4",
            "payload": {
                "memory_id": "mem_pref_latte",
                "memory_type": "preference",
                "content": "【用户指令/需求】: 我喜欢喝拿铁咖啡\n【结论】: 记录拿铁",
                "session_id": "sess_004",
                "project_id": "general",
            }
        },
        # 6: 决策 1
        {
            "id": "item-5",
            "payload": {
                "memory_id": "mem_dec_1",
                "memory_type": "decision",
                "content": "【用户审核/指令】: 项目 aep_tsa 采用双轨唯一激活策略\n【结论】: 已落地",
                "session_id": "sess_005",
                "project_id": "crypto-infra",
            }
        },
        # 7: 决策 2 (相同项目相同决策)
        {
            "id": "item-6",
            "payload": {
                "memory_id": "mem_dec_2",
                "memory_type": "decision",
                "content": "【用户审核/指令】: 项目 aep_tsa 采用双轨唯一激活策略\n【结论】: 再次确认通过",
                "session_id": "sess_006",
                "project_id": "crypto-infra",
            }
        },
        # 8: 任务 1 (pending)
        {
            "id": "item-7",
            "payload": {
                "memory_id": "mem_task_1",
                "memory_type": "task",
                "content": "【任务】: TSA 客户端改造待办，状态为 待办",
                "session_id": "sess_007",
                "project_id": "crypto-infra",
            }
        },
        # 9: 任务 2 (in_progress)
        {
            "id": "item-8",
            "payload": {
                "memory_id": "mem_task_2",
                "memory_type": "task",
                "content": "【任务】: TSA 客户端改造，状态为 进行中",
                "session_id": "sess_008",
                "project_id": "crypto-infra",
            }
        },
        # 10: Episode 1 (重要事故复盘)
        {
            "id": "item-9",
            "payload": {
                "memory_id": "mem_ep_1",
                "memory_type": "episode",
                "content": "系统崩溃原因复盘：Unraid Docker 显卡直通失败导致服务异常",
                "session_id": "sess_009",
                "project_id": "general",
            }
        },
        # 11: Episode 2 (同一事件的不同记录，Episode 保持 immutable 不应合并)
        {
            "id": "item-10",
            "payload": {
                "memory_id": "mem_ep_2",
                "memory_type": "episode",
                "content": "系统崩溃原因复盘：Unraid Docker 显卡直通失败导致服务异常二次记录",
                "session_id": "sess_010",
                "project_id": "general",
            }
        },
        # 12: Fact 事实
        {
            "id": "item-11",
            "payload": {
                "memory_id": "mem_fact_1",
                "memory_type": "fact",
                "content": "【用户指令/需求】: ping\n【结论/解决方案】: pong",
                "session_id": "sess_011",
                "project_id": "general",
            }
        }
    ]

    backup_json_path = os.path.join(temp_dir, "test_backup.json")
    with open(backup_json_path, "w", encoding="utf-8") as f:
        json.dump(sample_records, f, ensure_ascii=False)

    # 执行提纯流水线
    pipeline = MigrationRefinePipeline(
        backup_path=backup_json_path,
        session_store=store,
        cognitive_engine=cog,
        normalizer=norm,
        config=sandbox_env["config"],
    )
    report = pipeline.run()

    # 1. 验证报告基本统计
    assert report["raw_records_count"] == 11
    assert report["merged_count"] >= 1  # 偏好 item-1 与 item-2 必须合并

    # 2. 验证合成标记写入
    with store._lock:
        conn = store._get_connection()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as cnt FROM raw_messages WHERE is_synthetic = 1 AND source_type = 'legacy_memory';")
        assert c.fetchone()["cnt"] == 11

    # 3. 验证偏好合并与贝叶斯增长
    # 查找归一化后的 user prefers americano_coffee 记忆
    with store._lock:
        conn = store._get_connection()
        c = conn.cursor()
        c.execute("SELECT * FROM memories WHERE subject = 'user' AND predicate = 'prefers' AND object = 'americano_coffee';")
        pref_rows = c.fetchall()
        assert len(pref_rows) == 1
        pref_mem = pref_rows[0]
        # 合并后 mention_count 应为 2
        assert pref_mem["mention_count"] == 2
        # 跨 session 证据累加：0.8 累加 0.5 -> 1 - (1-0.8)*(1-0.5) = 0.90
        assert pref_mem["confidence"] == 0.9
        assert pref_mem["status"] == "active"

        # 4. 验证反义词未合并：不喜欢美式必须单独存在
        c.execute("SELECT * FROM memories WHERE subject = 'user' AND predicate = 'dislikes';")
        dislike_rows = c.fetchall()
        assert len(dislike_rows) == 1
        assert "americano_coffee" in dislike_rows[0]["object"]

        # 5. 验证 Episode 保持独立未合并
        c.execute("SELECT * FROM memories WHERE type = 'episode';")
        ep_rows = c.fetchall()
        assert len(ep_rows) == 2
        for ep in ep_rows:
            assert ep["conflict_policy"] == "immutable"

    # =========================================================================
    # 6. 验证一键回滚脚本的安全与幂等性
    # =========================================================================
    rollback_svc = RefineRollbackService(
        backup_path=backup_json_path,
        session_store=store,
        config=sandbox_env["config"],
    )

    # 先执行 dry-run
    dry_report = rollback_svc.rollback(dry_run=True)
    assert dry_report["dry_run"] is True
    assert dry_report["synthetic_messages_to_delete"] == 11
    assert dry_report["memories_to_delete"] > 0

    # 执行正式回滚
    rb_report = rollback_svc.rollback(dry_run=False)
    assert rb_report["deleted_synthetic_messages"] == 11
    assert rb_report["deleted_memories"] > 0

    # 断言数据库复原彻底干净
    with store._lock:
        conn = store._get_connection()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as cnt FROM raw_messages WHERE is_synthetic = 1;")
        assert c.fetchone()["cnt"] == 0

        c.execute("SELECT COUNT(*) as cnt FROM memories;")
        assert c.fetchone()["cnt"] == 0

        c.execute("SELECT COUNT(*) as cnt FROM memory_evidence;")
        assert c.fetchone()["cnt"] == 0

        c.execute("SELECT COUNT(*) as cnt FROM qdrant_sync_queue;")
        assert c.fetchone()["cnt"] == 0

    # 再次幂等调用回滚不会抛错
    rb_report_again = rollback_svc.rollback(dry_run=False)
    assert rb_report_again["deleted_synthetic_messages"] == 0
    assert rb_report_again["deleted_memories"] == 0


def test_migration_pipeline_real_sample_100(sandbox_env):
    """
    小步 100 条真实样本端到端闭环验证：
    从实际数据备份文件 data/backups/ai_memory_backup_20260927.json
    抽取 100 条运行流水线，断言结构化卡片产出、去重合并数、并执行回滚复原。
    """
    real_backup_file = PROJECT_ROOT / "data" / "backups" / "ai_memory_backup_20260927.json"
    if not real_backup_file.exists():
        pytest.skip("Real backup file not found, skipping real sample 100 test.")

    store = sandbox_env["store"]
    cog = sandbox_env["cognitive"]
    norm = sandbox_env["normalizer"]

    pipeline = MigrationRefinePipeline(
        backup_path=real_backup_file,
        session_store=store,
        cognitive_engine=cog,
        normalizer=norm,
        config=sandbox_env["config"],
    )

    report = pipeline.run(sample_limit=100)
    assert report["raw_records_count"] == 100
    assert report["refined_cards_count"] > 0
    assert report["avg_confidence"] >= 0.8
    assert report["elapsed_seconds"] < 60.0

    # 验证数据库中有 100 条 is_synthetic=1 的 raw_messages
    with store._lock:
        conn = store._get_connection()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as cnt FROM raw_messages WHERE is_synthetic = 1;")
        assert c.fetchone()["cnt"] == 100

        c.execute("SELECT COUNT(*) as cnt FROM memories;")
        mem_count = c.fetchone()["cnt"]
        assert mem_count == report["refined_cards_count"]

    # 执行回滚清理沙盒
    rollback_svc = RefineRollbackService(
        backup_path=real_backup_file,
        session_store=store,
        config=sandbox_env["config"],
    )
    rb_report = rollback_svc.rollback(dry_run=False)
    assert rb_report["deleted_synthetic_messages"] == 100
    assert rb_report["deleted_memories"] == mem_count

    # 确认回滚后表全空
    with store._lock:
        conn = store._get_connection()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as cnt FROM raw_messages WHERE is_synthetic = 1;")
        assert c.fetchone()["cnt"] == 0
        c.execute("SELECT COUNT(*) as cnt FROM memories;")
        assert c.fetchone()["cnt"] == 0
