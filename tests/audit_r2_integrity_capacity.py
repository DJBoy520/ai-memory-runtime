"""
Red-Team Audit Track R2: Data Integrity, Injection, Boundary & Capacity Tests
验证 AMR v5 数据完整性、SQL 参数化、畸形 JSON 容错与容量时序：
1. 参数化 SQL 边界字符（引号、分号、NUL 字节、多字节 Unicode、SQL 注入载荷）
2. JSON 字段容错与 [缺陷复现]: 目标记忆 conflicts_with 存在合法非列表 JSON（如 int/dict）时导致 create/update 崩溃
3. 大正文长度边界（min 5 chars, max 16000 chars）
4. 非默认 collection 全链路贯通（SQLite memories -> outbox -> Qdrant 目标集合）
5. 批量 200 次更新合并与 200 独立实体 batch_size=10 单轮处理上限
6. outbox_lag_seconds 计算与随时间推移的准确性
"""

import asyncio
import json
import time
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest

from src.core.session_store import SessionStore
from src.service.metrics import METRICS
from src.service.outbox_worker import OutboxWorker


class FakeAuditEngine:
    def __init__(self, dim: int = 16):
        self.dim = dim

    async def embed(self, texts: List[str]) -> List[List[float]]:
        return [[0.05] * self.dim for _ in texts]


class FakeAuditQdrant:
    def __init__(self):
        self.upsert_records: List[Dict[str, Any]] = []
        self.delete_records: List[Dict[str, Any]] = []
        self.client = MagicMock()

    def upsert_points(self, collection_name: str, points: List[Any], wait: bool = True):
        for p in points:
            self.upsert_records.append({"collection": collection_name, "id": p.id, "payload": p.payload})

    def delete_point_by_id(self, point_id: str, collection_name: str = "ai_memory", wait: bool = True):
        self.delete_records.append({"collection": collection_name, "point_id": point_id})


@pytest.fixture
def store(tmp_path):
    db_file = tmp_path / "audit_integrity.db"
    st = SessionStore(db_path=db_file)
    yield st
    st.close()


def test_sql_parameterization_boundary_characters(store):
    """
    攻击面 6.1: 恶意输入与特殊字符参数化验证
    包含 SQL 注入语句、单双引号、分号、注释符、NUL 字节、多字节 emoji
    断言：底层参数化绑定生效，完全不破坏 SQL 解析，读出数据字节无损。
    """
    dangerous_id = "mem_inj_'\";--/*DROP TABLE*/_\x00_test"
    dangerous_content = (
        "Content with SQL attack: '); DROP TABLE memories; SELECT * FROM users WHERE '1'='1"
        "\nNewlines \r\n and tabs \t and \x00 NUL bytes"
        "\nUnicode: 🚀⚡🤖 复杂的中文测试"
    )

    mem = store.create_memory_v3(
        memory_id=dangerous_id,
        content=dangerous_content,
        type="security/test",
    )
    assert mem["memory_id"] == dangerous_id

    # 查询回读校验
    got = store.get_memory_v3(dangerous_id)
    assert got is not None
    assert got["memory_id"] == dangerous_id
    assert got["content"] == dangerous_content

    # 验证 memory_revisions 表
    revs = store.get_memory_revisions(dangerous_id)
    assert len(revs) == 1
    assert revs[0]["content"] == dangerous_content

    # 验证 outbox 表中正确入队
    tasks = store.fetch_pending_projection_tasks()
    matching_tasks = [t for t in tasks if t["memory_id"] == dangerous_id]
    assert len(matching_tasks) == 1


def test_json_fields_error_tolerance_and_non_list_crash_defect(store):
    """
    攻击面 6.2: JSON 字段容错与非列表 JSON 导致的崩溃缺陷
    1. 畸形非法 JSON 文本: get_memory_v3 降级返回空列表，不抛异常
    2. [真实 Bug 复现]: 当目标记忆的 conflicts_with 被外部存入合法非列表 JSON（如 '123' 或 '{}'）时，
       其它记忆向其声明 conflicts_with，在 create_memory_v3 和 update_memory_v3 中
       由于缺少 isinstance(t_conflicts, list) 检查，直接调用 `in` 或 `.append()` 导致 TypeError / AttributeError 崩溃。
    """
    mem_id = "mem_corrupt_json_001"
    store.create_memory_v3(
        memory_id=mem_id,
        content="Normal content for json test",
    )

    # 手工在 SQLite 中向 source_refs 与 conflicts_with 写入非法非 JSON 字符串
    conn = store.get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE memories SET source_refs = '{unclosed json', conflicts_with = 'not a json' WHERE memory_id = ?;",
        (mem_id,),
    )

    # 1. get_memory_v3 容错
    got = store.get_memory_v3(mem_id)
    assert got["source_refs"] == [], "非法 JSON 应平滑降级为 []"
    assert got["conflicts_with"] == [], "非法 JSON 应平滑降级为 []"

    # 2. 目标记忆中存入了 int 型合法 JSON: '123'
    cur.execute("UPDATE memories SET conflicts_with = '123' WHERE memory_id = ?;", (mem_id,))

    # 新建另一条记忆，并声明 conflicts_with 指向该目标记忆
    # 期望规范: 目标记忆的 conflicts_with 被重置或规范化为列表，双向打标成功
    store.create_memory_v3(
        memory_id="mem_attacker_int",
        content="Attempting conflict with int-corrupted target",
        conflicts_with=[mem_id],
    )
    t_after_int = store.get_memory_v3(mem_id)
    assert "mem_attacker_int" in t_after_int["conflicts_with"]

    # 3. 目标记忆中存入了 dict 型合法 JSON: '{}'
    cur.execute("UPDATE memories SET conflicts_with = '{}' WHERE memory_id = ?;", (mem_id,))
    store.create_memory_v3(
        memory_id="mem_attacker_dict",
        content="Attempting conflict with dict-corrupted target",
        conflicts_with=[mem_id],
    )
    t_after_dict = store.get_memory_v3(mem_id)
    assert "mem_attacker_dict" in t_after_dict["conflicts_with"]


def test_large_content_and_length_boundaries(store):
    """
    攻击面 6.3: 正文字符长度边界与超限防御
    - 4 字符 (<5): 拒绝 (ValueError: Memory content too short)
    - 16000 字符 (最大边界): 正常创建
    - 16001 字符 (>16000): 拒绝 (ValueError: Memory content too long)
    """
    # 1. 太短
    with pytest.raises(ValueError) as exc_short:
        store.create_memory_v3(memory_id="mem_short", content="1234")
    assert "Memory content too short" in str(exc_short.value)

    # 2. 最大允许边界 16,000 字符
    max_content = "A" * 16000
    mem_max = store.create_memory_v3(
        memory_id="mem_max_16k",
        content=max_content,
    )
    assert len(mem_max["content"]) == 16000

    # 3. 超过最大边界 16,001 字符
    too_long_content = "A" * 16001
    with pytest.raises(ValueError) as exc_long:
        store.create_memory_v3(memory_id="mem_too_long", content=too_long_content)
    assert "Memory content too long" in str(exc_long.value)


@pytest.mark.asyncio
async def test_non_default_collection_full_pipeline(store):
    """
    攻击面 6.4: 非默认集合全链路贯通
    - 指定 collection_name='audit_custom_col'
    - memories.collection_name 保存正确
    - memory_projection_outbox.target_collection 路由正确
    - Worker 投影至 Qdrant 时，点位准确进入 'audit_custom_col' 集合
    """
    mem_id = "mem_custom_col_001"
    custom_col = "audit_custom_col"

    mem = store.create_memory_v3(
        memory_id=mem_id,
        content="Testing custom collection routing",
        collection_name=custom_col,
    )
    assert mem["collection_name"] == custom_col

    # 验证 outbox 记录
    tasks = store.fetch_pending_projection_tasks()
    t = [task for task in tasks if task["memory_id"] == mem_id][0]
    assert t["target_collection"] == custom_col

    # Worker 处理
    qdrant = FakeAuditQdrant()
    worker = OutboxWorker(
        session_store=store,
        qdrant_manager=qdrant,
        engine=FakeAuditEngine(),
        batch_size=10,
    )
    processed = await worker.run_once()
    assert processed >= 1

    # 验证 Qdrant 中目标集合完全对齐
    assert len(qdrant.upsert_records) >= 1
    match = next(r for r in qdrant.upsert_records if r["payload"]["memory_id"] == mem_id)
    assert match["collection"] == custom_col


@pytest.mark.asyncio
async def test_batch_200_consecutive_updates_merged_and_processed(store):
    """
    攻击面 7.1: 批量 200 次更新同一记忆
    - revision 单调升至 201
    - outbox 经过部分唯一索引合并，pending 中始终仅保留 1 条任务（最新 revision=201）
    - Worker 仅执行 1 次投影即追平全量变更
    """
    mem_id = "mem_batch_200"
    store.create_memory_v3(
        memory_id=mem_id,
        content="Batch update initial content",
    )

    # 连续执行 200 次更新
    for i in range(1, 201):
        store.update_memory_v3(
            memory_id=mem_id,
            content=f"Batch update content version {i+1}",
            expected_version=i,
            change_reason=f"Step {i}",
        )

    # 验证主表最新状态
    m = store.get_memory_v3(mem_id)
    assert m["version"] == 201
    assert m["projection_revision"] == 201

    # 验证 outbox 队列：恰有 1 条 pending 任务，revision 为 201
    tasks = store.fetch_pending_projection_tasks()
    m_tasks = [t for t in tasks if t["memory_id"] == mem_id]
    assert len(m_tasks) == 1, "200 次连续更新在 pending 队列中必须合并为唯一一条"
    assert m_tasks[0]["projection_revision"] == 201

    # Worker 处理
    qdrant = FakeAuditQdrant()
    worker = OutboxWorker(
        session_store=store,
        qdrant_manager=qdrant,
        engine=FakeAuditEngine(),
    )
    processed = await worker.run_once()
    assert processed == 1

    # 验证 Qdrant 写入最终版本 201
    assert qdrant.upsert_records[0]["payload"]["version"] == 201


@pytest.mark.asyncio
async def test_worker_batch_size_limit_and_lag_seconds(store):
    """
    攻击面 7.2: 200 条独立实体入队与 Worker batch_size=10 上限
    - 创建 200 个独立记忆 -> pending 队列累计 200 条任务
    - Worker 单轮 run_once(batch_size=10) 严格只处理 10 条
    - 剩余 190 条留在队列中
    - 验证 outbox_lag_seconds 指标反映最早 pending 任务的年龄
    """
    n_memories = 200
    for i in range(n_memories):
        store.create_memory_v3(
            memory_id=f"mem_mass_{i:03d}",
            content=f"Mass memory content {i:03d}",
        )

    backlog_init = store.count_projection_backlog()
    assert backlog_init["pending"] == n_memories
    assert backlog_init["dead_letter"] == 0

    # 运行单轮 Worker，批大小固定为 10
    qdrant = FakeAuditQdrant()
    worker = OutboxWorker(
        session_store=store,
        qdrant_manager=qdrant,
        engine=FakeAuditEngine(),
        batch_size=10,
    )
    processed = await worker.run_once()
    assert processed == 10, "单轮 run_once 处理数必须严格等于 batch_size=10"

    # 验证积压降为 190
    backlog_after = store.count_projection_backlog()
    assert backlog_after["pending"] == 190
    # run_once 在轮次开始时采样指标，因此第一轮采样为 200.0；
    # 在第二轮开始时采样为 190.0
    assert METRICS.snapshot()["gauges"]["outbox_backlog"] == 200.0
    # 执行第二轮处理 10 条，此时采样刷新为 190.0
    processed_2 = await worker.run_once()
    assert processed_2 == 10
    assert METRICS.snapshot()["gauges"]["outbox_backlog"] == 190.0

    # 校验 outbox_lag_seconds 计算逻辑
    # 模拟等待 1 秒
    time.sleep(1.05)
    backlog_later = store.count_projection_backlog()
    assert backlog_later["oldest_pending_age_seconds"] >= 1
