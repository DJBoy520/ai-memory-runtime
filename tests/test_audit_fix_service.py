"""
AMR v5 Service Layer Audit Fix Regression Tests
覆盖 R1 幂等与 R5 易用性/功能性缺陷修复验证：
1. status 大小写幂等重放 (memory_create / memory_update)
2. v3 superseded_by 精确 ID 重定向 (HISTORICAL 规范状态跳转与 3 跳防环及可见性约束)
3. storage.idempotency_retention_days 配置生效 (MemoryService 与 OutboxWorker)
4. search 结果 created_at 非空 (OutboxWorker 投影增补与 Exact-ID 回填)
5. reconciliation --dry-run 严格只读 (DB 文件 hash/mtime、备份目录、updated_at 均不变)
6. memory_get 携带 deletion_reason (删除原因可追溯)
"""

import asyncio
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock
import pytest

from config.settings import AppConfig, StorageConfig, load_config
from src.core.errors import IdempotencyConflict
from src.core.idempotency import compute_request_digest
from src.core.session_store import SessionStore
from src.reconciliation.engine import ReconciliationEngine
from src.service.memory_service import MemoryService
from src.service.memory_status import normalize_status
from src.service.outbox_worker import OutboxWorker


class DummyEngine:
    async def embed(self, texts: List[str]) -> List[List[float]]:
        return [[0.05] * 1024 for _ in texts]


class MockQdrantManager:
    def __init__(self):
        self.points: Dict[str, Dict[str, Any]] = {}
        self.upserted_points: List[Dict[str, Any]] = []

    def get_point_by_memory_id(self, memory_id: str, collection_name: str = "ai_memory") -> Optional[Dict[str, Any]]:
        pt = self.points.get(memory_id)
        if pt:
            return {"point_id": pt.get("id"), "collection": collection_name, "payload": pt.get("payload")}
        return None

    def upsert_points(self, collection_name: str, points: List[Any], wait: bool = True):
        for p in points:
            self.upserted_points.append({
                "collection": collection_name,
                "id": p.id,
                "vector": p.vector,
                "payload": p.payload,
            })
            mid = p.payload.get("memory_id") if isinstance(p.payload, dict) else None
            if mid:
                self.points[mid] = {
                    "id": p.id,
                    "payload": dict(p.payload),
                }

    def search_points(self, collection_name: str, query_vector: Any, limit: int = 10, **kwargs):
        class ScoredPoint:
            def __init__(self, id, score, payload):
                self.id = id
                self.score = score
                self.payload = payload

        allowed = kwargs.get("allowed_statuses")
        allowed_set = {s.upper() for s in allowed} if allowed else None

        results = []
        for mid, pt in self.points.items():
            payload = pt.get("payload") or {}
            st = normalize_status(payload.get("status"))
            if allowed_set and st not in allowed_set:
                continue
            results.append(ScoredPoint(pt["id"], 0.95, payload))
        return results[:limit]


@pytest.fixture
def test_env(tmp_path: Path):
    db_file = str(tmp_path / "test_audit_fix.db")
    store = SessionStore(db_path=db_file)
    qdrant = MockQdrantManager()
    engine = DummyEngine()
    service = MemoryService(
        engine=engine,
        qdrant_manager=qdrant,
        session_store=store,
    )
    worker = OutboxWorker(
        session_store=store,
        qdrant_manager=qdrant,
        engine=engine,
        poll_interval=0.05,
    )
    yield {
        "store": store,
        "qdrant": qdrant,
        "engine": engine,
        "service": service,
        "worker": worker,
        "db_path": db_file,
    }
    store.close()


# =========================================================================
# 1. status 大小写幂等重放
# =========================================================================

@pytest.mark.asyncio
async def test_status_case_insensitivity_idempotent_replay(test_env):
    """
    同 key 同内容但 status 大小写不同（如 'ACTIVE' vs 'active'）：
    应当被规范化后计算相同 digest，完成幂等快照重放，而不是抛出 -32006 IdempotencyConflict。
    """
    service = test_env["service"]

    # 1.1 Create 幂等重放 (ACTIVE -> active)
    create_key = "idem_case_create_001"
    res1 = await service.memory_create(
        content="Testing case-insensitive status for create",
        status="ACTIVE",
        idempotency_key=create_key,
        agent_id="test_agent",
    )
    mid = res1["memory_id"]
    assert "idempotent_replay" not in res1

    res1_replay = await service.memory_create(
        content="Testing case-insensitive status for create",
        status="active",
        idempotency_key=create_key,
        agent_id="test_agent",
    )
    assert res1_replay["memory_id"] == mid
    assert res1_replay.get("idempotent_replay") is True

    # 1.2 Update 幂等重放 (HISTORICAL -> historical)
    update_key = "idem_case_update_001"
    res2 = await service.memory_update(
        memory_id=mid,
        status="HISTORICAL",
        idempotency_key=update_key,
        agent_id="test_agent",
    )
    assert res2.get("status") == "HISTORICAL"
    assert "idempotent_replay" not in res2

    res2_replay = await service.memory_update(
        memory_id=mid,
        status="historical",
        idempotency_key=update_key,
        agent_id="test_agent",
    )
    assert res2_replay.get("idempotent_replay") is True
    assert res2_replay.get("status") == "HISTORICAL"


# =========================================================================
# 2. v3 superseded_by 精确 ID 重定向
# =========================================================================

@pytest.mark.asyncio
async def test_v3_superseded_redirect_in_search(test_env):
    """
    v3 精确 ID 检索时，若命中点为 HISTORICAL 且存在 superseded_by：
    应沿 superseded_by 链跳转（最多 3 跳防环），取到当前现役事实。
    同时验证可见性过滤与循环防环。
    """
    service = test_env["service"]
    qdrant = test_env["qdrant"]

    mid_v1 = "mem_20261110_000001"
    mid_v2 = "mem_20261110_000002"
    mid_v3 = "mem_20261110_000003"

    # 在 Qdrant 中放置三条记忆构成接替链：v1 -> v2 -> v3 (active)
    now = int(time.time())
    qdrant.points[mid_v1] = {
        "id": "pt_1",
        "payload": {
            "memory_id": mid_v1,
            "version": 1,
            "content": "Arch v1 (outdated)",
            "status": "HISTORICAL",
            "superseded_by": mid_v2,
            "type": "decision/arch",
            "created_at": now - 200,
            "updated_at": now - 100,
        },
    }
    qdrant.points[mid_v2] = {
        "id": "pt_2",
        "payload": {
            "memory_id": mid_v2,
            "version": 2,
            "content": "Arch v2 (interim)",
            "status": "HISTORICAL",
            "superseded_by": mid_v3,
            "type": "decision/arch",
            "created_at": now - 100,
            "updated_at": now - 50,
        },
    }
    qdrant.points[mid_v3] = {
        "id": "pt_3",
        "payload": {
            "memory_id": mid_v3,
            "version": 3,
            "content": "Arch v3 (current consensus)",
            "status": "ACTIVE",
            "superseded_by": None,
            "type": "decision/arch",
            "created_at": now - 50,
            "updated_at": now,
        },
    }

    # 检索携带过期的 mid_v1
    search_res = await service.memory_search(
        query=f"Looking for architectural consensus in {mid_v1}",
        limit=5,
    )
    results = search_res["results"]
    assert len(results) >= 1
    hit = next((r for r in results if r.get("memory_id") == mid_v3), None)
    assert hit is not None
    assert hit["matched_by"] == "exact_id"
    assert hit["superseded_from"] == mid_v1
    assert hit["content"] == "Arch v3 (current consensus)"
    assert hit["status"] == "ACTIVE"

    # 验证防环机制：构造环形接替链 A -> B -> A
    mid_loop_a = "mem_20261110_loop01"
    mid_loop_b = "mem_20261110_loop02"
    qdrant.points[mid_loop_a] = {
        "id": "pt_loop_a",
        "payload": {
            "memory_id": mid_loop_a,
            "status": "HISTORICAL",
            "superseded_by": mid_loop_b,
        },
    }
    qdrant.points[mid_loop_b] = {
        "id": "pt_loop_b",
        "payload": {
            "memory_id": mid_loop_b,
            "status": "HISTORICAL",
            "superseded_by": mid_loop_a,
        },
    }

    # 查询包含 loop_a：应当在 3 跳内安全终止，且因最终点不是 ACTIVE 而受可见性约束过滤
    loop_res = await service.memory_search(
        query=f"Check circular link {mid_loop_a}",
        limit=5,
    )
    loop_results = loop_res["results"]
    # loop_a/loop_b 为 HISTORICAL，默认只允许 ACTIVE，因此不会误召回
    assert not any(r["memory_id"] in (mid_loop_a, mid_loop_b) for r in loop_results)


# =========================================================================
# 3. storage.idempotency_retention_days 配置生效
# =========================================================================

def test_idempotency_retention_configuration(tmp_path):
    """
    验证 StorageConfig.idempotency_retention_days 正确传递至 SessionStore 的
    execute_idempotent 与 cleanup_expired_idempotency。
    """
    db_file = str(tmp_path / "test_retention_cfg.db")
    custom_cfg = AppConfig(
        storage=StorageConfig(
            sqlite_path=db_file,
            idempotency_retention_days=3,  # 自定义保留期 3 天
        )
    )

    store = SessionStore(db_path=db_file)
    service = MemoryService(session_store=store, config=custom_cfg)
    worker = OutboxWorker(session_store=store, config=custom_cfg)

    # 验证属性正确暴露配置值
    assert service.idempotency_retention_days == 3
    assert worker.idempotency_retention_days == 3

    # 在 DB 中插入两条记录：一条 5 天前（超过 3 天），一条 1 天前（未超过 3 天）
    now = int(time.time())
    ts_5d_ago = now - (5 * 86400)
    ts_1d_ago = now - (1 * 86400)

    digest_old = compute_request_digest("create", {"data": "old"})
    digest_recent = compute_request_digest("create", {"data": "recent"})

    with store.locked_write_transaction() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO idempotency_keys (agent_id_raw, op, idem_key, request_digest, memory_id, response_json, created_at)
            VALUES ('default', 'create', 'key_old_5d', ?, 'mem_old', '{"res": 1}', ?),
                   ('default', 'create', 'key_recent_1d', ?, 'mem_rec', '{"res": 2}', ?);
            """,
            (digest_old, ts_5d_ago, digest_recent, ts_1d_ago),
        )

    # 执行清理，传入配置的 retention_days=3
    deleted = store.cleanup_expired_idempotency(retention_days=worker.idempotency_retention_days)
    assert deleted == 1  # 只有 5 天前的那条被清理

    assert store.get_idempotency_record("default", "create", "key_old_5d") is None
    assert store.get_idempotency_record("default", "create", "key_recent_1d") is not None
    store.close()


# =========================================================================
# 4. search 结果 created_at 非空
# =========================================================================

@pytest.mark.asyncio
async def test_search_results_created_at_not_none(test_env):
    """
    验证 v3 OutboxWorker 写入 Qdrant 时包含 created_at（取自 SQLite SSOT）；
    memory_search 无论是精确 ID 还是向量检索，返回的 created_at 均非空。
    """
    store = test_env["store"]
    worker = test_env["worker"]
    service = test_env["service"]
    qdrant = test_env["qdrant"]

    # 1. 创建一条 v3 记忆
    mid = "mem_20261110_createdat"
    now_ts = int(time.time())
    with store.locked_write_transaction() as conn:
        res = store.create_memory_v3(
            memory_id=mid,
            content="Testing created_at timestamp persistence in projection",
            type="decision/arch",
            cursor=conn.cursor(),
        )

    # 2. Worker 消费 outbox 投递 Qdrant
    processed = await worker.run_once()
    assert processed == 1

    # 3. 校验 Qdrant payload 中的 created_at
    pt = qdrant.upserted_points[-1]
    payload = pt["payload"]
    assert payload["created_at"] is not None
    assert isinstance(payload["created_at"], int)
    assert payload["created_at"] >= now_ts - 5

    # 4. 校验 memory_search 精确 ID 命中带有 created_at 与 updated_at
    search_res = await service.memory_search(query=f"Locate exact {mid}")
    hits = search_res["results"]
    assert len(hits) >= 1
    hit = next((h for h in hits if h["memory_id"] == mid), None)
    assert hit is not None
    assert hit["created_at"] is not None
    assert hit["updated_at"] is not None
    assert hit["created_at"] == payload["created_at"]


# =========================================================================
# 5. reconciliation --dry-run 严格只读
# =========================================================================

def test_reconciliation_dry_run_no_writes_no_backup(tmp_path):
    """
    --dry-run 模式下，验证：
    1. phase_0 不向 reconciliation_checkpoints 插入记录
    2. phase_1 不创建备份文件或备份目录
    3. phase_2 不向 curation_candidates 写状态
    4. phase_4 不更新 memories.last_reconciled_at
    5. phase_6 不写审计归档记录
    6. SQLite 物理文件 hash 与 mtime 保持严格不变
    """
    db_file = str(tmp_path / "test_recon_dry_run.db")
    store = SessionStore(db_path=db_file)
    conn = store.get_connection()
    now = int(time.time()) - 1000

    # 插入一条待对账技术记忆
    conn.execute(
        """
        INSERT INTO memories (
            memory_id, qdrant_point_id, type, subject, predicate, content,
            valid_from, confidence, importance, mention_count, status,
            project_id, scope, source_agent, version, created_at, updated_at
        ) VALUES ('mem_recon_dry_1', 'pt_recon_dry_1', 'fact', 'test', 'inject',
                  '<!--TEMP_AMR_SYNC_MARKER--> test injection',
                  ?, 0.5, 0.2, 1, 'active', 'test', 'global', 'agent', 1, ?, ?);
        """,
        (now, now, now),
    )
    conn.commit()
    store.close()

    time.sleep(0.05)
    # 记录前置状态
    h1 = hashlib.sha256(open(db_file, "rb").read()).hexdigest()
    m1 = os.path.getmtime(db_file)

    # 记录 backups 目录状态
    backup_dir = Path("data/backups")
    initial_backups = set(backup_dir.glob("*")) if backup_dir.exists() else set()

    # 执行 dry-run 对账
    engine = ReconciliationEngine(dry_run=True, db_path=db_file, batch_id="batch_dry_test")
    engine.qdrant_client = MagicMock()
    engine.qdrant_client.collection_exists.return_value = True
    engine.qdrant_client.count.return_value.count = 1
    engine.qdrant_client.scroll.return_value = ([], None)

    summary = engine.run()
    engine.store.close()

    # 校验返回值
    assert summary.get("dry_run") is True
    assert summary.get("backup_info", {}).get("sqlite_backup") is None

    # 校验 SQLite 文件 Hash 与 mtime 完全未变
    h2 = hashlib.sha256(open(db_file, "rb").read()).hexdigest()
    m2 = os.path.getmtime(db_file)
    assert h1 == h2, "SQLite file content was modified during dry-run!"
    assert m1 == m2, "SQLite file mtime was modified during dry-run!"

    # 校验无新备份文件生成
    current_backups = set(backup_dir.glob("*")) if backup_dir.exists() else set()
    assert current_backups == initial_backups, "Backup file was created during dry-run!"

    # 检查数据库内部表没有任何写入
    conn_check = sqlite3.connect(db_file)
    cur = conn_check.cursor()

    cur.execute("SELECT count(*) FROM reconciliation_checkpoints;")
    assert cur.fetchone()[0] == 0, "Checkpoints were written during dry-run!"

    cur.execute("SELECT count(*) FROM curation_candidates;")
    assert cur.fetchone()[0] == 0, "Curation candidates were written during dry-run!"

    cur.execute("SELECT last_reconciled_at, updated_at FROM memories WHERE memory_id = 'mem_recon_dry_1';")
    row = cur.fetchone()
    assert row[0] is None, "last_reconciled_at was updated during dry-run!"
    assert row[1] == now, "updated_at was modified during dry-run!"
    conn_check.close()


# =========================================================================
# 6. memory_get 删除原因与元数据回填
# =========================================================================

@pytest.mark.asyncio
async def test_memory_get_includes_deletion_reason(test_env):
    """验证 memory_delete 后，memory_get 权威返回包含 deletion_reason"""
    service = test_env["service"]

    # 创建记忆
    res = await service.memory_create(
        content="Fact to be deleted with specific reason",
        agent_id="cleanup_bot",
    )
    mid = res["memory_id"]

    # 删除记忆
    del_res = await service.memory_delete(
        memory_id=mid,
        agent_id="cleanup_bot",
        reason="Stale knowledge invalidated by RFC-v5",
    )
    assert del_res["current_status"] == "DELETED"

    # memory_get 读取
    get_res = await service.memory_get(mid)
    assert get_res is not None
    assert get_res["status"] == "DELETED"
    assert get_res.get("deletion_reason") == "Stale knowledge invalidated by RFC-v5"
