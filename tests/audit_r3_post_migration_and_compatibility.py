"""
Audit R3 Track: Post-Migration Compatibility & Operational Semantics Tests
测试重点攻击面 5:
1. 新 schema 下旧队列排水 (Worker legacy drain + v3 promotion)
2. v3 投影全生命周期（创建、版本自增、合并 CAS、ux_outbox_pending_key 约束）
3. legacy 受控更新路径（delta envelope 与物理删除）
4. 单事务幂等控制表 (idempotency_keys)
5. C2 状态大写化迁移语义前置（分类标记门防止状态大写后二次污染）
"""

import asyncio
import json
from pathlib import Path
import sqlite3
import time
from unittest.mock import MagicMock
import pytest

from src.core.errors import IdempotencyConflict, LegacyPathForbidden
from src.core.session_store import SessionStore
from src.service.outbox_worker import OutboxWorker


class FakeEngine:
    def __init__(self, dim: int = 1024):
        self.dim = dim

    async def embed(self, texts):
        return [[0.05] * self.dim for _ in texts]


class FakeQdrant:
    def __init__(self):
        self.upserted_points = []
        self.deleted_points = []
        self.client = MagicMock()

    def upsert_points(self, collection_name: str, points, wait: bool = True):
        for p in points:
            self.upserted_points.append({"col": collection_name, "id": p.id, "payload": p.payload})

    def delete_point_by_id(self, point_id: str, collection_name: str = "ai_memory", wait: bool = True):
        self.deleted_points.append({"col": collection_name, "point_id": point_id})


@pytest.fixture
def migrated_store(tmp_path: Path):
    db_file = tmp_path / "post_mig.db"
    store = SessionStore(db_path=db_file)
    yield store
    store.close()


def test_v3_projection_workflow_and_constraints(migrated_store: SessionStore):
    """
    验证迁移后新 schema 下的 v3 投影全链路：
    1. create_memory_v3 生成 pending 任务，projection_revision=1
    2. bump_projection_revision 递增并在同事务中合并已存在的 pending 任务
    3. ux_outbox_pending_key 唯一约束生效（同一 target_collection + memory_id 不允许并存两条 pending）
    """
    mem = migrated_store.create_memory_v3(
        memory_id="mem_v3_post",
        content="Testing post migration v3 lifecycle",
        created_by_agent="test_agent",
    )
    assert mem["projection_status"] == "pending"
    assert mem["projection_revision"] == 1

    # 验证唯一约束
    with migrated_store._lock:
        conn = migrated_store._get_connection()
        cur = conn.cursor()
        with pytest.raises(sqlite3.IntegrityError):
            cur.execute(
                """
                INSERT INTO memory_projection_outbox (
                    target_collection, memory_id, projection_revision, status, created_at, updated_at
                ) VALUES ('ai_memory', 'mem_v3_post', 1, 'pending', 100, 100);
                """
            )

    # 正常流程：调用 enqueue_projection_task 会合并到已有 pending 行
    with migrated_store.locked_write_transaction() as conn:
        cur = conn.cursor()
        rev2 = migrated_store.bump_projection_revision(cur, "mem_v3_post")
        assert rev2 == 2
        outbox_id = migrated_store.enqueue_projection_task(cur, memory_id="mem_v3_post", projection_revision=rev2)

    tasks = migrated_store.fetch_pending_projection_tasks(limit=10)
    assert len(tasks) == 1
    assert tasks[0]["id"] == outbox_id
    assert tasks[0]["projection_revision"] == 2


@pytest.mark.asyncio
async def test_post_migration_worker_drain_and_v3_projection(migrated_store: SessionStore):
    """
    验证：迁移后 OutboxWorker 执行任务排水：
    - 新投影任务成功投递 FakeQdrant 并 CAS 标记为 done
    """
    migrated_store.create_memory_v3(
        memory_id="mem_worker_1",
        content="Vectorizable text for projection",
        created_by_agent="worker_test",
    )
    qdrant = FakeQdrant()
    engine = FakeEngine()
    worker = OutboxWorker(
        session_store=migrated_store,
        qdrant_manager=qdrant,
        engine=engine,
        batch_size=5,
        poll_interval=0.05,
    )

    processed = await worker.run_once()
    assert processed >= 1

    # 验证 Qdrant 写入
    assert len(qdrant.upserted_points) == 1
    assert qdrant.upserted_points[0]["payload"]["memory_id"] == "mem_worker_1"

    # 验证任务状态更新为 done 且 projected_revision 匹配
    with migrated_store._lock:
        c = migrated_store._get_connection().cursor()
        c.execute("SELECT status, projected_revision FROM memory_projection_outbox WHERE memory_id = 'mem_worker_1';")
        row = c.fetchone()
        assert row["status"] == "done"
        assert row["projected_revision"] == 1


def test_post_migration_legacy_delta_workflow(migrated_store: SessionStore):
    """
    验证：受控 legacy 路径（update_status_legacy_controlled）在迁移后的数据库上
    正常向 qdrant_sync_queue 入队 delta envelope，且禁止直接调用 v3 enqueue_projection_task。
    """
    # 插入一条 target 记忆和一条 legacy 记忆
    now = int(time.time())
    with migrated_store.locked_write_transaction() as conn:
        conn.cursor().execute(
            """
            INSERT INTO memories (
                memory_id, qdrant_point_id, type, subject, predicate, object,
                content, status, projection_class, scope, version, created_at, updated_at
            ) VALUES ('mem_v3_new', 'pt_v3_new', 'general', 's', 'p', 'o', 'content', 'ACTIVE', 'v3', 'global', 1, ?, ?);
            """,
            (now, now),
        )
        conn.cursor().execute(
            """
            INSERT INTO memories (
                memory_id, qdrant_point_id, type, subject, predicate, object,
                content, status, projection_class, scope, version, created_at, updated_at
            ) VALUES ('mem_leg_post', 'pt_leg_1', 'general', 's', 'p', 'o', 'content', 'ACTIVE', 'legacy', 'global', 1, ?, ?);
            """,
            (now, now),
        )

    # 1. 尝试直接给 legacy 记忆调用 v3 enqueue 应当被拦截
    with migrated_store.locked_write_transaction() as conn:
        cur = conn.cursor()
        with pytest.raises(LegacyPathForbidden):
            migrated_store.enqueue_projection_task(cur, memory_id="mem_leg_post", projection_revision=1)

    # 2. 调用受控 legacy 状态变更
    with migrated_store.locked_write_transaction() as conn:
        cur = conn.cursor()
        rows = migrated_store.resolve_memory_family(cur, "mem_leg_post")
        res_change = migrated_store.apply_status_change(
            cursor=cur,
            rows=rows,
            new_status="HISTORICAL",
            actor="test_governance",
            reason="superseded by newer record",
            superseded_by="mem_v3_new",
        )
        assert res_change["new_status"] == "HISTORICAL"

    with migrated_store._lock:
        c = migrated_store._get_connection().cursor()
        c.execute("SELECT op_type, payload_snapshot FROM qdrant_sync_queue WHERE memory_id = 'mem_leg_post';")
        row = c.fetchone()
        assert row["op_type"] == "update_payload"
        snap = json.loads(row["payload_snapshot"])
        assert snap["amr_delta_v1"] is True
        assert snap["op"] == "patch"
        assert snap["fields"]["status"] == "HISTORICAL"
        assert snap["fields"]["superseded_by"] == "mem_v3_new"


def test_post_migration_idempotency_table(migrated_store: SessionStore):
    """
    验证：迁移创建的 idempotency_keys 表在单事务原子写入中正常工作，
    支持占位、成功写入、以及相同 idem_key 的冲突判定。
    """
    agent = "agent_x"
    op = "memory_create"
    idem_key = "key_test_001"
    digest = "sha256:abcd"

    with migrated_store.locked_write_transaction() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO idempotency_keys (agent_id_raw, op, idem_key, request_digest, memory_id, response_json, created_at)
            VALUES (?, ?, ?, ?, 'mem_idem_1', '{"status":"ok"}', ?);
            """,
            (agent, op, idem_key, digest, int(time.time())),
        )

    # 再次插入相同的 (agent, op, idem_key) 必须触发主键冲突
    with migrated_store.locked_write_transaction() as conn:
        cur = conn.cursor()
        with pytest.raises(sqlite3.IntegrityError):
            cur.execute(
                """
                INSERT INTO idempotency_keys (agent_id_raw, op, idem_key, request_digest, memory_id, response_json, created_at)
                VALUES (?, ?, ?, ?, 'mem_idem_2', '{"status":"ok"}', ?);
                """,
                (agent, op, idem_key, digest, int(time.time())),
            )


def test_post_migration_c2_status_change_no_reclassification(tmp_path: Path):
    """
    【C2 状态迁移语义前置验证】
    RFC v5.3 F8: 存量分类是一次性门控，以 _amr_migrations 阻断重入。
    验证：在迁移完成后，若未来执行 C2 状态大写化迁移（如将 legacy 行的 status 从 'active' 改为 'ACTIVE'），
    再次实例化 SessionStore 时，不会因为 status 变为大写 ACTIVE 而把该行错误重新分类为 v3！
    """
    db_file = tmp_path / "c2_precondition.db"
    conn = sqlite3.connect(db_file)
    conn.execute(
        """
        CREATE TABLE memories (
            memory_id TEXT PRIMARY KEY,
            qdrant_point_id TEXT,
            content TEXT,
            status TEXT,
            source_refs TEXT,
            created_at INTEGER,
            updated_at INTEGER
        );
        """
    )
    now = int(time.time())
    # 初始状态为小写 active，无 revisions，source_refs 为空
    conn.execute("INSERT INTO memories VALUES ('mem_legacy_c2', 'pt_c2', 'content', 'active', '[]', ?, ?);", (now, now))
    conn.commit()
    conn.close()

    # 1. 跑 v5 迁移：分类为 legacy
    store = SessionStore(db_path=db_file)
    store.close()

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("SELECT projection_class, status FROM memories WHERE memory_id = 'mem_legacy_c2';")
    row = c.fetchone()
    assert row[0] == "legacy"
    assert row[1] == "active"

    # 2. 模拟后续运行 C2 状态枚举大写化更新：将 active 改为 ACTIVE
    c.execute("UPDATE memories SET status = 'ACTIVE' WHERE memory_id = 'mem_legacy_c2';")
    conn.commit()
    conn.close()

    # 3. 再次启动 SessionStore（模拟日常运维重启或二次初始化）
    store2 = SessionStore(db_path=db_file)
    store2.close()

    # 4. 验证：由于 _amr_migrations 存在 v5_projection_class_backfill 门控，
    # 回填未被重复执行，mem_legacy_c2 依然稳固保持为 legacy，没有被大写状态错误升级为 v3！
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("SELECT projection_class, status FROM memories WHERE memory_id = 'mem_legacy_c2';")
    row2 = c.fetchone()
    assert row2[0] == "legacy"
    assert row2[1] == "ACTIVE"
    conn.close()
