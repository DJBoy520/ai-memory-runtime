"""
AI Memory Runtime - Main Daemon Entrypoint.
Initializes Core Services (SessionStore, BGEM3Engine, QdrantManager, MemoryService),
hosts the DualUDSServer on dedicated Business UDS and Admin UDS,
and serves Agent RPC calls with full lifecycle and backpressure governance.
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

from config.settings import check_and_fix_file_permissions, load_config
from src.core.engine import BGEM3Engine
from src.core.errors import IdempotencyConflict
from src.core.idempotency import compute_request_digest
from src.core.qdrant import QdrantManager
from src.core.request_context import (
    get_client_ref,
    get_request_id,
    new_request_id,
    reset_client_ref,
    reset_request_id,
    set_client_ref,
    set_request_id,
)
from src.core.session_store import SessionStore
from src.interfaces.ipc.auth import authorize_agent
from src.interfaces.ipc.errors import RpcInvalidArgument, RpcNotFound
from src.interfaces.ipc.server import DualUDSServer
from src.service.memory_service import MemoryService
from src.service.memory_status import normalize_status
from src.service.metrics import METRICS
from src.service.outbox_worker import OutboxWorker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)]
)
logger = logging.getLogger("AMR-Daemon")


class RequestContextFilter(logging.Filter):
    """把 request_id 和 client_ref 注入 LogRecord（未设置时默认 '-'）。"""

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = get_request_id()
        record.client_ref = get_client_ref()
        return True


class AMRApplication:
    def __init__(
        self,
        config_path: str = None,
        *,
        engine=None,
        qdrant_manager=None,
        session_store=None,
    ):
        if config_path:
            cfg_file = Path(config_path)
        elif os.environ.get("AMR_CONFIG_PATH"):
            cfg_file = Path(os.environ["AMR_CONFIG_PATH"])
        else:
            cfg_file = Path(__file__).resolve().parent.parent / "config" / "config.yaml"

        check_and_fix_file_permissions(cfg_file)
        self.config = load_config(cfg_file)

        logger.info("Initializing Storage (SQLite WAL)...")
        self.session_store = session_store if session_store is not None else SessionStore(config=self.config)

        logger.info("Initializing BGE-M3 Engine (Tesla P4 GPU)...")
        self.engine = engine if engine is not None else BGEM3Engine(config=self.config.model)

        logger.info("Initializing Qdrant Manager...")
        if qdrant_manager is not None:
            self.qdrant_manager = qdrant_manager
        else:
            self.qdrant_manager = QdrantManager(config=self.config.qdrant)
            self.qdrant_manager.ensure_standard_collections()

        logger.info("Initializing Semantic Memory Service...")
        self.memory_service = MemoryService(
            engine=self.engine,
            qdrant_manager=self.qdrant_manager,
            session_store=self.session_store,
            config=self.config
        )

        # Dual UDS Server: Business vs Admin
        self.server = DualUDSServer(
            config=self.config,
            business_handler=self.dispatch_business_rpc,
            admin_handler=self.dispatch_admin_rpc,
        )

        logger.info("Initializing Outbox Worker...")
        self.outbox_worker = OutboxWorker(
            config=self.config,
            session_store=self.session_store,
            engine=self.engine,
            qdrant_manager=self.qdrant_manager,
        )

    async def _execute_with_idempotency(
        self,
        op: str,
        idem_key: Optional[str],
        agent_id: str,
        payload_for_digest: Dict[str, Any],
        coro_fn,
    ) -> Dict[str, Any]:
        """执行带幂等仲裁的写操作（RFC v5.2 §1/§2 / 外部审计 A4）。"""
        if not idem_key or not hasattr(self.session_store, "locked_write_transaction"):
            return await coro_fn()

        digest = compute_request_digest(op, payload_for_digest)
        retention_days = getattr(getattr(self.config, "storage", None), "idempotency_retention_days", 7)
        now = int(time.time())
        cutoff = now - (retention_days * 86400)

        # 1. 仲裁占位
        with self.session_store.locked_write_transaction() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO idempotency_keys (
                    agent_id_raw, op, idem_key, request_digest, memory_id, response_json, created_at
                ) VALUES (?, ?, ?, ?, NULL, NULL, ?)
                ON CONFLICT(agent_id_raw, op, idem_key) DO UPDATE
                  SET request_digest = excluded.request_digest,
                      memory_id = NULL,
                      response_json = NULL,
                      created_at = excluded.created_at
                  WHERE idempotency_keys.created_at < ?;
                """,
                (agent_id, op, idem_key, digest, now, cutoff),
            )
            if cur.rowcount == 0:
                cur.execute(
                    "SELECT request_digest, response_json, memory_id FROM idempotency_keys WHERE agent_id_raw = ? AND op = ? AND idem_key = ?;",
                    (agent_id, op, idem_key),
                )
                row = cur.fetchone()
                if row is None or row["request_digest"] != digest:
                    existing_mid = row["memory_id"] if row else None
                    raise IdempotencyConflict(
                        op=op,
                        idem_key=idem_key,
                        existing_memory_id=existing_mid,
                    )
                resp_data = json.loads(row["response_json"]) if row["response_json"] else {}
                return {**resp_data, "idempotent_replay": True}

        # 2. 执行实际业务协程
        try:
            res = await coro_fn()
        except Exception:
            # 业务失败时回滚占位，不烧 key
            try:
                with self.session_store.locked_write_transaction() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        "DELETE FROM idempotency_keys WHERE agent_id_raw = ? AND op = ? AND idem_key = ? AND response_json IS NULL;",
                        (agent_id, op, idem_key),
                    )
            except Exception:
                pass
            raise

        # 3. 提交持久化响应快照
        mid = res.get("memory_id") if isinstance(res, dict) else None
        try:
            with self.session_store.locked_write_transaction() as conn:
                cur = conn.cursor()
                cur.execute(
                    "UPDATE idempotency_keys SET memory_id = ?, response_json = ? WHERE agent_id_raw = ? AND op = ? AND idem_key = ?;",
                    (mid, json.dumps(res, ensure_ascii=False), agent_id, op, idem_key),
                )
        except Exception as ex:
            logger.warning("Failed to record idempotency snapshot for %s/%s: %s", op, idem_key, ex)

        return res

    async def dispatch_business_rpc(self, method: str, params: Dict[str, Any]) -> Any:
        """Route business RPC calls strictly from business UDS."""
        params = params or {}
        srv_req_id = new_request_id()
        c_ref = params.get("request_id") or params.get("instance_id") or params.get("client_ref") or "-"
        token_rid = set_request_id(srv_req_id)
        token_ref = set_client_ref(str(c_ref))
        try:
            agent_id, instance_id = authorize_agent(self.config, params)

            if method in ("memory.search", "memory_search"):
                raw_status = params.get("status")
                if raw_status:
                    norm_status = [normalize_status(s) for s in raw_status] if isinstance(raw_status, list) else normalize_status(raw_status)
                else:
                    norm_status = None
                return await self.memory_service.memory_search(
                    query=params.get("query"),
                    collections=params.get("collections"),
                    project_id=params.get("project_id"),
                    memory_type=params.get("memory_type"),
                    type=params.get("type"),
                    scope=params.get("scope"),
                    limit=params.get("limit", 5),
                    score_threshold=params.get("score_threshold"),
                    include_history=params.get("include_history", False),
                    status=norm_status,
                )
            elif method in ("memory.create", "memory_create"):
                idem_key = params.get("idempotency_key")
                create_payload = {
                    "content": params.get("content"),
                    "project_id": params.get("project_id", "global"),
                    "type": params.get("type") or params.get("memory_type", "general"),
                    "status": params.get("status", "ACTIVE"),
                    "collection_name": params.get("collection_name", "ai_memory"),
                }
                return await self._execute_with_idempotency(
                    "create",
                    idem_key,
                    agent_id,
                    create_payload,
                    lambda: self.memory_service.memory_create(
                        content=params.get("content"),
                        project_id=params.get("project_id", "global"),
                        type=params.get("type") or params.get("memory_type", "general"),
                        status=params.get("status", "ACTIVE"),
                        agent_id=agent_id,
                        source_refs=params.get("source_refs"),
                        conflicts_with=params.get("conflicts_with"),
                        collection_name=params.get("collection_name", "ai_memory"),
                    ),
                )
            elif method in ("memory.update", "memory_update"):
                idem_key = params.get("idempotency_key")
                update_payload = {
                    "memory_id": params.get("memory_id"),
                    "content": params.get("content"),
                    "type": params.get("type"),
                    "status": params.get("status"),
                    "expected_version": params.get("expected_version"),
                    "change_reason": params.get("change_reason"),
                    "collection_name": params.get("collection_name", "ai_memory"),
                }
                return await self._execute_with_idempotency(
                    "update",
                    idem_key,
                    agent_id,
                    update_payload,
                    lambda: self.memory_service.memory_update(
                        memory_id=params.get("memory_id"),
                        content=params.get("content"),
                        type=params.get("type"),
                        status=params.get("status"),
                        expected_version=params.get("expected_version"),
                        change_reason=params.get("change_reason"),
                        agent_id=agent_id,
                        conflicts_with=params.get("conflicts_with"),
                        collection_name=params.get("collection_name", "ai_memory"),
                    ),
                )
            elif method in ("memory.history", "memory_history"):
                return await self.memory_service.memory_history(memory_id=params.get("memory_id"))
            elif method in ("memory.delete", "memory_delete"):
                idem_key = params.get("idempotency_key")
                delete_payload = {
                    "memory_id": params.get("memory_id"),
                    "reason": params.get("reason"),
                }
                return await self._execute_with_idempotency(
                    "delete",
                    idem_key,
                    agent_id,
                    delete_payload,
                    lambda: self.memory_service.memory_delete(
                        memory_id=params.get("memory_id"),
                        agent_id=agent_id,
                        reason=params.get("reason"),
                    ),
                )
            elif method == "memory.record":
                # 兼容老版接口
                return await self.memory_service.memory_record(
                    content=params.get("content"),
                    memory_type=params.get("memory_type", "fact"),
                    scope=params.get("scope", "global"),
                    project_id=params.get("project_id"),
                    source_agent=agent_id,
                    session_id=params.get("session_id"),
                    source_message_ids=params.get("source_message_ids"),
                    tags=params.get("tags"),
                    collection_name=params.get("collection_name", "ai_memory"),
                )
            elif method == "embedding.generate":
                texts = params.get("texts", [])
                return await self.engine.embed(texts)
            elif method in ("memory.get", "memory_get"):
                return await self.memory_service.memory_get(memory_id=params.get("memory_id"))
            elif method in ("memory.update_status", "memory_update_status"):
                return await self.memory_service.memory_update_status(
                    memory_id=params.get("memory_id"),
                    new_status=params.get("new_status"),
                    superseded_by=params.get("superseded_by"),
                )
            elif method in ("session.ingest", "session_ingest"):
                return await self.memory_service.memory_ingest_session(
                    session_id=params.get("session_id"),
                    agent_id=agent_id,
                    project_id=params.get("project_id"),
                    messages=params.get("messages", []),
                )
            elif method in ("system.health", "system_health", "system.ping", "ping"):
                return {
                    "status": "healthy",
                    "service": "ai-memory-runtime",
                    "model_state": self.engine.state.value if hasattr(self.engine, "state") else "ready",
                    "qdrant_ok": self.qdrant_manager.is_healthy() if hasattr(self.qdrant_manager, "is_healthy") else True,
                    "sqlite_ok": True,
                }
            elif method in ("system.status", "system_status"):
                return {
                    "status": "running",
                    "allowed_agents": list(self.config.server.allowed_agents),
                    "engine": await self.engine.get_model_status(),
                }
            else:
                raise ValueError(f"Unknown business method: {method}")
        finally:
            reset_client_ref(token_ref)
            reset_request_id(token_rid)

    async def dispatch_admin_rpc(self, method: str, params: Dict[str, Any]) -> Any:
        """Route administration RPC calls strictly from admin UDS."""
        params = params or {}
        srv_req_id = new_request_id()
        c_ref = params.get("request_id") or params.get("instance_id") or params.get("client_ref") or "-"
        token_rid = set_request_id(srv_req_id)
        token_ref = set_client_ref(str(c_ref))
        try:
            if method in ("admin.status", "admin_status"):
                status = await self.engine.get_model_status()
                colls = [c.name for c in self.qdrant_manager.client.get_collections().collections]
                return {
                    "engine": status,
                    "collections": colls,
                    "sqlite_path": self.config.storage.sqlite_path,
                    "status": "healthy"
                }
            elif method in ("admin.load", "admin_load"):
                await self.engine.load_model()
                return {"status": "loaded", "state": self.engine.state.value}
            elif method in ("admin.unload", "admin_unload"):
                await self.engine.unload_model()
                return {"status": "unloaded", "state": self.engine.state.value}
            elif method in ("admin.collections", "admin_collections"):
                colls = self.qdrant_manager.client.get_collections().collections
                info = {}
                for c in colls:
                    cnt = self.qdrant_manager.client.count(c.name).count
                    info[c.name] = {"points_count": cnt}
                return info
            elif method in ("admin.snapshot", "admin_snapshot"):
                col = params.get("collection", "ai_memory")
                snap = self.qdrant_manager.client.create_snapshot(col)
                return {"collection": col, "snapshot_name": snap.name if hasattr(snap, "name") else str(snap)}
            elif method in ("status.metrics", "status_metrics", "admin.metrics"):
                return METRICS.snapshot()
            elif method in ("outbox.list_dead_letters", "outbox_list_dead_letters"):
                limit = params.get("limit", 50)
                if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
                    raise RpcInvalidArgument(f"Parameter 'limit' must be a positive integer, got {limit}")

                with self.session_store.locked_write_transaction() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        """
                        SELECT id, target_collection, memory_id, projection_revision,
                               retry_count, last_error, dead_letter_at
                        FROM memory_projection_outbox
                        WHERE status = 'dead_letter'
                        ORDER BY dead_letter_at DESC, id DESC
                        LIMIT ?;
                        """,
                        (limit,),
                    )
                    rows = cur.fetchall()
                    dead_letters = [
                        {
                            "id": r["id"],
                            "collections": r["target_collection"],
                            "target_collection": r["target_collection"],
                            "memory_id": r["memory_id"],
                            "revision": r["projection_revision"],
                            "projection_revision": r["projection_revision"],
                            "retry_count": r["retry_count"],
                            "last_error": r["last_error"],
                            "dead_letter_at": r["dead_letter_at"],
                        }
                        for r in rows
                    ]
                    return {"dead_letters": dead_letters, "total": len(dead_letters)}
            elif method in ("outbox.replay", "outbox_replay"):
                raw_task_ids = params.get("task_ids")
                if raw_task_ids is None and "task_id" in params:
                    raw_task_ids = [params["task_id"]]

                if raw_task_ids is not None:
                    if isinstance(raw_task_ids, int) and not isinstance(raw_task_ids, bool):
                        task_ids_filter = [raw_task_ids]
                    elif isinstance(raw_task_ids, (list, tuple)):
                        for t in raw_task_ids:
                            if not isinstance(t, int) or isinstance(t, bool):
                                raise RpcInvalidArgument(f"Invalid task_id '{t}', must be integer")
                        task_ids_filter = list(raw_task_ids)
                    else:
                        raise RpcInvalidArgument("task_ids must be a list of integers or integer")
                else:
                    task_ids_filter = None

                with self.session_store.locked_write_transaction() as conn:
                    cur = conn.cursor()
                    if task_ids_filter is not None:
                        if not task_ids_filter:
                            return {"status": "ok", "replayed_count": 0, "task_ids": []}
                        placeholders = ",".join("?" for _ in task_ids_filter)
                        cur.execute(
                            f"SELECT * FROM memory_projection_outbox WHERE id IN ({placeholders});",
                            task_ids_filter,
                        )
                        found_rows = {r["id"]: r for r in cur.fetchall()}
                        for tid in task_ids_filter:
                            if tid not in found_rows:
                                raise RpcNotFound(f"Task #{tid} not found in outbox")
                            if found_rows[tid]["status"] != "dead_letter":
                                raise RpcInvalidArgument(
                                    f"Task #{tid} is not in dead_letter status (status='{found_rows[tid]['status']}')"
                                )
                        target_tasks = [found_rows[tid] for tid in task_ids_filter]
                    else:
                        cur.execute(
                            "SELECT * FROM memory_projection_outbox WHERE status = 'dead_letter' ORDER BY id ASC;"
                        )
                        target_tasks = cur.fetchall()

                    replayed_ids = []
                    now_ts = int(time.time())
                    for task in target_tasks:
                        mid = task["memory_id"]
                        cur.execute(
                            "SELECT projection_revision, collection_name, projection_class FROM memories WHERE memory_id = ?;",
                            (mid,),
                        )
                        mem_row = cur.fetchone()
                        if mem_row is None:
                            # 记忆行已不存在：标 done
                            cur.execute(
                                "UPDATE memory_projection_outbox SET status = 'done', last_error = 'memory row missing; skip', updated_at = ? WHERE id = ?;",
                                (now_ts, task["id"]),
                            )
                            replayed_ids.append(task["id"])
                            continue

                        curr_rev = mem_row["projection_revision"]
                        col = mem_row["collection_name"] or task["target_collection"] or "ai_memory"

                        # 调用统一入队 helper（以 memories 当前 projection_revision 重新入队，合并 pending）
                        new_pending_id = self.session_store.enqueue_projection_task(
                            cur,
                            memory_id=mid,
                            projection_revision=curr_rev,
                            target_collection=col,
                        )

                        # 原 dead_letter 行标 status='done', last_error='absorbed by pending#<id>'
                        cur.execute(
                            "UPDATE memory_projection_outbox SET status = 'done', last_error = ?, updated_at = ? WHERE id = ?;",
                            (f"absorbed by pending#{new_pending_id}", now_ts, task["id"]),
                        )
                        replayed_ids.append(task["id"])

                    return {"status": "ok", "replayed_count": len(replayed_ids), "task_ids": replayed_ids}
            else:
                raise ValueError(f"Unknown admin method: {method}")
        finally:
            reset_client_ref(token_ref)
            reset_request_id(token_rid)

    async def start(self):
        logger.info("Starting Outbox Worker...")
        self.outbox_worker.start()
        if getattr(getattr(self.config, "model", None), "preload_on_start", False) and hasattr(self.engine, "preload"):
            logger.info("Preloading model in background as preload_on_start=True...")
            asyncio.create_task(self.engine.preload())
        logger.info("Starting AMR Dual UDS Server...")
        await self.server.start()
        logger.info(f"AMR Daemon running on:\n  - Business UDS: {self.config.server.business_socket}\n  - Admin UDS:    {self.config.server.admin_socket}")

    async def stop(self):
        logger.info("Stopping Outbox Worker...")
        self.outbox_worker.stop()
        logger.info("Stopping AMR Dual UDS Server...")
        await self.server.stop()
        self.engine.close()
        self.session_store.close()
        self.qdrant_manager.close()
        logger.info("AMR Daemon stopped cleanly.")


async def main():
    logging.getLogger().addFilter(RequestContextFilter())
    app = AMRApplication()
    await app.start()

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    await stop_event.wait()
    await app.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
