"""
AI Memory Runtime - Main Daemon Entrypoint.
Initializes Core Services (SessionStore, BGEM3Engine, QdrantManager, MemoryService),
hosts the DualUDSServer on dedicated Business UDS and Admin UDS,
and serves Agent RPC calls with full lifecycle and backpressure governance.
"""

import asyncio
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Any, Dict

from config.settings import check_and_fix_file_permissions, load_config
from src.core.engine import BGEM3Engine
from src.core.qdrant import QdrantManager
from src.core.session_store import SessionStore
from src.interfaces.ipc.server import DualUDSServer
from src.service.memory_service import MemoryService
from src.service.outbox_worker import OutboxWorker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)]
)
logger = logging.getLogger("AMR-Daemon")


class AMRApplication:
    def __init__(self, config_path: str = None):
        cfg_file = Path(config_path) if config_path else Path(__file__).resolve().parent.parent / "config" / "config.yaml"
        check_and_fix_file_permissions(cfg_file)
        self.config = load_config(cfg_file)

        logger.info("Initializing Storage (SQLite WAL)...")
        self.session_store = SessionStore(config=self.config)

        logger.info("Initializing BGE-M3 Engine (Tesla P4 GPU)...")
        self.engine = BGEM3Engine(config=self.config.model)

        logger.info("Initializing Qdrant Manager...")
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

    async def dispatch_business_rpc(self, method: str, params: Dict[str, Any]) -> Any:
        """Route business RPC calls strictly from business UDS."""
        # Agent 身份检查与规范化：统一从配置文件中动态读取，大小写归一化
        agent_id = (params.get("agent_id") or params.get("source_agent") or "system").lower()
        allowed_agents = {a.lower() for a in self.config.server.allowed_agents}
        if agent_id not in allowed_agents:
            raise ValueError(f"AGENT_UNAUTHORIZED: Unknown or untrusted agent_id '{agent_id}'")

        if method in ("memory.search", "memory_search"):
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
                status=params.get("status"),
            )
        elif method in ("memory.create", "memory_create"):
            return await self.memory_service.memory_create(
                content=params.get("content"),
                project_id=params.get("project_id", "global"),
                type=params.get("type") or params.get("memory_type", "general"),
                status=params.get("status", "ACTIVE"),
                agent_id=agent_id,
                source_refs=params.get("source_refs"),
                conflicts_with=params.get("conflicts_with"),
                collection_name=params.get("collection_name", "ai_memory"),
            )
        elif method in ("memory.update", "memory_update"):
            return await self.memory_service.memory_update(
                memory_id=params.get("memory_id"),
                content=params.get("content"),
                type=params.get("type"),
                status=params.get("status"),
                expected_version=params.get("expected_version"),
                change_reason=params.get("change_reason"),
                agent_id=agent_id,
                conflicts_with=params.get("conflicts_with"),
                collection_name=params.get("collection_name", "ai_memory"),
            )
        elif method in ("memory.history", "memory_history"):
            return await self.memory_service.memory_history(memory_id=params.get("memory_id"))
        elif method in ("memory.delete", "memory_delete"):
            return await self.memory_service.memory_delete(
                memory_id=params.get("memory_id"),
                agent_id=agent_id,
                reason=params.get("reason"),
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

    async def dispatch_admin_rpc(self, method: str, params: Dict[str, Any]) -> Any:
        """Route administration RPC calls strictly from admin UDS."""
        if method == "admin.status":
            status = await self.engine.get_model_status()
            colls = [c.name for c in self.qdrant_manager.client.get_collections().collections]
            return {
                "engine": status,
                "collections": colls,
                "sqlite_path": self.config.storage.sqlite_path,
                "status": "healthy"
            }
        elif method == "admin.load":
            await self.engine.load_model()
            return {"status": "loaded", "state": self.engine.state.value}
        elif method == "admin.unload":
            await self.engine.unload_model()
            return {"status": "unloaded", "state": self.engine.state.value}
        elif method == "admin.collections":
            colls = self.qdrant_manager.client.get_collections().collections
            info = {}
            for c in colls:
                cnt = self.qdrant_manager.client.count(c.name).count
                info[c.name] = {"points_count": cnt}
            return info
        elif method == "admin.snapshot":
            col = params.get("collection", "ai_memory")
            snap = self.qdrant_manager.client.create_snapshot(col)
            return {"collection": col, "snapshot_name": snap.name if hasattr(snap, "name") else str(snap)}
        else:
            raise ValueError(f"Unknown admin method: {method}")

    async def start(self):
        logger.info("Starting Outbox Worker...")
        self.outbox_worker.start()
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
    except (KeyboardInterrupt, SystemExit):
        pass
