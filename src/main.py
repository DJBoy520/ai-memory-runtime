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
            qdrant=self.qdrant_manager,
            session_store=self.session_store,
            config=self.config
        )

        # Dual UDS Server: Business vs Admin
        self.server = DualUDSServer(
            business_socket=self.config.server.business_socket,
            admin_socket=self.config.server.admin_socket,
            business_handler=self.dispatch_business_rpc,
            admin_handler=self.dispatch_admin_rpc,
            socket_mode=self.config.server.socket_mode,
            max_request_bytes=self.config.server.max_request_bytes,
        )

    async def dispatch_business_rpc(self, method: str, params: Dict[str, Any]) -> Any:
        """Route business RPC calls strictly from business UDS."""
        if method == "memory.search":
            return await self.memory_service.memory_search(
                query=params.get("query"),
                collections=params.get("collections"),
                project_id=params.get("project_id"),
                memory_type=params.get("memory_type"),
                scope=params.get("scope", "global"),
                limit=params.get("limit", 5),
                score_threshold=params.get("score_threshold"),
            )
        elif method == "memory.record":
            return await self.memory_service.memory_record(
                content=params.get("content"),
                memory_type=params.get("memory_type", "fact"),
                scope=params.get("scope", "global"),
                project_id=params.get("project_id"),
                source_agent=params.get("source_agent", "openclaw"),
                session_id=params.get("session_id"),
                source_message_ids=params.get("source_message_ids"),
                tags=params.get("tags"),
            )
        elif method == "memory.get":
            return await self.memory_service.memory_get(memory_id=params.get("memory_id"))
        elif method == "memory.update_status":
            return await self.memory_service.memory_update_status(
                memory_id=params.get("memory_id"),
                new_status=params.get("new_status"),
                superseded_by=params.get("superseded_by"),
            )
        elif method == "session.ingest":
            return await self.memory_service.memory_ingest_session(
                session_id=params.get("session_id"),
                agent_id=params.get("agent_id", "openclaw"),
                project_id=params.get("project_id"),
                messages=params.get("messages", []),
            )
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
        logger.info("Starting AMR Dual UDS Server...")
        await self.server.start()
        logger.info(f"AMR Daemon running on:\n  - Business UDS: {self.config.server.business_socket}\n  - Admin UDS:    {self.config.server.admin_socket}")

    async def stop(self):
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
