"""
AI Memory Runtime - Transactional Outbox Worker
后台发件箱同步守护进程：
- 独立后台守护协程/线程，按 memory_id 顺序消费 qdrant_sync_queue；
- 支持 upsert（调用 BGE-M3 生成 1024 维向量并推送到 Qdrant qdrant_point_id UUIDv5）；
- 支持 update_payload、delete（按 point_id 物理删除 Point）；
- 支持失败重试（上限 5 次）、异常捕获与状态更新，网络故障自愈。
遵循 Task 02 规范与 P0-16 架构标准。
"""

import asyncio
import json
import logging
import threading
import time
from typing import Any, Dict, List, Optional

from qdrant_client import models

from config.settings import AppConfig, load_config
from src.core.engine import BGEM3Engine
from src.core.qdrant import QdrantManager
from src.core.session_store import SessionStore

logger = logging.getLogger(__name__)


class OutboxWorker:
    """
    Transactional Outbox 异步同步守护 Worker
    """

    def __init__(
        self,
        session_store: Optional[SessionStore] = None,
        qdrant_manager: Optional[QdrantManager] = None,
        engine: Optional[BGEM3Engine] = None,
        config: Optional[AppConfig] = None,
        batch_size: int = 10,
        poll_interval: float = 1.0,
        default_collection: str = "ai_memory",
    ):
        self.config = config or load_config()
        self.session_store = session_store or SessionStore(config=self.config.storage)
        self.qdrant = qdrant_manager or QdrantManager.get_instance(config=self.config.qdrant)
        self.engine = engine or BGEM3Engine(config=self.config.model)
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self.default_collection = default_collection

        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    async def process_task(self, task: Dict[str, Any], collection_name: Optional[str] = None) -> bool:
        """
        处理单个 Outbox 任务：
        - upsert: 生成 1024 维向量并推送到 Qdrant；
        - update_payload: 更新已有 Point 的 payload；
        - delete: 从 Qdrant 中物理删除 point_id。
        """
        target_collection = collection_name or self.default_collection
        task_id = task["id"]
        memory_id = task["memory_id"]
        point_id = task["qdrant_point_id"]
        op_type = task["op_type"]
        payload_data = json.loads(task["payload_snapshot"]) if task.get("payload_snapshot") else {}

        try:
            if op_type == "upsert":
                # 获取待向量化的 content
                content = payload_data.get("content", "")
                if not content:
                    # 回查 SQLite memories 表获取最新 content
                    db_mem = self.session_store.get_memory(memory_id)
                    if db_mem:
                        content = db_mem.get("content", "")
                        payload_data.update(db_mem)

                # 生成向量
                embeddings = await self.engine.embed([content or " "])
                vector = embeddings[0]

                # 插入/更新 Qdrant point
                point = models.PointStruct(
                    id=point_id,
                    vector=vector,
                    payload=payload_data,
                )
                self.qdrant.upsert_points(
                    collection_name=target_collection,
                    points=[point],
                    wait=True,
                )

            elif op_type == "update_payload":
                # 按 memory_id 更新 payload
                self.qdrant.update_payload_by_memory_id(
                    memory_id=memory_id,
                    payload_updates=payload_data,
                    collections=[target_collection],
                    wait=True,
                )

            elif op_type == "delete":
                # 物理删除 point
                self.qdrant.delete_point_by_id(
                    point_id=point_id,
                    collection_name=target_collection,
                    wait=True,
                )

            else:
                raise ValueError(f"Unknown outbox op_type: {op_type}")

            # 标记完成
            self.session_store.mark_sync_task_done(task_id)
            logger.info(f"Outbox task {task_id} ({op_type}) for memory {memory_id} synced successfully.")
            return True

        except Exception as e:
            logger.error(f"Failed to process outbox task {task_id} ({op_type}) for {memory_id}: {e}")
            self.session_store.mark_sync_task_failed(task_id, str(e))
            return False

    async def run_once(self, collection_name: Optional[str] = None) -> int:
        """
        执行单轮同步批处理：
        从 SQLite 发件箱拉取一批 pending/failed 任务，保序执行并返回处理成功数量。
        """
        tasks = self.session_store.fetch_pending_sync_tasks(limit=self.batch_size)
        if not tasks:
            return 0

        success_count = 0
        for task in tasks:
            success = await self.process_task(task, collection_name=collection_name)
            if success:
                success_count += 1

        return success_count

    async def _async_run_loop(self) -> None:
        """后台轮询主协程"""
        while self._running:
            try:
                processed = await self.run_once()
                if processed == 0:
                    await asyncio.sleep(self.poll_interval)
            except Exception as e:
                logger.error(f"Error in outbox loop: {e}", exc_info=True)
                await asyncio.sleep(self.poll_interval)

    def start(self) -> None:
        """启动后台同步守护线程"""
        if self._running:
            return

        self._running = True

        def _worker_thread():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop
            try:
                loop.run_until_complete(self._async_run_loop())
            finally:
                loop.close()

        self._thread = threading.Thread(target=_worker_thread, daemon=True, name="amr_outbox_worker")
        self._thread.start()
        logger.info("OutboxWorker started.")

    def stop(self, timeout: float = 3.0) -> None:
        """停止后台守护线程"""
        if not self._running:
            return

        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        logger.info("OutboxWorker stopped.")
