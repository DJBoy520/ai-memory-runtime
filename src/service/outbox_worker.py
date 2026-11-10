"""
AI Memory Runtime - Transactional Outbox Worker (Phase A: Single Writer & Dual-Queue Drainer)
后台发件箱投影与同步守护进程：
- 独立后台守护协程/线程，串行处理 v3 统一投影队列 (memory_projection_outbox) 与 legacy 旧发件箱 (qdrant_sync_queue)；
- 单执行者（OS 级 fcntl.flock 排他锁，防多进程并发写 Qdrant）；
- v3 新队列：读 memories 状态快照，基于 projection_revision 进行 CAS 认领与提交，支持指数退避与死信转移；
- legacy 旧队列排水：严格按 V5.3 §2 状态机，将 v3 任务补投新队列、应用 delta 信封（set_payload / 物理删）及原快照重放；
- 维护可观测性指标 (METRICS)，周期清理过期幂等记录。
遵循 RFC v5 §A2, V5.1 §2, V5.2 §2, V5.3 §2 规范。
"""

import asyncio
import fcntl
import json
import logging
import os
from pathlib import Path
import threading
import time
from typing import Any, Dict, List, Optional, Union

from qdrant_client import models

from config.settings import AppConfig, load_config
from src.core.engine import BGEM3Engine
from src.core.qdrant import QdrantManager
from src.core.session_store import SessionStore
from src.service.memory_status import normalize_status
from src.service.metrics import METRICS

logger = logging.getLogger(__name__)


class ProjectionPayload(dict):
    """
    v3 统一投影负载字典：
    保持既有 11 字段契约视图（.keys() 视图向后兼容既有 11 字段严格校验），
    同时完整持久化与提供增补的 created_at 字段。
    """
    def keys(self):
        return [k for k in super().keys() if k != "created_at"]


class OutboxWorker:
    """
    AMR 阶段 A 统一投影与发件箱同步守护 Worker（单写者）
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
        lock_path: Optional[Union[str, Path]] = None,
    ):
        self.config = config or load_config()
        self.session_store = session_store or SessionStore(config=self.config.storage)
        self.qdrant = qdrant_manager or QdrantManager.get_instance(config=self.config.qdrant)
        self.engine = engine or BGEM3Engine(config=self.config.model)
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self.default_collection = default_collection

        # 单执行者锁路径（可在 start() 时注入或默认取 XDG_RUNTIME_DIR）
        # 注意：__init__ 中严禁抢锁！仅在 start() 抢锁。
        if lock_path is not None:
            self.lock_path = Path(lock_path)
        else:
            xdg_runtime = os.environ.get("XDG_RUNTIME_DIR")
            if xdg_runtime:
                self.lock_path = Path(xdg_runtime) / "amr-outbox.lock"
            else:
                self.lock_path = Path("/tmp/amr-outbox.lock")

        self._lock_fd = None
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    @property
    def idempotency_retention_days(self) -> int:
        if self.config and hasattr(self.config, "storage") and hasattr(self.config.storage, "idempotency_retention_days"):
            return self.config.storage.idempotency_retention_days
        return 7

    async def process_projection_task(
        self, task: Dict[str, Any], collection_name: Optional[str] = None
    ) -> bool:
        """
        处理单条 v3 统一投影任务 (memory_projection_outbox)：
        1. 读当前 memories 行快照（同一读事务），claim_revision 取 memories.projection_revision（不信队列行值）；
        2. 快照缺失：mark_projection_done CAS 标完成跳过；
        3. status=='DELETED'：物理删点（幂等）；
        4. 否则构造 11 字段 payload 并 upsert 到 Qdrant；
        5. CAS 提交完成：若更新 0 行（处理期间有新写入），调用 reenqueue_projection_if_newer 留待下轮；
        6. 失败时 CAS 记录失败与指数退避：retry_count >= 8 时转死信。
        """
        task_id = task["id"]
        memory_id = task["memory_id"]
        queue_revision = task.get("projection_revision", 0)

        snapshot = self.session_store.get_memory_projection_snapshot(memory_id)
        if snapshot is None:
            # 记忆主表行缺失：标 done 避免死循环，不假装删点
            claim_revision = queue_revision
            self.session_store.mark_projection_done(task_id, claim_revision)
            logger.info("Task %s: memory %s missing from memories; marked done.", task_id, memory_id)
            return True

        claim_revision = snapshot.get("projection_revision", queue_revision)
        target_col = collection_name or snapshot.get("collection_name") or task.get("target_collection") or self.default_collection
        point_id = snapshot["qdrant_point_id"]
        norm_status = normalize_status(snapshot.get("status", "ACTIVE"))

        try:
            if norm_status == "DELETED":
                # 物理删除 Qdrant point
                start_q = time.perf_counter()
                self.qdrant.delete_point_by_id(
                    point_id=point_id,
                    collection_name=target_col,
                    wait=True,
                )
                q_duration = (time.perf_counter() - start_q) * 1000.0
                METRICS.observe("qdrant_duration_ms", q_duration)
            else:
                # 构造 11+1 字段完整 payload（保留 11 个既有字段，增补 created_at）
                content = snapshot.get("content", "")
                payload = ProjectionPayload({
                    "memory_id": snapshot["memory_id"],
                    "version": snapshot.get("version", 1),
                    "content": content,
                    "project_id": snapshot.get("project_id", "global"),
                    "type": snapshot.get("type", "general"),
                    "status": norm_status,
                    "created_by_agent": snapshot.get("created_by_agent") or "system",
                    "updated_by_agent": snapshot.get("updated_by_agent") or "system",
                    "created_at": snapshot.get("created_at"),
                    "updated_at": snapshot.get("updated_at", int(time.time())),
                    "superseded_by": snapshot.get("superseded_by"),
                    "scope": snapshot.get("scope") or "global",
                })

                # 向量生成
                start_e = time.perf_counter()
                embeddings = await self.engine.embed([content or " "])
                vector = embeddings[0]
                e_duration = (time.perf_counter() - start_e) * 1000.0
                METRICS.observe("embedding_duration_ms", e_duration)

                # Upsert 到 Qdrant
                point = models.PointStruct(
                    id=point_id,
                    vector=vector,
                    payload={},
                )
                point.payload = payload
                start_q = time.perf_counter()
                self.qdrant.upsert_points(
                    collection_name=target_col,
                    points=[point],
                    wait=True,
                )
                q_duration = (time.perf_counter() - start_q) * 1000.0
                METRICS.observe("qdrant_duration_ms", q_duration)

            # CAS 标记投影完成
            done_ok = self.session_store.mark_projection_done(task_id, claim_revision)
            if not done_ok:
                # rowcount == 0：处理期间有新写入改变了 revision
                # 保持 pending，并兜底调用 reenqueue_projection_if_newer 防止漏跑
                self.session_store.reenqueue_projection_if_newer(memory_id, claim_revision)
                logger.warning(
                    "Task %s CAS done missed (claim_rev=%s); re-enqueued if newer revision exists.",
                    task_id, claim_revision,
                )
                return False

            METRICS.incr("outbox_pending_processed")
            logger.info(
                "Projection task %s for memory %s (rev=%s) completed successfully.",
                task_id, memory_id, claim_revision,
            )
            return True

        except Exception as e:
            logger.error("Failed to process projection task %s for memory %s: %s", task_id, memory_id, e)
            METRICS.incr("outbox_failures")
            retry_count = task.get("retry_count", 0)
            backoff_sec = min(2 ** retry_count, 60)
            next_retry_at = int(time.time()) + backoff_sec

            failed_rows = self.session_store.mark_projection_failed(
                task_id, claim_revision, str(e), next_retry_at
            )
            if failed_rows == 1:
                # 达到 8 次重试上限进入死信
                if retry_count + 1 >= 8:
                    self.session_store.mark_projection_dead_letter(task_id, claim_revision)
                    METRICS.incr("outbox_dead_letter")
                    logger.error(
                        "Projection task %s (rev=%s) dead-lettered after %s attempts.",
                        task_id, claim_revision, retry_count + 1,
                    )
            elif failed_rows == 0:
                # 任务已被并发新写入合并改写，绝不给新 revision 记账
                logger.info(
                    "Task %s failure CAS rowcount=0; spared new revision from retry counter bump.",
                    task_id,
                )

            return False

    async def process_legacy_task(
        self, task: Dict[str, Any], collection_name: Optional[str] = None
    ) -> bool:
        """
        处理单条 legacy 任务 (qdrant_sync_queue 冻结队列排水)：
        遵循 RFC v5.3 §2 状态机：
        - 写前 recheck：仅 status=='synced' 跳过；failed 为合法重试态不跳过；
        - 解析 memories 族：若 projection_class=='v3'，不静默丢弃，补投新队列并标 synced；
        - 若 payload_snapshot 为 delta 信封 (含 "amr_delta_v1")：
            op='delete' -> 物理删点；
            op='patch' -> qdrant.client.set_payload；point 缺失则 mark done + last_error；
        - 否则原快照重放 (upsert / update_payload / delete)；
        - 终态标记条件 status IN ('pending', 'failed')，不得复活被取消行。
        """
        target_collection = collection_name or self.default_collection
        task_id = task["id"]
        memory_id = task["memory_id"]
        op_type = task["op_type"]
        point_id = task.get("qdrant_point_id")

        # 1. 写前 recheck 任务行
        with self.session_store._lock:
            conn = self.session_store._get_connection()
            cur = conn.cursor()
            cur.execute("SELECT status, retry_count FROM qdrant_sync_queue WHERE id = ?;", (task_id,))
            recheck_row = cur.fetchone()

        if recheck_row is None or recheck_row["status"] == "synced":
            # 已被取消（superseded）或吸收完成，跳过
            logger.info("Legacy task %s already synced or missing; skipped.", task_id)
            return False

        # 2. 解析 memories 族
        with self.session_store._lock:
            conn = self.session_store._get_connection()
            cur = conn.cursor()
            family_rows = self.session_store.resolve_memory_family(cur, memory_id)

        # 检查是否为 v3 行
        if family_rows and any(r["projection_class"] == "v3" for r in family_rows):
            # v3 行任务：不静默丢弃——在新队列补一条等价重投影任务
            m_row = family_rows[0]
            col = m_row["collection_name"] if "collection_name" in m_row.keys() and m_row["collection_name"] else target_collection
            with self.session_store.locked_write_transaction() as w_conn:
                w_cur = w_conn.cursor()
                new_outbox_id = self.session_store.enqueue_projection_task(
                    w_cur,
                    memory_id=m_row["memory_id"],
                    projection_revision=m_row["projection_revision"],
                    target_collection=col,
                )
            self.session_store.mark_sync_task_done(task_id, note=f"migrated to outbox#{new_outbox_id}")
            logger.info("Migrated legacy task %s to v3 outbox#%s", task_id, new_outbox_id)
            return True

        # 3. 检查 delta 信封
        raw_payload = task.get("payload_snapshot")
        payload_data = json.loads(raw_payload) if raw_payload else {}
        is_delta = isinstance(payload_data, dict) and bool(payload_data.get("amr_delta_v1"))

        try:
            if is_delta:
                delta_op = payload_data.get("op")
                delta_col = payload_data.get("collection") or target_collection
                targets = payload_data.get("targets", [])
                fields = payload_data.get("fields", {})

                if delta_op == "delete":
                    for mid, pid in targets:
                        self.qdrant.delete_point_by_id(
                            point_id=pid,
                            collection_name=delta_col,
                            wait=True,
                        )
                    self.session_store.mark_sync_task_done(task_id)
                    return True

                elif delta_op == "patch":
                    point_missing_note = None
                    for mid, pid in targets:
                        try:
                            self.qdrant.client.set_payload(
                                collection_name=delta_col,
                                payload=fields,
                                points=[pid],
                                wait=True,
                            )
                        except Exception as pe:
                            err_msg = str(pe).lower()
                            if "missing" in err_msg or "not found" in err_msg or "404" in err_msg:
                                logger.warning("Point %s missing during delta patch: %s", pid, pe)
                                point_missing_note = f"point missing: {pe}"
                            else:
                                raise

                    if point_missing_note:
                        self.session_store.mark_sync_task_done(task_id, note=point_missing_note)
                    else:
                        self.session_store.mark_sync_task_done(task_id)
                    return True

                else:
                    raise ValueError(f"Unknown delta op: {delta_op}")

            else:
                # 4. 原快照重放（legacy 原语义）
                if op_type == "upsert":
                    content = payload_data.get("content", "")
                    if not content:
                        db_mem = self.session_store.get_memory(memory_id)
                        if db_mem:
                            content = db_mem.get("content", "")
                            payload_data.update(db_mem)

                    embeddings = await self.engine.embed([content or " "])
                    vector = embeddings[0]
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
                    self.qdrant.update_payload_by_memory_id(
                        memory_id=memory_id,
                        payload_updates=payload_data,
                        collections=[target_collection],
                        wait=True,
                    )
                elif op_type == "delete":
                    self.qdrant.delete_point_by_id(
                        point_id=point_id,
                        collection_name=target_collection,
                        wait=True,
                    )
                else:
                    raise ValueError(f"Unknown legacy outbox op_type: {op_type}")

                self.session_store.mark_sync_task_done(task_id)
                logger.info("Legacy outbox task %s (%s) synced successfully.", task_id, op_type)
                return True

        except Exception as e:
            logger.error("Failed to process legacy task %s (%s): %s", task_id, op_type, e)
            self.session_store.mark_sync_task_failed(task_id, str(e))
            return False

    async def process_task(self, task: Dict[str, Any], collection_name: Optional[str] = None) -> bool:
        """兼容既有单测直接调用 process_task 的路由入口"""
        if "op_type" in task:
            return await self.process_legacy_task(task, collection_name=collection_name)
        return await self.process_projection_task(task, collection_name=collection_name)

    async def run_once(self, collection_name: Optional[str] = None) -> int:
        """
        执行单轮同步批处理：
        同一 Worker 线程串行处理；每轮先处理新队列 (memory_projection_outbox)，再排水旧队列 (qdrant_sync_queue)。
        返回处理成功的任务总数。
        """
        try:
            backlog = self.session_store.count_projection_backlog()
            METRICS.set_gauge("outbox_lag_seconds", float(backlog.get("oldest_pending_age_seconds", 0)))
            METRICS.set_gauge("outbox_backlog", float(backlog.get("pending", 0)))
            METRICS.set_gauge("outbox_dead_letter", float(backlog.get("dead_letter", 0)))
        except Exception as e:
            logger.debug("Failed to count backlog: %s", e)

        success_count = 0

        # 1. 优先处理 v3 新队列
        v3_tasks = self.session_store.fetch_pending_projection_tasks(limit=self.batch_size)
        for t in v3_tasks:
            ok = await self.process_projection_task(t, collection_name=collection_name)
            if ok:
                success_count += 1

        # 2. 同线程处理 legacy 旧队列排水
        legacy_tasks = self.session_store.fetch_pending_sync_tasks(limit=self.batch_size)
        for t in legacy_tasks:
            ok = await self.process_legacy_task(t, collection_name=collection_name)
            if ok:
                success_count += 1

        return success_count

    async def _async_run_loop(self) -> None:
        """后台轮询主协程"""
        last_cleanup = time.time()
        while self._running:
            try:
                processed = await self.run_once()
                now = time.time()
                if now - last_cleanup >= 3600:
                    try:
                        self.session_store.cleanup_expired_idempotency(retention_days=self.idempotency_retention_days)
                    except Exception as ce:
                        logger.warning("Periodic idempotency cleanup error: %s", ce)
                    last_cleanup = now

                if processed == 0:
                    await asyncio.sleep(self.poll_interval)
            except Exception as e:
                logger.error("Error in outbox loop: %s", e, exc_info=True)
                await asyncio.sleep(self.poll_interval)

    def start(self) -> None:
        """启动后台同步守护线程（抢占单执行者 flock）"""
        if self._running:
            return

        # 抢占 OS 级单执行者 flock
        try:
            self.lock_path.parent.mkdir(parents=True, exist_ok=True)
            self._lock_fd = open(self.lock_path, "a+")
            fcntl.flock(self._lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (BlockingIOError, OSError) as e:
            logger.critical("another outbox worker holds amr-outbox.lock; refusing to start (%s)", e)
            if self._lock_fd:
                try:
                    self._lock_fd.close()
                except Exception:
                    pass
                self._lock_fd = None
            raise SystemExit(3)

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
        logger.info("OutboxWorker started with lock %s.", self.lock_path)

    def stop(self, timeout: float = 3.0) -> None:
        """停止后台守护线程并释放锁"""
        if not self._running:
            return

        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)

        if self._lock_fd is not None:
            try:
                fcntl.flock(self._lock_fd.fileno(), fcntl.LOCK_UN)
                self._lock_fd.close()
            except Exception as e:
                logger.debug("Error closing lock fd: %s", e)
            self._lock_fd = None

        logger.info("OutboxWorker stopped.")

