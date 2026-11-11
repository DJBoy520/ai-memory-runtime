"""
AI Memory Runtime - Memory Service 核心语义服务层
实现记忆语义检索、记忆持久化、超长 Token 滑动切片、四态流转及原始会话流水幂等摄取
遵循 DOC-AMR-03-DDD / DOC-AMR-04-API 规范
"""

import asyncio
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import logging
import re
import threading
import time
from typing import Any, Dict, List, Optional, Tuple, Union
import uuid

from qdrant_client import models

from config.settings import AppConfig, load_config
from src.core.engine import BGEM3Engine
from src.core.errors import IdempotencyConflict, LegacyPathForbidden, VersionConflictError
from src.core.idempotency import compute_request_digest
from src.core.qdrant import QdrantManager, STANDARD_COLLECTIONS
from src.core.request_context import get_request_id
from src.core.session_store import SessionStore
from src.service.memory_status import CANONICAL_STATUSES, normalize_status

logger = logging.getLogger(__name__)

# 状态四态
VALID_STATUSES = {"active", "superseded", "archived", "deleted"}
MAX_TOKEN_LIMIT = 8192
OVERLAP_TOKENS = 128


class MemoryService:
    """
    语义 Memory 核心服务
    整合 BGEM3Engine (向量生成), QdrantManager (向量数据库检索与存储), SessionStore (会话与消息溯源)
    """

    def __init__(
        self,
        engine: Optional[BGEM3Engine] = None,
        qdrant_manager: Optional[QdrantManager] = None,
        session_store: Optional[SessionStore] = None,
        config: Optional[AppConfig] = None,
    ):
        self.config = config or load_config()
        self.engine = engine or BGEM3Engine(config=self.config.model)
        self.qdrant = qdrant_manager or QdrantManager.get_instance(config=self.config.qdrant)
        self.session_store = session_store or SessionStore(config=self.config.storage)
        self._cached_tokenizer = None
        self._enrich_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="mem_enrich")
        self._enrich_semaphore = threading.Semaphore(2)
        self._closed: bool = False
        self._closing: bool = False
        self._close_lock = threading.Lock()
        self._teardown_lock = threading.Lock()
        self._shutdown_event = threading.Event()
        self._active_enrich_futures = set()
        self._active_enrich_lock = threading.Lock()

    @property
    def idempotency_retention_days(self) -> int:
        if self.config and hasattr(self.config, "storage") and hasattr(self.config.storage, "idempotency_retention_days"):
            return self.config.storage.idempotency_retention_days
        return 7

    def close(self, timeout: Optional[float] = None) -> None:
        """Reject new work and drain the executor before returning."""
        deadline = None if timeout is None else time.monotonic() + max(0.0, timeout)
        with self._close_lock:
            if self._closed:
                return
            self._closing = True
        acquired = self._teardown_lock.acquire() if deadline is None else self._teardown_lock.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise TimeoutError("MemoryService shutdown is still in progress")
        try:
            if self._closed:
                return
            while True:
                with self._active_enrich_lock:
                    futures = list(self._active_enrich_futures)
                if not futures:
                    break
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError("MemoryService close timed out with active enrichment tasks")
                concurrent.futures.wait(futures, timeout=min(remaining, 0.05) if remaining is not None else 0.05)
            self._enrich_executor.shutdown(wait=True, cancel_futures=True)
            with self._close_lock:
                self._closed = True
            self._shutdown_event.set()
        finally:
            self._teardown_lock.release()

    async def aclose(self, timeout: Optional[float] = None) -> None:
        """异步优雅停机：等待在途富化完成并关闭线程池"""
        await asyncio.to_thread(self.close, timeout=timeout)

    def _discard_active_enrich_future(self, fut: concurrent.futures.Future) -> None:
        with self._active_enrich_lock:
            self._active_enrich_futures.discard(fut)

    def _generate_memory_id(self) -> str:
        """生成符合 mem_YYYYMMDD_xxxxxx 格式的全局唯一 ID"""
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        rand_str = uuid.uuid4().hex[:6]
        return f"mem_{date_str}_{rand_str}"

    def chunk_text_by_tokens(
        self,
        text: str,
        max_tokens: int = MAX_TOKEN_LIMIT,
        overlap: int = OVERLAP_TOKENS,
    ) -> List[str]:
        """
        基于 Token 的文本切片。当文本长度超过 max_tokens (8192) 时，
        使用 overlap (128) 进行滑动窗口切片。
        若引擎尚未加载 tokenizer，则尝试临时加载或退化到安全字符估算。
        """
        tokenizer = getattr(self.engine, "tokenizer", None)
        if tokenizer is None:
            if self._cached_tokenizer is None:
                try:
                    from transformers import AutoTokenizer
                    self._cached_tokenizer = AutoTokenizer.from_pretrained(self.engine.model_path)
                except Exception as e:
                    logger.warning(f"Could not load tokenizer for chunking: {e}, falling back to approx character chunking")
                    self._cached_tokenizer = None
            tokenizer = self._cached_tokenizer

        if tokenizer is not None:
            # 使用真实分词器分块
            tokens = tokenizer.encode(text, add_special_tokens=False)
            if len(tokens) <= max_tokens:
                return [text]

            chunks: List[str] = []
            step = max_tokens - overlap
            if step <= 0:
                step = max_tokens

            for i in range(0, len(tokens), step):
                chunk_tokens = tokens[i : i + max_tokens]
                chunk_str = tokenizer.decode(chunk_tokens, skip_special_tokens=True)
                chunks.append(chunk_str)
                if i + max_tokens >= len(tokens):
                    break
            return chunks
        else:
            # 降级字符切分：中英混合通常 1 token ~ 1.5-2 字符
            # 8192 tokens ~ 16000 字符，128 tokens ~ 256 字符
            char_max = max_tokens * 2
            char_overlap = overlap * 2
            if len(text) <= char_max:
                return [text]

            chunks = []
            step = char_max - char_overlap
            for i in range(0, len(text), step):
                chunk_str = text[i : i + char_max]
                chunks.append(chunk_str)
                if i + char_max >= len(text):
                    break
            return chunks

    async def memory_search(
        self,
        query: str,
        collections: Optional[Union[List[str], str]] = None,
        project_id: Optional[str] = None,
        memory_type: Optional[str] = None,
        type: Optional[Union[str, List[str]]] = None,
        scope: Optional[str] = None,
        limit: int = 5,
        score_threshold: Optional[float] = None,
        include_history: bool = False,
        status: Optional[Union[str, List[str]]] = None,
    ) -> Dict[str, Any]:
        """
        记忆语义检索（检索语义契约 v1，见 DOC-AMR-04-API）
        - 生成 query 的 1024 维向量；
        - 并行在指定的 collections 中检索；
        - 作用域：显式传 project_id → [传入值] ∪ project_fallback_ids；不传 → 全库；
        - 状态可见性：默认仅 ACTIVE；include_history 追加 HISTORICAL/SUPERSEDED；
          PENDING_VERIFY/CONFLICT/TEMPORARY 仅显式传 status 时可见；
        - query 携带 mem_xxx ID 时确定性直取（不受作用域收窄约束，沿 superseded_by 链重定向）；
        - 排名一律用本次查询的实时 vector_score（payload 历史分仅作 stored_score 透出）。
        """
        if not query or not query.strip():
            return {"results": [], "total": 0}

        # JSON 客户端可能把布尔值传成字符串；避免 "false" 被 Python 当作真值。
        if isinstance(include_history, str):
            include_history = include_history.strip().lower() in ("1", "true", "yes", "on")
        else:
            include_history = bool(include_history)

        # 规范化 target collections
        if collections is None:
            target_cols = ["ai_memory"]
        elif isinstance(collections, str):
            if collections == "all":
                target_cols = list(STANDARD_COLLECTIONS)
            else:
                target_cols = [collections]
        else:
            if "all" in collections:
                target_cols = list(STANDARD_COLLECTIONS)
            else:
                target_cols = list(collections)

        # 限制 limit 范围
        limit = max(1, min(limit, 20))

        # F2-2: 调用方未显式给阈值时，应用 config 校准的默认低分截断（config.yaml search.default_score_threshold）
        if score_threshold is None:
            score_threshold = self.config.search.default_score_threshold

        # 1. 向量推理（调用 BGEM3Engine）
        embeddings = await self.engine.embed([query])
        query_vector = embeddings[0]

        # 2. 构建额外过滤条件 (project_id, type, scope)
        filter_conditions: List[models.FieldCondition] = []
        if project_id and project_id != "all":
            # 契约 v1 ①（DOC-AMR-04）：显式传 project_id 时收窄至 [传入值] ∪ project_fallback_ids。
            # 原为单值精确匹配，导致业务项目（aep-* 等）永远召不回 global/general 的通用记忆。
            scope_ids: List[str] = [project_id]
            for fid in self.config.search.project_fallback_ids:
                if fid and fid not in scope_ids:
                    scope_ids.append(fid)
            filter_conditions.append(
                models.FieldCondition(
                    key="project_id",
                    match=models.MatchAny(any=scope_ids),
                )
            )

        # 统一 type 与 memory_type 过滤
        req_type = type or memory_type
        if req_type:
            if isinstance(req_type, list):
                filter_conditions.append(
                    models.FieldCondition(
                        key="type",
                        match=models.MatchAny(any=req_type),
                    )
                )
            elif isinstance(req_type, str) and not req_type.endswith("/*"):
                filter_conditions.append(
                    models.FieldCondition(
                        key="type",
                        match=models.MatchValue(value=req_type),
                    )
                )

        if scope:
            filter_conditions.append(
                models.FieldCondition(
                    key="scope",
                    match=models.MatchValue(value=scope),
                )
            )

        # 确定状态过滤列表（自适应大小写兼容）
        if status:
            raw_statuses = [status] if isinstance(status, str) else list(status)
            expanded_statuses = set()
            for s in raw_statuses:
                expanded_statuses.add(s.lower())
                expanded_statuses.add(s.upper())
            allowed_statuses = list(expanded_statuses)
        elif include_history:
            allowed_statuses = ["ACTIVE", "active", "HISTORICAL", "historical", "SUPERSEDED", "superseded"]
        else:
            allowed_statuses = ["ACTIVE", "active"]

        # F2-1: 精确 ID 短路 —— query 携带 mem_xxx 时直取置顶（契约 v1：命中仍受状态可见性约束）。
        # 若命中点已被接替（SUPERSEDED 且有 superseded_by），沿链重定向到现役版本（最多 3 跳，防环），
        # 使 Agent 持有过期 ID 时仍能取到当前事实。
        exact_hits: List[Dict[str, Any]] = []
        allowed_status_set = {s.upper() for s in allowed_statuses} | {normalize_status(s) for s in allowed_statuses}
        for matched_id in dict.fromkeys(re.findall(r"mem_\d{8}_[0-9a-f]{6,}(?:_chunk_\d+)?", query)):
            redirect_from: Optional[str] = None
            point_info = await asyncio.to_thread(self.qdrant.get_point_by_memory_id, matched_id)
            seen_ids = {matched_id}
            for _hop in range(3):
                if not point_info:
                    break
                payload = point_info.get("payload") or {}
                status_raw = payload.get("status")
                target_id = payload.get("superseded_by")
                if normalize_status(status_raw) == "HISTORICAL" and target_id:
                    if target_id in seen_ids:
                        break
                    seen_ids.add(target_id)
                    redirect_from = redirect_from or matched_id
                    point_info = await asyncio.to_thread(self.qdrant.get_point_by_memory_id, target_id)
                    continue
                break
            if not point_info:
                continue
            payload = point_info.get("payload") or {}
            status_val = (payload.get("status") or "").upper()
            canonical_status = normalize_status(payload.get("status"))
            if status_val and status_val not in allowed_status_set and canonical_status not in allowed_status_set:
                continue
            p_type = payload.get("type") or payload.get("memory_type", "general")
            final_mid = payload.get("memory_id") or matched_id
            created_at_val = payload.get("created_at")
            updated_at_val = payload.get("updated_at")
            if (created_at_val is None or updated_at_val is None) and self.session_store:
                try:
                    db_snap = await asyncio.to_thread(
                        self.session_store.get_memory_projection_snapshot, final_mid
                    )
                    if db_snap:
                        if created_at_val is None:
                            created_at_val = db_snap.get("created_at")
                        if updated_at_val is None:
                            updated_at_val = db_snap.get("updated_at")
                except Exception as ex:
                    logger.debug("Failed to backfill timestamps for %s: %s", final_mid, ex)

            exact_hits.append({
                "memory_id": final_mid,
                "version": payload.get("version", 1),
                "content": payload.get("content", ""),
                "project_id": payload.get("project_id", "global"),
                "type": p_type,
                "status": status_val or "ACTIVE",
                "created_by_agent": payload.get("created_by_agent") or payload.get("source_agent", "system"),
                "updated_by_agent": payload.get("updated_by_agent") or payload.get("source_agent", "system"),
                "score": 1.0,
                "vector_score": 1.0,
                "final_score": 1.0,
                "stored_score": None,
                "matched_by": "exact_id",
                "superseded_from": redirect_from,
                "collection": point_info["collection"],
                "scope": payload.get("scope", "global"),
                "created_at": created_at_val,
                "updated_at": updated_at_val,
                "subject": payload.get("subject", ""),
                "predicate": payload.get("predicate", ""),
                "object": payload.get("object", None),
            })
        if exact_hits:
            logger.info(f"Exact-ID 短路命中 {len(exact_hits)} 条")

        # 3. 在 target collections 中检索
        async def _search_single_collection(col_name: str) -> List[Dict[str, Any]]:
            try:
                scored_points = await asyncio.to_thread(
                    self.qdrant.search_points,
                    collection_name=col_name,
                    query_vector=query_vector,
                    limit=limit * 2 if (isinstance(req_type, str) and req_type.endswith("/*")) else limit,
                    score_threshold=score_threshold,
                    filter_conditions=filter_conditions,
                    allowed_statuses=allowed_statuses,
                )
                items = []
                for sp in scored_points:
                    payload = sp.payload or {}
                    p_type = payload.get("type") or payload.get("memory_type", "general")
                    
                    # 前缀通配匹配 (如 decision/*)
                    if isinstance(req_type, str) and req_type.endswith("/*"):
                        prefix = req_type[:-2]
                        if not p_type.startswith(prefix):
                            continue

                    vec_score = round(float(sp.score), 4)
                    # 契约 v1：排名一律用本次实时向量分；payload 中历史静态分仅作为 stored_score 随行透出，永不参与排名
                    stored_score = payload.get("final_score")
                    if isinstance(stored_score, (int, float)):
                        stored_score = round(float(stored_score), 4)
                    else:
                        stored_score = None
                    final_score = vec_score

                    items.append({
                        "memory_id": payload.get("memory_id"),
                        "version": payload.get("version", 1),
                        "content": payload.get("content", ""),
                        "project_id": payload.get("project_id", "global"),
                        "type": p_type,
                        "status": (payload.get("status") or "ACTIVE").upper(),
                        "created_by_agent": payload.get("created_by_agent") or payload.get("source_agent", "system"),
                        "updated_by_agent": payload.get("updated_by_agent") or payload.get("source_agent", "system"),
                        "score": final_score,
                        "vector_score": vec_score,
                        "final_score": final_score,
                        "stored_score": stored_score,
                        "matched_by": "semantic",
                        "collection": col_name,
                        "scope": payload.get("scope", "global"),
                        "created_at": payload.get("created_at"),
                        "updated_at": payload.get("updated_at"),
                        "subject": payload.get("subject", ""),
                        "predicate": payload.get("predicate", ""),
                        "object": payload.get("object", None),
                    })
                return items
            except Exception as e:
                logger.error(f"Search collection '{col_name}' failed: {e}")
                return []

        search_tasks = [_search_single_collection(col) for col in target_cols]
        search_results = await asyncio.gather(*search_tasks)

        # 4. 聚合多集合结果并排序（F2-1 精确命中置顶，其余按实时向量分降序、平局按 updated_at 新者优先）
        all_results: List[Dict[str, Any]] = []
        for res_list in search_results:
            all_results.extend(res_list)

        all_results.sort(key=lambda x: (x.get("final_score", 0.0), x.get("updated_at") or 0), reverse=True)
        merged_results: List[Dict[str, Any]] = []
        seen_memory_ids: set = set()
        for item in exact_hits + all_results:
            mid = item.get("memory_id")
            if mid is not None and mid in seen_memory_ids:
                continue
            if mid is not None:
                seen_memory_ids.add(mid)
            merged_results.append(item)
        final_results = merged_results[:limit]

        return {
            "results": final_results,
            "total": len(final_results),
        }

    async def memory_record(
        self,
        content: str,
        memory_type: str = "fact",
        scope: str = "global",
        project_id: Optional[str] = None,
        source_agent: Optional[str] = None,
        session_id: Optional[str] = None,
        source_message_ids: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
        collection_name: str = "ai_memory",
    ) -> Dict[str, Any]:
        """
        显式沉淀记忆（legacy 通道，已补齐 v3 SSOT 底账）
        - 检查文本长度，当输入超过 8192 Token 时，自动按照带 128 Tokens 滑动重叠的窗口切片（Chunks）；
        - 单 chunk 直接生成 1024 维向量；多 chunk 生成多个向量并关联相同的 parent_memory_id 与 chunk_index；
        - 每个分片同步写入 SQLite memories 主表（1 点位 ↔ 1 行，qdrant_point_id 对齐）并经
          Transactional Outbox 保证投影自愈——修复历史"只写 Qdrant 无底账"被每日对账判定为孤儿投影下架的问题；
        - payload 保持 legacy 语义（memory_type / scope / tags / parent_memory_id / 会话溯源），
          与 v3 9 字段投影并存，待 Phase 3 tags 落地后再评估收敛；
        - 返回 memory_id, chunks_created, status="active", created_at。
        """
        if not content or not content.strip():
            raise ValueError("content must not be empty")

        now_ts = int(time.time())
        primary_memory_id = self._generate_memory_id()
        tags = tags or []
        source_message_ids = source_message_ids or []

        # 1. 切片判定
        chunks = self.chunk_text_by_tokens(
            text=content,
            max_tokens=MAX_TOKEN_LIMIT,
            overlap=OVERLAP_TOKENS,
        )
        total_chunks = len(chunks)

        # 2. 批量生成向量
        embeddings = await self.engine.embed(chunks)

        # 3. 构造 PointStruct 实体写入 Qdrant
        points: List[models.PointStruct] = []
        chunk_payloads: List[Dict[str, Any]] = []
        for idx, (chunk_text, vector) in enumerate(zip(chunks, embeddings)):
            chunk_mem_id = primary_memory_id if total_chunks == 1 else f"{primary_memory_id}_chunk_{idx}"
            point_uuid = str(uuid.uuid4())

            payload: Dict[str, Any] = {
                "memory_id": chunk_mem_id,
                "parent_memory_id": primary_memory_id if total_chunks > 1 else None,
                "chunk_index": idx,
                "total_chunks": total_chunks,
                "content": chunk_text,
                "memory_type": memory_type,
                "status": "active",
                "superseded_by": None,
                "scope": scope,
                "project_id": project_id,
                "source_agent": source_agent,
                "session_id": session_id,
                "source_message_ids": source_message_ids,
                "meta": {
                    "tags": tags,
                },
                "created_at": now_ts,
                "updated_at": now_ts,
            }

            points.append(
                models.PointStruct(
                    id=point_uuid,
                    vector=vector,
                    payload=payload,
                )
            )
            chunk_payloads.append({**payload, "point_id": point_uuid})

        # 4. SQLite SSOT 底账：1 分片 ↔ 1 行 memories 记录，并写入 Outbox（payload 快照与上方投影一致，
        #    保证 Worker 回放是幂等写入而不是覆盖成另一种 schema）
        evidence = [
            {"message_id": msg_id, "session_id": session_id or "", "evidence_strength": 0.5}
            for msg_id in source_message_ids
        ]
        for payload_with_point in chunk_payloads:
            await asyncio.to_thread(
                self.session_store.create_memory,
                memory_id=payload_with_point["memory_id"],
                subject="",
                predicate="",
                content=payload_with_point["content"],
                type=memory_type or "general",
                status="active",
                project_id=project_id or "global",
                scope=scope or "global",
                source_agent=source_agent or "system",
                root_memory_id=primary_memory_id,
                qdrant_point_id=payload_with_point["point_id"],
                evidence=evidence,
                operator=source_agent or "system",
                qdrant_payload=payload_with_point,
                collection_name=collection_name,
            )

        # 写入 Qdrant
        await asyncio.to_thread(
            self.qdrant.upsert_points,
            collection_name=collection_name,
            points=points,
        )

        if collection_name != "ai_memory":
            # 发件箱队列不携带 collection 维度，非默认集合的投影已由本方法直接写入，
            # 关闭对应任务，避免 Outbox Worker 把同一点位误推送到默认集合 ai_memory
            for payload_with_point in chunk_payloads:
                await asyncio.to_thread(
                    self.session_store.mark_pending_sync_tasks_done,
                    payload_with_point["memory_id"],
                )

        return {
            "memory_id": primary_memory_id,
            "status": "active",
            "chunks_created": total_chunks,
            "created_at": now_ts,
        }

    async def memory_get(self, memory_id: str) -> Optional[Dict[str, Any]]:
        """
        精准提取记忆与溯源 (SQLite SSOT + Best-effort Payload 富化)
        - 先从 SQLite 读取 (SessionStore.get_memory)；
        - SQLite 命中时，权威字段全部取自 SQLite，绝不被 payload 覆盖；
        - 富化唯一 legacy 键：meta.tags、parent_memory_id、chunk_index、total_chunks、session_id、source_message_ids、raw_messages；
        - 工程约束：专用线程池(2)，信号量(2)非阻塞获取(拿不到立即 degraded)，wait_for(0.2s)超时降级；
        - SQLite 无行时退回 Qdrant 路径 (enrichment="ok")；
        - SQLite 自身异常如实抛出。
        """
        # 1. 尝试从 SQLite (SSOT) 读取
        mem_row = await asyncio.to_thread(self.session_store.get_memory, memory_id)

        # 2. SQLite 无行：退回现 Qdrant 路径（保持旧返回形态，enrichment="ok"）
        if mem_row is None:
            point_info = await asyncio.to_thread(self.qdrant.get_point_by_memory_id, memory_id)
            if not point_info:
                return None

            payload = point_info["payload"] or {}
            sess_id = payload.get("session_id")
            src_msg_ids = payload.get("source_message_ids") or []

            raw_messages: List[Dict[str, Any]] = []
            if sess_id and src_msg_ids:
                try:
                    db_msgs = await asyncio.to_thread(
                        self.session_store.get_messages,
                        session_id=sess_id,
                        message_ids=src_msg_ids,
                    )
                    raw_messages = [
                        {
                            "message_id": m.get("message_id"),
                            "role": m.get("role"),
                            "content": m.get("content"),
                            "timestamp": m.get("timestamp"),
                        }
                        for m in (db_msgs or [])
                    ]
                except Exception as e:
                    logger.error(f"Failed to fetch raw messages for session {sess_id}: {e}")

            p_type = payload.get("type") or payload.get("memory_type", "general")
            return {
                "memory_id": payload.get("memory_id"),
                "parent_memory_id": payload.get("parent_memory_id"),
                "chunk_index": payload.get("chunk_index", 0),
                "total_chunks": payload.get("total_chunks", 1),
                "content": payload.get("content", ""),
                "memory_type": p_type,
                "type": p_type,
                "status": payload.get("status"),
                "superseded_by": payload.get("superseded_by"),
                "scope": payload.get("scope"),
                "project_id": payload.get("project_id"),
                "source_agent": payload.get("source_agent"),
                "session_id": sess_id,
                "source_message_ids": src_msg_ids,
                "raw_messages": raw_messages,
                "meta": payload.get("meta", {}),
                "created_at": payload.get("created_at"),
                "updated_at": payload.get("updated_at"),
                "enrichment": "ok",
            }

        # 3. SQLite 命中：提取权威字段（绝不被 payload 覆盖）
        mid = mem_row["memory_id"]
        content = mem_row["content"]
        m_type = mem_row.get("type") or "general"
        m_status = mem_row.get("status") or "ACTIVE"
        project_id = mem_row.get("project_id", "global")
        scope = mem_row.get("scope", "global")
        source_agent = mem_row.get("source_agent") or mem_row.get("created_by_agent", "system")
        created_by_agent = mem_row.get("created_by_agent") or mem_row.get("source_agent", "system")
        updated_by_agent = mem_row.get("updated_by_agent") or source_agent
        version = mem_row.get("version", 1)
        superseded_by = mem_row.get("superseded_by")
        created_at = mem_row.get("created_at")
        updated_at = mem_row.get("updated_at")
        p_class = mem_row.get("projection_class", "legacy")

        # 提取 SQLite 可得的溯源与族信息
        root_mid = mem_row.get("root_memory_id")
        parent_memory_id = root_mid if (root_mid and root_mid != mid) else None
        evidence = mem_row.get("evidence") or []
        session_id = evidence[0]["session_id"] if evidence else None
        source_message_ids = [e["message_id"] for e in evidence if "message_id" in e] if evidence else None
        chunk_index = None
        total_chunks = None
        meta = None

        # 4. Best-effort 富化（工程约束：专用线程池、非阻塞信号量、0.2s 超时）
        enrichment_status = "degraded"
        payload = None

        if not self._closing and not self._closed:
            acquired = self._enrich_semaphore.acquire(blocking=False)
            if acquired:
                fut = None
                try:
                    with self._close_lock:
                        if self._closing or self._closed:
                            raise RuntimeError("Service is closed or closing")
                        fut = self._enrich_executor.submit(
                            self.qdrant.get_point_by_memory_id,
                            mid,
                        )
                        with self._active_enrich_lock:
                            self._active_enrich_futures.add(fut)
                    fut.add_done_callback(lambda f: self._discard_active_enrich_future(f))

                    point_info = await asyncio.wait_for(
                        asyncio.wrap_future(fut),
                        timeout=0.2,
                    )
                    if point_info and point_info.get("payload"):
                        payload = point_info["payload"]
                    enrichment_status = "ok"
                except Exception as e:
                    logger.warning(f"Qdrant enrichment degraded for {mid}: {e}")
                    enrichment_status = "degraded"
                finally:
                    self._enrich_semaphore.release()
            else:
                logger.warning(f"Qdrant enrichment degraded for {mid}: semaphore busy (zero queue)")
                enrichment_status = "degraded"
        else:
            enrichment_status = "degraded"

        if enrichment_status == "ok" and payload:
            if parent_memory_id is None:
                parent_memory_id = payload.get("parent_memory_id")
            if chunk_index is None:
                chunk_index = payload.get("chunk_index")
            if total_chunks is None:
                total_chunks = payload.get("total_chunks")
            if session_id is None:
                session_id = payload.get("session_id")
            if source_message_ids is None:
                source_message_ids = payload.get("source_message_ids")
            if meta is None:
                meta = payload.get("meta")

        # 5. raw_messages 溯源消息提取 (同样 best-effort)
        raw_messages: Optional[List[Dict[str, Any]]] = None
        if session_id and source_message_ids:
            try:
                db_msgs = await asyncio.to_thread(
                    self.session_store.get_messages,
                    session_id=session_id,
                    message_ids=source_message_ids,
                )
                raw_messages = [
                    {
                        "message_id": m.get("message_id"),
                        "role": m.get("role"),
                        "content": m.get("content"),
                        "timestamp": m.get("timestamp"),
                    }
                    for m in (db_msgs or [])
                ]
            except Exception as e:
                logger.warning(f"Failed to fetch raw messages for session {session_id}: {e}")
                raw_messages = None
        else:
            if enrichment_status == "ok":
                raw_messages = []
            else:
                raw_messages = None

        # 6. legacy 兼容的状态映射
        if p_class == "legacy":
            if m_status in ("HISTORICAL", "superseded") and superseded_by:
                ret_status = "superseded"
            elif m_status in ("HISTORICAL", "archived") and not superseded_by:
                ret_status = "archived"
            elif m_status in ("DELETED", "deleted"):
                ret_status = "deleted"
            elif m_status in ("ACTIVE", "active"):
                ret_status = "active"
            else:
                ret_status = m_status
        else:
            ret_status = normalize_status(m_status)

        result: Dict[str, Any] = {
            "memory_id": mid,
            "content": content,
            "type": m_type,
            "memory_type": m_type,
            "status": ret_status,
            "project_id": project_id,
            "scope": scope,
            "source_agent": source_agent,
            "created_by_agent": created_by_agent,
            "updated_by_agent": updated_by_agent,
            "version": version,
            "superseded_by": superseded_by,
            "created_at": created_at,
            "updated_at": updated_at,
            "enrichment": enrichment_status,
            "meta": meta,
            "parent_memory_id": parent_memory_id,
            "chunk_index": chunk_index,
            "total_chunks": total_chunks,
            "session_id": session_id,
            "source_message_ids": source_message_ids,
            "raw_messages": raw_messages,
        }
        if p_class == "v3":
            try:
                result["source_refs"] = json.loads(mem_row.get("source_refs") or "[]")
            except Exception:
                result["source_refs"] = []
            try:
                result["conflicts_with"] = json.loads(mem_row.get("conflicts_with") or "[]")
            except Exception:
                result["conflicts_with"] = []
            result["projection_revision"] = mem_row.get("projection_revision", 0)
            result["collection_name"] = mem_row.get("collection_name", "ai_memory")
            result["projection_class"] = "v3"

        if mem_row.get("deletion_reason") is not None:
            result["deletion_reason"] = mem_row.get("deletion_reason")

        return result

    async def memory_update_status(
        self,
        memory_id: str,
        new_status: str,
        superseded_by: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        记忆状态流转 (统一分派 store.change_memory_status)
        - 使用 normalize_status 校验 canonical 6 态；
        - HISTORICAL 需 superseded_by（保留旧契约但映射 legacy 'superseded'）；
        - 调 store.change_memory_status(actor=..., superseded_by=...)；
        - 返回 previous_status / current_status / updated_at。
        """
        canonical = normalize_status(new_status)
        if canonical not in CANONICAL_STATUSES:
            raise ValueError(f"Invalid status '{new_status}', must be one of {CANONICAL_STATUSES}")
        if (canonical == "HISTORICAL" or new_status.lower() == "superseded") and new_status.lower() != "archived" and not superseded_by:
            raise ValueError("superseded_by is required when new_status is 'superseded'")

        res = await asyncio.to_thread(
            self.session_store.change_memory_status,
            memory_id=memory_id,
            new_status=canonical,
            actor="system",
            superseded_by=superseded_by,
        )

        prev_st = res.get("previous_status")
        curr_st = res.get("current_status")
        if new_status in ("superseded", "archived", "deleted", "active"):
            curr_st = new_status
        if prev_st and prev_st.upper() == "ACTIVE" and new_status.islower():
            prev_st = "active"

        return {
            "memory_id": memory_id,
            "previous_status": prev_st,
            "current_status": curr_st,
            "updated_at": res.get("updated_at", int(time.time())),
        }

    async def memory_ingest_session(
        self,
        session_id: str,
        agent_id: str,
        project_id: Optional[str],
        messages: List[Dict[str, Any]],
        checkpoint: Optional[Dict[str, Any]] = None,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        原始会话流水幂等摄取，支持附带会话检查点
        直接转发调用 SessionStore.ingest_messages 进行流水幂等入库，不调用大模型。
        """
        return await asyncio.to_thread(
            self.session_store.ingest_messages,
            session_id=session_id,
            agent_id=agent_id,
            project_id=project_id,
            messages=messages,
            checkpoint=checkpoint,
            idempotency_key=idempotency_key,
        )

    async def session_checkpoint(
        self,
        session_id: str,
        agent_id: str,
        project_id: str,
        completed: Any,
        in_progress: Any = None,
        next_steps: Any = None,
        context: Optional[Union[Dict[str, Any], str]] = None,
        checkpoint_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        messages: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        持久化会话检查点与跨 Agent 交接（完成、未完成、下一步）。
        """
        return await asyncio.to_thread(
            self.session_store.save_checkpoint,
            session_id=session_id,
            agent_id=agent_id,
            project_id=project_id,
            completed=completed,
            in_progress=in_progress,
            next_steps=next_steps,
            context=context,
            checkpoint_id=checkpoint_id,
            idempotency_key=idempotency_key,
            messages=messages,
        )

    async def session_get_handoff(
        self,
        project_id: str,
        session_id: Optional[str] = None,
        limit: int = 1,
    ) -> Dict[str, Any]:
        """
        按项目检索结构化交接记录（完成、未完成、下一步）。
        严格隔离跨项目数据。
        """
        return await asyncio.to_thread(
            self.session_store.get_handoff,
            project_id=project_id,
            session_id=session_id,
            limit=limit,
        )

    # =========================================================================
    # v3.0 Unified Multi-Agent Semantic Operations
    # =========================================================================

    async def _create_v3_memory(
        self,
        content: str,
        project_id: str = "global",
        type: str = "general",
        status: str = "ACTIVE",
        agent_id: str = "system",
        source_refs: Optional[List[str]] = None,
        conflicts_with: Optional[List[str]] = None,
        collection_name: str = "ai_memory",
        idempotency_key: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        旧方法名薄包装（供旧调用兼容，零同步 I/O，直接代理到 memory_create）。
        """
        return await self.memory_create(
            content=content,
            project_id=project_id,
            type=type,
            status=status,
            agent_id=agent_id,
            source_refs=source_refs,
            conflicts_with=conflicts_with,
            collection_name=collection_name,
            idempotency_key=idempotency_key,
            request_id=request_id,
        )

    async def memory_create(
        self,
        content: str,
        project_id: str = "global",
        type: str = "general",
        status: str = "ACTIVE",
        agent_id: str = "system",
        source_refs: Optional[List[str]] = None,
        conflicts_with: Optional[List[str]] = None,
        collection_name: str = "ai_memory",
        idempotency_key: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        v3.0 标准创建记忆（SQLite 提交即成功，零 embed、零 Qdrant I/O）：
        - 服务端 ID 在事务内生成并进快照；
        - store.execute_idempotent 带幂等仲裁与事务隔离；
        - business_fn 调 store.create_memory_v3(cursor=cursor, collection_name=..., request_id=...)；
        - 返回 result 至少含 memory_id/version/status/projection_status="pending"/projection_revision/collection_name；
        - 重放时附 idempotent_replay=True。
        """
        if not content or not content.strip():
            raise ValueError("content must not be empty")

        req_id = request_id or get_request_id()
        canonical_st = normalize_status(status)

        # 幂等摘要字段：content/project_id/type/status/agent_id/source_refs/conflicts_with/collection_name（排除 memory_id）
        digest_payload = {
            "content": content,
            "project_id": project_id or "global",
            "type": type or "general",
            "status": canonical_st,
            "agent_id": agent_id or "system",
            "source_refs": source_refs,
            "conflicts_with": conflicts_with,
            "collection_name": collection_name or "ai_memory",
        }
        digest = compute_request_digest("create", digest_payload)

        def business_fn(cur):
            mid = self._generate_memory_id()
            res = self.session_store.create_memory_v3(
                memory_id=mid,
                content=content,
                project_id=project_id or "global",
                type=type or "general",
                status=canonical_st,
                created_by_agent=agent_id or "system",
                source_refs=source_refs,
                conflicts_with=conflicts_with,
                collection_name=collection_name or "ai_memory",
                request_id=req_id,
                cursor=cur,
            )
            return (mid, res)

        res = await asyncio.to_thread(
            self.session_store.execute_idempotent,
            "create",
            idempotency_key,
            digest,
            business_fn,
            retention_days=self.idempotency_retention_days,
            agent_id_raw=agent_id or "default",
        )
        return res

    async def memory_update(
        self,
        memory_id: str,
        content: Optional[str] = None,
        type: Optional[str] = None,
        status: Optional[str] = None,
        expected_version: Optional[int] = None,
        change_reason: Optional[str] = None,
        agent_id: str = "system",
        conflicts_with: Optional[List[str]] = None,
        collection_name: str = "ai_memory",
        idempotency_key: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        v3.0 标准修改记忆（SQLite 提交即成功，零 embed、零 Qdrant I/O）：
        - 先读 store.get_memory(memory_id) 判断 projection_class：
            - 'legacy'：content 变更 -> raise LegacyPathForbidden；type 变更 -> LegacyPathForbidden；
              status 变更 -> store.change_memory_status(cursor=None, ...)；不允许走 update_memory_v3。
            - 'v3'：execute_idempotent("update", ...) -> store.update_memory_v3(cursor=cursor)。
        - digest 字段：memory_id/content/type/status/expected_version/change_reason/agent_id/conflicts_with/collection_name。
        - 返回 projection_status="pending"。
        """
        if not memory_id or not memory_id.strip():
            raise ValueError("memory_id must not be empty")

        mem_row = await asyncio.to_thread(self.session_store.get_memory, memory_id)
        if mem_row is None:
            def _check_family():
                with self.session_store._lock:
                    con = self.session_store._get_connection()
                    rows = self.session_store.resolve_memory_family(con.cursor(), memory_id)
                    return dict(rows[0]) if rows else None
            mem_row = await asyncio.to_thread(_check_family)
        if mem_row is None:
            raise KeyError(f"Memory '{memory_id}' not found")

        p_class = mem_row.get("projection_class", "legacy")
        if p_class == "legacy":
            if content is not None:
                raise LegacyPathForbidden(
                    memory_id=memory_id,
                    projection_class=p_class,
                    hint="legacy 记忆不支持正文变更",
                )
            if type is not None:
                raise LegacyPathForbidden(
                    memory_id=memory_id,
                    projection_class=p_class,
                    hint="legacy 记忆不支持类型变更",
                )
            if status is not None:
                canonical_st = normalize_status(status)
                digest_payload = {
                    "memory_id": memory_id,
                    "content": content,
                    "type": type,
                    "status": canonical_st,
                    "expected_version": expected_version,
                    "change_reason": change_reason,
                    "agent_id": agent_id or "system",
                    "conflicts_with": conflicts_with,
                    "collection_name": collection_name or "ai_memory",
                }
                digest = compute_request_digest("update", digest_payload)

                def legacy_fn(cur):
                    st_res = self.session_store.change_memory_status(
                        memory_id=memory_id,
                        new_status=canonical_st,
                        actor=agent_id or "system",
                        reason=change_reason,
                        cursor=cur,
                    )
                    st_res["projection_status"] = "pending"
                    return (memory_id, st_res)

                res = await asyncio.to_thread(
                    self.session_store.execute_idempotent,
                    "update",
                    idempotency_key,
                    digest,
                    legacy_fn,
                    retention_days=self.idempotency_retention_days,
                    agent_id_raw=agent_id or "default",
                )
                if "projection_status" not in res:
                    res["projection_status"] = "pending"
                return res

            return {**mem_row, "projection_status": "pending"}

        # v3 路径
        canonical_st = normalize_status(status) if status is not None else None
        digest_payload = {
            "memory_id": memory_id,
            "content": content,
            "type": type,
            "status": canonical_st,
            "expected_version": expected_version,
            "change_reason": change_reason,
            "agent_id": agent_id or "system",
            "conflicts_with": conflicts_with,
            "collection_name": collection_name or "ai_memory",
        }
        digest = compute_request_digest("update", digest_payload)

        def v3_fn(cur):
            up_res = self.session_store.update_memory_v3(
                memory_id=memory_id,
                content=content,
                type=type,
                status=canonical_st,
                expected_version=expected_version,
                change_reason=change_reason,
                agent_id=agent_id or "system",
                conflicts_with=conflicts_with,
                cursor=cur,
            )
            return (memory_id, up_res)

        res = await asyncio.to_thread(
            self.session_store.execute_idempotent,
            "update",
            idempotency_key,
            digest,
            v3_fn,
            retention_days=self.idempotency_retention_days,
            agent_id_raw=agent_id or "default",
        )
        if "projection_status" not in res:
            res["projection_status"] = "pending"
        return res

    async def memory_history(self, memory_id: str) -> Dict[str, Any]:
        """查询某条记忆的所有历史修订版本轨迹"""
        item = await asyncio.to_thread(self.session_store.get_memory_v3, memory_id)
        if not item:
            raise KeyError(f"Memory '{memory_id}' not found")
        revisions = await asyncio.to_thread(self.session_store.get_memory_revisions, memory_id)
        return {
            "memory_id": memory_id,
            "current_version": item["version"],
            "current_content": item["content"],
            "status": item["status"],
            "type": item["type"],
            "revisions": revisions,
        }

    async def memory_delete(
        self,
        memory_id: str,
        agent_id: str = "system",
        reason: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        逻辑软删除记忆（SQLite 提交即成功，零 Qdrant I/O）：
        - 统一走 store.change_memory_status(memory_id, "DELETED", actor=agent_id, reason=reason)；
          v3 行走新队列（Worker 删点），legacy 行走 amr_delta_v1 op='delete'；
        - 支持 idempotency_key（digest 含 memory_id/reason/agent_id）；
        - 返回 projection_status="pending"。
        """
        if not memory_id or not memory_id.strip():
            raise ValueError("memory_id must not be empty")

        digest_payload = {
            "memory_id": memory_id,
            "reason": reason,
            "agent_id": agent_id or "system",
        }
        digest = compute_request_digest("delete", digest_payload)

        def delete_fn(cur):
            del_res = self.session_store.change_memory_status(
                memory_id=memory_id,
                new_status="DELETED",
                actor=agent_id or "system",
                reason=reason,
                cursor=cur,
            )
            del_res["projection_status"] = "pending"
            return (memory_id, del_res)

        res = await asyncio.to_thread(
            self.session_store.execute_idempotent,
            "delete",
            idempotency_key,
            digest,
            delete_fn,
            retention_days=self.idempotency_retention_days,
            agent_id_raw=agent_id or "default",
        )
        if "projection_status" not in res:
            res["projection_status"] = "pending"
        return res

