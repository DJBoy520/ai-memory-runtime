"""
AI Memory Runtime - Memory Service 核心语义服务层
实现记忆语义检索、记忆持久化、超长 Token 滑动切片、四态流转及原始会话流水幂等摄取
遵循 DOC-AMR-03-DDD / DOC-AMR-04-API 规范
"""

import asyncio
from datetime import datetime, timezone
import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple, Union
import uuid

from qdrant_client import models

from config.settings import AppConfig, load_config
from src.core.engine import BGEM3Engine
from src.core.qdrant import QdrantManager, STANDARD_COLLECTIONS
from src.core.session_store import SessionStore

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
        allowed_status_set = {s.upper() for s in allowed_statuses}
        for matched_id in dict.fromkeys(re.findall(r"mem_\d{8}_[0-9a-f]{6,}(?:_chunk_\d+)?", query)):
            redirect_from: Optional[str] = None
            point_info = await asyncio.to_thread(self.qdrant.get_point_by_memory_id, matched_id)
            for _hop in range(3):
                if not point_info:
                    break
                payload = point_info["payload"] or {}
                status_val = (payload.get("status") or "").upper()
                if status_val == "SUPERSEDED" and payload.get("superseded_by"):
                    redirect_from = redirect_from or matched_id
                    point_info = await asyncio.to_thread(self.qdrant.get_point_by_memory_id, payload["superseded_by"])
                    continue
                break
            if not point_info:
                continue
            payload = point_info["payload"] or {}
            status_val = (payload.get("status") or "").upper()
            if status_val and status_val not in allowed_status_set:
                continue
            p_type = payload.get("type") or payload.get("memory_type", "general")
            exact_hits.append({
                "memory_id": payload.get("memory_id"),
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
                "created_at": payload.get("created_at"),
                "updated_at": payload.get("updated_at"),
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
        精准提取记忆与溯源
        - 从 Qdrant 获取记忆详情及元数据；
        - 若包含 session_id 与 source_message_ids，从 SessionStore 查询原始对话内容并组装 raw_messages 一起返回；
        - 若该 memory_id 存在多个切片，将组合完整信息。
        """
        point_info = await asyncio.to_thread(self.qdrant.get_point_by_memory_id, memory_id)
        if not point_info:
            return None

        payload = point_info["payload"]
        sess_id = payload.get("session_id")
        src_msg_ids = payload.get("source_message_ids") or []

        # 从 SessionStore 查询关联的原始对话
        raw_messages: List[Dict[str, Any]] = []
        if sess_id and src_msg_ids:
            try:
                db_msgs = self.session_store.get_messages(
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
                    for m in db_msgs
                ]
            except Exception as e:
                logger.error(f"Failed to fetch raw messages for session {sess_id}: {e}")

        result: Dict[str, Any] = {
            "memory_id": payload.get("memory_id"),
            "parent_memory_id": payload.get("parent_memory_id"),
            "chunk_index": payload.get("chunk_index", 0),
            "total_chunks": payload.get("total_chunks", 1),
            "content": payload.get("content", ""),
            "memory_type": payload.get("memory_type"),
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
        }
        return result

    async def memory_update_status(
        self,
        memory_id: str,
        new_status: str,
        superseded_by: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        记忆四态流转 (active / superseded / archived / deleted)
        - 验证状态合法性；
        - 若为 superseded，必须提供 superseded_by；
        - 更新 Qdrant 中该 memory_id 及其对应 chunk 的 payload 状态；
        - 返回 { memory_id, previous_status, current_status, updated_at }。
        """
        if new_status not in VALID_STATUSES:
            raise ValueError(f"Invalid status '{new_status}', must be one of {VALID_STATUSES}")

        if new_status == "superseded" and not superseded_by:
            raise ValueError("superseded_by is required when new_status is 'superseded'")

        # 获取当前状态
        current_item = await self.memory_get(memory_id)
        if not current_item:
            raise KeyError(f"Memory with ID '{memory_id}' not found")

        prev_status = current_item["status"]
        now_ts = int(time.time())

        payload_updates: Dict[str, Any] = {
            "status": new_status,
            "updated_at": now_ts,
        }
        if new_status == "superseded":
            payload_updates["superseded_by"] = superseded_by

        # 执行更新
        await asyncio.to_thread(
            self.qdrant.update_payload_by_memory_id,
            memory_id=memory_id,
            payload_updates=payload_updates,
        )

        return {
            "memory_id": memory_id,
            "previous_status": prev_status,
            "current_status": new_status,
            "updated_at": now_ts,
        }

    async def memory_ingest_session(
        self,
        session_id: str,
        agent_id: str,
        project_id: Optional[str],
        messages: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        原始会话流水幂等摄取
        直接转发调用 SessionStore.ingest_messages 进行流水幂等入库，不调用大模型。
        """
        return await asyncio.to_thread(
            self.session_store.ingest_messages,
            session_id=session_id,
            agent_id=agent_id,
            project_id=project_id,
            messages=messages,
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
    ) -> Dict[str, Any]:
        """
        v3.0 记忆创建内核（memory_create 标准入口）：
        - 生成 memory_id；
        - SQLite SSOT 原子入库（memories + revisions + Outbox + audit）；
        - Qdrant 确定性 point_id(UUIDv5) 写入 9 字段 Payload。
        """
        memory_id = self._generate_memory_id()
        point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, memory_id))
        text = content.strip()

        # 1. 向量生成 (调用 BGEM3Engine)
        embeddings = await self.engine.embed([text])
        vector = embeddings[0]

        # 2. SQLite SSOT 原子入库 (含 Outbox 与双向冲突标记)
        store_res = await asyncio.to_thread(
            self.session_store.create_memory_v3,
            memory_id=memory_id,
            content=text,
            project_id=project_id or "global",
            type=type or "general",
            status=status or "ACTIVE",
            created_by_agent=agent_id,
            source_refs=source_refs or [],
            conflicts_with=conflicts_with or [],
            qdrant_point_id=point_id,
            operator=agent_id,
        )

        # 3. 构造 9 字段 Payload 写入 Qdrant
        now_ts = int(time.time())
        payload = {
            "memory_id": memory_id,
            "version": 1,
            "content": text,
            "project_id": project_id or "global",
            "type": type or "general",
            "status": (status or "ACTIVE").upper(),
            "created_by_agent": agent_id,
            "updated_by_agent": agent_id,
            "updated_at": now_ts,
        }

        point = models.PointStruct(
            id=point_id,
            vector=vector,
            payload=payload,
        )

        await asyncio.to_thread(
            self.qdrant.upsert_points,
            collection_name=collection_name,
            points=[point],
        )

        return store_res

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
    ) -> Dict[str, Any]:
        """
        v3.0 标准创建记忆：
        - 校验内容与 6 态规则
        - 由 SessionStore 生成 SQLite 实体、版本快照(v1)与发件箱任务
        - 计算 1024 维向量并同步写入 Qdrant 9 字段 Payload
        - 返回标准记忆对象
        """
        if not content or not content.strip():
            raise ValueError("content must not be empty")

        return await self._create_v3_memory(
            content=content,
            project_id=project_id,
            type=type,
            status=status,
            agent_id=agent_id,
            source_refs=source_refs,
            conflicts_with=conflicts_with,
            collection_name=collection_name,
        )

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
    ) -> Dict[str, Any]:
        """
        v3.0 标准修改记忆：
        - 仅修改 type / status：就地更新元数据，不递增版本，不写 revisions 表，直接更新 Qdrant Payload
        - 修改 content：
            - 必须校验 expected_version（乐观锁）
            - 必须提供 change_reason
            - version ++ 并落入 memory_revisions 表
            - 重算 Embedding 向量并覆盖 Qdrant Point
        """
        # 1. 执行 SQLite SSOT 更新与校验
        updated_item = await asyncio.to_thread(
            self.session_store.update_memory_v3,
            memory_id=memory_id,
            content=content,
            type=type,
            status=status,
            expected_version=expected_version,
            change_reason=change_reason,
            agent_id=agent_id,
            conflicts_with=conflicts_with,
        )

        # 2. Qdrant 同步
        point_id = updated_item["qdrant_point_id"]
        now_ts = int(time.time())

        if content is not None and content.strip() != "":
            # 内容变更：重算向量并覆盖
            embeddings = await self.engine.embed([content.strip()])
            new_vector = embeddings[0]

            payload = {
                "memory_id": memory_id,
                "version": updated_item["version"],
                "content": content.strip(),
                "project_id": updated_item["project_id"],
                "type": updated_item["type"],
                "status": (updated_item["status"] or "ACTIVE").upper(),
                "created_by_agent": updated_item["created_by_agent"],
                "updated_by_agent": agent_id,
                "updated_at": now_ts,
            }
            point = models.PointStruct(
                id=point_id,
                vector=new_vector,
                payload=payload,
            )
            await asyncio.to_thread(
                self.qdrant.upsert_points,
                collection_name=collection_name,
                points=[point],
            )
        else:
            # 仅元数据更新：update_payload
            payload_updates = {
                "type": updated_item["type"],
                "status": (updated_item["status"] or "ACTIVE").upper(),
                "updated_by_agent": agent_id,
                "updated_at": now_ts,
            }
            await asyncio.to_thread(
                self.qdrant.update_payload_by_memory_id,
                memory_id=memory_id,
                payload_updates=payload_updates,
            )

        return updated_item

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
    ) -> Dict[str, Any]:
        """逻辑软删除记忆：状态置为 DELETED，常规与历史检索隐藏，保留底账与审计"""
        return await self.memory_update(
            memory_id=memory_id,
            status="DELETED",
            change_reason=reason or "Soft deleted via memory_delete",
            agent_id=agent_id,
        )

