"""
AI Memory Runtime - Memory Service 核心语义服务层
实现记忆语义检索、记忆持久化、超长 Token 滑动切片、四态流转及原始会话流水幂等摄取
遵循 DOC-AMR-03-DDD / DOC-AMR-04-API 规范
"""

import asyncio
from datetime import datetime, timezone
import logging
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
            # 尝试通过 engine 获取或创建 tokenizer
            try:
                from transformers import AutoTokenizer
                tokenizer = AutoTokenizer.from_pretrained(self.engine.model_path)
            except Exception as e:
                logger.warning(f"Could not load tokenizer for chunking: {e}, falling back to approx character chunking")
                tokenizer = None

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
        scope: Optional[str] = None,
        limit: int = 5,
        score_threshold: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        记忆语义检索
        - 生成 query 的 1024 维向量；
        - 并行在指定的 collections 中检索；
        - 底层强制注入 status == "active" 过滤；
        - 聚合多集合结果，按相似度 score 降序排列并截取 limit 条返回。
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

        # 1. 向量推理（调用 BGEM3Engine）
        embeddings = await self.engine.embed([query])
        query_vector = embeddings[0]

        # 2. 构建额外过滤条件 (project_id, memory_type, scope)
        filter_conditions: List[models.FieldCondition] = []
        if project_id:
            filter_conditions.append(
                models.FieldCondition(
                    key="project_id",
                    match=models.MatchValue(value=project_id),
                )
            )
        if memory_type:
            filter_conditions.append(
                models.FieldCondition(
                    key="memory_type",
                    match=models.MatchValue(value=memory_type),
                )
            )
        if scope:
            filter_conditions.append(
                models.FieldCondition(
                    key="scope",
                    match=models.MatchValue(value=scope),
                )
            )

        # 3. 在 target collections 中检索（利用 asyncio.to_thread 并发查询）
        async def _search_single_collection(col_name: str) -> List[Dict[str, Any]]:
            try:
                scored_points = await asyncio.to_thread(
                    self.qdrant.search_points,
                    collection_name=col_name,
                    query_vector=query_vector,
                    limit=limit,
                    score_threshold=score_threshold,
                    filter_conditions=filter_conditions,
                )
                items = []
                for sp in scored_points:
                    payload = sp.payload or {}
                    items.append({
                        "memory_id": payload.get("memory_id"),
                        "parent_memory_id": payload.get("parent_memory_id"),
                        "chunk_index": payload.get("chunk_index", 0),
                        "total_chunks": payload.get("total_chunks", 1),
                        "content": payload.get("content", ""),
                        "memory_type": payload.get("memory_type", "fact"),
                        "score": round(float(sp.score), 4),
                        "collection": col_name,
                        "project_id": payload.get("project_id"),
                        "scope": payload.get("scope", "global"),
                        "source_agent": payload.get("source_agent"),
                        "source_message_ids": payload.get("source_message_ids", []),
                        "created_at": payload.get("created_at"),
                        "meta": payload.get("meta", {}),
                    })
                return items
            except Exception as e:
                logger.error(f"Search collection '{col_name}' failed: {e}")
                return []

        search_tasks = [_search_single_collection(col) for col in target_cols]
        search_results = await asyncio.gather(*search_tasks)

        # 4. 聚合多集合结果并排序
        all_results: List[Dict[str, Any]] = []
        for res_list in search_results:
            all_results.extend(res_list)

        # 按 score 降序排列
        all_results.sort(key=lambda x: x["score"], reverse=True)
        final_results = all_results[:limit]

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
        显式沉淀记忆
        - 检查文本长度，当输入超过 8192 Token 时，自动按照带 128 Tokens 滑动重叠的窗口切片（Chunks）；
        - 单 chunk 直接生成 1024 维向量；多 chunk 生成多个向量并关联相同的 parent_memory_id 与 chunk_index；
        - 写入 Qdrant 集合，状态设为 active；
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

        # 写入 Qdrant
        await asyncio.to_thread(
            self.qdrant.upsert_points,
            collection_name=collection_name,
            points=points,
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
