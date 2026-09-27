"""
Qdrant 向量数据库适配器
遵循 DOC-AMR-03-DDD / DOC-AMR-04-API 规范
实现长连接复用（单例模式）、启动集合校验与初始化、向量检索与 Payload 操作
底层检索强制注入 status == "active"
"""

import logging
import threading
from typing import Any, Dict, List, Optional, Union
import uuid

from qdrant_client import QdrantClient, models
from qdrant_client.http.exceptions import UnexpectedResponse

from config.settings import AppConfig, QdrantConfig, load_config

logger = logging.getLogger(__name__)

STANDARD_COLLECTIONS = ["ai_memory", "crypto_standards", "project_docs"]
DEFAULT_VECTOR_SIZE = 1024
DEFAULT_DISTANCE = models.Distance.COSINE


class QdrantManager:
    """
    Qdrant 数据库管理器（单例模式）
    复用官方 QdrantClient 客户端连接，提供安全的数据检索和持久化封装
    """

    _instance: Optional["QdrantManager"] = None
    _lock = threading.RLock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        config: Optional[Union[AppConfig, QdrantConfig]] = None,
        client: Optional[QdrantClient] = None,
        url: Optional[str] = None,
        api_key: Optional[str] = None,
        timeout: Optional[float] = None,
        auto_init_collections: bool = True,
    ):
        # 避免单例重复初始化破坏现有连接
        if hasattr(self, "_initialized") and self._initialized:
            # 如果显式传入新 client，允许替换（主要用于单测内存 client 注入）
            if client is not None and self._client != client:
                with self._lock:
                    self._client = client
                    if auto_init_collections:
                        self.ensure_standard_collections()
            return

        with self._lock:
            if hasattr(self, "_initialized") and self._initialized:
                return

            if isinstance(config, AppConfig):
                self.config = config.qdrant
            elif isinstance(config, QdrantConfig):
                self.config = config
            else:
                app_cfg = load_config()
                self.config = app_cfg.qdrant

            self.url = url or self.config.url
            self.api_key = api_key if api_key is not None else self.config.api_key
            self.timeout = timeout if timeout is not None else self.config.timeout
            self.prefer_grpc = getattr(self.config, "prefer_grpc", True)
            self.collections = getattr(self.config, "collections", STANDARD_COLLECTIONS)

            if client is not None:
                self._client = client
            else:
                client_kwargs: Dict[str, Any] = {
                    "url": self.url,
                    "timeout": self.timeout,
                    "prefer_grpc": self.prefer_grpc,
                }
                if self.api_key:
                    client_kwargs["api_key"] = self.api_key
                self._client = QdrantClient(**client_kwargs)

            self._initialized = True

            if auto_init_collections:
                try:
                    self.ensure_standard_collections()
                except Exception as e:
                    logger.warning(
                        f"Qdrant ensure_standard_collections failed on init: {e} "
                        f"(Server at {self.url} might not be running yet)"
                    )

    @classmethod
    def get_instance(cls, **kwargs) -> "QdrantManager":
        """获取或创建 QdrantManager 单例"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls(**kwargs)
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """重置单例（主要用于测试隔离）"""
        with cls._lock:
            if cls._instance is not None:
                try:
                    cls._instance.close()
                except Exception:
                    pass
                cls._instance = None

    @property
    def client(self) -> QdrantClient:
        return self._client

    def close(self) -> None:
        """关闭底层连接句柄"""
        if hasattr(self, "_client") and self._client is not None:
            try:
                self._client.close()
            except Exception as e:
                logger.debug(f"Error closing QdrantClient: {e}")

    def ensure_standard_collections(
        self,
        collection_names: Optional[List[str]] = None,
        vector_size: int = DEFAULT_VECTOR_SIZE,
        distance: models.Distance = DEFAULT_DISTANCE,
    ) -> None:
        """
        确认标准集合存在；若不存在则自动创建 (vector_size=1024, distance=Cosine)
        """
        names_to_check = collection_names or self.collections or STANDARD_COLLECTIONS
        for name in names_to_check:
            self.ensure_collection(name, vector_size=vector_size, distance=distance)

    def ensure_collection(
        self,
        collection_name: str,
        vector_size: int = DEFAULT_VECTOR_SIZE,
        distance: models.Distance = DEFAULT_DISTANCE,
    ) -> bool:
        """
        检查指定集合是否存在，不存在则创建。
        返回 True 表示新建，False 表示已存在。
        """
        try:
            exists = self._client.collection_exists(collection_name=collection_name)
        except Exception as err:
            try:
                collections = [c.name for c in self._client.get_collections().collections]
                exists = collection_name in collections
            except Exception as e:
                logger.warning(f"Could not check collection {collection_name} on startup ({e}). Will check upon request.")
                return False

        if not exists:
            logger.info(f"Creating collection '{collection_name}' (dim={vector_size}, distance={distance.name})...")
            self._client.create_collection(
                collection_name=collection_name,
                vectors_config=models.VectorParams(
                    size=vector_size,
                    distance=distance,
                ),
            )
            # 为常用 payload 字段建立索引以提升高并发检索性能
            for field_name in ["status", "memory_id", "project_id", "memory_type", "type", "scope", "parent_memory_id"]:
                try:
                    self._client.create_payload_index(
                        collection_name=collection_name,
                        field_name=field_name,
                        field_schema=models.PayloadSchemaType.KEYWORD,
                    )
                except Exception as e:
                    logger.debug(f"Payload index creation note ({field_name}): {e}")
            return True
        return False

    def upsert_points(
        self,
        collection_name: str,
        points: List[models.PointStruct],
        wait: bool = True,
    ) -> Any:
        """
        批量写入或更新 points
        """
        if not points:
            return None
        return self._client.upsert(
            collection_name=collection_name,
            points=points,
            wait=wait,
        )

    def search_points(
        self,
        collection_name: str,
        query_vector: List[float],
        limit: int = 5,
        score_threshold: Optional[float] = None,
        filter_conditions: Optional[List[models.FieldCondition]] = None,
        extra_filter: Optional[models.Filter] = None,
    ) -> List[models.ScoredPoint]:
        """
        向量相似度搜索：底层服务端强制注入 status == "active" 过滤器
        支持通过 filter_conditions 或 extra_filter 叠加业务条件（如 project_id, memory_type 等）
        """
        # 强制底层约束：status 必须为 active
        must_conditions: List[Union[models.FieldCondition, models.Filter]] = [
            models.FieldCondition(
                key="status",
                match=models.MatchValue(value="active"),
            )
        ]

        if filter_conditions:
            must_conditions.extend(filter_conditions)

        must_not_conditions = None
        should_conditions = None

        if extra_filter is not None:
            if extra_filter.must:
                must_conditions.extend(extra_filter.must)
            if extra_filter.must_not:
                must_not_conditions = extra_filter.must_not
            if extra_filter.should:
                should_conditions = extra_filter.should

        combined_filter = models.Filter(
            must=must_conditions,
            must_not=must_not_conditions,
            should=should_conditions,
        )

        query_res = self._client.query_points(
            collection_name=collection_name,
            query=query_vector,
            query_filter=combined_filter,
            limit=limit,
            score_threshold=score_threshold,
            with_payload=True,
            with_vectors=False,
        )
        return query_res.points

    def get_point_by_memory_id(
        self,
        memory_id: str,
        collections: Optional[List[str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        按 memory_id 精确查询点位及其 payload 与所在集合
        """
        target_collections = collections or self.collections or STANDARD_COLLECTIONS
        for col in target_collections:
            records, _ = self._client.scroll(
                collection_name=col,
                scroll_filter=models.Filter(
                    must=[
                        models.FieldCondition(
                            key="memory_id",
                            match=models.MatchValue(value=memory_id),
                        )
                    ]
                ),
                limit=1,
                with_payload=True,
                with_vectors=False,
            )
            if records:
                rec = records[0]
                return {
                    "point_id": rec.id,
                    "collection": col,
                    "payload": rec.payload or {},
                }
        return None

    def update_payload_by_memory_id(
        self,
        memory_id: str,
        payload_updates: Dict[str, Any],
        collections: Optional[List[str]] = None,
        wait: bool = True,
    ) -> bool:
        """
        根据 memory_id 更新点位的 payload 字段（如状态变更 status, superseded_by 等）
        对该 memory_id 及其子 chunk（parent_memory_id == memory_id）统一同步更新
        """
        target_collections = collections or self.collections or STANDARD_COLLECTIONS
        updated_any = False

        filter_condition = models.Filter(
            should=[
                models.FieldCondition(
                    key="memory_id",
                    match=models.MatchValue(value=memory_id),
                ),
                models.FieldCondition(
                    key="parent_memory_id",
                    match=models.MatchValue(value=memory_id),
                ),
            ]
        )

        for col in target_collections:
            try:
                # 检查是否存在匹配点
                records, _ = self._client.scroll(
                    collection_name=col,
                    scroll_filter=filter_condition,
                    limit=1,
                    with_payload=False,
                )
                if records:
                    self._client.set_payload(
                        collection_name=col,
                        payload=payload_updates,
                        points=filter_condition,
                        wait=wait,
                    )
                    updated_any = True
            except Exception as e:
                logger.error(f"Failed to update payload in collection {col} for memory_id={memory_id}: {e}")
                raise

        return updated_any

    def delete_point_by_id(
        self,
        point_id: str,
        collection_name: str = "ai_memory",
        wait: bool = True,
    ) -> bool:
        """
        按 Qdrant point_id 物理删除点位
        """
        try:
            self._client.delete(
                collection_name=collection_name,
                points_selector=[point_id],
                wait=wait,
            )
            return True
        except Exception as e:
            logger.error(f"Failed to delete point {point_id} in {collection_name}: {e}")
            raise
