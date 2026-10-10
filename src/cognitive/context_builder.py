"""
AI Memory Runtime - Memory Context Pack Builder
遵循 RFC-005 第四节与 opencode_task_mos_v3.md：
确定性多维组装 Memory Context Pack：
1. 向量 Top-20 (基于新会话核心主题密集语义召回)
2. 同 project_id 全部活跃记忆
3. 同 entity (匹配 subject / object 实体名)
4. 最近 7 天内发生过变更的记忆
5. 历史已有的 SUPERSEDE 链条记忆
去重重排后截断在 30 条以内，作为只读背景直接注入 LLM System Prompt。
"""

import time
from typing import Any, Dict, List, Optional, Set
from src.core.session_store import SessionStore
from src.core.qdrant import QdrantManager
from src.core.engine import BGEM3Engine


class ContextBuilder:
    """
    确定性构建记忆上下文包 (Memory Context Pack)。
    严禁 LLM 在提纯时自主盲目检索，由 AMR 确定性宽召回并去重截断在 30 条内。
    """

    def __init__(
        self,
        session_store: SessionStore,
        qdrant_manager: Optional[QdrantManager] = None,
        engine: Optional[BGEM3Engine] = None,
        max_context_limit: int = 30,
    ):
        self.session_store = session_store
        self.qdrant_manager = qdrant_manager
        self.engine = engine
        self.max_context_limit = max_context_limit

    async def build_context_pack(
        self,
        project_id: str,
        recent_messages: List[Dict[str, Any]],
        entities: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        组装确定性 Context Pack。
        """
        candidates_map: Dict[str, Dict[str, Any]] = {}
        now = int(time.time())
        seven_days_ago = now - (7 * 86400)

        conn = self.session_store.get_connection()
        cursor = conn.cursor()

        # 1. 向量 Top-20 召回 (如果有 qdrant_manager 和 engine)
        if self.qdrant_manager and self.engine and recent_messages:
            try:
                # 拼接新会话的最后几条内容作为 query
                query_text = " ".join([m.get("content", "") for m in recent_messages[-5:]])[:1000]
                if query_text.strip():
                    embeddings = await self.engine.embed([query_text])
                    if embeddings:
                        search_results = self.qdrant_manager.search_points(
                            collection_name="ai_memory",
                            vector=embeddings[0],
                            limit=20,
                            score_threshold=0.3,
                        )
                        for pt in search_results:
                            m_id = pt.payload.get("memory_id")
                            if m_id:
                                mem = self.session_store.get_memory(m_id)
                                if mem and mem.get("status") in ("active", "candidate"):
                                    mem["_recall_source"] = "vector_top20"
                                    mem["_score"] = pt.score
                                    candidates_map[m_id] = mem
            except Exception as e:
                # 向量召回若未就绪或降级，记录但继续依赖 SQLite 多维召回
                pass

        # 2. 同 project_id 全部活跃记忆 (优先 limit 30)
        cursor.execute(
            """
            SELECT memory_id FROM memories 
            WHERE project_id = ? AND status IN ('active', 'candidate')
            ORDER BY updated_at DESC LIMIT 30
            """,
            (project_id,),
        )
        for row in cursor.fetchall():
            m_id = row["memory_id"]
            if m_id not in candidates_map:
                mem = self.session_store.get_memory(m_id)
                if mem:
                    mem["_recall_source"] = "project_active"
                    candidates_map[m_id] = mem

        # 3. 同 entity (匹配 subject / object 实体名)
        if entities:
            for ent in entities:
                cursor.execute(
                    """
                    SELECT memory_id FROM memories 
                    WHERE (subject = ? OR object = ?) AND status IN ('active', 'candidate')
                    ORDER BY updated_at DESC LIMIT 10
                    """,
                    (ent, ent),
                )
                for row in cursor.fetchall():
                    m_id = row["memory_id"]
                    if m_id not in candidates_map:
                        mem = self.session_store.get_memory(m_id)
                        if mem:
                            mem["_recall_source"] = "entity_match"
                            candidates_map[m_id] = mem

        # 4. 最近 7 天内发生过变更的记忆
        cursor.execute(
            """
            SELECT memory_id FROM memories
            WHERE project_id = ? AND updated_at >= ?
            ORDER BY updated_at DESC LIMIT 20
            """,
            (project_id, seven_days_ago),
        )
        for row in cursor.fetchall():
            m_id = row["memory_id"]
            if m_id not in candidates_map:
                mem = self.session_store.get_memory(m_id)
                if mem:
                    mem["_recall_source"] = "recent_7d"
                    candidates_map[m_id] = mem

        # 5. 历史已有的 SUPERSEDE 链条记忆
        cursor.execute(
            """
            SELECT memory_id FROM memories
            WHERE project_id = ? AND (superseded_by IS NOT NULL OR superseded_at IS NOT NULL)
            ORDER BY updated_at DESC LIMIT 10
            """,
            (project_id,),
        )
        for row in cursor.fetchall():
            m_id = row["memory_id"]
            if m_id not in candidates_map:
                mem = self.session_store.get_memory(m_id)
                if mem:
                    mem["_recall_source"] = "supersede_chain"
                    candidates_map[m_id] = mem

        # 排序并截断至 max_context_limit (30条)
        # 优先级：vector/score 优先，其次 updated_at
        all_candidates = list(candidates_map.values())
        all_candidates.sort(
            key=lambda x: (
                x.get("_score", 0.0),
                x.get("updated_at", 0),
            ),
            reverse=True,
        )

        return all_candidates[: self.max_context_limit]
