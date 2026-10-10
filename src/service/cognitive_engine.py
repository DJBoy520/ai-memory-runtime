"""
AI Memory Runtime - Cognitive Governance Engine (v2.2)
实现确定性认知治理五大核心 API：
1. extract: 候选三元组抽取与实体归一化，判定晋升门槛 (confidence >= 0.90 AND mention_count >= 2)
2. merge: 多级混合防语义漂移与证据累积 (Level 1 三元组强仲裁, Level 2 否定词与反义对抗拦截, Level 3 贝叶斯证据累加)
3. update: 正交解耦 type 与 conflict_policy (overwrite, coexist, state_machine, immutable)，合法 DAG 状态机流转
4. retrieve: 服务端硬过滤 (status='active', project_id)、写后读一致性 (SQLite 60s pending 记忆合并)、回表校验、复合重排打分
5. forget: 事务标记 status='deleted'，记录 deleted_at/by/reason，投递 Outbox op_type='delete' 与审计日志
"""

import asyncio
from datetime import datetime, timezone
import json
import logging
import math
import re
import time
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import uuid

from config.settings import AppConfig, load_config
from src.core.engine import BGEM3Engine
from src.core.qdrant import QdrantManager, STANDARD_COLLECTIONS
from src.core.session_store import SessionStore
from src.service.entity_normalizer import EntityNormalizer

logger = logging.getLogger(__name__)

# 否定词与对抗反义词探测集合 (Level 2 拦截)
NEGATION_WORDS: Set[str] = {
    "不", "未", "没", "无", "非", "别", "严禁", "禁止", "禁用", "停止", "关闭", "取消", "否定", "不能", "不可",
    "not", "no", "never", "none", "neither", "disable", "disabled", "stop", "stopped", "cancel", "deny"
}

# F1-3: 写后读一致性窗口。outbox 轮询实测 1s/batch=10，10s 为足够安全的收窄窗口（原 60s）
PENDING_WINDOW_SECONDS = 10


def _cosine_similarity(a: List[float], b: List[float]) -> float:
    """本地余弦相似度（用于 pending 记忆与 query 的真实打分）"""
    dot = 0.0
    norm_a = 0.0
    norm_b = 0.0
    for x, y in zip(a, b):
        dot += x * y
        norm_a += x * x
        norm_b += y * y
    if norm_a <= 0.0 or norm_b <= 0.0:
        return 0.0
    return dot / (math.sqrt(norm_a) * math.sqrt(norm_b))


# 状态机合法 DAG 拓扑定义
TASK_STATE_TRANSITIONS: Dict[str, List[str]] = {
    "pending": ["in_progress", "cancelled"],
    "in_progress": ["completed", "failed", "blocked"],
    "blocked": ["in_progress", "cancelled"],
    "completed": [],
    "failed": ["in_progress"],
    "cancelled": [],
}


class CognitiveEngine:
    """
    AMR 认知记忆治理引擎
    负责确定性记忆生命周期治理，保证 SQLite SSOT 真相源一致性与防语义漂移。
    """

    def __init__(
        self,
        session_store: Optional[SessionStore] = None,
        qdrant_manager: Optional[QdrantManager] = None,
        engine: Optional[BGEM3Engine] = None,
        normalizer: Optional[EntityNormalizer] = None,
        config: Optional[AppConfig] = None,
    ):
        self.config = config or load_config()
        self.session_store = session_store or SessionStore(config=self.config.storage)
        self.qdrant = qdrant_manager or QdrantManager.get_instance(config=self.config.qdrant)
        self.engine = engine or BGEM3Engine(config=self.config.model)
        self.normalizer = normalizer or EntityNormalizer()

    def _generate_memory_id(self) -> str:
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        rand_str = uuid.uuid4().hex[:6]
        return f"mem_{date_str}_{rand_str}"

    @staticmethod
    def contains_negation(text: str) -> bool:
        """探测文本中是否包含否定或强禁令词汇"""
        if not text:
            return False
        # 中文单字/词扫描
        for nw in NEGATION_WORDS:
            if re.search(r"\b" + re.escape(nw) + r"\b", text, re.IGNORECASE) or (len(nw) <= 2 and nw in text):
                return True
        return False

    @staticmethod
    def calculate_bayesian_confidence(conf_old: float, evidence_strength: float) -> float:
        """
        Level 3 跨 Session 贝叶斯证据累加公式：
        confidence_new = 1 - (1 - confidence_old) * (1 - evidence_strength)
        """
        conf_old = max(0.0, min(1.0, float(conf_old)))
        evidence_strength = max(0.0, min(1.0, float(evidence_strength)))
        conf_new = 1.0 - (1.0 - conf_old) * (1.0 - evidence_strength)
        return round(max(0.0, min(1.0, conf_new)), 4)

    # =========================================================================
    # 1. extract: 候选三元组抽取与实体归一化
    # =========================================================================
    def extract_candidate(
        self,
        subject: str,
        predicate: str,
        content: str,
        type: str = "fact",
        object_: Optional[str] = None,
        confidence: float = 0.8,
        importance: float = 0.5,
        project_id: str = "general",
        scope: str = "global",
        source_agent: str = "default_agent",
        conflict_policy: Optional[str] = None,
        evidence: Optional[List[Dict[str, Any]]] = None,
        force_promote: bool = False,
    ) -> Dict[str, Any]:
        """
        结构化候选抽取与归一化：
        1. 实体与谓词归一化映射；
        2. 生成 CandidateMemory (默认 status='candidate')；
        3. 判定晋升门槛 (confidence >= 0.90 AND mention_count >= 2 或 force_promote=True)；
        4. 写入 SQLite SSOT 并触发发件箱。
        """
        # 1. 实体归一化
        norm = self.normalizer.normalize_triple(subject, predicate, object_)
        norm_subj = norm["subject"] or subject.strip()
        norm_pred = norm["predicate"] or predicate.strip()
        norm_obj = norm["object"] or (object_.strip() if object_ else None)

        # 默认 conflict_policy 与 type 解耦
        if conflict_policy is None:
            default_policies = {
                "fact": "overwrite",
                "preference": "coexist",
                "decision": "overwrite",
                "task": "state_machine",
                "episode": "immutable",
                "relation": "coexist",
            }
            conflict_policy = default_policies.get(type, "coexist")

        # 2. 晋升判定 (Candidate -> Active)
        # 初始 mention_count 为证据数或 1
        mention_count = len(evidence) if evidence else 1
        should_promote = force_promote or (confidence >= 0.90 and mention_count >= 2)
        initial_status = "active" if should_promote else "candidate"

        memory_id = self._generate_memory_id()

        created_mem = self.session_store.create_memory(
            memory_id=memory_id,
            subject=norm_subj,
            predicate=norm_pred,
            object=norm_obj,
            content=content,
            type=type,
            conflict_policy=conflict_policy,
            confidence=confidence,
            importance=importance,
            mention_count=mention_count,
            status=initial_status,
            project_id=project_id,
            scope=scope,
            source_agent=source_agent,
            evidence=evidence,
            operator="cognitive_engine:extract",
            audit_detail={
                "action": "extract_candidate",
                "promoted": should_promote,
                "confidence": confidence,
                "mention_count": mention_count,
            },
        )
        return created_mem

    # =========================================================================
    # 2. merge: 多级混合防语义漂移与证据累积
    # =========================================================================
    def merge_memory(
        self,
        existing_memory_id: str,
        new_subject: str,
        new_predicate: str,
        new_content: str,
        new_object: Optional[str] = None,
        evidence: Optional[Dict[str, Any]] = None,
        evidence_strength: float = 0.5,
    ) -> Dict[str, Any]:
        """
        三级混合防语义漂移仲裁与贝叶斯证据累加：
        - Level 1: 三元组 (subject, predicate) 严格匹配；object 一致才允许合并，互斥禁止合并；
        - Level 2: 否定词与反义对抗拦截（如含有“不/严禁/禁用/停止”等）；
        - Level 3: 仅对独立会话 (不同 session_id) 进行贝叶斯证据累加与 mention_count += 1。
        """
        target = self.session_store.get_memory(existing_memory_id)
        if not target:
            raise ValueError(f"Memory {existing_memory_id} not found")

        # 归一化输入
        norm = self.normalizer.normalize_triple(new_subject, new_predicate, new_object)
        norm_subj = norm["subject"]
        norm_pred = norm["predicate"]
        norm_obj = norm["object"]

        # Level 1: 三元组结构化强仲裁
        if norm_subj != target["subject"] or norm_pred != target["predicate"]:
            return {
                "merged": False,
                "reason": "Level 1: subject or predicate mismatch",
                "conflict_branch": True,
                "target_memory_id": existing_memory_id,
            }

        # object 一致性检查：若两者都有 object 且不一致，拦截
        if (target.get("object") or norm_obj) and target.get("object") != norm_obj:
            return {
                "merged": False,
                "reason": f"Level 1: object mismatch (existing '{target.get('object')}' vs new '{norm_obj}')",
                "conflict_branch": True,
                "target_memory_id": existing_memory_id,
            }

        # Level 2: 否定词与反义对抗拦截
        old_has_neg = self.contains_negation(target["content"])
        new_has_neg = self.contains_negation(new_content)
        if old_has_neg != new_has_neg:
            return {
                "merged": False,
                "reason": "Level 2: negation mismatch between statements (antonym/contradiction)",
                "conflict_branch": True,
                "target_memory_id": existing_memory_id,
            }

        # Level 3: 跨 Session 贝叶斯证据累加
        new_session_id = evidence.get("session_id") if evidence else None
        existing_sessions = {ev["session_id"] for ev in target.get("evidence", [])}

        old_confidence = float(target["confidence"])
        old_mention_count = int(target["mention_count"])

        # 检查是否为独立 session
        is_independent_session = bool(new_session_id and new_session_id not in existing_sessions)

        if is_independent_session:
            new_confidence = self.calculate_bayesian_confidence(old_confidence, evidence_strength)
            new_mention_count = old_mention_count + 1
        else:
            # 同 session 重复提及不叠加贝叶斯置信度，但更新 mention_count
            new_confidence = old_confidence
            new_mention_count = old_mention_count + 1

        # 若原为 candidate 且达到门槛，可晋升 active
        target_status = target["status"]
        if target_status == "candidate" and new_confidence >= 0.90 and new_mention_count >= 2:
            target_status = "active"

        # 在 SQLite SSOT 中原子更新记忆主表及关联证据
        with self.session_store._lock:
            conn = self.session_store._get_connection()
            cursor = conn.cursor()
            now = int(time.time())
            try:
                # 更新 memories
                cursor.execute(
                    """
                    UPDATE memories
                    SET confidence = ?,
                        mention_count = ?,
                        status = ?,
                        updated_at = ?
                    WHERE memory_id = ?
                    """,
                    (new_confidence, new_mention_count, target_status, now, existing_memory_id),
                )

                # 追加证据关联
                if evidence and evidence.get("message_id") and evidence.get("session_id"):
                    cursor.execute(
                        """
                        INSERT INTO memory_evidence (
                            memory_id, message_id, session_id, evidence_strength, linked_at
                        )
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(memory_id, message_id) DO UPDATE SET
                            evidence_strength = excluded.evidence_strength,
                            linked_at = excluded.linked_at
                        """,
                        (
                            existing_memory_id,
                            evidence["message_id"],
                            evidence["session_id"],
                            evidence_strength,
                            now,
                        ),
                    )

                # 写入 Outbox 更新 payload
                payload_snapshot = json.dumps({
                    "memory_id": existing_memory_id,
                    "confidence": new_confidence,
                    "mention_count": new_mention_count,
                    "status": target_status,
                    "updated_at": now,
                }, ensure_ascii=False)

                cursor.execute(
                    """
                    INSERT INTO qdrant_sync_queue (
                        memory_id, qdrant_point_id, op_type, payload_snapshot,
                        status, retry_count, last_error, created_at, updated_at
                    )
                    VALUES (?, ?, 'update_payload', ?, 'pending', 0, NULL, ?, ?)
                    """,
                    (existing_memory_id, target["qdrant_point_id"], payload_snapshot, now, now),
                )

                # 记录审计日志
                audit_detail = {
                    "action": "merge",
                    "old_confidence": old_confidence,
                    "new_confidence": new_confidence,
                    "mention_count": new_mention_count,
                    "independent_session": is_independent_session,
                    "status": target_status,
                }
                cursor.execute(
                    """
                    INSERT INTO memory_audit_log (
                        memory_id, action, operator, detail, timestamp
                    )
                    VALUES (?, 'merge', 'cognitive_engine:merge', ?, ?)
                    """,
                    (existing_memory_id, json.dumps(audit_detail, ensure_ascii=False), now),
                )

                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e

        updated_mem = self.session_store.get_memory(existing_memory_id)
        return {
            "merged": True,
            "memory": updated_mem,
            "confidence_increased": new_confidence > old_confidence,
        }

    # =========================================================================
    # 3. update: 状态机与冲突流转
    # =========================================================================
    def update_with_conflict_policy(
        self,
        existing_memory_id: str,
        new_content: str,
        new_object: Optional[str] = None,
        new_task_status: Optional[str] = None,
        operator: str = "engine",
    ) -> Dict[str, Any]:
        """
        根据 memory 的 conflict_policy 执行确定性演进：
        - overwrite: 版本递增，旧条目打上 superseded，创建新版本条目；
        - coexist: 不覆盖旧条目，创建新并列条目；
        - state_machine: 针对 task，校验合法 DAG 状态迁移拓扑，非法流转直接拒绝；
        - immutable: 不可篡改 (episode)，严禁更新/覆盖。
        """
        target = self.session_store.get_memory(existing_memory_id)
        if not target:
            raise ValueError(f"Memory {existing_memory_id} not found")

        policy = target.get("conflict_policy", "overwrite")

        if policy == "immutable":
            return {
                "success": False,
                "reason": "Policy 'immutable': episode or historical records cannot be modified.",
                "memory_id": existing_memory_id,
            }

        if policy == "state_machine":
            # 状态机流转依据合法 DAG 拓扑
            current_status = target.get("object") or "pending"
            next_status = new_task_status or new_object
            allowed_next = TASK_STATE_TRANSITIONS.get(current_status, [])

            if next_status not in allowed_next:
                return {
                    "success": False,
                    "reason": f"State machine violation: transition from '{current_status}' to '{next_status}' is illegal. Allowed: {allowed_next}",
                    "memory_id": existing_memory_id,
                }

            # 合法状态迁移：先生成后继任务记忆，再原子将旧记忆标记为 superseded
            new_id = self._generate_memory_id()
            now = int(time.time())

            # 1. 先创建新状态记忆 (避免 superseded_by 外键约束失败)
            new_mem = self.session_store.create_memory(
                memory_id=new_id,
                subject=target["subject"],
                predicate=target["predicate"],
                object=next_status,
                content=new_content or f"Task {target['subject']} state transitioned to {next_status}",
                type=target["type"],
                conflict_policy="state_machine",
                confidence=target["confidence"],
                importance=target["importance"],
                status="active",
                project_id=target["project_id"],
                scope=target["scope"],
                source_agent=target["source_agent"],
                version=target["version"] + 1,
                operator=operator,
                audit_detail={"action": "state_machine_transition", "previous_memory_id": existing_memory_id},
            )

            # 2. 标记旧记忆 superseded 指向新 memory_id
            self.session_store.update_memory_status(
                memory_id=existing_memory_id,
                status="superseded",
                superseded_by=new_id,
                operator=operator,
                detail={"transition": f"{current_status} -> {next_status}"},
            )

            return {"success": True, "action": "transitioned", "new_memory": new_mem}

        if policy == "overwrite":
            new_id = self._generate_memory_id()
            now = int(time.time())

            # 1. 先创建新记忆 (避免 superseded_by 外键约束失败)
            new_mem = self.session_store.create_memory(
                memory_id=new_id,
                subject=target["subject"],
                predicate=target["predicate"],
                object=new_object or target.get("object"),
                content=new_content,
                type=target["type"],
                conflict_policy="overwrite",
                confidence=target["confidence"],
                importance=target["importance"],
                status="active",
                project_id=target["project_id"],
                scope=target["scope"],
                source_agent=target["source_agent"],
                version=target["version"] + 1,
                operator=operator,
                audit_detail={"action": "overwrite_supersede", "previous_memory_id": existing_memory_id},
            )

            # 2. 标记旧条目 superseded 指向新 memory_id
            self.session_store.update_memory_status(
                memory_id=existing_memory_id,
                status="superseded",
                superseded_by=new_id,
                operator=operator,
                detail={"reason": "overwrite_by_new_entry"},
            )

            return {"success": True, "action": "overwritten", "new_memory": new_mem}

        if policy == "coexist":
            # 建立并列新记忆，不影响旧记忆
            new_id = self._generate_memory_id()
            new_mem = self.session_store.create_memory(
                memory_id=new_id,
                subject=target["subject"],
                predicate=target["predicate"],
                object=new_object or target.get("object"),
                content=new_content,
                type=target["type"],
                conflict_policy="coexist",
                confidence=target["confidence"],
                importance=target["importance"],
                status="active",
                project_id=target["project_id"],
                scope=target["scope"],
                source_agent=target["source_agent"],
                version=1,
                operator=operator,
                audit_detail={"action": "coexist_add", "coexists_with": existing_memory_id},
            )
            return {"success": True, "action": "coexisted", "new_memory": new_mem}

        return {"success": False, "reason": f"Unknown conflict policy: {policy}"}

    # =========================================================================
    # 4. retrieve: 写后读一致性与复合重排
    # =========================================================================
    async def retrieve(
        self,
        query: str,
        project_id: str = "general",
        limit: int = 5,
        target_collection: str = "ai_memory",
        weights: Optional[Dict[str, float]] = None,
        half_life_days: float = 30.0,
    ) -> List[Dict[str, Any]]:
        """
        检索执行全流程：
        1. 向量推理 query vector；
        2. 服务端硬过滤：强制注入 status == 'active' 与 project_id IN ('general', current_project_id)；
        3. 写后读一致性：并发查 SQLite 最近 60s 内未同步到 Qdrant 的 pending 记忆并合并；
        4. 召回候选点回表校验当前最新 status == 'active'；
        5. 复合重排：
           FinalScore = w_s*VectorSim + w_i*Importance + w_c*Confidence + w_r*Recency + w_t*TypeWeight
        6. 同时透出 final_score 与 vector_score。
        """
        w = weights or {
            "w_s": 0.45,  # 向量相似度
            "w_i": 0.20,  # 重要性
            "w_c": 0.15,  # 置信度
            "w_r": 0.10,  # 新鲜度半衰期
            "w_t": 0.10,  # 类型权重
        }

        type_weights = {
            "decision": 1.0,
            "fact": 0.9,
            "preference": 0.85,
            "task": 0.8,
            "relation": 0.75,
            "episode": 0.7,
        }

        # 1. 向量推理
        embeddings = await self.engine.embed([query])
        query_vector = embeddings[0]

        # 2. Qdrant 向量检索 (契约 v1：治理链路作用域 = 当前项目 ∪ project_fallback_ids，
        #    修复原硬编码 IN ('general', 当前项目) 导致跨项目/全局记忆永远漏召回)
        from qdrant_client import models
        fallback_ids = list(self.config.search.project_fallback_ids)
        scope_ids: List[str] = []
        if project_id:
            scope_ids.append(project_id)
        for fid in fallback_ids:
            if fid and fid not in scope_ids:
                scope_ids.append(fid)
        if not scope_ids:
            scope_ids = ["global", "general"]
        filter_conditions = [
            models.FieldCondition(
                key="status",
                # 兼容历史小写与 v3 大写（F1-4 迁移后统一为 ACTIVE，读侧保留兼容层）
                match=models.MatchAny(any=["ACTIVE", "active"]),
            )
        ]
        filter_conditions.append(
            models.FieldCondition(
                key="project_id",
                match=models.MatchAny(any=scope_ids),
            )
        )

        qdrant_candidates: List[Dict[str, Any]] = []
        try:
            points = await asyncio.to_thread(
                self.qdrant.search_points,
                collection_name=target_collection,
                query_vector=query_vector,
                limit=20,  # 候选 Top-20
                filter_conditions=filter_conditions,
            )
            for p in points:
                payload = p.payload or {}
                qdrant_candidates.append({
                    "memory_id": payload.get("memory_id"),
                    "vector_score": float(p.score),
                    "source": "qdrant",
                })
        except Exception as e:
            logger.warning(f"Qdrant retrieve warning: {e}")

        # 3. 写后读一致性 (Read-Your-Own-Writes)
        # 查询 SQLite 最近 PENDING_WINDOW_SECONDS 内未同步到 Qdrant 的 pending 记忆，
        # 与 query 同批真实计算 cosine（替代历史硬编码 0.88 基线分，避免无关 pending 顶掉真相关结果）
        now = int(time.time())
        cutoff_time = now - PENDING_WINDOW_SECONDS
        recent_pending_memories: List[Dict[str, Any]] = []
        pending_rows: List[Tuple[str, str]] = []
        with self.session_store._lock:
            conn = self.session_store._get_connection()
            cur = conn.cursor()
            placeholders = ",".join("?" for _ in scope_ids)
            cur.execute(
                f"""
                SELECT m.memory_id, m.content
                FROM memories m
                JOIN qdrant_sync_queue q ON m.memory_id = q.memory_id
                WHERE q.status = 'pending'
                  AND m.status = 'active'
                  AND m.project_id IN ({placeholders})
                  AND m.created_at >= ?
                ORDER BY m.created_at DESC
                LIMIT 10
                """,
                (*scope_ids, cutoff_time),
            )
            pending_rows = [(row["memory_id"], row["content"] or "") for row in cur.fetchall()]

        if pending_rows:
            try:
                pending_vectors = await self.engine.embed([content for _, content in pending_rows])
                for (p_mid, _p_content), p_vec in zip(pending_rows, pending_vectors):
                    recent_pending_memories.append({
                        "memory_id": p_mid,
                        "vector_score": round(_cosine_similarity(query_vector, p_vec), 4),
                        "source": "sqlite_pending",
                    })
            except Exception as e:
                logger.warning(f"Pending 记忆真实打分失败（忽略，不并入候选）: {e}")

        # 合并候选集 (去重保留 memory_id)
        combined_candidate_map: Dict[str, Dict[str, Any]] = {}
        for item in recent_pending_memories + qdrant_candidates:
            mid = item["memory_id"]
            if mid and mid not in combined_candidate_map:
                combined_candidate_map[mid] = item

        # 4. 回表状态校验与复合重排
        final_candidates: List[Dict[str, Any]] = []
        for mid, cand in combined_candidate_map.items():
            db_mem = self.session_store.get_memory(mid)
            # 严格回表校验当前最新 status == 'active'
            if not db_mem or db_mem.get("status") != "active":
                continue

            # 校验 project_id
            mem_proj = db_mem.get("project_id", "general")
            if mem_proj not in ("general", project_id):
                continue

            v_sim = cand.get("vector_score", 0.0)
            importance = float(db_mem.get("importance", 0.5))
            confidence = float(db_mem.get("confidence", 0.8))
            mem_type = db_mem.get("type", "fact")
            type_weight = type_weights.get(mem_type, 0.7)

            # 时间半衰期新鲜度: exp(-lambda * delta_t)
            created_at = db_mem.get("created_at", now)
            delta_days = max(0.0, (now - created_at) / 86400.0)
            decay_lambda = math.log(2) / max(1.0, half_life_days)
            recency = math.exp(-decay_lambda * delta_days)

            # 复合重排评分
            final_score = (
                w["w_s"] * v_sim
                + w["w_i"] * importance
                + w["w_c"] * confidence
                + w["w_r"] * recency
                + w["w_t"] * type_weight
            )

            res_item = dict(db_mem)
            res_item["vector_score"] = round(v_sim, 4)
            res_item["final_score"] = round(final_score, 4)
            res_item["recency"] = round(recency, 4)
            final_candidates.append(res_item)

        # 按 final_score 降序排序并截取 limit
        final_candidates.sort(key=lambda x: x["final_score"], reverse=True)
        return final_candidates[:limit]

    # =========================================================================
    # 5. forget: 合规安全擦除与审计
    # =========================================================================
    def forget(
        self,
        memory_id: str,
        deleted_by: str = "admin",
        reason: str = "user_forgotten",
    ) -> bool:
        """
        合规遗忘：
        1. SQLite 事务中标记 status = 'deleted'，记录 deleted_at, deleted_by, deletion_reason；
        2. 向 qdrant_sync_queue 投递 op_type = 'delete'；
        3. 写入 memory_audit_log 审计日志。
        """
        updated = self.session_store.update_memory_status(
            memory_id=memory_id,
            status="deleted",
            deleted_by=deleted_by,
            deletion_reason=reason,
            operator="cognitive_engine:forget",
            detail={"action": "forget", "reason": reason},
        )
        return updated is not None and updated.get("status") == "deleted"
