"""
AI Memory Runtime - Cognitive Scheduler (认知提纯流水线驱动主控)
遵循 RFC-005 第一/五/八节与 opencode_task_mos_v3.md：
核心流水线：
增量窗口扫描 -> 确定性组装 Context Pack -> 调用 LLM Worker 提纯 -> Evidence/Coverage 双验证器门禁 -> 写入 curation_candidates 隔离表。
【核心铁律】：
1. AMR 拥有唯一主控调度权，LLM 仅作为认知计算工人；
2. 14 天 Dry-Run 铁律：所有提纯结果仅作为 PROPOSED 状态写入 curation_candidates 隔离表，绝对不得直接修改 memories SSOT。
"""

import json
import logging
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple

from src.core.session_store import SessionStore
from src.cognitive.evidence_validator import EvidenceValidator
from src.cognitive.coverage_validator import CoverageValidator
from src.cognitive.context_builder import ContextBuilder
from src.cognitive.prompt_manager import PromptManager

logger = logging.getLogger(__name__)


class CognitiveScheduler:
    """
    AMR 记忆操作系统核心认知整理流水线主控调度器。
    """

    def __init__(
        self,
        session_store: SessionStore,
        context_builder: Optional[ContextBuilder] = None,
        evidence_validator: Optional[EvidenceValidator] = None,
        coverage_validator: Optional[CoverageValidator] = None,
        prompt_manager: Optional[PromptManager] = None,
        llm_worker: Optional[Callable[[str, str], Any]] = None,
    ):
        self.session_store = session_store
        self.context_builder = context_builder or ContextBuilder(session_store=self.session_store)
        self.evidence_validator = evidence_validator or EvidenceValidator()
        self.coverage_validator = coverage_validator or CoverageValidator(strict_mode=False)
        self.prompt_manager = prompt_manager or PromptManager()
        self.llm_worker = llm_worker

    def scan_incremental_sessions(
        self,
        lookback_seconds: int = 86400,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        第一阶段：扫描近 24h 增量新会话与消息。
        """
        now = int(time.time())
        since_ts = now - lookback_seconds
        conn = self.session_store.get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT session_id, agent_id, project_id, started_at, ended_at, status
            FROM raw_sessions
            WHERE started_at >= ?
            ORDER BY started_at ASC
            LIMIT ?
            """,
            (since_ts, limit),
        )
        return [dict(r) for r in cursor.fetchall()]

    async def run_pipeline_for_session(
        self,
        session_id: str,
        batch_id: Optional[str] = None,
        mock_llm_result: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        为单个会话执行认知整理闭环流水线：
        1. 增量窗口扫描读取 raw_messages；
        2. 确定性组装 Memory Context Pack；
        3. 构建受控 Prompt 并调用 LLM Worker；
        4. 运行双验证器门禁 (Evidence + Coverage)；
        5. 将通过的提案以 PROPOSED 状态写入 curation_candidates 隔离表（14天 Dry-Run 铁律，绝不写 memories SSOT）。
        """
        now_ts = int(time.time())
        batch_id = batch_id or f"recon_{time.strftime('%Y%m%d_%H%M%S')}"

        # 1. 读取该 session 下所有原始消息
        conn = self.session_store.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT message_id, session_id, role, content, sequence, created_at
            FROM raw_messages
            WHERE session_id = ?
            ORDER BY sequence ASC
            """,
            (session_id,),
        )
        raw_msgs = [dict(r) for r in cursor.fetchall()]
        if not raw_msgs:
            # 兼容 messages 表
            cursor.execute(
                """
                SELECT message_id, session_id, role, content, sequence, timestamp as created_at
                FROM messages
                WHERE session_id = ?
                ORDER BY sequence ASC
                """,
                (session_id,),
            )
            raw_msgs = [dict(r) for r in cursor.fetchall()]

        if not raw_msgs:
            return {
                "batch_id": batch_id,
                "session_id": session_id,
                "status": "SKIPPED_EMPTY",
                "proposals_count": 0,
                "written_candidates": 0,
            }

        # 获取该 session 的 project_id
        cursor.execute("SELECT project_id FROM raw_sessions WHERE session_id = ?", (session_id,))
        s_row = cursor.fetchone()
        project_id = s_row["project_id"] if s_row and s_row["project_id"] else "general"

        raw_msg_ids = [m["message_id"] for m in raw_msgs]
        raw_msg_map = {m["message_id"]: m["content"] for m in raw_msgs}

        # 2. 组装 Context Pack
        context_pack = await self.context_builder.build_context_pack(
            project_id=project_id,
            recent_messages=raw_msgs,
        )

        # 3. 构造 Prompt 并调用 LLM
        system_prompt = self.prompt_manager.get_system_prompt(context_pack=context_pack)
        llm_output_data = None

        if mock_llm_result is not None:
            llm_output_data = mock_llm_result
        elif self.llm_worker is not None:
            user_content = json.dumps(
                [{"message_id": m["message_id"], "role": m["role"], "content": m["content"]} for m in raw_msgs],
                ensure_ascii=False,
            )
            resp = self.llm_worker(system_prompt, user_content)
            if isinstance(resp, str):
                llm_output_data = json.loads(resp)
            else:
                llm_output_data = resp
        else:
            # 无 Worker 时跳过 LLM 认知提纯
            return {
                "batch_id": batch_id,
                "session_id": session_id,
                "status": "NO_LLM_WORKER",
                "proposals_count": 0,
                "written_candidates": 0,
            }

        candidates = llm_output_data.get("candidates", [])
        declared_dispositions = llm_output_data.get("message_disposition", [])

        # 4. 双验证器门禁校验
        # 4.1 Coverage 校验与调和
        cov_valid, reconciled_disps, cov_err = self.coverage_validator.validate_and_reconcile(
            raw_message_ids=raw_msg_ids,
            proposals=candidates,
            declared_dispositions=declared_dispositions,
        )
        if not cov_valid:
            logger.warning(f"Coverage validation failed for session {session_id}: {cov_err}")
            return {
                "batch_id": batch_id,
                "session_id": session_id,
                "status": "COVERAGE_REJECTED",
                "error": cov_err,
                "proposals_count": len(candidates),
                "written_candidates": 0,
            }

        # 4.2 Evidence 校验
        valid_candidates = []
        invalid_candidates = []

        for cand in candidates:
            ev_ok, ev_reason = self.evidence_validator.validate_proposal(cand, raw_msg_map)
            if ev_ok:
                valid_candidates.append(cand)
            else:
                cand["rejection_reason"] = ev_reason
                invalid_candidates.append(cand)
                logger.info(f"Proposal rejected by EvidenceValidator: {ev_reason}")

        # 5. 写入 curation_candidates 隔离表（14 天 Dry-Run 铁律，绝不写 memories SSOT）
        written_count = 0
        session_window_json = json.dumps({
            "session_ids": [session_id],
            "start_msg_id": raw_msg_ids[0] if raw_msg_ids else None,
            "end_msg_id": raw_msg_ids[-1] if raw_msg_ids else None,
        })
        disposition_json = json.dumps(reconciled_disps, ensure_ascii=False)

        with self.session_store._lock:
            db_conn = self.session_store.get_connection()
            db_cursor = db_conn.cursor()

            for cand in valid_candidates:
                cand_id = cand.get("proposal_id") or f"prop_{time.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
                op = cand.get("operation", "EXTRACT")
                subject = cand.get("subject", "")
                predicate = cand.get("predicate", "")
                obj = cand.get("object")
                content = cand.get("content", "")
                ev_ids = json.dumps(cand.get("evidence_source_ids", []), ensure_ascii=False)
                spans = json.dumps(cand.get("extracted_spans", []), ensure_ascii=False)
                rationale = cand.get("rationale")
                rel_json = json.dumps(cand.get("proposed_relation"), ensure_ascii=False) if cand.get("proposed_relation") else None
                conf = cand.get("self_assessed_confidence", 0.85)
                target_mem_id = cand.get("target_memory_id")

                db_cursor.execute(
                    """
                    INSERT OR REPLACE INTO curation_candidates (
                        candidate_id, batch_id, memory_id, session_window_json,
                        operation, current_status, proposed_status, matched_rule_id,
                        reason, evidence_snapshot, subject, predicate, object,
                        content, evidence_source_ids, extracted_spans, message_disposition,
                        rationale, proposed_relation_json, prompt_version, model_name,
                        llm_confidence, state, rejection_reason, created_at, processed_at
                    )
                    VALUES (?, ?, ?, ?, ?, NULL, 'PROPOSED', 'COGNITIVE_DISTILL', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PROPOSED', NULL, ?, NULL)
                    """,
                    (
                        cand_id, batch_id, target_mem_id, session_window_json,
                        op, rationale or f"Cognitive distillation from session {session_id}",
                        spans, subject, predicate, obj,
                        content, ev_ids, spans, disposition_json,
                        rationale, rel_json, PromptManager.PROMPT_VERSION, "cognitive_worker",
                        conf, now_ts
                    ),
                )
                written_count += 1

            db_conn.commit()

        return {
            "batch_id": batch_id,
            "session_id": session_id,
            "status": "SUCCESS",
            "total_candidates": len(candidates),
            "valid_candidates": len(valid_candidates),
            "invalid_candidates": len(invalid_candidates),
            "written_candidates": written_count,
            "reconciled_dispositions_count": len(reconciled_disps),
        }
