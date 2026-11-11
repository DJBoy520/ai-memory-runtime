"""
AI Memory Runtime - Cognitive Scheduler (认知提纯流水线驱动主控)
遵循 RFC-005 第一/五/八节与 opencode_task_mos_v3.md：
核心流水线：
增量窗口扫描 -> 确定性组装 Context Pack -> 调用 LLM Worker 提纯 -> Evidence/Coverage 双验证器门禁 -> 写入 curation_candidates 隔离表。
【核心铁律】：
1. AMR 拥有唯一主控调度权，LLM 仅作为认知计算工人；
2. 14 天 Dry-Run 铁律：所有提纯结果仅作为 PROPOSED 状态写入 curation_candidates 隔离表，绝对不得直接修改 memories SSOT。
3. 通用会话接通：无缝打通 MCP session.ingest (messages/sessions 表) 与历史 raw_messages/raw_sessions 表。
4. 防编造防幻觉门禁：保留完整来源与原文字符证据链，严禁未经验证的事实自动生效。
"""

import asyncio
import inspect
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
from src.cognitive.model_worker import sanitize_text

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
        default_max_retries: int = 3,
        default_retry_delay: float = 0.0,
        model_name: Optional[str] = None,
    ):
        self.session_store = session_store
        self.context_builder = context_builder or ContextBuilder(session_store=self.session_store)
        self.evidence_validator = evidence_validator or EvidenceValidator()
        self.coverage_validator = coverage_validator or CoverageValidator(strict_mode=False)
        self.prompt_manager = prompt_manager or PromptManager()
        self.llm_worker = llm_worker
        self.default_max_retries = default_max_retries
        self.default_retry_delay = default_retry_delay
        self.model_name = model_name or getattr(llm_worker, "model_name", "cognitive_worker")

    def scan_incremental_sessions(
        self,
        lookback_seconds: int = 86400,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        第一阶段：扫描近 24h 增量新会话与消息。
        通用接通：兼容 MCP session.ingest 写入的 sessions 表以及历史 raw_sessions 表。
        支持 started_at、ended_at 以及消息活跃时间在 lookback 窗口内的增量会话。
        """
        now = int(time.time())
        since_ts = now - lookback_seconds if lookback_seconds > 0 else 0
        conn = self.session_store.get_connection()
        cursor = conn.cursor()

        # 动态检查数据库实际存在的会话表
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('sessions', 'raw_sessions', 'messages', 'raw_messages')"
        )
        existing_tables = {row["name"] for row in cursor.fetchall()}

        union_queries = []
        params: List[Any] = []

        if "sessions" in existing_tables:
            has_messages = "messages" in existing_tables
            if has_messages:
                query_sessions = """
                    SELECT session_id, agent_id, project_id, started_at, ended_at, status
                    FROM sessions
                    WHERE started_at >= ?
                       OR (ended_at IS NOT NULL AND ended_at >= ?)
                       OR session_id IN (SELECT DISTINCT session_id FROM messages WHERE timestamp >= ?)
                """
                params.extend([since_ts, since_ts, since_ts])
            else:
                query_sessions = """
                    SELECT session_id, agent_id, project_id, started_at, ended_at, status
                    FROM sessions
                    WHERE started_at >= ? OR (ended_at IS NOT NULL AND ended_at >= ?)
                """
                params.extend([since_ts, since_ts])
            union_queries.append(query_sessions)

        if "raw_sessions" in existing_tables:
            has_raw_messages = "raw_messages" in existing_tables
            if has_raw_messages:
                query_raw = """
                    SELECT session_id, agent_id, project_id, started_at, ended_at, status
                    FROM raw_sessions
                    WHERE started_at >= ?
                       OR (ended_at IS NOT NULL AND ended_at >= ?)
                       OR session_id IN (SELECT DISTINCT session_id FROM raw_messages WHERE created_at >= ?)
                """
                params.extend([since_ts, since_ts, since_ts])
            else:
                query_raw = """
                    SELECT session_id, agent_id, project_id, started_at, ended_at, status
                    FROM raw_sessions
                    WHERE started_at >= ? OR (ended_at IS NOT NULL AND ended_at >= ?)
                """
                params.extend([since_ts, since_ts])
            union_queries.append(query_raw)

        if not union_queries:
            return []

        full_query = f"""
            SELECT session_id, agent_id, project_id,
                   MIN(started_at) AS started_at,
                   MAX(COALESCE(ended_at, started_at)) AS ended_at,
                   MAX(status) AS status
            FROM (
                {" UNION ALL ".join(union_queries)}
            )
            GROUP BY session_id, agent_id, project_id
            ORDER BY ended_at ASC
            LIMIT ?
        """
        params.append(limit)

        cursor.execute(full_query, tuple(params))
        return [dict(r) for r in cursor.fetchall()]

    async def run_pipeline_for_session(
        self,
        session_id: str,
        batch_id: Optional[str] = None,
        mock_llm_result: Optional[Dict[str, Any]] = None,
        max_retries: Optional[int] = None,
        retry_delay: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        为单个会话执行认知整理闭环流水线：
        1. 增量窗口扫描读取 messages / raw_messages 通用消息；
        2. 确定性组装 Memory Context Pack；
        3. 构建受控 Prompt 并调用 LLM Worker（支持失败重试）；
        4. 运行双验证器门禁 (Evidence + Coverage)；
        5. 将通过的提案以 PROPOSED 状态写入 curation_candidates 隔离表（14天 Dry-Run 铁律，绝不写 memories SSOT）。
        """
        now_ts = int(time.time())
        batch_id = batch_id or f"recon_{time.strftime('%Y%m%d_%H%M%S')}"
        retries_limit = max_retries if max_retries is not None else self.default_max_retries
        delay = retry_delay if retry_delay is not None else self.default_retry_delay

        # 1. 通用读取该 session 下所有原始消息（兼容 raw_messages 与 messages 表，按 sequence 规整）
        conn = self.session_store.get_connection()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('sessions', 'raw_sessions', 'messages', 'raw_messages')"
        )
        existing_tables = {row["name"] for row in cursor.fetchall()}

        msgs_by_id: Dict[str, Dict[str, Any]] = {}

        if "sessions" in existing_tables and "raw_sessions" in existing_tables:
            cursor.execute("SELECT agent_id, project_id FROM sessions WHERE session_id = ?", (session_id,))
            current = cursor.fetchone()
            cursor.execute("SELECT agent_id, project_id FROM raw_sessions WHERE session_id = ?", (session_id,))
            raw = cursor.fetchone()
            if current and raw and (current["agent_id"], current["project_id"]) != (raw["agent_id"], raw["project_id"]):
                return {"batch_id": batch_id, "session_id": session_id, "status": "SESSION_ID_COLLISION", "written_candidates": 0}

        # 优先从 raw_messages 读取
        if "raw_messages" in existing_tables:
            cursor.execute(
                """
                SELECT message_id, session_id, role, content, sequence, created_at
                FROM raw_messages
                WHERE session_id = ?
                ORDER BY sequence ASC, created_at ASC
                """,
                (session_id,),
            )
            for r in cursor.fetchall():
                msgs_by_id[r["message_id"]] = dict(r)

        # 补充从 messages 表读取（MCP session.ingest / memory_ingest_session 写入的主表）
        if "messages" in existing_tables:
            cursor.execute(
                """
                SELECT message_id, session_id, role, content, sequence, timestamp as created_at
                FROM messages
                WHERE session_id = ?
                ORDER BY sequence ASC, timestamp ASC
                """,
                (session_id,),
            )
            for r in cursor.fetchall():
                if r["message_id"] in msgs_by_id and (r["role"], r["content"]) != (msgs_by_id[r["message_id"]]["role"], msgs_by_id[r["message_id"]]["content"]):
                    return {"batch_id": batch_id, "session_id": session_id, "status": "MESSAGE_ID_COLLISION", "written_candidates": 0}
                if r["message_id"] not in msgs_by_id:
                    msgs_by_id[r["message_id"]] = dict(r)

        raw_msgs = sorted(
            msgs_by_id.values(),
            key=lambda m: (m.get("sequence", 0), m.get("created_at", 0)),
        )

        if not raw_msgs:
            return {
                "batch_id": batch_id,
                "session_id": session_id,
                "status": "SKIPPED_EMPTY",
                "proposals_count": 0,
                "written_candidates": 0,
            }

        # 获取该 session 的 project_id 与 agent_id（兼容 sessions 与 raw_sessions 表）
        project_id = "general"
        agent_id = "system"
        s_row = None

        if "sessions" in existing_tables:
            cursor.execute("SELECT project_id, agent_id FROM sessions WHERE session_id = ?", (session_id,))
            s_row = cursor.fetchone()

        if not s_row and "raw_sessions" in existing_tables:
            cursor.execute("SELECT project_id, agent_id FROM raw_sessions WHERE session_id = ?", (session_id,))
            s_row = cursor.fetchone()

        if s_row:
            if s_row["project_id"]:
                project_id = s_row["project_id"]
            if "agent_id" in s_row.keys() and s_row["agent_id"]:
                agent_id = s_row["agent_id"]

        raw_msg_ids = [m["message_id"] for m in raw_msgs]
        raw_msg_map = {m["message_id"]: m["content"] for m in raw_msgs}

        # 2. 确定性组装 Context Pack
        context_pack = await self.context_builder.build_context_pack(
            project_id=project_id,
            recent_messages=raw_msgs,
        )

        # 3. 构造 Prompt 并调用 LLM Worker（支持失败重试）
        system_prompt = self.prompt_manager.get_system_prompt(context_pack=context_pack)
        llm_output_data = None

        if mock_llm_result is not None:
            llm_output_data = mock_llm_result
        elif self.llm_worker is not None:
            user_content = json.dumps(
                [{"message_id": m["message_id"], "role": m["role"], "content": m["content"]} for m in raw_msgs],
                ensure_ascii=False,
            )
            last_err = None
            for attempt in range(1, retries_limit + 1):
                try:
                    if inspect.iscoroutinefunction(self.llm_worker):
                        resp = await self.llm_worker(system_prompt, user_content)
                    else:
                        resp = self.llm_worker(system_prompt, user_content)
                        if inspect.iscoroutine(resp):
                            resp = await resp

                    if isinstance(resp, str):
                        llm_output_data = json.loads(resp)
                    elif isinstance(resp, dict):
                        llm_output_data = resp
                    else:
                        raise ValueError(f"Unexpected response type from LLM worker: {type(resp)}")

                    if not isinstance(llm_output_data, dict):
                        raise ValueError("LLM output must be a JSON dictionary object")
                    break
                except Exception as ex:
                    last_err = ex
                    clean_ex = sanitize_text(str(ex))
                    logger.warning(
                        f"LLM worker attempt {attempt}/{retries_limit} failed for session {session_id}: {clean_ex}"
                    )
                    if attempt < retries_limit:
                        if delay > 0:
                            await asyncio.sleep(delay)
                    else:
                        clean_last = sanitize_text(str(last_err))
                        logger.error(
                            f"All {retries_limit} LLM worker attempts failed for session {session_id}: {clean_last}"
                        )
                        return {
                            "batch_id": batch_id,
                            "session_id": session_id,
                            "project_id": project_id,
                            "agent_id": agent_id,
                            "status": "LLM_ERROR",
                            "error": clean_last,
                            "attempts": retries_limit,
                            "proposals_count": 0,
                            "written_candidates": 0,
                        }
        else:
            # 无 Worker 时跳过 LLM 认知提纯
            return {
                "batch_id": batch_id,
                "session_id": session_id,
                "project_id": project_id,
                "agent_id": agent_id,
                "status": "NO_LLM_WORKER",
                "proposals_count": 0,
                "written_candidates": 0,
            }

        candidates = llm_output_data.get("candidates", [])
        declared_dispositions = llm_output_data.get("message_disposition", [])

        # 4. 双验证器门禁校验
        # 4.1 Coverage 校验与调和（防遗漏/防沉默丢失）
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
                "project_id": project_id,
                "agent_id": agent_id,
                "status": "COVERAGE_REJECTED",
                "error": cov_err,
                "proposals_count": len(candidates),
                "written_candidates": 0,
            }

        # 4.2 Evidence 校验（防编造/防幻觉，严格比对原文与来源）
        valid_candidates = []
        invalid_candidates = []

        for cand in candidates:
            ev_ok, ev_reason = self.evidence_validator.validate_proposal(cand, raw_msg_map)
            if ev_ok:
                valid_candidates.append(cand)
            else:
                cand["rejection_reason"] = ev_reason
                invalid_candidates.append(cand)
                logger.info(f"Proposal rejected by EvidenceValidator for session {session_id}: {ev_reason}")

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

                db_cursor.execute("SELECT session_window_json, operation, subject, predicate, object, content, evidence_source_ids, extracted_spans, rationale, proposed_relation_json, llm_confidence, memory_id FROM curation_candidates WHERE candidate_id = ?", (cand_id,))
                prior = db_cursor.fetchone()
                if prior:
                    if tuple(prior) != (session_window_json, op, subject, predicate, obj, content, ev_ids, spans, rationale, rel_json, conf, target_mem_id):
                        db_conn.rollback()
                        raise ValueError(f"Candidate ID conflict: {cand_id}")
                    continue

                db_cursor.execute(
                    """
                    INSERT INTO curation_candidates (
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
                        rationale, rel_json, PromptManager.PROMPT_VERSION, self.model_name,
                        conf, now_ts
                    ),
                )
                written_count += 1

            db_conn.commit()

        return {
            "batch_id": batch_id,
            "session_id": session_id,
            "project_id": project_id,
            "agent_id": agent_id,
            "status": "SUCCESS",
            "total_candidates": len(candidates),
            "valid_candidates": len(valid_candidates),
            "invalid_candidates": len(invalid_candidates),
            "written_candidates": written_count,
            "reconciled_dispositions_count": len(reconciled_disps),
            "valid_candidate_ids": [c.get("proposal_id") for c in valid_candidates if c.get("proposal_id")],
            "invalid_details": [
                {"proposal_id": c.get("proposal_id"), "reason": c.get("rejection_reason")}
                for c in invalid_candidates
            ],
        }

    async def run_batch(
        self,
        lookback_seconds: int = 86400,
        limit: int = 50,
        batch_id: Optional[str] = None,
        max_retries: Optional[int] = None,
        retry_delay: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        端到端批量执行认知提纯流水线：
        1. 增量扫描 sessions / raw_sessions
        2. 遍历每个 session 执行流水线（支持失败重试与错误隔离）
        3. 统计汇总并返回批量处理审计报表
        """
        batch_id = batch_id or f"recon_batch_{time.strftime('%Y%m%d_%H%M%S')}"
        scanned_sessions = self.scan_incremental_sessions(lookback_seconds=lookback_seconds, limit=limit)

        results = []
        total_valid = 0
        total_invalid = 0
        total_written = 0
        succeeded_sessions = 0
        failed_sessions = 0
        skipped_sessions = 0

        for s in scanned_sessions:
            sess_id = s["session_id"]
            res = await self.run_pipeline_for_session(
                session_id=sess_id,
                batch_id=batch_id,
                max_retries=max_retries,
                retry_delay=retry_delay,
            )
            results.append(res)
            st = res.get("status")
            if st == "SUCCESS":
                succeeded_sessions += 1
                total_valid += res.get("valid_candidates", 0)
                total_invalid += res.get("invalid_candidates", 0)
                total_written += res.get("written_candidates", 0)
            elif st == "SKIPPED_EMPTY":
                skipped_sessions += 1
            else:
                failed_sessions += 1

        return {
            "batch_id": batch_id,
            "sessions_scanned": len(scanned_sessions),
            "sessions_succeeded": succeeded_sessions,
            "sessions_failed": failed_sessions,
            "sessions_skipped": skipped_sessions,
            "total_valid_candidates": total_valid,
            "total_invalid_candidates": total_invalid,
            "total_written_candidates": total_written,
            "details": results,
        }

