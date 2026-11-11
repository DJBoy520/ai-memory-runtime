"""
AI Memory Runtime - Cognitive Distillation Runner & Batch Orchestrator
遵循 RFC-004、RFC-005 及系统架构规范：
1. 主控调度：将 SessionStore、CognitiveScheduler 与 ModelWorker 无缝接通；
2. 默认安全：默认不发送真实会话、不自动启用外部模型；
3. 严格超时与重试保留：执行失败时错误原因清晰可见，并在专用状态表中持久化待重试；
4. 隐私安全保护：严禁在任何日志中打印凭据或会话内容；
5. 14 天 Dry-Run 铁律：所有提炼提案仅作为 PROPOSED 状态暂存 curation_candidates，绝不直接写 memories SSOT。
"""

import asyncio
import json
import logging
import os
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from src.core.session_store import SessionStore
from src.cognitive.scheduler import CognitiveScheduler
from src.cognitive.model_worker import (
    BaseModelWorker,
    DryRunWorker,
    MockModelWorker,
    OpenAICompatibleWorker,
    mask_secret,
    sanitize_text,
)

logger = logging.getLogger("amr_distill_runner")


def resolve_runner_config(
    cli_args: Optional[Any] = None,
    config_dict: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    配置分级仲裁解析（优先级：CLI 参数 > 环境变量 > 配置文件 > 安全默认值）。
    确保默认禁用外部模型，安全防泄漏。
    """
    config_dict = config_dict or {}

    # 1. 默认安全底线
    resolved: Dict[str, Any] = {
        "enable_external_model": False,
        "base_url": "http://localhost:8000/v1",
        "model": "gpt-4o",
        "api_key": "",
        "timeout": 30.0,
        "max_retries": 3,
        "retry_delay": 1.0,
        "lookback_seconds": 86400,
        "limit": 50,
        "dry_run": False,
    }

    # 2. 配置文件覆盖 (若有)
    if "distillation" in config_dict and isinstance(config_dict["distillation"], dict):
        d_cfg = config_dict["distillation"]
        for k in resolved:
            if k in d_cfg and d_cfg[k] is not None:
                resolved[k] = d_cfg[k]

    # 3. 环境变量覆盖
    env_enable = os.environ.get("AMR_DISTILL_ENABLE_EXTERNAL")
    if env_enable is not None:
        resolved["enable_external_model"] = env_enable.strip().lower() in ("1", "true", "yes", "on")

    env_base_url = (
        os.environ.get("AMR_DISTILL_BASE_URL")
        or os.environ.get("OPENAI_BASE_URL")
        or os.environ.get("OPENAI_API_BASE")
    )
    if env_base_url:
        resolved["base_url"] = env_base_url.strip()

    env_model = (
        os.environ.get("AMR_DISTILL_MODEL")
        or os.environ.get("OPENAI_MODEL")
        or os.environ.get("OPENAI_MODEL_NAME")
    )
    if env_model:
        resolved["model"] = env_model.strip()

    env_api_key = (
        os.environ.get("AMR_DISTILL_API_KEY")
        or os.environ.get("OPENAI_API_KEY")
    )
    if env_api_key:
        resolved["api_key"] = env_api_key.strip()

    env_timeout = os.environ.get("AMR_DISTILL_TIMEOUT")
    if env_timeout:
        try:
            resolved["timeout"] = float(env_timeout)
        except ValueError:
            pass

    env_retries = os.environ.get("AMR_DISTILL_MAX_RETRIES")
    if env_retries:
        try:
            resolved["max_retries"] = int(env_retries)
        except ValueError:
            pass

    env_delay = os.environ.get("AMR_DISTILL_RETRY_DELAY")
    if env_delay:
        try:
            resolved["retry_delay"] = float(env_delay)
        except ValueError:
            pass

    # 4. CLI 参数覆盖（最高优先级）
    if cli_args is not None:
        if getattr(cli_args, "enable_external_model", False):
            resolved["enable_external_model"] = True
        if getattr(cli_args, "base_url", None):
            resolved["base_url"] = cli_args.base_url.strip()
        if getattr(cli_args, "model", None):
            resolved["model"] = cli_args.model.strip()
        if getattr(cli_args, "api_key", None):
            resolved["api_key"] = cli_args.api_key.strip()
        if getattr(cli_args, "timeout", None) is not None:
            resolved["timeout"] = float(cli_args.timeout)
        if getattr(cli_args, "max_retries", None) is not None:
            resolved["max_retries"] = int(cli_args.max_retries)
        if getattr(cli_args, "retry_delay", None) is not None:
            resolved["retry_delay"] = float(cli_args.retry_delay)
        if getattr(cli_args, "lookback", None) is not None:
            resolved["lookback_seconds"] = int(cli_args.lookback)
        if getattr(cli_args, "limit", None) is not None:
            resolved["limit"] = int(cli_args.limit)
        if getattr(cli_args, "dry_run", False):
            resolved["dry_run"] = True

    return resolved


class DistillationRunner:
    """
    AMR 会话认知提纯调度执行主控：
    1. 负责会话增量扫描与候选过滤；
    2. 管理会话级提纯状态表 (curation_distillation_state)，记录失败历史以便精确重试；
    3. 驱动 CognitiveScheduler 执行端到端提取、校验与候选落库；
    4. 产出结构化审计报表，确保失败可见性。
    """

    def __init__(
        self,
        session_store: SessionStore,
        model_worker: Optional[BaseModelWorker] = None,
        scheduler: Optional[CognitiveScheduler] = None,
        allow_external: bool = False,
        dry_run: bool = False,
        default_max_retries: int = 3,
        default_retry_delay: float = 1.0,
    ):
        self.session_store = session_store
        self.allow_external = allow_external
        self.dry_run = dry_run
        self.default_max_retries = default_max_retries
        self.default_retry_delay = default_retry_delay

        # 确定装配的工作器
        if model_worker is not None:
            self.model_worker = model_worker
        elif self.dry_run or not self.allow_external:
            # 安全默认：装载 DryRunWorker，绝对不调用外部网络
            self.model_worker = DryRunWorker()
        else:
            raise ValueError(
                "External model is allowed but no model_worker was provided. "
                "Explicitly construct OpenAICompatibleWorker or supply model configuration."
            )

        # 调度器装配
        self.scheduler = scheduler or CognitiveScheduler(
            session_store=self.session_store,
            llm_worker=self.model_worker,
            default_max_retries=self.default_max_retries,
            default_retry_delay=self.default_retry_delay,
            model_name=getattr(self.model_worker, "model_name", "cognitive_worker"),
        )

        # 确保持久化状态表存在
        self._ensure_state_table()

    def _ensure_state_table(self) -> None:
        """
        初始化提纯状态记录表（curation_distillation_state），用于追踪会话提纯进度与保留重试上下文。
        不修改原 session_store.py 代码，通过标准数据库连接幂等创建。
        """
        with self.session_store._lock:
            conn = self.session_store.get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS curation_distillation_state (
                    session_id TEXT PRIMARY KEY,
                    batch_id TEXT NOT NULL,
                    project_id TEXT,
                    agent_id TEXT,
                    status TEXT NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED', 'SKIPPED')),
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_retries INTEGER NOT NULL DEFAULT 3,
                    last_error TEXT,
                    last_error_type TEXT,
                    model_name TEXT,
                    candidates_count INTEGER DEFAULT 0,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    next_retry_at INTEGER
                );
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_distill_status ON curation_distillation_state(status, next_retry_at);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_distill_batch ON curation_distillation_state(batch_id);"
            )
            conn.commit()

    def scan_sessions(
        self,
        lookback_seconds: int = 86400,
        limit: int = 50,
        retry_failed_only: bool = False,
        specific_session_id: Optional[str] = None,
        force: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        扫描需要执行提纯的会话列表：
        - specific_session_id: 针对单个目标会话；
        - retry_failed_only: 仅拉取已标记为 FAILED 待重试的会话；
        - 增量扫描：扫描窗口内的新会话，自动跳过已成功 (SUCCESS) 的会话（除非 force=True）。
        """
        conn = self.session_store.get_connection()
        cur = conn.cursor()

        # 场景 A: 指定单个特定会话
        if specific_session_id:
            # 兼容 sessions 表和 raw_sessions 表
            sess_info = None
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('sessions', 'raw_sessions')")
            tables = {r[0] for r in cur.fetchall()}

            if "sessions" in tables:
                cur.execute("SELECT session_id, agent_id, project_id, started_at, ended_at, status FROM sessions WHERE session_id = ?", (specific_session_id,))
                row = cur.fetchone()
                if row:
                    sess_info = dict(row)

            if not sess_info and "raw_sessions" in tables:
                cur.execute("SELECT session_id, agent_id, project_id, started_at, ended_at, status FROM raw_sessions WHERE session_id = ?", (specific_session_id,))
                row = cur.fetchone()
                if row:
                    sess_info = dict(row)

            if sess_info:
                return [sess_info]
            # 若表中未明确注册但消息可能存在，构造最小元数据
            return [{"session_id": specific_session_id, "agent_id": "system", "project_id": "general"}]

        # 场景 B: 仅拉取失败待重试的会话
        if retry_failed_only:
            cur.execute(
                """
                SELECT session_id, project_id, agent_id, attempts, last_error
                FROM curation_distillation_state
                WHERE status = 'FAILED'
                ORDER BY updated_at ASC
                LIMIT ?
                """,
                (limit,),
            )
            failed_rows = cur.fetchall()
            return [dict(r) for r in failed_rows]

        # 场景 C: 正常增量扫描
        scanned = self.scheduler.scan_incremental_sessions(lookback_seconds=lookback_seconds, limit=limit)
        if not scanned or force:
            return scanned

        # 过滤掉已经成功处理过的 session_id
        session_ids = [s["session_id"] for s in scanned]
        placeholders = ",".join("?" for _ in session_ids)
        cur.execute(
            f"SELECT session_id FROM curation_distillation_state WHERE status = 'SUCCESS' AND session_id IN ({placeholders})",
            tuple(session_ids),
        )
        succeeded_ids = {r[0] for r in cur.fetchall()}

        return [s for s in scanned if s["session_id"] not in succeeded_ids]

    async def run_pipeline_for_session(
        self,
        session_id: str,
        batch_id: Optional[str] = None,
        max_retries: Optional[int] = None,
        retry_delay: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        为单个会话执行认知整理闭环流水线，并同步落库提纯状态以保留重试跟踪。
        """
        now = int(time.time())
        batch_id = batch_id or f"distill_{time.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        retries_limit = max_retries if max_retries is not None else self.default_max_retries
        delay = retry_delay if retry_delay is not None else self.default_retry_delay

        # 登记或更新状态为 RUNNING
        with self.session_store._lock:
            conn = self.session_store.get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO curation_distillation_state (
                    session_id, batch_id, status, attempts, max_retries,
                    model_name, created_at, updated_at
                )
                VALUES (?, ?, 'RUNNING', 1, ?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    batch_id = excluded.batch_id,
                    status = 'RUNNING',
                    attempts = curation_distillation_state.attempts + 1,
                    updated_at = excluded.updated_at
                """,
                (
                    session_id,
                    batch_id,
                    retries_limit,
                    getattr(self.model_worker, "model_name", "unknown"),
                    now,
                    now,
                ),
            )
            conn.commit()

        # 记录脱敏日志（严禁打印会话内容）
        logger.info(
            "Starting distillation pipeline for session %s (batch: %s)",
            session_id,
            batch_id,
        )

        res = await self.scheduler.run_pipeline_for_session(
            session_id=session_id,
            batch_id=batch_id,
            max_retries=retries_limit,
            retry_delay=delay,
        )

        status = res.get("status", "UNKNOWN")
        written = res.get("written_candidates", 0)
        now_after = int(time.time())

        # 依据执行结果原子更新状态表
        with self.session_store._lock:
            conn = self.session_store.get_connection()
            cur = conn.cursor()
            if status == "SUCCESS":
                cur.execute(
                    """
                    UPDATE curation_distillation_state
                    SET status = 'SUCCESS',
                        candidates_count = ?,
                        last_error = NULL,
                        last_error_type = NULL,
                        updated_at = ?
                    WHERE session_id = ?
                    """,
                    (written, now_after, session_id),
                )
                logger.info(
                    "Session %s distillation succeeded: %d candidates proposed",
                    session_id,
                    written,
                )
            elif status == "SKIPPED_EMPTY":
                cur.execute(
                    """
                    UPDATE curation_distillation_state
                    SET status = 'SKIPPED',
                        updated_at = ?
                    WHERE session_id = ?
                    """,
                    (now_after, session_id),
                )
                logger.info("Session %s skipped: empty messages", session_id)
            else:
                # 失败状态保留待重试
                clean_err = sanitize_text(str(res.get("error", "Unknown error")))
                run_attempts = res.get("attempts", 1)
                cur.execute(
                    """
                    UPDATE curation_distillation_state
                    SET status = 'FAILED',
                        attempts = max(attempts, ?),
                        last_error = ?,
                        last_error_type = ?,
                        updated_at = ?
                    WHERE session_id = ?
                    """,
                    (run_attempts, clean_err, status, now_after, session_id),
                )
                logger.warning(
                    "Session %s distillation failed (%s): %s",
                    session_id,
                    status,
                    clean_err,
                )
            conn.commit()

        return res

    async def run_batch(
        self,
        lookback_seconds: int = 86400,
        limit: int = 50,
        batch_id: Optional[str] = None,
        retry_failed_only: bool = False,
        specific_session_id: Optional[str] = None,
        force: bool = False,
        max_retries: Optional[int] = None,
        retry_delay: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        端到端批量驱动执行：
        1. 增量或目标会话扫描；
        2. 依次调度认知提纯流水线（支持失败重试与错误隔离）；
        3. 产出最终批量审计结果。
        """
        batch_id = batch_id or f"distill_batch_{time.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

        scanned = self.scan_sessions(
            lookback_seconds=lookback_seconds,
            limit=limit,
            retry_failed_only=retry_failed_only,
            specific_session_id=specific_session_id,
            force=force,
        )

        results: List[Dict[str, Any]] = []
        succeeded = 0
        failed = 0
        skipped = 0
        total_candidates = 0

        for item in scanned:
            sid = item["session_id"]
            res = await self.run_pipeline_for_session(
                session_id=sid,
                batch_id=batch_id,
                max_retries=max_retries,
                retry_delay=retry_delay,
            )
            results.append(res)
            st = res.get("status")
            if st == "SUCCESS":
                succeeded += 1
                total_candidates += res.get("written_candidates", 0)
            elif st == "SKIPPED_EMPTY":
                skipped += 1
            else:
                failed += 1

        failed_details = [
            {
                "session_id": r.get("session_id"),
                "status": r.get("status"),
                "error": sanitize_text(str(r.get("error", ""))),
                "attempts": r.get("attempts", 1),
            }
            for r in results
            if r.get("status") not in ("SUCCESS", "SKIPPED_EMPTY")
        ]

        summary = {
            "batch_id": batch_id,
            "dry_run": self.dry_run,
            "external_model_enabled": getattr(self.model_worker, "is_external", False),
            "model_name": getattr(self.model_worker, "model_name", "unknown"),
            "sessions_scanned": len(scanned),
            "sessions_succeeded": succeeded,
            "sessions_failed": failed,
            "sessions_skipped": skipped,
            "total_written_candidates": total_candidates,
            "failed_sessions": failed_details,
        }
        return summary

    def get_failed_sessions(self, limit: int = 50) -> List[Dict[str, Any]]:
        """查询当前数据库中记录的失败待重试会话"""
        conn = self.session_store.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT session_id, batch_id, attempts, last_error_type, last_error, updated_at
            FROM curation_distillation_state
            WHERE status = 'FAILED'
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(r) for r in cur.fetchall()]

    def get_distillation_summary(self) -> Dict[str, Any]:
        """获取当前提纯全貌统计（按状态汇总）"""
        conn = self.session_store.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT status, count(*) as cnt
            FROM curation_distillation_state
            GROUP BY status
            """
        )
        counts = {r["status"]: r["cnt"] for r in cur.fetchall()}

        cur.execute("SELECT count(*) as total_proposals FROM curation_candidates WHERE state = 'PROPOSED'")
        total_proposals = cur.fetchone()["total_proposals"]

        return {
            "statuses": counts,
            "total_proposed_candidates": total_proposals,
        }

    def reset_failed_sessions(self, session_ids: Optional[List[str]] = None) -> int:
        """重置指定或全部失败会话为 PENDING 状态"""
        with self.session_store._lock:
            conn = self.session_store.get_connection()
            cur = conn.cursor()
            now = int(time.time())
            if session_ids:
                placeholders = ",".join("?" for _ in session_ids)
                cur.execute(
                    f"UPDATE curation_distillation_state SET status = 'PENDING', updated_at = ? WHERE status = 'FAILED' AND session_id IN ({placeholders})",
                    (now, *session_ids),
                )
            else:
                cur.execute(
                    "UPDATE curation_distillation_state SET status = 'PENDING', updated_at = ? WHERE status = 'FAILED'",
                    (now,),
                )
            conn.commit()
            return cur.rowcount
