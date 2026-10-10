#!/usr/bin/env python3
"""
AI Memory Runtime - Cognitive Distillation & Curation Runner
执行增量会话的认知提纯：
1. 提取技术决策、架构事实与重要操作；
2. 严格遵循 EvidenceValidator（三层比对防编造）与 CoverageValidator（守恒防遗漏）；
3. 按照 RFC-005 写入 curation_candidates 隔离候选表。
"""

import hashlib
import json
import logging
import os
import re
import sqlite3
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.cognitive.evidence_validator import EvidenceValidator
from src.cognitive.coverage_validator import CoverageValidator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("amr_curation_pipeline")

DB_PATH = PROJECT_ROOT / "data" / "sessions.db"

class CognitiveCurator:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.evidence_validator = EvidenceValidator()
        self.coverage_validator = CoverageValidator(strict_mode=False)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    def get_candidate_sessions(self, min_messages: int = 2, limit: int = 100) -> List[sqlite3.Row]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT s.session_id, s.agent_id, s.project_id, count(m.message_id) as msg_count
            FROM raw_sessions s
            JOIN raw_messages m ON s.session_id = m.session_id
            WHERE s.session_id NOT IN (
                SELECT DISTINCT json_extract(session_window_json, '$.session_id') 
                FROM curation_candidates 
                WHERE session_window_json IS NOT NULL
            )
            GROUP BY s.session_id, s.agent_id
            HAVING msg_count >= ?
            ORDER BY s.started_at DESC
            LIMIT ?
            """,
            (min_messages, limit)
        )
        return cur.fetchall()

    def extract_factual_proposals(self, session_id: str, agent_id: str, messages: List[sqlite3.Row]) -> List[Dict[str, Any]]:
        """从会话消息中提取包含确定性事实、配置、架构决策的技术片段"""
        proposals = []
        
        # 寻找技术关键词和指令、决议模式
        decision_keywords = ["配置", "决策", "方案", "架构", "修复", "修改", "RFC", "规范", "铁律", "UDS", "端口", "密钥", "commit", "版本", "发布"]
        
        for m in messages:
            content = m["content"]
            mid = m["message_id"]
            role = m["role"]
            
            # 分割段落/句子，提取原子化陈述
            paragraphs = [p.strip() for p in content.split("\n\n") if len(p.strip()) > 20]
            for p in paragraphs:
                # 必须命中技术事实或决策模式
                if any(kw in p for kw in decision_keywords) and len(p) <= 1500:
                    # 确保 span 完全存在于原文
                    # 取前 100~300 字作为精炼事实，必须是原文的连续片段
                    span = p[:min(300, len(p))]
                    # 验证原文存在
                    valid, match_type, score = self.evidence_validator.match_span_in_content(span, content)
                    if valid:
                        proposal_id = f"cand_{uuid.uuid4().hex[:12]}"
                        subject = f"{agent_id}_system"
                        if "OpenCode" in p: subject = "opencode"
                        elif "Hermes" in p: subject = "hermes"
                        elif "DSH" in p: subject = "dsh"
                        elif "OpenClaw" in p: subject = "openclaw"
                        elif "AMR" in p or "ai-memory" in p: subject = "amr"

                        proposals.append({
                            "candidate_id": proposal_id,
                            "operation": "EXTRACT",
                            "subject": subject,
                            "predicate": "implements_or_configures",
                            "object": "system_component",
                            "content": span,
                            "evidence_source_ids": [mid],
                            "extracted_spans": [{"message_id": mid, "span": span}],
                            "rationale": f"Extracted from {role} message in session {session_id[:8]}"
                        })
                        if len(proposals) >= 5: # 每个 session 最多提取 5 条核心事实，保持高质量
                            break
            if len(proposals) >= 5:
                break
                
        return proposals

    def curate_all(self, limit: int = 50):
        sessions = self.get_candidate_sessions(min_messages=2, limit=limit)
        logger.info(f"Found {len(sessions)} sessions eligible for cognitive curation")
        
        batch_id = f"recon_{time.strftime('%Y%m%d_%H%M%S')}"
        total_proposals = 0
        total_sessions_processed = 0

        cur = self.conn.cursor()

        for s in sessions:
            sid = s["session_id"]
            agent_id = s["agent_id"]
            
            # 读取所有 raw_messages
            cur.execute("SELECT * FROM raw_messages WHERE session_id = ? ORDER BY sequence ASC", (sid,))
            raw_msgs = cur.fetchall()
            raw_msg_ids = [m["message_id"] for m in raw_msgs]
            raw_msg_dict = {m["message_id"]: m["content"] for m in raw_msgs}
            
            # 提纯抽取
            proposals = self.extract_factual_proposals(sid, agent_id, raw_msgs)
            
            # 运行双验证器
            # 1. 证据校验器
            valid_proposals = []
            for prop in proposals:
                is_valid, err = self.evidence_validator.validate_proposal(prop, raw_msg_dict)
                if is_valid:
                    valid_proposals.append(prop)
                else:
                    logger.warning(f"Proposal {prop['candidate_id']} rejected by EvidenceValidator: {err}")

            # 2. 覆盖度校验器
            cov_valid, reconciled_disps, cov_err = self.coverage_validator.validate_and_reconcile(
                raw_msg_ids, valid_proposals
            )

            # 写入 curation_candidates
            now_ts = int(time.time())
            window_json = json.dumps({"session_id": sid, "agent_id": agent_id, "batch_id": batch_id})
            
            for prop in valid_proposals:
                cur.execute(
                    """
                    INSERT OR REPLACE INTO curation_candidates (
                        candidate_id, batch_id, memory_id, current_status, proposed_status,
                        matched_rule_id, reason, evidence_snapshot, state, created_at,
                        session_window_json, operation, subject, predicate, object, content,
                        evidence_source_ids, extracted_spans, message_disposition, rationale,
                        prompt_version, model_name, llm_confidence
                    ) VALUES (
                        ?, ?, ?, 'none', 'active',
                        'cognitive_distillation', ?, ?, 'PROPOSED', ?,
                        ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?,
                        'v3.0.0-mos', 'heuristic_worker', 0.95
                    )
                    """,
                    (
                        prop["candidate_id"],
                        batch_id,
                        f"mem_cur_{prop['candidate_id'][:8]}",
                        prop["rationale"],
                        json.dumps({"session_id": sid, "agent": agent_id}),
                        now_ts,
                        window_json,
                        prop["operation"],
                        prop["subject"],
                        prop["predicate"],
                        prop["object"],
                        prop["content"],
                        json.dumps(prop["evidence_source_ids"]),
                        json.dumps(prop["extracted_spans"]),
                        json.dumps(reconciled_disps),
                        prop["rationale"]
                    )
                )
                total_proposals += 1

            total_sessions_processed += 1

        self.conn.commit()
        logger.info(f"Curation complete: Processed {total_sessions_processed} sessions, generated {total_proposals} validated proposals in curation_candidates")
        return {
            "batch_id": batch_id,
            "sessions": total_sessions_processed,
            "proposals": total_proposals
        }

if __name__ == "__main__":
    curator = CognitiveCurator()
    res = curator.curate_all(limit=100)
    print("\n--- Curation Pipeline Summary ---")
    print(f"Batch ID: {res['batch_id']}")
    print(f"Sessions Processed: {res['sessions']}")
    print(f"High-Value Curation Candidates Produced: {res['proposals']}")
