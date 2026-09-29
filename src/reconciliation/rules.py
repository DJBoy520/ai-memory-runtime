"""
src/reconciliation/rules.py
实现确定性规则判定引擎 (Deterministic Rule Engine)
识别纯心跳、超长循环工具报错、临时协议片段注入等噪音
"""

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class CurationRule:
    rule_id: str
    name: str
    target_status: str
    reason: str


class RuleEngine:
    """
    确定性规则判定引擎：
    Rule Engine 只负责发现（DETECTED -> PROPOSED），不负责决定。
    """

    HEARTBEAT_PATTERNS = [
        re.compile(r"^ping\b", re.IGNORECASE),
        re.compile(r"^pong\b", re.IGNORECASE),
        re.compile(r"^heartbeat\b", re.IGNORECASE),
        re.compile(r"^\[HEARTBEAT\]", re.IGNORECASE),
    ]

    TOOL_LOOP_PATTERNS = [
        re.compile(r"(?:error:\s*(?:timeout|connection\s+refused|broken\s+pipe)\s*){3,}", re.IGNORECASE),
        re.compile(r"Traceback\s+\(most\s+recent\s+call\s+last\):.*(?:Repeated|RecursionError)", re.DOTALL | re.IGNORECASE),
    ]

    TEMPORARY_PROTOCOL_PATTERNS = [
        re.compile(r"^\[TEMP_INJECT_PROTOCOL_v\d+\]", re.IGNORECASE),
        re.compile(r"<!--TEMP_AMR_SYNC_MARKER-->", re.IGNORECASE),
    ]

    def scan_memory(self, memory: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        对单条记忆执行规则扫描。若命中规则，返回候选结构（状态为 PROPOSED），否则返回 None。
        """
        content = memory.get("content", "").strip()
        current_status = memory.get("status", "active")
        memory_id = memory.get("memory_id", "")

        # 仅针对 active 或 stale 进行噪音识别
        if current_status not in ("active", "stale"):
            return None

        # 1. 规则: 纯心跳噪音 (RULE_HEARTBEAT_NOISE) -> 建议转为 deleted 或 archived
        for pattern in self.HEARTBEAT_PATTERNS:
            if pattern.search(content) and len(content) < 50:
                return {
                    "memory_id": memory_id,
                    "current_status": current_status,
                    "proposed_status": "deleted",
                    "matched_rule_id": "RULE_HEARTBEAT_NOISE",
                    "reason": "检测到无意义的心跳探测文本",
                    "evidence_snapshot": content[:200],
                    "content": content,
                    "confidence": memory.get("confidence", 0.8),
                    "importance": memory.get("importance", 0.5),
                }

        # 2. 规则: 工具超长循环报错 (RULE_TOOL_LOOP_ERROR) -> 建议转为 deleted
        for pattern in self.TOOL_LOOP_PATTERNS:
            if pattern.search(content):
                return {
                    "memory_id": memory_id,
                    "current_status": current_status,
                    "proposed_status": "deleted",
                    "matched_rule_id": "RULE_TOOL_LOOP_ERROR",
                    "reason": "检测到工具死循环或堆栈爆炸报错片段",
                    "evidence_snapshot": content[:200],
                    "content": content,
                    "confidence": memory.get("confidence", 0.8),
                    "importance": memory.get("importance", 0.5),
                }

        # 3. 规则: 临时协议注入标记 (RULE_TEMP_PROTOCOL_INJECT) -> 建议转为 deleted
        for pattern in self.TEMPORARY_PROTOCOL_PATTERNS:
            if pattern.search(content):
                return {
                    "memory_id": memory_id,
                    "current_status": current_status,
                    "proposed_status": "deleted",
                    "matched_rule_id": "RULE_TEMP_PROTOCOL_INJECT",
                    "reason": "检测到历史临时协议测试注入标记",
                    "evidence_snapshot": content[:200],
                    "content": content,
                    "confidence": memory.get("confidence", 0.8),
                    "importance": memory.get("importance", 0.5),
                }

        return None

    def scan_all(self, memories: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """批量扫描全部记忆"""
        candidates = []
        for mem in memories:
            res = self.scan_memory(mem)
            if res:
                candidates.append(res)
        return candidates
