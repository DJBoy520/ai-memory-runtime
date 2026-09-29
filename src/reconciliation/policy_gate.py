"""
src/reconciliation/policy_gate.py
实现 RFC-003 第 3 与 第 5 节的策略闸门机制与状态流转机
"""

from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


class CandidateState(str, Enum):
    DETECTED = "DETECTED"
    PROPOSED = "PROPOSED"
    POLICY_CHECK = "POLICY_CHECK"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    APPLIED = "APPLIED"


class PolicyGate:
    """
    PolicyGate 策略审查闸门：
    1. 状态机流转: DETECTED -> PROPOSED -> POLICY_CHECK -> APPROVED / REJECTED -> APPLIED
    2. 熔断保护绝对上下限: MaxAllowedChanges = max(50, min(0.05 * TotalPoints, 5000))
    3. 白名单绝对保护:
       - 包含 `[LESSON_LEARNED]` 标记拒绝删除 (REJECTED)
       - 高置信度/高重要度记忆 (confidence >= 0.9 且 importance >= 0.8) 需人工确认，拒绝自动删除
    """

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run

    @staticmethod
    def calculate_max_allowed_changes(total_points: int) -> int:
        """
        计算单批次允许流转变更的最大数量限制算子：
        MaxAllowedChanges = max(50, min(0.05 * TotalPoints, 5000))
        """
        dynamic_limit = int(0.05 * total_points)
        clamped = min(dynamic_limit, 5000)
        return max(50, clamped)

    def evaluate(
        self,
        candidates: List[Dict[str, Any]],
        total_points: int,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
        """
        评估治理候选集：
        返回: (approved_candidates, rejected_candidates, evaluation_summary)
        """
        max_allowed = self.calculate_max_allowed_changes(total_points)
        approved: List[Dict[str, Any]] = []
        rejected: List[Dict[str, Any]] = []

        summary = {
            "total_candidates": len(candidates),
            "max_allowed": max_allowed,
            "whitelist_blocked": 0,
            "high_value_blocked": 0,
            "circuit_breaker_tripped": False,
            "dry_run": self.dry_run,
        }

        # 检查是否熔断：如果候选变更总数超过绝对熔断上限，触发熔断
        # 注：RFC 5.3 规定单批次允许流转变更的最大数量限制算子
        active_change_count = len(candidates)
        if active_change_count > max_allowed:
            summary["circuit_breaker_tripped"] = True
            # 熔断触发，全量拒绝或截断？RFC: 杜绝规则失效导致的大面积误删
            # 策略：超出熔断门限时，全部打回人工审核，避免误伤
            for cand in candidates:
                cand["state"] = CandidateState.REJECTED.value
                cand["rejection_reason"] = f"熔断拦截: 候选数量 {active_change_count} 超过安全阈值 {max_allowed}"
                rejected.append(cand)
            return approved, rejected, summary

        for cand in candidates:
            # 状态机：进入 POLICY_CHECK
            cand["state"] = CandidateState.POLICY_CHECK.value
            content = cand.get("content", "")
            proposed_status = cand.get("proposed_status", "")
            confidence = float(cand.get("confidence", 0.0))
            importance = float(cand.get("importance", 0.0))

            # 1. 白名单检查: [LESSON_LEARNED] 绝对保护
            if proposed_status == "deleted" and "[LESSON_LEARNED]" in content:
                cand["state"] = CandidateState.REJECTED.value
                cand["rejection_reason"] = "白名单保护: 包含 [LESSON_LEARNED] 绝对豁免删除"
                summary["whitelist_blocked"] += 1
                rejected.append(cand)
                continue

            # 2. 高置信度/高重要度保护: (confidence >= 0.9 且 importance >= 0.8) 禁止自动销毁
            if proposed_status == "deleted" and confidence >= 0.9 and importance >= 0.8:
                cand["state"] = CandidateState.REJECTED.value
                cand["rejection_reason"] = "高价值保护: confidence>=0.9 且 importance>=0.8 需人工复核"
                summary["high_value_blocked"] += 1
                rejected.append(cand)
                continue

            # 审核通过
            cand["state"] = CandidateState.APPROVED.value
            approved.append(cand)

        return approved, rejected, summary
