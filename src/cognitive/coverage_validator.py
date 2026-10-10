"""
AI Memory Runtime - Coverage Validator (防遗漏/防沉默丢失门禁)
遵循 RFC-005 第五节与 opencode_task_mos_v3.md：
检查 message_disposition（EXTRACTED / DISCARDED / UNPROCESSED 守恒）：
EXTRACTED + DISCARDED + UNPROCESSED == 当前输入窗口全部原始消息。
任何未被 LLM 声明处置的消息自动打上 UNPROCESSED，保留在原始消息中，绝不删除，记录在审计报表中。
"""

from typing import Any, Dict, List, Optional, Set, Tuple


class CoverageValidator:
    """
    覆盖度校验器：确保输入窗口内的每条原始消息都被明确处置 (EXTRACTED, DISCARDED, UNPROCESSED)。
    若未声明，或者存在未覆盖消息，进行自动修补与守恒性校验。
    """

    VALID_DISPOSITIONS = {"EXTRACTED", "DISCARDED", "UNPROCESSED"}

    def __init__(self, strict_mode: bool = False):
        """
        strict_mode: 若为 True，未声明的消息将直接导致校验失败；
                     若为 False (默认)，自动将未声明消息补全为 UNPROCESSED。
        """
        self.strict_mode = strict_mode

    def validate_and_reconcile(
        self,
        raw_message_ids: List[str],
        proposals: List[Dict[str, Any]],
        declared_dispositions: Optional[List[Dict[str, Any]]] = None,
    ) -> Tuple[bool, List[Dict[str, Any]], Optional[str]]:
        """
        验证并调和消息处置清单：
        - raw_message_ids: 当前提纯窗口内所有原始消息 ID
        - proposals: 提炼出的候选提案列表（提案内部可能自带 message_disposition）
        - declared_dispositions: LLM 顶层声明的 message_disposition 列表（若有）
        
        返回:
        (is_valid, reconciled_dispositions, error_msg)
        """
        all_raw_ids = set(raw_message_ids)
        disposition_map: Dict[str, Dict[str, Any]] = {}

        # 1. 收集所有顶层声明的 dispositions
        if declared_dispositions:
            for item in declared_dispositions:
                m_id = item.get("message_id")
                disp = item.get("disposition", "UNPROCESSED").upper()
                if disp not in self.VALID_DISPOSITIONS:
                    disp = "UNPROCESSED"
                if m_id:
                    disposition_map[m_id] = {
                        "message_id": m_id,
                        "disposition": disp,
                        "reason": item.get("reason"),
                    }

        # 2. 从 proposals 中收集 (提案内部声明的 extracted 或 disposition)
        for prop in proposals:
            # 来自 prop.message_disposition
            prop_disps = prop.get("message_disposition", [])
            for item in prop_disps:
                m_id = item.get("message_id")
                disp = item.get("disposition", "UNPROCESSED").upper()
                if disp not in self.VALID_DISPOSITIONS:
                    disp = "UNPROCESSED"
                if m_id and m_id not in disposition_map:
                    disposition_map[m_id] = {
                        "message_id": m_id,
                        "disposition": disp,
                        "reason": item.get("reason"),
                    }

            # 提案中列为 evidence_source_ids 的，默认为 EXTRACTED
            for e_id in prop.get("evidence_source_ids", []):
                if e_id in all_raw_ids:
                    disposition_map[e_id] = {
                        "message_id": e_id,
                        "disposition": "EXTRACTED",
                        "reason": prop.get("rationale") or "Used as proposal evidence",
                    }

        # 3. 检查守恒性与遗漏补全
        declared_ids = set(disposition_map.keys())
        missing_ids = all_raw_ids - declared_ids

        if missing_ids and self.strict_mode:
            return False, list(disposition_map.values()), f"COVERAGE_VIOLATION: Messages {list(missing_ids)} not accounted for in disposition"

        # 自动补全未声明的消息为 UNPROCESSED (防遗漏/防沉默丢失)
        for m_id in missing_ids:
            disposition_map[m_id] = {
                "message_id": m_id,
                "disposition": "UNPROCESSED",
                "reason": "Not processed by distiller; preserved as unprocessed raw evidence",
            }

        # 4. 排序生成规整的 dispositions 列表
        reconciled = [disposition_map[m_id] for m_id in raw_message_ids if m_id in disposition_map]

        # 5. 校验守恒公式
        extracted_cnt = sum(1 for d in reconciled if d["disposition"] == "EXTRACTED")
        discarded_cnt = sum(1 for d in reconciled if d["disposition"] == "DISCARDED")
        unprocessed_cnt = sum(1 for d in reconciled if d["disposition"] == "UNPROCESSED")
        total_cnt = len(raw_message_ids)

        if (extracted_cnt + discarded_cnt + unprocessed_cnt) != total_cnt:
            return False, reconciled, f"CONSERVATION_ERROR: {extracted_cnt}+{discarded_cnt}+{unprocessed_cnt} != {total_cnt}"

        return True, reconciled, None
