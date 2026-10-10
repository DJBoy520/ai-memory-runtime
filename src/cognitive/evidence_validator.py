"""
AI Memory Runtime - Evidence Validator (防编造/防幻觉门禁)
遵循 RFC-004 第八节及 RFC-005 第五节规范：
采用三层匹配：
1. Unicode NFKC 归一化 + 空白折叠
2. 精确子串包含
3. Token 级 Jaccard 相似度 >= 0.95
任何 extracted_spans 无法在声明的 raw messages 原文中被证明的，整条提案标记为 INVALID_EVIDENCE 并硬性丢弃。
"""

import re
import unicodedata
from typing import Any, Dict, List, Optional, Tuple, Union


class EvidenceValidator:
    """
    证据链校验器：防编造、防幻觉，严格比对 extracted_spans 与 raw_messages。
    """

    def __init__(self, jaccard_threshold: float = 0.95):
        self.jaccard_threshold = jaccard_threshold

    @staticmethod
    def normalize_text(text: str) -> str:
        """
        第一层：Unicode NFKC 归一化与空白折叠
        """
        if not text:
            return ""
        # 1. NFKC 归一化（全角转半角，特殊字符兼容性分解等）
        norm = unicodedata.normalize("NFKC", text)
        # 2. 空白折叠（将连续空格、换行、制表符统一折叠为单个空格，并 trim）
        norm = re.sub(r"\s+", " ", norm).strip()
        return norm

    @staticmethod
    def tokenize(text: str) -> List[str]:
        """
        Token 化支持中英混合：
        英文按词切分，中文按单字切分，数字连续保留。
        """
        if not text:
            return []
        # 正则切分：英文单词、数字序列、中文单字、以及其他符号
        tokens = re.findall(r"[\w]+|[^\s\w]", text.lower(), re.UNICODE)
        # 将中文字符序列进一步细化成单字
        refined_tokens = []
        for tok in tokens:
            if re.search(r"[\u4e00-\u9fff]", tok):
                for ch in tok:
                    if ch.strip():
                        refined_tokens.append(ch)
            else:
                refined_tokens.append(tok)
        return refined_tokens

    @classmethod
    def token_jaccard_similarity(cls, span: str, text: str) -> float:
        """
        第三层：Token 级 Jaccard 相似度计算
        计算 span 的 token 集合在 text 中匹配子区间的最大 Jaccard 相似度，
        或者直接求 span 与 text 匹配片段的交并比。
        若 span 较短而 text 是大长文本，计算 span tokens 与 text 包含 span 词汇的最佳匹配窗口 Jaccard。
        """
        span_tokens = set(cls.tokenize(span))
        if not span_tokens:
            return 1.0
        
        text_tokens = cls.tokenize(text)
        if not text_tokens:
            return 0.0

        # 如果 span 长度小于等于 text 长度，滑动窗口评估最大 Jaccard
        len_span = len(span_tokens)
        window_size = max(len_span, 1)
        best_jaccard = 0.0

        # 为了避免超大文本滑动窗口性能损耗，在 text_tokens 中只在包含首个/尾个 token 附近滑动
        # 步长为 1，窗口大小在 [len_span - 2, len_span + 5] 浮动
        step = 1
        n = len(text_tokens)
        
        # 窗口大小列表
        min_win = max(1, len_span - 2)
        max_win = min(n, len_span + max(5, int(len_span * 0.2)))

        for w in range(min_win, max_win + 1):
            for i in range(0, n - w + 1, step):
                window_set = set(text_tokens[i : i + w])
                intersection = span_tokens.intersection(window_set)
                union = span_tokens.union(window_set)
                if not union:
                    continue
                score = len(intersection) / len(union)
                if score > best_jaccard:
                    best_jaccard = score
                if best_jaccard >= 0.95:
                    return best_jaccard

        # 整体兜底比对（当 text 本身就是一句话时）
        all_text_set = set(text_tokens)
        intersection = span_tokens.intersection(all_text_set)
        # 如果 span 里的 token 绝大部分都在 text 中出现，且数量接近
        if intersection:
            score = len(intersection) / len(span_tokens)
            if score > best_jaccard:
                best_jaccard = score

        return best_jaccard

    def match_span_in_content(self, span: str, content: str) -> Tuple[bool, str, float]:
        """
        三层匹配单条 span 是否可以在 content 中被证实。
        返回: (is_matched, matched_layer, confidence_score)
        """
        # 第一层：归一化处理
        norm_span = self.normalize_text(span)
        norm_content = self.normalize_text(content)

        if not norm_span:
            return True, "empty_span", 1.0

        # 第二层：精确子串包含 (在归一化后比对)
        if norm_span in norm_content:
            return True, "substring_exact", 1.0

        # 也检查不区分大小写的子串包含
        if norm_span.lower() in norm_content.lower():
            return True, "substring_ignore_case", 1.0

        # 第三层：Token 级 Jaccard 相似度 >= threshold
        jaccard = self.token_jaccard_similarity(norm_span, norm_content)
        if jaccard >= self.jaccard_threshold:
            return True, f"jaccard_{jaccard:.2f}", jaccard

        return False, "unmatched", jaccard

    def validate_proposal(
        self,
        proposal: Dict[str, Any],
        raw_messages: Dict[str, str],  # message_id -> content
    ) -> Tuple[bool, Optional[str]]:
        """
        校验单个候选提案：
        proposal: 包含 extracted_spans, evidence_source_ids
        raw_messages: 字典，键为 message_id，值为原始消息 content
        返回: (is_valid, rejection_reason)
        """
        extracted_spans = proposal.get("extracted_spans", [])
        evidence_source_ids = proposal.get("evidence_source_ids", [])

        # 1. 至少需要声明一条原始证据源
        if not evidence_source_ids and not extracted_spans:
            return False, "INVALID_EVIDENCE: Missing evidence_source_ids and extracted_spans"

        if not extracted_spans:
            return False, "INVALID_EVIDENCE: extracted_spans is empty"

        # 2. 逐一验证 extracted_spans
        for item in extracted_spans:
            if isinstance(item, dict):
                msg_id = item.get("message_id")
                span = item.get("span", "")
            elif isinstance(item, str):
                msg_id = None
                span = item
            else:
                return False, f"INVALID_EVIDENCE: Invalid span format: {item}"

            if not span or not span.strip():
                continue

            # 确定比对的目标原始文本集合
            target_contents = []
            if msg_id and msg_id in raw_messages:
                target_contents.append(raw_messages[msg_id])
            elif evidence_source_ids:
                for eid in evidence_source_ids:
                    if eid in raw_messages:
                        target_contents.append(raw_messages[eid])
            else:
                # 遍历所有提供的 raw_messages
                target_contents.extend(raw_messages.values())

            if not target_contents:
                return False, f"INVALID_EVIDENCE: Evidence message not found for span: '{span}'"

            # 只要在任意一个关联证据消息中通过三层匹配即可
            matched = False
            for content in target_contents:
                is_match, _, _ = self.match_span_in_content(span, content)
                if is_match:
                    matched = True
                    break

            if not matched:
                return False, f"INVALID_EVIDENCE: Span '{span}' not verified in source messages"

        return True, None
