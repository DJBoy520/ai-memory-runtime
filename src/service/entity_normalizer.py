"""
AI Memory Runtime - Entity Normalizer Service
实体归一化服务：支持中英文同义词字典映射、大小写转换、连字符/下划线清洗、多余空格去除等标准化。
遵循 Task 02 契约与 P1-11 规范。
"""

import re
from typing import Dict, Optional


class EntityNormalizer:
    """
    实体与谓词归一化器
    提供基于别名映射表和通用规则的实体规范化。
    """

    # 内置通用实体别名映射表 (别名全部预先转换为小写比较)
    DEFAULT_ALIAS_MAP: Dict[str, str] = {
        # 饮料/偏好实体示例 (任务书与规格示例)
        "美式": "americano_coffee",
        "美式咖啡": "americano_coffee",
        "americano": "americano_coffee",
        "拿铁": "latte_coffee",
        "latte": "latte_coffee",
        # 系统/服务实体示例
        "aep-tsa": "aep_tsa",
        "aeptsa": "aep_tsa",
        "aep_tsa": "aep_tsa",
        "sub2api": "sub2api",
        "sub-2-api": "sub2api",
        "amr": "ai_memory_runtime",
        "ai-memory-runtime": "ai_memory_runtime",
        "qdrant": "qdrant",
        "sqlite": "sqlite",
        "bge-m3": "bge_m3",
        "bgem3": "bge_m3",
        # 通用主体
        "user": "user",
        "用户": "user",
        "我": "user",
        "assistant": "assistant",
        "ai": "assistant",
        "系统": "system",
        "system": "system",
    }

    # 谓词规范化映射 (同义谓词归一)
    PREDICATE_ALIAS_MAP: Dict[str, str] = {
        "偏好": "prefers",
        "喜欢": "prefers",
        "like": "prefers",
        "likes": "prefers",
        "love": "prefers",
        "loves": "prefers",
        "prefer": "prefers",
        "prefers": "prefers",
        "不喜欢": "dislikes",
        "讨厌": "dislikes",
        "dislike": "dislikes",
        "dislikes": "dislikes",
        "hate": "dislikes",
        "绑定端口": "binds_port",
        "bind_port": "binds_port",
        "binds_port": "binds_port",
        "port": "binds_port",
        "状态为": "status_is",
        "status": "status_is",
        "status_is": "status_is",
        "使用": "uses",
        "use": "uses",
        "uses": "uses",
        "采用": "uses",
        "策略为": "policy_is",
        "policy": "policy_is",
        "policy_is": "policy_is",
    }

    def __init__(self, custom_aliases: Optional[Dict[str, str]] = None):
        self._alias_map: Dict[str, str] = dict(self.DEFAULT_ALIAS_MAP)
        if custom_aliases:
            for k, v in custom_aliases.items():
                self._alias_map[k.strip().lower()] = v.strip().lower()

        self._pred_map: Dict[str, str] = dict(self.PREDICATE_ALIAS_MAP)

    def register_alias(self, alias: str, canonical: str) -> None:
        """动态注册别名映射"""
        if alias and canonical:
            self._alias_map[alias.strip().lower()] = canonical.strip().lower()

    def register_predicate_alias(self, alias: str, canonical: str) -> None:
        """动态注册谓词映射"""
        if alias and canonical:
            self._pred_map[alias.strip().lower()] = canonical.strip().lower()

    @staticmethod
    def _clean_text(text: str) -> str:
        """基础清洗：转小写，去除多余空格，将连字符/空格规范化为下划线"""
        cleaned = text.strip().lower()
        # 将连字符与空格转换为下划线
        cleaned = re.sub(r"[\s\-]+", "_", cleaned)
        # 去除首尾非字母数字下划线中文字符
        cleaned = re.sub(r"^[^\w\u4e00-\u9fa5]+|[^\w\u4e00-\u9fa5]+$", "", cleaned)
        return cleaned

    def normalize_entity(self, text: Optional[str]) -> str:
        """
        实体归一化
        1. 检查别名直接匹配（原始 text 与小写 text）；
        2. 若无别名映射，执行连字符、下划线、大小写规整；
        3. 再次检查清洗后别名映射。
        """
        if not text:
            return ""

        raw_key = text.strip().lower()
        if raw_key in self._alias_map:
            return self._alias_map[raw_key]

        cleaned = self._clean_text(text)
        if cleaned in self._alias_map:
            return self._alias_map[cleaned]

        return cleaned

    def normalize_predicate(self, text: Optional[str]) -> str:
        """谓词归一化"""
        if not text:
            return ""

        raw_key = text.strip().lower()
        if raw_key in self._pred_map:
            return self._pred_map[raw_key]

        cleaned = self._clean_text(text)
        if cleaned in self._pred_map:
            return self._pred_map[cleaned]

        return cleaned

    def normalize_triple(
        self,
        subject: str,
        predicate: str,
        object_: Optional[str] = None,
    ) -> Dict[str, Optional[str]]:
        """
        归一化三元组 (subject, predicate, object)
        """
        norm_subj = self.normalize_entity(subject)
        norm_pred = self.normalize_predicate(predicate)
        norm_obj = self.normalize_entity(object_) if object_ else None

        return {
            "subject": norm_subj,
            "predicate": norm_pred,
            "object": norm_obj,
        }
