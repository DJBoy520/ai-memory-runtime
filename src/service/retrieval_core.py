"""
AI Memory Runtime - RetrievalCore 接口与数据契约规范 (冻结稿)

本模块定义统一检索核心的抽象接口与候选数据结构 (Candidate)。
遵循 Phase 0 / F0-4 检索语义契约规范（2026-10-07 冻结）。
纯标准库类型契约定义，不包含任何外部依赖与实现逻辑。
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional

# 检索策略字面量类型别名
StrategyLiteral = Literal["dense", "fulltext", "sparse", "exact_id"]

# 主匹配原因字面量类型别名
MatchedByLiteral = Literal["semantic", "lexical", "exact_id"]


@dataclass(frozen=True)
class Candidate:
    """
    检索候选实体 (Candidate) 契约定义
    统一承载各检索通道产出的召回结果，严格隔离实时排序分与历史元数据。
    """

    memory_id: str
    """记忆唯一标识 ID"""

    project_id: str
    """所属项目标识 ID"""

    status: str
    """记忆生命周期状态 (如 ACTIVE, HISTORICAL 等)"""

    vector_score: float
    """唯一排名分：当前 query 与当前 memory content 的实时 cosine 相似度"""

    stored_score: Optional[float]
    """历史元数据，隔离于排名：payload 中的持久化历史静态分，仅随行透出，绝不参与排名"""

    retrieval_source: List[str]
    """产生候选的通道列表，可多值：dense / fulltext / sparse / exact_id (RRF 融合合并时记录全部来源)"""

    matched_by: str
    """主匹配原因：semantic / lexical / exact_id"""


class RetrievalCore(ABC):
    """
    统一检索核心抽象基类 (RetrievalCore)
    提供多通道召回与候选生成的抽象接口定义。
    """

    @abstractmethod
    def candidates(
        self,
        query: str,
        project_id: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        top_c: int = 20,
        strategies: Optional[List[StrategyLiteral]] = None,
    ) -> List[Candidate]:
        """
        检索候选集生成抽象方法

        :param query: 检索查询文本
        :param project_id: 可选限定项目 ID，若为 None 则为全库检索
        :param filters: 可选过滤条件字典
        :param top_c: 候选集截断数量，默认 20
        :param strategies: 检索策略列表，可选 dense / fulltext / sparse / exact_id
        :return: 候选实体 Candidate 列表
        """
        pass
