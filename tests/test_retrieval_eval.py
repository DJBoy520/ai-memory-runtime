"""
tests/test_retrieval_eval.py
检索评估指标与门禁逻辑单元测试 (F0-3)

纯内存自测，不依赖 GPU/Qdrant/SQLite，执行速度极快。
内嵌已知答案的小型黄金集与模拟检索结果，严格校验：
- recall@5, recall@10
- precision@5
- hit_rate@5
- MRR
- G1 leakage rate
- G2 exact-ID accuracy
- G3 contract violations
- no-result 噪声率
- duplicate rate
- 结果归一化（_chunk_N 截断归到 parent）
- 门禁判定与退出码
"""

import math
import pytest
from scripts.eval_retrieval import (
    GoldenQuery,
    SearchItem,
    QuerySearchResult,
    RetrievalBackend,
    calculate_query_metrics,
    aggregate_metrics,
    evaluate_dataset,
    normalize_memory_id,
)


class MockRetrievalBackend(RetrievalBackend):
    """内存模拟检索后端"""

    def __init__(self, mapping: dict[str, list[SearchItem]]):
        self.mapping = mapping

    def search(self, query: GoldenQuery, limit: int = 10) -> QuerySearchResult:
        items = self.mapping.get(query.query_id, [])
        return QuerySearchResult(
            query_id=query.query_id,
            results=items[:limit],
            latency_ms=12.5,
        )


def test_normalize_memory_id():
    """测试 memory_id 归一化（去除 _chunk_N 后缀）"""
    assert normalize_memory_id("mem_20261007_abcdef_chunk_1") == "mem_20261007_abcdef"
    assert normalize_memory_id("mem_20261007_abcdef_chunk_12") == "mem_20261007_abcdef"
    assert normalize_memory_id("mem_20261007_abcdef") == "mem_20261007_abcdef"
    assert normalize_memory_id(None) == ""
    assert normalize_memory_id("") == ""
    # 特殊前缀带 chunk
    assert normalize_memory_id("custom_chunk_0") == "custom"


def test_calculate_query_metrics_semantic():
    """
    测试单条语义 query 的精确指标计算：
    relevant_ids: [A, B]
    检索结果前 5 条: [A_chunk_1, C, D, B, E]
    hits in top-5 = {A, B} (2个)
    recall@5 = 2/2 = 1.0
    precision@5 = 2/5 = 0.4
    hit_rate@5 = 1.0
    MRR = 1/1 = 1.0 (第 1 个就是 A)
    """
    gq = GoldenQuery(
        query_id="q_sem_1",
        query="test semantic",
        category="semantic",
        project_id="aep-pki",
        relevant_ids=["mem_20261007_aaaaaa", "mem_20261007_bbbbbb"],
    )

    search_items = [
        SearchItem(memory_id="mem_20261007_aaaaaa_chunk_1", project_id="aep-pki", status="ACTIVE", score=0.9),
        SearchItem(memory_id="mem_20261007_cccccc", project_id="global", status="ACTIVE", score=0.8),
        SearchItem(memory_id="mem_20261007_dddddd", project_id="general", status="ACTIVE", score=0.7),
        SearchItem(memory_id="mem_20261007_bbbbbb", project_id="aep-pki", status="ACTIVE", score=0.6),
        SearchItem(memory_id="mem_20261007_eeeeee", project_id="aep-pki", status="ACTIVE", score=0.5),
    ]

    res = QuerySearchResult(query_id="q_sem_1", results=search_items, latency_ms=10.0)
    metrics = calculate_query_metrics(gq, res)

    assert metrics["recall_at_5"] == 1.0
    assert metrics["recall_at_10"] == 1.0
    assert metrics["precision_at_5"] == 0.4
    assert metrics["hit_rate_at_5"] == 1.0
    assert metrics["mrr"] == 1.0
    assert metrics["is_leaked"] is False
    assert metrics["contract_violations"] == 0


def test_calculate_query_metrics_mrr_and_recall_rank3():
    """
    测试 relevant 命中在第 3 位的场景：
    relevant_ids: [R1, R2]
    检索结果: [X, Y, R1, Z, W] (第 3 位命中 R1, R2 未命中)
    recall@5 = 1/2 = 0.5
    precision@5 = 1/5 = 0.2
    mrr = 1/3 = 0.33333333...
    """
    gq = GoldenQuery(
        query_id="q_sem_2",
        query="test rank 3",
        category="semantic",
        project_id="aep-tsa",
        relevant_ids=["mem_20261007_r11111", "mem_20261007_r22222"],
    )

    search_items = [
        SearchItem(memory_id="mem_20261007_xxxxxx", project_id="aep-tsa", status="ACTIVE", score=0.9),
        SearchItem(memory_id="mem_20261007_yyyyyy", project_id="aep-tsa", status="ACTIVE", score=0.85),
        SearchItem(memory_id="mem_20261007_r11111_chunk_2", project_id="aep-tsa", status="ACTIVE", score=0.8),
        SearchItem(memory_id="mem_20261007_zzzzzz", project_id="aep-tsa", status="ACTIVE", score=0.7),
        SearchItem(memory_id="mem_20261007_wwwwww", project_id="aep-tsa", status="ACTIVE", score=0.6),
    ]

    res = QuerySearchResult(query_id="q_sem_2", results=search_items, latency_ms=5.0)
    metrics = calculate_query_metrics(gq, res)

    assert metrics["recall_at_5"] == 0.5
    assert metrics["precision_at_5"] == 0.2
    assert metrics["hit_rate_at_5"] == 1.0
    assert math.isclose(metrics["mrr"], 1.0 / 3.0, rel_tol=1e-5)


def test_g1_project_leakage():
    """
    测试 G1 门禁 project leakage：
    query.project_id 为 'aep-pki'，检索结果中包含 'other-project' 即判为泄露。
    允许的集合为 {query.project_id, 'global', 'general'}。
    """
    gq = GoldenQuery(
        query_id="q_leak_1",
        query="query with leak",
        category="semantic",
        project_id="aep-pki",
        relevant_ids=["mem_20261007_aaaaaa"],
    )

    # 包含 other-project
    items_leaked = [
        SearchItem(memory_id="mem_20261007_aaaaaa", project_id="aep-pki", status="ACTIVE"),
        SearchItem(memory_id="mem_20261007_other1", project_id="other-project", status="ACTIVE"),
    ]
    res_leaked = QuerySearchResult(query_id="q_leak_1", results=items_leaked)
    m_leaked = calculate_query_metrics(gq, res_leaked)
    assert m_leaked["is_leaked"] is True

    # 仅包含 aep-pki, global, general (不泄露)
    items_safe = [
        SearchItem(memory_id="mem_20261007_aaaaaa", project_id="aep-pki", status="ACTIVE"),
        SearchItem(memory_id="mem_20261007_glob01", project_id="global", status="ACTIVE"),
        SearchItem(memory_id="mem_20261007_gen001", project_id="general", status="ACTIVE"),
    ]
    res_safe = QuerySearchResult(query_id="q_leak_1", results=items_safe)
    m_safe = calculate_query_metrics(gq, res_safe)
    assert m_safe["is_leaked"] is False

    # 若 query.project_id 为 None，不统计 G1
    gq_none = GoldenQuery(
        query_id="q_leak_none",
        query="query without project",
        category="semantic",
        project_id=None,
        relevant_ids=["mem_20261007_aaaaaa"],
    )
    m_none = calculate_query_metrics(gq_none, res_leaked)
    assert m_none["is_leaked"] is None


def test_g2_exact_id_accuracy():
    """
    测试 G2 门禁 exact-ID accuracy：
    category=exact_id 的 query，relevant_ids[0] 必须出现在 top-1 归一化结果。
    """
    target_id = "mem_20261007_abcdef"
    gq = GoldenQuery(
        query_id="q_exact_1",
        query="mem_20261007_abcdef",
        category="exact_id",
        project_id="global",
        relevant_ids=[target_id],
    )

    # top-1 命中 chunk 后缀，归一化后匹配成功
    items_hit = [
        SearchItem(memory_id=f"{target_id}_chunk_0", project_id="global", status="ACTIVE"),
        SearchItem(memory_id="mem_20261007_other1", project_id="global", status="ACTIVE"),
    ]
    m_hit = calculate_query_metrics(gq, QuerySearchResult("q_exact_1", items_hit))
    assert m_hit["exact_id_hit"] is True

    # top-1 没命中（落到 top-2），则 exact_id_hit 为 False
    items_miss = [
        SearchItem(memory_id="mem_20261007_other1", project_id="global", status="ACTIVE"),
        SearchItem(memory_id=target_id, project_id="global", status="ACTIVE"),
    ]
    m_miss = calculate_query_metrics(gq, QuerySearchResult("q_exact_1", items_miss))
    assert m_miss["exact_id_hit"] is False


def test_g3_contract_violation():
    """
    测试 G3 门禁 contract violation：
    默认检索下结果 status 必须为 ACTIVE，非 ACTIVE 计为违规。
    """
    gq = GoldenQuery(
        query_id="q_contract_1",
        query="test status",
        category="semantic",
        project_id="global",
        relevant_ids=["mem_20261007_aaaaaa"],
    )

    items = [
        SearchItem(memory_id="mem_20261007_aaaaaa", project_id="global", status="ACTIVE"),
        SearchItem(memory_id="mem_20261007_bbbbbb", project_id="global", status="HISTORICAL"),  # 违规 1
        SearchItem(memory_id="mem_20261007_cccccc", project_id="global", status="CONFLICT"),    # 违规 2
    ]
    m = calculate_query_metrics(gq, QuerySearchResult("q_contract_1", items))
    assert m["contract_violations"] == 2


def test_no_result_noise_rate():
    """
    测试 no-result 噪声率：
    category=no_result 且 top-1 score >= noise_threshold (默认 0.40)
    """
    gq = GoldenQuery(
        query_id="q_nores_1",
        query="impossible query",
        category="no_result",
        project_id="global",
        relevant_ids=[],
    )

    # score 0.45 >= 0.40 -> 判定为噪声
    items_noisy = [
        SearchItem(memory_id="mem_20261007_aaaaaa", score=0.45),
    ]
    m_noisy = calculate_query_metrics(gq, QuerySearchResult("q_nores_1", items_noisy), noise_threshold=0.40)
    assert m_noisy["no_result_noise"] is True

    # score 0.35 < 0.40 -> 未超阈值，无噪声
    items_quiet = [
        SearchItem(memory_id="mem_20261007_aaaaaa", score=0.35),
    ]
    m_quiet = calculate_query_metrics(gq, QuerySearchResult("q_nores_1", items_quiet), noise_threshold=0.40)
    assert m_quiet["no_result_noise"] is False

    # 结果为空 -> 无噪声
    m_empty = calculate_query_metrics(gq, QuerySearchResult("q_nores_1", []), noise_threshold=0.40)
    assert m_empty["no_result_noise"] is False


def test_duplicate_rate():
    """
    测试 duplicate rate 计算：
    top-5 内具有相同 parent_memory_id 或完全相同 content 的比例。
    """
    gq = GoldenQuery(
        query_id="q_dup_1",
        query="dup check",
        category="semantic",
        project_id="global",
        relevant_ids=["mem_20261007_aaaaaa"],
    )

    items = [
        # chunk 1 & 2 同属 parent aaaaaa -> 重复 1 条
        SearchItem(memory_id="mem_20261007_aaaaaa_chunk_1", content="content 1"),
        SearchItem(memory_id="mem_20261007_aaaaaa_chunk_2", content="content 2"),
        # 不同 parent 但完全相同 content -> 重复 1 条
        SearchItem(memory_id="mem_20261007_bbbbbb", content="same text"),
        SearchItem(memory_id="mem_20261007_cccccc", content="same text"),
        SearchItem(memory_id="mem_20261007_dddddd", content="unique text"),
    ]
    m = calculate_query_metrics(gq, QuerySearchResult("q_dup_1", items))
    # 5 条中有 2 条重复，比例 = 2 / 5 = 0.4
    assert math.isclose(m["duplicate_rate_5"], 0.4, rel_tol=1e-5)


def test_evaluate_dataset_end_to_end_gates_pass():
    """
    端到端测试 evaluate_dataset：所有 P0 门禁全部通过
    """
    queries = [
        GoldenQuery(
            query_id="q_sem",
            query="find pki cert",
            category="semantic",
            project_id="aep-pki",
            relevant_ids=["mem_20261007_cert01"],
        ),
        GoldenQuery(
            query_id="q_exact",
            query="mem_20261007_cert01",
            category="exact_id",
            project_id=None,
            relevant_ids=["mem_20261007_cert01"],
        ),
        GoldenQuery(
            query_id="q_nores",
            query="non existent item",
            category="no_result",
            project_id="global",
            relevant_ids=[],
        ),
    ]

    backend_data = {
        "q_sem": [
            SearchItem(memory_id="mem_20261007_cert01", project_id="aep-pki", status="ACTIVE", score=0.88),
            SearchItem(memory_id="mem_20261007_rootca", project_id="global", status="ACTIVE", score=0.75),
        ],
        "q_exact": [
            SearchItem(memory_id="mem_20261007_cert01_chunk_0", project_id="aep-pki", status="ACTIVE", score=1.0),
        ],
        "q_nores": [
            SearchItem(memory_id="mem_20261007_dummy", project_id="global", status="ACTIVE", score=0.20),
        ],
    }

    backend = MockRetrievalBackend(backend_data)
    overall, per_q, breakdowns = evaluate_dataset(queries, backend)

    assert overall["gates"]["passed"] is True
    assert overall["g1_leakage_rate"] == 0.0
    assert overall["g2_exact_id_accuracy"] == 1.0
    assert overall["g3_contract_violations"] == 0
    assert overall["recall_at_5"] == 1.0
    assert overall["precision_at_5"] == 0.2  # 1 hit / 5 slots = 0.2
    assert overall["no_result_noise_rate"] == 0.0
    assert len(per_q) == 3
    assert "by_category" in breakdowns
    assert "by_project_id" in breakdowns


def test_evaluate_dataset_gates_fail_triggers():
    """
    端到端测试 evaluate_dataset：检测到 G1 泄漏时 gates.passed 为 False
    """
    queries = [
        GoldenQuery(
            query_id="q_leak",
            query="pki secret",
            category="semantic",
            project_id="aep-pki",
            relevant_ids=["mem_20261007_pki001"],
        )
    ]
    backend_data = {
        "q_leak": [
            SearchItem(memory_id="mem_20261007_pki001", project_id="aep-chain", status="ACTIVE", score=0.9),
        ]
    }
    backend = MockRetrievalBackend(backend_data)
    overall, _, _ = evaluate_dataset(queries, backend)

    assert overall["gates"]["passed"] is False
    assert overall["gates"]["G1_leakage"]["passed"] is False
    assert overall["g1_leakage_rate"] == 1.0
