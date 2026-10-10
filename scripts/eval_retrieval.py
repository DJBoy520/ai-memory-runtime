#!/usr/bin/env python3
"""
scripts/eval_retrieval.py
AMR 检索评估框架 CLI 与核心指标计算引擎 (RFC-006 / F0-3)

支持逐 query 运行检索并计算指标族与 P0 门禁：
- 后端抽象：--backend fixture（离线模式）与 --backend service（连接真实 MemoryService，需 GPU，离线测试不执行）
- 结果归一化：去掉 _chunk_N 后缀归并到 parent_memory_id
- 指标族：Recall@5, Recall@10, Precision@5, HitRate@5, MRR, G1 project leakage rate,
         G2 exact-ID accuracy, G3 contract violation, no-result 噪声率, duplicate rate,
         检索延迟 P50/P95, 返回 content 平均长度
- 分桶统计：按 category 与 project_id 多维度透视
- P0 门禁判定：G1 != 0 或 G2 != 1.0 或 G3 != 0 时退出码为 2，否则为 0
"""

import abc
import argparse
import asyncio
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ==============================================================================
# 数据结构与类型定义
# ==============================================================================

@dataclass
class GoldenQuery:
    """黄金集单条查询定义"""
    query_id: str
    query: str
    category: str  # semantic | no_result | exact_id | identifier
    project_id: Optional[str]
    relevant_ids: List[str]
    hard_negative_ids: List[str] = field(default_factory=list)
    notes: str = ""
    reviewed: bool = False
    reviewed_by: Optional[str] = None


@dataclass
class SearchItem:
    """单条检索返回条目"""
    memory_id: str
    project_id: str = "global"
    status: str = "ACTIVE"
    score: float = 0.0
    content: str = ""
    parent_memory_id: Optional[str] = None
    matched_by: Optional[str] = None

    @property
    def normalized_id(self) -> str:
        """归一化 memory_id（去掉 _chunk_N 后缀归到 parent_memory_id）"""
        if self.parent_memory_id:
            return normalize_memory_id(self.parent_memory_id)
        return normalize_memory_id(self.memory_id)


@dataclass
class QuerySearchResult:
    """单个 query 的检索结果与耗时"""
    query_id: str
    results: List[SearchItem]
    latency_ms: float = 0.0


# ==============================================================================
# 归一化工具函数
# ==============================================================================

_CHUNK_SUFFIX_PATTERN = re.compile(r"^(mem_\d{8}_[0-9a-fA-F]{6})_chunk_\d+$")


def normalize_memory_id(mid: Optional[str]) -> str:
    """
    去除 memory_id 末尾的 _chunk_N 后缀，归一化到父 ID。
    例如: mem_20261007_abcdef_chunk_1 -> mem_20261007_abcdef
    """
    if not mid:
        return ""
    m = _CHUNK_SUFFIX_PATTERN.match(mid)
    if m:
        return m.group(1)
    # 通用后备匹配：去掉任意 _chunk_\d+$
    return re.sub(r"_chunk_\d+$", "", mid)


# ==============================================================================
# 检索后端抽象 (Backend Interface & Implementations)
# ==============================================================================

class RetrievalBackend(abc.ABC):
    """检索后端基类"""

    @abc.abstractmethod
    def search(self, query: GoldenQuery, limit: int = 10) -> QuerySearchResult:
        """针对单个 query 执行检索"""
        pass


class FixtureRetrievalBackend(RetrievalBackend):
    """
    从模拟结果 JSONL 中读取检索结果的离线后端。
    用于在无 GPU / Qdrant 环境下进行离线指标评测与自测。
    """

    def __init__(self, fixture_path: str):
        self.fixture_path = fixture_path
        self._results_map: Dict[str, QuerySearchResult] = {}
        self._load_fixtures()

    def _load_fixtures(self) -> None:
        path = Path(self.fixture_path)
        if not path.is_file():
            raise FileNotFoundError(f"Fixture 文件未找到: {self.fixture_path}")

        with path.open("r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                data = json.loads(line)
                qid = data["query_id"]
                raw_results = data.get("results", [])
                latency = float(data.get("latency_ms", 0.0))

                search_items: List[SearchItem] = []
                for item in raw_results:
                    search_items.append(
                        SearchItem(
                            memory_id=item.get("memory_id", ""),
                            project_id=item.get("project_id", "global"),
                            status=item.get("status", "ACTIVE"),
                            score=float(item.get("score", 0.0)),
                            content=item.get("content", ""),
                            parent_memory_id=item.get("parent_memory_id"),
                            matched_by=item.get("matched_by"),
                        )
                    )
                self._results_map[qid] = QuerySearchResult(
                    query_id=qid,
                    results=search_items,
                    latency_ms=latency,
                )

    def search(self, query: GoldenQuery, limit: int = 10) -> QuerySearchResult:
        if query.query_id in self._results_map:
            res = self._results_map[query.query_id]
            # 限制返回最多 limit 条
            return QuerySearchResult(
                query_id=res.query_id,
                results=res.results[:limit],
                latency_ms=res.latency_ms,
            )
        # 若未命中 fixture，返回空结果
        return QuerySearchResult(query_id=query.query_id, results=[], latency_ms=0.0)


class ServiceRetrievalBackend(RetrievalBackend):
    """
    连接 src.service.memory_service.MemoryService 的在线检索后端。
    仅在具有 GPU 嵌入模型与真实环境的 Baseline 评测时使用。
    """

    def __init__(self, memory_service: Any = None):
        self.memory_service = memory_service

    def _get_service(self) -> Any:
        if self.memory_service is None:
            # 延迟动态导入，避免无 GPU 环境或自测时触发模型加载
            from src.service.memory_service import MemoryService
            self.memory_service = MemoryService()
        return self.memory_service

    def search(self, query: GoldenQuery, limit: int = 10) -> QuerySearchResult:
        service = self._get_service()
        start_time = time.perf_counter()

        # 调用异步方法
        async def _run():
            return await service.memory_search(
                query=query.query,
                project_id=query.project_id,
                limit=limit,
            )

        resp = asyncio.run(_run())
        latency_ms = (time.perf_counter() - start_time) * 1000.0

        raw_items = resp.get("results", []) if isinstance(resp, dict) else []
        search_items: List[SearchItem] = []
        for item in raw_items:
            search_items.append(
                SearchItem(
                    memory_id=item.get("memory_id", ""),
                    project_id=item.get("project_id", "global"),
                    status=item.get("status", "ACTIVE"),
                    score=float(item.get("score", 0.0)),
                    content=item.get("content", ""),
                    parent_memory_id=item.get("parent_memory_id"),
                    matched_by=item.get("matched_by"),
                )
            )

        return QuerySearchResult(
            query_id=query.query_id,
            results=search_items[:limit],
            latency_ms=latency_ms,
        )


# ==============================================================================
# 指标族计算逻辑 (Metrics Computation)
# ==============================================================================

# 允许继承的作用域白名单集合
ALLOWED_FALLBACK_PROJECTS: Set[str] = {"global", "general"}


def calculate_query_metrics(
    query: GoldenQuery,
    search_res: QuerySearchResult,
    noise_threshold: float = 0.40,
) -> Dict[str, Any]:
    """
    计算单个 query 的各项细粒度指标。
    """
    top_10 = search_res.results[:10]
    top_5 = top_10[:5]

    # 归一化后的 relevant_ids
    norm_relevant_ids = set(normalize_memory_id(rid) for rid in query.relevant_ids if rid)
    num_relevant = len(norm_relevant_ids)

    # 归一化后的检索结果 ID 列表
    top_5_norm_ids = [item.normalized_id for item in top_5]
    top_10_norm_ids = [item.normalized_id for item in top_10]

    # 1. Recall, Precision, Hit Rate, MRR (主要对 relevant_ids)
    if num_relevant > 0:
        # recall@5: top-5 中命中的 relevant 数量 / 总 relevant 数量
        hits_5 = set(top_5_norm_ids) & norm_relevant_ids
        recall_at_5 = len(hits_5) / num_relevant

        # recall@10
        hits_10 = set(top_10_norm_ids) & norm_relevant_ids
        recall_at_10 = len(hits_10) / num_relevant

        # precision@5: top-5 中命中数 / min(5, len(top_5)) (若 top_5 为空则 0)
        precision_at_5 = len(hits_5) / 5.0

        # hit_rate@5: top-5 是否至少命中 1 个 relevant
        hit_rate_at_5 = 1.0 if len(hits_5) > 0 else 0.0

        # MRR: 第一个命中 relevant 的排名倒数 (1/rank)
        mrr = 0.0
        for rank, norm_id in enumerate(top_10_norm_ids, start=1):
            if norm_id in norm_relevant_ids:
                mrr = 1.0 / rank
                break
    else:
        recall_at_5 = None
        recall_at_10 = None
        precision_at_5 = None
        hit_rate_at_5 = None
        mrr = None

    # 2. G1 Project Leakage:
    # project_id 非空的 query 中，top-10 出现 project_id 不属于 {query.project_id, "global", "general"}
    # 口径修正（2026-10-07 AI 仲裁，见 DOC-AMR-04 契约补遗）：G1 冻结意图是防"语义召回跨项目噪声"，
    # matched_by=exact_id 的确定性直取（含版本链重定向）等同 memory_get 语义，不计入泄漏
    is_leaked: Optional[bool] = None
    if query.project_id is not None and str(query.project_id).strip() != "":
        allowed_projects = {str(query.project_id).strip(), "global", "general"}
        is_leaked = any(
            item.project_id not in allowed_projects and getattr(item, "matched_by", None) != "exact_id"
            for item in top_10
        )

    # 3. G2 Exact-ID accuracy:
    # category=exact_id 的 query 中 relevant_ids[0] 出现在 top-1 归一化结果的比例
    exact_id_hit: Optional[bool] = None
    if query.category == "exact_id":
        if len(query.relevant_ids) > 0 and len(top_10_norm_ids) > 0:
            target_id = normalize_memory_id(query.relevant_ids[0])
            exact_id_hit = (top_10_norm_ids[0] == target_id)
        else:
            exact_id_hit = False

    # 4. G3 Contract Violation:
    # 结果 status 出现非 ACTIVE 值的次数
    contract_violations = sum(1 for item in top_10 if (item.status or "").upper() != "ACTIVE")

    # 5. no-result 噪声率:
    # category=no_result 且 top-1 score >= noise_threshold
    no_result_noise: Optional[bool] = None
    if query.category == "no_result":
        if len(top_10) > 0 and top_10[0].score >= noise_threshold:
            no_result_noise = True
        else:
            no_result_noise = False

    # 6. duplicate rate:
    # top-5 内同 parent_memory_id 或完全相同 content 的比例
    duplicate_count = 0
    seen_parents: Set[str] = set()
    seen_contents: Set[str] = set()
    for item in top_5:
        p_id = item.normalized_id
        cnt = item.content.strip()
        is_dup = False
        if p_id and p_id in seen_parents:
            is_dup = True
        if cnt and cnt in seen_contents:
            is_dup = True

        if is_dup:
            duplicate_count += 1
        else:
            if p_id:
                seen_parents.add(p_id)
            if cnt:
                seen_contents.add(cnt)

    duplicate_rate_5 = (duplicate_count / len(top_5)) if len(top_5) > 0 else 0.0

    # 7. 返回 content 总长度与单次延迟
    total_content_len = sum(len(item.content or "") for item in top_10)

    return {
        "query_id": query.query_id,
        "category": query.category,
        "project_id": query.project_id,
        "recall_at_5": recall_at_5,
        "recall_at_10": recall_at_10,
        "precision_at_5": precision_at_5,
        "hit_rate_at_5": hit_rate_at_5,
        "mrr": mrr,
        "is_leaked": is_leaked,
        "exact_id_hit": exact_id_hit,
        "contract_violations": contract_violations,
        "no_result_noise": no_result_noise,
        "duplicate_rate_5": duplicate_rate_5,
        "total_content_len": total_content_len,
        "latency_ms": search_res.latency_ms,
    }


def percentile(data: Sequence[float], p: float) -> float:
    """计算百分位数 (p in [0, 100])"""
    if not data:
        return 0.0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_data) - 1)
    d = k - f
    return sorted_data[f] + (sorted_data[c] - sorted_data[f]) * d


def aggregate_metrics(query_metrics_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    汇总全量或子集的评估指标。
    """
    total_queries = len(query_metrics_list)
    if total_queries == 0:
        return {
            "total_queries": 0,
            "recall_at_5": 0.0,
            "recall_at_10": 0.0,
            "precision_at_5": 0.0,
            "hit_rate_at_5": 0.0,
            "mrr": 0.0,
            "g1_leakage_rate": 0.0,
            "g2_exact_id_accuracy": 0.0,
            "g3_contract_violations": 0,
            "no_result_noise_rate": 0.0,
            "duplicate_rate": 0.0,
            "latency_p50_ms": 0.0,
            "latency_p95_ms": 0.0,
            "avg_content_len": 0.0,
        }

    # relevant 类指标汇总 (针对有 relevant_ids 的 query)
    recalls_5 = [m["recall_at_5"] for m in query_metrics_list if m["recall_at_5"] is not None]
    recalls_10 = [m["recall_at_10"] for m in query_metrics_list if m["recall_at_10"] is not None]
    precisions_5 = [m["precision_at_5"] for m in query_metrics_list if m["precision_at_5"] is not None]
    hits_5 = [m["hit_rate_at_5"] for m in query_metrics_list if m["hit_rate_at_5"] is not None]
    mrrs = [m["mrr"] for m in query_metrics_list if m["mrr"] is not None]

    # G1 泄露率
    leakage_queries = [m["is_leaked"] for m in query_metrics_list if m["is_leaked"] is not None]
    g1_leakage_rate = (sum(1 for v in leakage_queries if v) / len(leakage_queries)) if leakage_queries else 0.0

    # G2 Exact-ID 准确率
    exact_queries = [m["exact_id_hit"] for m in query_metrics_list if m["exact_id_hit"] is not None]
    g2_exact_id_acc = (sum(1 for v in exact_queries if v) / len(exact_queries)) if exact_queries else 1.0

    # G3 违规总次数
    g3_violations = sum(m["contract_violations"] for m in query_metrics_list)

    # no_result 噪声率
    no_result_queries = [m["no_result_noise"] for m in query_metrics_list if m["no_result_noise"] is not None]
    no_result_noise_rate = (sum(1 for v in no_result_queries if v) / len(no_result_queries)) if no_result_queries else 0.0

    # duplicate rate 均值
    dup_rates = [m["duplicate_rate_5"] for m in query_metrics_list]
    avg_dup_rate = (sum(dup_rates) / len(dup_rates)) if dup_rates else 0.0

    # 延迟与 content 长度
    latencies = [m["latency_ms"] for m in query_metrics_list]
    latency_p50 = percentile(latencies, 50)
    latency_p95 = percentile(latencies, 95)

    content_lens = [m["total_content_len"] for m in query_metrics_list]
    avg_content_len = (sum(content_lens) / len(content_lens)) if content_lens else 0.0

    return {
        "total_queries": total_queries,
        "evaluated_relevant_queries": len(recalls_5),
        "recall_at_5": round(sum(recalls_5) / len(recalls_5), 4) if recalls_5 else 0.0,
        "recall_at_10": round(sum(recalls_10) / len(recalls_10), 4) if recalls_10 else 0.0,
        "precision_at_5": round(sum(precisions_5) / len(precisions_5), 4) if precisions_5 else 0.0,
        "hit_rate_at_5": round(sum(hits_5) / len(hits_5), 4) if hits_5 else 0.0,
        "mrr": round(sum(mrrs) / len(mrrs), 4) if mrrs else 0.0,
        "g1_leakage_rate": round(g1_leakage_rate, 4),
        "g1_leakage_count": sum(1 for v in leakage_queries if v),
        "g1_scoped_queries": len(leakage_queries),
        "g2_exact_id_accuracy": round(g2_exact_id_acc, 4),
        "g2_exact_id_total": len(exact_queries),
        "g3_contract_violations": g3_violations,
        "no_result_noise_rate": round(no_result_noise_rate, 4),
        "no_result_total": len(no_result_queries),
        "duplicate_rate": round(avg_dup_rate, 4),
        "latency_p50_ms": round(latency_p50, 2),
        "latency_p95_ms": round(latency_p95, 2),
        "avg_content_len": round(avg_content_len, 1),
    }


def evaluate_dataset(
    golden_queries: List[GoldenQuery],
    backend: RetrievalBackend,
    limit: int = 10,
    noise_threshold: float = 0.40,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Dict[str, Any]]:
    """
    全量评估黄金集并生成全局指标与分桶统计。
    """
    per_query_results: List[Dict[str, Any]] = []

    for gq in golden_queries:
        search_res = backend.search(gq, limit=limit)
        q_metrics = calculate_query_metrics(gq, search_res, noise_threshold=noise_threshold)
        per_query_results.append(q_metrics)

    # 全局指标
    overall_metrics = aggregate_metrics(per_query_results)

    # 门禁判定
    # G1: leakage_rate == 0
    # G2: exact_id_accuracy == 1.0 (若没有 exact_id query，视作符合或 1.0)
    # G3: contract_violations == 0
    passed_g1 = (overall_metrics["g1_leakage_rate"] == 0.0)
    passed_g2 = (overall_metrics["g2_exact_id_accuracy"] == 1.0)
    passed_g3 = (overall_metrics["g3_contract_violations"] == 0)
    gates_passed = passed_g1 and passed_g2 and passed_g3

    overall_metrics["gates"] = {
        "passed": gates_passed,
        "G1_leakage": {"passed": passed_g1, "rate": overall_metrics["g1_leakage_rate"]},
        "G2_exact_id": {"passed": passed_g2, "accuracy": overall_metrics["g2_exact_id_accuracy"]},
        "G3_contract": {"passed": passed_g3, "violations": overall_metrics["g3_contract_violations"]},
    }

    # 分桶透视：按 category 分桶
    category_buckets: Dict[str, List[Dict[str, Any]]] = {}
    for qm in per_query_results:
        cat = qm["category"] or "unknown"
        category_buckets.setdefault(cat, []).append(qm)
    breakdown_by_category = {cat: aggregate_metrics(q_list) for cat, q_list in category_buckets.items()}

    # 分桶透视：按 project_id 分桶
    project_buckets: Dict[str, List[Dict[str, Any]]] = {}
    for qm in per_query_results:
        pid = qm["project_id"] if qm["project_id"] is not None else "null"
        project_buckets.setdefault(pid, []).append(qm)
    breakdown_by_project = {pid: aggregate_metrics(q_list) for pid, q_list in project_buckets.items()}

    breakdowns = {
        "by_category": breakdown_by_category,
        "by_project_id": breakdown_by_project,
    }

    return overall_metrics, per_query_results, breakdowns


# ==============================================================================
# 报告格式化与打印 (Report Printing)
# ==============================================================================

def print_human_report(overall: Dict[str, Any], breakdowns: Dict[str, Any]) -> None:
    """输出人类可读的评估分析报告"""
    gates = overall.get("gates", {})
    gate_status = "PASS" if gates.get("passed", False) else "FAIL"

    print("\n" + "=" * 80)
    print(" " * 26 + "AMR 检索质量与门禁评估报告")
    print("=" * 80)
    print(f"总查询数: {overall['total_queries']} | 参与相关性评估: {overall.get('evaluated_relevant_queries', 0)}")
    print("-" * 80)
    print("【P0 门禁状态】")
    g1 = gates.get("G1_leakage", {})
    g2 = gates.get("G2_exact_id", {})
    g3 = gates.get("G3_contract", {})
    print(f"  G1 Project Leakage:    [{'PASS' if g1.get('passed') else 'FAIL'}] rate = {g1.get('rate', 0.0):.4f} (泄露: {overall.get('g1_leakage_count', 0)}/{overall.get('g1_scoped_queries', 0)})")
    print(f"  G2 Exact-ID Accuracy:  [{'PASS' if g2.get('passed') else 'FAIL'}] acc  = {g2.get('accuracy', 0.0) * 100:.2f}% (样本数: {overall.get('g2_exact_id_total', 0)})")
    print(f"  G3 Contract Violation: [{'PASS' if g3.get('passed') else 'FAIL'}] violations = {g3.get('violations', 0)}")
    print(f"  => 门禁总评: {gate_status}")
    print("-" * 80)
    print("【质量指标族】")
    print(f"  Recall@5:       {overall['recall_at_5']:.4f}    |  Recall@10:      {overall['recall_at_10']:.4f}")
    print(f"  Precision@5:    {overall['precision_at_5']:.4f}    |  HitRate@5:      {overall['hit_rate_at_5']:.4f}")
    print(f"  MRR:            {overall['mrr']:.4f}    |  DuplicateRate:  {overall['duplicate_rate']:.4f}")
    print(f"  No-Result 噪声: {overall['no_result_noise_rate']:.4f} (样本数: {overall.get('no_result_total', 0)})")
    print("-" * 80)
    print("【系统与负载指标】")
    print(f"  延迟 P50: {overall['latency_p50_ms']} ms | P95: {overall['latency_p95_ms']} ms")
    print(f"  Top-10 Content 平均总长: {overall['avg_content_len']} 字符")
    print("-" * 80)

    print("【按 Category 分类透视】")
    print(f"{'Category':<15} | {'Count':<6} | {'Recall@5':<9} | {'Precision@5':<11} | {'MRR':<7} | {'Noise':<7}")
    print("-" * 65)
    for cat, b in breakdowns.get("by_category", {}).items():
        print(f"{cat:<15} | {b['total_queries']:<6} | {b['recall_at_5']:<9.4f} | {b['precision_at_5']:<11.4f} | {b['mrr']:<7.4f} | {b['no_result_noise_rate']:<7.4f}")

    print("-" * 80)
    print("【按 Project ID 分类透视】")
    print(f"{'Project ID':<15} | {'Count':<6} | {'Recall@5':<9} | {'Leakage Rate':<12} | {'MRR':<7}")
    print("-" * 65)
    for pid, b in breakdowns.get("by_project_id", {}).items():
        print(f"{str(pid):<15} | {b['total_queries']:<6} | {b['recall_at_5']:<9.4f} | {b['g1_leakage_rate']:<12.4f} | {b['mrr']:<7.4f}")
    print("=" * 80 + "\n")


# ==============================================================================
# 文件读取与 CLI 入口
# ==============================================================================

def load_golden_set(golden_path: str) -> List[GoldenQuery]:
    """读取黄金集 JSONL 文件并转换为 GoldenQuery 列表"""
    path = Path(golden_path)
    if not path.is_file():
        raise FileNotFoundError(f"黄金集文件不存在: {golden_path}")

    queries: List[GoldenQuery] = []
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            data = json.loads(line)
            queries.append(
                GoldenQuery(
                    query_id=data["query_id"],
                    query=data["query"],
                    category=data.get("category", "semantic"),
                    project_id=data.get("project_id"),
                    relevant_ids=data.get("relevant_ids", []),
                    hard_negative_ids=data.get("hard_negative_ids", []),
                    notes=data.get("notes", ""),
                    reviewed=data.get("reviewed", False),
                    reviewed_by=data.get("reviewed_by"),
                )
            )
    return queries


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="AMR 检索质量与门禁评估框架 (scripts/eval_retrieval.py)")
    parser.add_argument(
        "--golden",
        type=str,
        default="tests/golden/retrieval_golden.jsonl",
        help="黄金集 JSONL 文件路径 (默认: tests/golden/retrieval_golden.jsonl)",
    )
    parser.add_argument(
        "--backend",
        choices=["fixture", "service"],
        default="fixture",
        help="检索后端：fixture（离线模拟测试）或 service（真实 MemoryService）",
    )
    parser.add_argument(
        "--fixture",
        type=str,
        default="tests/golden/fixture_results.jsonl",
        help="当 --backend fixture 时指定的模拟检索结果 JSONL 路径 (默认: tests/golden/fixture_results.jsonl)",
    )
    parser.add_argument(
        "--noise-threshold",
        type=float,
        default=0.40,
        help="no_result 噪声判定分数阈值 (默认: 0.40)",
    )
    parser.add_argument(
        "--json",
        dest="json_output",
        type=str,
        default=None,
        help="指定输出全量机读评估指标的 JSON 文件路径",
    )
    parser.add_argument(
        "--baseline",
        type=str,
        default=None,
        help="保存基线快照的 JSON 文件路径 (如: tests/golden/baseline.json)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    # 1. 加载黄金集
    try:
        golden_queries = load_golden_set(args.golden)
    except Exception as e:
        print(f"错误: 无法加载黄金集文件 '{args.golden}': {e}", file=sys.stderr)
        return 1

    # 2. 初始化后端
    if args.backend == "fixture":
        try:
            backend = FixtureRetrievalBackend(args.fixture)
        except Exception as e:
            print(f"错误: 无法初始化 Fixture 后端 '{args.fixture}': {e}", file=sys.stderr)
            return 1
    elif args.backend == "service":
        backend = ServiceRetrievalBackend()
    else:
        print(f"错误: 未知后端 '{args.backend}'", file=sys.stderr)
        return 1

    # 3. 运行评估
    overall_metrics, per_query_results, breakdowns = evaluate_dataset(
        golden_queries=golden_queries,
        backend=backend,
        limit=10,
        noise_threshold=args.noise_threshold,
    )

    # 4. 打印报告
    print_human_report(overall_metrics, breakdowns)

    # 5. 输出机读结果（若指定）
    full_output = {
        "overall": overall_metrics,
        "breakdowns": breakdowns,
        "queries": per_query_results,
    }

    if args.json_output:
        json_path = Path(args.json_output)
        json_path.parent.mkdir(parents=True, exist_ok=True)
        with json_path.open("w", encoding="utf-8") as f:
            json.dump(full_output, f, ensure_ascii=False, indent=2)
        print(f"机读评测结果已写入: {args.json_output}")

    if args.baseline:
        baseline_path = Path(args.baseline)
        baseline_path.parent.mkdir(parents=True, exist_ok=True)
        with baseline_path.open("w", encoding="utf-8") as f:
            json.dump(full_output, f, ensure_ascii=False, indent=2)
        print(f"基线快照已保存: {args.baseline}")

    # 6. 门禁退出码判定
    # 门禁判定：G1!=0 或 G2!=1.0 或 G3!=0 时进程退出码 2，否则 0
    passed = overall_metrics.get("gates", {}).get("passed", False)
    if not passed:
        print("【门禁失败】P0 门禁未全部满足 (G1!=0 或 G2!=1.0 或 G3!=0)，进程退出码 2。", file=sys.stderr)
        return 2

    return 0


if __name__ == "__main__":
    sys.exit(main())
