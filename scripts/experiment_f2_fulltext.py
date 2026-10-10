#!/usr/bin/env python3
"""
scripts/experiment_f2_fulltext.py
AMR 检索优化 Phase 2 / F2-0a：full-text 通道召回实验

实验设计（本文件为新建的实验脚本，不修改任何既有 src/**、config/** 与评估脚本）：
  1) 幂等为 Qdrant collection=ai_memory / field=content 建立 full-text payload 索引
     （TextIndexParams, tokenizer=MULTILINGUAL, min_token_len=2, max_token_len=20, lowercase=True）
  2) 单进程加载一次 MemoryService（BGEM3Engine 只加载一次，进程内复用）
  3) 对黄金集 530 条逐条计算两条通道：
     - dense 通道：await svc.memory_search(query, limit=20) → 取 memory_id + vector_score（top-20 排名）
     - fulltext 通道：client.scroll(content MatchText, limit=50, with_vectors=True)
       → 用本地余弦（query 嵌入 vs 点位向量）排序
  4) RRF 融合（k=60，score = Σ 1/(k+rank)），取 top-10
  5) 输出同一 run 内自含对照：dense-only top-10 vs fused top-10
     （Recall@5/@10、MRR、identifier 类 Recall@5/MRR、no_result 噪声率）

只读约束：Qdrant 仅执行 create_payload_index（content 字段，实验授权）与只读 scroll/get；
          不 upsert / 不 set_payload / 不 delete 点位；SQLite 完全不打开（注入 no-op SessionStore）。
"""

import argparse
import asyncio
import importlib.util
import json
import logging
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import numpy as np
from qdrant_client import models

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import load_config  # noqa: E402
from src.core.qdrant import QdrantManager  # noqa: E402
from src.service.memory_service import MemoryService  # noqa: E402

# ==============================================================================
# 常量配置
# ==============================================================================

COLLECTION = "ai_memory"
CONTENT_FIELD = "content"

DENSE_LIMIT = 20          # memory_search 内部 clamp 上限即 20
FULLTEXT_LIMIT = 50       # scroll 上限
RRF_K = 60
TOP_K = 10
NOISE_THRESHOLD = 0.40

GOLDEN_PATH = PROJECT_ROOT / "tests/golden/retrieval_golden.jsonl"
OUT_JSON = PROJECT_ROOT / "tests/golden/experiments/f2_fulltext.json"
OUT_DUMP = PROJECT_ROOT / "tests/golden/experiments/f2_dense_dump.jsonl"

logger = logging.getLogger("f2_fulltext")


# ==============================================================================
# 复用评估脚本中的黄金集加载与 ID 归一化（只读导入，保持口径一致）
# ==============================================================================

def _load_eval_module():
    spec = importlib.util.spec_from_file_location(
        "amr_eval_retrieval", str(PROJECT_ROOT / "scripts" / "eval_retrieval.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


_EVAL = _load_eval_module()
GoldenQuery = _EVAL.GoldenQuery
normalize_memory_id = _EVAL.normalize_memory_id
load_golden_set = _EVAL.load_golden_set


class _NoopSessionStore:
    """占位 SessionStore：本实验只调用 memory_search，不需要也不应触碰 SQLite 底账。"""

    def __getattr__(self, item: str):  # pragma: no cover - 防御性
        raise AttributeError(f"_NoopSessionStore does not provide '{item}' (SQLite must stay untouched)")


# ==============================================================================
# 1) full-text payload 索引（幂等）
# ==============================================================================

def ensure_fulltext_index(client: Any) -> Dict[str, Any]:
    """
    幂等创建 content 字段的 full-text 索引。
    qdrant-client 1.19.1 中枚举名为 models.TokenizerType（不存在 models.Tokenizer）。
    """
    result: Dict[str, Any] = {
        "collection": COLLECTION,
        "field": CONTENT_FIELD,
        "api_notes": "qdrant-client 1.19.1: tokenizer enum = models.TokenizerType.MULTILINGUAL",
        "tokenizer": "multilingual",
        "min_token_len": 2,
        "max_token_len": 20,
        "lowercase": True,
        "already_existed": None,
        "created_now": False,
        "error": None,
        "verified_present": None,
    }

    info = client.get_collection(COLLECTION)
    schema = info.payload_schema or {}
    result["already_existed"] = CONTENT_FIELD in schema
    result["payload_schema_before"] = sorted(schema.keys())

    if result["already_existed"]:
        result["verified_present"] = True
        return result

    params = models.TextIndexParams(
        type=models.TextIndexType.TEXT,
        tokenizer=models.TokenizerType.MULTILINGUAL,
        min_token_len=2,
        max_token_len=20,
        lowercase=True,
    )
    try:
        client.create_payload_index(
            collection_name=COLLECTION,
            field_name=CONTENT_FIELD,
            field_schema=params,
            wait=True,
        )
        result["created_now"] = True
    except Exception as exc:  # 索引已存在或并发创建时容错继续
        result["error"] = f"{type(exc).__name__}: {exc}"

    info2 = client.get_collection(COLLECTION)
    result["verified_present"] = CONTENT_FIELD in (info2.payload_schema or {})
    if not result["verified_present"]:
        raise RuntimeError(
            f"full-text index on {COLLECTION}.{CONTENT_FIELD} not present after creation attempt: {result['error']}"
        )
    return result


TOKENIZER_PROBES = [
    "8086", "2026", "20261007", "495f96", "REDACTED", "web-admin",
    "端口", "内网", "运维", "运行", "AEP", "QuantumCryptoVault Kafka",
    "8086 端口", "AEP 端口", "仅限内网访问",
]


def probe_tokenizer(client: Any, probes: Sequence[str] = TOKENIZER_PROBES) -> List[Dict[str, Any]]:
    """只读探测：单个/组合 token 在 content full-text 索引下的命中数（用于解释通道产出）。"""
    out: List[Dict[str, Any]] = []
    for text in probes:
        try:
            recs, _ = client.scroll(
                collection_name=COLLECTION,
                scroll_filter=models.Filter(
                    must=[models.FieldCondition(key=CONTENT_FIELD, match=models.MatchText(text=text))]
                ),
                limit=200,
                with_payload=False,
            )
            out.append({"probe": text, "matches": len(recs)})
        except Exception as exc:
            out.append({"probe": text, "matches": None, "error": f"{type(exc).__name__}: {exc}"})
    return out


def fulltext_yield(per_query: Sequence[Dict[str, Any]], diag_rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """fulltext 通道产出率：按 category 统计 0 候选的 query 占比。"""
    by_cat: Dict[str, Dict[str, int]] = {}
    cat_of = {q["query_id"]: q["category"] for q in per_query}
    for d in diag_rows:
        cat = cat_of.get(d["query_id"], d["category"])
        bucket = by_cat.setdefault(cat, {"n": 0, "zero": 0, "candidates": 0})
        bucket["n"] += 1
        bucket["candidates"] += d["n_fulltext_candidates"]
        if d["n_fulltext_candidates"] == 0:
            bucket["zero"] += 1
    for cat, b in by_cat.items():
        b["zero_rate"] = round(b["zero"] / b["n"], 4) if b["n"] else None
    total_n = sum(b["n"] for b in by_cat.values())
    total_zero = sum(b["zero"] for b in by_cat.values())
    return {
        "by_category": by_cat,
        "overall": {
            "n": total_n,
            "zero_candidate_queries": total_zero,
            "zero_rate": round(total_zero / total_n, 4) if total_n else None,
        },
    }


# ==============================================================================
# 2) 逐条查询：dense / fulltext / RRF
# ==============================================================================

def _dedupe_keep_best_rank(norm_ids: Sequence[str]) -> List[str]:
    """通道内按归一化 memory_id 去重，保留最佳（最前）名次。"""
    seen: Set[str] = set()
    out: List[str] = []
    for mid in norm_ids:
        if not mid or mid in seen:
            continue
        seen.add(mid)
        out.append(mid)
    return out


def rrf_fuse(
    channel_rank_lists: Sequence[Sequence[str]],
    k: int = RRF_K,
    top_k: int = TOP_K,
) -> List[Tuple[str, float, int]]:
    """RRF 融合：score = Σ 1/(k + rank)，返回 [(memory_id, rrf_score, best_rank), ...] top_k"""
    scores: Dict[str, float] = {}
    best_rank: Dict[str, int] = {}
    for ranks in channel_rank_lists:
        for rank, mid in enumerate(ranks, start=1):
            scores[mid] = scores.get(mid, 0.0) + 1.0 / (k + rank)
            if mid not in best_rank or rank < best_rank[mid]:
                best_rank[mid] = rank
    ordered = sorted(scores.items(), key=lambda kv: (-kv[1], best_rank[kv[0]], kv[0]))
    return [(mid, score, best_rank[mid]) for mid, score in ordered[:top_k]]


def rank_metrics(ranked_norm_ids: Sequence[str], relevant: Set[str]) -> Dict[str, Optional[float]]:
    """单条 query 的 Recall@5 / Recall@10 / MRR（在归一化 ID 口径上计算）。"""
    if not relevant:
        return {"recall_at_5": None, "recall_at_10": None, "mrr": None}
    top5 = list(dict.fromkeys(ranked_norm_ids[:5]))
    top10 = list(dict.fromkeys(ranked_norm_ids[:10]))
    hits5 = set(top5) & relevant
    hits10 = set(top10) & relevant
    mrr = 0.0
    for rank, mid in enumerate(ranked_norm_ids[:10], start=1):
        if mid in relevant:
            mrr = 1.0 / rank
            break
    return {
        "recall_at_5": len(hits5) / len(relevant),
        "recall_at_10": len(hits10) / len(relevant),
        "mrr": mrr,
    }


def _mean(values: Sequence[float]) -> Optional[float]:
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    return round(sum(vals) / len(vals), 4)


def aggregate(per_query: Sequence[Dict[str, Any]], channel: str) -> Dict[str, Any]:
    """聚合某个通道的指标（自动跳过无 relevant_ids 的 query）。"""
    r5 = [q[channel]["recall_at_5"] for q in per_query]
    r10 = [q[channel]["recall_at_10"] for q in per_query]
    mrr = [q[channel]["mrr"] for q in per_query]
    return {
        "n_queries": len(per_query),
        "n_scored": len([v for v in r5 if v is not None]),
        "recall_at_5": _mean(r5),
        "recall_at_10": _mean(r10),
        "mrr": _mean(mrr),
    }


def aggregate_noise(per_query: Sequence[Dict[str, Any]], channel: str) -> Optional[float]:
    """no_result 类噪声率：top-1 分数 >= 0.40 记噪。仅对 score 语义可比的通道有效。"""
    flags = [
        q[f"{channel}_noise"]
        for q in per_query
        if q["category"] == "no_result" and q[f"{channel}_noise"] is not None
    ]
    if not flags:
        return None
    return round(sum(1 for f in flags if f) / len(flags), 4)


async def run_experiment(
    svc: MemoryService,
    client: Any,
    golden: Sequence[Any],
    dump_fh,
    max_queries: int = 0,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """逐条执行 dense / fulltext / RRF 融合，返回 (per_query_records, diagnostics_rows)。"""
    per_query: List[Dict[str, Any]] = []
    diag_rows: List[Dict[str, Any]] = []

    queries = list(golden)
    if max_queries and max_queries > 0:
        queries = queries[:max_queries]

    t_start = time.perf_counter()
    for idx, gq in enumerate(queries, start=1):
        relevant = {normalize_memory_id(rid) for rid in (gq.relevant_ids or []) if rid}

        # --- 三条并发调用：query 嵌入（本地用）/ dense（实验口径）/ dense（project 过滤，辅助对照） ---
        qvec_task = asyncio.ensure_future(svc.engine.embed([gq.query]))
        dense_task = asyncio.ensure_future(svc.memory_search(gq.query, limit=DENSE_LIMIT))
        if gq.project_id:
            dense_pf_task = asyncio.ensure_future(
                svc.memory_search(gq.query, project_id=gq.project_id, limit=DENSE_LIMIT)
            )
        else:
            dense_pf_task = None

        t_dense_start = time.perf_counter()
        qvec, dense_resp = await asyncio.gather(qvec_task, dense_task)
        t_dense = (time.perf_counter() - t_dense_start) * 1000.0
        qvec_arr = np.asarray(qvec[0], dtype=np.float32)

        dense_pf_resp = None
        if dense_pf_task is not None:
            dense_pf_resp = await dense_pf_task

        dense_items = dense_resp.get("results", []) if isinstance(dense_resp, dict) else []
        pf_items = (
            dense_pf_resp.get("results", []) if isinstance(dense_pf_resp, dict) else []
        )

        def _to_rows(items: Sequence[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[str]]:
            rows: List[Dict[str, Any]] = []
            for rank, it in enumerate(items, start=1):
                mid = it.get("memory_id") or ""
                nid = normalize_memory_id(mid)
                rows.append(
                    {
                        "memory_id": mid,
                        "normalized_id": nid,
                        "score": float(it.get("vector_score", it.get("score", 0.0)) or 0.0),
                        "rank": rank,
                        "relevant": nid in relevant,
                        "project_id": it.get("project_id"),
                    }
                )
            return rows, _dedupe_keep_best_rank([r["normalized_id"] for r in rows])

        dense_rows, dense_ranked = _to_rows(dense_items)
        pf_rows, pf_ranked = _to_rows(pf_items)

        # --- fulltext 通道：scroll(content MatchText) + 本地余弦排序 ---
        t_ft_start = time.perf_counter()
        records, _offset = await asyncio.to_thread(
            client.scroll,
            collection_name=COLLECTION,
            scroll_filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key=CONTENT_FIELD,
                        match=models.MatchText(text=gq.query),
                    )
                ]
            ),
            limit=FULLTEXT_LIMIT,
            with_payload=True,
            with_vectors=True,
        )
        t_ft = (time.perf_counter() - t_ft_start) * 1000.0

        ft_scored: List[Tuple[float, str, Optional[str]]] = []
        qnorm = float(np.linalg.norm(qvec_arr)) or 1.0
        for rec in records:
            payload = rec.payload or {}
            mid = payload.get("memory_id") or ""
            vec = rec.vector
            if vec is None or not mid:
                continue
            v = np.asarray(vec, dtype=np.float32)
            denom = (float(np.linalg.norm(v)) or 1.0) * qnorm
            cos = float(np.dot(qvec_arr, v) / denom)
            ft_scored.append((cos, mid, payload.get("project_id")))
        ft_scored.sort(key=lambda t: (-t[0], t[1]))
        ft_rows = [
            {
                "memory_id": mid,
                "normalized_id": normalize_memory_id(mid),
                "cosine": round(cos, 6),
                "rank": rank,
                "relevant": normalize_memory_id(mid) in relevant,
                "project_id": pid,
            }
            for rank, (cos, mid, pid) in enumerate(ft_scored, start=1)
        ]
        ft_ranked = _dedupe_keep_best_rank([r["normalized_id"] for r in ft_rows])

        # --- RRF 融合（dense top-20 排名 + fulltext 排名） ---
        fused = rrf_fuse([dense_ranked, ft_ranked], k=RRF_K, top_k=TOP_K)
        fused_ranked = [mid for mid, _s, _r in fused]

        dense_metrics = rank_metrics(dense_ranked, relevant)
        fused_metrics = rank_metrics(fused_ranked, relevant)
        pf_metrics = rank_metrics(pf_ranked, relevant)

        dense_noise = None
        if gq.category == "no_result":
            dense_noise = bool(dense_rows) and dense_rows[0]["score"] >= NOISE_THRESHOLD

        fused_in_dense = len(set(fused_ranked) & set(dense_ranked[:TOP_K]))
        pf_in_dense = len(set(pf_ranked[:TOP_K]) & set(dense_ranked[:TOP_K]))

        rec: Dict[str, Any] = {
            "query_id": gq.query_id,
            "category": gq.category,
            "project_id": gq.project_id,
            "relevant_ids": sorted(relevant),
            "dense": dense_metrics,
            "fused": fused_metrics,
            "dense_project_filtered": pf_metrics,
            "dense_noise": dense_noise,
            "fused_noise": None,  # N/A：RRF 分数量纲与向量余弦不可比
        }
        per_query.append(rec)

        diag = {
            "query_id": gq.query_id,
            "category": gq.category,
            "n_dense": len(dense_rows),
            "n_fulltext_candidates": len(ft_rows),
            "fulltext_cap_hit": len(ft_rows) >= FULLTEXT_LIMIT,
            "n_fused_top10_from_dense_top10": fused_in_dense,
            "n_pf_top10_from_dense_top10": pf_in_dense,
            "latency_dense_ms": round(t_dense, 1),
            "latency_fulltext_ms": round(t_ft, 1),
        }
        diag_rows.append(diag)

        # --- dense dump 行（后续阈值分析依赖，逐条包含相关性标注） ---
        dump_fh.write(
            json.dumps(
                {
                    "query_id": gq.query_id,
                    "category": gq.category,
                    "top": [
                        {
                            "memory_id": r["memory_id"],
                            "normalized_id": r["normalized_id"],
                            "score": r["score"],
                            "relevant": r["relevant"],
                        }
                        for r in dense_rows[:TOP_K]
                    ],
                },
                ensure_ascii=False,
            )
            + "\n"
        )
        if idx % 25 == 0:
            dump_fh.flush()

        if idx % 50 == 0 or idx == len(queries):
            elapsed = time.perf_counter() - t_start
            rate = idx / elapsed if elapsed > 0 else 0.0
            print(
                f"  [{idx}/{len(queries)}] 已完成 ({(idx / len(queries)) * 100:.1f}%) "
                f"耗时 {elapsed:.0f}s ({rate:.1f} q/s)",
                flush=True,
            )

    return per_query, diag_rows


# ==============================================================================
# 报告输出
# ==============================================================================

def _pct(v: Optional[float]) -> str:
    return "N/A" if v is None else f"{v * 100:.2f}%"


def build_summary(
    per_query: Sequence[Dict[str, Any]],
    diag_rows: Sequence[Dict[str, Any]],
    golden: Sequence[Any],
    index_info: Dict[str, Any],
    elapsed_sec: float,
    tokenizer_probes: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    categories = ["semantic", "exact_id", "identifier", "no_result"]

    summary: Dict[str, Any] = {
        "experiment": "F2-0a full-text 通道召回（dense vs dense+fulltext RRF 融合）",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "golden_set": {
            "path": str(GOLDEN_PATH),
            "total": len(golden),
            "by_category": {c: sum(1 for g in golden if g.category == c) for c in categories},
        },
        "config": {
            "collection": COLLECTION,
            "dense_call": "await svc.memory_search(query, limit=20)  # 无 project 过滤，实验口径",
            "fulltext_call": "client.scroll(Filter(must=[FieldCondition(content, MatchText(query))]), limit=50, with_vectors=True) + 本地余弦排序",
            "rrf_k": RRF_K,
            "top_k": TOP_K,
            "noise_threshold": NOISE_THRESHOLD,
            "fulltext_index": index_info,
        },
        "dense_only": {},
        "fused_rrf": {},
        "dense_project_filtered_aux": {},
        "deltas_fused_minus_dense": {},
        "fulltext_channel_yield": fulltext_yield(per_query, diag_rows),
        "tokenizer_probes": tokenizer_probes or [],
        "conclusion": None,
        "diagnostics": {},
    }

    for name, key in (("dense_only", "dense"), ("fused_rrf", "fused"), ("dense_project_filtered_aux", "dense_project_filtered")):
        summary[name]["overall"] = aggregate(per_query, key)
        summary[name]["overall"]["no_result_noise_rate"] = (
            aggregate_noise(per_query, "dense") if key == "dense" else None
        )
        summary[name]["overall"]["no_result_noise_note"] = (
            "N/A (RRF 分数量纲与向量余弦不可比，噪声率对比只对 dense 通道有效)"
            if key != "dense"
            else None
        )
        summary[name]["by_category"] = {}
        for cat in categories:
            subset = [q for q in per_query if q["category"] == cat]
            bucket = aggregate(subset, key)
            if cat == "no_result":
                bucket["no_result_noise_rate"] = (
                    aggregate_noise(subset, "dense") if key == "dense" else None
                )
            summary[name]["by_category"][cat] = bucket

    # 全量分桶：semantic / exact_id / identifier 的 x@5 与 MRR 差值
    for cat in ["semantic", "exact_id", "identifier"]:
        d = summary["dense_only"]["by_category"][cat]
        f = summary["fused_rrf"]["by_category"][cat]
        summary["deltas_fused_minus_dense"][cat] = {
            "recall_at_5": round((f["recall_at_5"] or 0) - (d["recall_at_5"] or 0), 4),
            "recall_at_10": round((f["recall_at_10"] or 0) - (d["recall_at_10"] or 0), 4),
            "mrr": round((f["mrr"] or 0) - (d["mrr"] or 0), 4),
        }
    d_all = summary["dense_only"]["overall"]
    f_all = summary["fused_rrf"]["overall"]
    summary["deltas_fused_minus_dense"]["overall"] = {
        "recall_at_5": round((f_all["recall_at_5"] or 0) - (d_all["recall_at_5"] or 0), 4),
        "recall_at_10": round((f_all["recall_at_10"] or 0) - (d_all["recall_at_10"] or 0), 4),
        "mrr": round((f_all["mrr"] or 0) - (d_all["mrr"] or 0), 4),
    }

    ft_counts = [d["n_fulltext_candidates"] for d in diag_rows]
    summary["diagnostics"] = {
        "elapsed_sec": round(elapsed_sec, 1),
        "fulltext_candidates_avg": round(statistics.fmean(ft_counts), 2) if ft_counts else 0.0,
        "fulltext_candidates_max": max(ft_counts) if ft_counts else 0,
        "fulltext_cap_hit_queries": sum(1 for d in diag_rows if d["fulltext_cap_hit"]),
        "fulltext_zero_candidate_queries": sum(1 for d in diag_rows if d["n_fulltext_candidates"] == 0),
        "dense_empty_queries": sum(1 for d in diag_rows if d["n_dense"] == 0),
        "avg_fused_top10_items_already_in_dense_top10": round(
            statistics.fmean([d["n_fused_top10_from_dense_top10"] for d in diag_rows]), 2
        )
        if diag_rows
        else 0.0,
        "latency_dense_avg_ms": round(statistics.fmean([d["latency_dense_ms"] for d in diag_rows]), 1)
        if diag_rows
        else 0.0,
        "latency_fulltext_avg_ms": round(statistics.fmean([d["latency_fulltext_ms"] for d in diag_rows]), 1)
        if diag_rows
        else 0.0,
    }
    return summary


def print_report(summary: Dict[str, Any]) -> Tuple[bool, str]:
    d = summary["dense_only"]
    f = summary["fused_rrf"]
    pf = summary["dense_project_filtered_aux"]
    idx = summary["config"]["fulltext_index"]

    print("\n" + "=" * 96)
    print(" " * 30 + "F2-0a full-text 通道召回实验")
    print("=" * 96)
    print(
        f"黄金集: {summary['golden_set']['path']} | 总数: {summary['golden_set']['total']} "
        f"| 分桶: {summary['golden_set']['by_category']}"
    )
    print(
        f"content full-text 索引: existed={idx['already_existed']} created_now={idx['created_now']} "
        f"verified={idx['verified_present']} error={idx['error']}"
    )
    print(f"RRF k={summary['config']['rrf_k']} | top_k={summary['config']['top_k']} | 噪声阈值={summary['config']['noise_threshold']}")
    print(f"耗时: {summary['diagnostics']['elapsed_sec']}s")
    print("-" * 96)

    def row(label: str, dd: Optional[float], ff: Optional[float]) -> str:
        delta = ""
        if dd is not None and ff is not None:
            delta = f"{(ff - dd) * 100:+.2f} pp"
        return f"{label:<38} | {_pct(dd):>12} | {_pct(ff):>12} | {delta:>10}"

    print(f"{'指标':<38} | {'dense-only':>12} | {'fused(RRF)':>12} | {'Δ':>10}")
    print("-" * 96)
    print(row("Overall Recall@5", d["overall"]["recall_at_5"], f["overall"]["recall_at_5"]))
    print(row("Overall Recall@10", d["overall"]["recall_at_10"], f["overall"]["recall_at_10"]))
    print(row("Overall MRR", d["overall"]["mrr"], f["overall"]["mrr"]))
    print("-" * 96)
    for cat in ["semantic", "exact_id", "identifier"]:
        bd, bf = d["by_category"][cat], f["by_category"][cat]
        print(f"[{cat}] n={bd['n_queries']}")
        print(row(f"  {cat} Recall@5", bd["recall_at_5"], bf["recall_at_5"]))
        print(row(f"  {cat} Recall@10", bd["recall_at_10"], bf["recall_at_10"]))
        print(row(f"  {cat} MRR", bd["mrr"], bf["mrr"]))
        print("-" * 96)
    dn = d["by_category"]["no_result"]["no_result_noise_rate"]
    print(f"{'no_result 噪声率 (top-1 >= 0.40)':<38} | {_pct(dn):>12} | {'N/A':>12} | {'':>10}")
    print(f"  (融合通道噪声率 N/A：RRF 归一分数与向量余弦量纲不可比)")
    print("-" * 96)
    print("辅助对照（非实验口径，用于与既有 project 过滤基线核对）:")
    print(f"  dense + project_id 过滤: overall Recall@5={_pct(pf['overall']['recall_at_5'])} "
          f"Recall@10={_pct(pf['overall']['recall_at_10'])} MRR={_pct(pf['overall']['mrr'])} | "
          f"semantic R@5={_pct(pf['by_category']['semantic']['recall_at_5'])} "
          f"identifier R@5={_pct(pf['by_category']['identifier']['recall_at_5'])} "
          f"exact_id R@5={_pct(pf['by_category']['exact_id']['recall_at_5'])}")
    dg = summary["diagnostics"]
    print("-" * 96)
    print("诊断:")
    print(f"  fulltext 候选数 均值={dg['fulltext_candidates_avg']} 最大={dg['fulltext_candidates_max']} "
          f"| 触顶(={FULLTEXT_LIMIT}) queries={dg['fulltext_cap_hit_queries']} "
          f"| 零候选 queries={dg['fulltext_zero_candidate_queries']}")
    print(f"  融合 top-10 中已在 dense top-10 内 平均={dg['avg_fused_top10_items_already_in_dense_top10']}/10")
    print(f"  平均延迟: dense={dg['latency_dense_avg_ms']}ms fulltext={dg['latency_fulltext_avg_ms']}ms")
    y = summary.get("fulltext_channel_yield", {})
    print("-" * 96)
    print("fulltext 通道产出率（0 候选 = 该 query 融合退化为 dense-only）:")
    for cat, b in (y.get("by_category") or {}).items():
        print(f"  {cat:<11} n={b['n']:<4} 0候选={b['zero']:<4} 零候选率={_pct(b['zero_rate'])} 平均候选={b['candidates'] / b['n']:.1f}")
    if y.get("overall"):
        print(f"  总体: 0 候选 {y['overall']['zero_candidate_queries']}/{y['overall']['n']} = {_pct(y['overall']['zero_rate'])}")
    if summary.get("tokenizer_probes"):
        print("  分词器探测（MatchText 命中数，limit=200）: "
              + ", ".join(f"{p['probe']!r}->{p['matches']}" for p in summary["tokenizer_probes"][:8]))

    # 裁决：identifier / semantic 的 Recall@5 或 MRR 是否有 >= 2pp 提升
    accept, verdict = decide_verdict(summary)
    gains = _gain_rows(summary)
    print("=" * 96)
    print("裁决依据（identifier / semantic 的 Recall@5 与 MRR 提升）:")
    for cat, metric, delta in gains:
        flag = ">= +2pp" if delta >= 0.02 else ("<-2pp" if delta <= -0.02 else "")
        print(f"  {cat:<11} {metric:<9} Δ={delta * 100:+.2f} pp  {flag}")
    print(f"裁决: {verdict}")
    print("=" * 96 + "\n")
    return accept, verdict


def _gain_rows(summary: Dict[str, Any]) -> List[Tuple[str, str, float]]:
    gains: List[Tuple[str, str, float]] = []
    for cat in ["identifier", "semantic"]:
        delta = summary["deltas_fused_minus_dense"][cat]
        gains.append((cat, "Recall@5", delta["recall_at_5"]))
        gains.append((cat, "MRR", delta["mrr"]))
    return gains


def decide_verdict(summary: Dict[str, Any]) -> Tuple[bool, str]:
    """裁决：融合对 identifier/semantic 的 Recall@5/MRR 是否有 >= 2pp 提升。"""
    gains = _gain_rows(summary)
    qualified = [g for g in gains if g[2] >= 0.02]
    worse = [g for g in gains if g[2] < 0]
    accept = len(qualified) > 0 and (not worse or len(qualified) > len(worse))
    yield_info = summary.get("fulltext_channel_yield", {}).get("overall", {})
    zero_rate = yield_info.get("zero_rate")
    verdict = (
        "采纳：融合在 identifier/semantic 上存在 >=2pp 的提升"
        if accept and qualified
        else "放弃：融合未能在 identifier/semantic 上取得 >=2pp 的 Recall@5/MRR 提升"
    )
    if qualified:
        verdict += "（达标项: " + ", ".join(f"{c}/{m} {v * 100:+.2f}pp" for c, m, v in qualified) + "）"
    if worse:
        verdict += "（回退项: " + ", ".join(f"{c}/{m} {v * 100:+.2f}pp" for c, m, v in worse) + "）"
    if zero_rate is not None:
        verdict += f"；fulltext 通道 {zero_rate * 100:.1f}% 的 query 零候选，融合在多数 query 上退化为 dense-only"
    return accept, verdict


# ==============================================================================
# 主入口
# ==============================================================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="F2-0a full-text 通道召回实验")
    p.add_argument("--golden", type=str, default=str(GOLDEN_PATH))
    p.add_argument("--out-json", type=str, default=str(OUT_JSON))
    p.add_argument("--out-dump", type=str, default=str(OUT_DUMP))
    p.add_argument("--smoke-queries", type=int, default=5, help="同一进程内先跑 N 条 sanity（默认 5，0 关闭）")
    p.add_argument("--max-queries", type=int, default=0, help="最多评测条数（0=全部 530）")
    return p.parse_args()


async def async_main(args: argparse.Namespace) -> int:
    golden = load_golden_set(args.golden)
    print(f"黄金集加载: {len(golden)} 条", flush=True)

    # --- Qdrant 客户端（不自动初始化集合，避免任何集合级写操作） ---
    cfg = load_config()
    qm = QdrantManager(config=cfg.qdrant, auto_init_collections=False)
    client = qm.client

    index_info = ensure_fulltext_index(client)
    print(f"full-text 索引: {json.dumps(index_info, ensure_ascii=False)}", flush=True)

    probes = probe_tokenizer(client)
    print(
        "分词器探测: " + ", ".join(f"{p['probe']!r}->{p['matches']}" for p in probes),
        flush=True,
    )

    # --- 单次加载 MemoryService（BGEM3Engine 进程内复用；注入 no-op SessionStore 保 SQLite 只读） ---
    t_load = time.perf_counter()
    svc = MemoryService(
        qdrant_manager=qm,
        session_store=_NoopSessionStore(),
        config=cfg,
    )
    _ = await svc.engine.embed(["AMR F2-0a warmup"])
    print(f"MemoryService/BGEM3Engine 就绪 ({(time.perf_counter() - t_load):.1f}s, engine.state={svc.engine.state})", flush=True)

    out_json = Path(args.out_json)
    out_dump = Path(args.out_dump)
    out_json.parent.mkdir(parents=True, exist_ok=True)

    # --- smoke：同一进程内先验证 5 条，不额外加载模型 ---
    if args.smoke_queries and args.smoke_queries > 0:
        smoke_sink = _NullSink()
        per_q, diags = await run_experiment(svc, client, golden, smoke_sink, max_queries=args.smoke_queries)
        idx0 = per_q[0]
        print(
            f"  smoke: {idx0['query_id']} dense_top10_hit={idx0['dense']['recall_at_5']} "
            f"fused_top10_hit={idx0['fused']['recall_at_5']} "
            f"fulltext_candidates={diags[0]['n_fulltext_candidates']} "
            f"cap_hit={diags[0]['fulltext_cap_hit']}",
            flush=True,
        )
        if any(d["n_dense"] == 0 for d in diags):
            raise RuntimeError("smoke 自检失败：dense 通道返回空结果，中止本次运行")
        ft_total = sum(d["n_fulltext_candidates"] for d in diags)
        print(
            f"  smoke 自检通过: dense 均非空, fulltext 候选合计={ft_total}"
            f"（MatchText 为全体 token AND 语义，长中文 query 常为 0，属预期观察项）",
            flush=True,
        )

    # --- 全量真实运行 + 落盘 ---
    t_start = time.perf_counter()
    with out_dump.open("w", encoding="utf-8") as dump_fh:
        per_query, diag_rows = await run_experiment(
            svc, client, golden, dump_fh, max_queries=args.max_queries
        )
    elapsed = time.perf_counter() - t_start

    summary = build_summary(per_query, diag_rows, golden, index_info, elapsed, tokenizer_probes=probes)
    accept, verdict = decide_verdict(summary)
    summary["conclusion"] = {
        "accepted": accept,
        "verdict": verdict,
        "criteria": "identifier / semantic 的 Recall@5 或 MRR 提升 >= 2pp",
    }
    with out_json.open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    dump_lines = sum(1 for _ in out_dump.open("r", encoding="utf-8"))
    print(f"\n落盘: {out_json}\n落盘: {out_dump} ({dump_lines} 行)")

    print_report(summary)
    print(f"verdict: {verdict}")
    return 0


class _NullSink:
    """smoke 阶段的空 dump sink"""

    def write(self, _s: str) -> int:
        return 0

    def flush(self) -> None:
        return None


def main() -> int:
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    args = parse_args()
    return asyncio.run(async_main(args))


if __name__ == "__main__":
    sys.exit(main())
