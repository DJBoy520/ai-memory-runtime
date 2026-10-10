#!/usr/bin/env python3
"""
F2-2a: dense-only 检索默认分数阈值 ROC 校准（纯离线分析）。

输入:
  - tests/golden/experiments/f2_dense_dump.jsonl
      每行 {query_id, category, top:[{memory_id, normalized_id, score, relevant}, ...]}
      （dense 通道 top-10，分数为 Qdrant 余弦相似度，降序）
  - tests/golden/retrieval_golden.jsonl
      每行 {query_id, query, category, project_id, relevant_ids, hard_negative_ids, ...}
      判定基准：category != no_result 的 query 用 relevant_ids；no_result 的 relevant_ids 为空。

模拟: 保留 score >= T 的结果（过滤掉 score < T），在过滤后的列表上重算:
  - no_result 噪声率: no_result 类查询「top-1 存活即噪」= 过滤后至少剩 1 条结果的
    比例（本 dump 分数严格降序，存活结果必为前缀，故等价于「首条存活」）。
  - 整体 Recall@5 / MRR: 仅对 relevant_ids 非空的 490 条计算（与
    scripts/eval_retrieval.py:269-274、scripts/experiment_f2_fulltext.py:234-251 口径一致），
    分母包含 semantic/exact_id/identifier 三类。
  - Recall@5 损失(百分点) = (Recall@5(T=0) - Recall@5(T)) * 100。

排序口径: 与项目既有口径一致（scripts/experiment_f2_fulltext.py:205
_dedupe_keep_best_rank / :238 rank_metrics），先按 normalized_id 去重（保留最佳名次）
再做 top-5 截断；同时输出不去重变体作为稳健性对照（写入 JSON 的 variants 字段）。

阈值范围: T ∈ [0.30, 0.70] 步长 0.02（21 个点），T=0 为「不过滤」参照行。

推荐规则:
  1) 可行集 = {T : Recall@5 损失 <= 2 个百分点}
  2) 可行集非空 -> 取噪声率最低者；并列取更小的 T（过滤更保守，损失更小）。
  3) 可行集为空 -> 取 (噪声率最低, 损失最小, T 最小) 并如实说明无解。
  4) 若 no_result 类 top-1 分数全部 >= 0.70，则报告「该分数段无法用阈值区分」。

本脚本只读 JSON/JSONL，不加载 GPU 模型、不访问 Qdrant 与 SQLite。
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, OrderedDict
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DUMP = PROJECT_ROOT / "tests/golden/experiments/f2_dense_dump.jsonl"
DEFAULT_GOLDEN = PROJECT_ROOT / "tests/golden/retrieval_golden.jsonl"
DEFAULT_OUT = PROJECT_ROOT / "tests/golden/experiments/f2_threshold.json"

T_MIN = 0.30
T_MAX = 0.70
T_STEP = 0.02
LOSS_BUDGET_PP = 2.0  # 百分点
NOISE_UNSEPARABLE_AT = 0.70  # no_result top-1 全 >= 该值即无法在扫描区间内区分
EPS = 1e-9

_CHUNK_SUFFIX_PATTERN = re.compile(r"^(mem_\d{8}_[0-9a-fA-F]{6})_chunk_\d+$")


def normalize_memory_id(mid: Optional[str]) -> str:
    """与 scripts/eval_retrieval.py:84 同口径：去掉 _chunk_N 后缀归到父 ID。"""
    if not mid:
        return ""
    m = _CHUNK_SUFFIX_PATTERN.match(mid)
    if m:
        return m.group(1)
    return re.sub(r"_chunk_\d+$", "", mid)


def threshold_grid() -> List[float]:
    n = int(round((T_MAX - T_MIN) / T_STEP)) + 1
    return [round(T_MIN + i * T_STEP, 2) for i in range(n)]


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:  # pragma: no cover - 数据损坏时如实报错
                raise SystemExit(f"[fatal] {path}:{line_no} JSON 解析失败: {exc}") from exc
    return rows


def surviving_ids(row: Dict[str, Any], t: float, dedupe: bool) -> List[str]:
    """过滤 score < T 后（可选按 normalized_id 去重）的排名列表。"""
    out: List[str] = []
    seen: Set[str] = set()
    for item in row.get("top", []):
        score = float(item.get("score", 0.0) or 0.0)
        if t > 0 and score < t - EPS:
            continue
        nid = item.get("normalized_id") or normalize_memory_id(item.get("memory_id"))
        if not nid:
            continue
        if dedupe:
            if nid in seen:
                continue
            seen.add(nid)
        out.append(nid)
    return out


def evaluate(
    rows: Sequence[Dict[str, Any]],
    golden: Dict[str, Dict[str, Any]],
    t: float,
    dedupe: bool = True,
) -> Dict[str, Any]:
    """在阈值 T 下重算整体与分 category 指标。"""
    r5_hits: List[float] = []
    mrrs: List[float] = []
    cat_r5: Dict[str, List[float]] = {}
    cat_mrr: Dict[str, List[float]] = {}
    noise_flags: List[int] = []

    for row in rows:
        qid = row.get("query_id", "")
        gq = golden.get(qid, {})
        rel = {normalize_memory_id(rid) for rid in (gq.get("relevant_ids") or []) if rid}
        ids = surviving_ids(row, t, dedupe)
        cat = row.get("category") or gq.get("category") or "unknown"

        if rel:
            hit5 = 1.0 if set(ids[:5]) & rel else 0.0
            mrr = 0.0
            for rank, nid in enumerate(ids, start=1):
                if nid in rel:
                    mrr = 1.0 / rank
                    break
            r5_hits.append(hit5)
            mrrs.append(mrr)
            cat_r5.setdefault(cat, []).append(hit5)
            cat_mrr.setdefault(cat, []).append(mrr)

        if cat == "no_result":
            noise_flags.append(1 if ids else 0)

    n_scored = len(r5_hits)
    out: Dict[str, Any] = {
        "threshold": round(t, 4),
        "n_scored": n_scored,
        "recall5": round(sum(r5_hits) / n_scored, 6) if n_scored else None,
        "mrr": round(sum(mrrs) / n_scored, 6) if n_scored else None,
        "no_result_total": len(noise_flags),
        "no_result_noise_count": sum(noise_flags),
        "noise_rate": round(sum(noise_flags) / len(noise_flags), 6) if noise_flags else None,
        "by_category": {
            cat: {
                "n": len(cat_r5[cat]),
                "recall5": round(sum(cat_r5[cat]) / len(cat_r5[cat]), 6),
                "mrr": round(sum(cat_mrr[cat]) / len(cat_mrr[cat]), 6),
            }
            for cat in sorted(cat_r5)
        },
    }
    return out


def format_table(records: Sequence[Dict[str, Any]], baseline_recall5: float) -> str:
    lines = [
        "threshold  noise_rate  recall5   mrr      recall5_loss_pp",
        "---------  ----------  --------  -------  --------------",
    ]
    for rec in records:
        loss_pp = (baseline_recall5 - rec["recall5"]) * 100.0
        lines.append(
            f"{rec['threshold']:>9.2f}  {rec['noise_rate']:>10.4f}  {rec['recall5']:>8.4f}  "
            f"{rec['mrr']:>7.4f}  {loss_pp:>14.2f}"
        )
    return "\n".join(lines)


def recommend(
    sweep: Sequence[Dict[str, Any]], baseline_recall5: float
) -> Dict[str, Any]:
    """按 ask 的规则推荐阈值，返回结构化结果（含无解分支）。"""
    feasible = [
        rec
        for rec in sweep
        if (baseline_recall5 - rec["recall5"]) * 100.0 <= LOSS_BUDGET_PP + EPS
    ]
    if feasible:
        best = min(feasible, key=lambda r: (r["noise_rate"], r["threshold"]))
        return {
            "mode": "loss_constrained_min_noise",
            "feasible_count": len(feasible),
            "feasible_thresholds": [r["threshold"] for r in feasible],
            "threshold": best["threshold"],
            "noise_rate": best["noise_rate"],
            "recall5": best["recall5"],
            "mrr": best["mrr"],
            "recall5_loss_pp": round((baseline_recall5 - best["recall5"]) * 100.0, 4),
        }
    best = min(sweep, key=lambda r: (r["noise_rate"], (baseline_recall5 - r["recall5"]), r["threshold"]))
    return {
        "mode": "no_feasible_fallback",
        "feasible_count": 0,
        "feasible_thresholds": [],
        "threshold": best["threshold"],
        "noise_rate": best["noise_rate"],
        "recall5": best["recall5"],
        "mrr": best["mrr"],
        "recall5_loss_pp": round((baseline_recall5 - best["recall5"]) * 100.0, 4),
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="F2-2a dense 分数阈值 ROC 校准（离线）")
    parser.add_argument("--dump", type=Path, default=DEFAULT_DUMP)
    parser.add_argument("--golden", type=Path, default=DEFAULT_GOLDEN)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--no-dedupe", action="store_true", help="关闭 normalized_id 去重（仅调试）")
    args = parser.parse_args(argv)

    for p in (args.dump, args.golden):
        if not p.exists():
            print(f"[fatal] 输入不存在: {p}", file=sys.stderr)
            return 2

    dump_rows = load_jsonl(args.dump)
    golden_rows = load_jsonl(args.golden)
    golden = {g["query_id"]: g for g in golden_rows}

    # ---- 输入自检（如实记录，不改变判定口径）----
    dump_cats = Counter(r.get("category") for r in dump_rows)
    golden_cats = Counter(g.get("category") for g in golden_rows)
    relevant_queries = [g["query_id"] for g in golden_rows if (g.get("relevant_ids") or [])]
    no_result_queries = [g["query_id"] for g in golden_rows if g.get("category") == "no_result"]
    missing = sorted(set(golden) - {r.get("query_id") for r in dump_rows})
    flag_mismatch = 0
    for row in dump_rows:
        rel = {normalize_memory_id(x) for x in (golden.get(row.get("query_id"), {}).get("relevant_ids") or [])}
        for item in row.get("top", []):
            nid = item.get("normalized_id") or normalize_memory_id(item.get("memory_id"))
            if bool(item.get("relevant")) != bool(nid in rel):
                flag_mismatch += 1

    nr_top1 = sorted(
        float(r["top"][0]["score"])
        for r in dump_rows
        if r.get("category") == "no_result" and r.get("top")
    )

    # ---- T=0 基线（不过滤）----
    baseline = evaluate(dump_rows, golden, 0.0, dedupe=not args.no_dedupe)
    base_r5 = baseline["recall5"]
    base_mrr = baseline["mrr"]
    base_noise = baseline["noise_rate"]

    # ---- ROC 扫描 ----
    sweep = [evaluate(dump_rows, golden, t, dedupe=not args.no_dedupe) for t in threshold_grid()]
    robustness = [
        evaluate(dump_rows, golden, t, dedupe=args.no_dedupe) for t in threshold_grid()
    ]
    rec = recommend(sweep, base_r5)

    # 全分离点：noise_rate 首次为 0 的阈值
    zero_noise_t = next((r["threshold"] for r in sweep if r["noise_rate"] == 0.0), None)
    zero_noise_loss_pp = None
    if zero_noise_t is not None:
        zrec = next(r for r in sweep if r["threshold"] == zero_noise_t)
        zero_noise_loss_pp = round((base_r5 - zrec["recall5"]) * 100.0, 2)

    unseparable = bool(nr_top1) and min(nr_top1) >= NOISE_UNSEPARABLE_AT - EPS
    notes: List[str] = []
    if unseparable:
        notes.append(
            f"no_result 类 top-1 分数全部 >= {NOISE_UNSEPARABLE_AT:.2f}，该分数段无法用阈值区分"
            "（扫描区间内噪声率恒为 100%）。"
        )
    elif zero_noise_t is not None:
        notes.append(
            f"no_result 噪声在 T={zero_noise_t:.2f} 才降到 0（该类 top-1 最高分 "
            f"{max(nr_top1):.4f}），但此时 Recall@5 损失 {zero_noise_loss_pp:.2f} pp，"
            f"超出 {LOSS_BUDGET_PP:.0f} pp 预算，故未被推荐。"
        )
    notes.append(
        f"本 dump 为 dense-only top-10（未叠加 F2-1 ID 短路重排），其 T=0 基线 "
        f"Recall@5={base_r5:.4f} / MRR={base_mrr:.4f}；损失预算以本 dump 的 T=0 为锚。"
    )

    table_text = format_table([baseline] + sweep, base_r5)
    if flag_mismatch:
        notes.append(f"dump 的 relevant 标注与黄金集 relevant_ids 有 {flag_mismatch} 处不一致（已按 relevant_ids 判定）。")

    print("=" * 78)
    print("F2-2a dense-only 分数阈值 ROC 校准（离线，top-10 dump 重放）")
    print("=" * 78)
    print(f"dump: {args.dump}")
    print(f"golden: {args.golden}  ({len(golden_rows)} 条, 类别: {dict(golden_cats)})")
    print(f"dump 行数: {len(dump_rows)}  类别: {dict(dump_cats)}  缺失 query: {len(missing)}")
    print(f"relevant 非空 {len(relevant_queries)} 条, no_result {len(no_result_queries)} 条, "
          f"标注不一致 {flag_mismatch} 处")
    print(f"排序口径: normalized_id 去重={'否' if args.no_dedupe else '是'}（项目 rank_metrics 口径）")
    print(f"no_result top-1 分数范围: [{min(nr_top1):.4f}, {max(nr_top1):.4f}]")
    print("-" * 78)
    print(f"T=0 基线: Recall@5={base_r5:.4f}  MRR={base_mrr:.4f}  噪声率={base_noise:.4f}")
    print("-" * 78)
    print(table_text)
    print("-" * 78)
    print(f"推荐阈值: T={rec['threshold']:.2f} ({rec['mode']})")
    print(f"  噪声率: {base_noise:.4f} -> {rec['noise_rate']:.4f}")
    print(f"  Recall@5: {base_r5:.4f} -> {rec['recall5']:.4f} (损失 {rec['recall5_loss_pp']:.2f} pp)")
    print(f"  MRR: {base_mrr:.4f} -> {rec['mrr']:.4f}")
    print(f"  可行阈值(损失<={LOSS_BUDGET_PP:.0f}pp): {rec['feasible_thresholds'] or '无'}")
    for n in notes:
        print(f"NOTE: {n}")
    print("=" * 78)

    payload = {
        "experiment": "F2-2a dense-only 检索默认阈值 ROC 校准",
        "generated_by": "scripts/analyze_f2_threshold.py",
        "inputs": {
            "dump": str(args.dump.relative_to(PROJECT_ROOT)) if args.dump.is_relative_to(PROJECT_ROOT) else str(args.dump),
            "golden": str(args.golden.relative_to(PROJECT_ROOT)) if args.golden.is_relative_to(PROJECT_ROOT) else str(args.golden),
        },
        "config": {
            "t_min": T_MIN,
            "t_max": T_MAX,
            "t_step": T_STEP,
            "filter_rule": "keep score >= T (drop score < T)",
            "loss_budget_pp": LOSS_BUDGET_PP,
            "dedupe": not args.no_dedupe,
            "noise_rule": "no_result top-1 存活即噪（存活列表非空）",
        },
        "diagnostics": {
            "dump_rows": len(dump_rows),
            "golden_rows": len(golden_rows),
            "golden_categories": dict(golden_cats),
            "dump_categories": dict(dump_cats),
            "scored_queries": baseline["n_scored"],
            "no_result_queries": baseline["no_result_total"],
            "missing_in_dump": missing,
            "relevant_flag_mismatches": flag_mismatch,
            "no_result_top1_scores": nr_top1,
            "no_result_unseparable_in_sweep": unseparable,
        },
        "baseline_t0": baseline,
        "roc_table": [
            {
                "threshold": r["threshold"],
                "noise_rate": r["noise_rate"],
                "recall5": r["recall5"],
                "mrr": r["mrr"],
                "recall5_loss_pp": round((base_r5 - r["recall5"]) * 100.0, 4),
                "by_category": r["by_category"],
            }
            for r in sweep
        ],
        "robustness_no_dedupe": [
            {
                "threshold": r["threshold"],
                "noise_rate": r["noise_rate"],
                "recall5": r["recall5"],
                "mrr": r["mrr"],
                "recall5_loss_pp": round((base_r5 - r["recall5"]) * 100.0, 4),
            }
            for r in robustness
        ],
        "recommendation": {
            **rec,
            "noise_before": base_noise,
            "recall5_before": base_r5,
            "mrr_before": base_mrr,
            "zero_noise_threshold": zero_noise_t,
            "zero_noise_loss_pp": zero_noise_loss_pp,
            "rule": (
                "在 Recall@5 损失 <= 2pp 约束下取噪声率最低的 T；"
                "并列取更小 T；若无可行解则取(噪声最低, 损失最小, T 最小)。"
            ),
        },
        "notes": notes,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.write("\n")
    print(f"[ok] 结果已写入 {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
