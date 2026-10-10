#!/usr/bin/env python3
"""
AI Memory Runtime - 黄金评估集构建与校验脚本

支持三个子命令：
  --extract：从 data/sessions.db 只读查询 status='ACTIVE' 的记忆，按 project_id × type 分层抽样 150 条，
             输出 tests/golden/work/memories_sample.jsonl，并按顺序均分 4 份写入 sample_batch_1.jsonl ~ 4.jsonl。
  --special-sets：从 memories_sample.jsonl 为每条样本记忆生成 1 条 exact-ID 专项 query，
                  输出 tests/golden/work/special_exact_id.jsonl。
  --validate PATH：校验黄金集 JSONL 格式与业务约束。
"""

import argparse
import json
import os
import re
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

VALID_CATEGORIES = {"semantic", "no_result", "exact_id", "identifier"}
# memory_id 格式：mem_YYYYMMDD_xxxxxx，支持十六进制字符（6位以上，如 6位或8位等）及可选的 _chunk_N 后缀
MEMORY_ID_REGEX = re.compile(r"^mem_\d{8}_[0-9a-fA-F]+(_chunk_\d+)?$")


def extract_samples(
    db_path: str = "data/sessions.db",
    target_count: int = 150,
    output_dir: str = "tests/golden/work",
    seed: int = 42,
) -> Dict[str, Any]:
    """
    从 SQLite 只读查询 status='ACTIVE' 的记忆，按 project_id × type 进行分层抽样。
    若各层按占比计算不足单层全部，则按最大余数法分配配额；单层不足则全取。
    抽取结果按顺序写入 memories_sample.jsonl，并均分 4 份写入 sample_batch_1.jsonl ~ 4.jsonl。
    """
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"数据库文件不存在: {db_path}")

    # 使用 SQLite 只读 URI 确保只读安全
    uri = f"file:{os.path.abspath(db_path)}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT memory_id, content, project_id, type
            FROM memories
            WHERE status = 'ACTIVE'
            ORDER BY project_id, type, memory_id
            """
        )
        rows = cursor.fetchall()
    finally:
        conn.close()

    total_available = len(rows)
    if total_available == 0:
        raise ValueError("数据库中没有 status='ACTIVE' 的记忆数据！")

    # 按 project_id × type 分层
    strata: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        strata[(r[2], r[3])].append(
            {
                "memory_id": r[0],
                "content": r[1],
                "project_id": r[2],
                "type": r[3],
            }
        )

    # 分层抽样配额分配：最大余数法（Hamilton method）
    sample_target = min(target_count, total_available)
    quotas: Dict[Tuple[str, str], int] = {}
    remainders: List[Tuple[float, int, Tuple[str, str]]] = []

    for key, items in strata.items():
        cnt = len(items)
        exact_quota = sample_target * cnt / total_available
        fl = int(exact_quota)
        quotas[key] = fl
        remainders.append((exact_quota - fl, cnt, key))

    allocated = sum(quotas.values())
    to_distribute = sample_target - allocated

    # 余数降序，相同余数按层容量降序，再按层名确定顺序
    remainders.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
    for i in range(to_distribute):
        key = remainders[i][2]
        quotas[key] += 1

    # 各层不足则全取 (cap at len(items))
    for key, items in strata.items():
        if quotas[key] > len(items):
            quotas[key] = len(items)

    # 抽取样本（保持确定性）
    sampled_records: List[Dict[str, Any]] = []
    import random

    for key in sorted(strata.keys()):
        items = strata[key]
        q = quotas[key]
        if q >= len(items):
            sampled_records.extend(items)
        else:
            rng = random.Random(seed + hash(key))
            sampled_records.extend(rng.sample(items, q))

    # 按层与 memory_id 稳定排序，方便批次顺序切分
    sampled_records.sort(key=lambda x: (x["project_id"], x["type"], x["memory_id"]))

    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    # 输出 tests/golden/work/memories_sample.jsonl
    main_sample_file = out_path / "memories_sample.jsonl"
    with open(main_sample_file, "w", encoding="utf-8") as f:
        for item in sampled_records:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    # 按顺序均分 4 份
    n = len(sampled_records)
    batch_files = []
    group_sizes = []
    batch_count = 4

    start_idx = 0
    for b_idx in range(batch_count):
        size = n // batch_count + (1 if b_idx < n % batch_count else 0)
        end_idx = start_idx + size
        batch_items = sampled_records[start_idx:end_idx]
        start_idx = end_idx

        batch_file = out_path / f"sample_batch_{b_idx + 1}.jsonl"
        with open(batch_file, "w", encoding="utf-8") as f:
            for item in batch_items:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")

        batch_files.append(str(batch_file.relative_to(Path.cwd()) if batch_file.is_relative_to(Path.cwd()) else batch_file))
        group_sizes.append(len(batch_items))

    # 统计分布
    by_proj: Dict[str, int] = defaultdict(int)
    by_type: Dict[str, int] = defaultdict(int)
    for item in sampled_records:
        by_proj[item["project_id"]] += 1
        by_type[item["type"]] += 1

    summary_lines = [
        f"分层抽样总数: {len(sampled_records)}/{total_available}",
        f"项目分布: {dict(sorted(by_proj.items(), key=lambda x: -x[1]))}",
        f"类型分布: {dict(sorted(by_type.items(), key=lambda x: -x[1]))}",
    ]
    summary_text = "\n".join(summary_lines)

    return {
        "sampled": len(sampled_records),
        "groups": batch_files,
        "groupSizes": group_sizes,
        "by_project": dict(by_proj),
        "by_type": dict(by_type),
        "summary": summary_text,
    }


def generate_special_exact_id(
    sample_file: str = "tests/golden/work/memories_sample.jsonl",
    output_file: str = "tests/golden/work/special_exact_id.jsonl",
) -> int:
    """
    从 memories_sample.jsonl 为每条样本记忆生成 1 条 exact-ID 专项 query。
    格式符合黄金集 JSONL 规范：
    {
      "query_id": "q_exact_0001",
      "query": memory_id,
      "category": "exact_id",
      "project_id": item["project_id"],
      "relevant_ids": [memory_id],
      "hard_negative_ids": [],
      "notes": "exact-ID 专项直查 query",
      "reviewed": False,
      "reviewed_by": None
    }
    """
    if not os.path.exists(sample_file):
        raise FileNotFoundError(f"样本文件不存在: {sample_file}")

    queries = []
    with open(sample_file, "r", encoding="utf-8") as f:
        for idx, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            item = json.loads(line)
            m_id = item["memory_id"]
            p_id = item.get("project_id")

            # 自然问句形式与直接 ID 结合
            # 奇数用纯 ID，偶数用包含该 ID 的直接问句
            if idx % 2 == 1:
                q_text = m_id
            else:
                q_text = f"查询记忆 {m_id} 的具体内容"

            queries.append(
                {
                    "query_id": f"q_exact_{idx:04d}",
                    "query": q_text,
                    "category": "exact_id",
                    "project_id": p_id,
                    "relevant_ids": [m_id],
                    "hard_negative_ids": [],
                    "notes": f"exact-ID 直查: {m_id}",
                    "reviewed": False,
                    "reviewed_by": None,
                }
            )

    out_path = Path(output_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        for q in queries:
            f.write(json.dumps(q, ensure_ascii=False) + "\n")

    return len(queries)


def validate_golden_file(golden_path: str) -> Tuple[bool, List[str]]:
    """
    校验黄金集 JSONL 文件：
    1. 每行必须为合法 JSON 且包含所有必需字段；
    2. query_id 全局唯一；
    3. category 属于 {"semantic", "no_result", "exact_id", "identifier"}；
    4. category=semantic 时 relevant_ids 非空，且 hard_negative_ids 包含 2~5 条；
    5. category=no_result 时 relevant_ids 必须为空列表；
    6. category=exact_id 时 relevant_ids 必须包含 1 条且与 query 目标一致；
    7. 所有 relevant_ids 和 hard_negative_ids 中的 ID 格式合法（符合 mem_YYYYMMDD_xxxxxx 格式）。
    """
    required_keys = {
        "query_id",
        "query",
        "category",
        "project_id",
        "relevant_ids",
        "hard_negative_ids",
        "notes",
        "reviewed",
        "reviewed_by",
    }

    if not os.path.exists(golden_path):
        return False, [f"文件不存在: {golden_path}"]

    issues: List[str] = []
    seen_query_ids = set()

    with open(golden_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line_str = line.strip()
            if not line_str:
                continue

            try:
                row = json.loads(line_str)
            except Exception as e:
                issues.append(f"第 {line_num} 行不是合法 JSON: {e}")
                continue

            # 字段齐全检查
            missing_keys = required_keys - set(row.keys())
            if missing_keys:
                issues.append(f"第 {line_num} 行缺少必需字段: {missing_keys}")
                continue

            qid = row["query_id"]
            if not qid or not isinstance(qid, str):
                issues.append(f"第 {line_num} 行 query_id 非法或为空: {qid}")
            elif qid in seen_query_ids:
                issues.append(f"第 {line_num} 行 query_id 重复: {qid}")
            else:
                seen_query_ids.add(qid)

            cat = row["category"]
            if cat not in VALID_CATEGORIES:
                issues.append(f"第 {line_num} 行 category 非法: {cat} (有效值: {VALID_CATEGORIES})")

            rel_ids = row.get("relevant_ids", [])
            hard_neg = row.get("hard_negative_ids", [])

            if not isinstance(rel_ids, list):
                issues.append(f"第 {line_num} 行 relevant_ids 必须为列表")
                rel_ids = []
            if not isinstance(hard_neg, list):
                issues.append(f"第 {line_num} 行 hard_negative_ids 必须为列表")
                hard_neg = []

            # 规则：semantic 的 relevant_ids 非空且 hard_negative_ids 2~5 条
            if cat == "semantic":
                if len(rel_ids) == 0:
                    issues.append(f"第 {line_num} 行 [semantic] relevant_ids 不能为空")
                if not (2 <= len(hard_neg) <= 5):
                    # 允许在 notes 注明无法凑齐的原因
                    notes = row.get("notes") or ""
                    if len(hard_neg) < 2 and "凑不齐" in notes or "hard negative" in notes.lower():
                        pass
                    else:
                        issues.append(
                            f"第 {line_num} 行 [semantic] hard_negative_ids 应有 2~5 条，当前为 {len(hard_neg)} 条"
                        )

            # 规则：no_result 的 relevant_ids 必须为空
            if cat == "no_result":
                if len(rel_ids) > 0:
                    issues.append(f"第 {line_num} 行 [no_result] relevant_ids 必须为空列表，当前为 {rel_ids}")

            # 检查 memory_id 格式
            for mid in rel_ids:
                if not isinstance(mid, str) or not MEMORY_ID_REGEX.match(mid):
                    issues.append(f"第 {line_num} 行 relevant_ids 中的 memory_id 格式非法: {mid}")
            for mid in hard_neg:
                if not isinstance(mid, str) or not MEMORY_ID_REGEX.match(mid):
                    issues.append(f"第 {line_num} 行 hard_negative_ids 中的 memory_id 格式非法: {mid}")

    is_ok = len(issues) == 0
    return is_ok, issues


def main():
    parser = argparse.ArgumentParser(description="AMR 黄金评估集构建与校验工具")
    parser.add_argument("--extract", action="store_true", help="执行 ACTIVE 记忆分层抽样 (150 条) 并均分 4 批")
    parser.add_argument("--special-sets", action="store_true", help="为抽样记忆生成 exact-ID 专项 query 集")
    parser.add_argument("--validate", type=str, metavar="PATH", help="校验指定黄金集 JSONL 文件规范")
    parser.add_argument("--db", type=str, default="data/sessions.db", help="SQLite 数据库路径 (默认: data/sessions.db)")
    parser.add_argument("--count", type=int, default=150, help="抽样目标数量 (默认: 150)")
    parser.add_argument("--work-dir", type=str, default="tests/golden/work", help="中间工作目录")

    args = parser.parse_args()

    if not (args.extract or args.special_sets or args.validate):
        parser.print_help()
        sys.exit(1)

    if args.extract:
        print(f"[*] 开始从 {args.db} 执行分层抽样 (目标 {args.count} 条)...")
        res = extract_samples(
            db_path=args.db,
            target_count=args.count,
            output_dir=args.work_dir,
        )
        print(f"[+] 抽样完成: 共抽得 {res['sampled']} 条记忆。")
        print(f"[+] 分批文件:")
        for grp, sz in zip(res["groups"], res["groupSizes"]):
            print(f"    - {grp} ({sz} 条)")
        print(f"[+] 抽样分布摘要:\n{res['summary']}")

    if args.special_sets:
        sample_file = os.path.join(args.work_dir, "memories_sample.jsonl")
        out_file = os.path.join(args.work_dir, "special_exact_id.jsonl")
        print(f"[*] 开始从 {sample_file} 生成 exact-ID 专项 query...")
        cnt = generate_special_exact_id(sample_file=sample_file, output_file=out_file)
        print(f"[+] 专项 exact-ID query 生成完成: 共 {cnt} 条，写入 {out_file}")

    if args.validate:
        target_path = args.validate
        print(f"[*] 开始校验黄金集规范: {target_path} ...")
        ok, issues = validate_golden_file(target_path)
        if ok:
            print(f"[+] 校验通过！文件符合规范: {target_path}")
            sys.exit(0)
        else:
            print(f"[-] 校验失败！共发现 {len(issues)} 处违规:")
            for issue in issues[:30]:
                print(f"    - {issue}")
            if len(issues) > 30:
                print(f"    ... 还有 {len(issues) - 30} 处违规未显示")
            sys.exit(1)


if __name__ == "__main__":
    main()
