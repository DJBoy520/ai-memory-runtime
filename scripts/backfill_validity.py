#!/usr/bin/env python3
"""crypto_standards 有效性元数据回填（多模型共识终版方案 §3，v2.2.0 配套）。

输入：标准元数据目录 standards_seed.json（含现行/废止/替代链与 verified 标记）。
输出：为 crypto_standards 文档切块点位补 standard_status/superseded_by/source_tier/verified/catalog_version。

设计约束（评审共识）：
- 家族内匹配：目录条目按 family 归一化为匹配键，仅在对应家族的点集合内匹配，防跨家族歧义；
- 归一化：标识符小写、[_/. ] 归一为 -，RFC/GMT/GB/T 前缀与扩展名剥离；
- 显式 ID 写入：先 scroll 取点位台账（ID+标识字段）→ 本地匹配生成 manifest →
  dry-run 对账（异常不写）→ 按条目批量 set_payload（同条目所有 chunk 同值，仅新增键）；
- 未匹配点位：standard_status=unknown（fail-open，不猜）；
- 对账报告：每条目命中数 / unknown 数 / 总覆盖。

用法：
  python3 backfill_validity.py --catalog /path/standards_seed.json [--dry-run] [--apply]
（默认 dry-run；--apply 才写。）
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import defaultdict

from qdrant_client import QdrantClient
from qdrant_client.http import models

CATALOG_VERSION = "seed-2026-11-21"
SCROLL_PAGE = 256

# 家族 → 点标识识别正则（家族内匹配，防跨家族歧义）
FAMILY_PATTERNS = {
    "FIPS": re.compile(r"fips[-_ ]?(?P<num>\d{2,3}(?:-\d+)?)", re.I),
    "RFC": re.compile(r"rfc[-_ ]?(?P<num>\d{3,5})", re.I),
    "GB": re.compile(r"gb[/\\t-]*[ -]?(?P<num>\d{4,6}(?:\.\d+)?)[ -]?(\d{4})?"),
    "GM/T": re.compile(r"gm[/\\t-]*[ -]?(?P<num>\d{4}(?:\.\d+)?)[ -]?(\d{4})?", re.I),
    "SP": re.compile(r"(?:sp|special publication)[-_ ]?800[-_ ]?(?P<num>\d{1,3}[a-z]?(?:r\d)?)", re.I),
    "IR": re.compile(r"(?:ir|ir\b|interagency report)[-_ ]?(?P<num>\d{3,4})", re.I),
    "ISO/IEC": re.compile(r"iso[/ie-]+(?P<num>\d{4,5}(?:-\d+)?)", re.I),
}


def norm_id(s: str) -> str:
    return re.sub(r"[_/\\. ]+", "-", (s or "").lower())


def catalog_key(entry: dict) -> tuple | None:
    """从目录条目提取 (family, number, year)。"""
    sid = entry.get("standard_id", "")
    fam = entry.get("family", "")
    if fam == "FIPS":
        m = re.search(r"(\d{2,3}(?:-\d+)?)", sid.replace("FIPS", ""))
        return ("FIPS", norm_id(m.group(1)), None) if m else None
    if fam == "RFC":
        m = re.search(r"(\d{3,5})", sid)
        return ("RFC", m.group(1), None) if m else None
    if fam in ("GB/T", "GB"):
        m = re.search(r"(\d{4,6}(?:\.\d+)?)[-\s]?(\d{4})?", sid.replace("GB/T", "").replace("GB", ""))
        return ("GB", norm_id(m.group(1)), m.group(2)) if m else None
    if fam == "GM/T":
        m = re.search(r"(\d{4}(?:\.\d+)?)[-\s]?(\d{4})?", sid.replace("GM/T", ""))
        return ("GM/T", norm_id(m.group(1)), m.group(2)) if m else None
    return None


def point_signature(payload: dict) -> str:
    """点位标识签名：file_name + standard_id + relative_path 归一化拼接。"""
    return norm_id(" ".join(filter(None, [
        payload.get("file_name", ""), payload.get("standard_id", ""), payload.get("relative_path", ""),
    ])))


def match_entry(fam: str, num: str, year: str | None, sig: str) -> bool:
    """家族内匹配：数字必须出现，且若目录有年份则签名也需含该年份（或签名无任何年份）。"""
    if fam == "FIPS":
        return bool(re.search(rf"(?:^|-)fips-{num}(?:-|$)|fips-{num}", sig))
    if fam == "RFC":
        return bool(re.search(rf"(?:^|-)rfc-{num}(?:-|$)|rfc-{num}", sig))
    if fam in ("GB", "GM/T"):
        # 前缀族（gb/gmt/gm）出现 + 数字出现；年份若目录指定则须匹配
        prefix = "gb" if fam == "GB" else r"gm(?:t)?"
        pat = rf"(?:^|-){prefix}(?:-)?{num}(?:-|$)"
        if not re.search(pat, sig):
            return False
        if year and re.search(r"\d{4}", sig):
            return year in sig
        return True
    return False


def status_fields(entry: dict) -> dict:
    st = entry.get("status", "unknown")
    if st.startswith("current"):
        st = "current"
    elif st.startswith("obsolete"):
        st = "obsolete"
    st = st if st in ("current", "obsolete", "withdrawn", "unknown", "pending_crawl") else "unknown"
    verified = bool(entry.get("verified", False))
    tier = "official"
    return {
        "standard_status": st,
        "superseded_by": entry.get("superseded_by"),
        "source_tier": tier,
        "verified": verified,
        "catalog_version": CATALOG_VERSION,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--qdrant-url", default=os.environ.get("AMR_QDRANT_URL", "http://192.168.30.161:6333"))
    ap.add_argument("--api-key", default=os.environ.get("AMR_QDRANT_API_KEY", ""))
    ap.add_argument("--collection", default="crypto_standards")
    ap.add_argument("--dry-run", action="store_true", default=True)
    ap.add_argument("--apply", dest="dry_run", action="store_false")
    args = ap.parse_args()

    catalog = json.load(open(args.catalog))
    entries = []
    for e in catalog.get("entries", []):
        key = catalog_key(e)
        if key and e.get("status") not in ("pending_crawl",):
            entries.append((key, e))
    print(f"[catalog] matchable entries: {len(entries)}")

    client = QdrantClient(url=args.qdrant_url, api_key=args.api_key or None, timeout=60)

    # 1. 点位台账（显式 ID）
    ledger: list[tuple[str, dict]] = []
    offset = None
    while True:
        kwargs = {"collection_name": args.collection, "limit": SCROLL_PAGE,
                  "with_payload": True, "with_vectors": False}
        if offset is not None:
            kwargs["offset"] = offset
        pts, nxt = client.scroll(**kwargs)
        for p in pts or []:
            ledger.append((str(p.id), p.payload or {}))
        if not nxt:
            break
        offset = nxt
    print(f"[ledger] points: {len(ledger)}")

    # 2. 匹配（家族内；最长数字键优先）
    entry_points: dict[tuple, list[str]] = defaultdict(list)
    unmatched_ids: list[str] = []
    for pid, payload in ledger:
        sig = point_signature(payload)
        best_key, best_len = None, -1
        for key, _e in entries:
            fam, num, year = key
            if match_entry(fam, num, year, sig) and len(num) > best_len:
                best_key, best_len = key, len(num)
        if best_key:
            entry_points[best_key].append(pid)
        else:
            unmatched_ids.append(pid)
    print(f"[match] entries with hits: {len(entry_points)}; unmatched points: {len(unmatched_ids)}")

    # 3. manifest 与 dry-run 报告
    manifest = {}
    for key, e in entries:
        pids = entry_points.get(key, [])
        manifest["|".join(str(x) for x in key)] = {"points": len(pids), "status": e.get("status"), "verified": e.get("verified")}
        if pids:
            print(f"  {key[0]} {key[1]}" + (f" ({key[2]})" if key[2] else "") +
                  f" => {len(pids)} pts [{e.get('status')}]")
    print(f"[manifest] {json.dumps({k: v['points'] for k, v in manifest.items()}, ensure_ascii=False)[:600]}")

    if args.dry_run:
        print("[dry-run] no writes. Re-run with --apply to write.")
        return 0

    # 4. 显式 ID 批量写入（同条目同值；仅新增键）
    ok = failed = 0
    for key, e in entries:
        pids = entry_points.get(key, [])
        if not pids:
            continue
        try:
            client.set_payload(collection_name=args.collection, points=pids,
                               payload=status_fields(e), wait=True)
            ok += len(pids)
        except Exception as ex:
            failed += len(pids)
            print(f"[error] {key}: {ex}")
    # 未匹配点位 → unknown（fail-open，不猜具体状态）
    unknown_payload = {"standard_status": "unknown", "source_tier": "official",
                       "verified": False, "catalog_version": CATALOG_VERSION}
    for i in range(0, len(unmatched_ids), 500):
        try:
            client.set_payload(collection_name=args.collection, points=unmatched_ids[i:i + 500],
                               payload=unknown_payload, wait=True)
            ok += min(500, len(unmatched_ids) - i)
        except Exception as ex:
            failed += 500
            print(f"[error] unknown batch: {ex}")

    # 5. 对账：按 standard_status 计数
    counts = {}
    for st in ("current", "obsolete", "withdrawn", "unknown"):
        c = client.count(args.collection, exact=True, count_filter=models.Filter(
            must=[models.FieldCondition(key="standard_status", match=models.MatchValue(value=st))]
        )).count
        counts[st] = c
    print("[reconcile] " + json.dumps({
        "written_points": ok, "failed": failed, "status_counts": counts,
        "total": sum(counts.values()), "ledger": len(ledger),
    }, ensure_ascii=False))
    manifest_path = os.path.join(os.path.dirname(args.catalog), "validity_manifest.json")
    json.dump(manifest, open(manifest_path, "w"), ensure_ascii=False, indent=1)
    print(f"[manifest saved] {manifest_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
