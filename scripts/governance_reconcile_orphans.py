#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AMR 向量层 <-> SSOT 游离点位对账治理。

背景：memory_update 会把 status 同步到 Qdrant payload（实测有效），但 ai_memory_v2 里存在
没有 SQLite memories 行对应的游离点位，它们不受 SSOT 生命周期约束，会持续出现在检索结果里。
同理也可能存在「有 SSOT 行但没有点位」的反向缺失，那部分记忆对任何 agent 都不可见。

安全：默认 dry-run；--apply 需 --backup（自动 dump 受影响点位 payload 到 manifest，可逐条回滚）。
只动 payload.status，不删点位、不动向量。

用法:
  python3 scripts/governance_reconcile_orphans.py
  python3 scripts/governance_reconcile_orphans.py --apply --backup
"""
import argparse
import json
import os
import re
import sqlite3
import sys
import time
import urllib.request

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

CFG = os.path.join(ROOT, "config", "config.yaml")
MANIFEST_DIR = os.path.join(ROOT, "data", "backups")


def load_conn():
    cfg = yaml.safe_load(open(CFG, encoding="utf-8"))
    q = cfg.get("qdrant") or cfg.get("vector_db") or {}
    url = q.get("url") or re.search(r'url:\s*"(http[^"]+)"', open(CFG).read()).group(1)
    key = q.get("api_key") or ""
    declared = q.get("collections")
    if isinstance(declared, dict):
        cand = [declared.get("memory")] + list(declared.values())
    elif isinstance(declared, list):
        cand = list(declared)
    else:
        cand = [declared]
    cand = [c for c in cand if isinstance(c, str) and c] + ["ai_memory_v2", "ai_memory"]
    db = os.path.join(ROOT, "data", "sessions.db")
    return url, key, cand, sqlite3.connect(db)


def detect_collection(url, key, candidates):
    """在候选集合里找出真正承载记忆点位的那个：采样一个点，看 payload 是否含 memory_id。"""
    names = [c["name"] for c in req(url, key, "/collections")["result"]["collections"]]
    for n in candidates + [x for x in names if x not in candidates]:
        if n not in names:
            continue
        try:
            pts = req(url, key, "/collections/%s/points/scroll" % n,
                      {"limit": 3, "with_payload": ["memory_id"], "with_vector": False})["result"]["points"]
        except Exception:
            continue
        if any("memory_id" in (p.get("payload") or {}) for p in pts):
            return n, names
    return (candidates[0] if candidates else "ai_memory_v2"), names


def req(url, key, path, body=None):
    r = urllib.request.Request(url + path,
                               data=json.dumps(body).encode() if body is not None else None,
                               headers={"Content-Type": "application/json", "api-key": key})
    with urllib.request.urlopen(r, timeout=120) as f:
        return json.load(f)


def scroll_all(url, key, coll):
    pts, off = [], None
    while True:
        body = {"limit": 256, "with_payload": ["memory_id", "status", "type", "project_id"],
                "offset": off, "with_vector": False}
        res = req(url, key, "/collections/%s/points/scroll" % coll, body)["result"]
        pts.extend(res["points"])
        off = res.get("next_page_offset")
        if not off:
            return pts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--backup", action="store_true")
    ap.add_argument("--collection", default=None)
    ap.add_argument("--limit-report", type=int, default=40)
    a = ap.parse_args()

    url, key, candidates, db = load_conn()
    if a.collection:
        coll = a.collection
    else:
        coll, names = detect_collection(url, key, candidates)
        print("全部集合: %s" % names)
    print("qdrant=%s collection=%s" % (url, coll))

    pts = scroll_all(url, key, coll)
    pmap = {}
    for p in pts:
        mid = (p.get("payload") or {}).get("memory_id")
        if mid:
            pmap.setdefault(mid, []).append(p)
    rows = {r[0]: r[1] for r in db.execute("select memory_id, status from memories")}
    print("点位 %d（有 memory_id 的 %d）/ SSOT 行 %d" % (len(pts), len(pmap), len(rows)))

    orphan_ids = sorted(set(pmap) - set(rows))          # 向量有、底账无
    missing_ids = sorted(set(rows) - set(pmap))          # 底账有、向量无
    stale = []                                           # 两边都有但 status 不一致
    for mid, prs in pmap.items():
        if mid in rows:
            for p in prs:
                if (p["payload"].get("status") or "").upper() != (rows[mid] or "").upper():
                    stale.append((mid, p["id"], p["payload"].get("status"), rows[mid]))

    print("\n游离点位（向量有/底账无）: %d 个 memory_id" % len(orphan_ids))
    for mid in orphan_ids[:a.limit_report]:
        p = pmap[mid][0]["payload"]
        print("   %-26s payload.status=%-11s type=%-10s project=%s" %
              (mid, p.get("status"), p.get("type"), p.get("project_id")))
    print("\n底账有/向量无: %d 条" % len(missing_ids))
    for mid in missing_ids[:a.limit_report]:
        print("   %-26s sqlite.status=%s" % (mid, rows[mid]))
    print("\n两边 status 不一致: %d 处" % len(stale))
    for s in stale[:a.limit_report]:
        print("   %-26s point=%-11s sqlite=%s" % (s[0], s[1], s[3]))

    if not a.apply:
        print("\nDRY-RUN：未改动。加 --apply --backup 执行：游离点位 payload.status -> DELETED、"
              "status 不一致 -> 以底账为准回写。")
        return 0
    if not a.backup:
        print("\n拒绝执行：--apply 必须同时 --backup（生成回滚 manifest）。")
        return 2

    os.makedirs(MANIFEST_DIR, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    man = os.path.join(MANIFEST_DIR, "orphan_reconcile_%s.json" % ts)
    changed = []
    ops = []
    for mid in orphan_ids:
        for p in pmap[mid]:
            ops.append({"id": p["id"], "payload": {"status": "DELETED"}})
            changed.append({"kind": "orphan_soft_deleted", "memory_id": mid, "point": p["id"],
                            "prev_status": p["payload"].get("status")})
    for mid, pid, pstat, sstat in stale:
        ops.append({"id": pid, "payload": {"status": sstat}})
        changed.append({"kind": "status_resync", "memory_id": mid, "point": pid,
                        "prev_status": pstat, "new_status": sstat})
    json.dump({"ts": ts, "collection": coll, "changed": changed},
              open(man, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print("\n回滚 manifest: %s（%d 处）" % (man, len(changed)))

    # 逐点 set payload（Qdrant 批量接口不支持每点不同值）
    done = 0
    for o in ops:
        req(url, key, "/collections/%s/points/payload?wait=true" % coll,
            {"points": [o["id"]], "payload": o["payload"]})
        done += 1
    print("已回写 %d 个点位 payload.status" % done)
    return 0


if __name__ == "__main__":
    sys.exit(main())
