#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AMR project_id 归一迁移（SSOT + Qdrant payload 双写）。

为什么需要：memory_update 接口不接受 project_id（只有 content/type/status/conflicts_with），
项目域拼写变体（SignalKeepers / signal-keepers / amr / 把会话 ID 写成项目名的 aep-sess-zcode）
无法经接口纠正。本脚本用映射表驱动，改映射不改代码。

安全：默认 dry-run；--apply 先写回滚 manifest（记录每条 memory_id 的旧 project_id 与点位 id），
再 SQLite UPDATE + Qdrant set payload，最后复算断言，不符即 exit 1。

用法:
  python3 scripts/governance_project_norm.py
  python3 scripts/governance_project_norm.py --apply
  python3 scripts/governance_project_norm.py --apply --map config/project_alias.json
"""
import argparse
import json
import os
import sqlite3
import sys
import time

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
from governance_reconcile_orphans import detect_collection, req  # noqa: E402

DEFAULT_MAP = {
    "SignalKeepers": "signalkeepers",
    "signal-keepers": "signalkeepers",
    "Signal-Keepers": "signalkeepers",
    "amr": "ai-memory-runtime",
    "AMR": "ai-memory-runtime",
    "aep-sess-zcode": "aep",
    "": "global",
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--map", dest="mapfile", default=None)
    ap.add_argument("--collection", default=None)
    a = ap.parse_args()

    amap = DEFAULT_MAP
    if a.mapfile:
        p = a.mapfile if os.path.isabs(a.mapfile) else os.path.join(ROOT, a.mapfile)
        amap = json.load(open(p, encoding="utf-8"))
    print("映射表 %d 条: %s" % (len(amap), json.dumps(amap, ensure_ascii=False)))

    cfg = yaml.safe_load(open(os.path.join(ROOT, "config", "config.yaml"), encoding="utf-8"))
    q = cfg["qdrant"]
    url, key = q["url"], q.get("api_key") or ""
    coll = a.collection
    if not coll:
        declared = q.get("collections")
        cand = list(declared) if isinstance(declared, list) else [declared]
        coll, _ = detect_collection(url, key, [c for c in cand if isinstance(c, str)]
                                   + ["ai_memory_v2", "ai_memory"])
    print("collection =", coll)

    db = sqlite3.connect(os.path.join(ROOT, "data", "sessions.db"))
    rows = list(db.execute("select memory_id, project_id, qdrant_point_id, status "
                           "from memories where project_id in (%s)"
                           % ",".join("?" * len(amap)), list(amap)))
    if not rows:
        print("没有需要归一的行。")
        return 0
    print("\n命中 %d 行：" % len(rows))
    plan = []
    for mid, old, pid, st in rows:
        new = amap.get(old, old)
        print("   %-26s %-16s -> %-18s (status=%s)" % (mid, repr(old), new, st))
        plan.append({"memory_id": mid, "old": old, "new": new, "point": pid})

    if not a.apply:
        print("\nDRY-RUN：未写库。加 --apply 执行。")
        return 0

    man = os.path.join(ROOT, "data", "backups",
                       "project_norm_%s.json" % time.strftime("%Y%m%d_%H%M%S"))
    json.dump({"ts": time.time(), "collection": coll, "plan": plan},
              open(man, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print("回滚 manifest:", man)

    for p in plan:
        db.execute("update memories set project_id=?, updated_at=? where memory_id=?",
                   (p["new"], int(time.time()), p["memory_id"]))
        if p["point"]:
            req(url, key, "/collections/%s/points/payload?wait=true" % coll,
                {"points": [p["point"]], "payload": {"project_id": p["new"]}})
    db.commit()

    bad = []
    for p in plan:
        got = db.execute("select project_id from memories where memory_id=?",
                         (p["memory_id"],)).fetchone()
        if not got or got[0] != p["new"]:
            bad.append((p["memory_id"], got))
        sc = req(url, key, "/collections/%s/points/scroll" % coll,
                 {"limit": 1, "with_payload": ["project_id"],
                  "filter": {"must": [{"key": "memory_id", "match": {"value": p["memory_id"]}}]}}
                 )["result"]["points"]
        if sc and sc[0]["payload"].get("project_id") != p["new"]:
            bad.append((p["memory_id"], sc[0]["payload"].get("project_id")))
    if bad:
        print("!! 断言失败 %d 处: %s" % (len(bad), bad))
        return 1
    print("全部 %d 行 SSOT 与 payload 均已归一，断言通过。" % len(plan))
    return 0


if __name__ == "__main__":
    sys.exit(main())
