"""请求幂等摘要与规范化序列化工具。"""
import hashlib
import json
from typing import Any, Dict


def canonical_json(obj: Any) -> str:
    """产生确定的规范化 JSON 字符串：键排序、紧凑无多余空格、支持非 ASCII 字符。"""
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def compute_request_digest(op: str, payload: Dict[str, Any]) -> str:
    """计算幂等请求摘要：sha256(canonical_json({"op": op, "payload": payload}))。"""
    envelope = {
        "op": op,
        "payload": payload,
    }
    raw = canonical_json(envelope)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
