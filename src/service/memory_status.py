"""内存记忆实体状态规范化与枚举校验模块。

定义标准 6 态与历史 legacy 状态的映射归一。
"""
from typing import Optional, Set

CANONICAL_STATUSES: Set[str] = {
    "ACTIVE",
    "PENDING_VERIFY",
    "CONFLICT",
    "HISTORICAL",
    "TEMPORARY",
    "DELETED",
}

_LEGACY_STATUS_MAP = {
    "active": "ACTIVE",
    "superseded": "HISTORICAL",
    "archived": "HISTORICAL",
    "deleted": "DELETED",
}


def normalize_status(value: Optional[str]) -> str:
    """将状态值规范化为标准大写枚举。

    规则：
    - None / 空串 -> "ACTIVE"
    - legacy 映射：
        active -> ACTIVE
        superseded -> HISTORICAL
        archived -> HISTORICAL
        deleted -> DELETED
    - 其它（如 candidate, stale, TEMPORARY 等） -> strip().upper()
    """
    if value is None:
        return "ACTIVE"
    cleaned = str(value).strip()
    if not cleaned:
        return "ACTIVE"
    lower = cleaned.lower()
    if lower in _LEGACY_STATUS_MAP:
        return _LEGACY_STATUS_MAP[lower]
    return cleaned.upper()


def is_status(value: Optional[str], *canonical: str) -> bool:
    """判断给定状态值（经过 normalize_status 后）是否属于指定的标准状态集。"""
    norm = normalize_status(value)
    canonical_norms = {normalize_status(c) for c in canonical}
    return norm in canonical_norms
