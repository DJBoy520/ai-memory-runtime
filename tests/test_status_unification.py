"""测试状态统一与规范化映射（RFC v5 C2 / src/service/memory_status.py）。

重点断言：
1. normalize_status 全映射（空/None -> ACTIVE, legacy 四态 -> 标准大写 6 态）；
2. is_status 辅助谓词大小写与多态兼容；
3. 接口层（AMRApplication.dispatch_business_rpc）在 search 参数上支持大小写与列表形式规范化。
"""
import pytest
from unittest.mock import AsyncMock, MagicMock

from src.service.memory_status import CANONICAL_STATUSES, is_status, normalize_status
from src.main import AMRApplication


def test_normalize_status_canonical_and_legacy_mappings():
    """验证 normalize_status 的全部映射规则与边界值。"""
    # 1. 默认与空值
    assert normalize_status(None) == "ACTIVE"
    assert normalize_status("") == "ACTIVE"
    assert normalize_status("   ") == "ACTIVE"

    # 2. Legacy 映射：
    # active -> ACTIVE
    assert normalize_status("active") == "ACTIVE"
    assert normalize_status("  active  ") == "ACTIVE"
    assert normalize_status("ACTIVE") == "ACTIVE"

    # superseded -> HISTORICAL
    assert normalize_status("superseded") == "HISTORICAL"
    assert normalize_status("SUPERSEDED") == "HISTORICAL"
    assert normalize_status("  superseded \n") == "HISTORICAL"

    # archived -> HISTORICAL
    assert normalize_status("archived") == "HISTORICAL"
    assert normalize_status("ARCHIVED") == "HISTORICAL"
    assert normalize_status("  Archived  ") == "HISTORICAL"

    # deleted -> DELETED
    assert normalize_status("deleted") == "DELETED"
    assert normalize_status("DELETED") == "DELETED"
    assert normalize_status("Deleted") == "DELETED"

    # 3. 其它标准 6 态
    assert normalize_status("pending_verify") == "PENDING_VERIFY"
    assert normalize_status("PENDING_VERIFY") == "PENDING_VERIFY"
    assert normalize_status("conflict") == "CONFLICT"
    assert normalize_status("CONFLICT") == "CONFLICT"
    assert normalize_status("temporary") == "TEMPORARY"
    assert normalize_status("TEMPORARY") == "TEMPORARY"

    # 4. 任意其它非标准值（strip().upper()）
    assert normalize_status("candidate") == "CANDIDATE"
    assert normalize_status("stale") == "STALE"
    assert normalize_status("  custom_status  ") == "CUSTOM_STATUS"


def test_is_status_predicate():
    """验证 is_status 谓词的大小写与等价映射判断。"""
    # 单态判断
    assert is_status("active", "ACTIVE") is True
    assert is_status("ACTIVE", "active") is True
    assert is_status("superseded", "HISTORICAL") is True
    assert is_status("archived", "HISTORICAL") is True
    assert is_status("deleted", "DELETED") is True

    # 多态候选判断
    assert is_status("superseded", "ACTIVE", "HISTORICAL") is True
    assert is_status("active", "HISTORICAL", "DELETED") is False

    # 空值默认 ACTIVE
    assert is_status(None, "ACTIVE") is True
    assert is_status("", "ACTIVE") is True


@pytest.mark.asyncio
async def test_search_status_normalization_in_dispatch():
    """验证 AMRApplication.dispatch_business_rpc 中 search 方法对 status 的统一规范化。"""
    mock_mem_service = MagicMock()
    mock_mem_service.memory_search = AsyncMock(return_value={"results": [], "total": 0})

    fake_engine = MagicMock()
    fake_engine.state = MagicMock(value="ready")
    fake_engine.get_model_status = AsyncMock(return_value={"state": "ready"})

    fake_qdrant = MagicMock()
    fake_qdrant.is_healthy.return_value = True

    app = AMRApplication(
        engine=fake_engine,
        qdrant_manager=fake_qdrant,
        session_store=MagicMock(),
    )
    app.memory_service = mock_mem_service

    # 1. 传入小写 legacy 字符串 'superseded' -> 规范化为 'HISTORICAL'
    await app.dispatch_business_rpc("memory.search", {
        "query": "test query",
        "status": "  superseded  ",
    })
    _, kwargs1 = mock_mem_service.memory_search.call_args
    assert kwargs1["status"] == "HISTORICAL"

    # 2. 传入状态列表 ['active', 'archived'] -> 规范化为 ['ACTIVE', 'HISTORICAL']
    await app.dispatch_business_rpc("memory.search", {
        "query": "test query 2",
        "status": ["active", "archived"],
    })
    _, kwargs2 = mock_mem_service.memory_search.call_args
    assert kwargs2["status"] == ["ACTIVE", "HISTORICAL"]

    # 3. 未传 status -> None
    await app.dispatch_business_rpc("memory.search", {
        "query": "test query 3",
    })
    _, kwargs3 = mock_mem_service.memory_search.call_args
    assert kwargs3["status"] is None
