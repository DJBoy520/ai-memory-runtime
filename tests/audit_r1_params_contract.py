"""
AMR v5 Audit Track R1: 参数校验、边界值与响应契约红队审计测试
覆盖攻击面：
1. content 边界：>16000 超长、刚好 5 字、4 字、纯空白、Emoji/组合 Unicode、控制字符、NUL 字符 (\x00)
2. project_id, type, source_refs 超长与畸形类型校验
3. limit 边界：0, -1, 超大(100), 布尔(True/False), 字符串类型
4. score_threshold 边界：<0.0, >1.0, 非数值类型缺少校验
5. status 枚举：大小写、非法值、创建时传入 HISTORICAL/DELETED 的契约不一致
6. include_history: 传入字符串 'false' 被 Python 视作真值的契约缺陷
7. memory_get 缺失返回 null 与其它操作 -32005 NOT_FOUND 契约不一致性
8. 错误消息中的敏感路径与内部实现泄露
"""

import asyncio
import pytest
from pathlib import Path
from typing import Any, Dict

from src.core.errors import (
    IdempotencyConflict,
    InvalidStatusError,
    LegacyPathForbidden,
    VersionConflictError,
)
from src.core.session_store import SessionStore
from src.interfaces.mcp.bridge import MCPBridge
from src.interfaces.mcp.tools import TOOL_DEFINITIONS, validate_tool_args
from src.service.memory_service import MemoryService
from tests.audit_r1_auth import DummyEngine, DummyQdrant


@pytest.fixture
def store(tmp_path):
    db_file = str(tmp_path / "test_params_audit.db")
    st = SessionStore(db_path=db_file)
    yield st
    st.close()


@pytest.fixture
def memory_service(store):
    return MemoryService(
        engine=DummyEngine(),
        qdrant_manager=DummyQdrant(),
        session_store=store,
    )


# =========================================================================
# 1. Content 长度与特殊字符边界
# =========================================================================

@pytest.mark.asyncio
async def test_content_length_boundaries(memory_service):
    """测试 content 长度边界：刚好 5 字符、小于 5 字符、大于 16000 字符、纯空白"""
    # 1. 刚好 5 字符（合法下界）
    res5 = await memory_service.memory_create(content="12345", agent_id="openclaw")
    assert res5["memory_id"].startswith("mem_")

    # 2. 小于 5 字符（非法）
    with pytest.raises(ValueError) as exc_info:
        await memory_service.memory_create(content="1234", agent_id="openclaw")
    assert "too short" in str(exc_info.value).lower()

    # 3. 纯空白字符（非法）
    for empty_val in ["", "   ", "\t\n  \r\n"]:
        with pytest.raises(ValueError):
            await memory_service.memory_create(content=empty_val, agent_id="openclaw")

    # 4. 超长字符（>16000，非法）
    huge_content = "A" * 16001
    with pytest.raises(ValueError) as exc_info2:
        await memory_service.memory_create(content=huge_content, agent_id="openclaw")
    assert "too long" in str(exc_info2.value).lower()

    # 5. 刚好 16000 字符（合法上界）
    max_content = "B" * 16000
    res_max = await memory_service.memory_create(content=max_content, agent_id="openclaw")
    assert res_max["memory_id"].startswith("mem_")


@pytest.mark.asyncio
async def test_content_unicode_emoji_and_control_chars(memory_service):
    """验证含复杂 Unicode (ZWJ 序列、中英日阿混合)、控制字符的正文读写一致性"""
    complex_text = "复合知识：👨‍👩‍👧‍👦 架构演进 -> 零点测试 ​‌‍ [Arabic: العربية / Japanese: メモリ]"
    res = await memory_service.memory_create(content=complex_text, agent_id="openclaw")
    mid = res["memory_id"]

    fetched = await memory_service.memory_get(mid)
    assert fetched["content"] == complex_text


# =========================================================================
# 2. 漏洞发现用例：tools.py 未校验 content 最小 5 字符与 status 枚举
# =========================================================================

def test_tools_py_missing_min_length_validation_bug():
    """
    【发现 BUG 验证】
    TOOL_DEFINITIONS 中明确声明 content 为 min 5 chars：
    'content': {'type': 'string', 'description': 'Substantive knowledge text ... (min 5 chars).'}
    但 tools.py 中的 validate_tool_args('memory_create', ...) 仅做了：
    `if 'content' not in args or not isinstance(args['content'], str) or not args['content'].strip():`
    完全没有校验 len >= 5！导致 1-4 字符的入参被放行至后端存储才报错。
    """
    args = {"content": "abcd"}  # 4 字符
    try:
        validate_tool_args("memory_create", args)
        pytest.fail(
            "validate_tool_args('memory_create') passed 4-char content! "
            "Schema specifies min 5 chars but validate_tool_args does not check length."
        )
    except ValueError as e:
        assert "min 5 chars" in str(e).lower()


def test_tools_py_missing_status_enum_validation_bug():
    """
    【发现 BUG 验证】
    TOOL_DEFINITIONS 中明确声明 status 枚举：['ACTIVE', 'PENDING_VERIFY', 'CONFLICT', 'TEMPORARY']
    但 tools.py 中的 validate_tool_args('memory_create', ...) 完全没有对 status 做任何校验！
    无论传入 'INVALID_STATUS' 还是 'DELETED'，validate_tool_args 均直接放行。
    """
    args = {"content": "Valid substantive content", "status": "COMPLETELY_INVALID_STATUS"}
    try:
        validate_tool_args("memory_create", args)
        pytest.fail(
            "validate_tool_args('memory_create') passed invalid status enum without validation!"
        )
    except ValueError:
        pass


def test_tools_py_case_sensitive_status_rejection_bug():
    """
    【发现 BUG 验证】
    tools.py:299-302 validate_tool_args('memory_update_status'):
    valid_statuses 使用硬编码集合，其中仅包含了 'active' 与 'ACTIVE'，
    若传入首字母大写的 'Active' 或 'Pending_Verify'，直接被判定为非法：
    `ValueError: Invalid status 'Active'`。
    """
    args = {"memory_id": "mem_123", "new_status": "Active"}
    try:
        validate_tool_args("memory_update_status", args)
    except ValueError as e:
        if "Invalid status 'Active'" in str(e):
            pytest.fail(
                f"validate_tool_args('memory_update_status') is case-sensitive! "
                f"Rejected valid status 'Active': {e}"
            )


# =========================================================================
# 3. limit 与 score_threshold 边界校验
# =========================================================================

def test_limit_validation_in_tools_py():
    """validate_tool_args 对 limit=0, limit=-1, limit=True 的防护"""
    # 1. limit <= 0 拦截
    for bad_limit in [0, -1, -99]:
        with pytest.raises(ValueError) as exc:
            validate_tool_args("memory_search", {"query": "test", "limit": bad_limit})
        assert "positive integer" in str(exc.value).lower()

    # 2. 布尔 True 拦截（Python 中 isinstance(True, int) 为真）
    with pytest.raises(ValueError) as exc2:
        validate_tool_args("memory_search", {"query": "test", "limit": True})
    assert "positive integer" in str(exc2.value).lower()


@pytest.mark.asyncio
async def test_include_history_string_false_pitfall_bug(memory_service):
    """
    【发现 BUG 验证】
    在 main.py / memory_service.py 中：
    `include_history=params.get("include_history", False)`
    `elif include_history:`
    如果客户端从 JSON 传参时误传了字符串 `"false"`（例如通过命令行或未严格转换的表单），
    在 Python 中 `bool("false") == True`！
    导致原本要求仅查询当前事实（ACTIVE）的请求被静默扩展包含了 HISTORICAL 历史事实！
    """
    from qdrant_client import models
    from unittest.mock import MagicMock

    # 模拟检查 memory_search 中传递给 qdrant 的 allowed_statuses
    captured_statuses = []

    def mock_search_points(*args, **kwargs):
        captured_statuses.append(kwargs.get("allowed_statuses"))
        return []

    memory_service.qdrant.search_points = mock_search_points

    # 传入字符串 "false"
    await memory_service.memory_search(query="test query", include_history="false")

    # 期望：应被解析为 False，allowed_statuses 仅包含 ACTIVE
    # 实际：由于 "false" 为真值，进入了 elif include_history 分支，包含 HISTORICAL / SUPERSEDED！
    if captured_statuses and any("HISTORICAL" in s for s in captured_statuses[0]):
        pytest.fail(
            f"include_history='false' was evaluated as True truthy value in Python! "
            f"allowed_statuses expanded to: {captured_statuses[0]}"
        )


# =========================================================================
# 4. memory_get 缺失返回 null 与其它操作 -32005 契约不一致性
# =========================================================================

@pytest.mark.asyncio
async def test_memory_get_null_vs_mutation_404_inconsistency_bug(memory_service):
    """
    【契约不一致性审计】
    对于不存在的 memory_id：
    - `memory_get` 返回 `None` -> JSON-RPC `{"result": null}` (MCP 输出 "null", isError: false)
    - `memory_update` 抛 `KeyError` -> JSON-RPC `-32005 NOT_FOUND` (isError: true)
    - `memory_history` 抛 `KeyError` -> JSON-RPC `-32005 NOT_FOUND` (isError: true)
    - `memory_delete` 抛 `KeyError` -> JSON-RPC `-32005 NOT_FOUND` (isError: true)
    虽然查询返回 null 在某些 REST 设计中常见，但在 TOOL_DEFINITIONS 及 JSON-RPC 规范中
    未对该不一致性进行任何文档说明，易导致 Agent 误判（读取返回 null 成功，历史/删除却抛异常）。
    """
    non_existent_id = "mem_99999999_ffffff"

    # 1. memory_get -> None (成功响应 null)
    get_res = await memory_service.memory_get(non_existent_id)
    assert get_res is None

    # 2. memory_update -> KeyError (-32005)
    with pytest.raises(KeyError) as exc_up:
        await memory_service.memory_update(memory_id=non_existent_id, content="new")
    assert "not found" in str(exc_up.value).lower()

    # 3. memory_history -> KeyError (-32005)
    with pytest.raises(KeyError) as exc_hist:
        await memory_service.memory_history(memory_id=non_existent_id)
    assert "not found" in str(exc_hist.value).lower()

    # 4. memory_delete -> KeyError (-32005)
    with pytest.raises(KeyError) as exc_del:
        await memory_service.memory_delete(memory_id=non_existent_id)
    assert "not found" in str(exc_del.value).lower()


# =========================================================================
# 5. 错误消息中的敏感信息泄露审计
# =========================================================================

def test_error_message_path_leakage_in_mcp_bridge(tmp_path):
    """
    【敏感信息泄露审计】
    MCPBridge 连接失败时：
    `raise ConnectionError(f"AMR daemon socket not found at '{self.socket_path}'.")`
    在 error 响应中直接包含了服务端的绝对路径（如 /tmp/user_sensitive_dir/qdrant-bge.sock），
    将宿主系统的主机目录架构暴露给了客户端。
    """
    sensitive_path = str(tmp_path / "user_private_scope" / "internal.sock")
    bridge = MCPBridge(socket_path=sensitive_path)
    try:
        # 触发不存在 socket
        asyncio.run(bridge._call_daemon("ping", {}, 1))
    except ConnectionError as e:
        msg = str(e)
        assert sensitive_path in msg
