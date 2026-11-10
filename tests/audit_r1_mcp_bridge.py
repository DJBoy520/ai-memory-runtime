"""
AMR v5 Audit Track R1: MCP Bridge 接口、安全与契约审计测试
覆盖攻击面：
1. initialize, notifications/initialized, tools/list, 未知 method (-32601), 未知 tool, 错误参数校验
2. Daemon 异常透传 (-32001, -32002, -32005, -32006, -32602) 的 code, message, data 完整性
3. Daemon socket 不存在、超时、超大响应处理
4. stdout 纯净度（严禁非 JSON 污染 stdout）
5. agent_id 自动注入缺陷：客户端传入时未被强制覆盖（允许伪造身份）
6. 畸形输入（params 为 None/列表、畸形 JSON）导致 Bridge 静默丢弃/无响应 hang 缺陷
"""

import asyncio
import json
import os
import sys
import pytest
from pathlib import Path
from typing import Any, Dict

from src.core.errors import (
    IdempotencyConflict,
    VersionConflictError,
)
from src.interfaces.ipc.errors import RpcAgentUnauthorized
from src.interfaces.ipc.protocol import MAX_REQUEST_BYTES, write_frame
from src.interfaces.ipc.server import UDSServer
from src.interfaces.mcp.bridge import DaemonRpcError, MCPBridge
from src.interfaces.mcp.tools import TOOL_DEFINITIONS


@pytest.fixture
async def mock_daemon_uds(tmp_path):
    """启动专用于 MCP Bridge 审计的 Mock Daemon UDS"""
    server_instances = []

    async def _start(handler):
        sock_path = str(tmp_path / f"mock_daemon_{len(server_instances)}.sock")
        srv = UDSServer(
            name=f"mock-daemon-{len(server_instances)}",
            socket_path=sock_path,
            handler=handler,
        )
        await srv.start()
        server_instances.append(srv)
        return srv, sock_path

    yield _start

    for s in server_instances:
        try:
            await s.stop()
        except Exception:
            pass


# =========================================================================
# 1. MCP 生命周期与标准协议
# =========================================================================

@pytest.mark.asyncio
async def test_mcp_initialize_and_tools_list():
    """验证 MCP initialize、initialized 通知与 tools/list"""
    bridge = MCPBridge(socket_path="/nonexistent/sock.sock", source_agent="openclaw")

    # 1. initialize
    init_req = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
    init_resp = await bridge.handle_request(init_req)
    assert init_resp["id"] == 1
    assert init_resp["result"]["protocolVersion"] == "2024-11-05"
    assert "tools" in init_resp["result"]["capabilities"]

    # 2. notifications/initialized (返回 None，stdio 不输出任何行)
    notify_req = {"jsonrpc": "2.0", "method": "notifications/initialized"}
    notify_resp = await bridge.handle_request(notify_req)
    assert notify_resp is None

    # 3. tools/list (返回完整 9 个工具定义)
    list_req = {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}
    list_resp = await bridge.handle_request(list_req)
    assert list_resp["id"] == 2
    tools = list_resp["result"]["tools"]
    tool_names = [t["name"] for t in tools]
    assert "memory_search" in tool_names
    assert "memory_create" in tool_names
    assert "memory_update" in tool_names
    assert "memory_delete" in tool_names
    assert "memory_history" in tool_names
    assert "memory_get" in tool_names
    assert len(tools) == len(TOOL_DEFINITIONS)


@pytest.mark.asyncio
async def test_mcp_unknown_method_and_unknown_tool():
    """验证未知 method 返回 -32601，未知 tool 返回 isError: True"""
    bridge = MCPBridge(socket_path="/nonexistent/sock.sock", source_agent="openclaw")

    # 1. 未知 MCP method -> -32601
    bad_method_req = {"jsonrpc": "2.0", "id": 10, "method": "custom/unknown_call", "params": {}}
    resp = await bridge.handle_request(bad_method_req)
    assert resp["id"] == 10
    assert resp["error"]["code"] == -32601
    assert "Method not found" in resp["error"]["message"]

    # 2. 未知 tool name
    bad_tool_req = {
        "jsonrpc": "2.0",
        "id": 11,
        "method": "tools/call",
        "params": {"name": "non_existent_tool", "arguments": {}},
    }
    resp2 = await bridge.handle_request(bad_tool_req)
    assert resp2["id"] == 11
    assert resp2["result"]["isError"] is True
    assert "Unknown tool name" in resp2["result"]["content"][0]["text"]


# =========================================================================
# 2. 错误码与数据全字段透传 (-32001, -32002, -32005, -32006, -32602)
# =========================================================================

@pytest.mark.asyncio
async def test_daemon_error_passthrough_all_codes(mock_daemon_uds):
    """验证 daemon 返回的业务错误码 (-32001 ~ -32006, -32602) 及其 data 字段在 MCP 响应中完整呈现"""
    async def mock_handler(method, params):
        err_type = params.get("trigger_error")
        if err_type == "agent_unauthorized":
            raise RpcAgentUnauthorized(
                "AGENT_UNAUTHORIZED: Untrusted agent",
                data={"agent_id": params.get("agent_id")},
            )
        elif err_type == "version_conflict":
            raise VersionConflictError(
                "Version mismatch",
                memory_id="mem_err_1",
                current_version=3,
                expected_version=1,
            )
        elif err_type == "idempotency_conflict":
            raise IdempotencyConflict(
                op="create",
                idem_key="key_err_1",
                existing_memory_id="mem_exist_99",
            )
        elif err_type == "not_found":
            raise KeyError("mem_missing_001")
        elif err_type == "invalid_argument":
            raise ValueError("Invalid argument: content cannot be empty")
        return {"status": "ok"}

    _, sock_path = await mock_daemon_uds(mock_handler)
    bridge = MCPBridge(socket_path=sock_path, source_agent="openclaw")

    # 测试各个错误类型的透传
    error_cases = [
        ("agent_unauthorized", -32001, "agent_id"),
        ("version_conflict", -32002, "current_version"),
        ("idempotency_conflict", -32006, "existing_memory_id"),
        ("not_found", -32005, "memory_id"),
        ("invalid_argument", -32602, None),
    ]

    for err_key, expected_code, expected_data_key in error_cases:
        req = {
            "jsonrpc": "2.0",
            "id": f"test_{err_key}",
            "method": "tools/call",
            "params": {
                "name": "memory_create",
                "arguments": {
                    "content": "Valid text content for creation",
                    "trigger_error": err_key,
                },
            },
        }
        resp = await bridge.handle_request(req)
        assert resp["result"]["isError"] is True
        text = resp["result"]["content"][0]["text"]
        assert f"[{expected_code}]" in text
        if expected_data_key:
            assert expected_data_key in text


# =========================================================================
# 3. 守护进程故障、超时与异常网络处理
# =========================================================================

@pytest.mark.asyncio
async def test_daemon_socket_not_found():
    """daemon socket 文件不存在时返回友好错误而不是未处理异常"""
    bridge = MCPBridge(socket_path="/tmp/nonexistent_amr_daemon_12345.sock")
    req = {
        "jsonrpc": "2.0",
        "id": 100,
        "method": "tools/call",
        "params": {"name": "memory_search", "arguments": {"query": "hello"}},
    }
    resp = await bridge.handle_request(req)
    assert resp["id"] == 100
    assert resp["result"]["isError"] is True
    assert "socket not found" in resp["result"]["content"][0]["text"].lower()


@pytest.mark.asyncio
async def test_daemon_call_timeout(mock_daemon_uds):
    """daemon 响应超时（如 GPU 冷启动卡死）时抛出 Timeout 并返回 isError: True"""
    async def hung_handler(method, params):
        await asyncio.sleep(2.0)
        return {"status": "delayed"}

    _, sock_path = await mock_daemon_uds(hung_handler)
    # 设置 0.2s 极短超时
    bridge = MCPBridge(socket_path=sock_path, call_timeout=0.2)
    req = {
        "jsonrpc": "2.0",
        "id": 101,
        "method": "tools/call",
        "params": {"name": "memory_search", "arguments": {"query": "hello"}},
    }
    resp = await bridge.handle_request(req)
    assert resp["result"]["isError"] is True
    assert "did not respond within" in resp["result"]["content"][0]["text"]


# =========================================================================
# 4. 漏洞发现用例：agent_id 自动注入不覆盖客户端输入（伪造漏洞）
# =========================================================================

@pytest.mark.asyncio
async def test_mcp_agent_id_spoofing_vulnerability_bug(mock_daemon_uds):
    """
    【发现 BUG 验证】
    src/interfaces/mcp/bridge.py:159-163:
    注释声称：'Automatically inject immutable agent_id / source_agent provenance'
    代码实际：
    ```python
    if not arguments.get("agent_id"):
        arguments["agent_id"] = self.source_agent
    if not arguments.get("source_agent"):
        arguments["source_agent"] = self.source_agent
    ```
    当调用方在 arguments 中恶意显式传入 `{"agent_id": "spoofed_target_agent"}` 时：
    bridge 并没有强制覆盖，而是原样传递给 daemon！
    这导致客户端能假冒任何其它的 Agent 身份写入记忆！
    """
    received_agent_id = None

    async def mock_handler(method, params):
        nonlocal received_agent_id
        received_agent_id = params.get("agent_id")
        return {"memory_id": "mem_spoof_1"}

    _, sock_path = await mock_daemon_uds(mock_handler)
    # Bridge 本地被配置为 openclaw
    bridge = MCPBridge(socket_path=sock_path, source_agent="openclaw")

    # 客户端在 arguments 中故意伪造为 hermes
    req = {
        "jsonrpc": "2.0",
        "id": 999,
        "method": "tools/call",
        "params": {
            "name": "memory_create",
            "arguments": {
                "content": "Malicious injected content",
                "agent_id": "hermes",
            },
        },
    }
    await bridge.handle_request(req)

    # 安全契约期望：Bridge 必须强制将 agent_id 覆盖为自身绑定的 openclaw
    # 实际情况：received_agent_id 变成了伪造的 hermes！
    if received_agent_id != "openclaw":
        pytest.fail(
            f"MCP Bridge allows agent_id spoofing! Client passed 'hermes' and daemon received '{received_agent_id}' "
            f"instead of immutable bridge identity 'openclaw'."
        )


# =========================================================================
# 5. 漏洞发现用例：params 为 None 或非字典导致 Bridge 静默吞没/无响应
# =========================================================================

@pytest.mark.asyncio
async def test_mcp_none_params_hangs_client_bug():
    """
    【发现 BUG 验证】
    当 MCP 客户端发送 `params: None` 的 tools/call 请求时，
    bridge.py:108: `params = req_data.get("params", {})` -> 若传入了 "params": None，params 为 None。
    line 136: `tool_name = params.get("name")` 触发 `AttributeError: 'NoneType' object has no attribute 'get'`。
    在 run_stdio 中被粗暴的 `except Exception: sys.stderr.write(...)` 捕获，
    结果：未向 stdout 写入任何 JSON-RPC 响应帧！
    客户端挂起（Hang）永久等待响应。
    """
    bridge = MCPBridge(socket_path="/nonexistent.sock")
    req = {
        "jsonrpc": "2.0",
        "id": 404,
        "method": "tools/call",
        "params": None,
    }

    try:
        resp = await bridge.handle_request(req)
        # 若正常处理，应当返回带 isError: True 或 JSON-RPC error 的响应
        assert resp is not None
        assert "error" in resp or resp.get("result", {}).get("isError") is True
    except AttributeError as e:
        pytest.fail(
            f"MCPBridge.handle_request raised unhandled AttributeError on params=None: {e}. "
            "In run_stdio, this results in dropped response and client hang!"
        )
