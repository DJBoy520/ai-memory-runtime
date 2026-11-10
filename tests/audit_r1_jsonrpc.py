"""
AMR v5 Audit Track R1: JSON-RPC 协议层与异常映射审计测试
覆盖攻击面：
1. 缺 method / method 非字符串 / params 为 None、字符串、数组；id 为 null/超大数字/对象
2. 非法 JSON、截断帧、声明长度与实际不符、超大帧（>max_request_bytes）、根 payload 为非对象（list/scalar）
3. 单连接连续多个请求、响应写失败/客户端提前断开、慢客户端
4. 未知 method；异常映射是否会吞掉 RpcError；ValueError 前缀识别是否误判；双重前缀缺陷
"""

import asyncio
import json
import struct
import pytest
from pathlib import Path
from typing import Any, Dict

from src.core.errors import (
    IdempotencyConflict,
    InvalidStatusError,
    LegacyPathForbidden,
    ModelLoadingPendingError,
    VersionConflictError,
)
from src.interfaces.ipc.errors import (
    RpcAgentUnauthorized,
    RpcError,
    RpcIdempotencyConflict,
    RpcInvalidArgument,
    RpcNotFound,
)
from src.interfaces.ipc.protocol import (
    HEADER_STRUCT,
    MAX_REQUEST_BYTES,
    encode_frame,
    make_jsonrpc_request,
    read_frame,
    write_frame,
)
from src.interfaces.ipc.server import UDSServer


@pytest.fixture
async def uds_server_fixture(tmp_path):
    server_instances = []

    async def _start(handler, max_bytes=MAX_REQUEST_BYTES):
        sock_path = str(tmp_path / f"uds_{len(server_instances)}.sock")
        srv = UDSServer(
            name=f"test-uds-{len(server_instances)}",
            socket_path=sock_path,
            handler=handler,
            max_request_bytes=max_bytes,
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


async def _send_raw_bytes_and_read(sock_path: str, raw_data: bytes, timeout: float = 2.0) -> bytes:
    reader, writer = await asyncio.open_unix_connection(sock_path)
    try:
        writer.write(raw_data)
        await writer.drain()
        resp_data = await asyncio.wait_for(reader.read(65536), timeout=timeout)
        return resp_data
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


async def _send_frame_and_read(sock_path: str, payload: Any, timeout: float = 2.0) -> Dict[str, Any]:
    reader, writer = await asyncio.open_unix_connection(sock_path)
    try:
        await write_frame(writer, payload)
        resp = await asyncio.wait_for(read_frame(reader), timeout=timeout)
        return resp
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


# =========================================================================
# 1. 协议基础字段畸变 (method / params / id)
# =========================================================================

@pytest.mark.asyncio
async def test_missing_method_returns_invalid_request(uds_server_fixture):
    """请求缺少 method 字段时返回 -32600 Invalid Request"""
    async def mock_handler(method, params):
        return {"status": "ok"}

    _, sock_path = await uds_server_fixture(mock_handler)
    req = {"jsonrpc": "2.0", "id": 101}
    resp = await _send_frame_and_read(sock_path, req)
    assert resp["jsonrpc"] == "2.0"
    assert resp["id"] == 101
    assert resp["error"]["code"] == -32600
    assert "missing method" in resp["error"]["message"].lower()


@pytest.mark.asyncio
async def test_non_string_method_returns_invalid_request(uds_server_fixture):
    """method 非字符串（整数/列表/对象）时返回 -32600 Invalid Request"""
    async def mock_handler(method, params):
        return {"status": "ok"}

    _, sock_path = await uds_server_fixture(mock_handler)
    for bad_method in [12345, ["ping"], {"name": "ping"}, False, None]:
        req = {"jsonrpc": "2.0", "method": bad_method, "id": 102}
        resp = await _send_frame_and_read(sock_path, req)
        assert resp["error"]["code"] == -32600
        assert resp["id"] == 102


@pytest.mark.asyncio
async def test_empty_string_method_returns_invalid_request(uds_server_fixture):
    """method 为空字符串时返回 -32600 Invalid Request"""
    async def mock_handler(method, params):
        return {"status": "ok"}

    _, sock_path = await uds_server_fixture(mock_handler)
    req = {"jsonrpc": "2.0", "method": "", "id": 103}
    resp = await _send_frame_and_read(sock_path, req)
    assert resp["error"]["code"] == -32600
    assert resp["id"] == 103


@pytest.mark.asyncio
async def test_params_none_or_missing(uds_server_fixture):
    """params 为 None 或未提供时正常调用 handler 并传递 None"""
    received_params = []

    async def mock_handler(method, params):
        received_params.append(params)
        return {"status": "ok"}

    _, sock_path = await uds_server_fixture(mock_handler)
    req1 = {"jsonrpc": "2.0", "method": "test.ping", "id": 1}
    resp1 = await _send_frame_and_read(sock_path, req1)
    assert resp1["result"] == {"status": "ok"}
    assert received_params[0] is None

    req2 = {"jsonrpc": "2.0", "method": "test.ping", "params": None, "id": 2}
    resp2 = await _send_frame_and_read(sock_path, req2)
    assert resp2["result"] == {"status": "ok"}
    assert received_params[1] is None


@pytest.mark.asyncio
async def test_id_null_huge_int_and_object(uds_server_fixture):
    """验证 id 字段支持 null、超大整数、复杂对象与数组透传"""
    async def mock_handler(method, params):
        return {"echo": method}

    _, sock_path = await uds_server_fixture(mock_handler)

    # 1. id 为 None (null)
    req_null = {"jsonrpc": "2.0", "method": "test.echo", "id": None}
    resp_null = await _send_frame_and_read(sock_path, req_null)
    assert resp_null["id"] is None
    assert resp_null["result"]["echo"] == "test.echo"

    # 2. id 为超大整数 (超过 64 位 uint64)
    huge_id = 10**35 + 7
    req_huge = {"jsonrpc": "2.0", "method": "test.echo", "id": huge_id}
    resp_huge = await _send_frame_and_read(sock_path, req_huge)
    assert resp_huge["id"] == huge_id

    # 3. id 为对象
    obj_id = {"session": "sess_123", "seq": 999}
    req_obj = {"jsonrpc": "2.0", "method": "test.echo", "id": obj_id}
    resp_obj = await _send_frame_and_read(sock_path, req_obj)
    assert resp_obj["id"] == obj_id


# =========================================================================
# 2. 帧编解码与畸变攻击面
# =========================================================================

@pytest.mark.asyncio
async def test_malformed_json_returns_parse_error(uds_server_fixture):
    """发送非法 JSON 字符串时返回 -32700 Parse error"""
    async def mock_handler(method, params):
        return {}

    _, sock_path = await uds_server_fixture(mock_handler)
    bad_json = b"{'invalid_json': true,"  # 单引号与未闭合
    frame = HEADER_STRUCT.pack(len(bad_json)) + bad_json
    raw_resp = await _send_raw_bytes_and_read(sock_path, frame)
    assert len(raw_resp) >= 4
    (resp_len,) = HEADER_STRUCT.unpack(raw_resp[:4])
    resp_json = json.loads(raw_resp[4:4+resp_len].decode("utf-8"))
    assert resp_json["error"]["code"] == -32700
    assert "parse error" in resp_json["error"]["message"].lower()


@pytest.mark.asyncio
async def test_declared_length_zero_returns_parse_error(uds_server_fixture):
    """声明长度为 0 时抛出 FrameDecodeError 并返回 -32700 Parse error"""
    async def mock_handler(method, params):
        return {}

    _, sock_path = await uds_server_fixture(mock_handler)
    frame = HEADER_STRUCT.pack(0)
    raw_resp = await _send_raw_bytes_and_read(sock_path, frame)
    assert len(raw_resp) >= 4
    (resp_len,) = HEADER_STRUCT.unpack(raw_resp[:4])
    resp_json = json.loads(raw_resp[4:4+resp_len].decode("utf-8"))
    assert resp_json["error"]["code"] == -32700


@pytest.mark.asyncio
async def test_frame_declared_too_large_rejected(uds_server_fixture):
    """声明包头超出 max_request_bytes 时返回 -32600 并断开连接"""
    async def mock_handler(method, params):
        return {}

    # 设置较小的限制便于测试（1024 字节）
    _, sock_path = await uds_server_fixture(mock_handler, max_bytes=1024)
    # 声明 2048 字节
    frame_hdr = HEADER_STRUCT.pack(2048)
    reader, writer = await asyncio.open_unix_connection(sock_path)
    try:
        writer.write(frame_hdr + b"X" * 10)
        await writer.drain()
        resp = await read_frame(reader, max_bytes=4096)
        assert resp["error"]["code"] == -32600
        assert "frame too large" in resp["error"]["message"].lower()

        # 服务端应当断开连接
        eof_bytes = await reader.read(10)
        assert len(eof_bytes) == 0
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


@pytest.mark.asyncio
async def test_notification_replied_violating_jsonrpc_spec_bug(uds_server_fixture):
    """
    【发现 BUG 验证】
    RFC JSON-RPC 2.0 §4.1:
    'A Notification is a Request object without an 'id' member. The Server MUST NOT reply to a Notification.'
    AMR 实现中，缺少 id 时 req.get('id') 返回 None，UDSServer 仍然执行并发送了 `{"jsonrpc": "2.0", "result": ..., "id": null}`。
    """
    async def mock_handler(method, params):
        return {"status": "acked"}

    _, sock_path = await uds_server_fixture(mock_handler)
    # 不带 id 的 Notification 请求
    req = {"jsonrpc": "2.0", "method": "notify.event", "params": {"event": "hello"}}

    reader, writer = await asyncio.open_unix_connection(sock_path)
    try:
        await write_frame(writer, req)
        # 尝试读取，若在 0.3s 内读到了响应，说明违反了规范发送了响应
        try:
            resp = await asyncio.wait_for(read_frame(reader), timeout=0.3)
            pytest.fail(f"UDSServer replied to a Notification request (MUST NOT reply per JSON-RPC 2.0 §4.1): {resp}")
        except asyncio.TimeoutError:
            # 正常：没有响应
            pass
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


@pytest.mark.asyncio
async def test_incomplete_header_and_incomplete_payload(uds_server_fixture):
    """测试客户端提前断开（包头不全、payload 不全）时服务端优雅关闭无挂死"""
    async def mock_handler(method, params):
        return {}

    _, sock_path = await uds_server_fixture(mock_handler)

    # 1. 包头仅发送 2 字节即断开
    reader1, writer1 = await asyncio.open_unix_connection(sock_path)
    writer1.write(b"\x00\x00")
    await writer1.drain()
    writer1.close()
    await writer1.wait_closed()

    # 2. 声明 100 字节但仅发送 10 字节即断开
    reader2, writer2 = await asyncio.open_unix_connection(sock_path)
    writer2.write(HEADER_STRUCT.pack(100) + b"1234567890")
    await writer2.drain()
    writer2.close()
    await writer2.wait_closed()

    # 3. 验证服务端仍然可用
    resp = await _send_frame_and_read(sock_path, {"jsonrpc": "2.0", "method": "ping", "id": 99})
    assert resp["result"] == {}


# =========================================================================
# 3. 漏洞发现用例：非字典 JSON-RPC Request 导致服务端崩溃断连
# =========================================================================

@pytest.mark.asyncio
async def test_non_dict_json_payload_connection_crash_bug(uds_server_fixture):
    """
    【发现 BUG 验证】
    当客户端发送合法 JSON 但根不是字典（如 `[1, 2, 3]`、`"hello"`、`123`、`null`）时：
    RFC JSON-RPC 2.0 规范：若不是有效 Request 对象，必须返回 -32600 Invalid Request 错误。
    AMR 当前实现：server.py:88 `req.get('id')` 直接在 list/str/int 上调用 AttributeError，
    导致连接被意外关闭，客户端无法获得任何 JSON-RPC 响应！
    """
    async def mock_handler(method, params):
        return {}

    _, sock_path = await uds_server_fixture(mock_handler)
    raw_payload = json.dumps([1, 2, 3]).encode("utf-8")
    frame = HEADER_STRUCT.pack(len(raw_payload)) + raw_payload

    reader, writer = await asyncio.open_unix_connection(sock_path)
    try:
        writer.write(frame)
        await writer.drain()

        # 标准期望：得到 -32600 Invalid Request 响应帧
        # 实际情况：UDSServer 崩溃抛 AttributeError 并直接 close writer，导致此处抛 ConnectionClosedError 或 EOF
        try:
            resp = await asyncio.wait_for(read_frame(reader), timeout=1.0)
            assert resp["error"]["code"] == -32600
        except Exception as e:
            pytest.fail(
                f"UDSServer crashed/closed connection on non-dict payload without returning -32600 Invalid Request: "
                f"{type(e).__name__} ({e})"
            )
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


# =========================================================================
# 4. 连接生命周期与高压连续请求
# =========================================================================

@pytest.mark.asyncio
async def test_single_connection_sequential_pipelined_requests(uds_server_fixture):
    """单连接上连续发送多个请求，服务端必须严格按序响应"""
    async def mock_handler(method, params):
        return {"seq": params.get("seq")}

    _, sock_path = await uds_server_fixture(mock_handler)
    reader, writer = await asyncio.open_unix_connection(sock_path)
    try:
        for seq in range(10):
            req = {"jsonrpc": "2.0", "method": "echo", "params": {"seq": seq}, "id": seq}
            await write_frame(writer, req)
            resp = await read_frame(reader)
            assert resp["id"] == seq
            assert resp["result"]["seq"] == seq
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


@pytest.mark.asyncio
async def test_client_early_disconnect_during_handler(uds_server_fixture):
    """客户端在 handler 执行中或刚发送后立即断开，服务端写响应捕获 BrokenPipeError 并不崩溃"""
    handler_started = asyncio.Event()

    async def slow_handler(method, params):
        handler_started.set()
        await asyncio.sleep(0.2)
        return {"status": "done"}

    _, sock_path = await uds_server_fixture(slow_handler)
    reader, writer = await asyncio.open_unix_connection(sock_path)
    req = {"jsonrpc": "2.0", "method": "slow", "id": 1}
    await write_frame(writer, req)
    await handler_started.wait()

    # 客户端在服务端睡眠中立即关闭连接
    writer.close()
    await writer.wait_closed()

    # 验证后续新连接正常工作
    resp = await _send_frame_and_read(sock_path, {"jsonrpc": "2.0", "method": "ping", "id": 2})
    assert resp["id"] == 2


# =========================================================================
# 5. 未知 Method 与 错误码契约审计
# =========================================================================

@pytest.mark.asyncio
async def test_unknown_method_error_code_inconsistency_bug(uds_server_fixture):
    """
    【发现 BUG 验证】
    RFC JSON-RPC 2.0 规范：未知方法必须返回 -32601 (Method not found)。
    MCP Bridge 内部实现：返回 -32601。
    AMR main.dispatch_business_rpc: `raise ValueError(f"Unknown business method: {method}")`
    经 UDSServer._map_exception_to_error_response 映射时：
    因不是 AGENT_UNAUTHORIZED/VERSION_CONFLICT/NOT_FOUND，落入 else 默认返回 -32602 (INVALID_ARGUMENT)！
    """
    from src.main import AMRApplication
    from tests.audit_r1_auth import DummyEngine, DummyQdrant
    from src.core.session_store import SessionStore

    # 使用进程内轻量实例测试 dispatch_business_rpc
    app = AMRApplication(
        engine=DummyEngine(),
        qdrant_manager=DummyQdrant(),
        session_store=SessionStore(db_path=":memory:"),
    )

    _, sock_path = await uds_server_fixture(app.dispatch_business_rpc)
    req = {
        "jsonrpc": "2.0",
        "method": "completely.nonexistent.method",
        "params": {"agent_id": "system"},
        "id": 888,
    }
    resp = await _send_frame_and_read(sock_path, req)

    # 标准 JSON-RPC 2.0 要求 -32601
    # AMR 目前返回 -32602
    if resp["error"]["code"] != -32601:
        pytest.fail(
            f"Unknown method returned code {resp['error']['code']} (expected -32601 Method not found). "
            f"Message: '{resp['error']['message']}'"
        )


@pytest.mark.asyncio
async def test_rpc_error_duplicate_prefix_bug(uds_server_fixture):
    """
    【发现 BUG 验证】
    src/interfaces/ipc/errors.py RpcError.to_jsonrpc_error:
    `message = f"{self.error_code}: {self.message}"`
    而在 auth.py 中抛出：
    `RpcAgentUnauthorized(f"AGENT_UNAUTHORIZED: Unknown or untrusted agent_id '{agent_id}'", ...)`
    导致最终响应消息为：
    `AGENT_UNAUTHORIZED: AGENT_UNAUTHORIZED: Unknown or untrusted agent_id ...` (双重前缀重复)
    """
    async def mock_handler(method, params):
        raise RpcAgentUnauthorized(
            "AGENT_UNAUTHORIZED: Unknown or untrusted agent_id 'bad_agent'",
            data={"agent_id": "bad_agent"},
        )

    _, sock_path = await uds_server_fixture(mock_handler)
    req = {"jsonrpc": "2.0", "method": "test.auth", "id": 555}
    resp = await _send_frame_and_read(sock_path, req)

    msg = resp["error"]["message"]
    if msg.startswith("AGENT_UNAUTHORIZED: AGENT_UNAUTHORIZED:"):
        pytest.fail(
            f"Found duplicate error code prefix in JSON-RPC error message: '{msg}'"
        )


@pytest.mark.asyncio
async def test_non_dict_params_causes_internal_error_bug(uds_server_fixture):
    """
    【发现 BUG 验证】
    当客户端传入非 dict 的 params（例如 params=["foo"] 或 params="string"）时，
    main.py:114 `c_ref = params.get(...)` 抛出 AttributeError，
    UDSServer 将其映射为 -32603 Internal error: 'list' object has no attribute 'get'，
    既泄露了内部实现，又违背了参数非法应返回 -32602 (Invalid params) 的规范。
    """
    from src.main import AMRApplication
    from tests.audit_r1_auth import DummyEngine, DummyQdrant
    from src.core.session_store import SessionStore

    app = AMRApplication(
        engine=DummyEngine(),
        qdrant_manager=DummyQdrant(),
        session_store=SessionStore(db_path=":memory:"),
    )

    _, sock_path = await uds_server_fixture(app.dispatch_business_rpc)
    req = {
        "jsonrpc": "2.0",
        "method": "ping",
        "params": ["malformed_param_list"],
        "id": 777,
    }
    resp = await _send_frame_and_read(sock_path, req)

    if resp["error"]["code"] == -32603:
        pytest.fail(
            f"Non-dict params resulted in code -32603 Internal error instead of -32602 Invalid params: "
            f"message='{resp['error']['message']}'"
        )
