"""测试 IPC 错误映射契约（UDSServer._handle_client）。

重点断言：
1. RpcError 首位直通，不被 ValueError 分支吞掉；
2. VersionConflictError -> -32002，且 data 包含 {memory_id, current_version, expected_version}；
3. ModelLoadingPendingError -> -32003，且 data 包含 {model_state, retry_after_ms}；
4. QueueFullError -> -32004，且 data 包含 {active_requests, max_queue_size}；
5. KeyError -> -32005，且 data 包含 {memory_id}；
6. IdempotencyConflict -> -32006，且 data 包含 {op, idem_key, existing_memory_id}；
7. LegacyPathForbidden / InvalidStatusError -> -32602，携带可诊断 data；
8. 历史前缀 ValueError：AGENT_UNAUTHORIZED -> -32001，VERSION_CONFLICT -> -32002，NOT_FOUND -> -32005，其余 -> -32602；
9. 未知 Exception -> -32603 INTERNAL。
"""
import asyncio
import os
import pytest
from pathlib import Path

from src.core.errors import (
    IdempotencyConflict,
    InvalidStatusError,
    LegacyPathForbidden,
    ModelLoadingPendingError,
    VersionConflictError,
)
from src.core.engine import QueueFullError
from src.interfaces.ipc.errors import (
    RpcAgentUnauthorized,
    RpcError,
    RpcIdempotencyConflict,
    RpcInvalidArgument,
    RpcModelLoading,
    RpcNotFound,
    RpcQueueFull,
    RpcVersionConflict,
)
from src.interfaces.ipc.protocol import (
    make_jsonrpc_request,
    read_frame,
    write_frame,
)
from src.interfaces.ipc.server import UDSServer


@pytest.fixture
async def uds_server_factory(tmp_path):
    server_instances = []

    async def _create(handler):
        sock_path = str(tmp_path / f"test_err_{len(server_instances)}.sock")
        server = UDSServer(
            name="test-err",
            socket_path=sock_path,
            handler=handler,
        )
        await server.start()
        server_instances.append(server)
        return server, sock_path

    yield _create

    for s in server_instances:
        await s.stop()


async def _rpc_call(sock_path: str, method: str, params=None, req_id=1):
    reader, writer = await asyncio.open_unix_connection(sock_path)
    try:
        req = make_jsonrpc_request(method, params, req_id=req_id)
        await write_frame(writer, req)
        resp = await read_frame(reader)
        return resp
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_rpc_error_passthrough_not_swallowed_by_valueerror(uds_server_factory):
    """RpcError（继承 ValueError）必须首位直通，保留自定义 code 与 data，不被通用 ValueError 吞成 -32602。"""
    class CustomRpcError(RpcError):
        code = -32099
        error_code = "CUSTOM_ERROR"

    async def handler(method, params):
        if method == "raise_custom":
            raise CustomRpcError("Custom failure message", data={"extra_key": "val123"})
        elif method == "raise_auth":
            raise RpcAgentUnauthorized("Agent unauthorized test", data={"agent_id": "test_bot"})
        elif method == "raise_conflict":
            raise RpcVersionConflict("Conflict occurred", data={"memory_id": "m1", "current_version": 2})

    _, sock = await uds_server_factory(handler)

    resp1 = await _rpc_call(sock, "raise_custom", req_id=101)
    assert resp1["id"] == 101
    assert "error" in resp1
    assert resp1["error"]["code"] == -32099
    assert "CUSTOM_ERROR" in resp1["error"]["message"]
    assert resp1["error"]["data"] == {"extra_key": "val123"}

    resp2 = await _rpc_call(sock, "raise_auth", req_id=102)
    assert resp2["error"]["code"] == -32001
    assert "AGENT_UNAUTHORIZED" in resp2["error"]["message"]
    assert resp2["error"]["data"] == {"agent_id": "test_bot"}

    resp3 = await _rpc_call(sock, "raise_conflict", req_id=103)
    assert resp3["error"]["code"] == -32002
    assert "VERSION_CONFLICT" in resp3["error"]["message"]
    assert resp3["error"]["data"]["current_version"] == 2


@pytest.mark.asyncio
async def test_version_conflict_error_mapping(uds_server_factory):
    """core.errors.VersionConflictError 映射为 -32002，完整保留 data 字段。"""
    async def handler(method, params):
        raise VersionConflictError(
            "Version mismatch",
            memory_id="mem_abc_1",
            current_version=3,
            expected_version=2,
        )

    _, sock = await uds_server_factory(handler)
    resp = await _rpc_call(sock, "memory.update", req_id=201)

    assert resp["id"] == 201
    err = resp["error"]
    assert err["code"] == -32002
    assert "VERSION_CONFLICT" in err["message"]
    assert err["data"] == {
        "memory_id": "mem_abc_1",
        "current_version": 3,
        "expected_version": 2,
    }


@pytest.mark.asyncio
async def test_model_loading_pending_error_mapping(uds_server_factory):
    """core.errors.ModelLoadingPendingError 映射为 -32003 MODEL_LOADING。"""
    async def handler(method, params):
        raise ModelLoadingPendingError(
            "Model is warming up",
            model_state="LOADING",
            retry_after_ms=2500,
        )

    _, sock = await uds_server_factory(handler)
    resp = await _rpc_call(sock, "memory.search", req_id=301)

    assert resp["id"] == 301
    err = resp["error"]
    assert err["code"] == -32003
    assert "MODEL_LOADING" in err["message"]
    assert err["data"]["model_state"] == "LOADING"
    assert err["data"]["retry_after_ms"] == 2500


@pytest.mark.asyncio
async def test_queue_full_error_mapping(uds_server_factory):
    """engine.QueueFullError 映射为 -32004 QUEUE_FULL，自动提取 active_requests / max_queue_size。"""
    async def handler(method, params):
        raise QueueFullError("Inference queue full (64/64)")

    _, sock = await uds_server_factory(handler)
    resp = await _rpc_call(sock, "embedding.generate", req_id=401)

    assert resp["id"] == 401
    err = resp["error"]
    assert err["code"] == -32004
    assert "QUEUE_FULL" in err["message"]
    assert err["data"]["active_requests"] == 64
    assert err["data"]["max_queue_size"] == 64


@pytest.mark.asyncio
async def test_key_error_not_found_mapping(uds_server_factory):
    """KeyError 映射为 -32005 NOT_FOUND，data 携带 memory_id。"""
    async def handler(method, params):
        raise KeyError("mem_missing_999")

    _, sock = await uds_server_factory(handler)
    resp = await _rpc_call(sock, "memory.get", req_id=501)

    assert resp["id"] == 501
    err = resp["error"]
    assert err["code"] == -32005
    assert "NOT_FOUND" in err["message"]
    assert err["data"] == {"memory_id": "mem_missing_999"}


@pytest.mark.asyncio
async def test_idempotency_conflict_mapping(uds_server_factory):
    """IdempotencyConflict 映射为 -32006 IDEMPOTENCY_CONFLICT。"""
    async def handler(method, params):
        raise IdempotencyConflict(
            op="create",
            idem_key="key_replay_xyz",
            existing_memory_id="mem_existing_888",
        )

    _, sock = await uds_server_factory(handler)
    resp = await _rpc_call(sock, "memory.create", req_id=601)

    assert resp["id"] == 601
    err = resp["error"]
    assert err["code"] == -32006
    assert "IDEMPOTENCY_CONFLICT" in err["message"]
    assert err["data"] == {
        "op": "create",
        "idem_key": "key_replay_xyz",
        "existing_memory_id": "mem_existing_888",
    }


@pytest.mark.asyncio
async def test_legacy_path_forbidden_mapping(uds_server_factory):
    """LegacyPathForbidden 映射为 -32602 INVALID_ARGUMENT，携带可诊断字段。"""
    async def handler(method, params):
        raise LegacyPathForbidden(
            memory_id="mem_legacy_001",
            projection_class="legacy",
            hint="legacy 记忆不支持正文变更",
        )

    _, sock = await uds_server_factory(handler)
    resp = await _rpc_call(sock, "memory.update", req_id=701)

    assert resp["id"] == 701
    err = resp["error"]
    assert err["code"] == -32602
    assert "INVALID_ARGUMENT" in err["message"]
    assert err["data"]["memory_id"] == "mem_legacy_001"
    assert err["data"]["projection_class"] == "legacy"
    assert "legacy 记忆不支持正文变更" in err["data"]["hint"]


@pytest.mark.asyncio
async def test_invalid_status_error_mapping(uds_server_factory):
    """InvalidStatusError 映射为 -32602 INVALID_ARGUMENT。"""
    async def handler(method, params):
        raise InvalidStatusError(
            "Status not recognized",
            value="UNKNOWN_STATE",
            allowed=["ACTIVE", "HISTORICAL", "DELETED"],
        )

    _, sock = await uds_server_factory(handler)
    resp = await _rpc_call(sock, "memory.create", req_id=702)

    assert resp["id"] == 702
    err = resp["error"]
    assert err["code"] == -32602
    assert err["data"]["value"] == "UNKNOWN_STATE"
    assert "ACTIVE" in err["data"]["allowed"]


@pytest.mark.asyncio
async def test_legacy_valueerror_prefix_mapping(uds_server_factory):
    """其它普通 ValueError 根据消息前缀精准映射为历史兼容业务码。"""
    async def handler(method, params):
        if method == "auth_prefix":
            raise ValueError("AGENT_UNAUTHORIZED: Untrusted agent 'bad_agent'")
        elif method == "conflict_prefix":
            raise ValueError("VERSION_CONFLICT: Expected 1 but found 2")
        elif method == "notfound_prefix":
            raise ValueError("NOT_FOUND: Item mem_123 does not exist")
        else:
            raise ValueError("Parameter 'query' cannot be blank")

    _, sock = await uds_server_factory(handler)

    r1 = await _rpc_call(sock, "auth_prefix", req_id=801)
    assert r1["error"]["code"] == -32001

    r2 = await _rpc_call(sock, "conflict_prefix", req_id=802)
    assert r2["error"]["code"] == -32002

    r3 = await _rpc_call(sock, "notfound_prefix", req_id=803)
    assert r3["error"]["code"] == -32005

    r4 = await _rpc_call(sock, "other_value_error", req_id=804)
    assert r4["error"]["code"] == -32602
    assert "INVALID_ARGUMENT" in r4["error"]["message"]


@pytest.mark.asyncio
async def test_unexpected_exception_internal_mapping(uds_server_factory):
    """未捕获的常规 Exception 统一兜底为 -32603 INTERNAL。"""
    async def handler(method, params):
        raise RuntimeError("Disk read failure or database locked")

    _, sock = await uds_server_factory(handler)
    resp = await _rpc_call(sock, "system.status", req_id=901)

    assert resp["id"] == 901
    err = resp["error"]
    assert err["code"] == -32603
    assert "Internal error" in err["message"]
    assert "Disk read failure" in err["message"]
