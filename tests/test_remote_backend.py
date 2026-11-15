"""隔离测试远程 MCP 后端的工具名到 daemon RPC 方法映射。

本测试只注入内存中的异步 RPC 桩，不连接任何 daemon、socket 或生产数据库。
"""

from typing import Any, Dict, List, Mapping, Optional

import pytest

from src.interfaces.mcp.remote_backend import (
    LOCAL_ONLY_TOOLS,
    TOOL_RPC_METHODS,
    LocalOnlyToolError,
    RemoteBackend,
    UnknownToolError,
)
from src.interfaces.mcp.tools import TOOL_DEFINITIONS

# 与 src/interfaces/mcp/bridge.py 中 business tool -> daemon RPC 的映射保持一致。
EXPECTED_RPC_METHODS = {
    "memory_search": "memory.search",
    "memory_create": "memory.create",
    "memory_update": "memory.update",
    "memory_history": "memory.history",
    "memory_delete": "memory.delete",
    "memory_get": "memory.get",
    "memory_record": "memory.record",
    "memory_update_status": "memory.update_status",
    "memory_ingest_session": "session.ingest",
    "session_checkpoint": "session.checkpoint",
    "memory_session_checkpoint": "session.checkpoint",
    "session_get_handoff": "session.get_handoff",
    "memory_get_handoff": "session.get_handoff",
    "memory_candidates": "memory.candidates",
    "memory_approve_candidate": "memory.approve_candidate",
}


class RecordingRpc:
    """记录注入的 rpc(method, params) 调用，不执行任何 I/O。"""

    def __init__(self, result: Optional[Any] = None) -> None:
        self.result = {"ok": True} if result is None else result
        self.calls: List[tuple[str, Dict[str, Any]]] = []

    async def __call__(self, method: str, params: Mapping[str, Any]) -> Any:
        self.calls.append((method, dict(params)))
        return self.result


def _declared_mcp_tool_names() -> set[str]:
    return {
        entry["name"]
        for entry in TOOL_DEFINITIONS
        if isinstance(entry, Mapping) and isinstance(entry.get("name"), str)
    }


def test_business_tool_mapping_matches_bridge_and_excludes_local_only_tools() -> None:
    assert dict(TOOL_RPC_METHODS) == EXPECTED_RPC_METHODS
    assert set(TOOL_RPC_METHODS).isdisjoint(LOCAL_ONLY_TOOLS)

    declared = _declared_mcp_tool_names()
    assert set(TOOL_RPC_METHODS) <= declared
    assert declared - set(TOOL_RPC_METHODS) == set(LOCAL_ONLY_TOOLS)


@pytest.mark.asyncio
async def test_memory_search_maps_to_memory_search_rpc_without_mutating_arguments() -> None:
    rpc = RecordingRpc(result={"results": [], "total": 0})
    backend = RemoteBackend(rpc)
    arguments = {
        "query": "SM4 policy",
        "project_id": "project-a",
        "limit": 3,
        "agent_id": "gateway_agent",
        "source_agent": "gateway_agent",
    }
    original = dict(arguments)

    result = await backend.call("memory_search", arguments)

    assert result == {"results": [], "total": 0}
    assert rpc.calls == [("memory.search", original)]
    assert rpc.calls[0][1] is not arguments
    assert arguments == original


@pytest.mark.asyncio
async def test_memory_ingest_session_maps_to_session_ingest() -> None:
    rpc = RecordingRpc(result={"status": "accepted"})
    backend = RemoteBackend(rpc)
    arguments = {
        "session_id": "session-1",
        "project_id": "project-a",
        "messages": [
            {
                "message_id": "message-1",
                "role": "user",
                "content": "Remember this decision.",
            }
        ],
        "agent_id": "gateway_agent",
        "source_agent": "gateway_agent",
    }

    result = await backend("memory_ingest_session", arguments)

    assert result == {"status": "accepted"}
    assert rpc.calls == [("session.ingest", arguments)]


@pytest.mark.asyncio
async def test_session_checkpoint_maps_to_session_checkpoint() -> None:
    rpc = RecordingRpc(result={"checkpoint": {"checkpoint_id": "cp-1"}})
    backend = RemoteBackend(rpc)
    arguments = {
        "session_id": "session-1",
        "project_id": "project-a",
        "completed": ["implemented mapping"],
        "in_progress": ["review"],
        "next_steps": ["run integration tests"],
        "agent_id": "gateway_agent",
        "source_agent": "gateway_agent",
    }

    result = await backend.handle("session_checkpoint", arguments)

    assert result == {"checkpoint": {"checkpoint_id": "cp-1"}}
    assert rpc.calls == [("session.checkpoint", arguments)]


@pytest.mark.asyncio
async def test_memory_session_checkpoint_alias_maps_to_session_checkpoint() -> None:
    rpc = RecordingRpc()
    backend = RemoteBackend(rpc)

    await backend.call("memory_session_checkpoint", {"session_id": "s", "project_id": "p"})

    assert rpc.calls == [
        ("session.checkpoint", {"session_id": "s", "project_id": "p"})
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tool_name",
    ["unknown_tool", "memory.spool_status", "memory_spool_status ", ""],
)
async def test_unknown_tool_is_rejected_before_rpc(tool_name: str) -> None:
    rpc = RecordingRpc()
    backend = RemoteBackend(rpc)

    with pytest.raises(UnknownToolError):
        await backend.call(tool_name, {})

    assert rpc.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("tool_name", sorted(LOCAL_ONLY_TOOLS))
async def test_local_only_tools_are_rejected_before_rpc(tool_name: str) -> None:
    rpc = RecordingRpc()
    backend = RemoteBackend(rpc)

    with pytest.raises(LocalOnlyToolError):
        await backend(tool_name, {"agent_id": "gateway_agent"})

    assert rpc.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("params", [None, [], "query=x", 1, True])
async def test_non_mapping_params_are_rejected_before_rpc(params: Any) -> None:
    rpc = RecordingRpc()
    backend = RemoteBackend(rpc)

    with pytest.raises(ValueError):
        await backend.call("memory_search", params)

    assert rpc.calls == []
