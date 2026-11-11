"""
Lightweight Stdio MCP Server Bridge for AI Memory Runtime.
Communicates via standard JSON-RPC 2.0 stdio with LLM Agent,
and forwards requests to the background daemon via UDS (/run/user/1000/qdrant-bge.sock).

STRICT SAFETY:
- Memory footprint <= 20MB, startup time < 50ms.
- ZERO PyTorch / CUDA / transformers imports.
- Stdout strictly reserved for JSON-RPC messages; all logs to stderr.
"""

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

# Ensure project root is in sys.path before importing config
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from config.settings import load_config
from src.interfaces.ipc.protocol import (
    HEADER_STRUCT,
    make_jsonrpc_request,
    read_frame,
    write_frame,
)
from src.interfaces.mcp.tools import TOOL_DEFINITIONS, validate_tool_args

def _resolve_socket_path() -> str:
    """
    解析 daemon 业务 socket 路径，优先级：
    1. 环境变量 AMR_SOCKET_PATH（便于多实例/测试隔离）
    2. config.yaml 的 server.business_socket（与服务端单一事实来源对齐）
    3. /run/user/<uid>/qdrant-bge.sock（默认约定，兼容 uid != 1000）
    """
    env_path = os.environ.get("AMR_SOCKET_PATH")
    if env_path:
        return env_path
    try:
        configured = load_config().server.business_socket
        if configured:
            return configured
    except Exception:
        pass
    return f"/run/user/{os.getuid()}/qdrant-bge.sock"


DEFAULT_SOCKET_PATH = _resolve_socket_path()
DEFAULT_SOURCE_AGENT = os.environ.get("AMR_SOURCE_AGENT", "openclaw")
# RPC 调用上限：BGE-M3 冷启动加载可达 25s，无上限时 daemon 卡死会永久挂起宿主 Agent 的工具调用
DEFAULT_CALL_TIMEOUT_SECONDS = float(os.environ.get("AMR_BRIDGE_TIMEOUT_SECONDS", "120"))


class DaemonRpcError(Exception):
    """Encapsulates JSON-RPC error code, message, and data returned by the daemon."""

    def __init__(self, code: int, message: str, data: Optional[Any] = None) -> None:
        super().__init__(f"[{code}] {message}")
        self.code = code
        self.message = message
        self.data = data


class MCPBridge:
    def __init__(
        self,
        socket_path: str = DEFAULT_SOCKET_PATH,
        source_agent: str = DEFAULT_SOURCE_AGENT,
        call_timeout: float = DEFAULT_CALL_TIMEOUT_SECONDS,
    ):
        self.socket_path = socket_path
        self.source_agent = source_agent
        self.call_timeout = call_timeout

    async def _call_daemon(self, method: str, params: Dict[str, Any], req_id: Any) -> Dict[str, Any]:
        """Connect to UDS and dispatch RPC request to daemon."""
        if not os.path.exists(self.socket_path):
            raise ConnectionError(
                f"AMR daemon socket not found at '{self.socket_path}'. Ensure the daemon service is running."
            )

        reader, writer = await asyncio.open_unix_connection(self.socket_path)
        try:
            req_dict = make_jsonrpc_request(method=method, params=params, req_id=req_id)
            await write_frame(writer, req_dict)
            try:
                resp_dict = await asyncio.wait_for(read_frame(reader), timeout=self.call_timeout)
            except asyncio.TimeoutError:
                raise TimeoutError(
                    f"AMR daemon did not respond within {self.call_timeout:.0f}s for {method}"
                ) from None
            if "error" in resp_dict and resp_dict["error"]:
                err = resp_dict["error"]
                code = err.get("code", -32603)
                msg = err.get("message", "")
                data = err.get("data")
                raise DaemonRpcError(code=code, message=msg, data=data)
            return resp_dict.get("result", {})
        finally:
            writer.close()
            try:
                await asyncio.wait_for(writer.wait_closed(), timeout=5.0)
            except Exception:
                pass

    async def handle_request(self, req_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process incoming MCP JSON-RPC requests."""
        if not isinstance(req_data, dict):
            return {
                "jsonrpc": "2.0",
                "id": None,
                "error": {
                    "code": -32600,
                    "message": "Invalid Request: root payload must be an object",
                },
            }

        req_id = req_data.get("id")
        method = req_data.get("method")
        params = req_data.get("params")
        if params is None:
            params = {}
        elif not isinstance(params, dict):
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {
                    "code": -32602,
                    "message": f"INVALID_ARGUMENT: Parameter 'params' must be a dictionary, got {type(params).__name__}",
                },
            }

        # Handle MCP Lifecycle & Inspection
        if method == "initialize":
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {"tools": {}},
                    "serverInfo": {
                        "name": "ai-memory-runtime-mcp",
                        "version": "1.0.0"
                    }
                }
            }

        if method == "notifications/initialized":
            return None

        if method == "tools/list":
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"tools": TOOL_DEFINITIONS}
            }

        if method == "tools/call":
            tool_name = params.get("name")
            arguments = params.get("arguments")
            if arguments is None:
                arguments = {}
            elif not isinstance(arguments, dict):
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": "Error: Parameter 'arguments' must be a dictionary"}
                        ],
                        "isError": True,
                    },
                }

            try:
                validate_tool_args(tool_name, arguments)
                # Map MCP tool calls to Daemon RPC methods
                rpc_method_map = {
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

                rpc_method = rpc_method_map.get(tool_name)
                if not rpc_method:
                    raise ValueError(f"Unknown tool name: {tool_name}")

                # Validate caller identity and force immutable source_agent provenance (prevent spoofing)
                raw_agent = arguments.get("agent_id")
                if raw_agent is not None and str(raw_agent).strip():
                    cleaned_agent = str(raw_agent).strip().lower()
                    try:
                        cfg = load_config()
                        allowed_list = getattr(getattr(cfg, "server", None), "allowed_agents", [])
                        allowed_agents = {str(a).strip().lower() for a in allowed_list}
                    except Exception:
                        allowed_agents = set()
                    if allowed_agents and cleaned_agent not in allowed_agents:
                        raise DaemonRpcError(
                            code=-32001,
                            message=f"AGENT_UNAUTHORIZED: Unknown or untrusted agent_id '{cleaned_agent}'",
                            data={"agent_id": cleaned_agent},
                        )

                # Force immutable agent_id / source_agent provenance (prevent spoofing)
                arguments["agent_id"] = self.source_agent
                arguments["source_agent"] = self.source_agent

                res = await self._call_daemon(rpc_method, arguments, req_id)
                formatted_text = json.dumps(res, ensure_ascii=False, indent=2)

                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": formatted_text}
                        ],
                        "isError": False
                    }
                }
            except DaemonRpcError as e:
                sys.stderr.write(f"[AMR-Bridge] Daemon RPC error executing {tool_name}: {e}\n")
                lines = [f"[{e.code}] {e.message}"]
                if e.data is not None:
                    lines.append(json.dumps(e.data, ensure_ascii=False))
                error_text = "\n".join(lines)
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": error_text}
                        ],
                        "isError": True,
                    },
                }
            except Exception as e:
                sys.stderr.write(f"[AMR-Bridge] Error executing {tool_name}: {e}\n")
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": f"Error: {str(e)}"}
                        ],
                        "isError": True
                    }
                }

        # Fallback for unrecognized methods
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32601, "message": f"Method not found: {method}"}
        }

    async def run_stdio(self):
        """Standard IO main loop for MCP Agent communication."""
        loop = asyncio.get_running_loop()
        # F3-2: 显式设置 reader 上限为 config.server.max_request_bytes（默认 4MB）。
        # 不传 limit 时 asyncio 默认 64KB，超长单条 JSON-RPC（如会话批量摄取）会抛 LimitOverrunError
        max_request_bytes = load_config().server.max_request_bytes
        reader = asyncio.StreamReader(limit=max_request_bytes)
        protocol = asyncio.StreamReaderProtocol(reader)
        await loop.connect_read_pipe(lambda: protocol, sys.stdin)

        while True:
            line = await reader.readline()
            if not line:
                break
            line_str = line.decode("utf-8").strip()
            if not line_str:
                continue

            try:
                req_data = json.loads(line_str)
                resp = await self.handle_request(req_data)
                if resp is not None:
                    out = json.dumps(resp, ensure_ascii=False) + "\n"
                    sys.stdout.write(out)
                    sys.stdout.flush()
            except Exception as err:
                sys.stderr.write(f"[AMR-Bridge] stdio parse error: {err}\n")


if __name__ == "__main__":
    bridge = MCPBridge()
    try:
        asyncio.run(bridge.run_stdio())
    except KeyboardInterrupt:
        pass
