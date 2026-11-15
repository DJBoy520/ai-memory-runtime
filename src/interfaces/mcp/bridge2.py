"""2.x stdio MCP shim.

面向只支持 stdio MCP 的客户端（ZCode、DSH MCP client 等）：把标准 MCP 工具
调用转发到本地 `amr2-adapter` 的 UDS。身份由适配器按 socket 绑定，本模块
不读取生产配置、不注入 `agent_id`/`source_agent`、不提供 admin/spool 能力。

与 1.x `bridge.py` 的区别：
- socket 默认指向 `amr2-<agent>.sock`，而不是 daemon 业务 socket；
- 不访问 `config.settings`，不读取生产 allowed_agents；
- 不包含本机 spool 管理工具；
- 适配器返回的 JSON-RPC 错误保持 code/message/data，转换为 MCP 工具错误。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.interfaces.ipc.protocol import (  # noqa: E402
    MAX_REQUEST_BYTES,
    ConnectionClosedError,
    FrameDecodeError,
    FrameTooLargeError,
    make_jsonrpc_request,
    read_frame,
    write_frame,
)
from src.interfaces.mcp.remote_backend import (  # noqa: E402
    LOCAL_ONLY_TOOLS,
    TOOL_RPC_METHODS,
)
from src.interfaces.mcp.tools import TOOL_DEFINITIONS, validate_tool_args  # noqa: E402

DEFAULT_CALL_TIMEOUT_SECONDS = float(
    os.environ.get("AMR2_BRIDGE_TIMEOUT_SECONDS", "120")
)
_SERVER_NAME = "ai-memory-runtime-mcp2"
_SERVER_VERSION = "2.0.0"
_PROTOCOL_VERSION = "2024-11-05"


def resolve_socket_path(explicit: Optional[str] = None) -> str:
    """解析适配器 UDS 路径：显式参数 > AMR2_SOCKET_PATH > XDG 默认。"""
    if explicit and explicit.strip():
        return explicit.strip()
    env_path = os.environ.get("AMR2_SOCKET_PATH")
    if env_path and env_path.strip():
        return env_path.strip()
    agent = re.sub(r"[^A-Za-z0-9_-]", "", os.environ.get("AMR2_AGENT", "mcp"))
    agent = agent or "mcp"
    xdg = os.environ.get("XDG_RUNTIME_DIR")
    if xdg and xdg.strip():
        return f"{xdg.strip()}/amr2-{agent}.sock"
    if sys.platform.startswith("linux") and hasattr(os, "getuid"):
        return f"/run/user/{os.getuid()}/amr2-{agent}.sock"
    return f"{tempfile.gettempdir()}/amr2-{agent}.sock"


class AdapterRpcError(Exception):
    """适配器返回的 JSON-RPC 错误（保留 code/message/data）。"""

    def __init__(self, code: int, message: str, data: Any = None) -> None:
        super().__init__(f"[{code}] {message}")
        self.code = code
        self.message = message
        self.data = data


def _remote_tools() -> list[dict]:
    """只暴露适配器可路由的业务工具，不暴露本机专用工具。"""
    return [
        definition
        for definition in TOOL_DEFINITIONS
        if definition.get("name") in TOOL_RPC_METHODS
        and definition.get("name") not in LOCAL_ONLY_TOOLS
    ]


class AdapterBridge:
    def __init__(
        self,
        socket_path: Optional[str] = None,
        call_timeout: float = DEFAULT_CALL_TIMEOUT_SECONDS,
        max_request_bytes: int = MAX_REQUEST_BYTES,
    ) -> None:
        self.socket_path = resolve_socket_path(socket_path)
        self.call_timeout = call_timeout
        self.max_request_bytes = max_request_bytes
        self._request_id = 0

    def _next_id(self) -> int:
        self._request_id += 1
        return self._request_id

    async def _call_adapter(
        self, method: str, params: Dict[str, Any], req_id: Any
    ) -> Any:
        # 不先做 exists 检查：直接连接，避免检查/打开之间的 TOCTOU；
        # 缺失/非 socket 由 open_unix_connection 抛出 OSError。
        reader, writer = await asyncio.wait_for(
            asyncio.open_unix_connection(self.socket_path),
            timeout=self.call_timeout,
        )
        try:
            request = make_jsonrpc_request(method, params, req_id=req_id)
            await write_frame(writer, request, max_bytes=self.max_request_bytes)
            response = await asyncio.wait_for(
                read_frame(reader, max_bytes=self.max_request_bytes),
                timeout=self.call_timeout,
            )
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

        if not isinstance(response, dict):
            raise AdapterRpcError(-32603, "INVALID_RESPONSE")
        error = response.get("error")
        if error:
            if not isinstance(error, dict):
                raise AdapterRpcError(-32603, "INVALID_ERROR")
            code = error.get("code", -32603)
            if isinstance(code, bool) or not isinstance(code, int):
                code = -32603
            raise AdapterRpcError(code, str(error.get("message", "")), error.get("data"))
        return response.get("result")

    @staticmethod
    def _tool_error(message: str) -> dict:
        return {
            "content": [{"type": "text", "text": message}],
            "isError": True,
        }

    async def handle_request(self, request: Any) -> Optional[dict]:
        if not isinstance(request, dict):
            return {
                "jsonrpc": "2.0",
                "id": None,
                "error": {"code": -32600, "message": "INVALID_REQUEST"},
            }

        req_id = request.get("id")
        method = request.get("method")
        params = request.get("params") or {}
        if request.get("params") is None:
            params = {}
        elif not isinstance(params, dict):
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32602, "message": "INVALID_ARGUMENT: params"},
            }

        if method == "initialize":
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "protocolVersion": _PROTOCOL_VERSION,
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": _SERVER_NAME, "version": _SERVER_VERSION},
                },
            }
        if method == "ping":
            return {"jsonrpc": "2.0", "id": req_id, "result": {}}
        if isinstance(method, str) and method.startswith("notifications/"):
            return None
        if method == "tools/list":
            return {"jsonrpc": "2.0", "id": req_id, "result": {"tools": _remote_tools()}}
        if method != "tools/call":
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32601, "message": "METHOD_NOT_FOUND"},
            }

        tool_name = params.get("name")
        arguments = params.get("arguments")
        if arguments is None:
            arguments = {}
        if not isinstance(arguments, dict):
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": self._tool_error("Error: invalid tool arguments"),
            }
        if not isinstance(tool_name, str) or tool_name not in TOOL_RPC_METHODS:
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": self._tool_error("Error: unknown tool"),
            }
        if tool_name in LOCAL_ONLY_TOOLS:
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": self._tool_error("Error: tool not available"),
            }

        try:
            validate_tool_args(tool_name, arguments)
        except ValueError:
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": self._tool_error("Error: invalid tool arguments"),
            }

        forwarded = dict(arguments)
        # 身份由适配器绑定；不把客户端字段传给后端。
        forwarded.pop("agent_id", None)
        forwarded.pop("source_agent", None)
        rpc_method = TOOL_RPC_METHODS[tool_name]
        try:
            result = await self._call_adapter(rpc_method, forwarded, self._next_id())
        except AdapterRpcError as exc:
            text = f"[{exc.code}] {exc.message}"
            if exc.data is not None:
                text += "\n" + json.dumps(exc.data, ensure_ascii=False)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": self._tool_error(text),
            }
        except (ConnectionError, OSError, asyncio.TimeoutError, FrameTooLargeError, FrameDecodeError, ConnectionClosedError):
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": self._tool_error("Error: AMR 2.x adapter unavailable"),
            }

        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "content": [
                    {"type": "text", "text": json.dumps(result, ensure_ascii=False, indent=2)}
                ],
                "isError": False,
            },
        }

    async def run_stdio(self) -> None:
        loop = asyncio.get_running_loop()
        reader = asyncio.StreamReader(limit=self.max_request_bytes)
        protocol = asyncio.StreamReaderProtocol(reader)
        await loop.connect_read_pipe(lambda: protocol, sys.stdin)

        while True:
            try:
                line = await reader.readline()
            except (asyncio.LimitOverrunError, ValueError) as exc:
                # 单行超过上限：关闭连接，避免反复读取同一超长缓冲。
                sys.stderr.write(f"[AMR2-Bridge] input line too large: {exc}\n")
                break
            if not line:
                break
            try:
                text = line.decode("utf-8").strip()
            except UnicodeDecodeError:
                sys.stderr.write("[AMR2-Bridge] input is not UTF-8\n")
                continue
            if not text:
                continue
            try:
                request = json.loads(text)
                response = await self.handle_request(request)
                if response is not None:
                    sys.stdout.write(json.dumps(response, ensure_ascii=False) + "\n")
                    sys.stdout.flush()
            except json.JSONDecodeError:
                sys.stderr.write("[AMR2-Bridge] invalid JSON input\n")
            except Exception as exc:
                # 单个请求的意外错误不能打崩常驻 stdio 进程。
                sys.stderr.write(f"[AMR2-Bridge] request failed: {type(exc).__name__}\n")


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="AMR 2.x stdio MCP shim")
    parser.add_argument("--socket", default=None, help="adapter UDS path")
    parser.add_argument("--agent", default=None, help="agent name for default socket")
    parser.add_argument("--timeout", type=float, default=DEFAULT_CALL_TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.agent:
        os.environ["AMR2_AGENT"] = args.agent
    bridge = AdapterBridge(socket_path=args.socket, call_timeout=args.timeout)
    try:
        asyncio.run(bridge.run_stdio())
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
