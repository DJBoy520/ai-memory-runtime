"""2.x 本地 UDS 适配器服务。

每个固定身份一个 Unix socket；插件使用 1.x 的 daemon 方法名与帧格式，
适配器把调用转发到认证 HTTPS MCP，并把结果/错误还原为 1.x JSON-RPC 语义。

安全不变量：
- socket 权限 0600，父目录必须属于当前用户且不可 group/other 写；
- 只有配置内的绑定身份可连接；客户端传入的 agent_id/source_agent 被剥离；
- 未知/本机专用/管理方法不触达后端；
- 适配器不自动重试、不新增重放队列；
- 日志不含令牌、请求正文或响应正文。
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import logging
import os
import socket as socket_module
import stat
import time
from typing import Any, Dict, Mapping, Optional, Set, Tuple

from src.interfaces.adapter.client import (
    AdapterError,
    RemoteToolClient,
)
from src.interfaces.adapter.config import (
    AdapterBinding,
    AdapterConfig,
    load_adapter_config,
)
from src.interfaces.adapter.mapping import UnknownMethodError, resolve_tool
from src.interfaces.ipc.protocol import (
    ConnectionClosedError,
    FrameDecodeError,
    FrameTooLargeError,
    make_jsonrpc_error,
    make_jsonrpc_response,
    read_frame,
    write_frame,
)
from src.interfaces.mcp.remote_tls import build_client_ssl_context

__all__ = ["AdapterService"]

logger = logging.getLogger("amr2.adapter")

_STOP_GRACE_SECONDS = 5.0
_PING_METHOD = "amr.ping"


class AdapterService:
    """由显式配置驱动的本地适配器；构造期不建 socket、不连接远程。"""

    def __init__(self, config: AdapterConfig) -> None:
        if not isinstance(config, AdapterConfig):
            raise TypeError(
                "config must be an instance of AdapterConfig, "
                f"got {type(config).__name__}"
            )
        self._config = config
        # 客户端 TLS 上下文在构造期构建：CA 缺失/非法时立即失败关闭。
        self._ssl_context = build_client_ssl_context(config.ca_file)
        self._clients: Dict[str, RemoteToolClient] = {}
        self._servers: Dict[str, asyncio.AbstractServer] = {}
        self._tasks: Set[asyncio.Task[Any]] = set()
        # 只清理本实例实际创建过的 socket，避免误删另一个运行实例。
        self._created_sockets: Set[str] = set()
        self._lifecycle_lock = asyncio.Lock()
        self._started = False

    @classmethod
    def from_config_file(cls, path: Any) -> "AdapterService":
        return cls(load_adapter_config(path))

    @property
    def is_started(self) -> bool:
        return self._started

    @property
    def socket_paths(self) -> Tuple[str, ...]:
        return tuple(binding.socket_path for binding in self._config.bindings)

    async def start(self) -> None:
        """启动全部绑定；已启动时抛 RuntimeError。失败时清理已建资源。"""
        async with self._lifecycle_lock:
            await self._start_locked()

    async def _start_locked(self) -> None:
        if self._started:
            raise RuntimeError("AdapterService is already started")

        seen: Set[str] = set()
        for binding in self._config.bindings:
            real_path = os.path.realpath(binding.socket_path)
            if real_path in seen:
                raise ValueError("adapter bindings must not contain duplicate sockets")
            seen.add(real_path)

        try:
            for binding in self._config.bindings:
                self._clients[binding.socket_path] = RemoteToolClient(
                    self._config.remote_url,
                    binding.token,
                    self._ssl_context,
                    connect_timeout=self._config.connect_timeout,
                    read_timeout=self._config.read_timeout,
                    max_response_bytes=self._config.max_response_bytes,
                )
            for binding in self._config.bindings:
                await self._start_binding(binding)
        except BaseException:
            await self._shutdown()
            raise
        else:
            # 全部绑定真正监听后才置位，避免“启动中”被当成可用。
            self._started = True

    async def _start_binding(self, binding: AdapterBinding) -> None:
        socket_path = binding.socket_path
        self._prepare_parent_dir(socket_path)
        self._prepare_socket_path(socket_path)

        client = self._clients[socket_path]

        async def handle(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            await self._handle_connection(binding, client, reader, writer)

        # 先 bind 再 chmod/listen，最后把已就绪的 socket 交给 asyncio；
        # 不修改进程级 umask，也不存在“先 0755 后 chmod”的可连接窗口。
        sock = socket_module.socket(socket_module.AF_UNIX, socket_module.SOCK_STREAM)
        try:
            sock.bind(socket_path)
            os.chmod(socket_path, 0o600)
            sock.listen(128)
            sock.setblocking(False)
            server = await asyncio.start_unix_server(handle, sock=sock)
        except BaseException:
            with contextlib.suppress(OSError):
                sock.close()
            with contextlib.suppress(OSError):
                os.unlink(socket_path)
            raise
        self._servers[socket_path] = server
        self._created_sockets.add(socket_path)
        logger.info(
            "adapter listening on %s (agent=%s)", socket_path, binding.agent_id
        )

    @staticmethod
    def _prepare_parent_dir(socket_path: str) -> None:
        parent = os.path.dirname(socket_path) or "."
        if not os.path.isdir(parent):
            os.makedirs(parent, mode=0o700, exist_ok=True)
            os.chmod(parent, 0o700)
            return
        link_info = os.lstat(parent)
        if stat.S_ISLNK(link_info.st_mode):
            raise ValueError("adapter socket parent directory must not be a symlink")
        info = os.stat(parent)
        if info.st_uid != os.getuid():
            raise ValueError("adapter socket parent directory must be owned by the current user")
        if info.st_mode & 0o022:
            raise ValueError("adapter socket parent directory must not be group/other writable")

    @staticmethod
    def _prepare_socket_path(socket_path: str) -> None:
        if not os.path.lexists(socket_path):
            return
        info = os.lstat(socket_path)
        if not stat.S_ISSOCK(info.st_mode):
            # 绝不删除普通文件/目录/软链。
            raise ValueError("adapter socket path exists and is not a socket")
        if info.st_uid != os.getuid():
            raise ValueError("adapter socket path is owned by another user")

        # 防止两个适配器实例互相抢占：能连上说明已有实例在监听。
        probe = socket_module.socket(socket_module.AF_UNIX, socket_module.SOCK_STREAM)
        probe.settimeout(0.2)
        try:
            probe.connect(socket_path)
        except (ConnectionRefusedError, FileNotFoundError, OSError):
            pass
        else:
            raise ValueError("adapter socket is already in use")
        finally:
            with contextlib.suppress(OSError):
                probe.close()

        os.unlink(socket_path)

    async def _handle_connection(
        self,
        binding: AdapterBinding,
        client: RemoteToolClient,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._tasks.add(task)
        try:
            while True:
                try:
                    request = await read_frame(
                        reader, max_bytes=self._config.max_request_bytes
                    )
                except ConnectionClosedError:
                    break
                except FrameTooLargeError:
                    await self._write_error(
                        writer, -32600, "INVALID_REQUEST: frame too large", None
                    )
                    break
                except FrameDecodeError:
                    if not await self._write_error(
                        writer, -32700, "PARSE_ERROR: invalid frame", None
                    ):
                        break
                    continue

                if not await self._dispatch(binding, client, request, writer):
                    break
        finally:
            if task is not None:
                self._tasks.discard(task)
            # A peer may reset the socket after its last response. Closing is
            # best-effort cleanup; never surface that reset as an unhandled
            # client callback exception.
            try:
                writer.close()
                await writer.wait_closed()
            except (Exception, asyncio.CancelledError):
                pass

    async def _dispatch(
        self,
        binding: AdapterBinding,
        client: RemoteToolClient,
        request: Any,
        writer: asyncio.StreamWriter,
    ) -> bool:
        """处理一个请求；返回 False 表示连接应关闭。"""
        started = time.monotonic()
        if not isinstance(request, dict):
            return await self._write_error(
                writer, -32600, "INVALID_REQUEST: payload must be an object", None
            )

        has_id = "id" in request
        req_id = request.get("id")
        method = request.get("method")
        params = request.get("params")

        # The socket identifies the intended binding, but does not authenticate
        # a caller that shares its Unix UID. Require the binding's bearer token
        # on every request, including ping, before dispatching any operation.
        presented = request.get("auth_token")
        try:
            candidate = hashlib.sha256(presented.encode("ascii")).digest() if isinstance(presented, str) else b""
        except UnicodeEncodeError:
            candidate = b""
        expected = hashlib.sha256(binding.token.encode("ascii")).digest()
        if not hmac.compare_digest(candidate, expected):
            if has_id:
                return await self._write_error(writer, -32012, "AUTH_FAILED", req_id)
            return True

        if not isinstance(method, str) or not method:
            if has_id and not await self._write_error(
                writer, -32600, "INVALID_REQUEST: missing method", req_id
            ):
                return False
            return True
        if params is not None and not isinstance(params, dict):
            if has_id and not await self._write_error(
                writer, -32602, "INVALID_ARGUMENT: params must be an object", req_id
            ):
                return False
            return True

        if method == _PING_METHOD:
            if has_id and not await self._write(
                writer, make_jsonrpc_response({"status": "ok"}, req_id)
            ):
                return False
            return True

        if method == 'amr.tools':
            if has_id and not await self._write(writer, make_jsonrpc_response({'tools': list(binding.tools)}, req_id)):
                return False
            return True

        try:
            tool = resolve_tool(method)
            if tool not in binding.tools:
                raise UnknownMethodError("unknown method")
        except UnknownMethodError:
            if has_id and not await self._write_error(
                writer, -32601, "METHOD_NOT_FOUND: unknown method", req_id
            ):
                return False
            self._log_call(binding, method, "denied", started)
            return True

        arguments = dict(params or {})
        # 身份由适配器绑定；客户端字段一律剥离，服务端还会再次覆盖。
        arguments.pop("agent_id", None)
        arguments.pop("source_agent", None)

        try:
            result = await client.call_tool(tool, arguments)
        except AdapterError as exc:
            if has_id and not await self._write(
                writer, exc.to_jsonrpc_error(req_id)
            ):
                return False
            self._log_call(binding, method, "error", started)
            return True
        except Exception as exc:
            # 不把原始异常文本写入响应或日志。
            logger.error("adapter internal error: %s", type(exc).__name__)
            if has_id and not await self._write_error(
                writer, -32603, "INTERNAL: tool call failed", req_id
            ):
                return False
            self._log_call(binding, method, "internal", started)
            return True

        if has_id and not await self._write(
            writer, make_jsonrpc_response(result, req_id)
        ):
            return False
        self._log_call(binding, method, "ok", started)
        return True

    @staticmethod
    def _log_call(
        binding: AdapterBinding, method: str, outcome: str, started: float
    ) -> None:
        elapsed_ms = (time.monotonic() - started) * 1000.0
        logger.debug(
            "adapter call agent=%s method=%s outcome=%s elapsed_ms=%.1f",
            binding.agent_id,
            method,
            outcome,
            elapsed_ms,
        )

    async def _write(self, writer: asyncio.StreamWriter, payload: Mapping[str, Any]) -> bool:
        """写一个响应帧；返回 False 表示连接不可继续（调用方应关闭）。"""
        try:
            await write_frame(
                writer, dict(payload), max_bytes=self._config.max_response_bytes
            )
            return True
        except FrameTooLargeError:
            # 结果超过配置的响应上限：无法再写错误帧，显式关闭连接，
            # 不能静默吞掉让客户端一直等。
            logger.warning("adapter response exceeded max_response_bytes")
            return False
        except (ConnectionError, OSError):
            return False
        except (TypeError, ValueError):
            logger.warning("adapter response is not JSON serializable")
            return False

    async def _write_error(
        self,
        writer: asyncio.StreamWriter,
        code: int,
        message: str,
        req_id: Any,
    ) -> bool:
        return await self._write(
            writer, make_jsonrpc_error(code, message, None, req_id)
        )

    async def stop(self) -> None:
        """停止全部监听并清理 socket；重复调用安全。"""
        async with self._lifecycle_lock:
            await self._shutdown()

    async def _shutdown(self) -> None:
        self._started = False

        servers = list(self._servers.values())
        self._servers.clear()
        for server in servers:
            server.close()

        if servers:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(
                    asyncio.gather(
                        *(server.wait_closed() for server in servers),
                        return_exceptions=True,
                    ),
                    timeout=_STOP_GRACE_SECONDS,
                )

        current = asyncio.current_task()
        tasks = [
            task
            for task in self._tasks
            if not task.done() and task is not current
        ]
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=_STOP_GRACE_SECONDS)
            for task in pending:
                task.cancel()
            if pending:
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(
                        asyncio.gather(*pending, return_exceptions=True),
                        timeout=_STOP_GRACE_SECONDS,
                    )

        for socket_path in list(self._created_sockets):
            self._remove_socket(socket_path)
        self._created_sockets.clear()

        clients = list(self._clients.values())
        self._clients.clear()
        for client in clients:
            with contextlib.suppress(Exception):
                await client.aclose()

    @staticmethod
    def _remove_socket(socket_path: str) -> None:
        try:
            info = os.lstat(socket_path)
        except OSError:
            return
        if stat.S_ISSOCK(info.st_mode):
            with contextlib.suppress(OSError):
                os.unlink(socket_path)
