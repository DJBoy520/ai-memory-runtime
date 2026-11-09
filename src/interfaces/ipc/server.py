import asyncio
import logging
import os
import re
import stat
from pathlib import Path
from typing import Any, Callable, Coroutine, Dict, Optional

from config.settings import AppConfig, settings
from src.core.errors import (
    IdempotencyConflict,
    InvalidStatusError,
    LegacyPathForbidden,
    ModelLoadingPendingError,
    VersionConflictError,
)
from src.core.request_context import get_request_id
from src.interfaces.ipc.errors import RpcError
from src.interfaces.ipc.protocol import (
    ConnectionClosedError,
    FrameDecodeError,
    FrameTooLargeError,
    make_jsonrpc_error,
    make_jsonrpc_response,
    read_frame,
    write_frame,
)

try:
    from src.core.engine import QueueFullError
except ImportError:
    class QueueFullError(Exception):
        pass

logger = logging.getLogger("ipc.server")

# Handler 类型定义：async def handler(method: str, params: Any) -> Any
RPCHandler = Callable[[str, Any], Coroutine[Any, Any, Any]]


class UDSServer:
    """
    单个 Unix Domain Socket 服务实例
    """
    def __init__(
        self,
        name: str,
        socket_path: str,
        handler: RPCHandler,
        socket_mode: int = 0o600,
        max_request_bytes: int = 4 * 1024 * 1024,
    ):
        self.name = name
        self.socket_path = Path(socket_path)
        self.handler = handler
        self.socket_mode = socket_mode
        self.max_request_bytes = max_request_bytes
        self.server: Optional[asyncio.Server] = None
        self._running = False

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            while self._running:
                try:
                    req = await read_frame(reader, max_bytes=self.max_request_bytes)
                except ConnectionClosedError:
                    break
                except FrameTooLargeError as e:
                    logger.warning("[%s] Frame too large: %s. Closing connection.", self.name, e)
                    # 发送错误响应后断开
                    err_resp = make_jsonrpc_error(-32600, f"Frame too large: {e}", req_id=None)
                    try:
                        await write_frame(writer, err_resp, max_bytes=self.max_request_bytes)
                    except Exception:
                        pass
                    break
                except FrameDecodeError as e:
                    logger.warning("[%s] Decode error: %s", self.name, e)
                    err_resp = make_jsonrpc_error(-32700, f"Parse error: {e}", req_id=None)
                    try:
                        await write_frame(writer, err_resp, max_bytes=self.max_request_bytes)
                    except Exception:
                        pass
                    continue

                req_id = req.get("id")
                method = req.get("method")
                params = req.get("params")

                if not method or not isinstance(method, str):
                    err_resp = make_jsonrpc_error(-32600, "Invalid Request: missing method", req_id=req_id)
                    await write_frame(writer, err_resp, max_bytes=self.max_request_bytes)
                    continue

                try:
                    res = await self.handler(method, params)
                    resp = make_jsonrpc_response(res, req_id=req_id)
                except Exception as ex:
                    resp = self._map_exception_to_error_response(ex, method, req_id)

                try:
                    await write_frame(writer, resp, max_bytes=self.max_request_bytes)
                except (ConnectionResetError, BrokenPipeError):
                    logger.info("[%s] Client disconnected before response could be sent", self.name)
                    break

        except (ConnectionResetError, BrokenPipeError):
            logger.info("[%s] Client connection reset", self.name)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.exception("[%s] Unexpected socket error: %s", self.name, e)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    def _map_exception_to_error_response(self, ex: Exception, method: str, req_id: Any) -> Dict[str, Any]:
        """按规范将各类业务异常与系统异常映射为稳定的 JSON-RPC 错误对象（RFC v5 / GPT-AUDIT C1）。"""
        req_ctx_id = get_request_id()

        # 1. RpcError 首位直通，防止被 ValueError 分支提前吞掉
        if isinstance(ex, RpcError):
            logger.warning("[%s] Handler RpcError executing method '%s' (request_id=%s): %s", self.name, method, req_ctx_id, ex)
            return ex.to_jsonrpc_error(req_id)

        # 2. core.errors.VersionConflictError -> -32002 VERSION_CONFLICT
        if isinstance(ex, VersionConflictError):
            logger.warning("[%s] Version conflict executing method '%s' (request_id=%s): %s", self.name, method, req_ctx_id, ex)
            data = {
                "memory_id": getattr(ex, "memory_id", None),
                "current_version": getattr(ex, "current_version", None),
                "expected_version": getattr(ex, "expected_version", None),
            }
            msg = str(ex) if str(ex).startswith("VERSION_CONFLICT:") else f"VERSION_CONFLICT: {str(ex)}"
            return make_jsonrpc_error(-32002, msg, data=data, req_id=req_id)

        # 3. core.errors.ModelLoadingPendingError -> -32003 MODEL_LOADING
        if isinstance(ex, ModelLoadingPendingError):
            logger.warning("[%s] Model loading pending executing method '%s' (request_id=%s): %s", self.name, method, req_ctx_id, ex)
            data = {
                "model_state": getattr(ex, "model_state", "LOADING"),
                "retry_after_ms": getattr(ex, "retry_after_ms", 1000),
            }
            msg = str(ex) if str(ex).startswith("MODEL_LOADING:") else f"MODEL_LOADING: {str(ex)}"
            return make_jsonrpc_error(-32003, msg, data=data, req_id=req_id)

        # 4. engine.QueueFullError -> -32004 QUEUE_FULL
        if isinstance(ex, QueueFullError):
            logger.warning("[%s] Queue full executing method '%s' (request_id=%s): %s", self.name, method, req_ctx_id, ex)
            data = {}
            if hasattr(ex, "active_requests") and getattr(ex, "active_requests") is not None:
                data["active_requests"] = getattr(ex, "active_requests")
            if hasattr(ex, "max_queue_size") and getattr(ex, "max_queue_size") is not None:
                data["max_queue_size"] = getattr(ex, "max_queue_size")
            if "active_requests" not in data or "max_queue_size" not in data:
                m = re.search(r"\((\d+)/(\d+)\)", str(ex))
                if m:
                    data.setdefault("active_requests", int(m.group(1)))
                    data.setdefault("max_queue_size", int(m.group(2)))
            msg = str(ex) if str(ex).startswith("QUEUE_FULL:") else f"QUEUE_FULL: {str(ex)}"
            return make_jsonrpc_error(-32004, msg, data=data, req_id=req_id)

        # 5. KeyError -> -32005 NOT_FOUND
        if isinstance(ex, KeyError):
            logger.warning("[%s] Resource not found executing method '%s' (request_id=%s): %s", self.name, method, req_ctx_id, ex)
            mid = ex.args[0] if ex.args else None
            data = {"memory_id": mid}
            msg = str(ex) if str(ex).startswith("NOT_FOUND:") else f"NOT_FOUND: {str(ex)}"
            return make_jsonrpc_error(-32005, msg, data=data, req_id=req_id)

        # 6. IdempotencyConflict -> -32006 IDEMPOTENCY_CONFLICT
        if isinstance(ex, IdempotencyConflict):
            logger.warning("[%s] Idempotency conflict executing method '%s' (request_id=%s): %s", self.name, method, req_ctx_id, ex)
            data = {
                "op": getattr(ex, "op", None),
                "idem_key": getattr(ex, "idem_key", None),
                "existing_memory_id": getattr(ex, "existing_memory_id", None),
            }
            msg = str(ex) if str(ex).startswith("IDEMPOTENCY_CONFLICT:") else f"IDEMPOTENCY_CONFLICT: {str(ex)}"
            return make_jsonrpc_error(-32006, msg, data=data, req_id=req_id)

        # 7. LegacyPathForbidden / InvalidStatusError -> -32602 INVALID_ARGUMENT
        if isinstance(ex, LegacyPathForbidden):
            logger.warning("[%s] Legacy path forbidden executing method '%s' (request_id=%s): %s", self.name, method, req_ctx_id, ex)
            data = {
                "memory_id": getattr(ex, "memory_id", None),
                "projection_class": getattr(ex, "projection_class", None),
                "hint": getattr(ex, "hint", None),
            }
            msg = str(ex) if str(ex).startswith("INVALID_ARGUMENT:") else f"INVALID_ARGUMENT: {str(ex)}"
            return make_jsonrpc_error(-32602, msg, data=data, req_id=req_id)

        if isinstance(ex, InvalidStatusError):
            logger.warning("[%s] Invalid status executing method '%s' (request_id=%s): %s", self.name, method, req_ctx_id, ex)
            data = {
                "value": getattr(ex, "value", None),
                "allowed": getattr(ex, "allowed", None),
            }
            msg = str(ex) if str(ex).startswith("INVALID_ARGUMENT:") else f"INVALID_ARGUMENT: {str(ex)}"
            return make_jsonrpc_error(-32602, msg, data=data, req_id=req_id)

        # 8. 其它 ValueError：按消息前缀匹配
        if isinstance(ex, ValueError):
            msg_str = str(ex)
            logger.warning("[%s] ValueError executing method '%s' (request_id=%s): %s", self.name, method, req_ctx_id, ex)
            if msg_str.startswith("AGENT_UNAUTHORIZED:"):
                data = getattr(ex, "data", {})
                return make_jsonrpc_error(-32001, msg_str, data=data, req_id=req_id)
            elif msg_str.startswith("VERSION_CONFLICT:"):
                data = getattr(ex, "data", {})
                return make_jsonrpc_error(-32002, msg_str, data=data, req_id=req_id)
            elif msg_str.startswith("NOT_FOUND:"):
                data = getattr(ex, "data", {})
                return make_jsonrpc_error(-32005, msg_str, data=data, req_id=req_id)
            else:
                data = getattr(ex, "data", None)
                msg = msg_str if msg_str.startswith("INVALID_ARGUMENT:") else f"INVALID_ARGUMENT: {msg_str}"
                return make_jsonrpc_error(-32602, msg, data=data, req_id=req_id)

        # 9. 其余 Exception -> -32603 INTERNAL
        logger.exception("[%s] Internal error executing method '%s' (request_id=%s): %s", self.name, method, req_ctx_id, ex)
        return make_jsonrpc_error(-32603, f"Internal error: {str(ex)}", req_id=req_id)

    async def start(self) -> None:
        # 确保父级目录存在
        self.socket_path.parent.mkdir(parents=True, exist_ok=True)
        # 若已有残留的同名 socket 文件，先清除
        if self.socket_path.exists() or self.socket_path.is_socket():
            try:
                self.socket_path.unlink()
            except OSError:
                pass

        self._running = True
        self.server = await asyncio.start_unix_server(
            self._handle_client,
            path=str(self.socket_path),
        )

        # 严格应用 0600 文件权限
        os.chmod(self.socket_path, self.socket_mode)
        logger.info(
            "[%s] UDS Server listening on %s (mode: %s)",
            self.name,
            self.socket_path,
            oct(self.socket_mode),
        )

    async def stop(self) -> None:
        self._running = False
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
        if self.socket_path.exists():
            try:
                self.socket_path.unlink()
            except OSError:
                pass
        logger.info("[%s] UDS Server stopped and socket removed", self.name)


class DualUDSServer:
    """
    双 UDS 服务管理器（业务 Socket + 管理 Socket）
    实现业务与管理权限和通道的完全物理隔离
    """
    def __init__(
        self,
        config: Optional[AppConfig] = None,
        business_handler: Optional[RPCHandler] = None,
        admin_handler: Optional[RPCHandler] = None,
    ):
        self.config = config or settings
        self.business_handler = business_handler or self._default_business_handler
        self.admin_handler = admin_handler or self._default_admin_handler

        self.business_server = UDSServer(
            name="business",
            socket_path=self.config.server.business_socket,
            handler=self.business_handler,
            socket_mode=self.config.server.socket_mode,
            max_request_bytes=self.config.server.max_request_bytes,
        )

        self.admin_server = UDSServer(
            name="admin",
            socket_path=self.config.server.admin_socket,
            handler=self.admin_handler,
            socket_mode=self.config.server.socket_mode,
            max_request_bytes=self.config.server.max_request_bytes,
        )

    async def _default_business_handler(self, method: str, params: Any) -> Any:
        # 默认的回显/占位实现（供后续 Step 4 / 5 注入具体 MemoryService）
        return {"echo_method": method, "params": params, "status": "business_ack"}

    async def _default_admin_handler(self, method: str, params: Any) -> Any:
        # 默认的管理回显实现（供后续 Step 3 / 5 注入指标和状态机）
        if method == "admin.get_status":
            return {"status": "ok", "role": "admin"}
        return {"echo_method": method, "params": params, "status": "admin_ack"}

    async def start(self) -> None:
        await self.business_server.start()
        await self.admin_server.start()
        logger.info("DualUDSServer started successfully.")

    async def stop(self) -> None:
        await asyncio.gather(
            self.business_server.stop(),
            self.admin_server.stop(),
            return_exceptions=True,
        )
        logger.info("DualUDSServer stopped successfully.")
