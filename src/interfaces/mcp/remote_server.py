"""远程 MCP HTTPS 服务：显式 TLS 材料 + 进程内 uvicorn 生命周期。

本模块把已验证的 :class:`~src.interfaces.mcp.remote_service.RemoteServiceConfig`
装配为可由调用方显式启动/停止的 HTTPS 服务。

安全不变量：
- TLS 材料在构造阶段加载并校验（证书链、私钥匹配、有效期），任何错误都以
  :class:`TLSMaterialError` 失败关闭；错误文本不包含证书或私钥内容；
- 不提供明文监听选项、默认证书路径或生产 UDS 默认值，所有输入均来自显式
  配置；
- 服务不劫持进程信号：uvicorn 的 ``capture_signals`` 被替换为
  ``contextlib.nullcontext``，生命周期完全由调用方控制；
- ``start`` 在服务真正监听后才返回，并暴露实际绑定端口，便于测试使用
  ``port=0``；启动失败会清理任务和服务器状态。
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import ssl
import time
from typing import Any, Optional

import uvicorn
from starlette.applications import Starlette

from .remote_runtime import build_runtime
from .remote_service import RemoteServiceConfig, load_config, validate_crypto_profile

__all__ = [
    "RemoteMcpService",
    "RemoteServerError",
    "TLSMaterialError",
    "load_server_ssl_context",
]

_START_TIMEOUT = 10.0
_STOP_TIMEOUT = 10.0
_POLL_INTERVAL = 0.005


class RemoteServerError(ValueError):
    """远程 MCP 服务生命周期或 TLS 材料无效。"""


class TLSMaterialError(RemoteServerError):
    """证书或私钥缺失、损坏、不匹配或不在有效期内。"""


def _as_path(value: Any, field: str) -> str:
    if isinstance(value, os.PathLike):
        value = os.fspath(value)
    if not isinstance(value, str) or not value:
        raise TLSMaterialError(f"{field} must be a non-empty filesystem path")
    return value


def _require_ecdsa_certificate(cert_path: str) -> None:
    """严格 TLS 配置要求在加载任何私钥材料前确认证书公钥为 ECDSA。"""
    try:
        from cryptography import x509
        from cryptography.hazmat.primitives.asymmetric import ec

        with open(cert_path, "rb") as cert_handle:
            cert = x509.load_pem_x509_certificate(cert_handle.read())
    except OSError as exc:
        raise TLSMaterialError("server certificate could not be read") from exc
    except ValueError as exc:
        raise TLSMaterialError("ECDSA certificate could not be decoded") from exc
    if not isinstance(cert.public_key(), ec.EllipticCurvePublicKey):
        raise TLSMaterialError("ECDSA certificate required by crypto_profile")


def load_server_ssl_context(cert_file: Any, key_file: Any, *, crypto_profile: str = "system") -> ssl.SSLContext:
    """加载并校验显式提供的服务端证书与私钥。

    Args:
        cert_file: PEM 证书链路径。
        key_file: PEM 私钥路径。
        crypto_profile: TLS 算法档位；严格档会在加载私钥前校验证书类型。

    Returns:
        仅支持 TLS 1.2+ 的服务端 ``ssl.SSLContext``。

    Raises:
        TLSMaterialError: 路径无效、文件缺失/不可读、证书与私钥损坏或不
            匹配、证书尚未生效或已经过期、证书类型不符合 ``crypto_profile``。
            错误文本永远不会包含证书或私钥内容。
    """
    cert_path = _as_path(cert_file, "cert_file")
    key_path = _as_path(key_file, "key_file")
    validate_crypto_profile(crypto_profile)
    if crypto_profile == "ecdsa_aes128_gcm_sha256_tls12":
        _require_ecdsa_certificate(cert_path)

    try:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        if crypto_profile == "ecdsa_aes128_gcm_sha256_tls12":
            context.maximum_version = ssl.TLSVersion.TLSv1_2
            context.set_ciphers("ECDHE-ECDSA-AES128-GCM-SHA256")
    except (ssl.SSLError, ValueError) as exc:
        raise TLSMaterialError("unable to create a server TLS 1.2+ context") from exc

    # load_cert_chain 同时验证证书可解析且私钥与证书公钥匹配。
    try:
        context.load_cert_chain(cert_path, key_path)
    except (ssl.SSLError, OSError, ValueError) as exc:
        raise TLSMaterialError(
            "server certificate and private key could not be loaded"
        ) from exc

    try:
        decoded = ssl._ssl._test_decode_cert(cert_path)  # type: ignore[attr-defined]
        not_before = ssl.cert_time_to_seconds(decoded["notBefore"])
        not_after = ssl.cert_time_to_seconds(decoded["notAfter"])
    except (ssl.SSLError, OSError, ValueError, KeyError, AttributeError) as exc:
        raise TLSMaterialError("server certificate could not be decoded") from exc

    now = time.time()
    if now < not_before:
        raise TLSMaterialError("server certificate is not valid yet")
    if now > not_after:
        raise TLSMaterialError("server certificate has expired")

    return context


def _no_signal_capture():
    """不允许 uvicorn 接管进程信号；调用方拥有完整生命周期控制权。"""
    return contextlib.nullcontext()


async def _serve_guarded(server: uvicorn.Server) -> None:
    """运行 uvicorn，但阻止其 ``sys.exit`` 逃逸到宿主事件循环。

    uvicorn 在端口被占用或 lifespan 启动失败时调用 ``sys.exit()``。在
    CPython 3.12 中，Task 内抛出的 ``SystemExit`` 不会停留在任务里，而是
    直接从事件循环抛出并终止宿主进程。因此必须在协程内部就地捕获并转换为
    :class:`RemoteServerError`，让 ``start()`` 的调用方看到可处理的异常。
    """
    try:
        await server.serve()
    except SystemExit as exc:
        raise RemoteServerError(
            "server exited before it started listening "
            f"(exit code: {exc.code})"
        ) from exc


class RemoteMcpService:
    """由显式配置驱动的远程 MCP HTTPS 服务。

    构造阶段只校验配置类型并加载 TLS 材料；不会构建 ASGI 应用、不会打开
    套接字，也不会连接业务/管理 UDS。
    """

    def __init__(self, config: RemoteServiceConfig) -> None:
        if not isinstance(config, RemoteServiceConfig):
            raise TypeError(
                "config must be an instance of RemoteServiceConfig, "
                f"got {type(config).__name__}"
            )
        self._config = config
        self._ssl_context = load_server_ssl_context(
            config.cert_file, config.key_file, crypto_profile=config.crypto_profile
        )
        self._server: Optional[uvicorn.Server] = None
        self._task: Optional[asyncio.Task[None]] = None
        self._started = False
        self._bound_port: Optional[int] = None

    @classmethod
    def from_config_file(cls, path: Any) -> "RemoteMcpService":
        """从 JSON 配置文件加载并构造服务（等价于 ``load_config`` + 构造）。"""
        return cls(load_config(path))

    @property
    def config(self) -> RemoteServiceConfig:
        return self._config

    @property
    def is_started(self) -> bool:
        return self._started

    @property
    def bound_port(self) -> Optional[int]:
        """实际监听端口；仅在服务已启动时可用。"""
        return self._bound_port

    def build_app(self) -> Starlette:
        """构造 ASGI 应用；每次调用都会得到新的应用实例。"""
        config = self._config
        return build_runtime(
            config.business_socket,
            config.admin_socket,
            config.authenticator,
            allowed_hosts=config["allowed_hosts"],
            allowed_origins=config.get("allowed_origins"),
        )

    async def start(self) -> None:
        """启动 HTTPS 服务并等待其真正开始监听。

        Raises:
            RuntimeError: 服务已经启动（或正在启动）。
            RemoteServerError: 服务未能在监听前完成启动（端口占用、lifespan
                失败等）；uvicorn 的 ``SystemExit`` 不会逃逸到宿主进程。
            TimeoutError: 10 秒内未开始监听。
        """
        if self._started or self._task is not None or self._server is not None:
            raise RuntimeError("RemoteMcpService is already started")

        app = self.build_app()
        config = uvicorn.Config(
            app,
            host=self._config.bind_host,
            port=self._config.bind_port,
            ssl_context_factory=lambda _config, _default: self._ssl_context,
            log_level="warning",
            access_log=False,
            server_header=False,
        )
        server = uvicorn.Server(config)
        # uvicorn 0.54 的 Server.serve() 会捕获 SIGINT/SIGTERM；这里替换为
        # 空上下文，避免测试或嵌入方被进程级信号劫持。
        server.capture_signals = _no_signal_capture  # type: ignore[method-assign]

        task: asyncio.Task[None] = asyncio.create_task(_serve_guarded(server))
        self._server = server
        self._task = task

        try:
            await self._wait_until_started(server, task)
            self._bound_port = self._extract_bound_port(server)
        except BaseException:
            await self._abort_startup(server, task)
            raise

        self._started = True

    async def _wait_until_started(
        self,
        server: uvicorn.Server,
        task: asyncio.Task[None],
    ) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _START_TIMEOUT
        while not server.started:
            if task.done():
                # 已完成的启动任务可能带着真实失败。uvicorn 在端口被占用或
                # lifespan 启动失败时会调用 sys.exit()，绝不能把 SystemExit
                # 原样抛给嵌入方（那会终止宿主进程）；转换为本模块的错误。
                failure = task.exception()
                if isinstance(failure, SystemExit):
                    raise RemoteServerError(
                        "server exited before it started listening "
                        f"(exit code: {failure.code})"
                    ) from failure
                if failure is not None:
                    raise failure
                raise RemoteServerError(
                    "server exited before it started listening"
                )
            if loop.time() >= deadline:
                raise TimeoutError(
                    f"RemoteMcpService did not start within {_START_TIMEOUT:g}s"
                )
            await asyncio.sleep(_POLL_INTERVAL)

    @staticmethod
    def _extract_bound_port(server: uvicorn.Server) -> int:
        try:
            bound_socket = server.servers[0].sockets[0]
            port = bound_socket.getsockname()[1]
        except (AttributeError, IndexError, OSError) as exc:
            raise RemoteServerError(
                "server started without a bound TCP socket"
            ) from exc
        if isinstance(port, bool) or not isinstance(port, int) or port <= 0:
            raise RemoteServerError("server did not report a valid bound port")
        return port

    async def _abort_startup(
        self,
        server: uvicorn.Server,
        task: asyncio.Task[None],
    ) -> None:
        """启动失败后的尽力清理；不掩盖调用方正在处理的原始异常。"""
        server.should_exit = True
        if not task.done():
            task.cancel()
        # gather(return_exceptions=True) 不会因任务本身失败而再次抛出。
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(
                asyncio.gather(task, return_exceptions=True),
                timeout=_STOP_TIMEOUT,
            )
        self._server = None
        self._task = None
        self._started = False
        self._bound_port = None

    async def stop(self) -> None:
        """停止服务并等待监听套接字关闭；重复调用是安全的。

        致命错误（取消以外的异常）会原样抛出，且状态在等待前已清理，
        因此后续 ``stop`` 仍然幂等。
        """
        server = self._server
        task = self._task
        if server is None and task is None:
            self._started = False
            return

        self._started = False
        self._server = None
        self._task = None
        self._bound_port = None

        if server is not None:
            server.should_exit = True
        if task is None:
            return

        try:
            await asyncio.wait_for(task, timeout=_STOP_TIMEOUT)
        except asyncio.TimeoutError:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def serve(self) -> None:
        """启动服务并等待其结束（可选便捷方法）。"""
        if not self._started:
            await self.start()
        task = self._task
        if task is None:
            return
        try:
            await task
        finally:
            self._started = False
            self._server = None
            self._task = None
            self._bound_port = None
