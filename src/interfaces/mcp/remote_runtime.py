"""远程 MCP 运行时装配：把认证、RPC 后端与 ASGI 应用组合起来。

本模块只做构造，不连接 socket、不启动监听、不读取生产配置。
"""

from __future__ import annotations

import os
from typing import Any, Iterable

from starlette.applications import Starlette

from src.interfaces.mcp.remote_auth import TokenAuthenticator
from src.interfaces.mcp.remote_backend import RemoteBackend
from src.interfaces.mcp.remote_mcp import build_app
from src.interfaces.mcp.remote_rpc import RemoteRpc

__all__ = ["build_runtime"]


def _require_absolute_path(value: Any, field: str) -> str:
    """校验套接字路径：非空、绝对路径（不解析，仅做纯路径判定）。"""
    if isinstance(value, os.PathLike):
        value = os.fspath(value)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty absolute path")
    if not os.path.isabs(value):
        raise ValueError(f"{field} must be a non-empty absolute path")
    return value


def build_runtime(
    business_socket: str,
    admin_socket: str,
    authenticator: TokenAuthenticator,
    *,
    allowed_hosts: Iterable[str] | None = None,
    allowed_origins: Iterable[str] | None = None,
) -> Starlette:
    """构造远程 MCP ASGI 应用；仅构造对象，不建立任何连接。

    Args:
        business_socket: 显式业务 UDS 路径（绝对路径）。
        admin_socket: 显式管理 UDS 路径（绝对路径），必须与业务路径不同。
        authenticator: 已构造的令牌鉴权器；本函数直接复用，绝不会根据
            其他参数再构造第二套身份来源。
        allowed_hosts: 允许的 Host 头值，语义见 :func:`build_app`。
        allowed_origins: 允许的 Origin 头值，语义见 :func:`build_app`。

    Raises:
        TypeError: ``authenticator`` 不是 :class:`TokenAuthenticator`。
        ValueError: 套接字路径非法或指向同一真实路径。
    """
    if not isinstance(authenticator, TokenAuthenticator):
        raise TypeError(
            "authenticator must be an instance of TokenAuthenticator, "
            f"got {type(authenticator).__name__}"
        )

    business_path = _require_absolute_path(business_socket, "business_socket")
    admin_path = _require_absolute_path(admin_socket, "admin_socket")

    # 业务套接字与管理套接字必须是不同的文件（按真实路径比较，防止别名/软链）。
    if os.path.realpath(business_path) == os.path.realpath(admin_path):
        raise ValueError("business_socket must not refer to admin_socket")

    rpc = RemoteRpc(business_path)
    backend = RemoteBackend(rpc)

    async def readiness(identity):
        health = await rpc('system.health', {'agent_id': identity.agent_id})
        if not isinstance(health, dict) or health.get('qdrant_ok') is not True or health.get('sqlite_ok') is not True or str(health.get('model_state', '')).upper() not in ('READY', 'IDLE'):
            raise RuntimeError('daemon dependencies unavailable')

    return build_app(
        authenticator,
        backend,
        allowed_hosts=allowed_hosts,
        allowed_origins=allowed_origins,
        readiness=readiness,
    )
