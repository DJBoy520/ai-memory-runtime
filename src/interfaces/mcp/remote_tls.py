"""远程 MCP 客户端 TLS 配置。

本模块只负责构造客户端 ``ssl.SSLContext``，不发起连接、不读取业务配置，
也不包含任何绕过证书校验的开关。

安全不变量：
- 基于 ``ssl.create_default_context(Purpose.SERVER_AUTH)``，默认使用系统信任库；
- ``verify_mode`` 固定为 ``ssl.CERT_REQUIRED`` 且 ``check_hostname`` 始终为
  ``True``：证书链与主机名都必须校验，本模块不提供关闭校验的参数；
- 可选 ``ca_file`` 用于私有 CA：加载后该文件成为信任锚，未受信任的链会被
  拒绝；文件缺失、不是普通文件、为空或内容不是有效证书时，构造阶段即
  ``TLSConfigError`` 失败（fail closed），绝不回退到“跳过校验”；
- 最低协议版本为 TLS 1.2。
"""

import os
import ssl
from pathlib import Path
from typing import Optional, Union

__all__ = ["TLSConfigError", "build_client_ssl_context"]

PathLike = Union[str, os.PathLike]


class TLSConfigError(ValueError):
    """客户端 TLS 配置无效（CA 文件缺失、不可读或不含有效证书等）。"""


def _validated_ca_path(ca_file: PathLike) -> str:
    """校验自定义 CA 文件并返回绝对路径；任何异常都转换为配置错误。"""
    try:
        path = Path(os.fspath(ca_file))
    except TypeError as exc:
        raise TLSConfigError("ca_file 必须是文件系统路径") from exc

    try:
        if not path.exists():
            raise TLSConfigError(f"CA 文件不存在: {path}")
        if not path.is_file():
            raise TLSConfigError(f"CA 路径不是普通文件: {path}")
        if path.stat().st_size == 0:
            raise TLSConfigError(f"CA 文件为空: {path}")
        return str(path.resolve(strict=True))
    except OSError as exc:
        raise TLSConfigError(f"CA 文件不可读: {path}") from exc


def build_client_ssl_context(
    ca_file: Optional[PathLike] = None,
) -> ssl.SSLContext:
    """构造强制校验证书链与 hostname 的客户端 TLS 上下文。

    Args:
        ca_file: 可选的自定义 CA 证书文件（PEM）。为 ``None`` 时使用系统
            信任库；提供时只信任该文件中的 CA。

    Raises:
        TLSConfigError: CA 文件缺失、不可读、为空或无法解析为证书。
    """
    ca_path = None if ca_file is None else _validated_ca_path(ca_file)

    try:
        context = ssl.create_default_context(
            purpose=ssl.Purpose.SERVER_AUTH,
            cafile=ca_path,
        )
    except (ssl.SSLError, OSError, ValueError) as exc:
        # 不把 OpenSSL 的原始细节（可能包含文件内容片段）直接暴露给调用方。
        target = f"CA 文件: {ca_path}" if ca_path else "系统信任库"
        raise TLSConfigError(f"无法加载 {target}") from exc

    # 显式重申安全不变量，避免未来默认值变化导致校验被弱化。
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED
    context.minimum_version = ssl.TLSVersion.TLSv1_2

    if context.verify_mode != ssl.CERT_REQUIRED or not context.check_hostname:
        raise TLSConfigError("拒绝创建未强制校验证书链与 hostname 的 TLS 上下文")

    return context
