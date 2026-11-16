"""2.x UDS 适配器配置校验。

只做离线校验：读取 JSON、检查路径/权限/令牌/绑定；不建 socket、不连接远程、
不读取生产配置。所有错误都是 ValueError，消息不含令牌。

配置形状::

    {
      "remote_url": "https://amr.internal:8443/mcp",
      "ca_file": "/etc/amr/tls/ca.crt",          # 可选
      "credentials_file": "/etc/amr/remote-credentials.json",
      "connect_timeout": 5.0,                     # 可选
      "read_timeout": 120.0,                      # 可选
      "max_request_bytes": 4194304,               # 可选
      "bindings": [
        {"socket_path": "/run/user/1000/amr2-openclaw.sock",
         "agent_id": "openclaw"}
      ]
    }

凭证文件沿用服务端格式 ``{token: {"agent_id": str, "tools": [...]}}``，
权限必须为 0600/0400。每个 binding 的 agent_id 必须恰好对应一个令牌；
绑定从令牌继承工具白名单。
"""

from __future__ import annotations

import json
import math
import os
import stat
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Tuple
from urllib.parse import urlsplit

from src.interfaces.ipc.protocol import MAX_REQUEST_BYTES
from src.interfaces.mcp.remote_auth import MIN_TOKEN_LENGTH, TokenAuthenticator

__all__ = [
    "AdapterBinding",
    "AdapterConfig",
    "load_adapter_config",
]

_ALLOWED_CREDENTIAL_MODES = frozenset({0o600, 0o400})
_DEFAULT_CONNECT_TIMEOUT = 5.0
_DEFAULT_READ_TIMEOUT = 120.0
_DEFAULT_MAX_RESPONSE_BYTES = 8 * 1024 * 1024
# 配置/凭证是小型 JSON；限制读取上限，避免被替换成巨型文件耗尽内存。
_MAX_CONFIG_BYTES = 1024 * 1024


@dataclass(frozen=True)
class AdapterBinding:
    """一个固定身份对应的本地 UDS 监听。"""

    socket_path: str
    agent_id: str
    tools: Tuple[str, ...]
    # 令牌绝不能出现在 repr/日志中。
    token: str = field(repr=False)


@dataclass(frozen=True)
class AdapterConfig:
    """校验后的适配器配置。"""

    remote_url: str
    credentials_file: str
    bindings: Tuple[AdapterBinding, ...]
    ca_file: Optional[str] = None
    connect_timeout: float = _DEFAULT_CONNECT_TIMEOUT
    read_timeout: float = _DEFAULT_READ_TIMEOUT
    max_request_bytes: int = MAX_REQUEST_BYTES
    max_response_bytes: int = _DEFAULT_MAX_RESPONSE_BYTES


def _fail(message: str) -> ValueError:
    return ValueError(message)


def _require_absolute_path(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _fail(f"'{field_name}' must be a non-empty absolute path")
    if not os.path.isabs(value):
        raise _fail(f"'{field_name}' must be an absolute path")
    return os.path.normpath(value)


def _ensure_regular_readable(path: str, field_name: str) -> None:
    if not os.path.exists(path):
        raise _fail(f"'{field_name}' does not exist")
    try:
        info = os.stat(path)
    except OSError:
        raise _fail(f"'{field_name}' cannot be inspected")
    if not stat.S_ISREG(info.st_mode):
        raise _fail(f"'{field_name}' must be a regular file")
    if not os.access(path, os.R_OK):
        raise _fail(f"'{field_name}' is not readable")
    try:
        with open(path, "rb") as handle:
            handle.read(1)
    except OSError:
        raise _fail(f"'{field_name}' is not readable")


def _read_json_object(path: str, field_name: str) -> Dict[str, Any]:
    try:
        # 先按上限读取，避免 getsize/read 之间的 TOCTOU 把巨型文件读入内存。
        with open(path, "rb") as handle:
            raw = handle.read(_MAX_CONFIG_BYTES + 1)
    except OSError:
        raise _fail(f"'{field_name}' cannot be read")
    if len(raw) > _MAX_CONFIG_BYTES:
        raise _fail(f"'{field_name}' is too large")
    try:
        text = raw.decode("utf-8")
    except (OSError, UnicodeDecodeError):
        raise _fail(f"'{field_name}' cannot be read as UTF-8 text")
    if not text.strip():
        raise _fail(f"'{field_name}' must not be empty")
    try:
        data = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        raise _fail(f"'{field_name}' is not valid JSON")
    if not isinstance(data, dict):
        raise _fail(f"'{field_name}' must contain a JSON object")
    return data


def _validate_positive_number(value: Any, field_name: str, default: float) -> float:
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _fail(f"'{field_name}' must be a number")
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise _fail(f"'{field_name}' must be a finite positive number")
    return number


def _validate_remote_url(value: Any) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise _fail("'remote_url' must be a non-empty URL")
    parsed = urlsplit(value)
    if parsed.scheme != "https":
        raise _fail("'remote_url' must use https")
    if not parsed.hostname:
        raise _fail("'remote_url' must contain a host")
    if parsed.username is not None or parsed.password is not None:
        raise _fail("'remote_url' must not contain userinfo")
    if parsed.query or parsed.fragment:
        raise _fail("'remote_url' must not contain query or fragment")
    try:
        port = parsed.port
    except ValueError:
        raise _fail("'remote_url' has an invalid port")
    if port is not None and not 1 <= port <= 65535:
        raise _fail("'remote_url' has an invalid port")
    if not parsed.path:
        return value + "/mcp"
    if not parsed.path.startswith("/"):
        raise _fail("'remote_url' path must be absolute")
    return value


def _validate_credentials(
    path: str,
) -> Dict[str, List[Tuple[str, Tuple[str, ...]]]]:
    """返回 ``agent_id -> [(token, tools), ...]``；令牌不出现在错误里。"""
    try:
        mode = stat.S_IMODE(os.stat(path).st_mode)
    except OSError:
        raise _fail("'credentials_file' cannot be inspected")
    if mode not in _ALLOWED_CREDENTIAL_MODES:
        raise _fail("'credentials_file' must have permissions 0600 or 0400")

    raw = _read_json_object(path, "credentials_file")
    if not raw:
        raise _fail("'credentials_file' must contain at least one token entry")
    for token in raw:
        if not isinstance(token, str) or len(token) < MIN_TOKEN_LENGTH:
            raise _fail(
                "credentials tokens must be strings of at least "
                f"{MIN_TOKEN_LENGTH} characters"
            )

    # 复用唯一鉴权器校验字符集、身份与工具列表；不写第二套认证逻辑。
    authenticator = TokenAuthenticator(raw)
    by_agent: Dict[str, List[Tuple[str, Tuple[str, ...]]]] = {}
    for token, entry in raw.items():
        identity = authenticator.authenticate(token)
        if identity is None:  # pragma: no cover - TokenAuthenticator 已校验
            raise _fail("'credentials_file' contains an unusable token")
        by_agent.setdefault(identity.agent_id, []).append((token, identity.tools))
    return by_agent


def _validate_bindings(
    raw: Any,
    by_agent: Mapping[str, List[Tuple[str, Tuple[str, ...]]]],
) -> Tuple[AdapterBinding, ...]:
    if not isinstance(raw, list):
        raise _fail("'bindings' must be a list")

    bindings: List[AdapterBinding] = []
    seen_paths: set[str] = set()
    seen_agents: set[str] = set()

    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            raise _fail(f"'bindings[{index}]' must be an object")
        socket_path = _require_absolute_path(
            item.get("socket_path"), f"bindings[{index}].socket_path"
        )
        agent_id = item.get("agent_id")
        if not isinstance(agent_id, str) or not agent_id or agent_id != agent_id.strip():
            raise _fail(f"'bindings[{index}].agent_id' must be a non-empty string")

        # realpath 解析已存在的符号链接（包括末级 alias）；再按“真实父目录
        # + 文件名”比较，覆盖父目录尚不存在/为软链的等价路径。
        real_socket = os.path.realpath(socket_path)
        parent_real = os.path.join(
            os.path.realpath(os.path.dirname(socket_path)),
            os.path.basename(socket_path),
        )
        if real_socket in seen_paths or parent_real in seen_paths:
            raise _fail("'bindings' must not contain duplicate socket paths")
        if agent_id in seen_agents:
            raise _fail("'bindings' must not contain duplicate agent_id values")
        seen_paths.add(real_socket)
        seen_paths.add(parent_real)
        seen_agents.add(agent_id)

        tokens = by_agent.get(agent_id, [])
        if len(tokens) != 1:
            raise _fail(
                "each binding agent_id must match exactly one credentials token"
            )
        token, tools = tokens[0]
        bindings.append(
            AdapterBinding(
                socket_path=socket_path,
                agent_id=agent_id,
                tools=tuple(tools),
                token=token,
            )
        )

    return tuple(bindings)


def load_adapter_config(path: Any) -> AdapterConfig:
    """读取并校验适配器 JSON 配置；不建 socket、不连接远程。"""
    if not isinstance(path, (str, os.PathLike)) or not os.fspath(path):
        raise _fail("configuration path must be a non-empty path")
    config_path = os.fspath(path)
    if not os.path.exists(config_path):
        raise _fail("configuration file does not exist")
    _ensure_regular_readable(config_path, "configuration file")

    data = _read_json_object(config_path, "configuration file")
    for required in ("remote_url", "credentials_file", "bindings"):
        if required not in data:
            raise _fail(f"missing required configuration key: {required}")

    remote_url = _validate_remote_url(data["remote_url"])

    credentials_file = _require_absolute_path(
        data["credentials_file"], "credentials_file"
    )
    _ensure_regular_readable(credentials_file, "credentials_file")
    by_agent = _validate_credentials(credentials_file)

    ca_file: Optional[str] = None
    if data.get("ca_file") is not None:
        ca_file = _require_absolute_path(data["ca_file"], "ca_file")
        _ensure_regular_readable(ca_file, "ca_file")

    max_request_bytes = data.get("max_request_bytes", MAX_REQUEST_BYTES)
    if isinstance(max_request_bytes, bool) or not isinstance(max_request_bytes, int):
        raise _fail("'max_request_bytes' must be an integer")
    if max_request_bytes <= 0:
        raise _fail("'max_request_bytes' must be greater than zero")

    max_response_bytes = data.get("max_response_bytes", _DEFAULT_MAX_RESPONSE_BYTES)
    if isinstance(max_response_bytes, bool) or not isinstance(max_response_bytes, int):
        raise _fail("'max_response_bytes' must be an integer")
    if max_response_bytes <= 0:
        raise _fail("'max_response_bytes' must be greater than zero")

    return AdapterConfig(
        remote_url=remote_url,
        credentials_file=credentials_file,
        bindings=_validate_bindings(data["bindings"], by_agent),
        ca_file=ca_file,
        connect_timeout=_validate_positive_number(
            data.get("connect_timeout"), "connect_timeout", _DEFAULT_CONNECT_TIMEOUT
        ),
        read_timeout=_validate_positive_number(
            data.get("read_timeout"), "read_timeout", _DEFAULT_READ_TIMEOUT
        ),
        max_request_bytes=max_request_bytes,
        max_response_bytes=max_response_bytes,
    )
