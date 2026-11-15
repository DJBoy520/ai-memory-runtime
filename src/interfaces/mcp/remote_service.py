"""MCP remote service configuration validation.

This module only performs *offline* validation of a JSON configuration file.
It never opens sockets, starts listeners or performs network I/O.

All validation failures are reported as :class:`ValueError` and error messages
never contain secret material (tokens, file contents, ...).
"""

from __future__ import annotations

import json
import os
import stat
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .remote_auth import MIN_TOKEN_LENGTH, TokenAuthenticator

__all__ = ["TokenAuthenticator", "RemoteServiceConfig", "load_config"]

_REQUIRED_KEYS: Tuple[str, ...] = (
    "business_socket",
    "admin_socket",
    "credentials_file",
    "cert_file",
    "key_file",
    "allowed_hosts",
)

# 可选网络参数的安全默认值：只监听回环地址，端口不与任何生产配置关联。
_DEFAULT_BIND_HOST = "127.0.0.1"
_DEFAULT_BIND_PORT = 8443

# Credentials must not be group/world readable.
_ALLOWED_CREDENTIAL_MODES = frozenset({0o600, 0o400})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fail(message: str) -> "ValueError":
    """Build a ValueError without ever embedding secret values."""
    return ValueError(message)


def _as_absolute_path(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _fail(f"'{field}' must be a non-empty path string")
    if not os.path.isabs(value):
        raise _fail(f"'{field}' must be an absolute path")
    return os.path.normpath(value)


def _as_file_path(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _fail(f"'{field}' must be a non-empty path string")
    return value


def _ensure_readable_regular_file(path: str, field: str) -> None:
    """Validate that *path* is an existing, readable regular file."""
    if not os.path.exists(path):
        raise _fail(f"'{field}' does not exist")
    try:
        info = os.stat(path)
    except OSError:
        raise _fail(f"'{field}' cannot be inspected")
    if not stat.S_ISREG(info.st_mode):
        raise _fail(f"'{field}' must be a regular file")
    if not os.access(path, os.R_OK):
        raise _fail(f"'{field}' is not readable")
    # Confirm readability without retaining any file content.
    try:
        with open(path, "rb") as handle:
            handle.read(1)
    except OSError:
        raise _fail(f"'{field}' is not readable")


def _read_json_object(path: str, field: str) -> Dict[str, Any]:
    try:
        text = Path(path).read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        raise _fail(f"'{field}' cannot be read as UTF-8 text")
    if not text.strip():
        raise _fail(f"'{field}' must not be empty")
    try:
        data = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        raise _fail(f"'{field}' is not valid JSON")
    if not isinstance(data, dict):
        raise _fail(f"'{field}' must contain a JSON object")
    return data


def _validate_bind_host(value: Any) -> str:
    """Validate an explicit listen host (IPv4, IPv6, or host name).

    The value must already be a bare host: no surrounding whitespace, no
    embedded whitespace/control characters and no URL or path syntax.  The
    address family is deliberately not guessed here; binding is the
    responsibility of the server, and an invalid host simply fails there.
    """
    if not isinstance(value, str) or not value:
        raise _fail("'bind_host' must be a non-empty string")
    if value != value.strip():
        raise _fail("'bind_host' must not have surrounding whitespace")
    if any(ch.isspace() or ord(ch) < 0x20 or ord(ch) == 0x7F for ch in value):
        raise _fail("'bind_host' must not contain whitespace or control characters")
    if "/" in value or "://" in value:
        raise _fail("'bind_host' must be a bare host name or IP address")
    return value


def _validate_bind_port(value: Any) -> int:
    """Validate the TCP port; ``bool`` is rejected even though it is an int."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise _fail("'bind_port' must be an integer")
    if value < 0 or value > 65535:
        raise _fail("'bind_port' must be between 0 and 65535")
    return value


def _validate_allowed_origins(value: Any) -> List[str]:
    """Type/non-empty checks only; strict origin syntax is enforced by build_app."""
    if not isinstance(value, list) or not value:
        raise _fail("'allowed_origins' must be a non-empty list of origins")
    origins: List[str] = []
    for origin in value:
        if not isinstance(origin, str) or not origin:
            raise _fail("'allowed_origins' must contain non-empty strings")
        origins.append(origin)
    return origins


# ---------------------------------------------------------------------------
# Validated configuration container
# ---------------------------------------------------------------------------


class RemoteServiceConfig(dict):
    """Validated configuration supporting both key and attribute access.

    ``bind_host`` defaults to the loopback interface and ``bind_port`` to 8443;
    neither value is ever inferred from production sockets or files.
    ``allowed_origins`` is optional and ``None`` keeps :func:`build_app`'s
    loopback-only default.
    """

    def __init__(
        self,
        *,
        business_socket: str,
        admin_socket: str,
        credentials_file: str,
        cert_file: str,
        key_file: str,
        allowed_hosts: List[str],
        authenticator: TokenAuthenticator,
        bind_host: str = _DEFAULT_BIND_HOST,
        bind_port: int = _DEFAULT_BIND_PORT,
        allowed_origins: Optional[List[str]] = None,
    ):
        super().__init__(
            business_socket=business_socket,
            admin_socket=admin_socket,
            credentials_file=credentials_file,
            cert_file=cert_file,
            key_file=key_file,
            allowed_hosts=list(allowed_hosts),
            bind_host=_validate_bind_host(bind_host),
            bind_port=_validate_bind_port(bind_port),
            allowed_origins=(
                None if allowed_origins is None else _validate_allowed_origins(allowed_origins)
            ),
        )
        self.authenticator = authenticator

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name) from None

    def to_dict(self) -> Dict[str, Any]:
        """Return the non-secret configuration fields as a plain dict."""
        return {key: list(value) if isinstance(value, list) else value
                for key, value in self.items()}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_config(path: Any) -> RemoteServiceConfig:
    """Load and validate an MCP remote service JSON configuration.

    All of ``business_socket``, ``admin_socket``, ``credentials_file``,
    ``cert_file``, ``key_file`` and ``allowed_hosts`` are mandatory. Paths for
    the business/admin sockets must be absolute and distinct; TLS files must be
    readable regular files; the credentials file must be mode 0600/0400 and
    contain a non-empty ``token -> {agent_id, tools}`` JSON object which is
    loaded into a :class:`TokenAuthenticator`.

    Optional keys ``bind_host`` (default ``127.0.0.1``), ``bind_port`` (default
    ``8443``) and ``allowed_origins`` (default ``None``) are validated when
    present.  An explicitly present ``allowed_origins`` must be a non-empty
    list of non-empty strings; origin syntax is validated by
    :func:`build_app` when the application is constructed.

    Raises:
        ValueError: for any invalid or missing configuration.
    """
    if not isinstance(path, (str, os.PathLike)) or not os.fspath(path):
        raise _fail("configuration path must be a non-empty path")

    config_path = os.fspath(path)
    if not os.path.exists(config_path):
        raise _fail("configuration file does not exist")
    _ensure_readable_regular_file(config_path, "configuration file")

    data = _read_json_object(config_path, "configuration file")

    missing = [key for key in _REQUIRED_KEYS if key not in data]
    if missing:
        raise _fail("missing required configuration key(s): " + ", ".join(missing))

    business_socket = _as_absolute_path(data["business_socket"], "business_socket")
    admin_socket = _as_absolute_path(data["admin_socket"], "admin_socket")
    if os.path.realpath(business_socket) == os.path.realpath(admin_socket):
        raise _fail("'business_socket' and 'admin_socket' must be different paths")

    cert_file = _as_file_path(data["cert_file"], "cert_file")
    key_file = _as_file_path(data["key_file"], "key_file")
    _ensure_readable_regular_file(cert_file, "cert_file")
    _ensure_readable_regular_file(key_file, "key_file")

    credentials_file = _as_file_path(data["credentials_file"], "credentials_file")
    _ensure_readable_regular_file(credentials_file, "credentials_file")

    try:
        mode = stat.S_IMODE(os.stat(credentials_file).st_mode)
    except OSError:
        raise _fail("'credentials_file' cannot be inspected")
    if mode not in _ALLOWED_CREDENTIAL_MODES:
        raise _fail("'credentials_file' must have permissions 0600 or 0400")

    raw_credentials = _read_json_object(credentials_file, "credentials_file")
    if not raw_credentials:
        raise _fail("credentials file must contain at least one token entry")
    for token in raw_credentials:
        # 只做长度策略；令牌字符集/身份/工具列表由 TokenAuthenticator 统一校验，
        # 避免在这里出现第二套认证实现。
        if not isinstance(token, str) or len(token) < MIN_TOKEN_LENGTH:
            raise _fail(
                "credentials file contains a token shorter than "
                f"{MIN_TOKEN_LENGTH} characters"
            )
    authenticator = TokenAuthenticator(raw_credentials)

    allowed_hosts_raw = data["allowed_hosts"]
    if not isinstance(allowed_hosts_raw, list) or not allowed_hosts_raw:
        raise _fail("'allowed_hosts' must be a non-empty list of host names")
    allowed_hosts: List[str] = []
    for host in allowed_hosts_raw:
        if (
            not isinstance(host, str)
            or not host
            or host != host.strip()
            or any(ch.isspace() or ord(ch) < 0x20 or ord(ch) == 0x7F for ch in host)
        ):
            raise _fail("'allowed_hosts' must contain non-empty host names")
        allowed_hosts.append(host)

    bind_host = (
        _validate_bind_host(data["bind_host"])
        if "bind_host" in data
        else _DEFAULT_BIND_HOST
    )
    bind_port = (
        _validate_bind_port(data["bind_port"])
        if "bind_port" in data
        else _DEFAULT_BIND_PORT
    )
    allowed_origins = (
        _validate_allowed_origins(data["allowed_origins"])
        if "allowed_origins" in data
        else None
    )

    return RemoteServiceConfig(
        business_socket=business_socket,
        admin_socket=admin_socket,
        credentials_file=credentials_file,
        cert_file=cert_file,
        key_file=key_file,
        allowed_hosts=allowed_hosts,
        authenticator=authenticator,
        bind_host=bind_host,
        bind_port=bind_port,
        allowed_origins=allowed_origins,
    )
