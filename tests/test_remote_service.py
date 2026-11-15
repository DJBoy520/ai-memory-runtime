"""Tests for src.interfaces.mcp.remote_service.load_config validation."""

import json
import os

import pytest

from src.interfaces.mcp import remote_auth
from src.interfaces.mcp.remote_auth import TokenIdentity
from src.interfaces.mcp.remote_service import (
    TokenAuthenticator,
    load_config,
)


CREDENTIALS_CONTENT = {
    "test-secret": {
        "agent_id": "claude_code",
        "tools": ["memory_search"],
    }
}


def _build_config(tmp_path, credentials_file, cert_file, key_file):
    """Return a fully-populated, otherwise-valid config dict."""
    return {
        "business_socket": str(tmp_path / "business.sock"),
        "admin_socket": str(tmp_path / "admin.sock"),
        "credentials_file": str(credentials_file),
        "cert_file": str(cert_file),
        "key_file": str(key_file),
        "allowed_hosts": ["localhost", "127.0.0.1"],
    }


def _write_config(tmp_path, config):
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    return config_path


def _credentials_file(tmp_path, mode=0o600, content=None):
    creds = tmp_path / "credentials.json"
    creds.write_text(
        json.dumps(CREDENTIALS_CONTENT if content is None else content),
        encoding="utf-8",
    )
    os.chmod(creds, mode)
    return creds


def _tls_files(tmp_path):
    cert_file = tmp_path / "server.crt"
    key_file = tmp_path / "server.key"
    cert_file.write_text("dummy-cert", encoding="utf-8")
    key_file.write_text("dummy-key", encoding="utf-8")
    os.chmod(key_file, 0o600)
    return cert_file, key_file


def _valid_config_path(tmp_path, credentials_file=None, content=None):
    creds = credentials_file or _credentials_file(tmp_path, content=content)
    cert_file, key_file = _tls_files(tmp_path)
    config = _build_config(tmp_path, creds, cert_file, key_file)
    return _write_config(tmp_path, config)


def test_remote_service_reuses_remote_auth_token_authenticator():
    """remote_service 不得重复定义 TokenAuthenticator。"""
    assert TokenAuthenticator is remote_auth.TokenAuthenticator


def test_valid_credentials_return_real_token_identity(tmp_path):
    """有效凭证必须返回真实 TokenIdentity，而不是普通字典。"""
    config_path = _valid_config_path(tmp_path)

    config = load_config(str(config_path))

    assert isinstance(config.authenticator, remote_auth.TokenAuthenticator)
    identity = config.authenticator.authenticate("test-secret")

    assert type(identity) is TokenIdentity
    assert identity.agent_id == "claude_code"
    assert identity.tools == ("memory_search",)
    assert identity.allows_tool("memory_search") is True
    assert identity.allows_tool("memory_create") is False
    assert config.authenticator.authenticate("wrong-token") is None


def test_valid_credentials_keep_per_token_tool_permissions(tmp_path):
    """每个令牌的工具权限必须独立，不得互相串用。"""
    content = {
        "token-claude": {
            "agent_id": "claude_code",
            "tools": ["memory_search", "memory_get"],
        },
        "token-hermes": {
            "agent_id": "hermes",
            "tools": ["memory_create"],
        },
    }
    config_path = _valid_config_path(tmp_path, content=content)

    config = load_config(str(config_path))
    claude = config.authenticator.authenticate("token-claude")
    hermes = config.authenticator.authenticate("token-hermes")

    assert type(claude) is TokenIdentity
    assert type(hermes) is TokenIdentity

    assert claude.agent_id == "claude_code"
    assert claude.tools == ("memory_search", "memory_get")
    assert claude.allows_tool("memory_search") is True
    assert claude.allows_tool("memory_get") is True
    assert claude.allows_tool("memory_create") is False

    assert hermes.agent_id == "hermes"
    assert hermes.tools == ("memory_create",)
    assert hermes.allows_tool("memory_create") is True
    assert hermes.allows_tool("memory_search") is False
    assert hermes.allows_tool("memory_get") is False


def test_business_and_admin_socket_symlink_aliases_are_rejected(tmp_path):
    """真实路径相同的 business/admin socket 必须被拒绝。"""
    creds = _credentials_file(tmp_path)
    cert_file, key_file = _tls_files(tmp_path)

    admin_socket = tmp_path / "admin.sock"
    business_alias = tmp_path / "business-alias.sock"
    os.symlink(admin_socket, business_alias)

    config = _build_config(tmp_path, creds, cert_file, key_file)
    config["business_socket"] = str(business_alias)
    config["admin_socket"] = str(admin_socket)
    config_path = _write_config(tmp_path, config)

    with pytest.raises(ValueError):
        load_config(str(config_path))


@pytest.mark.parametrize("mode", [0o640, 0o604, 0o660, 0o606, 0o666])
def test_credentials_file_group_or_other_accessible_is_rejected(tmp_path, mode):
    creds = _credentials_file(tmp_path, mode=mode)
    cert_file, key_file = _tls_files(tmp_path)

    config = _build_config(tmp_path, creds, cert_file, key_file)
    config_path = _write_config(tmp_path, config)

    with pytest.raises(ValueError):
        load_config(str(config_path))


@pytest.mark.parametrize("mode", [0o600, 0o400])
def test_credentials_file_owner_only_permissions_are_accepted(tmp_path, mode):
    creds = _credentials_file(tmp_path, mode=mode)
    cert_file, key_file = _tls_files(tmp_path)

    config = _build_config(tmp_path, creds, cert_file, key_file)
    config_path = _write_config(tmp_path, config)

    config = load_config(str(config_path))

    identity = config.authenticator.authenticate("test-secret")
    assert type(identity) is TokenIdentity
    assert identity.allows_tool("memory_search") is True


def test_missing_cert_file_raises_value_error(tmp_path):
    creds = _credentials_file(tmp_path)
    _, key_file = _tls_files(tmp_path)
    missing_cert = tmp_path / "does-not-exist.crt"

    config = _build_config(tmp_path, creds, missing_cert, key_file)
    config_path = _write_config(tmp_path, config)

    with pytest.raises(ValueError):
        load_config(str(config_path))


def test_credentials_file_with_0644_permissions_raises_value_error(tmp_path):
    creds = _credentials_file(tmp_path, mode=0o644)
    cert_file, key_file = _tls_files(tmp_path)

    config = _build_config(tmp_path, creds, cert_file, key_file)
    config_path = _write_config(tmp_path, config)

    with pytest.raises(ValueError):
        load_config(str(config_path))


def test_missing_credentials_file_raises_value_error(tmp_path):
    missing_creds = tmp_path / "missing-credentials.json"
    cert_file, key_file = _tls_files(tmp_path)

    config = _build_config(tmp_path, missing_creds, cert_file, key_file)
    config_path = _write_config(tmp_path, config)

    with pytest.raises(ValueError):
        load_config(str(config_path))


# ---------------------------------------------------------------------------
# 必填字段：缺键 / 空值 / 错误类型一律拒绝（最终字段名契约）。
# ---------------------------------------------------------------------------

_REQUIRED_CONFIG_FIELDS = (
    "business_socket",
    "admin_socket",
    "credentials_file",
    "cert_file",
    "key_file",
    "allowed_hosts",
)


@pytest.mark.parametrize("missing_field", _REQUIRED_CONFIG_FIELDS)
def test_missing_required_config_key_raises_value_error(tmp_path, missing_field):
    """JSON 中缺少任一必填字段都必须拒绝，不得推断默认值。"""
    creds = _credentials_file(tmp_path)
    cert_file, key_file = _tls_files(tmp_path)

    config = _build_config(tmp_path, creds, cert_file, key_file)
    del config[missing_field]
    config_path = _write_config(tmp_path, config)

    with pytest.raises(ValueError):
        load_config(str(config_path))


@pytest.mark.parametrize("field", ["cert_file", "key_file"])
def test_tls_path_that_is_a_directory_raises_value_error(tmp_path, field):
    """TLS 路径必须是普通文件，同名目录不得被当成证书/私钥。"""
    creds = _credentials_file(tmp_path)
    cert_file, key_file = _tls_files(tmp_path)

    target = cert_file if field == "cert_file" else key_file
    target.unlink()
    target.mkdir()

    config = _build_config(tmp_path, creds, cert_file, key_file)
    config_path = _write_config(tmp_path, config)

    with pytest.raises(ValueError):
        load_config(str(config_path))


@pytest.mark.parametrize("field", ["business_socket", "admin_socket"])
@pytest.mark.parametrize("value", ["", "   ", "relative.sock", None])
def test_invalid_socket_path_raises_value_error(tmp_path, field, value):
    """业务/管理 socket 必须是非空绝对路径。"""
    creds = _credentials_file(tmp_path)
    cert_file, key_file = _tls_files(tmp_path)

    config = _build_config(tmp_path, creds, cert_file, key_file)
    config[field] = value
    config_path = _write_config(tmp_path, config)

    with pytest.raises(ValueError):
        load_config(str(config_path))


@pytest.mark.parametrize("field", ["cert_file", "key_file", "credentials_file"])
@pytest.mark.parametrize("blank", ["", "   ", None])
def test_blank_required_file_path_raises_value_error(tmp_path, field, blank):
    """证书/私钥/凭证路径不得为空。"""
    creds = _credentials_file(tmp_path)
    cert_file, key_file = _tls_files(tmp_path)

    config = _build_config(tmp_path, creds, cert_file, key_file)
    config[field] = blank
    config_path = _write_config(tmp_path, config)

    with pytest.raises(ValueError):
        load_config(str(config_path))


# ---------------------------------------------------------------------------
# 凭证文件内容：空文件 / 非法 JSON / 非法条目。
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "body",
    ["", "not json", "{", "[]", '"string"', "null", "42"],
)
def test_credentials_file_must_be_a_valid_json_object(tmp_path, body):
    """凭证文件必须是非空、合法 JSON 对象。"""
    creds = tmp_path / "credentials.json"
    creds.write_text(body, encoding="utf-8")
    os.chmod(creds, 0o600)
    cert_file, key_file = _tls_files(tmp_path)

    config = _build_config(tmp_path, creds, cert_file, key_file)
    config_path = _write_config(tmp_path, config)

    with pytest.raises(ValueError):
        load_config(str(config_path))


@pytest.mark.parametrize(
    "entry",
    [
        {"tools": ["memory_search"]},
        {"agent_id": "claude_code"},
        {"agent_id": "claude_code", "tools": []},
        {"agent_id": "claude_code", "tools": "memory_search"},
        {"agent_id": "claude_code", "tools": [""]},
        {"agent_id": "claude_code", "tools": [None]},
        {"agent_id": "", "tools": ["memory_search"]},
        {"agent_id": 123, "tools": ["memory_search"]},
        "not-an-object",
    ],
)
def test_invalid_credentials_entry_raises_value_error(tmp_path, entry):
    """每条凭证必须包含有效 agent_id 与非空 tools 列表。"""
    config_path = _valid_config_path(tmp_path, content={"test-secret": entry})

    with pytest.raises(ValueError):
        load_config(str(config_path))


# ---------------------------------------------------------------------------
# allowed_hosts 与配置读取边界。
# ---------------------------------------------------------------------------


def test_empty_allowed_hosts_raises_value_error(tmp_path):
    """空 allowed_hosts 必须拒绝，不得退化为无限制。"""
    creds = _credentials_file(tmp_path)
    cert_file, key_file = _tls_files(tmp_path)

    config = _build_config(tmp_path, creds, cert_file, key_file)
    config["allowed_hosts"] = []
    config_path = _write_config(tmp_path, config)

    with pytest.raises(ValueError):
        load_config(str(config_path))


@pytest.mark.parametrize(
    "allowed_hosts",
    ["", "localhost", {"localhost": True}, [""], ["  "], [None], [123]],
)
def test_invalid_allowed_hosts_raises_value_error(tmp_path, allowed_hosts):
    """allowed_hosts 必须是非空 host 字符串列表。"""
    creds = _credentials_file(tmp_path)
    cert_file, key_file = _tls_files(tmp_path)

    config = _build_config(tmp_path, creds, cert_file, key_file)
    config["allowed_hosts"] = allowed_hosts
    config_path = _write_config(tmp_path, config)

    with pytest.raises(ValueError):
        load_config(str(config_path))


def test_allowed_hosts_are_not_silently_widened(tmp_path):
    """窄列表必须原样保留，不得被扩成通配符或空值。"""
    creds = _credentials_file(tmp_path)
    cert_file, key_file = _tls_files(tmp_path)

    config = _build_config(tmp_path, creds, cert_file, key_file)
    config["allowed_hosts"] = ["127.0.0.1"]
    config_path = _write_config(tmp_path, config)

    loaded = load_config(str(config_path))

    assert list(loaded.allowed_hosts) == ["127.0.0.1"]


# ---------------------------------------------------------------------------
# 可选网络参数：bind_host / bind_port / allowed_origins。
# ---------------------------------------------------------------------------


def test_bind_defaults_are_loopback_and_8443(tmp_path):
    """未显式配置时只监听回环地址与固定默认端口。"""
    loaded = load_config(str(_valid_config_path(tmp_path)))

    assert loaded.bind_host == "127.0.0.1"
    assert loaded.bind_port == 8443
    assert loaded.allowed_origins is None


def test_bind_and_origins_are_read_explicitly(tmp_path):
    """显式配置必须原样读取，不做推断。"""
    creds = _credentials_file(tmp_path)
    cert_file, key_file = _tls_files(tmp_path)

    config = _build_config(tmp_path, creds, cert_file, key_file)
    config.update(
        bind_host="10.0.0.5",
        bind_port=9443,
        allowed_origins=["https://amr.example.test", "https://amr.example.test:8443"],
    )
    loaded = load_config(str(_write_config(tmp_path, config)))

    assert loaded.bind_host == "10.0.0.5"
    assert loaded.bind_port == 9443
    assert list(loaded.allowed_origins) == [
        "https://amr.example.test",
        "https://amr.example.test:8443",
    ]


@pytest.mark.parametrize(
    "bind_host",
    ["", "   ", " 127.0.0.1", "127.0.0.1 ", "host name", "http://localhost", "/tmp/x", None, 123],
)
def test_invalid_bind_host_raises_value_error(tmp_path, bind_host):
    """bind_host 必须是裸主机名/IP，不得含空白、URL 或路径语法。"""
    creds = _credentials_file(tmp_path)
    cert_file, key_file = _tls_files(tmp_path)

    config = _build_config(tmp_path, creds, cert_file, key_file)
    config["bind_host"] = bind_host
    config_path = _write_config(tmp_path, config)

    with pytest.raises(ValueError):
        load_config(str(config_path))


@pytest.mark.parametrize(
    "bind_port", [-1, 65536, 70000, "8443", 8443.0, True, None]
)
def test_invalid_bind_port_raises_value_error(tmp_path, bind_port):
    """bind_port 必须是 0..65535 的整数，bool/浮点/字符串一律拒绝。"""
    creds = _credentials_file(tmp_path)
    cert_file, key_file = _tls_files(tmp_path)

    config = _build_config(tmp_path, creds, cert_file, key_file)
    config["bind_port"] = bind_port
    config_path = _write_config(tmp_path, config)

    with pytest.raises(ValueError):
        load_config(str(config_path))


@pytest.mark.parametrize("bind_port", [0, 1, 8443, 65535])
def test_valid_bind_port_is_accepted(tmp_path, bind_port):
    creds = _credentials_file(tmp_path)
    cert_file, key_file = _tls_files(tmp_path)

    config = _build_config(tmp_path, creds, cert_file, key_file)
    config["bind_port"] = bind_port
    loaded = load_config(str(_write_config(tmp_path, config)))

    assert loaded.bind_port == bind_port


@pytest.mark.parametrize(
    "allowed_origins",
    [[], "", [""], [None], [123], "https://localhost", {"a": 1}, None],
)
def test_invalid_allowed_origins_raises_value_error(tmp_path, allowed_origins):
    """allowed_origins 出现时必须是非空 list[str]。"""
    creds = _credentials_file(tmp_path)
    cert_file, key_file = _tls_files(tmp_path)

    config = _build_config(tmp_path, creds, cert_file, key_file)
    config["allowed_origins"] = allowed_origins
    config_path = _write_config(tmp_path, config)

    with pytest.raises(ValueError):
        load_config(str(config_path))


def test_unknown_extra_config_keys_are_ignored(tmp_path):
    """向前兼容：新增未知键不应让已有配置无法加载。"""
    creds = _credentials_file(tmp_path)
    cert_file, key_file = _tls_files(tmp_path)

    config = _build_config(tmp_path, creds, cert_file, key_file)
    config["future_option"] = {"nested": True}
    config_path = _write_config(tmp_path, config)

    loaded = load_config(str(config_path))

    assert loaded.credentials_file == str(creds)


def test_pathlike_config_path_is_accepted(tmp_path):
    """load_config 同时接受 str 与 os.PathLike 配置路径。"""
    config_path = _valid_config_path(tmp_path)

    loaded = load_config(config_path)

    identity = loaded.authenticator.authenticate("test-secret")
    assert type(identity) is TokenIdentity


def test_missing_config_file_raises_value_error(tmp_path):
    with pytest.raises(ValueError):
        load_config(str(tmp_path / "missing-config.json"))


@pytest.mark.parametrize("body", ["", "not json", "{", "[]", "null", "42"])
def test_invalid_config_file_content_raises_value_error(tmp_path, body):
    """配置文件本身必须是合法 JSON 对象。"""
    config_path = tmp_path / "config.json"
    config_path.write_text(body, encoding="utf-8")

    with pytest.raises(ValueError):
        load_config(str(config_path))


def test_load_config_does_not_create_sockets_or_modify_credentials(tmp_path):
    """配置校验只读取文件，不建 socket、不改动凭证。"""
    creds = _credentials_file(tmp_path)
    cert_file, key_file = _tls_files(tmp_path)
    config_path = _write_config(
        tmp_path, _build_config(tmp_path, creds, cert_file, key_file)
    )
    before = creds.read_bytes()

    load_config(str(config_path))

    assert creds.read_bytes() == before
    assert not (tmp_path / "business.sock").exists()
    assert not (tmp_path / "admin.sock").exists()


def test_token_value_is_not_exposed_in_config(tmp_path):
    """配置对象只持有路径，不承载或渲染 token 明文。"""
    loaded = load_config(str(_valid_config_path(tmp_path)))

    rendered = repr(loaded) + str(loaded)
    assert "test-secret" not in rendered
