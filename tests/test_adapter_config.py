"""2.x adapter 配置契约测试（只读、无 socket/网络）。"""

import json
import os

import pytest

from src.interfaces.adapter.config import load_adapter_config
from src.interfaces.mcp.remote_auth import MIN_TOKEN_LENGTH

TOKEN = "t" * 40
TOKEN_2 = "u" * 40
AGENT = "openclaw"
AGENT_2 = "hermes"
CREDENTIALS = {
    TOKEN: {"agent_id": AGENT, "tools": ["memory_search"]},
    TOKEN_2: {"agent_id": AGENT_2, "tools": ["memory_search", "memory_update"]},
}


def _write(path, data, mode=None):
    path.write_text(json.dumps(data), encoding="utf-8")
    if mode is not None:
        os.chmod(path, mode)
    return path


def _credentials(tmp_path, content=None, mode=0o600):
    return _write(tmp_path / "credentials.json", CREDENTIALS if content is None else content, mode)


def _config_payload(tmp_path, credentials_file, **overrides):
    payload = {
        "remote_url": "https://amr.internal:8443/mcp",
        "credentials_file": str(credentials_file),
        "bindings": [
            {"socket_path": str(tmp_path / "run" / "openclaw.sock"), "agent_id": AGENT},
        ],
    }
    payload.update(overrides)
    return payload


def _load(tmp_path, payload):
    return load_adapter_config(str(_write(tmp_path / "adapter.json", payload)))


def test_valid_config_loads_bindings_and_hides_tokens(tmp_path):
    config = _load(tmp_path, _config_payload(tmp_path, _credentials(tmp_path)))

    assert config.remote_url == "https://amr.internal:8443/mcp"
    assert len(config.bindings) == 1
    binding = config.bindings[0]
    assert binding.agent_id == AGENT
    assert binding.tools == ("memory_search",)
    assert binding.token == TOKEN
    assert TOKEN not in repr(config)
    assert TOKEN not in str(config)


@pytest.mark.parametrize(
    "url",
    [
        "http://amr.internal:8443/mcp",
        "https:///mcp",
        "https://user:pass@amr.internal/mcp",
        "https://amr.internal/mcp?x=1",
        "https://amr.internal/mcp#frag",
        " https://amr.internal/mcp",
        "https://amr.internal/mcp ",
        "",
        None,
    ],
)
def test_remote_url_must_be_https_without_userinfo_query_fragment(tmp_path, url):
    with pytest.raises(ValueError):
        _load(tmp_path, _config_payload(tmp_path, _credentials(tmp_path), remote_url=url))


def test_remote_url_without_path_defaults_to_mcp(tmp_path):
    config = _load(
        tmp_path,
        _config_payload(
            tmp_path, _credentials(tmp_path), remote_url="https://amr.internal:8443"
        ),
    )
    assert config.remote_url == "https://amr.internal:8443/mcp"


@pytest.mark.parametrize("mode", [0o644, 0o640, 0o604, 0o660])
def test_credentials_permissions_are_enforced(tmp_path, mode):
    with pytest.raises(ValueError):
        _load(tmp_path, _config_payload(tmp_path, _credentials(tmp_path, mode=mode)))


@pytest.mark.parametrize(
    "content",
    [
        {},
        "not-json",
        {TOKEN: {"agent_id": AGENT, "tools": []}},
        {TOKEN[: MIN_TOKEN_LENGTH - 1]: {"agent_id": AGENT, "tools": ["memory_search"]}},
        {TOKEN: {"tools": ["memory_search"]}},
        {TOKEN: {"agent_id": AGENT}},
    ],
)
def test_invalid_credentials_are_rejected(tmp_path, content):
    creds = tmp_path / "credentials.json"
    if isinstance(content, str):
        creds.write_text(content, encoding="utf-8")
        os.chmod(creds, 0o600)
    else:
        _write(creds, content, 0o600)
    with pytest.raises(ValueError):
        _load(tmp_path, _config_payload(tmp_path, creds))


def test_binding_without_token_is_rejected(tmp_path):
    payload = _config_payload(tmp_path, _credentials(tmp_path))
    payload["bindings"] = [
        {"socket_path": str(tmp_path / "run" / "unknown.sock"), "agent_id": "unknown"}
    ]
    with pytest.raises(ValueError):
        _load(tmp_path, payload)


def test_binding_with_multiple_tokens_for_same_agent_is_rejected(tmp_path):
    creds = _credentials(
        tmp_path,
        {
            TOKEN: {"agent_id": AGENT, "tools": ["memory_search"]},
            TOKEN_2: {"agent_id": AGENT, "tools": ["memory_search"]},
        },
    )
    with pytest.raises(ValueError):
        _load(tmp_path, _config_payload(tmp_path, creds))


def test_duplicate_socket_paths_are_rejected(tmp_path):
    payload = _config_payload(tmp_path, _credentials(tmp_path))
    payload["bindings"].append(dict(payload["bindings"][0], agent_id=AGENT_2))
    with pytest.raises(ValueError):
        _load(tmp_path, payload)


def test_duplicate_agent_ids_are_rejected(tmp_path):
    payload = _config_payload(tmp_path, _credentials(tmp_path))
    payload["bindings"].append(
        {"socket_path": str(tmp_path / "run" / "other.sock"), "agent_id": AGENT}
    )
    with pytest.raises(ValueError):
        _load(tmp_path, payload)


def test_symlink_alias_socket_paths_are_rejected(tmp_path):
    run = tmp_path / "run"
    run.mkdir()
    first = run / "openclaw.sock"
    second = run / "alias.sock"
    os.symlink(first, second)
    payload = _config_payload(tmp_path, _credentials(tmp_path))
    payload["bindings"] = [
        {"socket_path": str(first), "agent_id": AGENT},
        {"socket_path": str(second), "agent_id": AGENT_2},
    ]
    with pytest.raises(ValueError):
        _load(tmp_path, payload)


@pytest.mark.parametrize("field,value", [
    ("connect_timeout", 0),
    ("connect_timeout", -1),
    ("connect_timeout", True),
    ("connect_timeout", "5"),
    ("connect_timeout", float("inf")),
    ("read_timeout", 0),
    ("read_timeout", float("nan")),
    ("max_request_bytes", 0),
    ("max_request_bytes", -5),
    ("max_request_bytes", "4MB"),
    ("max_response_bytes", 0),
    ("max_response_bytes", -1),
    ("max_response_bytes", True),
    ("max_response_bytes", "8MB"),
])
def test_invalid_timeouts_and_limits_are_rejected(tmp_path, field, value):
    with pytest.raises(ValueError):
        _load(
            tmp_path,
            _config_payload(tmp_path, _credentials(tmp_path), **{field: value}),
        )


def test_missing_ca_file_is_rejected(tmp_path):
    with pytest.raises(ValueError):
        _load(
            tmp_path,
            _config_payload(
                tmp_path,
                _credentials(tmp_path),
                ca_file=str(tmp_path / "missing-ca.pem"),
            ),
        )


def test_config_does_not_create_sockets(tmp_path):
    socket_path = tmp_path / "run" / "openclaw.sock"
    _load(tmp_path, _config_payload(tmp_path, _credentials(tmp_path)))

    assert not socket_path.exists()


def test_token_is_not_echoed_in_error_messages(tmp_path):
    payload = _config_payload(tmp_path, _credentials(tmp_path))
    payload["bindings"][0]["agent_id"] = "unknown"
    with pytest.raises(ValueError) as excinfo:
        _load(tmp_path, payload)
    assert TOKEN not in str(excinfo.value)


def test_oversized_config_or_credentials_are_rejected(tmp_path):
    credentials = tmp_path / "credentials.json"
    credentials.write_text("{" + "x" * (1024 * 1024 + 1), encoding="utf-8")
    os.chmod(credentials, 0o600)
    with pytest.raises(ValueError):
        _load(tmp_path, _config_payload(tmp_path, credentials))

    payload_path = tmp_path / "adapter.json"
    payload_path.write_text("{" + "y" * (1024 * 1024 + 1), encoding="utf-8")
    with pytest.raises(ValueError):
        load_adapter_config(str(payload_path))


def test_empty_bindings_allowed_for_https_only_deployment(tmp_path):
    config = _load(tmp_path, _config_payload(tmp_path, _credentials(tmp_path), bindings=[]))
    assert config.bindings == ()
