import json
from types import SimpleNamespace
import pytest
from src.interfaces.ipc.auth import authorize_agent
from src.interfaces.ipc.errors import RpcAgentUnauthorized, RpcRegistryUnavailable


def test_daemon_identity_list_tracks_active_credentials_and_fails_closed(tmp_path, monkeypatch):
    credentials = tmp_path/'credentials.json'
    credentials.write_text(json.dumps({'a'*43:{'agent_id':'new_agent','tools':['memory_search']}})); credentials.chmod(0o600)
    monkeypatch.setenv('AMR_AGENT_CREDENTIALS_FILE',str(credentials))
    cfg = SimpleNamespace(server=SimpleNamespace(allowed_agents=['system','old_agent']))
    assert authorize_agent(cfg, {'agent_id':'new_agent'})[0] == 'new_agent'
    with pytest.raises(RpcAgentUnauthorized): authorize_agent(cfg, {'agent_id':'old_agent'})
    assert authorize_agent(cfg, {'agent_id':'system'})[0] == 'system'
    credentials.unlink()
    with pytest.raises(RpcRegistryUnavailable): authorize_agent(cfg, {'agent_id':'new_agent'})
