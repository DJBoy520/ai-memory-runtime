import json
import os
import pytest
from src.agent_access.registry import Registry, RegistryError


def test_agent_lifecycle_and_independent_credentials(tmp_path):
    reg = Registry(tmp_path / 'private')
    reg.add('openclaw', ['memory_search', 'memory_ingest_session'], '/run/user/1000/amr2-openclaw.sock')
    reg.add('hermes', ['memory_search'], '/run/user/1000/amr2-hermes.sock')
    first = reg.snapshot()
    assert first['agents']['openclaw']['token'] != first['agents']['hermes']['token']
    assert len(first['agents']['openclaw']['token']) >= 43
    reg.rotate('openclaw')
    rotated = reg.snapshot()
    assert rotated['agents']['openclaw']['token'] != first['agents']['openclaw']['token']
    assert rotated['agents']['hermes']['token'] == first['agents']['hermes']['token']
    reg.set_tools('hermes', ['memory_create'])
    reg.disable('openclaw')
    exported = reg.render(tmp_path / 'output', {'remote_url':'https://localhost/mcp', 'ca_file':'/tmp/ca'})
    creds = json.loads((exported / 'credentials.json').read_text())
    assert len(creds) == 1
    assert next(iter(creds.values())) == {'agent_id':'hermes', 'tools':['memory_create']}
    assert not (exported / 'tokens' / 'openclaw.token').exists()
    assert (exported / 'tokens' / 'hermes.token').stat().st_mode & 0o777 == 0o600
    assert (exported / 'credentials.json').stat().st_mode & 0o777 == 0o600
    listing = reg.list_agents()
    assert 'token' not in json.dumps(listing)
    assert rotated['agents']['hermes']['token'] not in json.dumps(listing)


def test_invalid_inputs_and_duplicate_binding_are_rejected(tmp_path):
    reg = Registry(tmp_path / 'private')
    for ident in ('../escape', '', 'A', 'a/b'):
        with pytest.raises(RegistryError): reg.add(ident, ['memory_search'])
    with pytest.raises(RegistryError): reg.add('x', ['unknown_tool'])
    with pytest.raises(RegistryError): reg.add('x', [])
    reg.add('x', ['memory_search'], '/tmp/unique.sock')
    with pytest.raises(RegistryError): reg.add('x', ['memory_search'])
    with pytest.raises(RegistryError): reg.add('y', ['memory_search'], '/tmp/unique.sock')
    with pytest.raises(RegistryError): reg.set_tools('x', [])
    with pytest.raises(RegistryError): reg.disable('unknown')


def test_rejects_unsafe_registry_and_does_not_overwrite_export(tmp_path):
    reg = Registry(tmp_path / 'private')
    reg.add('x', ['memory_search'])
    assert (tmp_path / 'private').stat().st_mode & 0o777 == 0o700
    assert (tmp_path / 'private' / 'registry.json').stat().st_mode & 0o777 == 0o600
    out = tmp_path / 'output'; out.mkdir()
    with pytest.raises(RegistryError): reg.render(out, {'remote_url':'https://localhost/mcp'})
    os.chmod(tmp_path / 'private' / 'registry.json', 0o644)
    with pytest.raises(RegistryError): reg.snapshot()


def test_cli_does_not_print_secret_and_refuses_unknown_tools(tmp_path, capsys):
    from src.agent_access.cli import main
    root = str(tmp_path / 'private')
    assert main(['--registry',root,'add','hermes','--tools','memory_search']) == 0
    assert main(['--registry',root,'list']) == 0
    output = capsys.readouterr().out
    assert 'hermes' in output and 'memory_search' in output
    token = Registry(root).snapshot()['agents']['hermes']['token']
    assert token not in output
    assert main(['--registry',root,'grant','hermes','--tools','not_real']) != 0


def test_import_existing_is_all_or_nothing_and_preserves_tokens(tmp_path):
    cred = tmp_path / 'creds.json'
    token = 'A' * 43
    cred.write_text(json.dumps({token: {'agent_id':'hermes','tools':['memory_search']}}))
    cred.chmod(0o600)
    adapter = tmp_path / 'adapter.json'
    adapter.write_text(json.dumps({'bindings':[{'agent_id':'hermes','socket_path':'/tmp/hermes.sock'}]}))
    reg = Registry(tmp_path / 'private')
    reg.import_existing(cred, adapter)
    assert reg.snapshot()['agents']['hermes']['token'] == token
    assert reg.snapshot()['agents']['hermes']['socket'] == '/tmp/hermes.sock'
    with pytest.raises(RegistryError): reg.import_existing(cred, adapter)
    assert list(reg.list_agents()) == ['hermes']


def test_import_rejects_credentials_readable_by_others(tmp_path):
    cred = tmp_path / 'creds.json'
    cred.write_text('{}'); cred.chmod(0o644)
    reg = Registry(tmp_path / 'private')
    with pytest.raises(RegistryError): reg.import_existing(cred, None)
    assert not reg.list_agents()


def test_render_remote_snapshot_references_same_credentials(tmp_path):
    reg = Registry(tmp_path / 'private')
    reg.add('hermes',['memory_search'],'/tmp/hermes.sock')
    out = reg.render(tmp_path / 'generation', {'remote_url':'https://localhost/mcp'}, {'business_socket':'/tmp/daemon.sock'})
    remote = json.loads((out / 'remote.json').read_text())
    adapter = json.loads((out / 'adapter.json').read_text())
    assert remote['credentials_file'] == adapter['credentials_file'] == str(out / 'credentials.json')
    assert (out / 'remote.json').stat().st_mode & 0o777 == 0o600


def test_activation_stops_services_before_switch_and_stays_stopped_on_failure(tmp_path):
    from src.agent_access.activation import activate_generation, ActivationError
    root = tmp_path / 'private'; root.mkdir(mode=0o700)
    generations = root / 'generations'; generations.mkdir(mode=0o700)
    gen = generations / 'g1'; gen.mkdir(mode=0o700)
    current = root / 'current'
    calls = []
    def run(args):
        calls.append((args, current.exists()))
        if args[:2] == ['start','amr2-adapter.service']:
            raise ActivationError('start failed')
    with pytest.raises(ActivationError):
        activate_generation(root, gen, run=run, validate=lambda _: None)
    assert calls[0] == (['stop','amr2-adapter.service','amr2-mcp.service'], False)
    assert current.resolve() == gen
    assert calls[-1][0] == ['stop','amr2-adapter.service','amr2-mcp.service']


def test_activation_refuses_snapshot_outside_registry(tmp_path):
    from src.agent_access.activation import activate_generation, ActivationError
    root = tmp_path / 'private'; root.mkdir(mode=0o700)
    outside = tmp_path / 'outside'; outside.mkdir()
    calls = []
    with pytest.raises(ActivationError):
        activate_generation(root, outside, run=lambda x: calls.append(x), validate=lambda _: None)
    assert not calls


def test_activation_rejects_stale_grants_before_stopping(tmp_path):
    from src.agent_access.activation import activate_registry_generation, ActivationError
    reg = Registry(tmp_path / 'private')
    reg.add('a',['memory_search'],'/tmp/a.sock')
    genroot = reg.root / 'generations'; genroot.mkdir(mode=0o700)
    gen = reg.render(genroot / 'g1', {'remote_url':'https://localhost/mcp'}, {'business_socket':'/tmp/d'})
    reg.disable('a')
    calls = []
    with pytest.raises(ActivationError):
        activate_registry_generation(reg, gen, run=lambda cmd: calls.append(cmd), validate=lambda _: None)
    assert not calls


def test_activation_disconnects_old_snapshot_before_stop_for_crash_safety(tmp_path):
    from src.agent_access.activation import activate_generation, ActivationError
    root = tmp_path / 'private'; root.mkdir(mode=0o700)
    generations = root / 'generations'; generations.mkdir(mode=0o700)
    old = generations / 'old'; old.mkdir(mode=0o700)
    new = generations / 'new'; new.mkdir(mode=0o700)
    (root / 'current').symlink_to(old)
    calls = []
    def run(args):
        calls.append(args)
        if args[0] == 'stop':
            assert not (root / 'current').exists(), 'old snapshot could return after a crash'
            raise ActivationError('simulate failure')
    with pytest.raises(ActivationError): activate_generation(root, new, run=run, validate=lambda _: None)
    assert not (root / 'current').exists()


def test_status_identifies_pending_registry_changes_without_exposing_tokens(tmp_path):
    from src.agent_access.activation import registry_status
    reg = Registry(tmp_path / 'private'); reg.add('a',['memory_search'],'/tmp/a.sock')
    genroot = reg.root / 'generations'; genroot.mkdir(mode=0o700)
    gen = reg.render(genroot / 'g1', {'remote_url':'https://localhost/mcp'}, {})
    (reg.root / 'current').symlink_to(gen)
    assert registry_status(reg)['matches_active'] is True
    reg.set_tools('a',['memory_create'])
    status = registry_status(reg)
    assert status['matches_active'] is False
    assert reg.snapshot()['agents']['a']['token'] not in json.dumps(status)


def test_disabled_then_reenabled_agent_never_reuses_revoked_token(tmp_path):
    reg = Registry(tmp_path / 'private'); reg.add('a',['memory_search'])
    old = reg.snapshot()['agents']['a']['token']
    reg.disable('a'); reg.enable('a')
    assert reg.snapshot()['agents']['a']['token'] != old


def test_last_agent_can_be_revoked_without_reopening_after_restart(tmp_path):
    from src.agent_access.activation import activate_deny_all, ActivationError
    reg = Registry(tmp_path / 'private'); reg.add('a',['memory_search'])
    calls = []
    with pytest.raises(ActivationError): activate_deny_all(reg, run=calls.append)
    assert not calls
    old = reg.root / 'old'; old.mkdir(mode=0o700)
    (reg.root / 'current').symlink_to(old)
    reg.disable('a')
    def run(args):
        calls.append(args)
        assert not (reg.root / 'current').exists()
    activate_deny_all(reg, run=run)
    assert calls == [['stop','amr2-adapter.service','amr2-mcp.service']]
    assert (reg.root / 'current').is_symlink()
    assert not (reg.root / 'current').exists()


def test_alias_grants_are_rejected_in_favor_of_canonical_tools(tmp_path):
    reg = Registry(tmp_path / 'private')
    for name in ('memory_session_checkpoint', 'memory_get_handoff'):
        with pytest.raises(RegistryError): reg.add('a', [name])
    reg.add('a', ['session_checkpoint', 'session_get_handoff'])


def test_missing_initialized_inventory_fails_closed(tmp_path):
    reg = Registry(tmp_path / 'private'); reg.add('a',['memory_search'])
    (reg.root / 'registry.json').unlink()
    with pytest.raises(RegistryError): reg.list_agents()
    with pytest.raises(RegistryError): reg.add('b',['memory_search'])


def test_activation_stops_if_readiness_probe_fails(tmp_path):
    from src.agent_access.activation import activate_generation, ActivationError
    root = tmp_path/'private'; root.mkdir(mode=0o700)
    parent = root/'generations'; parent.mkdir(mode=0o700)
    gen = parent/'one'; gen.mkdir(mode=0o700)
    calls = []
    def fail(_): raise ActivationError('not ready')
    with pytest.raises(ActivationError):
        activate_generation(root, gen, run=calls.append, validate=lambda _: None, ready=fail)
    assert calls[-1] == ['stop','amr2-adapter.service','amr2-mcp.service']
