"""Fail-closed activation for preconfigured generation-based systemd units."""
from __future__ import annotations
import json
import os
from pathlib import Path
import secrets
import ssl
import stat
import subprocess
import socket
import struct
import time
import urllib.parse
import urllib.request

from src.interfaces.adapter.config import load_adapter_config
from src.interfaces.mcp.remote_service import load_config


class ActivationError(RuntimeError):
    pass


def _systemctl(args):
    try:
        subprocess.run(['systemctl', '--user', *args], check=True, capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.SubprocessError):
        raise ActivationError('systemd operation failed: ' + args[0]) from None


def check_unit_paths(root):
    """Refuse activation unless installed units read the generation pointer."""
    root = Path(root).absolute()
    for unit, name in (('amr2-mcp.service', 'remote.json'), ('amr2-adapter.service', 'adapter.json')):
        try:
            result = subprocess.run(['systemctl', '--user', 'show', unit, '-p', 'ExecStart', '-p', 'ExecStartPre', '--value'], check=True, capture_output=True, text=True, timeout=10)
        except (OSError, subprocess.SubprocessError):
            raise ActivationError('cannot inspect service unit') from None
        if str(root / 'current' / name) not in result.stdout:
            raise ActivationError('service units do not use registry current paths')


def validate_generation(path):
    """Validate runtime config and ensure both services reference this snapshot."""
    path = Path(path)
    try:
        for name in ('credentials.json', 'adapter.json', 'remote.json'):
            p = path / name; info = p.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600:
                raise ValueError('unsafe file')
        adapter = load_adapter_config(path / 'adapter.json')
        remote = load_config(path / 'remote.json')
        expected = str(path / 'credentials.json')
        if adapter.credentials_file != expected or remote.credentials_file != expected:
            raise ValueError('credentials file outside snapshot')
        key_info = os.stat(remote.key_file)
        if key_info.st_uid != os.getuid() or stat.S_IMODE(key_info.st_mode) not in (0o400, 0o600):
            raise ValueError('unsafe TLS private key permissions')
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(remote.cert_file, remote.key_file)
        raw = json.loads((path / 'credentials.json').read_text(encoding='utf-8'))
        from src.interfaces.adapter.credential import read_token
        from src.agent_access.registry import _id
        for token, identity in raw.items():
            token_path = path / 'tokens' / (_id(identity['agent_id']) + '.token')
            if read_token(token_path) != token:
                raise ValueError('credential file mismatch')
    except (OSError, ValueError, TypeError, KeyError, ssl.SSLError) as exc:
        raise ActivationError('snapshot validation failed') from exc


def _switch_pointer(root, target):
    temp = root / ('.current-' + secrets.token_hex(8))
    try:
        os.symlink(str(target), temp)
        os.replace(temp, root / 'current')
        dfd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        try: os.fsync(dfd)
        finally: os.close(dfd)
    finally:
        if os.path.lexists(temp): os.unlink(temp)


def wait_ready(generation, timeout=180):
    """Check TLS health and authenticate to each configured adapter socket."""
    from src.interfaces.adapter.credential import read_token
    path = Path(generation)
    config = load_adapter_config(path / 'adapter.json')
    parsed = urllib.parse.urlsplit(config.remote_url)
    health_url = urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, '/readyz', '', ''))
    credentials = json.loads((path / 'credentials.json').read_text(encoding='utf-8'))
    if not credentials:
        raise ActivationError('no credential available for readiness probe')
    context = ssl.create_default_context(cafile=config.ca_file)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=context))
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            for probe_token in credentials:
                request = urllib.request.Request(health_url, headers={'Authorization': 'Bearer ' + probe_token})
                with opener.open(request, timeout=2) as response:
                    if response.status != 200: raise ValueError('health failed')
            for binding in config.bindings:
                token = read_token(path / 'tokens' / (binding.agent_id + '.token'))
                request = json.dumps({'jsonrpc':'2.0','id':1,'method':'amr.ping','params':{},'auth_token':token}).encode()
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                    sock.settimeout(2); sock.connect(binding.socket_path)
                    sock.sendall(struct.pack('>I', len(request)) + request)
                    def exact(size):
                        buf = b''
                        while len(buf) < size:
                            chunk = sock.recv(size - len(buf))
                            if not chunk: raise OSError('socket closed')
                            buf += chunk
                        return buf
                    size = struct.unpack('>I', exact(4))[0]
                    if size == 0 or size > 65536: raise ValueError('invalid response')
                    result = json.loads(exact(size))
                    if result.get('id') != 1 or result.get('result') != {'status':'ok'}:
                        raise ValueError('authenticated ping failed')
            return
        except (OSError, ValueError, KeyError, TimeoutError):
            time.sleep(0.2)
    raise ActivationError('services did not become ready before deadline')


def activate_generation(root, generation, *, run=_systemctl, validate=validate_generation, ready=wait_ready):
    """Stop both services, atomically switch pointer, start and check both.

    If anything fails after stopping, both services are stopped again. This
    intentionally does not fall back to old credentials after a revocation.
    Callers must ensure service units use root/current/{remote,adapter}.json.
    """
    root = Path(root).absolute()
    generation = Path(generation).absolute()
    parent = root / 'generations'
    stopping = False
    try:
        if generation.parent != parent or generation.is_symlink() or not generation.is_dir():
            raise ActivationError('generation must be a direct child of registry generations')
        for p in (root, parent, generation):
            s = p.lstat()
            if not stat.S_ISDIR(s.st_mode) or s.st_uid != os.getuid() or stat.S_IMODE(s.st_mode) != 0o700:
                raise ActivationError('generation directories must be private and owned')
        current = root / 'current'
        if os.path.lexists(current) and not current.is_symlink():
            raise ActivationError('current pointer is not a symlink')
        validate(generation)
        # If the process or machine dies before the stop completes, a restart
        # must not restore the previous, possibly revoked generation.
        stopping = True
        _switch_pointer(root, root / ('.blocked-' + secrets.token_hex(16)))
        run(['stop', 'amr2-adapter.service', 'amr2-mcp.service'])
        _switch_pointer(root, generation)
        run(['start', 'amr2-mcp.service'])
        run(['start', 'amr2-adapter.service'])
        run(['is-active', '--quiet', 'amr2-mcp.service'])
        run(['is-active', '--quiet', 'amr2-adapter.service'])
        ready(generation)
    except (ActivationError, OSError, ValueError):
        # A failed stop itself may be partial. Never restart with prior grants.
        if stopping:
            try:
                run(['stop', 'amr2-adapter.service', 'amr2-mcp.service'])
            except Exception:
                raise ActivationError('activation failed; could not stop both services; access may remain active') from None
        raise


def activate_registry_generation(registry, generation, *, run=_systemctl, validate=validate_generation, ready=wait_ready):
    """Refuse stale grants and serialize registry changes throughout restart."""
    generation = Path(generation).absolute()
    with registry._locked():
        state = registry._read()
        enabled = {n: a for n, a in state['agents'].items() if a['enabled']}
        expected_credentials = {a['token']: {'agent_id': n, 'tools': a['tools']} for n, a in enabled.items()}
        expected_bindings = sorted((n, a['socket']) for n, a in enabled.items() if a['socket'])
        try:
            credentials = json.loads((generation / 'credentials.json').read_text(encoding='utf-8'))
            adapter = json.loads((generation / 'adapter.json').read_text(encoding='utf-8'))
            actual_bindings = sorted((b['agent_id'], b['socket_path']) for b in adapter['bindings'])
        except (OSError, ValueError, TypeError, KeyError):
            raise ActivationError('snapshot is incomplete') from None
        if credentials != expected_credentials or actual_bindings != expected_bindings:
            raise ActivationError('snapshot differs from current registry')
        activate_generation(registry.root, generation, run=run, validate=validate, ready=ready)


def registry_status(registry):
    """Compare desired state to pointer contents; does not inspect live processes."""
    with registry._locked():
        data = registry._read()
        enabled = {n: a for n, a in data['agents'].items() if a['enabled']}
        expected = {a['token']: {'agent_id': n, 'tools': a['tools']} for n, a in enabled.items()}
        bindings = sorted((n, a['socket']) for n, a in enabled.items() if a['socket'])
        try:
            current = registry.root / 'current'
            if not current.is_symlink(): raise ValueError('no current generation')
            target = current.resolve(strict=True)
            if target.parent != (registry.root / 'generations').resolve(): raise ValueError('invalid current generation')
            actual = json.loads((target / 'credentials.json').read_text(encoding='utf-8'))
            adapter = json.loads((target / 'adapter.json').read_text(encoding='utf-8'))
            actual_bindings = sorted((b['agent_id'], b['socket_path']) for b in adapter['bindings'])
            return {'matches_active': actual == expected and actual_bindings == bindings,
                    'generation': target.name,
                    'note': 'compares files only; running service state is not checked'}
        except (OSError, ValueError, KeyError, TypeError):
            return {'matches_active': False, 'generation': None,
                    'note': 'active snapshot missing or invalid; running service state is not checked'}


def activate_deny_all(registry, *, run=_systemctl):
    """Invalidate the pointer persistently and stop all network entry points."""
    with registry._locked():
        if any(a['enabled'] for a in registry._read()['agents'].values()):
            raise ActivationError('deny-all requires every agent to be disabled')
        current = registry.root / 'current'
        if os.path.lexists(current) and not current.is_symlink():
            raise ActivationError('current pointer is not a symlink')
        try:
            _switch_pointer(registry.root, registry.root / ('.blocked-' + secrets.token_hex(16)))
            run(['stop', 'amr2-adapter.service', 'amr2-mcp.service'])
        except Exception as exc:
            try:
                run(['stop', 'amr2-adapter.service', 'amr2-mcp.service'])
            except Exception:
                raise ActivationError('could not stop both services; access may remain active') from None
            raise ActivationError('deny-all failed; services stopped, pointer may be unchanged') from exc
