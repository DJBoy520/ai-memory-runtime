"""Private local agent inventory and offline deployment snapshot generator.

No changes are applied to a running service by this module. An export is a
new directory, never an in-place update of active configuration.
"""
from __future__ import annotations

import contextlib
import fcntl
import json
import os
from pathlib import Path
import re
import secrets
import stat
import tempfile

from src.interfaces.adapter.mapping import TOOL_TO_METHOD


class RegistryError(ValueError):
    pass


_ID = re.compile(r"[a-z][a-z0-9_-]{0,63}\Z")


def _tools(value):
    if not isinstance(value, list) or not value or any(not isinstance(t, str) or t not in TOOL_TO_METHOD for t in value):
        raise RegistryError('tools must be a non-empty list of known remote tools')
    if len(set(value)) != len(value):
        raise RegistryError('duplicate tool')
    return list(value)


def _id(value):
    if not isinstance(value, str) or not _ID.fullmatch(value):
        raise RegistryError('invalid agent id')
    return value


def _private_file(path, data):
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    with os.fdopen(fd, 'w', encoding='utf-8') as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())


class Registry:
    def __init__(self, directory):
        self.root = Path(directory).absolute()
        if not self.root.exists():
            self.root.mkdir(mode=0o700, parents=True)
        st = self.root.lstat()
        if not stat.S_ISDIR(st.st_mode) or st.st_uid != os.getuid() or stat.S_IMODE(st.st_mode) != 0o700:
            raise RegistryError('registry directory must be an owned private directory (0700)')
        self.path = self.root / 'registry.json'
        self.marker = self.root / '.initialized'
        self.lock = self.root / '.lock'

    @contextlib.contextmanager
    def _locked(self):
        fd = os.open(self.lock, os.O_CREAT | os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0), 0o600)
        try:
            s = os.fstat(fd)
            if not stat.S_ISREG(s.st_mode) or s.st_uid != os.getuid() or stat.S_IMODE(s.st_mode) != 0o600:
                raise RegistryError('unsafe lock file')
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            os.close(fd)

    def _read(self):
        try:
            fd = os.open(self.path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
        except FileNotFoundError:
            if os.path.lexists(self.marker):
                raise RegistryError('initialized registry is missing') from None
            return {'version': 1, 'agents': {}}
        try:
            with os.fdopen(fd, 'r', encoding='utf-8') as f:
                s = os.fstat(f.fileno())
                if not stat.S_ISREG(s.st_mode) or s.st_uid != os.getuid() or stat.S_IMODE(s.st_mode) != 0o600 or s.st_size > 1024 * 1024:
                    raise RegistryError('unsafe registry file')
                data = json.load(f)
            self._validate(data)
            return data
        except (ValueError, OSError, UnicodeError, TypeError, KeyError) as e:
            raise RegistryError('invalid registry') from e

    def _validate(self, data):
        if not isinstance(data, dict) or data.get('version') != 1 or not isinstance(data.get('agents'), dict):
            raise RegistryError('invalid registry structure')
        tokens, sockets = set(), set()
        for name, entry in data['agents'].items():
            _id(name)
            if not isinstance(entry, dict) or type(entry.get('enabled')) is not bool:
                raise RegistryError('invalid agent')
            _tools(entry.get('tools'))
            token, socket = entry.get('token'), entry.get('socket')
            if not isinstance(token, str) or not re.fullmatch(r'[A-Za-z0-9_-]{43,}', token) or token in tokens:
                raise RegistryError('invalid or duplicate credential')
            tokens.add(token)
            if socket is not None:
                if not isinstance(socket, str) or not os.path.isabs(socket) or '\x00' in socket:
                    raise RegistryError('invalid socket')
                canonical = os.path.realpath(socket)
                if canonical in sockets:
                    raise RegistryError('duplicate socket')
                sockets.add(canonical)

    def _write(self, data):
        self._validate(data)
        if not os.path.lexists(self.marker):
            _private_file(self.marker, '1\n')
            marker_dir = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
            try: os.fsync(marker_dir)
            finally: os.close(marker_dir)
        fd, tmp = tempfile.mkstemp(prefix='.registry-', dir=self.root)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, sort_keys=True)
                f.write('\n'); f.flush(); os.fsync(f.fileno())
            os.replace(tmp, self.path)
            dfd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
            try: os.fsync(dfd)
            finally: os.close(dfd)
        finally:
            if os.path.exists(tmp): os.unlink(tmp)

    def snapshot(self):
        with self._locked(): return self._read()

    def list_agents(self):
        data = self.snapshot()
        return {name: {k: v for k, v in entry.items() if k != 'token'} for name, entry in data['agents'].items()}

    def import_existing(self, credentials_path, adapter_path=None):
        """Import an existing deployment into an empty registry; never print tokens."""
        try:
            fd = os.open(credentials_path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
            with os.fdopen(fd, 'r', encoding='utf-8') as f:
                st = os.fstat(f.fileno())
                if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or stat.S_IMODE(st.st_mode) not in (0o400, 0o600) or st.st_size > 1024 * 1024:
                    raise RegistryError('unsafe credentials file')
                credentials = json.load(f)
            bindings = []
            if adapter_path is not None:
                with open(adapter_path, encoding='utf-8') as f:
                    bindings = json.load(f)['bindings']
            if not isinstance(credentials, dict) or not credentials or not isinstance(bindings, list):
                raise RegistryError('invalid import')
            data = {'version': 1, 'agents': {}}
            for token, entry in credentials.items():
                name = entry['agent_id']
                if name in data['agents']:
                    raise RegistryError('multiple credentials for one agent')
                data['agents'][name] = {'enabled': True, 'tools': entry['tools'], 'socket': None, 'token': token}
            seen = set()
            for binding in bindings:
                name = binding['agent_id']
                if name in seen or name not in data['agents']:
                    raise RegistryError('invalid adapter binding')
                seen.add(name)
                data['agents'][name]['socket'] = binding['socket_path']
            self._validate(data)
        except (OSError, ValueError, TypeError, KeyError, UnicodeError) as exc:
            raise RegistryError('invalid or unsafe import') from exc
        with self._locked():
            if self._read()['agents']:
                raise RegistryError('registry must be empty to import')
            self._write(data)

    def add(self, name, tools, socket=None):
        _id(name); _tools(tools)
        with self._locked():
            data = self._read()
            if name in data['agents']: raise RegistryError('agent already exists')
            data['agents'][name] = {'enabled': True, 'tools': list(tools), 'socket': socket, 'token': secrets.token_urlsafe(32)}
            self._write(data)

    def _change(self, name, operation):
        _id(name)
        with self._locked():
            data = self._read()
            if name not in data['agents']: raise RegistryError('agent not found')
            operation(data['agents'][name])
            self._write(data)

    def rotate(self, name): self._change(name, lambda a: a.update(token=secrets.token_urlsafe(32)))
    def set_socket(self, name, socket):
        _id(name)
        if socket is not None:
            if not isinstance(socket, str) or not os.path.isabs(socket) or '\x00' in socket:
                raise RegistryError('invalid socket')
        self._change(name, lambda a: a.update(socket=socket))
    def disable(self, name): self._change(name, lambda a: a.update(enabled=False, token=secrets.token_urlsafe(32)))
    def enable(self, name): self._change(name, lambda a: a.update(enabled=True))
    def set_tools(self, name, tools):
        _tools(tools)
        self._change(name, lambda a: a.update(tools=list(tools)))

    def render(self, output, adapter_base, remote_base=None):
        data = self.snapshot()
        active = {k: v for k, v in data['agents'].items() if v['enabled']}
        if not active: raise RegistryError('at least one agent must be enabled')
        if not isinstance(adapter_base, dict) or not isinstance(adapter_base.get('remote_url'), str) or not adapter_base['remote_url'].startswith('https://'):
            raise RegistryError('adapter base requires an https remote_url')
        if remote_base is not None and not isinstance(remote_base, dict):
            raise RegistryError('remote base must be an object')
        out = Path(output).absolute()
        try: out.mkdir(mode=0o700)
        except FileExistsError: raise RegistryError('output directory already exists') from None
        (out / 'tokens').mkdir(mode=0o700)
        credentials = {a['token']: {'agent_id': n, 'tools': a['tools']} for n, a in active.items()}
        _private_file(out / 'credentials.json', json.dumps(credentials, indent=2) + '\n')
        for n, a in active.items(): _private_file(out / 'tokens' / (n + '.token'), a['token'] + '\n')
        adapter = dict(adapter_base)
        adapter['credentials_file'] = str(out / 'credentials.json')
        adapter['bindings'] = [{'agent_id': n, 'socket_path': a['socket']} for n, a in active.items() if a['socket']]
        _private_file(out / 'adapter.json', json.dumps(adapter, indent=2) + '\n')
        if remote_base is not None:
            remote = dict(remote_base)
            remote['credentials_file'] = str(out / 'credentials.json')
            _private_file(out / 'remote.json', json.dumps(remote, indent=2) + '\n')
        for directory in (out / 'tokens', out, out.parent):
            fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
            try: os.fsync(fd)
            finally: os.close(fd)
        return out
