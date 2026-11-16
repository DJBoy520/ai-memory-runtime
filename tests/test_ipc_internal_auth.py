import asyncio
from src.interfaces.ipc.server import UDSServer
from src.interfaces.ipc.protocol import read_frame, write_frame


def test_configured_internal_socket_rejects_missing_and_invalid_tokens(tmp_path):
    async def run():
        calls = []
        async def handler(method, params):
            calls.append((method,params)); return {'ok':True}
        path = str(tmp_path / 'internal.sock')
        server = UDSServer('business', path, handler, internal_token='x'*43)
        await server.start()
        try:
            for token in (None, 'y'*43, 123, 'é'):
                reader, writer = await asyncio.open_unix_connection(path)
                req = {'jsonrpc':'2.0','id':1,'method':'memory.search','params':{}}
                if token is not None: req['auth_token'] = token
                await write_frame(writer,req)
                response = await read_frame(reader)
                assert response['error']['code'] == -32012
                writer.close(); await writer.wait_closed()
            assert not calls
            reader, writer = await asyncio.open_unix_connection(path)
            await write_frame(writer, {'jsonrpc':'2.0','id':2,'method':'memory.search','params':{},'auth_token':'x'*43})
            assert (await read_frame(reader))['result'] == {'ok':True}
            assert calls == [('memory.search', {})]
            writer.close(); await writer.wait_closed()
        finally:
            await server.stop()
    asyncio.run(run())


def test_remote_rpc_can_authenticate_internal_daemon(tmp_path, monkeypatch):
    from src.interfaces.mcp.remote_rpc import RemoteRpc
    credential = tmp_path / 'internal.token'
    credential.write_text('x'*43, encoding='ascii'); credential.chmod(0o600)
    monkeypatch.setenv('AMR_INTERNAL_TOKEN_FILE', str(credential))
    async def run():
        async def handler(method, params): return {'ok':True}
        path = str(tmp_path / 'internal.sock')
        server = UDSServer('business', path, handler, internal_token='x'*43)
        await server.start()
        try:
            assert await RemoteRpc(path)('memory.search', {'query':'x'}) == {'ok':True}
        finally:
            await server.stop()
    asyncio.run(run())


def test_required_internal_auth_refuses_missing_token_file(monkeypatch):
    import pytest
    from src.interfaces.ipc.server import DualUDSServer
    monkeypatch.setenv('AMR_INTERNAL_AUTH_REQUIRED', '1')
    monkeypatch.delenv('AMR_INTERNAL_TOKEN_FILE', raising=False)
    with pytest.raises(ValueError, match='internal'):
        DualUDSServer()


def test_doctor_authenticates_internal_socket(tmp_path, monkeypatch):
    from scripts.amr_doctor import send_ipc_rpc
    credential = tmp_path / 'internal.token'
    credential.write_text('x'*43, encoding='ascii'); credential.chmod(0o600)
    monkeypatch.setenv('AMR_ADMIN_TOKEN_FILE', str(credential))
    async def run():
        async def handler(method, params): return {'status':'ok'}
        path = str(tmp_path / 'admin.sock')
        server = UDSServer('admin', path, handler, internal_token='x'*43)
        await server.start()
        try:
            assert await send_ipc_rpc(path, 'admin.status') == {'status':'ok'}
        finally:
            await server.stop()
    asyncio.run(run())


def test_business_and_admin_require_distinct_credentials(tmp_path, monkeypatch):
    import pytest
    from types import SimpleNamespace
    from src.interfaces.ipc.server import DualUDSServer
    business = tmp_path / 'business.token'; business.write_text('b'*43); business.chmod(0o600)
    admin = tmp_path / 'admin.token'; admin.write_text('a'*43); admin.chmod(0o600)
    monkeypatch.setenv('AMR_INTERNAL_AUTH_REQUIRED', '1')
    monkeypatch.setenv('AMR_INTERNAL_TOKEN_FILE', str(business))
    monkeypatch.setenv('AMR_ADMIN_TOKEN_FILE', str(admin))
    monkeypatch.setenv('AMR_AGENT_CREDENTIALS_FILE', str(tmp_path / 'agents.json'))
    cfg = SimpleNamespace(server=SimpleNamespace(business_socket=str(tmp_path/'b.sock'), admin_socket=str(tmp_path/'a.sock'), socket_mode=0o600, max_request_bytes=4096))
    async def run():
        server = DualUDSServer(config=cfg); await server.start()
        try:
            reader, writer = await asyncio.open_unix_connection(str(tmp_path/'a.sock'))
            await write_frame(writer, {'jsonrpc':'2.0','id':1,'method':'admin.status','auth_token':'b'*43})
            assert (await read_frame(reader))['error']['code'] == -32012
            writer.close(); await writer.wait_closed()
            reader, writer = await asyncio.open_unix_connection(str(tmp_path/'a.sock'))
            await write_frame(writer, {'jsonrpc':'2.0','id':2,'method':'admin.get_status','auth_token':'a'*43})
            assert 'result' in await read_frame(reader)
            writer.close(); await writer.wait_closed()
        finally:
            await server.stop()
    asyncio.run(run())
