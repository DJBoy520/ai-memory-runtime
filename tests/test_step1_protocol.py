import asyncio
import os
import stat
import struct
import tempfile
from pathlib import Path
import pytest
import pytest_asyncio

from config.settings import AppConfig, ServerConfig, check_and_fix_file_permissions, load_config
from src.interfaces.ipc.protocol import (
    HEADER_STRUCT,
    ConnectionClosedError,
    FrameDecodeError,
    FrameTooLargeError,
    encode_frame,
    make_jsonrpc_error,
    make_jsonrpc_request,
    make_jsonrpc_response,
    read_frame,
    write_frame,
)
from src.interfaces.ipc.server import DualUDSServer, UDSServer


class TestProtocolCodec:
    """测试 UDS 帧编解码器核心能力"""

    def test_encode_and_header(self):
        msg = {"jsonrpc": "2.0", "method": "memory.search", "params": {"query": "test"}, "id": 1}
        frame = encode_frame(msg)
        assert len(frame) > 4
        length = struct.unpack(">I", frame[:4])[0]
        assert length == len(frame) - 4

    def test_encode_too_large(self):
        # 模拟超 4MB 编码拦截
        large_bytes = b"x" * (4 * 1024 * 1024 + 1)
        with pytest.raises(FrameTooLargeError):
            encode_frame(large_bytes, max_bytes=4 * 1024 * 1024)

    @pytest.mark.asyncio
    async def test_read_and_write_frame(self):
        # 使用 asyncio.StreamReader 模拟内存读取
        reader = asyncio.StreamReader()
        msg = {"jsonrpc": "2.0", "result": "ok", "id": 10}
        frame = encode_frame(msg)
        reader.feed_data(frame)
        reader.feed_eof()

        decoded = await read_frame(reader)
        assert decoded["id"] == 10
        assert decoded["result"] == "ok"

    @pytest.mark.asyncio
    async def test_read_frame_header_too_large(self):
        reader = asyncio.StreamReader()
        # 伪造一个声明 5MB 长度的头
        declared_len = 5 * 1024 * 1024
        header = struct.pack(">I", declared_len)
        reader.feed_data(header + b"padding")
        reader.feed_eof()

        with pytest.raises(FrameTooLargeError):
            await read_frame(reader, max_bytes=4 * 1024 * 1024)

    @pytest.mark.asyncio
    async def test_read_empty_payload(self):
        reader = asyncio.StreamReader()
        # 声明 0 字节 payload
        header = struct.pack(">I", 0)
        reader.feed_data(header)
        reader.feed_eof()

        with pytest.raises(FrameDecodeError):
            await read_frame(reader)

    @pytest.mark.asyncio
    async def test_incomplete_stream(self):
        reader = asyncio.StreamReader()
        # 仅有 2 个字节的头
        reader.feed_data(b"\x00\x00")
        reader.feed_eof()

        with pytest.raises(ConnectionClosedError):
            await read_frame(reader)


class TestUDSServerAndDualSockets:
    """测试双 Socket 监听、文件权限、并发通信与大包防御"""

    @pytest_asyncio.fixture
    async def temp_sockets(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            biz_sock = str(Path(tmpdir) / "test-biz.sock")
            adm_sock = str(Path(tmpdir) / "test-admin.sock")
            yield biz_sock, adm_sock

    @pytest.mark.asyncio
    async def test_dual_server_startup_and_permissions(self, temp_sockets):
        biz_sock, adm_sock = temp_sockets
        cfg = AppConfig(
            server=ServerConfig(
                business_socket=biz_sock,
                admin_socket=adm_sock,
                socket_mode=0o600,
            )
        )

        async def biz_handler(method: str, params: any):
            return {"service": "business", "method": method, "params": params}

        async def adm_handler(method: str, params: any):
            return {"service": "admin", "method": method, "params": params}

        dual_server = DualUDSServer(
            config=cfg,
            business_handler=biz_handler,
            admin_handler=adm_handler,
        )

        await dual_server.start()
        try:
            # 验证 socket 文件存在
            biz_path = Path(biz_sock)
            adm_path = Path(adm_sock)
            assert biz_path.exists() and biz_path.is_socket()
            assert adm_path.exists() and adm_path.is_socket()

            # 验证权限为严格 0600
            biz_mode = stat.S_IMODE(os.stat(biz_sock).st_mode)
            adm_mode = stat.S_IMODE(os.stat(adm_sock).st_mode)
            assert biz_mode == 0o600, f"Expected 0600, got {oct(biz_mode)}"
            assert adm_mode == 0o600, f"Expected 0600, got {oct(adm_mode)}"

            # 客户端连接测试：业务 Socket
            reader_biz, writer_biz = await asyncio.open_unix_connection(path=biz_sock)
            req = make_jsonrpc_request("memory.search", {"query": "hello"}, req_id=101)
            await write_frame(writer_biz, req)
            resp = await read_frame(reader_biz)
            assert resp["id"] == 101
            assert resp["result"]["service"] == "business"
            writer_biz.close()
            await writer_biz.wait_closed()

            # 客户端连接测试：管理 Socket
            reader_adm, writer_adm = await asyncio.open_unix_connection(path=adm_sock)
            req_adm = make_jsonrpc_request("admin.get_status", {}, req_id=202)
            await write_frame(writer_adm, req_adm)
            resp_adm = await read_frame(reader_adm)
            assert resp_adm["id"] == 202
            assert resp_adm["result"]["service"] == "admin"
            writer_adm.close()
            await writer_adm.wait_closed()

        finally:
            await dual_server.stop()
            assert not Path(biz_sock).exists()
            assert not Path(adm_sock).exists()

    @pytest.mark.asyncio
    async def test_server_large_frame_cutoff(self, temp_sockets):
        """测试发送超 4MB 帧时服务端立即拦截切断连接"""
        biz_sock, adm_sock = temp_sockets
        cfg = AppConfig(
            server=ServerConfig(
                business_socket=biz_sock,
                admin_socket=adm_sock,
                socket_mode=0o600,
                max_request_bytes=1024 * 1024,  # 测试限流阈值 1MB
            )
        )

        dual_server = DualUDSServer(config=cfg)
        await dual_server.start()

        try:
            reader, writer = await asyncio.open_unix_connection(path=biz_sock)
            # 发送一个声明长度 2MB 的大包头
            too_large_len = 2 * 1024 * 1024
            header = HEADER_STRUCT.pack(too_large_len)
            writer.write(header + b"x" * 100)
            await writer.drain()

            # 期待服务端回送错误或切断连接
            try:
                resp = await read_frame(reader, max_bytes=4 * 1024 * 1024)
                assert "error" in resp
                assert resp["error"]["code"] == -32600
            except ConnectionClosedError:
                # 直接被断开连接也是符合预期的防御行为
                pass

            writer.close()
            await writer.wait_closed()
        finally:
            await dual_server.stop()


class TestConfigFileSafety:
    """测试配置文件的权限与加载安全性"""

    def test_config_permissions(self):
        config_path = Path("/home/dj/WorkSpaces/qdrant-bge-memory/config/config.yaml")
        if config_path.exists():
            check_and_fix_file_permissions(config_path)
            mode = stat.S_IMODE(os.stat(config_path).st_mode)
            assert mode == 0o600
