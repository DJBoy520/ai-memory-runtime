import asyncio
import json
import struct
from typing import Any, Dict, Optional, Union

# 默认单包最大限制 4MB
MAX_REQUEST_BYTES = 4 * 1024 * 1024  # 4,194,304 bytes
HEADER_STRUCT = struct.Struct(">I")  # uint32 Big-Endian (4 bytes)


class ProtocolError(Exception):
    """协议异常基类"""
    pass


class FrameTooLargeError(ProtocolError):
    """数据帧大小超出 MAX_REQUEST_BYTES 限制"""
    pass


class FrameDecodeError(ProtocolError):
    """JSON 解码异常或帧格式错误"""
    pass


class ConnectionClosedError(ProtocolError):
    """连接已过早关闭"""
    pass


def encode_frame(payload: Union[Dict[str, Any], list, str, bytes], max_bytes: int = MAX_REQUEST_BYTES) -> bytes:
    """
    将 JSON-RPC 消息对象编码为带 4 字节 uint32_be 长度前缀的字节流。
    """
    if isinstance(payload, bytes):
        payload_bytes = payload
    elif isinstance(payload, str):
        payload_bytes = payload.encode("utf-8")
    else:
        payload_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    length = len(payload_bytes)
    if length > max_bytes:
        raise FrameTooLargeError(
            f"Payload length {length} exceeds maximum allowed {max_bytes} bytes"
        )

    return HEADER_STRUCT.pack(length) + payload_bytes


async def read_frame(
    reader: asyncio.StreamReader,
    max_bytes: int = MAX_REQUEST_BYTES,
) -> Dict[str, Any]:
    """
    从 asyncio.StreamReader 中读取一个 Length-Prefixed 帧并反序列化为 JSON-RPC 字典。
    如果包头超出 max_bytes 限制，抛出 FrameTooLargeError。
    如果遭遇 EOF 或空帧，按情况抛出 ConnectionClosedError 或 FrameDecodeError。
    """
    try:
        header = await reader.readexactly(HEADER_STRUCT.size)
    except asyncio.IncompleteReadError as e:
        if len(e.partial) == 0:
            raise ConnectionClosedError("Connection closed cleanly by peer") from e
        raise ConnectionClosedError(
            f"Incomplete frame header: received {len(e.partial)} bytes, expected 4"
        ) from e

    (length,) = HEADER_STRUCT.unpack(header)

    if length > max_bytes:
        raise FrameTooLargeError(
            f"Frame declared length {length} exceeds max allowed {max_bytes} bytes"
        )
    if length == 0:
        raise FrameDecodeError("Empty payload received (length=0)")

    try:
        payload_bytes = await reader.readexactly(length)
    except asyncio.IncompleteReadError as e:
        raise ConnectionClosedError(
            f"Incomplete payload: received {len(e.partial)} bytes, expected {length}"
        ) from e

    try:
        data = json.loads(payload_bytes.decode("utf-8"))
    except Exception as e:
        raise FrameDecodeError(f"Failed to decode JSON payload: {e}") from e

    return data


async def write_frame(
    writer: asyncio.StreamWriter,
    payload: Union[Dict[str, Any], list, str, bytes],
    max_bytes: int = MAX_REQUEST_BYTES,
) -> None:
    """
    将 JSON-RPC 对象打包并写入 StreamWriter，自动 flush (drain)。
    """
    frame_data = encode_frame(payload, max_bytes=max_bytes)
    writer.write(frame_data)
    await writer.drain()


def make_jsonrpc_request(
    method: str,
    params: Optional[Union[Dict[str, Any], list]] = None,
    req_id: Optional[Union[int, str]] = 1,
) -> Dict[str, Any]:
    """构造标准 JSON-RPC 2.0 请求对象"""
    req: Dict[str, Any] = {
        "jsonrpc": "2.0",
        "method": method,
        "id": req_id,
    }
    if params is not None:
        req["params"] = params
    return req


def make_jsonrpc_response(
    result: Any,
    req_id: Optional[Union[int, str]] = 1,
) -> Dict[str, Any]:
    """构造标准 JSON-RPC 2.0 成功响应"""
    return {
        "jsonrpc": "2.0",
        "result": result,
        "id": req_id,
    }


def make_jsonrpc_error(
    code: int,
    message: str,
    data: Optional[Any] = None,
    req_id: Optional[Union[int, str]] = None,
) -> Dict[str, Any]:
    """构造标准 JSON-RPC 2.0 错误响应"""
    err_obj: Dict[str, Any] = {
        "code": code,
        "message": message,
    }
    if data is not None:
        err_obj["data"] = data

    return {
        "jsonrpc": "2.0",
        "error": err_obj,
        "id": req_id,
    }
