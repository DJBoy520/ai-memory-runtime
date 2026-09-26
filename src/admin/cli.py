import sys
from pathlib import Path
root_dir = str(Path(__file__).resolve().parent.parent.parent)
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)
"""
Admin CLI Tool for AI Memory Runtime.
Connects directly to the dedicated Admin UDS (/run/user/1000/qdrant-bge-admin.sock).
Zero HTTP ports. Zero TCP traffic. Full administrative control.

Commands:
- status: Query 6-state state machine and GPU memory statistics
- load: Explicitly load BGE-M3 model into GPU memory
- unload: Explicitly unload BGE-M3 model to release GPU memory to MinerU
- collections: List standard Qdrant collections and point counts
- snapshot <collection_name>: Create local snapshot of a Qdrant collection
"""

import argparse
import asyncio
import json
import os
import sys
from typing import Any, Dict

from src.interfaces.ipc.protocol import (
    make_jsonrpc_request,
    read_frame,
    write_frame,
)

DEFAULT_ADMIN_SOCKET = f"/run/user/{os.getuid()}/qdrant-bge-admin.sock"


class AdminClient:
    def __init__(self, socket_path: str = DEFAULT_ADMIN_SOCKET):
        self.socket_path = socket_path

    async def _send_command(self, method: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        params = params or {}
        if not os.path.exists(self.socket_path):
            raise ConnectionError(
                f"Admin socket not found at '{self.socket_path}'. Daemon might not be running."
            )

        reader, writer = await asyncio.open_unix_connection(self.socket_path)
        try:
            req = make_jsonrpc_request(method=method, params=params, req_id=1)
            await write_frame(writer, req)
            resp = await read_frame(reader)
            if "error" in resp and resp["error"]:
                err = resp["error"]
                raise RuntimeError(f"Admin Error ({err.get('code')}): {err.get('message')}")
            return resp.get("result", {})
        finally:
            writer.close()
            await writer.wait_closed()

    async def status(self) -> Dict[str, Any]:
        return await self._send_command("admin.status")

    async def load(self) -> Dict[str, Any]:
        return await self._send_command("admin.load")

    async def unload(self) -> Dict[str, Any]:
        return await self._send_command("admin.unload")

    async def collections(self) -> Dict[str, Any]:
        return await self._send_command("admin.collections")

    async def snapshot(self, collection_name: str) -> Dict[str, Any]:
        return await self._send_command("admin.snapshot", {"collection": collection_name})


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="admin-cli",
        description="Admin CLI for AI Memory Runtime management over dedicated Admin UDS."
    )
    parser.add_argument(
        "--socket",
        "-s",
        default=DEFAULT_ADMIN_SOCKET,
        help="Path to dedicated Admin Unix Domain Socket."
    )

    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("status", help="Get 6-state status and GPU memory metrics.")
    subparsers.add_parser("load", help="Explicitly load BGE-M3 model.")
    subparsers.add_parser("unload", help="Explicitly unload BGE-M3 model.")
    subparsers.add_parser("collections", help="List Qdrant collections.")

    snap_parser = subparsers.add_parser("snapshot", help="Create a collection snapshot.")
    snap_parser.add_argument("collection", help="Collection name (e.g. ai_memory).")

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    client = AdminClient(socket_path=args.socket)

    async def run():
        if args.command == "status":
            res = await client.status()
        elif args.command == "load":
            res = await client.load()
        elif args.command == "unload":
            res = await client.unload()
        elif args.command == "collections":
            res = await client.collections()
        elif args.command == "snapshot":
            res = await client.snapshot(args.collection)
        else:
            raise ValueError(f"Unknown command: {args.command}")

        print(json.dumps(res, ensure_ascii=False, indent=2))

    try:
        asyncio.run(run())
    except Exception as e:
        sys.stderr.write(f"Error: {e}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
