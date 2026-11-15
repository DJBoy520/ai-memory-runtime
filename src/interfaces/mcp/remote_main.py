"""AMR 2.x remote MCP HTTPS service CLI.

Usage: python -m src.interfaces.mcp.remote_main --config /etc/amr/remote-service.json
"""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys
from typing import Optional, Sequence

from src.interfaces.mcp.remote_server import RemoteMcpService
from src.interfaces.mcp.remote_service import load_config


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AMR 2.x authenticated HTTPS MCP service")
    parser.add_argument("--config", required=True, help="remote service JSON config path")
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate configuration and TLS material, then exit",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    try:
        config = load_config(args.config)
    except (ValueError, OSError) as exc:
        sys.stderr.write(f"amr2-mcp configuration error: {exc}\n")
        return 2

    if args.check:
        # 构造服务会加载并校验证书/私钥匹配与有效期，但不会监听。
        try:
            RemoteMcpService(config)
        except (ValueError, OSError) as exc:
            sys.stderr.write(f"amr2-mcp TLS material error: {exc}\n")
            return 2
        sys.stdout.write(
            f"amr2-mcp configuration ok: {config.bind_host}:{config.bind_port}\n"
        )
        return 0

    try:
        return asyncio.run(_serve(RemoteMcpService(config)))
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        sys.stderr.write(f"amr2-mcp failed: {type(exc).__name__}: {exc}\n")
        return 1


async def _serve(service: RemoteMcpService) -> int:
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stop.set)
        except NotImplementedError:
            pass

    await service.start()
    sys.stdout.write(
        f"amr2-mcp listening on https://{service.config.bind_host}:{service.bound_port}/mcp\n"
    )
    sys.stdout.flush()
    try:
        await stop.wait()
    finally:
        await service.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
