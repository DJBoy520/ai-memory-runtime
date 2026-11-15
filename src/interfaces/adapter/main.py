"""AMR 2.x adapter CLI.

Usage: python -m src.interfaces.adapter.main --config /etc/amr/adapter.json
"""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys
from typing import Optional, Sequence

from src.interfaces.adapter.config import load_adapter_config
from src.interfaces.adapter.server import AdapterService


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AMR 2.x local UDS adapter")
    parser.add_argument("--config", required=True, help="adapter JSON config path")
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate configuration and exit without opening sockets",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    try:
        config = load_adapter_config(args.config)
    except (ValueError, OSError) as exc:
        sys.stderr.write(f"amr2-adapter configuration error: {exc}\n")
        return 2

    if args.check:
        sys.stdout.write(
            "amr2-adapter configuration ok: "
            + ", ".join(binding.socket_path for binding in config.bindings)
            + "\n"
        )
        return 0

    try:
        return asyncio.run(_serve(AdapterService(config)))
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        sys.stderr.write(f"amr2-adapter failed: {type(exc).__name__}: {exc}\n")
        return 1


async def _serve(service: AdapterService) -> int:
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stop.set)
        except NotImplementedError:
            pass

    await service.start()
    sys.stdout.write(
        "amr2-adapter listening on "
        + ", ".join(service.socket_paths)
        + "\n"
    )
    sys.stdout.flush()
    try:
        await stop.wait()
    finally:
        await service.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
