#!/usr/bin/env bash
# Lightweight wrapper to invoke AMR MCP stdio bridge over SSH or locally
export PYTHONPATH="/home/dj/WorkSpaces/ai-memory-runtime:${PYTHONPATH:-}"
export AMR_SOURCE_AGENT="${AMR_SOURCE_AGENT:-zcode}"
exec /usr/bin/python3 /home/dj/WorkSpaces/ai-memory-runtime/src/interfaces/mcp/bridge.py "$@"
