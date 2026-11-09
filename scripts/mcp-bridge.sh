#!/usr/bin/env bash
# Lightweight wrapper to invoke AMR MCP stdio bridge over SSH or locally
export PYTHONPATH="/home/dj/WorkSpaces/ai-memory-runtime:${PYTHONPATH:-}"
export AMR_SOURCE_AGENT="${AMR_SOURCE_AGENT:-zcode}"
exec /home/dj/WorkSpaces/ai-memory-runtime/.venv/bin/python /home/dj/WorkSpaces/ai-memory-runtime/src/interfaces/mcp/bridge.py "$@"
