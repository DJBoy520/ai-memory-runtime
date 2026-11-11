#!/usr/bin/env bash
# Lightweight wrapper to invoke AMR MCP stdio bridge over SSH or locally
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

export PYTHONPATH="${ROOT}${PYTHONPATH:+:${PYTHONPATH}}"
export AMR_SOURCE_AGENT="${AMR_SOURCE_AGENT:-openclaw}"
export AMR_SPOOL_DIR="${AMR_SPOOL_DIR:-${XDG_DATA_HOME:-${HOME}/.local/share}/amr/mcp-spool}"
# AMR_SOCKET_PATH is preserved if set in environment

PYTHON_BIN="${ROOT}/.venv/bin/python"
if [ ! -x "${PYTHON_BIN}" ]; then
    if [ -n "${AMR_PYTHON:-}" ] && [ -x "${AMR_PYTHON}" ]; then
        PYTHON_BIN="${AMR_PYTHON}"
    elif [ -n "${VIRTUAL_ENV:-}" ] && [ -x "${VIRTUAL_ENV}/bin/python" ]; then
        PYTHON_BIN="${VIRTUAL_ENV}/bin/python"
    elif git_common="$(git -C "${ROOT}" rev-parse --git-common-dir 2>/dev/null)" && [ -x "${git_common}/../.venv/bin/python" ]; then
        PYTHON_BIN="$(cd "${git_common}/.." && pwd)/.venv/bin/python"
    elif command -v python3 >/dev/null 2>&1; then
        PYTHON_BIN="$(command -v python3)"
    else
        PYTHON_BIN="python3"
    fi
fi

exec "${PYTHON_BIN}" "${ROOT}/src/interfaces/mcp/bridge.py" "$@"
