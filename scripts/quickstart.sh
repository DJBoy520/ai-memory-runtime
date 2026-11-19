#!/usr/bin/env bash
set -euo pipefail
umask 077
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
TRYOUT="${XDG_DATA_HOME:-$HOME}/.amr-tryout"
RUNTIME_BASE="${XDG_RUNTIME_DIR:-$TRYOUT/run}"
RUNTIME="$RUNTIME_BASE/amr-tryout-$(id -u)"
MODE=run
for arg in "$@"; do
  case "$arg" in
    --cpu) ;;
    --download-only) MODE=download ;;
    --clean) MODE=clean ;;
    *) echo "Usage: bash scripts/quickstart.sh [--cpu] [--download-only|--clean]" >&2; exit 2 ;;
  esac
done
if [ "$MODE" = clean ]; then
  if [ -d "$TRYOUT" ]; then
    exec 9>"$TRYOUT/.lock"
    flock -n 9 || { echo '试用实例正在运行，请先停止后再清理。' >&2; exit 1; }
    rm -rf -- "$TRYOUT"
  fi
  if [ "$RUNTIME_BASE" != "$TRYOUT/run" ]; then rm -rf -- "$RUNTIME"; fi
  echo "已删除试用目录 $TRYOUT 和试用 socket 目录。"
  exit 0
fi
command -v python3 >/dev/null || { echo '需要 Python 3.10+。' >&2; exit 1; }
python3 -c 'import sys; assert sys.version_info >= (3,10), "Python 3.10+ required"'
mkdir -p "$TRYOUT" "$RUNTIME" "$TRYOUT/spool"
chmod 700 "$TRYOUT" "$RUNTIME" "$TRYOUT/spool"
exec 9>"$TRYOUT/.lock"
flock -n 9 || { echo '试用实例已在运行，请先停止。' >&2; exit 1; }
if [ -r /proc/meminfo ]; then
  available_kb="$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)"
  if [ "${available_kb:-0}" -lt 3145728 ]; then
    echo "提示：可用内存低于 3 GiB，BGE-M3 CPU 推理可能很慢或加载失败。" >&2
  fi
fi
PY="$TRYOUT/venv/bin/python"
if [ ! -x "$PY" ]; then python3 -m venv "$TRYOUT/venv"; fi
if [ ! -f "$TRYOUT/dependencies-v2.ready" ]; then
  echo '安装 CPU 版 PyTorch（固定版本，首次下载可能需要几分钟）……'
  "$PY" -m pip install 'torch==2.9.1' --index-url https://download.pytorch.org/whl/cpu
  "$PY" -m pip install 'transformers==4.57.6' 'qdrant-client==1.12.2' 'pydantic==2.12.5' 'PyYAML==6.0.3' 'jsonschema==4.23.0'
  touch "$TRYOUT/dependencies-v2.ready"
fi
export AMR_TRYOUT_DIR="$TRYOUT" AMR_TRYOUT_RUNTIME="$RUNTIME" PYTHONPATH="$ROOT"
unset AMR_INTERNAL_AUTH_REQUIRED AMR_INTERNAL_TOKEN_FILE AMR_ADMIN_TOKEN_FILE AMR_AGENT_CREDENTIALS_FILE AMR2_TOKEN_FILE AMR_CONFIG_PATH
export XDG_RUNTIME_DIR="$RUNTIME"
echo '首次需要下载 BGE-M3 权重约 2.2 GB；20 Mbit/s 下仅下载约 15 分钟。已缓存时会复用。'
if ! "$PY" - <<'PY'
import os
from pathlib import Path
from huggingface_hub import snapshot_download
root = Path(os.environ['AMR_TRYOUT_DIR'])
try:
    local = Path(os.environ['PYTHONPATH'].split(os.pathsep)[0]) / 'models' / 'bge-m3'
    if (local / 'config.json').is_file() and ((local / 'pytorch_model.bin').is_file() or (local / 'model.safetensors').is_file()):
        model = str(local)
    else:
        patterns = ['config.json', 'tokenizer.json', 'tokenizer_config.json', 'special_tokens_map.json', 'sentencepiece.bpe.model', 'pytorch_model.bin']
        try:
            model = snapshot_download('BAAI/bge-m3', allow_patterns=patterns, local_files_only=True)
            if not all((Path(model) / name).is_file() for name in ('config.json', 'tokenizer.json', 'pytorch_model.bin')):
                raise FileNotFoundError('incomplete cached model')
        except Exception:
            model = snapshot_download('BAAI/bge-m3', allow_patterns=patterns)
    (root / 'model-path').write_text(model)
except Exception as exc:
    raise SystemExit(f'模型下载失败：{exc}')
PY
then exit 1; fi
if [ "$MODE" = download ]; then echo '模型已就绪。运行 bash scripts/quickstart.sh --cpu 启动。'; exit 0; fi
"$PY" - <<'PY'
import json, os
from pathlib import Path
root = Path(os.environ['AMR_TRYOUT_DIR'])
runtime = Path(os.environ['AMR_TRYOUT_RUNTIME'])
config = {'server': {'business_socket': str(runtime / 'business.sock'), 'admin_socket': str(runtime / 'admin.sock'), 'allowed_agents': ['claude_code', 'codex']}, 'model': {'device': 'cpu', 'fp16': False, 'idle_timeout_seconds': 0, 'request_load_wait_seconds': 180}, 'storage': {'sqlite_path': str(root / 'memories.db')}, 'qdrant': {'collections': ['ai_memory']}}
path = root / 'config.json'
if not path.exists():
    path.write_text(json.dumps(config, indent=2))
    path.chmod(0o600)
PY
export AMR_TRYOUT_MODEL="$(cat "$TRYOUT/model-path")"
exec "$PY" "$ROOT/scripts/quickstart_runtime.py"
