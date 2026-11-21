#!/usr/bin/env bash
# AMR 一键部署（面向外部用户）：Qdrant 服务器已就绪的前提下，
#   curl -fsSL https://raw.githubusercontent.com/DJBoy520/ai-memory-runtime/main/deploy/docker/oneclick.sh -o oneclick.sh
#   AMR_QDRANT_URL=http://<qdrant-host>:6333 AMR_QDRANT_API_KEY=<key> \
#     AMR_MODEL_DIR=/path/to/models/bge-m3 bash oneclick.sh
# 自包含：内嵌 compose 与配置模板，自带生成 TLS(私有 CA)/token/credentials，
# 需要 docker + docker compose + openssl；模型不随镜像分发，需先从官方源下载（脚本会校验并给出命令）。
set -euo pipefail
umask 077

# ---------- 参数 ----------
AMR_DIR="${AMR_DIR:-$HOME/amr-deploy}"
AMR_VERSION="${AMR_VERSION:-2.2.2}"
AMR_IMAGE="${AMR_IMAGE:-ckddkk/amr}"
AMR_QDRANT_URL="${AMR_QDRANT_URL:?需要 AMR_QDRANT_URL，例如 http://192.0.2.50:6333}"
AMR_QDRANT_API_KEY="${AMR_QDRANT_API_KEY:?需要 AMR_QDRANT_API_KEY}"
AMR_COLLECTION="${AMR_COLLECTION:-ai_memory}"        # 全新 Qdrant 用默认；指向共享 Qdrant 时务必换名隔离
AMR_HTTP_PORT="${AMR_HTTP_PORT:-8443}"               # 宿主端口（容器内固定 8443）
AMR_MODEL_DIR="${AMR_MODEL_DIR:?需要 AMR_MODEL_DIR（BGE-M3 模型目录，官方源下载，见文末提示）}"
AMR_CN="${AMR_CN:-amr-local.example.net}"
AMR_IP="${AMR_IP:-$(hostname -I 2>/dev/null | awk '{print $1}')}"; AMR_IP="${AMR_IP:-127.0.0.1}"
AMR_DAYS="${AMR_CERT_DAYS:-3287}"

command -v docker >/dev/null || { echo '需要 docker' >&2; exit 1; }
docker compose version >/dev/null 2>&1 || { echo '需要 docker compose v2' >&2; exit 1; }
command -v openssl >/dev/null || { echo '需要 openssl' >&2; exit 1; }
[ -f "$AMR_MODEL_DIR/pytorch_model.bin" ] || {
  echo "模型缺失：$AMR_MODEL_DIR/pytorch_model.bin。官方源下载（约 2.2GB）：" >&2
  echo '  pip install -U "huggingface_hub"' >&2
  echo '  hf download BAAI/bge-m3 --include "config.json" "tokenizer.json" "tokenizer_config.json" "special_tokens_map.json" "sentencepiece.bpe.model" "pytorch_model.bin" --local-dir '"$AMR_MODEL_DIR" >&2
  exit 1
}

# ---------- 目录 ----------
mkdir -p "$AMR_DIR"/{config,secrets/tls,data,tokens}
cd "$AMR_DIR"

# ---------- TLS：自包含私有 CA + 叶子（SAN=域名+宿主IP+本机回环） ----------
if [ ! -f secrets/tls/server.key ]; then
  openssl ecparam -name prime256v1 -genkey -noout -out secrets/tls/root.key
  openssl req -x509 -new -key secrets/tls/root.key -sha256 -days "$AMR_DAYS" \
    -subj "/CN=AMR-Local-Root-CA/O=AMR/C=CN" \
    -addext "basicConstraints = critical, CA:TRUE" \
    -addext "keyUsage = critical, keyCertSign, cRLSign" -out secrets/tls/root.crt
  openssl ecparam -name prime256v1 -genkey -noout -out secrets/tls/server.key
  openssl req -new -key secrets/tls/server.key -subj "/CN=${AMR_CN}/O=AMR/C=CN" -out server.csr
  EXT="$(mktemp)"; trap 'rm -f "$EXT"' EXIT
  cat > "$EXT" <<EOF2
basicConstraints = critical, CA:FALSE
keyUsage = critical, digitalSignature, keyAgreement
extendedKeyUsage = serverAuth
subjectAltName = DNS:${AMR_CN}, IP:${AMR_IP}, IP:127.0.0.1
EOF2
  openssl x509 -req -in server.csr -CA secrets/tls/root.crt -CAkey secrets/tls/root.key \
    -CAcreateserial -days "$AMR_DAYS" -sha256 -extfile "$EXT" -out secrets/tls/server-leaf.crt
  cat secrets/tls/server-leaf.crt secrets/tls/root.crt > secrets/tls/server-chain.crt
  echo "TLS 已生成（root.key 仅留本目录，勿分发）"
fi

# ---------- 身份：internal/admin token + 两个演示 agent（claude_code/codex） ----------
INTERNAL_TOKEN="$(openssl rand -hex 32)"
ADMIN_TOKEN="$(openssl rand -hex 32)"
printf '%s' "$INTERNAL_TOKEN" > secrets/internal.token
printf '%s' "$ADMIN_TOKEN" > secrets/admin.token
TOK_CLAUDE="$(openssl rand -hex 32)"; TOK_CODEX="$(openssl rand -hex 32)"
cat > secrets/credentials.json <<EOF2
{
 "$TOK_CLAUDE": {"agent_id": "claude_code", "tools": ["memory_create", "memory_search", "memory_get", "memory_update"]},
 "$TOK_CODEX": {"agent_id": "codex", "tools": ["memory_create", "memory_search", "memory_get"]}
}
EOF2
printf '%s' "$TOK_CLAUDE" > tokens/claude_code.token
printf '%s' "$TOK_CODEX" > tokens/codex.token

# ---------- daemon 配置 ----------
cat > config/config.yaml <<EOF2
server:
  business_socket: "/run/amr/qdrant-bge.sock"
  admin_socket: "/run/amr/qdrant-bge-admin.sock"
  socket_mode: 0o600
  max_request_bytes: 4194304
  allowed_agents: ["claude_code", "codex", "system", "default_agent", "test_runner"]
model:
  name_or_path: "/models/bge-m3"
  device: "cpu"
  fp16: false
  max_batch: 16
  max_queue_size: 64
  idle_timeout_seconds: 0
  max_token_length: 8192
  preload_on_start: true
  load_timeout_seconds: 300.0
  request_load_wait_seconds: 25.0
  load_stall_exit_seconds: 420.0
  watchdog_exit_enabled: true
storage:
  sqlite_path: "/data/amr/sessions.db"
  busy_timeout: 5000
  wal_enabled: true
  idempotency_retention_days: 7
qdrant:
  url: "$AMR_QDRANT_URL"
  api_key: "$AMR_QDRANT_API_KEY"
  timeout: 10.0
  collections: ["$AMR_COLLECTION"]
search:
  default_score_threshold: 0.52
  project_fallback_ids: ["global", "general"]
EOF2

# ---------- gateway 配置 ----------
cat > config/remote.json <<EOF2
{
 "business_socket": "/run/amr/qdrant-bge.sock",
 "admin_socket": "/run/amr/qdrant-bge-admin.sock",
 "credentials_file": "/etc/amr/secrets/credentials.json",
 "cert_file": "/etc/amr/secrets/tls/server-chain.crt",
 "key_file": "/etc/amr/secrets/tls/server.key",
 "allowed_hosts": ["127.0.0.1:${AMR_HTTP_PORT}", "localhost:${AMR_HTTP_PORT}", "${AMR_IP}:${AMR_HTTP_PORT}", "${AMR_CN}:${AMR_HTTP_PORT}"],
 "allowed_origins": ["https://${AMR_CN}:8443"],
 "bind_host": "0.0.0.0",
 "bind_port": 8443,
 "crypto_profile": "system"
}
EOF2

# ---------- compose（不用固定 container_name，避免与其他部署冲突） ----------
cat > docker-compose.yml <<EOF2
name: amr-$(basename "$AMR_DIR" | tr -c 'a-z0-9' '-')
services:
  amr-daemon:
    image: ${AMR_IMAGE}:${AMR_VERSION}
    user: "1000:1000"
    restart: unless-stopped
    environment:
      AMR_CONFIG_PATH: /etc/amr/config.yaml
      AMR_INTERNAL_AUTH_REQUIRED: "1"
      AMR_INTERNAL_TOKEN_FILE: /etc/amr/secrets/internal.token
      AMR_ADMIN_TOKEN_FILE: /etc/amr/secrets/admin.token
      AMR_AGENT_CREDENTIALS_FILE: /etc/amr/secrets/credentials.json
      XDG_RUNTIME_DIR: /run/amr
      PYTHONUNBUFFERED: "1"
    command: ["sh", "-c", "rm -f /run/amr/*.sock; exec python src/main.py"]
    volumes:
      - amr-run:/run/amr
      - ./config/config.yaml:/etc/amr/config.yaml:ro
      - ./secrets:/etc/amr/secrets:ro
      - ./data:/data/amr
      - ${AMR_MODEL_DIR}:/models/bge-m3:ro
    healthcheck:
      test: ["CMD", "sh", "-c", "test -S /run/amr/qdrant-bge.sock"]
      interval: 30s
      timeout: 5s
      retries: 5
      start_period: 240s
  amr-gateway:
    image: ${AMR_IMAGE}:${AMR_VERSION}
    user: "1000:1000"
    restart: unless-stopped
    depends_on:
      - amr-daemon
    ports:
      - "${AMR_HTTP_PORT}:8443"
    environment:
      AMR_INTERNAL_AUTH_REQUIRED: "1"
      AMR_INTERNAL_TOKEN_FILE: /etc/amr/secrets/internal.token
      XDG_RUNTIME_DIR: /run/amr
      PYTHONUNBUFFERED: "1"
    command: >
      sh -c 'while [ ! -S /run/amr/qdrant-bge.sock ]; do sleep 1; done;
             exec python -m src.interfaces.mcp.remote_main --config /etc/amr/remote.json'
    volumes:
      - amr-run:/run/amr
      - ./config/remote.json:/etc/amr/remote.json:ro
      - ./secrets:/etc/amr/secrets:ro
    healthcheck:
      test: ["CMD", "curl", "-kfsS", "https://127.0.0.1:8443/healthz"]
      interval: 30s
      timeout: 5s
      retries: 5
      start_period: 60s
volumes:
  amr-run:
EOF2

# ---------- 权限 ----------
CHOWN_UID="${AMR_CHOWN_UID:-1000}"
if command -v chown >/dev/null; then chown -R "$CHOWN_UID:$CHOWN_UID" config secrets data 2>/dev/null || true; fi
chmod 700 config secrets
find config secrets -type f -exec chmod 600 {} \;

# ---------- 启动 + 等健康 ----------
docker compose up -d
echo "等待容器 healthy（模型 CPU 冷加载最长约 2 分钟）……"
for i in $(seq 1 60); do
  GW="$(docker compose ps -q amr-gateway | head -1)"
  DM="$(docker compose ps -q amr-daemon | head -1)"
  if [ -n "$GW" ] && [ -n "$DM" ] \
     && [ "$(docker inspect -f '{{.State.Health.Status}}' "$GW")" = healthy ] \
     && [ "$(docker inspect -f '{{.State.Health.Status}}' "$DM")" = healthy ]; then
    break
  fi
  sleep 5
  [ "$i" = 60 ] && { echo '健康等待超时，请 docker compose logs 排查' >&2; docker compose logs --tail 20 >&2; exit 1; }
done

echo
echo "================ READY ================"
echo "HTTPS MCP : https://127.0.0.1:${AMR_HTTP_PORT}/mcp （LAN 内用 https://${AMR_IP}:${AMR_HTTP_PORT}/mcp）"
echo "集合      : $AMR_COLLECTION"
echo "agent 令牌: $AMR_DIR/tokens/claude_code.token / tokens/codex.token"
echo "客户端 CA : $AMR_DIR/secrets/tls/root.crt —— 客户端必须信任它，任何客户端都不要关证书校验"
echo "  信任方式(三选一, 详见 DOC-AMR-10 §1.5.1):"
echo "    A 系统信任库(推荐, 同机一次到位):"
echo "       sudo cp $AMR_DIR/secrets/tls/root.crt /usr/local/share/ca-certificates/amr-local.crt && sudo update-ca-certificates"
echo "    B 环境变量(无 sudo): Node 系 export NODE_EXTRA_CA_CERTS=$AMR_DIR/secrets/tls/root.crt"
echo "                       Rust/Python 系 export SSL_CERT_FILE=$AMR_DIR/secrets/tls/root.crt"
echo "    C 每客户端显式配置: adapter/bridge 的 ca_file 字段或 curl --cacert"
echo "自检     : curl -s -H \"Authorization: Bearer \$(cat $AMR_DIR/tokens/claude_code.token)\" https://127.0.0.1:${AMR_HTTP_PORT}/readyz"
echo "           （配置系统信任后不带 --cacert 直接跑也应是 200；对照: 证书错误说明信任尚未生效）"
