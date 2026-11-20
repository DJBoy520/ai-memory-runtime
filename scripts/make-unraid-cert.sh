#!/usr/bin/env bash
# 为 Unraid 上的 AMR HTTPS 网关生成独立私有 CA + 叶子证书（amr-unraid.djper.cn）。
#
# 背景：现有 amr.djper.cn 叶子证书由 AEP-Issuing-ECDSA-CA 签发，但 intermediate CA
# 私钥不在本机，无法签发新叶子；且两套后端并存期间需要可区分的主机名。
# 本脚本生成自包含的新 CA（仅覆盖 Unraid 实例），不触碰旧证书链。
#
# 用法：
#   bash scripts/make-unraid-cert.sh [输出目录]
# 默认输出目录：~/.config/amr/tls-unraid/
# 产物：
#   root.crt                 新私有根 CA（分发给所有连接 Unraid 网关的客户端做 ca_file）
#   server-chain.crt         叶子+根 链（网关 cert_file）
#   server.key               叶子私钥（0600，网关 key_file）
#   amr-unraid.djper.cn.csr  留档
set -euo pipefail
umask 077

OUT="${1:-$HOME/.config/amr/tls-unraid}"
CN="${AMR_UNRAID_CN:-amr-unraid.djper.cn}"
DAYS="${AMR_CERT_DAYS:-3287}"   # ~9 年，与内网私有 CA 惯例一致

mkdir -p "$OUT"
cd "$OUT"

if [ -e root.key ] || [ -e server.key ]; then
  echo "输出目录已存在证书材料（$OUT），如需重签请先手动备份并清空。" >&2
  exit 1
fi

# 1) 自包含根 CA（ECDSA P-256，与旧链算法一致）
openssl ecparam -name prime256v1 -genkey -noout -out root.key
openssl req -x509 -new -key root.key -sha256 -days "$DAYS" \
  -subj "/CN=AEP-Unraid-Root-CA/OU=AEP/O=AEP/L=Beijing/ST=Beijing/C=CN" \
  -out root.crt

# 2) 叶子私钥 + CSR（SAN 覆盖域名与本机宿主 IP）
openssl ecparam -name prime256v1 -genkey -noout -out server.key
openssl req -new -key server.key \
  -subj "/CN=${CN}/OU=AEP TLS Server Intl/O=AEP/L=Beijing/ST=Beijing/C=CN" \
  -out "${CN}.csr"

EXT="$(mktemp)"
trap 'rm -f "$EXT"' EXIT
cat > "$EXT" <<EOF
basicConstraints = critical, CA:FALSE
keyUsage = critical, digitalSignature, keyAgreement
extendedKeyUsage = serverAuth
subjectAltName = DNS:${CN}, IP:192.168.30.161
EOF

openssl x509 -req -in "${CN}.csr" -CA root.crt -CAkey root.key \
  -CAcreateserial -days "$DAYS" -sha256 -extfile "$EXT" -out server-leaf.crt

cat server-leaf.crt root.crt > server-chain.crt

chmod 0600 root.key server.key
chmod 0644 root.crt server-leaf.crt server-chain.crt "${CN}.csr"

echo "已生成："
ls -l "$OUT"
echo
echo "客户端 ca_file 使用：$OUT/root.crt"
