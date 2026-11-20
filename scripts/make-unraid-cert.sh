#!/usr/bin/env bash
# 为 Unraid 上的 AMR HTTPS 网关生成独立私有 CA + 叶子证书。
#
# 背景：现有生产叶子证书由 AEP-Issuing-ECDSA-CA 签发，但 intermediate CA
# 私钥不在本机，无法签发新叶子；且两套后端并存期间需要可区分的主机名。
# 本脚本生成自包含的新 CA（仅覆盖 Unraid 实例），不触碰旧证书链。
#
# 用法：
#   AMR_UNRAID_CN=amr-unraid.example.net AMR_UNRAID_IP=192.0.2.50 \
#     bash scripts/make-unraid-cert.sh [输出目录]
#   真实部署时通过环境变量传入实际域名与宿主 IP（默认占位符，不入库真值）。
# 默认输出目录：~/.config/amr/tls-unraid/
# 产物：
#   root.crt                 新私有根 CA（分发给所有连接 Unraid 网关的客户端做 ca_file）
#   server-chain.crt         叶子+根 链（网关 cert_file）
#   server.key               叶子私钥（0600，网关 key_file）
#   <CN>.csr                 留档
# 注意：root.key 是新 CA 私钥，只保存在本目录；不要拷入 Unraid 部署目录/仓库/镜像。
set -euo pipefail
umask 077

OUT="${1:-$HOME/.config/amr/tls-unraid}"
CN="${AMR_UNRAID_CN:-amr-unraid.example.net}"
IP="${AMR_UNRAID_IP:-192.0.2.50}"
DAYS="${AMR_CERT_DAYS:-3287}"   # ~9 年，与内网私有 CA 惯例一致

mkdir -p "$OUT"
cd "$OUT"

if [ -e root.key ] || [ -e server.key ]; then
  echo "输出目录已存在证书材料（$OUT），如需重签请先手动备份并清空。" >&2
  exit 1
fi

# 1) 自包含根 CA（ECDSA P-256，与旧链算法一致；显式注入 CA 扩展）
openssl ecparam -name prime256v1 -genkey -noout -out root.key
openssl req -x509 -new -key root.key -sha256 -days "$DAYS" \
  -subj "/CN=AEP-Unraid-Root-CA/OU=AEP/O=AEP/L=Beijing/ST=Beijing/C=CN" \
  -addext "basicConstraints = critical, CA:TRUE" \
  -addext "keyUsage = critical, keyCertSign, cRLSign" \
  -addext "subjectKeyIdentifier = hash" \
  -out root.crt

# 2) 叶子私钥 + CSR（SAN 覆盖域名与宿主 IP）
openssl ecparam -name prime256v1 -genkey -noout -out server.key
openssl req -new -key server.key \
  -subj "/CN=${CN}/OU=AEP TLS Server Intl/O=AEP/L=Beijing/ST=Beijing/C=CN" \
  -addext "subjectAltName = DNS:${CN}, IP:${IP}" \
  -out "${CN}.csr"

EXT="$(mktemp)"
trap 'rm -f "$EXT"' EXIT
cat > "$EXT" <<EOF
basicConstraints = critical, CA:FALSE
keyUsage = critical, digitalSignature, keyAgreement
extendedKeyUsage = serverAuth
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid
subjectAltName = DNS:${CN}, IP:${IP}
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
