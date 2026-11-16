# AMR 2.x 生产切换记录（2026-11-16）

## 拓扑与边界

- 主机：`192.168.30.166`（Ubuntu，用户级 systemd）
- 内网域名：`amr.djper.cn`，由 AdGuard Home DNS 重写指向 `192.168.30.166`
- **仅内网**：`amr2-mcp` 绑定 `192.168.30.166:8443`，未做 WAN 端口映射；不要添加 8443 转发
- 数据库/Qdrant 未迁移：仍使用 `data/sessions.db` 与 `192.168.30.161:6333`

## 证书（AEP-PKI ACME，DNS-01）

| 项 | 值 |
| --- | --- |
| Profile | `aep-tls-server-intl-v1`（ECDSA P-256，180 天） |
| 签发 | `https://ca.aep.org.cn/acme/directory`，acme.sh v3.1.7，账号 ES256 |
| 挑战 | `_acme-challenge.amr.djper.cn TXT`（手动添加，AEP 的 unbound 解析） |
| Leaf | `CN=amr.djper.cn`，serial `1FDE84933AD477258C56CC49A1CB9F9E` |
| 有效期 | 2026-11-16 12:15:50Z → 2027-05-15 12:15:50Z |
| 链 | leaf → `AEP-Issuing-ECDSA-CA` → `AEP-Root-ECDSA-CA` |
| 根指纹 | `6C:0F:FB:28:5E:8C:AD:C1:…:DB:84:AE:6F`（SHA-256） |
| 文件 | `~/.config/amr/tls/`：`server-chain.crt`（叶+中间）、`server.key`(0600)、`root.crt`、`intermediate.crt` |
| ACME 状态 | `~/.config/amr/acme/amr.djper.cn_ecc/`（账号、订单、fullchain） |

> 早期曾用 REST 直签（`/api/v1/enroll/intl` + 一次性 enrollment token）签发过同 CA 的证书，后由 ACME 证书替换；根指纹一致，客户端信任配置未变。

**续期**：DNS-01 手动模式，180 天内需重新取 TXT 并 `acme.sh --renew`；如需自动续期，接 djper.cn 的 DNS API（`--dns dns_ali` 等）。

## 服务与配置

| 单元 | 状态 | 启动 | 配置 |
| --- | --- | --- | --- |
| `amr.service` | active / enabled | 2.x daemon（`src/main.py`） | `config/config.yaml` |
| `amr2-mcp.service` | active / enabled | 远程 HTTPS MCP | `~/.config/amr/amr2-remote.json` |
| `amr2-adapter.service` | active / enabled | 5 个 UDS | `~/.config/amr/amr2-adapter.json` |

- `amr2-mcp` 使用独立 venv：`~/.local/share/amr/remote-venv`（`requirements-remote.txt`，`mcp==1.30.0`，uvicorn 0.54）。
- `amr2-adapter` 使用生产 `.venv`（httpx，不依赖 `mcp`）。
- 凭证：`~/.config/amr/remote-credentials.json`（0600），七个身份
  dsh/openclaw/opencode/hermes/zcode/claude_code/codex；每身份独立 token，工具白名单为
  `memory_search` / `memory_create` / `memory_ingest_session`。
- 每身份 token 落在 `~/.config/amr/tokens/<agent>.env`（0600），供客户端 `AMR_TOKEN` 使用。
- 适配器 socket：`/run/user/1000/amr2-{dsh,openclaw,opencode,hermes,zcode}.sock`（0600）。

## 已停止的旧服务

| 单元 | 操作 |
| --- | --- |
| `amr-distillation.timer` / `.service` | disable + stop；切换前该 oneshot 卡在 activating 并反复重试外部模型 |
| `mineru.service`（MinerU2.5-Pro PDF 解析） | disable + stop |
| 1.x `amr` daemon 进程 | 由同一 `amr.service` 单元以 2.x 代码重启替换 |

## 切换前后验证

| 检查 | 结果 |
| --- | --- |
| `remote_main --check` | ok（`192.168.30.166:8443`） |
| `openssl s_client -verify_hostname amr.djper.cn` | `Verify return code: 0`，TLS 1.3 |
| `/healthz` | 200 |
| `/mcp` 无 token | 401 |
| token `tools/list` | 3 个工具（按令牌白名单） |
| `memory_search`（经 HTTPS） | 成功，返回生产记忆 |
| `session.ingest` rev=17 新消息 | `inserted=1` |
| 同消息 rev=10 旧快照 | `ignored=1`（2.x 水位生效） |
| 测试数据 | 已从 `sessions/messages/ingest_log/message_source_revisions` 删除 |
| adapter `amr.ping`（dsh/openclaw） | `{"status":"ok"}` |
| adapter `session.ingest` | 成功；伪造的 `agent_id` 被绑定为 `dsh` |
| 旧 1.x `bridge.py`（运行中的 DSH） | 仍可调用同一 daemon socket（2.x 向后兼容） |

## 备份与回滚

切换前备份：`~/backups/amr-pre2x-20261116-201705/`
（`sessions.db` 通过 SQLite backup API 复制、`config.yaml`、`~/.config/amr` 配置副本、`SHA256SUMS`）。

回滚步骤：

```sh
systemctl --user stop amr2-adapter amr2-mcp
# 恢复 sessions.db 与 config.yaml（保留当前 2.x 副本以便排查）
systemctl --user restart amr.service
```

- 2.x 只新增 `message_source_revisions` 表，1.x daemon 忽略它，回滚不需改 schema。
- 若要完整退回 1.x 代码线，需 `git checkout release/1.x` 后重启 `amr.service`（本次未做）。

## 尚未完成的客户端切换

1. **DSH web**：当前仍加载切换前的旧插件（走 `qdrant-bge.sock` 直连 daemon）。要启用
   2.x 插件（`amr2-dsh.sock`、SQLite spool、水位归档），需改 profile 后重启 DSH，
   **会断开当前会话**。
   - `mcp-amr` 改为 `python -m src.interfaces.mcp.bridge2 --agent dsh`
   - 插件 `config`：`socketPath=/run/user/1000/amr2-dsh.sock`、`spoolBackend=sqlite`、
     `persistenceDir=~/.local/share/amr-spool/dsh-web`
2. **OpenClaw / OpenCode / Hermes / ZCode**：插件 `socketPath` 指向对应 `amr2-*.sock`；当前均未运行。
3. **Claude Code / Codex**：`https://amr.djper.cn:8443/mcp` + `NODE_EXTRA_CA_CERTS` /
   `SSL_CERT_FILE=~/.config/amr/tls/root.crt`，token 取对应 `tokens/<agent>.env`。

## 已知注意点

- `amr.djper.cn` 的 DNS 重写在 AdGuard Home；若 AdGuard 不可用，内网解析会回落到公网
  `101.200.162.104`，客户端会连不上。建议在路由器再留一条同等 host override。
- 证书只有 DNS SAN，客户端必须用主机名，不能用 `192.168.30.166` 直连（主机名校验失败）。
- 手动 DNS-01 续期；到期前 30 天应重新取 TXT 并换证。
