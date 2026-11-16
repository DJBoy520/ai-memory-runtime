# AMR 2.x 生产切换交接（2026-11-16）

- **交接对象**：Codex
- **前序交接**：[HANDOFF-20261116-AMR2.md](HANDOFF-20261116-AMR2.md)（2.x 代码开发阶段，HEAD `33abbae`）
- **本交接范围**：代码复审与修复、隔离演练、2.x 生产切换、证书签发、身份鉴权验证
- **当前分支**：`release/2.x`，HEAD `50f123b`，**工作区干净**
- **未 push**：2.x 全部提交只在本地
- **生产状态**：**已切到 2.x**，`amr.service` / `amr2-mcp` / `amr2-adapter` 三个单元 active
- **DSH 未切换**：web profile 仍加载旧插件（`bridge.py` → `bridge.qdrant-bge.sock`）

---

## 1. 一句话结论

2.x **服务端已上线并在内网可用**：HTTPS + AEP 证书 + Bearer token 鉴权、五个 adapter
socket、水位归档全部验证通过。**未完成的是各 agent 客户端接入**——目前没有任何一个
agent 真正使用 2.x 路径，DSH 尤其需要重启才能加载新插件。

---

## 2. 先读什么

按顺序：

1. [本交接第 3 节](#3-当前生产状态) —— 先看清线上跑的是什么。
2. [DEPLOYMENT-RECORD-20261116-AMR2.md](DEPLOYMENT-RECORD-20261116-AMR2.md) —— 切换记录、证书指纹、备份与回滚。
3. [isolated-rehearsal.md](superpowers/notes/2026-11-16-isolated-rehearsal.md) —— 隔离全栈演练结果与暴露的问题。
4. [cross-audit-log.md Round 9](superpowers/notes/2026-11-15-cross-audit-log.md) —— 本轮 Gemini 复审 17 条的逐条判定。
5. [DOC-AMR-09](DOC-AMR-09-CLIENT-2X.md) / [DOC-AMR-10](DOC-AMR-10-DEPLOY-2X.md) —— 客户端契约与部署。
6. [HANDOFF-20261116-AMR2.md](HANDOFF-20261116-AMR2.md) §4 —— 不可回退的设计决策。

---

## 3. 当前生产状态

### 3.1 服务（用户级 systemd，用户 `dj`）

| 单元 | 状态 | 启动 | 配置 |
| --- | --- | --- | --- |
| `amr.service` | active / enabled | 2.x daemon（`src/main.py`） | `config/config.yaml` |
| `amr2-mcp.service` | active / enabled | HTTPS MCP，绑定 `192.168.30.166:8443` | `~/.config/amr/amr2-remote.json` |
| `amr2-adapter.service` | active / enabled | 5 个 UDS | `~/.config/amr/amr2-adapter.json` |
| `mineru.service` | **inactive / disabled** | PDF 解析，已按要求停止 | — |
| `amr-distillation.timer` | **inactive / disabled** | 切换前卡在 activating 反复失败，已停 | — |
| `amr-reconciliation.timer` | active / waiting | 未动，每天 02:30 干跑 | — |

- `amr2-mcp` 用独立 venv：`~/.local/share/amr/remote-venv`（`requirements-remote.txt`，`mcp==1.30.0`）。**生产 `.venv` 不含 mcp，这是刻意的**。
- `amr2-adapter` 用生产 `.venv`（只需要 httpx）。
- adapter socket：`/run/user/1000/amr2-{dsh,openclaw,opencode,hermes,zcode}.sock`，0600。

### 3.2 网络与 DNS

- `amr.djper.cn` → `192.168.30.166`，由 **AdGuard Home DNS 重写**实现（`192.168.30.2` 是 DNS 服务器）。
- **仅内网**：`amr2-mcp` 绑 `192.168.30.166`，无 WAN 端口映射。**不要加 8443 转发。**
- 风险：AdGuard 是唯一解析点，挂了会回落到公网 `101.200.162.104` 导致客户端连不上。建议在 OPNsense 再加一条同值 host override 兜底。

### 3.3 证书（AEP-PKI ACME / DNS-01）

| 项 | 值 |
| --- | --- |
| Profile | `aep-tls-server-intl-v1`（ECDSA P-256，180 天） |
| ACME | `https://ca.aep.org.cn/acme/directory`，acme.sh v3.1.7，账号 ES256 |
| 挑战 | `_acme-challenge.amr.djper.cn TXT`，**手动添加**（AEP 的 unbound 解析） |
| leaf | `CN=amr.djper.cn`，serial `1FDE84933AD477258C56CC49A1CB9F9E` |
| 有效期 | 2026-11-16 12:15:50Z → **2027-05-15** 12:15:50Z |
| 链 | leaf → `AEP-Issuing-ECDSA-CA` → `AEP-Root-ECDSA-CA` |
| 根指纹 | `6C:0F:FB:28:5E:8C:AD:C1:7E:ED:70:47:B0:3E:8E:0D:F0:4E:C2:7C:19:78:E8:9D:6D:A6:30:C8:DB:84:AE:6F` |

文件（`~/.config/amr/tls/`）：

| 文件 | 用途 |
| --- | --- |
| `server-chain.crt` | **服务端 `cert_file`**：叶子 + 中间，**不含根** |
| `server.key` | 服务端私钥，0600 |
| `root.crt` | 信任锚，adapter `ca_file` 与各 agent 信任库 |
| `intermediate.crt` | 备份用 |
| `backup-rest-*/` | 早期 REST 直签证书的留档 |
| `~/.config/amr/acme/` | acme.sh 的 home/config：账号、订单、fullchain |

**要点**：

- AEP 返回的 `fullchain_pem` 默认带根，**已拆开**；服务端链放根会导致严格客户端告警。
- 证书只含 **DNS SAN**，客户端必须用主机名，**不能用 `192.168.30.166` 直连**。
- **手动续期**：到期前 30 天需重新取 TXT 并 `acme.sh --renew`。自动续期需接 djper.cn 的 DNS API。
- REST 直签链路仍可用（`/api/v1/enroll/intl` + 一次性 token，AEP-PKI 里已建 `client_id=amr2-tls`），签发脚本逻辑见会话记录，未入库。

---

## 4. 本轮代码变更（`e95e37d..50f123b`，全部本地未 push）

```text
50f123b docs(amr2): record the 2.x production switch on 2026-11-16
f07da09 docs(amr2): record isolated full-stack rehearsal results and gaps
3035578 docs(amr2): record post-review commit map and pre-deploy risks
bbadd3c feat(dsh): transactional SQLite spool and versioned turn archival
a80b2b6 fix(clients): prefer explicit socket config over socket env vars
235f21d feat(amr2): per-message source revision watermarks in session ingest
85c067a feat(amr2): selectable TLS crypto profile for the remote service
e95e37d docs(amr2): add session handoff for Codex   ← 交接起点
```

| 提交 | 内容 |
| --- | --- |
| `85c067a` | `crypto_profile`：`system` / `ecdsa_aes128_gcm_sha256_tls12`；ECDSA 校验移到加载私钥之前；SM4 套件保留但 fail-closed |
| `235f21d` | `source_revision` 水位：新表 `message_source_revisions`，旧水位不覆盖新内容、同水位不同内容整批失败、新水位同内容推进水位 |
| `a80b2b6` | 四个插件 socket 优先级改为「显式配置 > 环境变量」，与 DOC-09 一致 |
| `bbadd3c` | DSH SQLite spool（事务、generation 防 ABA、离线导入）、版本化快照归档、持久化屏障有界重试、自动 replay 退避 |
| `3035578` `f07da09` `50f123b` | 复审记录、演练记录、切换记录 |

### 4.1 本轮修复的真实缺陷

1. **DSH 持久化屏障可永久阻塞队列**（高）：`ctx.sessions.flush()` 永不返回 true 时队首任务永不出队，后续归档全被堵死直到溢出。修复为限 3 次重试 + 会话已 dispose 时直接直发 + 告警。
2. **`SQLiteSpool.remove` 遇 `SQLITE_BUSY` 抛原始错误**：改为记录并返回 false，ack 路径发现记录仍在则重排 replay。
3. **ECDSA 校验顺序**：原在 `load_cert_chain` 之后（已加载私钥），移到之前。
4. **水位测试断言过弱**：补水位值断言、顺序推进、先更新后冲突的整批回滚用例。

### 4.2 Gemini 复审（Round 9）

17 条发现：1 high 已修、3 条测试/低危修复、13 条判为误报或既定设计并附证据。典型误报：

- 「`node:sqlite` 错误无 `errcode`」—— **Node v24.16.0 与 v24.22.1 实测都有 `errcode=5`**。
- 「fork 时 `session.seq !== lastSeq+1`」—— 读 `dsh-session` 源码确认 `seq === log.length`、`snapshotEvents` 切 `[0, seq)`，恒等。

逐条判定在交叉审计记录 Round 9，**没有采纳的建议也已写明理由，不要重复推翻**。

---

## 5. 身份鉴权（已实测）

### 5.1 四道闸门

1. **传输层**：HTTPS，强制校验证书链 + 主机名，无关闭开关。**没有 mTLS**。
2. **远程 Bearer token → 身份**（[remote_auth.py](../src/interfaces/mcp/remote_auth.py)）：
   `~/.config/amr/remote-credentials.json` 中 `{token: {agent_id, tools}}`；SHA-256 预哈希后常量时间比较；未知 token → 401；`tools/list` 与 `tools/call` 均受 `tools` 白名单约束。
3. **本地 UDS → 固定身份**（[adapter/server.py](../src/interfaces/adapter/server.py)）：
   每个 binding 是「socket → 一个 agent_id」，适配器用该身份 token 调远程；客户端传的 `agent_id`/`source_agent` 被剥离。
4. **daemon `allowed_agents`**（[ipc/auth.py](../src/interfaces/ipc/auth.py)）：
   不在 `config.yaml: server.allowed_agents` → `-32001 AGENT_UNAUTHORIZED`。

要能使用，必须同时存在于 credentials、adapter bindings、daemon 名单三处。

### 5.2 openclaw / hermes 实测结果

| 测试 | 结果 |
| --- | --- |
| adapter `amr.ping`（两个 socket） | ok |
| adapter / HTTPS ingest 时伪造对方 `agent_id` | 落库仍是各自身份 |
| HTTPS `tools/list` | 各 3 个工具 |
| HTTPS `memory_update`（白名单外） | `Error: tool call not permitted` |
| 伪造 token | HTTP 401 |
| 合法 token + agent 不在 `allowed_agents`（临时加 `rogue`） | `-32001`，retryable=false |

测试数据已清理，rogue token 已移除。

### 5.3 身份清单

| agent | socket | token 文件（0600） |
| --- | --- | --- |
| openclaw | `amr2-openclaw.sock` | `~/.config/amr/tokens/openclaw.env` |
| hermes | `amr2-hermes.sock` | `~/.config/amr/tokens/hermes.env` |
| opencode | `amr2-opencode.sock` | `tokens/opencode.env` |
| zcode | `amr2-zcode.sock` | `tokens/zcode.env` |
| dsh | `amr2-dsh.sock` | `tokens/dsh.env` |
| claude_code / codex | 无本地 socket，走远程 HTTPS | `tokens/claude_code.env`、`tokens/codex.env` |

工具白名单统一为 `memory_search` / `memory_create` / `memory_ingest_session`。

### 5.4 安全边界（务必理解）

- **同 uid 进程不是隔离边界**：`/run/user/1000` 0700、socket 0600，但同一用户任何进程都能连任意 `amr2-*.sock`。socket 身份是**归属与审计边界**，不是进程间防护。要真隔离需独立 OS 用户。
- **token 泄露 = 身份完全冒用**；token 无状态，轮换需重启两个服务。
- **`memory.search` 不传 `project_id` 时全库检索**（按契约）。工具白名单控制"能否调用"，不控制"能读哪些项目"——**项目级隔离不存在**。

---

## 6. 复现命令

```bash
cd /home/dj/WorkSpaces/ai-memory-runtime

# 服务状态
systemctl --user is-active amr.service amr2-mcp.service amr2-adapter.service
systemctl --user is-enabled amr2-mcp.service amr2-adapter.service

# 配置与证书校验
PYTHONPATH=. ~/.local/share/amr/remote-venv/bin/python -m src.interfaces.mcp.remote_main \
  --check --config ~/.config/amr/amr2-remote.json
PYTHONPATH=. .venv/bin/python -m src.interfaces.adapter.main \
  --check --config ~/.config/amr/amr2-adapter.json

# 端到端（内网 DNS 已生效）
curl -sS --cacert ~/.config/amr/tls/root.crt -o /dev/null -w '%{http_code}\n' \
  https://amr.djper.cn:8443/healthz          # 期望 200
curl -sS --cacert ~/.config/amr/tls/root.crt -o /dev/null -w '%{http_code}\n' \
  -X POST https://amr.djper.cn:8443/mcp -H 'content-type: application/json' -d '{}'   # 期望 401

# 测试套件
.venv/bin/python -m pytest -q                                        # 860 passed / 15 skipped
AMR_REQUIRE_REMOTE=1 /tmp/amr-remote-venv/bin/python -m pytest \
  tests/test_remote_*.py tests/test_http_auth.py tests/test_adapter_*.py tests/test_bridge2.py -q   # 508 passed
cd plugins/dsh-amr && node --test tests/*.test.js                    # 306
```

> `/tmp/amr-remote-venv` 是测试用临时 venv，重启后需重建；线上服务用的是
> `~/.local/share/amr/remote-venv`。

---

## 7. 未完成项

### P0：客户端接入（真正剩下的工作）

| agent | 需要做的 | 风险 |
| --- | --- | --- |
| **DSH web** | 改 `~/.dsh/profiles/web/cordis.patch.yml`：`mcp-amr` 的 `bridge.py` → `bridge2 --agent dsh`；给 `amr` 插件配 `socketPath=/run/user/1000/amr2-dsh.sock`、`spoolBackend=sqlite`、`persistenceDir=~/.local/share/amr-spool/dsh-web` | **重启 DSH 会断开当前会话** |
| openclaw | 插件 `socketPath` → `amr2-openclaw.sock` | 当前未运行 |
| opencode | 插件 `socketPath` → `amr2-opencode.sock` | 当前未运行 |
| hermes | 插件 `socket_path` → `amr2-hermes.sock` | 当前未运行 |
| zcode | MCP server 改 `bridge2 --agent zcode` | — |
| claude_code / codex | `https://amr.djper.cn:8443/mcp` + `NODE_EXTRA_CA_CERTS` / `SSL_CERT_FILE` | — |

**DSH 重启是必踩的坑**：web profile 用 `file:` 软链本仓库，重启即加载新插件代码。
新插件默认写 `~/.local/share/amr-spool/dsh`（**多 profile 共享**），且版本化快照要求
`ctx.sessions` 与 `session/flush`。**重启前请确认 adapter 在跑**，否则快照会进 spool 排队。

### P1：代码层待办

1. **适配器错误码误导**：远程「工具不在白名单」被归为 `-32601 METHOD_NOT_FOUND`，与"方法不存在"无法区分。拒绝行为正确，但排错困难。修法：给权限拒绝一个稳定码（如 `-32003 PERMISSION_DENIED`），同步改 `remote_mcp` 信封、adapter 映射、测试。
2. **项目级隔离缺失**：能调 `memory_search` 的 agent 可读全库。需要产品决策。
3. **召回阈值未校准**：插件默认 `scoreThreshold: 0.65`；演练中中文问句对英文记忆实测 **0.600**，被漏召。应按真实语料定档。
4. **工具型会话会把推理过程归档**：演练中一次带文件工具的会话归档了 16 条 assistant 消息（含推理/检索步骤），不是"只归档最终回答"。需确认是否符合预期，或按 `source.kind`/`tool` 过滤。

### P2：运维与发布

5. **ACME 手动续期**：2027-05-15 到期。建议接 djper.cn DNS API 改自动。
6. **DNS 单点**：AdGuard 挂了内网解析会回落公网，建议加 OPNsense host override 兜底。
7. **Qdrant 投影不受配置隔离**：`memory_create` 的 `collection_name` 默认 `ai_memory`，改 `qdrant.collections` 不会隔离写入。演练时已证实并清理。若要蓝绿/演练隔离，需独立 Qdrant 实例。
8. **未 push / 未开 PR**：`release/2.x` 只在本地；`main` 仍是 1.x。
9. **`/tmp/amr-test-contamination-openclaw-20261116/`** 复核后是否删除（前序交接待办）。
10. **Docker/Compose**：仓库仍只有 systemd 模板，无 Unraid 容器方案。

---

## 8. 回滚

切换前备份：`~/backups/amr-pre2x-20261116-201705/`
（`sessions.db` 经 SQLite backup API 复制、`config.yaml`、`~/.config/amr` 副本、`SHA256SUMS`）

```bash
systemctl --user stop amr2-adapter amr2-mcp
# 恢复 sessions.db / config.yaml（先另存当前副本）
systemctl --user restart amr.service
```

- 2.x 只新增 `message_source_revisions` 表，1.x 忽略它，**回滚不需要改 schema**。
- 如需退回 1.x 代码线：`git checkout release/1.x` 后重启 `amr.service`（本轮未做）。
- 重新启用 PDF 解析与蒸馏：`systemctl --user enable --now mineru.service amr-distillation.timer`（后者的失败原因未排查）。

---

## 9. 陷阱清单（踩过的坑）

1. **同一 `XDG_RUNTIME_DIR` 不能起第二个 daemon**：`amr-outbox.lock` 会让新实例直接退出（退出码 3）。演练时必须改 `XDG_RUNTIME_DIR`。
2. **生产 `.venv` 没有 `mcp`**：`amr2-mcp` 必须用 `~/.local/share/amr/remote-venv`，否则导入失败。
3. **`node:sqlite` 的 `errcode` 可用**，不要基于"没有 errcode"去改重试判定。
4. **服务端链不要放根证书**；AEP 的 `fullchain_pem` 默认带根，必须拆。
5. **证书只有 DNS SAN**，用 IP 直连必然主机名校验失败。
6. **DSH 的 `patchReload: live`**：改 web profile 可能被热加载，不一定要整进程重启；但插件 bundle 变更仍需重启。
7. **`amr2-mcp` / `amr2-adapter` 无热加载**：改凭证或 binding 必须 restart。
8. **测试写生产库**：本轮所有自测会话均已按 `session_id` 前缀清理；后续测试请沿用此做法并记录清理语句。

---

## 10. 交接检查清单

```bash
cd /home/dj/WorkSpaces/ai-memory-runtime
git status --short                 # 应为空
git log --oneline -1               # 50f123b
systemctl --user is-active amr.service amr2-mcp.service amr2-adapter.service   # 三个 active
ls /run/user/1000/amr2-*.sock      # 5 个
getent hosts amr.djper.cn          # 192.168.30.166
openssl x509 -in ~/.config/amr/tls/server-leaf.crt -noout -enddate   # 2027-05-15
```

---

## 11. 需要拍板的问题

1. **DSH 何时重启切换**？（会断当前会话）
2. 项目级记忆隔离要不要做？做成什么粒度（project_id 白名单 / 按 agent 限制）？
3. 召回阈值定多少？归档是否保留推理与工具步骤？
4. ACME 是否改自动续期（需要 djper.cn 的 DNS API）。
5. 2.x 是否 push / 开 PR，`main` 何时合并。
6. 适配器权限拒绝错误码是否按 P1-1 修改。

---

**交接人备注**：本轮所有"已通过"结论都有对应命令与记录；未在真实 agent 会话中验证过的
内容（各插件端到端、DSH 新插件）已在上文单列。请不要把"服务已上线"当成"agent 已接入"。
