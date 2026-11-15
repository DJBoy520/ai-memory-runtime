# DOC-AMR-10: 2.x 部署与迁移（草案）

日期：2026-11-16
状态：**未部署**。本文是 2.x 的部署/迁移草案；1.x 生产仍运行在
`main` / `release/1.x`。任何切换都需先完成本文的演练与回滚验证。

## 1. 拓扑

```text
 Ubuntu VM                                      Unraid / 容器
┌────────────────────────────────┐             ┌──────────────────────────────┐
│ Claude Code / Codex / OpenCode │  HTTPS      │ AMR daemon (1.x data)         │
│   (直连 /mcp)                  │────────────►│   business UDS (容器内)       │
│                                │  TLS+Token  │   SQLite SSOT + Qdrant        │
│ OpenClaw/Hermes/OpenCode/DSH   │             │                              │
│   │ 本地 UDS 0600              │             │ amr2-mcp (HTTPS /mcp)         │
│   ▼                            │  HTTPS      │   /healthz (无认证存活探针)    │
│ amr2-adapter  ─────────────────┼────────────►│                              │
└────────────────────────────────┘             └──────────────────────────────┘
```

- **amr2-mcp**：认证 HTTPS MCP 服务，读取 AMR business UDS；管理 UDS 不暴露。
- **amr2-adapter**：Ubuntu 侧本地 UDS 适配器，固定身份并持有令牌；不新增队列。
- **客户端**：支持远程 MCP 的直连；只支持 stdio 的用 `bridge2`；hook 插件用适配器。

## 2. 前置条件

1. 2.x 代码在 `release/2.x`，`VERSION=2.0.0-dev.1`（正式发布前会更新）。
2. 远程依赖：`requirements-remote.txt`（`mcp==1.30.0`、`uvicorn`、`httpx`）。
3. 服务端证书（SAN 包含客户端实际使用的 DNS 名）与私钥；私有 CA 需分发给客户端。
4. 每个 agent 一个独立高熵令牌（`secrets.token_urlsafe(32)`，43 字符），
   凭证文件权限 `0600/0400`。
5. AMR business socket 的属主与运行 `amr2-mcp` 的用户一致（或有访问权限）。

示例文件：

- [amr2-remote.example.json](../config/examples/amr2-remote.example.json)
- [amr2-adapter.example.json](../config/examples/amr2-adapter.example.json)
- [amr2-credentials.example.json](../config/examples/amr2-credentials.example.json)

复制后：

```bash
install -d -m 0700 /etc/amr /etc/amr/tls
install -m 0600 amr2-credentials.json /etc/amr/remote-credentials.json
install -m 0644 server.crt /etc/amr/tls/server.crt
install -m 0600 server.key /etc/amr/tls/server.key
```

先做离线校验（不监听、不连接）：

```bash
.venv/bin/python -m src.interfaces.mcp.remote_main \
  --check --config /etc/amr/amr2-remote.json
.venv/bin/python -m src.interfaces.adapter.main \
  --check --config ~/.config/amr/amr2-adapter.json
```

## 3. systemd

模板见 [deploy/systemd](../deploy/systemd)。

### 3.1 Ubuntu 适配器（user unit）

```bash
mkdir -p ~/.config/systemd/user ~/.config/amr
cp deploy/systemd/amr2-adapter.service ~/.config/systemd/user/
# 按实际路径修改 ExecStart/Environment
systemctl --user daemon-reload
systemctl --user enable --now amr2-adapter
journalctl --user -u amr2-adapter -f
```

- socket 位于 `$XDG_RUNTIME_DIR/amr2-<agent>.sock`，权限 `0600`。
- 适配器只清理本实例创建的 socket；重复启动会拒绝。
- 父目录必须属于当前用户且不可 group/other 写。

### 3.2 远程 MCP（system unit / 容器）

```bash
sudo install -d -o amr -g amr -m 0750 /opt/amr /etc/amr
sudo cp deploy/systemd/amr2-mcp.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now amr2-mcp
```

- `ExecStartPre --check` 会在启动前校验证书/私钥匹配与有效期。
- `bind_host=0.0.0.0` 只应在容器私网/覆盖网络中配合防火墙使用；单机可保持
  `127.0.0.1` 并由反向代理终止 TLS（注意代理信任边界与来源限制）。
- 管理 UDS 不进入容器网络。

## 4. 健康检查

```bash
# 远程 MCP 存活探针（无认证、无依赖信息）
curl --fail --silent \
  --cacert /etc/amr/tls/ca.crt \
  --resolve amr.internal:8443:127.0.0.1 \
  https://amr.internal:8443/healthz

# 适配器 ping（本地）
.venv/bin/python - <<'PY'
import asyncio, json
from src.interfaces.ipc.protocol import read_frame, write_frame
async def main():
    r, w = await asyncio.open_unix_connection("/run/user/1000/amr2-openclaw.sock")
    await write_frame(w, {"jsonrpc":"2.0","id":1,"method":"amr.ping","params":{}})
    print(await read_frame(r))
    w.close(); await w.wait_closed()
asyncio.run(main())
PY
```

## 5. 客户端接入

### 5.1 Claude Code（HTTP MCP + header）

```bash
export AMR_TOKEN='...'
export NODE_EXTRA_CA_CERTS=/path/to/ca.crt
claude mcp add --transport http amr https://amr.internal:8443/mcp \
  --header "Authorization: Bearer $AMR_TOKEN"
```

### 5.2 Codex（streamable HTTP + bearer env）

```bash
export AMR_TOKEN='...'
export SSL_CERT_FILE=/path/to/ca.crt
codex mcp add amr --url https://amr.internal:8443/mcp \
  --bearer-token-env-var AMR_TOKEN
```

Codex 的 1.x 配置曾把来源误设为 `claude_code`；2.x 不再发送来源字段，
身份由令牌绑定，迁移时删除旧的 `AMR_SOURCE_AGENT` 覆盖。

### 5.3 OpenCode

```bash
opencode mcp add amr --url https://amr.internal:8443/mcp \
  --header "Authorization=Bearer $AMR_TOKEN"
```

同时把 `opencode-amr` 插件升级到 2.0.0，`socketPath` 指向
`/run/user/1000/amr2-opencode.sock`。

### 5.4 ZCode（仅 stdio）

`~/.zcode/cli/config.json` 的 `mcp.servers.amr`：

```json
{
  "command": "/home/dj/WorkSpaces/ai-memory-runtime/.venv/bin/python",
  "args": ["-m", "src.interfaces.mcp.bridge2", "--agent", "zcode"],
  "env": {
    "PYTHONPATH": "/home/dj/WorkSpaces/ai-memory-runtime"
  }
}
```

### 5.5 DSH（MCP 客户端）

把 Cordis 中 `mcp-amr` 的启动命令从 `bridge.py` 改为
`-m src.interfaces.mcp.bridge2 --agent dsh`；`dsh-amr` 插件升级 2.0.0。

### 5.6 Doubao / Windows

- 优先直连 HTTPS MCP（把 CA 加入客户端信任库）。
- 只能 UDS 时，用 SSH streamlocal 转发到适配器 socket：

```bash
ssh -N -L /tmp/amr2-doubao.sock:/run/user/1000/amr2-doubao.sock user@host
```

- 不要把令牌写入 URL、命令行历史或日志；用环境变量或受限文件。

## 6. 迁移与回滚

1. **冻结写入**：停止 1.x 以及所有会写 AMR 的客户端/定时任务；确认无并发写者。
2. **备份**：SQLite（WAL 一致备份）+ Qdrant 快照 + 配置/凭证；记录校验和。
3. **准备 2.x**：部署 `amr2-mcp`、适配器与客户端配置；先跑 `--check` 和
   `/healthz`，再在隔离测试数据上验证工具调用与会话归档。
4. **切换**：用备份恢复/启动 2.x；验证 `memory_search`、`memory_get`、
   `session.ingest`、`memory.update` 与错误码；逐个 agent 切换。
5. **回滚**：停 2.x 写者 → 按演练过的备份恢复 → 启动 1.x → 验证客户端；
   明确切换后写入可能丢失，不声称无损回滚。
6. **禁止**：1.x 与 2.x 同时写同一数据目录；不要用 dry-run 代替备份。

## 7. 目标机验证清单（未执行）

- [ ] Unraid 上容器私网、TLS 证书 SAN、客户端 CA 信任与主机名校验。
- [ ] Tesla P4 / CUDA / PyTorch 组合、显存与设备实际使用。
- [ ] SQLite/WAL、Qdrant、模型、spool 的持久卷与权限。
- [ ] 定时蒸馏/对账任务迁移；日志与健康检查。
- [ ] 备份恢复、切换与回滚演练（含写入丢失说明）。
- [ ] 1.x 生产未受影响的证据（独立数据目录/端口/服务）。

## 8. 安全清单

- 管理 UDS 不暴露；MCP 仅暴露业务工具。
- 令牌每 agent 独立、`0600` 存储、不写日志/URL；支持轮换与吊销。
- TLS 最低 1.2，客户端强制证书链与主机名校验，无跳过开关。
- `amr2-adapter` socket `0600`，父目录属主/权限校验，拒绝重复启动。
- 日志不含正文/令牌/证书内容；`/healthz` 只返回固定状态。
- 生产 1.x 的 `config.yaml`、`amr.service` 在切换前不得被 2.x 修改。
