# DOC-AMR-09: 2.x 客户端接入契约

日期：2026-11-15
状态：2.x 开发中；1.x 见 `DOC-AMR-06`。

## 1. 两种接入方式

| 方式 | 适用 | 传输 | 身份 |
| --- | --- | --- | --- |
| **A. 直连 HTTPS MCP** | 支持远程 MCP 的标准客户端（Claude Code、Codex、ZCode 等，需验证） | `https://<host>:<port>/mcp` | 客户端自带 `Authorization: Bearer <token>` |
| **B. 本地适配器** | 需要 hook 低延迟或不支持远程 MCP 的插件（OpenClaw、Hermes、OpenCode、DSH、Doubao） | 本地 UDS（4 字节长度前缀 + JSON-RPC 2.0） | 适配器按 socket 固定身份，插件不持有令牌 |

两种方式共享服务端授权：令牌绑定 `agent_id` 与工具允许列表，服务端覆盖
`agent_id`/`source_agent`。管理接口仍只走本机 admin UDS。

## 2. 适配器 socket 约定

- 每个固定身份一个 socket：`$XDG_RUNTIME_DIR/amr2-<agent>.sock`，权限 `0600`。
- 适配器配置中的 `bindings` 决定 socket 与身份；同一 socket 只对应一个身份。
- 插件 socket 解析优先级：
  插件显式 `socketPath`（Hermes 也支持 `socket_path`）> `AMR2_SOCKET_PATH` > 旧 `AMR_SOCKET_PATH`
  迁移回退 > `$XDG_RUNTIME_DIR/amr2-<agent>.sock` > 平台默认。
- 插件只配置 `socketPath`，不得再传 `source_agent`/`agent_id`；传了也会被适配器
  剥离。
- 适配器提供控制方法 `amr.ping`：成功返回 `{"status":"ok"}`，不触达后端、
  不泄露身份或依赖信息。插件启动时应异步调用一次；失败时按 fail-open 降级，
  不得崩溃宿主。
- 适配器限制请求/响应帧大小（默认 4MB / 8MB，均可配置）；响应超限时返回明确
  协议错误或关闭连接，不会让调用方无限等待。

## 3. 方法与错误契约

daemon 方法名、参数、成功响应与 1.x 一致（见 `DOC-AMR-06`）。错误码：

| code | 分类 | 含义 | 客户端行为 |
| --- | --- | --- | --- |
| -32001 | `AGENT_UNAUTHORIZED` | 服务端拒绝适配器身份 | 永久错误，记录一次并降级 |
| -32002 | `VERSION_CONFLICT` | 乐观锁冲突 | 不自动重试；由模型/用户合并 |
| -32003 | `MODEL_LOADING` | 模型加载中 | 可短退避重试 |
| -32004 | `QUEUE_FULL` | 服务端队列满 | 可退避重试 |
| -32005 | `NOT_FOUND` | 资源不存在 | 不重试 |
| -32006 | `IDEMPOTENCY_CONFLICT` | 幂等键冲突 | 不重试，需人工裁决 |
| -32010 | `ADAPTER_UNAVAILABLE` | 网络/TLS/超时可重试 | 有限退避重试；写操作先落 spool |
| -32011 | `ADAPTER_PROTOCOL_ERROR` | 远程响应不符合 MCP/JSON-RPC | 记录并降级，不无限重试 |
| -32012 | `ADAPTER_REMOTE_AUTH_FAILED` | 服务端拒绝适配器令牌 | 永久错误，告警一次 |
| -32601 | `METHOD_NOT_FOUND` | 未知/未授权方法 | 记录并降级 |
| -32602 | `INVALID_ARGUMENT` | 参数错误 | 不重试 |
| -32603 | `INTERNAL` | 兜底 | 记录并降级 |

- 认证失败/证书失败必须与暂时性断网区分；不得无限重试，也不得把失败报告成
  “已排队”。
- 适配器不重试写操作；超时后返回明确错误，由客户端用幂等键安全重试。

## 4. 会话归档与离线队列

- 优先使用带 `id` 的 `request("session.ingest", ...)` 获取服务端确认；只有确认
  后才把本地 spool 记录标记为已提交。
- 超时/网络错误：记录保持 pending，按 `(session_id, message_id)` 与
  `idempotency_key` 重试；禁止重复追加。
- `notify()`（无 id）仅为兼容 1.x；2.x 插件不得用它做会话归档，因为无法区分
  “已提交”与“丢失”。
- spool 必须有容量/磁盘余量上限；空间不足显式失败，不静默丢弃。
- 适配器本身无队列、无重试。

## 5. 各 agent 2.x 配置

### 5.1 直连 HTTPS MCP（示例，具体字段以客户端版本为准）

```json
{
  "url": "https://amr.internal:8443/mcp",
  "headers": { "Authorization": "Bearer <per-agent-token>" },
  "ca": "/etc/amr/tls/ca.crt"
}
```

- 必须校验证书链与主机名；不得提供跳过校验的选项。
- 令牌不写入 URL、日志或仓库；每个 agent 独立。

### 5.2 适配器插件

```json
{
  "socketPath": "/run/user/1000/amr2-openclaw.sock",
  "deadlineMs": 80,
  "ingestDeadlineMs": 1500,
  "persistenceDir": "/home/<user>/.local/share/amr-spool/openclaw"
}
```

- 不再配置 `sourceAgent`；身份由适配器绑定。
- 插件版本从 `2.0.0` 起，包名不变。
- 升级时先启动 2.x 适配器与新 socket，再切换插件配置；保留 1.x socket 作为回退。

### 5.3 仅支持 stdio MCP 的客户端（ZCode、DSH MCP 等）

使用 2.x stdio shim `src/interfaces/mcp/bridge2.py`：

```json
{
  "command": "<repo>/.venv/bin/python",
  "args": ["-m", "src.interfaces.mcp.bridge2", "--agent", "zcode"],
  "env": { "PYTHONPATH": "<repo>" }
}
```

- shim 默认连接 `$XDG_RUNTIME_DIR/amr2-<agent>.sock`，可用
  `AMR2_SOCKET_PATH` 或 `--socket` 覆盖。
- 不读取生产 `config.yaml`，不注入 `agent_id`/`source_agent`，不暴露
  `memory_spool_*`；身份由适配器绑定。
- 超时默认 120s（`AMR2_BRIDGE_TIMEOUT_SECONDS` 可调）。

## 6. 各 agent 迁移矩阵

| Agent | 2.x 方式 | 状态 |
| --- | --- | --- |
| Claude Code | A（HTTP MCP + header）；私有 CA 用 `NODE_EXTRA_CA_CERTS` | 待配置验证 |
| Codex | A（`--url` + bearer token env）；来源改为 `codex` | 待配置验证 |
| ZCode | B + `bridge2` stdio shim | shim 已实现 |
| OpenCode | 原生插件 2.x（adapter UDS） | 已完成 15 tests |
| OpenClaw | 原生插件 2.x（adapter UDS + 现有 spool） | 已完成 162 tests |
| Hermes | 原生插件 2.x（adapter UDS + JSONL spool） | 已完成 23 tests |
| DSH | 原生插件 2.x + `bridge2`（工具调用） | 插件 262 tests；shim 已实现 |
| Doubao | B（SSH 转发 socket）或 A | 待迁移 |
| Windows/其他 | A 或 SSH → B | 待验证 |

## 7. 测试要求

- 每个插件至少：`amr.ping` 健康检查、`memory.search` 成功、`session.ingest`
  确认后 spool 标记、`-32010` 重试/降级、`-32012` 不无限重试、无 `source_agent`
  伪造。
- 使用假适配器 UDS 或真实 2.x 适配器 + 临时 TLS；禁止连接生产。
- 测试不得依赖固定端口/固定 socket 路径。
