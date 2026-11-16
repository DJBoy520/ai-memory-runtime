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
- `source_revision` 是可选的、每个会话内可比较的非负整数快照水位，不是消息
  排序位置或时间戳。新服务在同一数据库事务中保存每条消息的水位：较旧水位
  不覆盖较新内容；相同水位、不同内容是冲突；较新水位的相同内容也推进水位。
  受保护消息若收到无水位的不同内容会拒绝，避免旧客户端绕过保护。已有无水位
  消息只能以相同内容建立初始水位；内容不一致会使整个入库事务失败，客户端
  需保留该批记录供人工核对，不能据此声称无缝迁移。
- DSH 有完整、连续 session 事件快照时，使用投影后的消息和快照末尾事件序号；
  该序号只适合同一 session 的连续历史，不能用于比较不同 session、fork，或对
  旧备份重建出的分叉历史。旧版 daemon 不执行此保护，回滚期间不得假定它有效；
  服务记录被旧版直接更改后，新版会拒绝不一致的基线。删除/投影移除的传播不在
  本机制内。旧队列没有可靠水位时不得按当前时间补造。
- DSH 默认依赖 `session/event`。没有该事件的旧宿主可显式设置
  `legacyTurnEnd: true` 启用无水位回调；它无法提供乱序保护，不应与版本化会话
  回调混用。旧宿主的 `session/event` 若缺少可验证的投影及水位，默认跳过归档并
  告警；可显式设置 `legacyUnversionedSnapshots: true` 兼容，但同样没有乱序保护。
  升级前应确认宿主支持这些接口。
- DSH 的 `spoolBackend`、`persistenceDir`、`spoolMaxBytes`、`spoolMaxRecords`、
  `replayInitialDelayMs`、`replayMaxDelayMs` 可在 Cordis 插件配置中指定；默认
  重放退避从 1 秒增长到 60 秒。默认 `jsonl` 后端的多实例必须使用独立目录。
  可显式选择 `sqlite` 后端，以事务保护本地记录；当前多个投递进程共享同一队列
  仍可能让旧版本请求晚到并覆盖新版本，**SQLite 也必须每实例使用独立目录**。
  SQLite 要求 Node 的 `node:sqlite` 可用、目录属主私有。不要让旧版进程访问 SQLite
  后端目录；本地 SQLite 锁不应在未经验证的网络文件系统上使用。入队失败只记录
  错误，不发送该次归档；需恢复存储并重新触发归档。SQLite 短暂锁竞争时，插件
  最多在进程内暂存 100 条、2 MiB 后重试；这些记录在成功入库前不具备崩溃持久性，
  队列满时显式记录失败。
  若 JSONL 包含损坏记录，插件保留原文件并停止对该 spool 的发送和修改；
  应先备份原文件，人工修复后再恢复运行。
- 版本化快照在发送前等待 `ctx.sessions.flush(session)` 返回成功的持久化屏障；
  等待中的 turn 只保存在内存，进程崩溃会丢失。屏障最多重试 3 次：宿主没有
  durability listener、会话已 dispose，或重试仍失败时，插件记录告警后直接归档
  （至少一次投递，服务端仍按 `(session_id, message_id)` 幂等），避免队首永久
  阻塞后续归档。

DSH Cordis 插件配置示例（路径需替换为对应实例的路径）：

```yaml
config:
  spoolBackend: sqlite
  socketPath: /run/user/1000/amr2-dsh.sock
  persistenceDir: /home/example/.local/share/amr-spool/dsh-instance-1
  spoolMaxBytes: 8388608
  spoolMaxRecords: 1000
  replayInitialDelayMs: 1000
  replayMaxDelayMs: 60000
```

SQLite 的 `spoolMaxBytes` 限制入队记录的逻辑序列化字节，并非数据库及日志文件的
磁盘占用上限；部署时仍需监控磁盘空间。SQLite 初始化失败时记录错误，插件会
在重放或新归档时重新尝试打开，不会回退到 JSONL。入队失败的那次归档未持久化，
仍需在修复后重新触发。JSONL → SQLite 必须先停止所有旧版和
新版写者并备份，再运行：

```sh
node plugins/dsh-amr/migrate-spool.js --spool-dir /absolute/path/to/spool --writers-stopped
```

命令保留原 JSONL，事务导入并绑定文件摘要；文件后来发生变化会拒绝启动。切换前
应核对待处理和永久失败记录；禁止混用旧版 JSONL 写者。迁移命令的参数是操作者对
停写状态的声明，不会自动检测或停止其他进程。

隔离会话验证：本机 DSH 0.2.2-rc.1 使用临时 profile、本地假 OpenAI 兼容模型和
假 UDS 适配器完成一轮；观察到 `amr.ping`、`memory.search`、召回内容进入模型
请求，以及仅含真实用户输入和助手回答的 `session.ingest`，假适配器确认后 SQLite
spool 为空。此验证不涉及真实模型、真实适配器、远程服务或生产数据。

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
