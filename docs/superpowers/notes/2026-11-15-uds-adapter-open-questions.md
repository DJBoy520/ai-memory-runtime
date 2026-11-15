# UDS 适配器：实现前必须定的问题（2026-11-15）

状态：设计备忘，未开始实现。M1（HTTPS MCP 服务）已完成，见
[可启动的认证 HTTPS MCP 服务](2026-11-15-m1-tls-service.md)。

## 目标拓扑

- AMR 与 HTTPS MCP 端点将来运行在 Unraid 容器；Ubuntu 上的现有插件
  （OpenClaw/Hermes/OpenCode/DSH）继续通过本地 Unix socket 调用。
- 适配器是 Ubuntu 侧的本地 UDS 服务：固定身份、方法白名单、只暴露业务工具，
  通过认证 HTTPS（客户端严格校验证书与主机名）访问 AMR。
- 适配器不得把 admin/本机专用能力（`memory_spool_*`）透出，不接受客户端
  伪造 `agent_id`/`source_agent`。

## 已观察到的插件方法

本仓库插件实际调用（grep 统计）：

| daemon 方法 | 对应 MCP 工具 | 备注 |
| --- | --- | --- |
| `memory.search` | `memory_search` | 最常用 |
| `memory.create` | `memory_create` | |
| `memory.record` | `memory_record` | 兼容入口 |
| `session.ingest` | `memory_ingest_session` | 会话归档 |
| `session.checkpoint` | `session_checkpoint` / `memory_session_checkpoint` | 两个工具名同一 RPC，适配器应只选一个规范名 |
| `session.get_handoff` | `session_get_handoff` / `memory_get_handoff` | 同上 |
| `memory.update` / `memory.history` / `memory.delete` / `memory.get` / `memory.update_status` | 同名工具 | 由 `TOOL_RPC_METHODS` 反查 |
| `memory.candidates` / `memory.approve_candidate` | 同名工具 | 管理性写操作，需确认是否开放给本地插件 |

`memory.session_checkpoint` 与 `memory.get_handoff` 是别名，不应产生两套映射。

## 关键未决问题：错误语义

现有插件依赖 daemon JSON-RPC 错误码（如 `AGENT_UNAUTHORIZED`、`VERSION_CONFLICT`、
`INVALID_ARGUMENT`、`NOT_FOUND`）。MCP 的 `tools/call` 只保留
`isError` + 文本，且当前服务端出于防泄露把异常折叠为固定文本：

- `Error: tool call not permitted`
- `Error: invalid tool arguments`
- `Error: tool call failed`

如果适配器直接走 MCP 协议，**无法无损还原**错误码，违反设计文档“保留 RPC
参数、响应与错误语义”。可选方案：

| 方案 | 做法 | 优点 | 代价 |
| --- | --- | --- | --- |
| A. 同进程 Gateway | 适配器与 AMR 同机，直接调 `Gateway.handle_identity` | 错误码零损失、无网络 | 只适用于同机；不满足 Ubuntu→Unraid 拆分 |
| B. MCP 结构化错误 | 在工具结果的 `structuredContent`/文本中附加 `{code,message,data}`，适配器解包 | 单一协议、可远程 | 标准 MCP 客户端也会看到信封；需定义脱敏范围 |
| C. 内部 legacy JSON-RPC over HTTPS | 专用认证端点保留 daemon 错误对象，只给适配器用 | 语义完全保真 | 多一套端点与鉴权面，需独立审计 |
| D. 有损映射 | 只把三种固定文本映射为通用错误码 | 实现最简单 | 丢失 `VERSION_CONFLICT` 等可重试/可裁决信息 |

建议：先做 A 用于“客户端仍与 AMR 同机”的过渡期；跨机时优先 C（内部端点严格
限制来源与工具集），B 作为次选。**在选定前不要实现适配器**，否则会把错误语义
问题固化进插件。

## 队列与可靠性

- 不新增第二套重放队列：现有客户端 spool 继续负责会话/检查点排队；适配器保持
  无状态转发。
- `session.ingest`/`session.checkpoint` 必须沿用服务端
  `(session_id, message_id)` 去重与幂等键；超时后的重试不得重复写入。
- 必须区分“已排队”与“服务端已确认”；响应丢失（已提交但客户端未收到）时返回
  明确状态，不冒充成功。
- 认证失败/证书失败与暂时性断网必须区分：前者不无限重试，后者可重试并暴露
  不泄露正文的状态。

## 固定身份与授权

- 每个固定身份一个独立 UDS 路径 + 独立凭证；socket 权限 `0600`，目录仅用户可访问。
- 适配器持有对应令牌，调用 HTTPS 时注入 `Authorization`；客户端传入的
  `agent_id`/`source_agent` 一律覆盖或拒绝。
- 方法白名单由固定映射表决定；未知方法与 `memory_spool_*` 一律拒绝。
- 日志只记录方法名、身份、结果分类与耗时，不记录参数正文、令牌或证书内容。

## 建议的测试契约（实现时先写）

1. 临时 UDS + 临时服务端：`memory.search` 成功；响应形状与 daemon 一致。
2. 固定身份：客户端伪造 `agent_id` 不生效；不同 socket 身份隔离。
3. 未知方法/`memory_spool_status`/admin 方法被拒绝，且不触达后端。
4. 断网 → 明确错误；恢复后可重试；`session.ingest` 重试不重复写入。
5. 服务端已提交但响应丢失 → 状态明确，不报“已排队”。
6. 证据：日志不含令牌/正文；测试只用 tmp_path，禁止连接生产。

## 需要拍板的问题

1. 适配器与 AMR 是否始终同机？（决定 A 是否可用）
2. 跨机时选 B 还是 C？（错误语义 vs 端点数量）
3. `memory.candidates`/`memory_approve_candidate` 是否开放给本地插件？
4. 适配器是否复用现有客户端 spool，还是仅做无状态转发？（建议后者）
