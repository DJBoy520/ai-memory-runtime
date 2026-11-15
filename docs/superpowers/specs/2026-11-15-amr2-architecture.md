# AMR 2.x 接入架构（草案，2026-11-15）

状态：服务端、结构化错误、UDS 适配器与 DSH 2.x 插件已实现并通过隔离测试；
其余插件与部署仍在开发。1.x 基线为 `main` / `release/1.x`（提交 `e4d8677`，
`v5.1.0-complete-consolidation`）；2.x 开发在 `release/2.x`。

## 1. 目标与非目标

**目标**

- AMR 可在 Unraid 容器中通过认证 HTTPS 暴露标准 MCP；Ubuntu 侧现有 agent
  继续低延迟接入。
- 每个 agent 使用独立凭证与固定身份；管理能力不通过网络暴露。
- 保留现有 `memory.*` / `session.*` 语义与错误码；迁移不要求改写调用参数。
- 1.x 与 2.x 数据格式兼容，但同一时刻只有一个写者。

**非目标**

- 不做多租户或项目级权限隔离（同一用户的多 agent 共享记忆）。
- 不在 2.x 首版做跨主机双写、自动故障切换。
- 不在本阶段替换生产 `amr.service`；迁移需单独演练。

## 2. 拓扑

```text
 Ubuntu VM                                   Unraid 容器
┌──────────────────────────────┐            ┌───────────────────────────────┐
│ Claude Code / Codex / ZCode  │  HTTPS     │  AMR daemon                   │
│   (支持远程 MCP 的客户端) ────┼───────────►│  /mcp  Streamable HTTP        │
│                              │  TLS+Token │  Bearer → 固定 agent 身份      │
│ OpenClaw / Hermes / OpenCode │            │  Gateway → 业务工具 → UDS     │
│ DSH / Doubao                 │            │  SQLite SSOT + Qdrant         │
│   │ 本地 UDS (0600)          │            └───────────────────────────────┘
│   ▼                          │
│ amr2-adapter                 │  HTTPS     │
│   方法映射 / 身份注入 ────────┼───────────►│
│   错误码还原 / 无独立队列     │  TLS+Token │
└──────────────────────────────┘            └───────────────────────────────┘
```

- **标准 MCP 客户端**：直接连 `https://<host>:<port>/mcp`，用各自令牌。
- **Hook 型插件**：连 Ubuntu 本地 `amr2-adapter`（UDS），由适配器统一持有令牌
  并访问远程 MCP；适配器不新增重放队列。
- 两种路径共享同一套服务端授权（身份绑定、工具白名单、参数校验）。

## 3. 服务端

- 端点：`/mcp`（标准 Streamable HTTP，stateless、JSON 响应）。
- 认证：`Authorization: Bearer <token>`；令牌绑定 `agent_id` 与工具允许列表；
  服务端覆盖 `agent_id`/`source_agent`。
- 传输：仅 TLS 1.2+；默认 `127.0.0.1:8443`。跨机部署必须显式设置
  `bind_host` 到容器私网/覆盖网络地址，并由防火墙与安全组限制来源；回环默认
  值本身不能支撑 Ubuntu→Unraid 直连。管理 socket 不通过网络暴露。
- 健康检查：`/healthz` 只返回 `{"status":"ok"}`，不包含版本、路径、依赖状态；
  是否要求认证由部署层决定，默认仅绑定回环时开放。
- 本地专用工具（`memory_spool_status`/`memory_spool_retry`）远程一律拒绝。

### 3.1 结构化错误（2.x 新增）

当前 MCP 把 daemon 错误折叠为固定文本，适配器无法还原 `VERSION_CONFLICT`
等语义。2.x 在 `CallToolResult` 中增加结构化错误信封：

```json
{
  "isError": true,
  "content": [{"type": "text", "text": "Error: version conflict"}],
  "structuredContent": {
    "amr_error": {
      "code": -32002,
      "message_class": "VERSION_CONFLICT",
      "retryable": false,
      "data": {"current_version": 3}
    }
  }
}
```

- `content.text` 只给标准客户端看，保持简短、不含内部路径/标识。
- `structuredContent.amr_error` 只包含白名单字段；原始异常文本、路径、SQL、
  令牌一律不进入。
- 适配器优先读取 `structuredContent`，缺失时回退为通用错误。
- 认证/传输错误仍由 HTTP 状态码与 JSON-RPC 错误区分，不伪装成工具结果。

## 4. 适配器（`amr2-adapter`）

- 位置：Ubuntu 侧独立进程；每个固定身份一个 UDS 监听（`0600`，父目录受限）。
- 配置：远程 URL、CA 文件、凭证文件（`0600`）、绑定表
  `{socket_path, token 引用, agent_id, tools}`。
- 协议：与 1.x daemon 相同的 4 字节长度前缀 + UTF-8 JSON-RPC 2.0；请求/响应
  形状保持，插件参数无需改写。
- 方法映射：daemon 方法 → MCP 工具（见 §5）；未知方法、`memory_spool_*`、
  管理方法一律拒绝，且不触达后端。
- 身份：适配器按 socket 绑定令牌；客户端传入的 `agent_id`/`source_agent`
  被丢弃；日志只记方法名、身份、结果分类、耗时。
- 并发：同一连接串行处理，每连接独立读写上下文，禁止跨任务共享 writer；
  不同连接可并发。适配器不自动重试任何请求。
- 通知语义：无 `id` 的 JSON-RPC 通知按 1.x 语义执行（如 `session.ingest`），
  不写回响应；未知/未授权通知只记录并跳过。2.x 插件应改用带 `id` 的请求
  获取确认，通知仅用于 1.x 兼容。
- 错误：`amr_error` 还原为原 JSON-RPC 错误码/消息/数据；网络/TLS/认证失败
  映射为明确的适配器错误码（不重试、不冒充已提交）。
- 队列：适配器无状态且**不自动重试**；会话归档的离线排队仍由客户端 spool 负责。
  超时后返回“状态未知”，依赖 `(session_id, message_id)` 与幂等键安全重试；
  非幂等写操作不得由适配器自动重放。
- 生命周期：`start`/`stop` 幂等；启动时清理残留 socket；失败关闭。

## 5. 方法矩阵（1.x → 2.x 工具）

| daemon 方法 | MCP 工具 | 备注 |
| --- | --- | --- |
| `memory.search` | `memory_search` | |
| `memory.create` | `memory_create` | |
| `memory.get` | `memory_get` | |
| `memory.update` | `memory_update` | |
| `memory.history` | `memory_history` | |
| `memory.delete` | `memory_delete` | |
| `memory.record` | `memory_record` | 兼容 |
| `memory.update_status` | `memory_update_status` | 兼容 |
| `session.ingest` | `memory_ingest_session` | |
| `session.checkpoint` | `session_checkpoint` | `memory_session_checkpoint` 为别名，适配器只用规范名 |
| `session.get_handoff` | `session_get_handoff` | `memory_get_handoff` 为别名 |
| `memory.candidates` | `memory_candidates` | 是否开放给插件待定 |
| `memory.approve_candidate` | `memory_approve_candidate` | 管理性写，默认不开放给 hook 插件 |
| `memory_spool_*` | — | 本机专用，适配器拒绝 |

## 6. 各 agent 插件矩阵

| Agent | 1.x 接入 | 2.x 接入 | 仓库内文件 | 2.x 状态 |
| --- | --- | --- | --- | --- |
| Claude Code | MCP stdio bridge | 优先直接 HTTPS MCP；不支持则 2.x stdio shim | 外部配置 | 待验证客户端能力 |
| Codex | MCP stdio bridge，来源误设 `claude_code` | 直接 HTTPS MCP 或 shim；来源改为 `codex` | 外部配置 | 待修来源 |
| ZCode | MCP stdio bridge | 直接 HTTPS MCP；否则 shim | 外部配置 | 待验证 |
| OpenCode | 原生 UDS 插件 + MCP bridge | 适配器 + 2.x 插件 | `plugins/opencode-amr` | 待重写 |
| OpenClaw | 原生 UDS 插件 | 适配器 + 2.x 插件 | `plugins/openclaw-amr` | 待重写 |
| Hermes | 原生 UDS Python 插件 | 适配器 + 2.x 插件 | `plugins/hermes-amr` | 待重写 |
| DSH | 原生 UDS Cordis 插件 | 适配器 + 2.x 插件 | `plugins/dsh-amr` | 待重写 |
| Doubao | 用户技能 → SSH stdio bridge | 适配器（SSH 转发 UDS）或 HTTPS MCP | 仓库外技能 | 待迁移 |
| 其他/Windows | SSH stdio bridge | HTTPS MCP 或 SSH → 适配器 UDS | 外部配置 | 待验证 |

原则：优先让客户端使用标准 HTTPS MCP；只有需要本地 hook 低延迟或客户端不支持
远程 MCP 时，才走 Ubuntu 适配器。插件包版本统一从 `2.0.0` 起，包名不变。

## 7. 兼容与迁移

- 数据：SQLite schema / Qdrant 集合 / 工具语义不变；2.x 可直接打开 1.x 备份。
- SQLite 已启用 WAL 与 `busy_timeout`（1.x 既有实现）；2.x 不改变写者模型。
- 单写者：迁移前停 1.x，复制一致备份，验证索引与客户端，再启动 2.x；
  回滚按演练过的备份恢复，明确可能丢失切换后的写入。
- 客户端：1.x 插件与 2.x 适配器不能共用同一 socket；迁移按 agent 逐个切换，
  保留 1.x 回退路径。
- 管理接口继续只走本地 admin UDS；2.x 不新增网络管理面。

## 8. 测试与交叉审计

- 单元/集成：隔离 TLS 证书、临时 UDS、假 daemon；禁止连接生产。
- 每个阶段执行：`AMR_REQUIRE_REMOTE=1` 远程测试 + 生产 venv 全量回归。
- 交叉审计：DeepSeek 子 agent 实现 → Gemini 按文件/差分块审计（JSON 输出）
  → 主控逐条判定（接受/驳回/误报）→ 修复并补测试 → Gemini 复审，直到无
  未关闭的高/中风险项。Gemini 的每条发现必须能对应到代码位置与触发条件，
  否则不采纳。
- 至少覆盖：认证绕过、路径/身份伪造、错误语义、离线/重试幂等、并发、
  生命周期、日志脱敏、证书校验。

## 9. 待拍板

1. `memory_candidates` / `memory_approve_candidate` 是否开放给 hook 插件？
   （建议：默认不开放，管理操作仍走本机 admin。）
2. 标准客户端直连 HTTPS 与适配器路径的默认选择（建议：能直连则直连）。
3. `/healthz` 是否要求认证（建议：仅回环/私网开放，不返回依赖细节）。
