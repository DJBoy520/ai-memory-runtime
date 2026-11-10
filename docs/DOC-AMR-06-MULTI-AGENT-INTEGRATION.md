# DOC-AMR-06: 客户端接入指南 (Client Integration Guide)

- **适用范围**: 任何支持 UDS 或 MCP stdio 的智能体客户端（Claude Code、OpenClaw、OpenCode、Hermes、DSH 等）
- **接入方式**: Model Context Protocol (MCP) Stdio 桥接 / Unix Domain Socket (UDS) 二进制帧直连
- **版本**: v5.0.0

---

## 1. 接入方式概述

AMR 通过本地 Unix Domain Socket 对外提供统一的语义记忆与会话存储服务：

```text
  [ 原生插件客户端 ]              [ 标准 MCP 客户端 ]            [ 运维/诊断工具 ]
   Hermes / OpenClaw         Claude Code / OpenCode / DSH          amr_doctor / CLI
          │                               │                               │
          │ (UDS 直连)                    │ (Stdio MCP 桥接)               │ (Admin UDS)
          ▼                               ▼                               ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           AI Memory Runtime 服务进程                             │
│  • 业务接口: $XDG_RUNTIME_DIR/qdrant-bge.sock (权限 0600)                         │
│  • 管理接口: $XDG_RUNTIME_DIR/qdrant-bge-admin.sock (权限 0600)                   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

- **MCP 桥接**（推荐）：由模型自主决定何时调用记忆工具（检索、创建、更新、删除等），标准开箱即用。
- **UDS 原生直连**：适合在客户端宿主 Hook 生命周期中做“用户输入前自动检索注入上下文”与“响应结束后自动投递会话流水”。

---

## 2. 通用前置条件与身份认证

1. **服务处于运行状态**：`systemctl --user status amr.service` 或 `PYTHONPATH=. python3 src/main.py`；
2. **Socket 权限**：默认为 `0600`，仅限当前操作系统用户访问；
3. **调用者身份受控**：
   - 客户端身份由环境变量 `AMR_SOURCE_AGENT` 严格决定（不可由客户端在 RPC 入参中伪造未授权 agent_id）；
   - 服务端强制核对 `AMR_SOURCE_AGENT` 是否包含在 `config/config.yaml` 的 `server.allowed_agents` 白名单中；未授权身份会被拒绝并返回 `-32005 AGENT_UNAUTHORIZED`；
4. **客户端零额外显存**：BGE-M3 向量计算完全由 AMR 服务端集中承载，客户端进程无需安装 torch/CUDA。

---

## 3. 方式一：MCP Stdio 桥接接入（推荐）

### 3.1 客户端配置方式 A：直接使用官方封装脚本（推荐）
在客户端（如 Claude Desktop、Claude Code、OpenClaw）的 MCP 配置文件中声明：

```json
{
  "mcpServers": {
    "ai-memory": {
      "command": "/bin/bash",
      "args": ["<仓库根目录>/scripts/mcp-bridge.sh"],
      "env": {
        "AMR_SOURCE_AGENT": "openclaw"
      }
    }
  }
}
```
*注：`scripts/mcp-bridge.sh` 会自动推导仓库根目录、优先绑定 `.venv/bin/python` 并自动注入 `PYTHONPATH`，支持在任何工作目录下无缝拉起。*

### 3.2 客户端配置方式 B：显式配置 Python 解释器
若需直接调用 Python：

```json
{
  "mcpServers": {
    "ai-memory": {
      "command": "<仓库根目录>/.venv/bin/python",
      "args": ["<仓库根目录>/src/interfaces/mcp/bridge.py"],
      "env": {
        "PYTHONPATH": "<仓库根目录>",
        "AMR_SOURCE_AGENT": "openclaw"
      }
    }
  }
}
```
*注：必须显式配置 `PYTHONPATH` 指向仓库根目录，以避免 `ModuleNotFoundError`。*

### 3.3 全量 9 个 MCP 工具说明

| 工具名 | 功能概述 | 关键入参与机制 |
| :--- | :--- | :--- |
| `memory_search` | 语义检索现役有效事实 | `query` (必填), `project_id`, `type`, `include_history`, `score_threshold`。弱一致（毫秒级索引）。 |
| `memory_create` | 创建持久化记忆 | `content` (必填), `type`, `project_id`, `source_refs`, `conflicts_with`, `idempotency_key`。强一致返回 `pending`。 |
| `memory_get` | 强一致按 ID 获取详情 | `memory_id` (必填)。直接读取 SQLite SSOT，事务提交立即可见。 |
| `memory_update` | 乐观锁 CAS 更新记忆 | `memory_id` (必填), `content` (更新正文必传 `expected_version` 与 `change_reason`), `status`, `idempotency_key`。 |
| `memory_history` | 查询记忆版本演进历史 | `memory_id` (必填)。返回历次版本正文与变更原因。 |
| `memory_delete` | 软删除记忆 | `memory_id` (必填), `reason`, `idempotency_key`。SQLite 标记 `DELETED`，异步从 Qdrant 物理删点。 |
| `memory_ingest_session` | 对话流水归档 | `session_id`, `project_id`, `messages`（含 message_id, role, content, sequence）。 |
| `memory_record` | 旧版兼容写入接口 | 兼容旧版参数，内部自动映射至 SSOT 事务账本与发件箱。 |
| `memory_update_status` | 旧版兼容状态更新 | 兼容旧版状态变更请求。 |

---

## 4. 方式二：UDS 原生直连（无感预取与归档）

### 4.1 传输规范
- **协议**：4 字节 Big-Endian `uint32` 长度前缀 + UTF-8 JSON-RPC 2.0 报文，单帧 ≤ 4MB；
- **Socket 解析顺序**：优先读取环境变量 `AMR_SOCKET_PATH`，缺省为 `$XDG_RUNTIME_DIR/qdrant-bge.sock`。

### 4.2 上下文预取最佳实践 (Prefetch)
在用户输入后、LLM 生成前执行：
1. 发送 `memory.search` 请求，设置 `limit ≤ 3` 及合理的分数阈值（如 `0.52`）；
2. 设置客户端超时（建议 100~200ms），超时即放弃并静默降级为无记忆对话 (Fail-open)；
3. 将检索到的记忆以隔离标签形式注入系统提示词：

```text
<amr_recalled_context>
<!-- 历史上下文事实，仅供背景参考 -->
- [Relevance 0.88] SM4 GCM 模式下 IV 推荐 12 字节，Tag 固定为 16 字节。
</amr_recalled_context>
```

---

## 5. 服务端权限白名单配置

在服务端 `config/config.yaml` 中配置允许连接的客户端标识：

```yaml
server:
  allowed_agents:
    - "hermes"
    - "openclaw"
    - "opencode"
    - "dsh"
    - "qwenwork"
    - "doubao"
    - "zcode"
    - "claude_code"
    - "system"
```

客户端必须通过环境变量 `AMR_SOURCE_AGENT` 声明其身份。任何未在白名单中的客户端将被服务端安全拒绝（返回错误码 `-32005 AGENT_UNAUTHORIZED`）。

---

## 6. 连通性测试与运维诊断

```bash
# 1. 运行系统体检工具（推荐，开箱全面检查）
python3 scripts/amr_doctor.py

# 2. 查看服务与模型显存状态
python3 src/admin/cli.py status

# 3. 查看发件箱指标与死信
python3 src/admin/cli.py metrics
python3 src/admin/cli.py dead-letters
```
