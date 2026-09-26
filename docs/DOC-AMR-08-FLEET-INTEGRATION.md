# 方案说明书：OpenClaw 与 OpenCode 统一接入 AI Memory Runtime (AMR) 架构方案

- **设计方**：Hermes Agent
- **审核方**：OpenClaw Auditor
- **执行方**：OpenCode（待审核通过后下发）
- **定案时间**：2026-09-26
- **文档编号**：DOC-AMR-08-FLEET-INTEGRATION

---

## 一、背景与现状痛点 (Problem Statement)

当前工作区已成功部署并运行 **AI Memory Runtime (AMR)**（守护进程 `qdrant-bge.service`，监听 UDS `/run/user/1000/qdrant-bge.sock`，具备 BGE-M3 动态加载、3600秒空闲自动卸载显存能力，且已配置好 Qdrant API Key 鉴权）。

### 现状与隐患分析：

1. **Hermes Agent**：
   - 状态：**已完全接入** AMR（原生 UDS 插件 `plugins/memory/amr` + `qdrant-bge` stdio MCP Bridge）。

2. **OpenClaw**：
   - 当前配置：`~/.openclaw/openclaw.json` 中的 `ai-memory` 仍指向旧版单体脚本：
     ```json
     "command": "/home/dj/WorkSpaces/openclaw/knowledge-base/.venv/bin/python3",
     "args": ["/home/dj/WorkSpaces/ai-history-ingest/scripts/memory_mcp_server.py"]
     ```
   - **痛点 1（显存重复浪费）**：旧脚本在每次 OpenClaw 启动 MCP 时，都会在进程内部独立加载一遍 BGE-M3 模型，额外霸占约 **1.26GB GPU 显存**，无法享受 AMR 的 3600 秒空闲自动卸载机制。
   - **痛点 2（鉴权阻断）**：旧脚本内部硬编码为 `QdrantClient(url=QDRANT_URL)`，**未配置 api_key**。在今天 Qdrant 开启强鉴权（401 Unauthorized）后，OpenClaw 调用旧脚本的任何读写都会直接失败。
   - **痛点 3（数据孤岛）**：旧脚本的数据存储逻辑与 Hermes 不完全一致，无法做到跨 Agent 记忆版本演化（`superseded`）。

3. **OpenCode**：
   - 当前配置：`~/.config/opencode/opencode.jsonc` 中的 `mcpServers.ai-memory` 与 `mcp.ai-memory` 同样指向上述旧脚本 `memory_mcp_server.py`。
   - 同样面临 401 鉴权失败与重复加载模型的风险。

---

## 二、目标与收益 (Goals & Benefits)

1. **统一接入 AMR 标准 MCP Bridge**：
   将 OpenClaw 与 OpenCode 的 `ai-memory` MCP 服务器统一迁移到 AMR 官方的 stdio 桥接器：
   `/home/dj/WorkSpaces/ai-memory-runtime/src/interfaces/mcp/bridge.py`
2. **显存彻底归一（Zero-VRAM Idle）**：
   所有 Agent 进程都不再私自加载 PyTorch/Transformers 嵌入模型。全部请求通过轻量级 stdio 转发至 AMR 统一的 UDS 守护进程，显存常驻开销降为 0MB，空闲 1 小时自动回收显存。
3. **安全鉴权解耦**：
   OpenClaw 与 OpenCode 无需感知 Qdrant 的 IP 与 API Key，全部由底层 AMR 统一纳管，杜绝凭据多处散落。
4. **Agent 标识隔离与共享模型**：
   通过环境变量注入 `AMR_SOURCE_AGENT=openclaw` 与 `AMR_SOURCE_AGENT=opencode`。存储时在 Payload 中明确标注入库来源，检索时默认全局共享，完美契合“所有 AI Assistant 共享同一个长期语义记忆底座”的设计哲学。

---

## 三、详细改造方案 (Implementation Details)

### 方案对比评估

| 评估维度 | 方案 A：直接修改旧脚本补齐 api_key | 方案 B（推荐）：OpenClaw/OpenCode 接入 AMR stdio Bridge |
| :--- | :--- | :--- |
| **显存占用** | ❌ 极差。每个 Agent 独立吃 1.26G 显存（2 个 Agent = 2.5G+） | ✅ 最优。0 额外显存，共享 AMR 守护进程与 3600s 自动卸载 |
| **凭据管理** | ❌ 差。API Key 散落在多个遗留脚本中 | ✅ 极高。统一由 `config/config.yaml` 纳管，0600 权限保护 |
| **记忆同步** | ❌ 割裂。底层 Schema 和状态演化不兼容 | ✅ 完美。Hermes、OpenClaw、OpenCode 读写完全同构 |
| **改动风险** | ⚠️ 维护两个分叉的代码仓库 | ✅ 极低。仅修改两处客户端标准 JSON 配置，随时可平滑回滚 |

---

### 具体修改清单

#### 1. OpenClaw 配置迁移 (`~/.openclaw/openclaw.json`)

**修改前**（位于 `mcp.servers` 层级）：
```json
"mcp": {
  "servers": {
    "ai-memory": {
      "command": "/home/dj/WorkSpaces/openclaw/knowledge-base/.venv/bin/python3",
      "args": [
        "/home/dj/WorkSpaces/ai-history-ingest/scripts/memory_mcp_server.py"
      ],
      "enabled": true
    }
  }
}
```

**修改后**（按审计意见，严格位于 `mcp.servers` 内部）：
```json
"mcp": {
  "servers": {
    "ai-memory": {
      "command": "/usr/bin/python3",
      "args": [
        "/home/dj/WorkSpaces/ai-memory-runtime/src/interfaces/mcp/bridge.py"
      ],
      "env": {
        "PYTHONPATH": "/home/dj/WorkSpaces/ai-memory-runtime",
        "AMR_SOURCE_AGENT": "openclaw"
      },
      "enabled": true
    }
  }
}
```

#### 2. OpenCode 配置迁移 (`~/.config/opencode/opencode.jsonc`)

**修改前**：
```jsonc
  "mcpServers": {
    "ai-memory": {
      "type": "stdio",
      "command": "/home/dj/WorkSpaces/openclaw/knowledge-base/.venv/bin/python3",
      "args": [
        "/home/dj/WorkSpaces/ai-history-ingest/scripts/memory_mcp_server.py"
      ],
      "enabled": true
    }
  },
  "mcp": {
    "ai-memory": {
      "type": "local",
      "command": [
        "/home/dj/WorkSpaces/openclaw/knowledge-base/.venv/bin/python3",
        "/home/dj/WorkSpaces/ai-history-ingest/scripts/memory_mcp_server.py"
      ]
    }
  }
```

**修改后**：
```jsonc
  "mcpServers": {
    "ai-memory": {
      "type": "stdio",
      "command": "/usr/bin/python3",
      "args": [
        "/home/dj/WorkSpaces/ai-memory-runtime/src/interfaces/mcp/bridge.py"
      ],
      "env": {
        "PYTHONPATH": "/home/dj/WorkSpaces/ai-memory-runtime",
        "AMR_SOURCE_AGENT": "opencode"
      },
      "enabled": true
    }
  },
  "mcp": {
    "ai-memory": {
      "type": "local",
      "command": [
        "/usr/bin/python3",
        "/home/dj/WorkSpaces/ai-memory-runtime/src/interfaces/mcp/bridge.py"
      ],
      "environment": {
        "PYTHONPATH": "/home/dj/WorkSpaces/ai-memory-runtime",
        "AMR_SOURCE_AGENT": "opencode"
      }
    }
  }
```

---

## 四、安全与回滚预案 (Rollback Plan)

1. **备份先行**：
   在应用修改前，自动创建带时间戳的镜像备份：
   - `~/.openclaw/openclaw.json.bak-<timestamp>`
   - `~/.config/opencode/opencode.jsonc.bak-<timestamp>`
2. **平滑回滚**：
   若 OpenClaw 或 OpenCode 启动加载 MCP 出现语法异常，直接恢复备份文件并重启服务，RTO < 30 秒。
3. **验收标准**：
   - 执行 `openclaw mcp test ai-memory` 或对通调用；
   - 检查 OpenCode 与 OpenClaw 能够成功列出 `memory_search`、`memory_record`、`memory_get`、`memory_ingest_session` 5 个标准工具；
   - 真实发起一次检索，断言 Qdrant 鉴权正常，显存无多余占用。
