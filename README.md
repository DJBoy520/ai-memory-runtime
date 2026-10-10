# AI Memory Runtime (AMR)

面向多智能体（Hermes Agent、OpenClaw、OpenCode、DSH、QwenWork 等）的本地语义记忆与会话存储组件。

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![Storage](https://img.shields.io/badge/storage-SQLite%20WAL-lightgrey)](docs/DOC-AMR-02-ADD.md)
[![Vector](https://img.shields.io/badge/vector-Qdrant-red)](https://qdrant.tech/)
[![Protocol](https://img.shields.io/badge/protocol-UDS%20%7C%20MCP-orange)](docs/DOC-AMR-04-API.md)

---

## 主要功能

- **本地持久化存储**：原始会话记录与提炼卡片存入本地 SQLite（WAL 模式），方便本地审计与回溯。
- **异步向量同步**：采用事务发件箱（Outbox）机制异步写入 Qdrant，降低外部网络或存储异常对主流程的影响。
- **来源文本比对**：支持关联并校验原始会话文本跨度（Span Match），辅助提升提炼结果准确度。
- **本地 IPC 通信**：基于 Unix Domain Socket (UDS) 与自定义二进制帧协议，适合同机进程间调用。
- **基础对账校验**：提供定时比对脚本，检查 SQLite 记录与向量索引的一致性并支持按需重试。

---

## 架构示意

```text
  [ Hermes Agent ]    [ OpenClaw ]    [ OpenCode / DSH ]    [ Other Agents ]
         │                  │                  │                   │
         └─────────┬────────┴─────────┬────────┘                   │
                   │ (本地 UDS 连接)  │ (Stdio MCP 桥接)           │
                   ▼                  ▼                            ▼
┌────────────────────────────────────────────────────────────────────────┐
│                        AI Memory Runtime (AMR)                         │
│                                                                        │
│   • 协议接口：UDS Socket ($XDG_RUNTIME_DIR/qdrant-bge.sock) / MCP Stdio│
│   • 本地数据：SQLite WAL (sessions.db: sessions / memories / outbox)   │
│   • 向量索引：BGE-M3 模型推理 ──(后台队列同步)──► Qdrant 实例            │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 快速开始

### 1. 安装依赖

```bash
git clone https://github.com/DJBoy520/ai-memory-runtime.git
cd ai-memory-runtime
pip install torch transformers sentence-transformers qdrant-client pyyaml pydantic
```

### 2. 基础配置

复制配置示例：

```bash
cp config/config.example.yaml config/config.yaml
chmod 600 config/config.yaml
```

参考配置（`config/config.yaml`）：

```yaml
server:
  business_socket: "/run/user/1000/qdrant-bge.sock"
  admin_socket: "/run/user/1000/qdrant-bge-admin.sock"
  allowed_agents: ["hermes", "openclaw", "opencode", "dsh", "qwenwork"]

storage:
  sqlite_path: "data/sessions.db"

qdrant:
  url: "http://127.0.0.1:6333"
  api_key: ""

search:
  default_score_threshold: 0.52
  project_fallback_ids: ["global", "general"]
```

### 3. 启动服务

```bash
# 方式 A：前台运行
python3 src/main.py

# 方式 B：作为 systemd 用户服务运行
mkdir -p ~/.config/systemd/user/
cp systemd/amr.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now amr.service
```

---

## 客户端接入方式

### Hermes Agent (UDS)
```bash
hermes config set memory.provider amr
```

### OpenClaw / 通用 Agent (MCP)
在客户端 MCP 配置中添加：
```json
{
  "mcpServers": {
    "ai-memory": {
      "command": "python3",
      "args": ["src/interfaces/mcp/bridge.py"],
      "env": { "AMR_SOURCE_AGENT": "openclaw" }
    }
  }
}
```

---

## 日常运维

```bash
# 检查运行与队列状态
python3 src/admin/cli.py status

# 运行数据对账脚本 (检查模式)
python3 scripts/daily_memory_reconciliation.py --dry-run

# 运行测试用例
pytest tests/test_step1_protocol.py tests/test_v2_engine.py
```

---

## 文档参考

- [需求说明 (PRD)](docs/DOC-AMR-01-PRD.md)
- [架构设计 (ADD)](docs/DOC-AMR-02-ADD.md)
- [接口规范 (API)](docs/DOC-AMR-04-API.md)
- [客户端接入指南](docs/DOC-AMR-06-MULTI-AGENT-INTEGRATION.md)
- [RFC 提案](docs/rfcs/)

---

## 许可证

[Apache License 2.0](LICENSE)
