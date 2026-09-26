# AI Memory Runtime (AMR)

> **Unified High-Performance Semantic Memory Runtime & GPU Resource Daemon for Autonomous AI Agents**

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue)](https://www.python.org/)
[![Qdrant](https://img.shields.io/badge/vector--db-Qdrant-red)](https://qdrant.tech/)
[![BAAI](https://img.shields.io/badge/embeddings-BGE--M3-green)](https://huggingface.co/BAAI/bge-m3)
[![Protocol](https://img.shields.io/badge/protocol-UDS%20%7C%20MCP%20%7C%20JSON--RPC-orange)](docs/)

---

## 📖 项目简介 (Overview)

**AI Memory Runtime (AMR)** 是专为多 Agent 协作生态（如 Hermes Agent、OpenClaw、OpenCode、DSH 等）打造的高性能统一语义记忆底座与 GPU 显存守护调度器。

在多 Agent 协同工作流中，如果各 Agent 分别加载嵌入模型或直接裸调向量数据库，极易造成 **GPU 显存暴涨、会话上下文割裂、重复提炼与跨 Agent 记忆孤岛**。AMR 通过以下核心架构彻底解决上述痛点：

1. **共享语义中枢**：所有 Agent 不直接裸连底层数据库，统一由 AMR 提供 `memory_search`、`memory_record`、`memory_ingest_session` 等标准生命周期管理。
2. **动静分离与溯源**：Raw Session 原始对话流水写入本地 SQLite WAL 审计库（确保存证可溯源）；原子化提炼后的高质量事实卡片（Fact/Decision/Preference）经由 BGE-M3 向量化沉淀至 Qdrant。
3. **动态显存守护 (Zero-VRAM Idle)**：内建按需加载与空闲自动卸载机制（默认空闲 3600 秒未活动自动释放 GPU VRAM），在轻量 GPU（如 Tesla P4 8G / 消费级显卡）上与其他高负载推理服务和谐共存。
4. **双通道极速接入**：
   - **Unix Domain Socket (UDS)**：首字响应延迟敏感场景，纯二进制/Big-Endian Framing 纳秒级进程间通信；
   - **Model Context Protocol (MCP)**：支持各大 Agent 平台通过 stdio 一键接入标准工具链。

---

## 🏗️ 架构拓扑 (Architecture)

```text
┌────────────────────────────────────────────────────────────────────────┐
│                        Autonomous AI Agent Fleet                       │
│      Hermes Agent        OpenClaw           OpenCode           DSH     │
└───────────┬──────────────────┬──────────────────┬───────────────┬──────┘
            │ (UDS / 0.3s)     │ (Stdio MCP)      │ (Stdio MCP)   │
            ▼                  ▼                  ▼               ▼
┌────────────────────────────────────────────────────────────────────────┐
│                        AI Memory Runtime (AMR)                         │
│                                                                        │
│   ┌───────────────────────────┐      ┌─────────────────────────────┐   │
│   │   Business UDS Engine     │      │     Admin UDS Control       │   │
│   │ (/run/user/.../qdrant-bge)│      │ (.../qdrant-bge-admin.sock) │   │
│   └─────────────┬─────────────┘      └──────────────┬──────────────┘   │
│                 │                                   │                  │
│                 ▼                                   ▼                  │
│   ┌────────────────────────────────────────────────────────────────┐   │
│   │             Dynamic Inference & VRAM Daemon                    │   │
│   │      BGE-M3 Dense + Sparse (ColBERT) Embedding Engine          │   │
│   │      (Auto load on request / Auto offload after idle 3600s)    │   │
│   └──────────────────────┬─────────────────────────────────────────┘   │
│                          │                                             │
│         ┌────────────────┴───────────────┐                             │
│         ▼                                ▼                             │
│   ┌───────────────┐              ┌─────────────────────────────┐       │
│   │  SQLite WAL   │              │     Distributed Qdrant      │       │
│   │  (Raw Dialog) │              │    Collection: ai_memory    │       │
│   └───────────────┘              └─────────────────────────────┘       │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 🚀 快速开始与部署指南 (Deployment)

### 1. 环境依赖 (Prerequisites)

- Linux (Ubuntu 20.04+ / Debian 11+ / Arch / RedHat)
- Python 3.10+
- NVIDIA GPU (Compute Capability 6.0+，支持 CUDA 11/12)
- Qdrant 向量数据库实例（局域网或本地 Docker：`docker run -p 6333:6333 qdrant/qdrant`）

### 2. 安装与准备 (Installation)

```bash
# 克隆仓库
git clone https://github.com/DJBoy520/qdrant-bge-memory.git
cd qdrant-bge-memory

# 安装 Python 核心依赖
pip install torch transformers sentence-transformers qdrant-client pyyaml pytest

# 下载 BGE-M3 模型权重至本地 models 目录
mkdir -p models
# 可通过 huggingface-cli 或 git lfs 下载:
# git clone https://huggingface.co/BAAI/bge-m3 models/bge-m3
```

### 3. 配置管理 (Configuration)

复制模板并根据实际环境调整配置（`config/config.yaml` 拥有 0600 权限保障安全）：

```yaml
server:
  business_socket: "/run/user/1000/qdrant-bge.sock"
  admin_socket: "/run/user/1000/qdrant-bge-admin.sock"
  idle_timeout_seconds: 3600    # 空闲自动卸载显存倒计时（秒）
  device: "cuda:0"              # 推理设备: cuda:0 或 cpu

qdrant:
  url: "http://192.168.30.161:6333"   # 内部 Qdrant 服务端点
  collection_name: "ai_memory"
  vector_size: 1024

storage:
  sqlite_path: "data/amr_sessions.db"
```

### 4. 运行全量单元测试 (Verification)

AMR 包含覆盖端到端 UDS 协议、动态加载、半包接收与 Qdrant 交互的严密测试套件：

```bash
pytest tests/ -v
# 输出: 167 passed in 12.3s
```

### 5. 配置为 Systemd 用户守护进程 (开机自启)

```bash
# 复制 systemd service 配置
mkdir -p ~/.config/systemd/user/
cp systemd/qdrant-bge.service ~/.config/systemd/user/

# 开启用户驻留（确保注销登录后依然自启运行）
loginctl enable-linger $USER

# 激活与启动服务
systemctl --user daemon-reload
systemctl --user enable qdrant-bge.service
systemctl --user start qdrant-bge.service

# 查看服务运行状态
systemctl --user status qdrant-bge.service
```

---

## 🛠️ 管理与日常排障 (Admin CLI)

AMR 提供了轻量独立的管理命令行工具：

```bash
# 查看运行时健康状态、显存分配与加载状态
python3 src/admin/cli.py status

# 强制触发模型预热加载
python3 src/admin/cli.py load

# 手动立即卸载模型并释放 GPU 显存
python3 src/admin/cli.py unload

# 调整空闲超时时间（实时生效）
python3 src/admin/cli.py set-idle-timeout 1800
```

---

## 🔌 Agent 集成方式 (Agent Integrations)

### 1. Hermes Agent 原生无感记忆接入

Hermes 原生 AMR 插件置于 `plugins/memory/amr`：

- **输入预取 (Prefetch)**：每轮对话开始前 0.3 秒（300ms）自动向 UDS 发送并发检索，无感注入 Prompt；
- **输出归档 (Sync Turn)**：对话结束后后台异步持久化会话流。

在 Hermes 中一键切换生效：
```bash
hermes config set memory.provider amr
```

### 2. OpenClaw / Claude / OpenCode (MCP 方式)

通过内置的标准 stdio MCP Bridge 连接：

```bash
hermes mcp add qdrant-bge \
  --command /usr/bin/python3 \
  --env PYTHONPATH=/path/to/qdrant-bge-memory AMR_SOURCE_AGENT=hermes \
  --args /path/to/qdrant-bge-memory/src/interfaces/mcp/bridge.py
```

提供标准 MCP Tools：
- `memory_search`：语义向量与元数据混合召回；
- `memory_record`：结构化写入单条关键事实；
- `memory_get`：按 ID 精确溯源原始证据；
- `memory_update_status`：记忆版本演化管理（active / superseded / archived）；
- `memory_ingest_session`：会话级整包清洗与事实提炼。

---

## 🔐 权威存证与合规保障 (AEP Notarized)

本工程遵循 **AEP (Attestation & Evidence Exchange Protocol)** 可信数字证据基础设施协议。

- **全局统一存证中心**：`~/.aep/`
- **签名算法**：国家密码管理局 SM2 国密数字签名 + SM3 密码杂凑算法
- **证书合规性**：三级权威证书链（含 Root-CA、Issuing-CA）
- **时间与链上锚定**：L3 RFC3161/GM-T 0033 时间戳 + L4 区块链不可变锚定（`https://chain.aep.org.cn`）
- **验证指令**：
  ```bash
  aep --data-dir ~/.aep validate <path-to-evidence.aep>
  ```

---

## 📄 开源许可证 (License)

本项目采用 [Apache License 2.0](LICENSE) 许可证。
