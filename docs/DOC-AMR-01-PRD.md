# AI Memory Runtime - 需求规格说明书 (PRD)

**文档标识**：`DOC-AMR-01-PRD`  
**版本号**：`v1.0.0`  
**定案日期**：2026-09-26  
**密级状态**：内部架构标准  
**编写方**：OpenClaw（架构团队）  
**审计方**：Hermes（架构专家）  
**执行方**：opencode（代码交付）

---

## 1. 项目背景与问题陈述

当前本地工作站拥有 Tesla P4 GPU（7.6GB 显存），运行多个自主 AI Agent（OpenClaw、Hermes、opencode、Cursor 等）。由于各 Agent 缺乏统一的长期记忆基础设施，面临以下系统级痛点：

1. **显存灾难（OOM）**：
   - MinerU 视觉模型常驻消耗 ~2.64GB 显存，解析大图/长文档时瞬时峰值达 4.5GB+。
   - 各 Agent 若私自 `import torch` 载入 BGE-M3，每个实例霸占 ~1.26GB 显存，多 Agent 并发直接引发 CUDA OOM 致命崩溃。
2. **知识与记忆孤岛**：
   - 无法跨 Agent 共享工程决策、国密规范（`crypto_standards`）与长期记忆。
   - 缺乏全局的会话溯源、知识演进与软删除机制。
3. **架构职责倒挂与“公共 GPU 厨房”风险**：
   - 若直接暴露底层向量引擎（`bge_embed`），各 Agent 会在私有代码中自建向量流水线，造成治理混乱、Schema 污染与队列雪崩。

---

## 2. 系统定位与核心愿景

- **系统定位**：**AI Memory Runtime（本机 AI 语义记忆与知识运行时）**。
- **核心愿景**：为内网所有 AI Assistant 提供统一的长期语义记忆、结构化知识检索、原始会话幂等追溯与 GPU 显存托管守护。
- **关键设计边界**：
  - 本系统是 **Memory Runtime**，不是单纯的“Qdrant MCP”；底层更换存储（如 Qdrant -> Milvus）或更换嵌入模型，上层 AI Agent 完全无感。
  - 纯本机 IPC 架构（Unix Domain Socket，UDS），物理上杜绝内网暴露与端口扫描。

---

## 3. 功能性需求 (Functional Requirements)

### 3.1 语义记忆管理 (Memory Management)
- **FR-MEM-01 记忆语义检索 (`memory_search`)**：
  - 支持自然语言语义查询，返回按相似度归一化排名的结构化记忆片段。
  - 支持多集合（`ai_memory`, `crypto_standards`, `project_docs`）聚合检索。
  - **强制安全过滤**：服务端底层硬性过滤 `status == "active"`，绝不召回被废弃或删除的陈旧记忆。
- **FR-MEM-02 显式记忆沉淀 (`memory_record`)**：
  - 由 Agent 在对话结束后或关键节点主动提炼调用，明确记录长期事实、决策、代码规范。
  - **长文本分块**：当输入 Token 超过 8192 时，自动进行带滑动窗口（Overlap 128 Tokens）的分块存储，记录 `parent_memory_id` 与 `chunk_index`。
- **FR-MEM-03 记忆精准提取与溯源 (`memory_get`)**：
  - 根据 `memory_id` 获取记忆详情，并必须返回关联的 `source_message_ids` 与 `session_id`，形成可核验的证据溯源链。
- **FR-MEM-04 记忆状态四态化流转 (`memory_update_status`)**：
  - 记忆不采用物理硬删除，支持四态流转：
    - `active`：当前生效中的有效记忆。
    - `superseded`：被新决策替代（保留历史决策演变链，指向新记忆 ID）。
    - `archived`：归档冻结。
    - `deleted`：标记删除。

### 3.2 原始会话审计与持久化 (Session Ingestion)
- **FR-SES-01 原始会话落盘 (`memory_ingest_session`)**：
  - 负责原始对话流水的无损保存，解耦业务记忆提炼与流水审计。
  - 存储于本地 SQLite（WAL 模式）。
- **FR-SES-02 幂等性与版本变更 (Revision) 识别**：
  - 建立 `(session_id, message_id)` 联合主键校验。
  - 引入 `content_hash`（SHA-256）：若 ID 相同但 Hash 变动，自动识别为消息修订（Revision）并更新，杜绝静默吞数据或重复膨胀。

### 3.3 模型守护与显存动态调度 (GPU Runtime)
- **FR-MOD-01 物理单例驻留**：
  - 全局唯一守护进程持有 BGE-M3 模型（FP16，显存锁定 ~1.18GB）。
- **FR-MOD-02 5分钟闲置自动释放 (Idle Timeout)**：
  - 设置 300 秒计时器；连续 5 分钟无推理请求时，自动执行 `del model` + `gc.collect()` + `torch.cuda.empty_cache()`，将显存归还给操作系统与 MinerU。
- **FR-MOD-03 按需自愈与自动唤醒 (Auto-load)**：
  - 下次检索或写入到达时，自动触发模型冷启动载入（耗时约 3~5 秒）。

### 3.5 智能体无感记忆运行时 (Fleet Zero-Friction Runtime Pipeline)
- **FR-FLT-01 运行时驱动无感预取 (Zero-Friction Prefetch & Injection)**：
  - 记忆能力从“Agent Tool Call”降级定位升级为 **Agent Runtime Context Pipeline**。
  - 在大模型推理前由生命周期钩子（如 `before_prompt_build`）自动前置拦截用户输入，向 AMR UDS 发送 `memory.search`，硬截止时间（Hard Deadline）≤ 80ms ~ 100ms；
  - 命中记忆通过严格的 XML 标签（`<amr_recalled_context>`）与转义进行隔离，注入 Prompt 前置上下文，模型天然携带历史决策与背景，主会话无 Tool Call 停顿。
- **FR-FLT-02 轮次结束无感流水归档 (Zero-Friction Session Ingestion)**：
  - 会话轮次生成完毕后，由生命周期钩子（如 `after_turn` / `on_message_complete`）触发非阻塞异步投递（Fire-and-forget，At-most-once 交付语义，主线程等待 0ms）；
  - 自动向 AMR UDS 发送 `session.ingest`，参数包含 `session_id`, `agent_id`, `project_id`, 消息列表（带时序严格递增的 `sequence` 与 `content_hash`），自动落盘 SQLite `sessions.db`；
  - 后台静默完成会话审计并按需触发长效事实沉淀。
- **FR-FLT-03 全智能体极薄适配器规范 (Ultra-Thin Adapter Pattern)**：
  - 接入 AMR 的客户端（Hermes 插件、OpenClaw 插件、OpenCode 扩展）定位为纯胶水适配层；
  - 严格遵守 **Zero-VRAM 铁律**（0 显存额外开销），严禁在适配器内自建向量计算、本地 SQLite 或直连底层 Qdrant 数据库；
  - 严格遵循 Fail-Open 兜底原则，AMR 离线或异常时静默降级为普通无记忆对话，绝不阻断宿主 Agent 交互。

---

## 4. 非功能性需求 (Non-Functional Requirements)

### 4.1 显存与资源红线
- **NFR-RES-01**：在 MinerU 常驻 (2.64GB) 下，BGE-M3 激活时总显存占用 ≤ 4.2GB，常年稳保 3.5GB+ 弹性显存。
- **NFR-RES-02**：MCP Bridge 客户端内存占用 ≤ 25MB，启动耗时 ≤ 100ms，严禁引入 PyTorch 或 CUDA。

### 4.2 性能与并发
- **NFR-PERF-01**：单条文本 Embedding 推理耗时 ≤ 35ms（GPU FP16）。
- **NFR-PERF-02**：五层流控防御机制（4MB 请求上限、字符数截断、8192 Token 分块、Batch 16 批处理、Queue 64 队列深度）。

### 4.3 安全与权限
- **NFR-SEC-01**：零 TCP 端口暴露，全部走本地 UDS 文件通信，Socket 文件权限 `0600`。
- **NFR-SEC-02**：普通 Agent 严禁暴露 `bge_embed` 底层工具与集合删除接口。
- **NFR-SEC-03**：敏感凭证（Qdrant Key）仅保存在 `config.yaml`（权限 `0600`），绝不进 Git，日志自动脱敏。
