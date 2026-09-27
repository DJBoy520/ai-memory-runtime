# AI Memory Runtime (AMR) 认知记忆引擎架构升级与全量离线提纯方案（v2.1 终案）

## 一、 现状与痛点分析（Context & Problem Statement）

### 1.1 现状盘点
- **基础设施**：宿主机部署了 Tesla P4 (8GB VRAM) 显卡，常驻运行 BGE-M3 (1024 维) 向量推理引擎（Zero-VRAM 3600s 卸载机制），通过 Unix Domain Socket (`/run/user/1000/qdrant-bge.sock`) 提供微秒级 IPC 通信。
- **存储拓扑**：本地 SQLite (`data/sessions.db`) 记录原始会话消息；远端 Unraid Docker (`192.168.30.161:6333`) 运行 Qdrant，维护 `ai_memory` (现存 1,986 条点位) 及若干业务知识库。
- **多智能体生态**：Hermes、OpenClaw、OpenCode、DSH 四大核心 Agent 已通过轻量插件挂载 AMR UDS，具备输入无感 Prefetch 与输出会话 Ingest 能力。

### 1.2 核心痛点
1. **Qdrant 被误用作“事实真相源（SSOT）”**：记忆的增删改查直接作用于 Qdrant，缺乏强约束的关系型元数据表，导致版本控制、状态回滚与多字段精确关联极度困难。
2. **“原始对话”与“记忆知识”混为一谈**：此前写入 Qdrant 的多为包含“用户指令”、“结论/解决方案”的长段落原始对话，向量语义被大量口语、工具报错和中间推理稀释，严重降低检索精度。
3. **缺乏“记忆生命周期（Lifecycle）”与确定性治理**：
   - 冲突处理过于机械（仅靠时间覆盖），忽视了偏好的“可共存性”、项目的“状态机流转”以及事件的“不可篡改性”；
   - 重复表达直接产生冗余点位，缺乏基于证据累加（Evidence Accrual）的“记忆合并（Merge）”与置信度自增强机制；
   - 检索打分依赖单一向量相似度，缺乏时效衰减、置信度与重要性权重的复合重排。

---

## 二、 总体需求与核心原则（Requirements & Principles）

### 2.1 核心原则
1. **单一真相源（SSOT）下沉关系库**：
   - SQLite/PostgreSQL 为唯一事实真相源，全量沉淀 `raw_messages`（证据层）与 `memories`（知识层）；
   - Qdrant 定位为纯粹的 **只读检索索引（Retrieval Index Projection）**，可随时由关系库 100% 幂等重建。
2. **证据（Evidence）与记忆（Memory）严格解耦**：
   - 用户单次会话中的表达仅为 Evidence（L0）；
   - 经过确定性抽取形成 Candidate（L1），经多轮交叉验证或高置信确认后晋升为 Confirmed Memory（L2），失效后进入 Archived（L3）。
3. **“LLM 负责理解，Engine 负责治理”**：
   - LLM 仅负责实体抽取、三元组解构、指代消解与语义判定；
   - 引擎代码（确定性逻辑）负责 Schema 校验、权限隔离、版本递增、冲突策略调度、合并去重与状态机流转。
4. **分类分级的冲突与合并模型**：
   - 区分覆盖型、共存型、状态型、事件型 4 种冲突模式；
   - 重复提及不新增点位，而是强化原有记忆的 `mention_count`、`confidence` 并追加 `evidence_ids`。

---

## 三、 目标架构设计（Target Architecture）

### 3.1 系统分层拓扑

```
┌─────────────────────────────────────────────────────────────────┐
│                    多智能体接入层 (Agent Layer)                 │
│      Hermes  │  OpenClaw (zxm)  │  OpenCode  │  DSH (Web)       │
└───────────────────────────────┬─────────────────────────────────┘
                                │ UDS / JSON-RPC 2.0 (Zero-VRAM)
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│              AI Memory Runtime (AMR) 认知引擎中枢                │
│                                                                 │
│  ┌───────────────────┐    ┌──────────────────────────────────┐  │
│  │   Pipeline 层     │    │           Governance 层          │  │
│  │  - Normalizer     │    │  - Schema 强校验 (Pydantic v2)   │  │
│  │  - Noise Filter   │    │  - Conflict Resolver (4 种策略)  │  │
│  │  - LLM Extractor  │    │  - Hybrid Merger (三元组+NLI)    │  │
│  │  - Dedup Engine   │    │  - Composite Scorer (复合重排)   │  │
│  └─────────┬─────────┘    └────────────────┬─────────────────┘  │
│            ▼                               ▼                    │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │               关系型真相源 (SQLite WAL / SSOT)            │  │
│  │   - raw_sessions (会话表)      - raw_messages (证据表)    │  │
│  │   - memories (记忆主表)        - memory_evidence (溯源关联)│  │
│  │   - qdrant_sync_queue (Transactional Outbox 异步发件箱)   │  │
│  └─────────────────────────────┬─────────────────────────────┘  │
│                                │ Outbox Worker 消费 / 向量化   │
│                                ▼                                │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │            Qdrant 向量检索加速层 (Retrieval Index)        │  │
│  │   - collection: ai_memory (1024 维 Cosine)                │  │
│  │   - payload: memory_id, type, status, subject, confidence │  │
│  │   - Blue-Green Alias 机制保障热切换零瞬断                 │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 关系型数据库 Schema 设计 (DDL)

```sql
-- 1. 会话表与原始消息表（证据层 Evidence）
CREATE TABLE IF NOT EXISTS raw_sessions (
    session_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    project_id TEXT DEFAULT 'general',
    started_at INTEGER NOT NULL,
    ended_at INTEGER,
    status TEXT DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS raw_messages (
    message_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES raw_sessions(session_id),
    role TEXT NOT NULL CHECK(role IN ('user', 'assistant', 'system')),
    content TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_msg_session ON raw_messages(session_id);
CREATE INDEX IF NOT EXISTS idx_msg_hash ON raw_messages(content_hash);

-- 2. 结构化记忆主表（知识层 Memory SSOT）
CREATE TABLE IF NOT EXISTS memories (
    memory_id TEXT PRIMARY KEY,          -- e.g. mem_20260927_xxxxxx
    type TEXT NOT NULL CHECK(type IN ('fact', 'preference', 'decision', 'task', 'episode', 'relation')),
    subject TEXT NOT NULL,               -- 实体主语 (如 'user', 'aep_chain', 'sub2api')
    predicate TEXT NOT NULL,             -- 谓词关系 (如 'likes', 'uses_architecture', 'binds_port')
    object TEXT,                         -- 宾语内容
    content TEXT NOT NULL,               -- 原子化纯净陈述句 (作为向量化输入)
    
    valid_from INTEGER NOT NULL,         -- 生效时间戳
    valid_to INTEGER,                    -- 失效时间戳 (NULL 表示永久有效)
    
    confidence REAL NOT NULL DEFAULT 0.8,-- 置信度 [0.0 - 1.0]
    importance REAL NOT NULL DEFAULT 0.5,-- 重要度 [0.0 - 1.0]
    mention_count INTEGER NOT NULL DEFAULT 1, -- 提及频次
    
    status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('candidate', 'active', 'superseded', 'archived', 'deleted')),
    conflict_policy TEXT NOT NULL DEFAULT 'coexist' CHECK(conflict_policy IN ('overwrite', 'coexist', 'state_machine', 'immutable')),
    superseded_by TEXT REFERENCES memories(memory_id),
    
    project_id TEXT NOT NULL DEFAULT 'general',
    scope TEXT NOT NULL DEFAULT 'global' CHECK(scope IN ('global', 'project', 'agent', 'session')),
    source_agent TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_mem_lookup ON memories(project_id, type, status);
CREATE INDEX IF NOT EXISTS idx_mem_subject ON memories(subject, predicate);

-- 3. 记忆与证据溯源多对多关联表
CREATE TABLE IF NOT EXISTS memory_evidence (
    memory_id TEXT NOT NULL REFERENCES memories(memory_id),
    message_id TEXT NOT NULL REFERENCES raw_messages(message_id),
    session_id TEXT NOT NULL,
    linked_at INTEGER NOT NULL,
    PRIMARY KEY (memory_id, message_id)
);

-- 4. 采纳审计建议：Transactional Outbox 异步同步队列表 (保障跨机高可用与最终一致性)
CREATE TABLE IF NOT EXISTS qdrant_sync_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    memory_id TEXT NOT NULL,
    op_type TEXT NOT NULL CHECK(op_type IN ('upsert', 'delete', 'update_payload')),
    payload_snapshot TEXT,               -- JSON 序列化快照
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'synced', 'failed')),
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sync_status ON qdrant_sync_queue(status, retry_count);
```

---

## 四、 核心治理算法逻辑（吸收审计改进项）

### 4.1 吸收建议：状态机上下文与 DAG 定义域（State Machine Registry）
针对 `conflict_policy = 'state_machine'` 的记忆，引擎内置并可扩展合法状态流转拓扑（DAG）。
- 注册表配置示例：
  ```python
  STATE_TRANSITIONS = {
      "task_lifecycle": {
          "pending": ["in_progress", "cancelled"],
          "in_progress": ["completed", "failed", "blocked"],
          "blocked": ["in_progress", "cancelled"],
          "completed": [],
          "failed": ["in_progress"]
      }
  }
  ```
- **流转仲裁规则**：
  若新状态不属于当前活跃记忆的前置合法后继，拒绝自动覆盖并打上标记；合法迁移时，原子更新前序为 `superseded`，新节点生效。

### 4.2 吸收建议：混合仲裁防语义漂移（Hybrid Merge Arbitration）
彻底废除“单靠 0.88 Cosine 相似度判同”的高危逻辑，改为两级严格混合仲裁：
1. **第一层（结构化三元组精确仲裁）**：
   - 检查 `subject` 与 `predicate`：
     - 若 `object` 完全相同或规范化归一化后一致 $\rightarrow$ **100% 确定为同义合并（Merge）**，执行 `mention_count + 1` 与 `confidence + 0.05`；
     - 若 `object` 发生变化或互斥 $\rightarrow$ **强制禁止 Merge**，自动流转至 `conflict_policy`（执行覆盖或版本演进）。
2. **第二层（向量相似度 + NLI 反义校验）**：
   - 当三元组结构不完全匹配但向量余弦相似度 $\ge 0.88$ 时：
   - 执行轻量规则过滤（否定词探测：`不/未/严禁/禁用/开启`），确认句子逻辑方向一致（蕴含）方可合并；若探测到反义或矛盾，直接作为新冲突分支处理。

### 4.3 复合检索重排公式（Composite Retrieval Ranking）
当 Agent 发起查询时，Qdrant 负责召回候选 Top-K（**硬性锁定 $K \le 20$，前置硬过滤 `status == 'active'`**），引擎层在 0.3ms 内完成复合数学重排：
$$\text{FinalScore} = 0.45 \cdot \text{VectorSim} + 0.20 \cdot \text{Importance} + 0.15 \cdot \text{Confidence} + 0.10 \cdot \text{Recency} + 0.10 \cdot \text{TypeWeight}$$
- 时间半衰期衰减：$\text{Recency} = \exp(-\lambda \cdot \Delta t)$；
- 确保从 Qdrant 到 UDS 最终返回给 Agent 的全流程时延严格控制在 **< 40ms** 预算内。

---

## 五、 现存 1,986 条历史记忆离线提纯合并方案（吸收审计改进项）

现存 Qdrant 的 1,986 条数据已完成冷备份，接下来按 4 阶段进行全量提纯与平滑治理：

### 阶段 1：数据分类与合成证据挂载（Synthetic Evidence Ingest）
- 针对现存数据缺少规范消息指针的问题，在 SQLite 中预植虚拟归档会话：
  - `session_id = 'legacy_migration_20260927'`，`agent_id = 'system_migrator'`；
  - 将原 1,986 条原始点位作为历史原始消息写入 `raw_messages`，赋予唯一 `message_id`，为后续提纯建立坚不可摧的溯源外键链条。

### 阶段 2：原子知识抽取（LLM Batch Refining）
- 编写离线批处理脚本 `scripts/migrate_refine_v2.py`；
- 使用本地大模型或 Sub2API 批量将冗长问答（`【用户指令】...【结论】...`）提炼为标准 JSON Schema：
  ```json
  {
    "type": "decision",
    "subject": "aep_tsa",
    "predicate": "activation_policy",
    "object": "dual_track_unique_active",
    "content": "AEP-TSA采用双轨唯一激活策略，国密SM2与国际ECDSA各保持唯一激活实例，操作列仅显示激活。",
    "importance": 0.85,
    "confidence": 0.95
  }
  ```

### 阶段 3：确定性合并与冲突消解（Engine Ingestion）
- 提纯后的候选知识依次喂入 AMR 引擎，走混合合并（Hybrid Merge）与冲突逻辑；
- 预计将 1,986 条原始碎片压缩、归并为 **300 ~ 400 条高质量核心知识卡片**；
- 完整写入本地 SQLite `memories` 表，并在 `memory_evidence` 中关联对应的原始迁移消息 ID。

### 阶段 4：Qdrant 蓝绿集合热切换（Blue-Green Aliasing）
1. 创建全新的 Qdrant 集合：`ai_memory_v2`（配置 1024 维 Cosine 与 Payload 索引）；
2. Outbox Worker 将提纯后的 300~400 条记忆推流向量化并批量写入 `ai_memory_v2`；
3. 执行端到端真实语义验证；
4. 调用 Qdrant `update_aliases` API 执行原子无缝切换：
   - 将现有别名 `ai_memory` 指向 `ai_memory_v2`；
   - 原旧集合重命名为 `ai_memory_legacy_frozen` 并保留 7 天作为热备，业务 Agent **零闪断、零感知**。

---

## 六、 各 AI 助手接口契约适配（Contract Adapter）

为防范字段变更导致现有插件抛出异常，AMR 的 UDS / JSON-RPC 服务层增加**双向向前兼容适配层（Backward-Compatible Adapter）**：
- **返回结构兼容**：
  返回的每个命中项中，既保留旧插件消费的扁平字段（`content`, `score`, `project_id`, `memory_id`, `session_id`, `source_message_ids`），又携带最新结构化实体（`subject`, `predicate`, `object`, `confidence`, `status`, `version`）；
- **Agent 零改动平滑收益**：
  - **Hermes**：无需修改代码，自动在预取中获得字字珠玑的短陈述句，大幅节约 Context Window；
  - **OpenClaw (小龙虾)**：`before_prompt_build` 获得更高信噪比的背景，`agent_end` 继续推流原始会话；
  - **OpenCode**：编程时精准召回代码规范与架构铁律，避免被过时 bug 修复碎片误导；
  - **DSH (Web)**：继续享受透明的 `systemPrompt.context` 自动注入。
