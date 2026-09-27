# AI Memory Runtime (AMR) 认知记忆引擎架构升级与离线提纯实施规格（v2.2 终版）

> **主架构冻结**：SQLite SSOT、Evidence/Memory 分层、LLM Extract、Deterministic Governance、Merge/Conflict、Transactional Outbox、Qdrant Projection、Composite Retrieval。  
> **核心原则**：LLM 负责理解与候选提取，确定性 Memory Engine 负责 Schema 校验、权限、冲突、版本、合并与审计。Qdrant 永远只是只读检索索引（Retrieval Index Projection），绝非真相源。

---

## 一、 现状与核心痛点（Context & Problem）

1. **Qdrant 越权充当 SSOT**：记忆直接写在向量库，缺乏强约束的关系型真相源，多字段事务、版本回滚、级联清理几乎不可做。
2. **“对话证据”与“长期记忆”混同**：长段问答、工具超限报错、中间推理过程直接入库，严重污染向量空间并稀释语义。
3. **缺乏确定性记忆生命周期治理**：
   - 冲突处理一刀切（仅靠时间覆盖），破坏了“共存偏好”、“状态机单向流转”和“不可篡改事件”；
   - 缺乏证据累积合并机制，复述刷高置信度，相似度判同存在反义词/否定句语义漂移漏洞；
   - 异地网络分区（Unraid Docker）下缺乏 Outbox 故障自愈，且多 Agent 并发写 SQLite 面临锁冲突风险。

---

## 二、 核心存储与 Schema 规约（DDL）

### 2.1 SQLite 核心表（SSOT 真相源）

严格定死 4 张核心业务表 + 2 张工程治理表（支持原子事务与幂等回溯）：

```sql
-- 1. 原始会话表
CREATE TABLE IF NOT EXISTS raw_sessions (
    session_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    project_id TEXT DEFAULT 'general',
    started_at INTEGER NOT NULL,
    ended_at INTEGER,
    status TEXT DEFAULT 'active'
);

-- 2. 原始消息证据表（支持证据分类 source_type，吸收建议 P1-15）
CREATE TABLE IF NOT EXISTS raw_messages (
    message_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES raw_sessions(session_id),
    role TEXT NOT NULL CHECK(role IN ('user', 'assistant', 'system')),
    content TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    source_type TEXT NOT NULL DEFAULT 'user_message' 
        CHECK(source_type IN ('user_message', 'assistant_message', 'tool_output', 'system', 'legacy_memory', 'imported')),
    is_synthetic INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_msg_session ON raw_messages(session_id);
CREATE INDEX IF NOT EXISTS idx_msg_hash ON raw_messages(content_hash);

-- 3. 记忆主表（知识层 Memory SSOT，吸收建议 P0-1、P0-8、P1-10、P1-14）
CREATE TABLE IF NOT EXISTS memories (
    memory_id TEXT PRIMARY KEY,                       -- e.g. mem_20260927_xxxxxx
    qdrant_point_id TEXT NOT NULL UNIQUE,             -- UUIDv5(NAMESPACE_URL, memory_id)，保证与 Qdrant 严格一一对应 (P0-1)
    
    type TEXT NOT NULL CHECK(type IN ('fact', 'preference', 'decision', 'task', 'episode', 'relation')),
    conflict_policy TEXT NOT NULL DEFAULT 'coexist' 
        CHECK(conflict_policy IN ('overwrite', 'coexist', 'state_machine', 'immutable')), -- 与 type 解耦 (P1-10)
        
    subject TEXT NOT NULL,                            -- 实体归一化后主语 (如 'user', 'aep_tsa', 'sub2api') (P1-11)
    predicate TEXT NOT NULL,                          -- 谓词关系 (如 'likes', 'binds_port', 'status_is')
    object TEXT,                                      -- 宾语内容
    content TEXT NOT NULL,                            -- 原子化纯净陈述句 (用于向量化)
    
    valid_from INTEGER NOT NULL,                      -- 生效时间戳
    valid_to INTEGER,                                 -- 失效时间戳 (NULL 表示当前持续有效)
    validity_type TEXT NOT NULL DEFAULT 'open_ended' 
        CHECK(validity_type IN ('open_ended', 'bounded', 'unknown')), -- 消除 NULL 语义混淆 (P1-14)
    
    confidence REAL NOT NULL DEFAULT 0.8,             -- 贝叶斯证据累加置信度 [0.0 - 1.0] (P0-3)
    importance REAL NOT NULL DEFAULT 0.5,             -- 重要度打分 [0.0 - 1.0]
    mention_count INTEGER NOT NULL DEFAULT 1,         -- 独立提及频次
    
    status TEXT NOT NULL DEFAULT 'candidate' 
        CHECK(status IN ('candidate', 'active', 'superseded', 'archived', 'deleted')),
    superseded_by TEXT REFERENCES memories(memory_id),
    
    project_id TEXT NOT NULL DEFAULT 'general',
    scope TEXT NOT NULL DEFAULT 'global' CHECK(scope IN ('global', 'project', 'agent', 'session')),
    source_agent TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    
    deleted_at INTEGER,                               -- 遗忘审计字段 (P0-8)
    deleted_by TEXT,
    deletion_reason TEXT,
    
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_mem_lookup ON memories(project_id, type, status);
CREATE INDEX IF NOT EXISTS idx_mem_subject ON memories(subject, predicate);
CREATE INDEX IF NOT EXISTS idx_mem_point ON memories(qdrant_point_id);

-- 4. 记忆与证据溯源多对多关联表
CREATE TABLE IF NOT EXISTS memory_evidence (
    memory_id TEXT NOT NULL REFERENCES memories(memory_id),
    message_id TEXT NOT NULL REFERENCES raw_messages(message_id),
    session_id TEXT NOT NULL,
    evidence_strength REAL NOT NULL DEFAULT 0.5,      -- 证据强度 (0.1 ~ 0.8)
    linked_at INTEGER NOT NULL,
    PRIMARY KEY (memory_id, message_id)
);

-- 5. Transactional Outbox 异步同步发件箱队列 (按 memory_id 顺序消费，故障自愈，吸收建议 P0-16)
CREATE TABLE IF NOT EXISTS qdrant_sync_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    memory_id TEXT NOT NULL,
    qdrant_point_id TEXT NOT NULL,
    op_type TEXT NOT NULL CHECK(op_type IN ('upsert', 'delete', 'update_payload')),
    payload_snapshot TEXT,                            -- 序列化快照
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'synced', 'failed')),
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sync_status ON qdrant_sync_queue(status, retry_count);
CREATE INDEX IF NOT EXISTS idx_sync_order ON qdrant_sync_queue(memory_id, id);

-- 6. 治理与生命周期审计日志表 (吸收建议 P1-19)
CREATE TABLE IF NOT EXISTS memory_audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    memory_id TEXT NOT NULL,
    action TEXT NOT NULL CHECK(action IN ('create', 'promote', 'merge', 'update', 'supersede', 'archive', 'delete')),
    operator TEXT NOT NULL,                           -- 'engine', 'agent_xxx', 'user'
    detail TEXT,                                      -- JSON 审计上下文
    timestamp INTEGER NOT NULL
);
```

---

## 三、 核心治理算法与五大核心 API

AMR 明确定义并收敛为 **5 个核心 API 契约**：

### 1. `extract` (LLM 结构化候选抽取)
- **定位**：LLM 仅负责从对话流抽取候选三元组与陈述句，严禁 LLM 决定记忆持久化。
- **流程**：
  1. 调用 LLM（`temperature=0`, JSON Schema 约束）；
  2. 实体归一化服务（P1-11）：别名表映射（如“美式”、“Americano” $\rightarrow$ `americano_coffee`；“AEP-TSA” $\rightarrow$ `aep_tsa`）；
  3. 输出 `CandidateMemory`（状态初始化为 `status: 'candidate'`）。
- **晋升规则（Candidate $\rightarrow$ Confirmed，吸收建议 P1-9）**：
  必须满足 `confidence >= 0.90 AND mention_count >= 2 AND no_active_conflict`，或用户指令明确要求“记住”，否则保留在 candidate 队列，杜绝单次幻觉污染。

### 2. `merge` (多级混合防语义漂移与证据累积，吸收建议 P0-2、P0-3、P1-12)
- **废除“仅靠 0.88 Cosine 判同”与简单加法**，执行 3 级严密仲裁：
  - **Level 1（三元组结构化强仲裁）**：
    - `subject` 与 `predicate` 完全匹配：
      - 若 `object` 完全相同 $\rightarrow$ **仅作为 Merge Candidate（P0-2）**，进入 Evidence Validation；
      - 若 `object` 互斥或改变 $\rightarrow$ **硬拦截禁止合并**，自动分流至 `conflict_policy`。
  - **Level 2（否定词与反义对抗过滤）**：
    - 针对候选进行否定词探测（`不/未/严禁/禁用/开启/停止`），逻辑相反者判定为冲突，禁止合并。
  - **Level 3（贝叶斯证据累加 Evidence Accrual，P0-3）**：
    - 严禁简单 `+0.05` 导致复述刷满置信度；
    - 仅对来自不同 `session_id` 或明确独立来源的证据累加：
      $$\text{confidence}_{new} = 1 - (1 - \text{confidence}_{old}) \times (1 - \text{evidence\_strength})$$
    - 更新 `mention_count += 1`，追加 `memory_evidence` 映射。

### 3. `update` (状态机与冲突流转，吸收建议 P1-10)
- 独立解耦 `type` 与 `conflict_policy`：
  - `fact` $\rightarrow$ 默认 `overwrite` 或 `coexist`；
  - `preference` $\rightarrow$ 默认 `coexist`（但允许指定 `overwrite` 显式纠偏）；
  - `decision` $\rightarrow$ 默认 `overwrite`（版本递增，旧条目打上 `valid_to` 和 `superseded_by`）；
  - `task` $\rightarrow$ 默认 `state_machine`（基于合法 DAG 校验状态迁移，非前置合法状态直接拒绝）；
  - `episode` $\rightarrow$ 默认 `immutable`（不可变历史事件，永不覆盖）。

### 4. `retrieve` (写后读一致性与复合重排，吸收建议 P0-4、P0-5、P1-13、P1-20)
- **服务端权限硬过滤（P0-5）**：
  - 客户端传参不可信，服务端强制追加 Payload 过滤：
    `filter = {"must": [{"key": "status", "match": {"value": "active"}}, {"key": "project_id", "match": {"any": ["general", current_project_id]}}]}`。
- **写后读一致性（Read-Your-Own-Writes，P0-4）**：
  - 检索执行时，首先在 SQLite 查询当前会话最近 60s 内未同步到 Qdrant 的 `pending` 记忆；
  - 与 Qdrant 召回的 Top-20 候选点合并；
- **回表状态校验**：
  - 召回的点强制回 SQLite 校验当前最新 `status == 'active'`，杜绝 Qdrant 索引延迟导致的脏数据召回；
- **复合重排（Composite Ranking）**：
  $$\text{FinalScore} = w_s \cdot \text{VectorSim} + w_i \cdot \text{Importance} + w_c \cdot \text{Confidence} + w_r \cdot \text{Recency} + w_t \cdot \text{TypeWeight}$$
  - API 向后兼容（P1-20）：返回对象中同时透出 `final_score` 与 `vector_score`，防止旧插件误判。

### 5. `forget` (合规安全擦除与审计，吸收建议 P0-8)
- 用户或 Agent 指令要求遗忘/删除时：
  1. SQLite 事务中更新 `status = 'deleted'`，记录 `deleted_at`, `deleted_by`, `deletion_reason`；
  2. 向 `qdrant_sync_queue` 插入 `op_type = 'delete'`；
  3. Worker 异步调用 Qdrant API 真正删除该 `qdrant_point_id`；
  4. 写入 `memory_audit_log` 完成合规记录。

---

## 四、 架构工程保障与硬隔离（P0/P1）

1. **Qdrant Point ID 映射锁定（P0-1）**：
   - 彻底废除使用业务 `memory_id` 充当 point ID；
   - 统一采用 Python `uuid.uuid5(uuid.NAMESPACE_URL, memory_id)`，保证全局唯一、稳定确定性且 100% 契合 Qdrant UUID 规范。
2. **SQLite 单写串行化（Concurrency Guard，P0-6）**：
   - 严禁任何 Agent 进程直连 SQLite 文件；
   - 所有读写严格收敛至 AMR UDS 守护进程（单写线程串行化，WAL 模式开启，`busy_timeout = 5000ms`）。
3. **Outbox 队列顺序保障与监控（P0-16）**：
   - 同一 `memory_id` 严格按自增 `id` 顺序消费，杜绝乱序导致 Qdrant 状态回退；
   - 重试上限 5 次，超限转入死信队列并记录告警。
4. **算力与资源开销解耦（P1-18）**：
   - Tesla P4 GPU 在线推理保持纯轻量（仅 BGE-M3 驻留，零显存占用守护，时延稳定 < 35ms）；
   - 离线提纯等密集 LLM 任务统一走外部 Sub2API 调度，绝不挤占宿主机有限的 8GB 显存。

---

## 五、 全量 1,986 条历史记忆离线提纯合并路线图（吸收建议 P0-7、P1-17）

采用 **“先 100 条小步跑通闭环，再全量 1,986 条蓝绿切换”** 的稳健演进策略：

### 第一阶段：原型验证（100 条端到端闭环验证）
1. 从 `ai_memory_backup_20260927.json` 抽取前 100 条真实样本；
2. 注入合成证据标记 `is_synthetic = 1`, `source_type = 'legacy_memory'`，沉淀进 SQLite；
3. 执行 LLM 提纯 $\rightarrow$ 实体归一化 $\rightarrow$ 混合合并与冲突仲裁；
4. 深度验证：比对提纯前后的信噪比，断言证据累加与状态流转；验证回滚脚本 100% 可用。

### 第二阶段：全量提纯与蓝绿无缝热切换（1,986 条全量）
1. 目标设定为“减冗余+保覆盖+可追溯+降噪”（P1-17），episode 记忆不强制合并；
2. 在 Qdrant 创建全新集合 `ai_memory_v2`（1024 维 Cosine）；
3. Outbox Worker 将提纯后的精炼知识批量向量化写入 `ai_memory_v2`；
4. 执行端到端检索校验（召回率、准确率断言全绿）；
5. **原子别名切换（Blue-Green Switch，P0-7）**：
   切换前暂停外部写入，调用 Qdrant `update_aliases`，将别名 `ai_memory` 瞬间切换到 `ai_memory_v2`；原集合保留 7 天作为冷备份。线上 Agent 零闪断、零感知。
