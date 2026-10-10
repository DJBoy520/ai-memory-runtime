# RFC-005: AMR 记忆操作系统 (MOS v3.0) 核心架构规范

- **版本**: v3.0.0-FINAL
- **所属系统**: AI Memory Runtime (AMR)
- **定位**: 跨 Agent 共享的**记忆操作系统 (Memory Operating System, MOS)**
- **核心原则**: AMR 主导调度，LLM 负责认知计算；Raw Memory 为不可变证据，SQLite 保存经过治理的事实 SSOT，Qdrant 仅作为可重建的检索投影。

---

## 一、 系统定位与主客体秩序 (AMR 主导，LLM 执行)

### 1. 彻底纠偏主客关系
- **严禁 LLM 作为整理流程的主控**：LLM 绝无自主决定何时搜索、何时停下、直接调用写操作的权限。
- **AMR 拥有唯一调度权**：由 AMR 内部的 `Cognitive Scheduler` 严格控制“何时唤醒 LLM、构造什么 Context Pack 喂给 LLM、如何校验并处理 LLM 返回的提案”。
- **LLM 权限定位**：LLM 仅是 AMR 调度的**无状态认知计算引擎（Cognitive Worker）**，只有提出候选提案（Memory Proposals）的权限，零写权限。

```text
                       AMR 记忆操作系统
                              │
                    Cognitive Scheduler (主控)
                              │
         ┌────────────────────┴────────────────────┐
         ▼                                         ▼
   近 24h 增量新会话                         Memory Context Pack
   (raw_messages 提取)                      (多维确定性宽召回)
         │                                         │
         └────────────────────┬────────────────────┘
                              ▼
                     LLM Distiller (计算引擎)
                              │
                              ▼
                       Memory Proposals
                              │
         ┌────────────────────┴────────────────────┐
         ▼                                         ▼
  Evidence Validator                       Coverage Validator
  (防编造: 三层匹配原文)                   (防漏: 检查 message_disposition)
         │                                         │
         └────────────────────┬────────────────────┘
                              ▼
                         Policy Gate
                    (动态熔断 + 白名单保护)
                              ▼
                    SQLite SSOT Transaction
                    (Chunky Commit, 200/批)
                              ▼
                    memory_projection_outbox
                              ▼
                      Qdrant Projection
```

---

## 二、 物理隔离与 Invariant-08 绝对落地（老板实操定案）

### 1. 物理隔离真相
- **Qdrant 唯一访问密钥保护**：Qdrant 向量数据库配置了唯一的强 API Key，**该 Key 仅物理部署在 AMR 守护进程配置中**（`~/.config/amr/config.yaml`，权限 `0600`）。
- **外部 Agent 物理切断**：Hermes、OpenClaw、OpenCode、DSH 本地配置中均**无权**持有 Qdrant API Key，从网络与凭据层面物理杜绝了任何外部直写或越权访问 Qdrant 的可能。
- **唯一合法通道**：所有对记忆的读取（`memory.search`）、写入（`session.ingest`）、更新及提纯，必须严格穿透 AMR 的 Unix Domain Socket（`/run/user/1000/qdrant-bge.sock`，权限 `0600`）或 MCP 代理桥。

### 2. 差额成因与 10 条漂移点位定案说明
- **实测现状**：当前 SQLite `memories` 为 432 条，Qdrant `ai_memory` 为 442 点位，差异恰好为 **10 条**。
- **查实原因**：这 10 条点位正是今天下午（19:10~19:40）我们在本会话中讨论“定时任务、规则代码、RFC 审计建议”时，由于网关长连接尚未触发会话关闭落盘，在 Qdrant 侧产生的会话活跃投影。
- **处置原则**：对账脚本扫描时，识别为未落盘会话，正常等待会话空闲或由对账流程进行优雅补登，绝非越权写入。

---

## 三、 宪法级十三条一致性不变量 (Invariants 01~13)

- **Invariant-01 (SSOT 唯一性)**: SQLite 是全系统唯一不可变事实来源。一切状态流转、版本迭代与删除标记必须在 SQLite 事务中完成并固化。
- **Invariant-02 (投影无多余性)**: Qdrant 不得产生 SQLite 中不存在的点位；仅 `active` 与 `stale` 允许存在于主检索投影。
- **Invariant-03 (删除不可逆检索)**: SQLite 中状态为 `deleted` 的记忆，绝对不得出现在 Qdrant 的任何 active 检索投影中（强拒识保证）。
- **Invariant-04 (归档非毁灭性)**: `archived` 可从 Qdrant 下架，但绝对不得从 SQLite 消失，血缘与唤醒路径畅通。
- **Invariant-05 (标识符生命周期唯一性)**: `memory_id` 全局不可复用。记忆演化采用版本递增 (`version + 1`) 或取代 (`superseded_by`)。
- **Invariant-06 (治理全链路可审计可回滚)**: 任何自动化治理必须带 `curation_batch_id`、全量审计日志，且具备一键还原回滚能力。
- **Invariant-07 (无损灾备自愈性)**: 当 Qdrant 完全损坏或丢失时，仅凭 SQLite 可重建。重建时必须过滤 `status IN ('deleted', 'archived')`，且 SQLite 必须固化 Embedding Provenance。
- **Invariant-08 (单入口物理收敛)**: 所有记忆增删改查与整理，必须通过 AMR 的 UDS 或 MCP 接口。Qdrant API Key 仅限 AMR 持有，网络与物理隔离。
- **Invariant-09 (整理提案不可直接落地)**: LLM 整理输出仅为 `PROPOSED` 状态的候选，未经 Evidence/Coverage 验证器和 Policy Gate 裁决，绝对不得写入 SSOT。
- **Invariant-10 (输入窗口与影响范围分离)**: 24h 仅代表增量新会话扫描窗口，历史比对范围通过 Context Pack 机制跨越全周期，不设时间上限。
- **Invariant-11 (记忆演化链条不可覆盖)**: 记忆事实的更迭必须记录历史版本（`previous_version_id`, `root_memory_id`），严禁原地无痕覆写破坏演进轨迹。
- **Invariant-12 (原始证据永不抹除)**: `raw_sessions` 与 `raw_messages` 无论经历何种提纯，永远保留，只增不删。
- **Invariant-13 (双验证器诚实边界)**: 双验证器负责结构性与完整性保障，最终语义兜底依托 14 天 Dry-Run 人工抽检与 Raw 层可逆性。

---

## 四、 Memory Context Pack 机制 (多维召回代替 LLM 自主搜索)

严禁 LLM 在提纯时自主调 `memory_search` 盲目检索。由 AMR 的 `Context Builder` 在内存中确定性组装 `Memory Context Pack`：

```text
Memory Context Pack (只读上下文，受控输入)
├── 向量 Top-20 (基于新会话核心主题的密集语义召回)
├── 同 project_id 全部活跃记忆
├── 同 entity (匹配 subject / object 实体名)
├── 最近 7 天内发生过变更的记忆
├── 历史已有的 SUPERSEDE 链条记忆
└── 同 tenant 隔离边界
```
- **规模控制**：去重重排后截断在 30 条以内，作为只读背景直接注入 LLM System Prompt。

---

## 五、 `memory.curate` 协议定稿与双验证器

### 1. 结构化提案契约 (LLM 填表返回)
```json
{
  "proposal_id": "prop_20260930_023000_xxxx",
  "batch_id": "recon_20260930_023000",
  "session_window": {
    "session_ids": ["sess_001"],
    "start_msg_id": "raw_msg_101",
    "end_msg_id": "raw_msg_150"
  },
  "operation": "EXTRACT | MERGE | UPDATE | SUPERSEDE | LINK | ARCHIVE",
  "target_memory_id": "mem_xxx 或 null",
  "subject": "实体名",
  "predicate": "关系/行为",
  "object": "目标/属性",
  "content": "高密度事实（必须一字不差保留数值、端口、路径、命令、失败教训）",
  "evidence_source_ids": ["raw_msg_101", "raw_msg_102"],
  "extracted_spans": [
    { "message_id": "raw_msg_101", "span": "原文中的原句片段" }
  ],
  "message_disposition": [
    { "message_id": "raw_msg_101", "disposition": "EXTRACTED" },
    { "message_id": "raw_msg_102", "disposition": "EXTRACTED" },
    { "message_id": "raw_msg_103", "disposition": "DISCARDED", "reason": "纯客套寒暄" },
    { "message_id": "raw_msg_104", "disposition": "UNPROCESSED" }
  ],
  "rationale": "提炼理由与演化逻辑说明",
  "proposed_relation": {
    "type": "UNCHANGED | UPDATE | SUPERSEDE | CONFLICT | MERGE | NEW",
    "related_memory_id": "mem_xxx 或 null"
  },
  "self_assessed_confidence": 0.85
}
```

### 2. 双验证器（双保险门禁）
- **Evidence Validator (防编造/防幻觉)**：
  - 采用三层匹配：空白折叠/Unicode NFKC 归一化 $\rightarrow$ 精确子串包含 $\rightarrow$ Token 级 Jaccard $\ge 0.95$；
  - 凡是 `extracted_spans` 无法在原始会话中被证明的，整条提案标记为 `INVALID_EVIDENCE` 并硬性丢弃。
- **Coverage Validator (防遗漏/防沉默丢失)**：
  - 检查约束：$\text{EXTRACTED} + \text{DISCARDED} + \text{UNPROCESSED} == \text{当前输入窗口全部原始消息}$；
  - 任何未被 LLM 声明处置的消息自动打上 `UNPROCESSED`，保留在原始消息中，绝不删除，记录在审计报表中。

---

## 六、 SQLite Schema 终极演进 (Memory Evolution & Versioning)

在 `sessions.db` 中固化版本演进与多投影支持：

```sql
-- 1. memories 表增加版本演化与保护维度
ALTER TABLE memories ADD COLUMN version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE memories ADD COLUMN previous_version_id TEXT;
ALTER TABLE memories ADD COLUMN root_memory_id TEXT;
ALTER TABLE memories ADD COLUMN superseded_by TEXT;
ALTER TABLE memories ADD COLUMN superseded_at INTEGER;
ALTER TABLE memories ADD COLUMN protection_level TEXT NOT NULL DEFAULT 'NONE'; -- NONE, MANUAL, LESSON, SYSTEM

-- 2. 治理候选表 (隔离存储，支撑人工抽检与 Dry-Run)
CREATE TABLE IF NOT EXISTS curation_candidates (
    candidate_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    memory_id TEXT,
    session_window_json TEXT NOT NULL,
    operation TEXT NOT NULL, -- EXTRACT, MERGE, UPDATE, SUPERSEDE, LINK, ARCHIVE
    current_status TEXT,
    proposed_status TEXT,
    subject TEXT,
    predicate TEXT,
    object TEXT,
    content TEXT NOT NULL,
    evidence_source_ids TEXT NOT NULL, -- JSON Array
    extracted_spans TEXT NOT NULL,      -- JSON Array
    message_disposition TEXT NOT NULL,  -- JSON Array
    rationale TEXT,
    proposed_relation_json TEXT,
    prompt_version TEXT NOT NULL,
    model_name TEXT NOT NULL,
    llm_confidence REAL,
    state TEXT NOT NULL DEFAULT 'PROPOSED', -- PROPOSED, APPROVED, REJECTED, APPLIED
    rejection_reason TEXT,
    created_at INTEGER NOT NULL,
    processed_at INTEGER
);

-- 3. 抽象投影发件箱 (解耦 Qdrant，幂等防重)
CREATE TABLE IF NOT EXISTS memory_projection_outbox (
    id TEXT PRIMARY KEY,
    memory_id TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    projection_type TEXT NOT NULL DEFAULT 'qdrant_main',
    op_type TEXT NOT NULL, -- upsert, delete, update_payload
    payload_snapshot TEXT,
    idempotency_key TEXT UNIQUE, -- memory_id + version + projection_type + op_type
    status TEXT NOT NULL DEFAULT 'PENDING', -- PENDING, PROCESSING, COMPLETED, DEAD_LETTER
    retry_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at INTEGER DEFAULT 0,
    dead_letter_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
```

---

## 七、 动态熔断算子与安全策略

Policy Gate 单批允许流转变更的最大数量限制算子：
$$\text{MaxAllowedChanges} = \min(5000, \max(20, 0.03 \times \text{TotalPoints}))$$
- 当数据量为 432 条时，允许最大变动为 **20 条** (占比约 4.6%)，彻底封死了早期小数据量下因模型幻觉导致的系统大震荡；
- 超出阈值时，自动触发整批熔断，拒绝写入并发送系统告警。

---

## 八、 落地节奏与实施规约

1. **第一阶段：冻结与代码改造（AMR 内部落地）**
   - 在 AMR 源码建立 `src/cognitive/` 模块（实现 `scheduler.py`, `context_builder.py`, `evidence_validator.py`, `coverage_validator.py`）；
   - 在 `session_store.py` 补充 Versioning 演进字段与 DDL 迁移；
2. **第二阶段：14 天 Dry-Run 观测期**
   - 凌晨 02:30 定时任务触发：RuleEngine 先行去噪 $\rightarrow$ Scheduler 组装 Context Pack 唤醒 LLM $\rightarrow$ 双验证器校验 $\rightarrow$ 写入 `curation_candidates` 报表；
   - 期间完全不碰正式 `memories` SSOT，每日出具抽检报表；
3. **第三阶段：正式转正**
   - 人工抽检质量通过后，放开 Policy Gate 写入，进入生产常态治理。
