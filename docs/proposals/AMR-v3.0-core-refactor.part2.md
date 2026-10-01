## 2. Q1｜标准 MCP 工具定义（Tools Schema）

### 2.1 设计原则（先立规矩，再列清单）

1. **工具名 = RPC 方法名的点号换下划线**（`memory_search` ↔ `memory.search`）。映射表零歧义、零脑内翻译，四个 Agent（Hermes / OpenClaw / OpenCode / DSH）插件共用同一份 `tools.py` 与同一份映射。
2. **写路径按权限四权分离**，一个工具只干一件事：
   - 内容变更权 → `memory_update`（必带 `expected_version`，必增版本、必重算向量）
   - 元数据变更权 → `memory_set_metadata`（只改 `type`/`status`，**不增版本、不重算向量**）
   - 冲突打标权 → `memory_conflict`（唯一能把双方同时置 `CONFLICT` 的入口）
   - 销毁权 → `memory_delete`（软删除，带 `reason`）
   废止 `memory_record`（语义模糊，无法表达版本锁）与 `memory_update_status`（会把"改状态"和"改内容"混为一谈）。
3. **身份不可伪造**：`created_by_agent` / `updated_by_agent` 由 **MCP Bridge 硬编码注入**，工具 schema 中**不暴露**该参数。沿用 `src/interfaces/mcp/bridge.py:110-114` 的既有注入位点，扩展为对全部写工具的注入。
4. **返回值最小化**：写工具只回**链路续接所需的三个字段**（`memory_id` / `version` / `status`），不回灌整个 payload。回灌全文是 token 浪费与二次幻觉源。
5. **错误必须可自愈**：错误体带 `retryable` 与 `details.expected_version/current_version`，Agent 能据此重新 `get` 并重试，无需人工介入。
6. **默认安全**：`memory_search` 默认只搜 `ACTIVE`；要搜其他状态必须显式给 `status_filter`。写工具默认 `project_id = "global"`、`type = "general"`。

### 2.2 工具清单（10 个）

| # | 工具名 | 权限 | RPC 方法 | 说明 |
|---|---|---|---|---|
| 1 | `memory_search` | R | `memory.search` | 语义检索，默认只搜 ACTIVE |
| 2 | `memory_get` | R | `memory.get` | 按 ID 精取（读 SQLite SSOT，强 RYOW） |
| 3 | `memory_history` | R | `memory.history` | 版本与状态时间线 |
| 4 | `memory_create` | W | `memory.create` | 新建记忆（替代 `memory_record`） |
| 5 | `memory_update` | W | `memory.update` | 内容更新，版本锁 |
| 6 | `memory_set_metadata` | W | `memory.set_metadata` | type/status 变更，不增版本 |
| 7 | `memory_conflict` | W | `memory.conflict` | 声明冲突，双向原子打标 |
| 8 | `memory_delete` | W | `memory.delete` | 软删除 |
| 9 | `memory_reconcile` | W（受限） | `memory.reconcile` | 触发/查询 TEMPORARY 整理提案 |
| 10 | `session_ingest` | W | `session.ingest` | 原始会话流水幂等摄取（原名 `memory_ingest_session`） |

**过渡期**：`memory_record` → `memory_create`、`memory_update_status` → `memory_set_metadata`、`memory_ingest_session` → `session_ingest` 保留 **1 个 release** 的别名，`tools/list` 中标记 `deprecated: true`，日志打 WARN 计数，release 结束后硬删除。

### 2.3 逐个工具 Schema

#### 2.3.1 `memory_search`

```json
{
  "name": "memory_search",
  "description": "Semantic search over long-term memories. Searches ACTIVE memories by default; pass status_filter to include pending/conflicting/historical records. Results are eventually consistent (Qdrant projection).",
  "inputSchema": {
    "type": "object",
    "properties": {
      "query":        { "type": "string", "minLength": 1, "description": "Natural-language query." },
      "project_id":   { "type": "string", "description": "Restrict to one project. Omit for global scope." },
      "type":         { "type": "string", "description": "Namespace/name filter, e.g. 'amr.decision'. Prefix match is allowed ('amr.*')." },
      "status_filter":{
        "type": "array",
        "items": { "type": "string", "enum": ["ACTIVE","PENDING_VERIFY","CONFLICT","HISTORICAL","TEMPORARY","DELETED"] },
        "description": "Defaults to ['ACTIVE']. Explicitly opt in to other lifecycle states."
      },
      "collections":  { "type": "array", "items": { "type": "string" },
                        "description": "Defaults to ['ai_memory']. Use ['all'] to search every standard collection." },
      "limit":        { "type": "integer", "minimum": 1, "maximum": 20, "default": 5 },
      "score_threshold": { "type": "number", "minimum": 0.0, "maximum": 1.0 },
      "created_by_agent": { "type": "string", "description": "Optional provenance filter." }
    },
    "required": ["query"],
    "additionalProperties": false
  }
}
```

返回（`content[0].text`，下同）：
```json
{ "ok": true,
  "data": { "results": [
      { "memory_id": "mem_20261001_a1b2c3", "version": 3, "content": "…",
        "project_id": "aep", "type": "amr.decision", "status": "ACTIVE",
        "created_by_agent": "opencode", "updated_by_agent": "hermes",
        "updated_at": 1790352000, "score": 0.8842 }
    ], "total": 1 },
  "meta": { "stale_projection": false, "outbox_depth": 3 } }
```
**要点**：结果**只回 9 字段投影 + `score`**。`meta.stale_projection` 标记该次检索是否有未落盘的挂起 Outbox，供 Agent 自行决定是否改用 `memory_get` 强读。

#### 2.3.2 `memory_get`

```json
{
  "name": "memory_get",
  "description": "Fetch one memory by ID from the authoritative SQLite store. Strongly consistent (read-your-own-writes guaranteed), unlike memory_search.",
  "inputSchema": {
    "type": "object",
    "properties": {
      "memory_id":     { "type": "string" },
      "include_refs":  { "type": "boolean", "default": false, "description": "Include resolved source_refs and conflicts_with." }
    },
    "required": ["memory_id"],
    "additionalProperties": false
  }
}
```
返回：11 字段 + 可选 `source_refs` / `conflicts_with`。**未找到**返回 `isError=true` 且 `error.code = -32004`（不是空对象——空对象是 Agent 幻觉的温床）。

#### 2.3.3 `memory_history`

```json
{
  "name": "memory_history",
  "description": "Read the version and status timeline of a memory, including change_reason and the agent that made each change.",
  "inputSchema": {
    "type": "object",
    "properties": {
      "memory_id": { "type": "string" },
      "limit":     { "type": "integer", "minimum": 1, "maximum": 100, "default": 20 }
    },
    "required": ["memory_id"],
    "additionalProperties": false
  }
}
```
返回：
```json
{ "ok": true, "data": { "memory_id": "mem_…", "current_version": 3,
  "entries": [
    { "version": 3, "status": "ACTIVE", "change_reason": "修正 SM4 Tag 长度",
      "changed_by_agent": "hermes", "changed_at": 1790352000, "kind": "content" },
    { "version": 2, "status": "PENDING_VERIFY", "change_reason": "初始提炼",
      "changed_by_agent": "opencode", "changed_at": 1790351000, "kind": "metadata" }
  ] } }
```
数据源：`memory_audit_log`（已存在，见 `src/core/session_store.py:227`）+ 版本快照表（需新增，见 §5.2）。
**不可省略**：这张时间线是"为什么这条记忆变成了现在这样"的唯一凭据，也是冲突仲裁的裁决依据。

#### 2.3.4 `memory_create`

```json
{
  "name": "memory_create",
  "description": "Create a new durable memory. The created_by_agent field is injected by the runtime and cannot be set by the caller.",
  "inputSchema": {
    "type": "object",
    "properties": {
      "content":       { "type": "string", "minLength": 1 },
      "project_id":    { "type": "string", "default": "global" },
      "type":          { "type": "string", "default": "general",
                         "description": "Namespace/name, e.g. 'amr.rule', 'aep.spec', 'general'." },
      "status":        { "type": "string",
                         "enum": ["ACTIVE","PENDING_VERIFY","TEMPORARY"],
                         "default": "ACTIVE",
                         "description": "Creation is restricted to these three states. CONFLICT/HISTORICAL/DELETED are reachable only via dedicated tools." },
      "source_refs":   { "type": "array",
                         "items": { "type": "object",
                           "required": ["kind","id"],
                           "properties": { "kind": {"type":"string","enum":["message","memory","file","url","task"]},
                                           "id": {"type":"string"},
                                           "session_id": {"type":"string"},
                                           "hash": {"type":"string"} } },
                         "description": "Weak references for provenance. No strong edges are stored." },
      "idempotency_key":{ "type": "string",
                          "description": "Optional client-generated key. Replaying the same key returns the original memory_id instead of creating a duplicate." }
    },
    "required": ["content"],
    "additionalProperties": false
  }
}
```
返回：`{"ok":true,"data":{"memory_id":"mem_…","version":1,"status":"ACTIVE","created_at":1790352000},"meta":{"deduplicated":false}}`

**要点**：
- `status` 白名单**只允许三个**。不允许建库即 `CONFLICT`（必须走 `memory_conflict`），不允许建库即 `HISTORICAL`/`DELETED`。
- `idempotency_key` 命中时返回原 ID 并置 `meta.deduplicated=true`——这是防 Agent 重试放大重复记忆的最后一道闸。
- 超长内容自动分块（沿用 `MAX_TOKEN_LIMIT=8192` / `OVERLAP_TOKENS=128`），但**分块只发生在投影层**：SSOT 里仍是一条 11 字段记忆，`chunks` 数由 `meta.chunks` 回报，`memory_id` 不裂变（废除现存的 `_chunk_N` 后缀 ID，它破坏 1:1 映射）。

#### 2.3.5 `memory_update`（内容变更，版本锁）

```json
{
  "name": "memory_update",
  "description": "Update the CONTENT of an existing memory. Requires the version you read; if another agent changed it first the call fails with VERSION_CONFLICT (-32001) and changes nothing. This bumps version and recomputes the embedding.",
  "inputSchema": {
    "type": "object",
    "properties": {
      "memory_id":        { "type": "string" },
      "content":          { "type": "string", "minLength": 1 },
      "expected_version": { "type": "integer", "minimum": 1,
                            "description": "The version you last read. Mandatory: protects against blind overwrite." },
      "change_reason":    { "type": "string", "minLength": 4,
                            "description": "Mandatory human-readable justification, stored in the audit trail." },
      "source_refs_add":  { "type": "array", "items": { "type": "object" },
                            "description": "Weak references appended to the existing source_refs (never replaces them)." }
    },
    "required": ["memory_id", "content", "expected_version", "change_reason"],
    "additionalProperties": false
  }
}
```
成功：`{"ok":true,"data":{"memory_id":"mem_…","previous_version":3,"version":4,"status":"ACTIVE","updated_at":1790352100},"meta":{"reembedded":true}}`

冲突：
```json
{ "ok": false,
  "error": { "code": -32001, "name": "VERSION_CONFLICT",
             "message": "expected_version 3 but current is 4",
             "retryable": true,
             "details": { "memory_id": "mem_…", "expected_version": 3, "current_version": 4,
                          "current_status": "ACTIVE" } },
  "hint": "Call memory_get, re-apply your change to the newer content, then retry with expected_version=4." }
```
**要点**：`change_reason` 设 `minLength: 4` 是刻意的低门槛——目标是**强制留下理由**而不是为难 Agent，同时把"无理由盲写"变成不可表达的操作。

#### 2.3.6 `memory_set_metadata`（元数据变更，不增版本）

```json
{
  "name": "memory_set_metadata",
  "description": "Change type and/or status only. This is a metadata change: no version bump, no re-embedding, no VERSION_CONFLICT. Use memory_update to change content.",
  "inputSchema": {
    "type": "object",
    "properties": {
      "memory_id":  { "type": "string" },
      "type":       { "type": "string", "description": "New namespace/name. Omit to leave unchanged." },
      "status":     { "type": "string",
                      "enum": ["ACTIVE","PENDING_VERIFY","CONFLICT","HISTORICAL","TEMPORARY","DELETED"] },
      "reason":     { "type": "string", "minLength": 4 }
    },
    "required": ["memory_id"],
    "anyOf": [ { "required": ["type"] }, { "required": ["status"] } ],
    "additionalProperties": false
  }
}
```
返回：`{"ok":true,"data":{"memory_id":"mem_…","version":3,"status":"HISTORICAL","type":"amr.decision","updated_at":…},"meta":{"version_bumped":false}}`

**要点与护栏**：
- 这是**唯一不增版本**的写工具，`meta.version_bumped:false` 显式回执，便于验收断言。
- **`status` 不得由本工具设为 `DELETED`**（走 `memory_delete`，以获得 `deletion_reason` 与归档语义）；设为 `DELETED` 时返回 `-32002 INVALID_STATUS_TRANSITION`。
- **`status` 不得由本工具设为 `CONFLICT`**（入侵 `memory_conflict` 的职责，会绕过双向打标）。返回 `-32002` 并在 message 中指明正确工具。
- 非法迁移（如 `DELETED → ACTIVE` 经业务通道）返回 `-32002`，`details.allowed_transitions` 列出该状态下的合法目标。

#### 2.3.7 `memory_conflict`（冲突双向原子打标）

```json
{
  "name": "memory_conflict",
  "description": "Declare that a newly observed fact contradicts an existing memory. Atomically creates the new memory in CONFLICT status and flips the target memory to CONFLICT as well, linking them via conflicts_with. Both sides are always marked together.",
  "inputSchema": {
    "type": "object",
    "properties": {
      "conflicts_with": { "type": "string", "description": "memory_id of the existing memory being contradicted." },
      "content":        { "type": "string", "minLength": 1, "description": "The contradicting statement, as observed." },
      "project_id":     { "type": "string", "default": "global" },
      "type":          { "type": "string", "default": "general" },
      "evidence_refs":  { "type": "array", "items": { "type": "object" }, "minItems": 1,
                          "description": "Mandatory provenance for the contradicting claim." },
      "change_reason":  { "type": "string", "minLength": 4, "description": "Why these two contradict." }
    },
    "required": ["conflicts_with", "content", "evidence_refs", "change_reason"],
    "additionalProperties": false
  }
}
```
返回：
```json
{ "ok": true,
  "data": { "new_memory_id": "mem_20261001_new001", "new_version": 1, "new_status": "CONFLICT",
            "target_memory_id": "mem_20260926_01a2b3", "target_previous_status": "ACTIVE",
            "target_status": "CONFLICT", "linked": true },
  "meta": { "atomic": true, "conflict_group": ["mem_20260926_01a2b3","mem_20261001_new001"] } }
```
**要点（这是本方案最需要严密的单点）**：
- 三条写操作在**同一个 SQLite 事务**内完成：① 插入新记忆（`status=CONFLICT`, `conflicts_with=[target]`）② UPDATE 目标记忆 `status=CONFLICT`、`conflicts_with` 追加新 ID ③ 审计双写。任一失败整体回滚，**不存在单边 `CONFLICT`**（不变式 I-3）。
- 目标不存在 → `-32004 MEMORY_NOT_FOUND`，不产生任何副作用。
- 目标已是 `DELETED` → `-32002`，禁止与已销毁记忆建立争议关系。
- Outbox 侧为**两条** projection 任务（新记忆 upsert + 目标 update_payload），但 `conflicts_with` **不进 9 字段投影**（见 D1），因此目标记忆的 Qdrant payload 变更仅为 `status`。
- 仲裁出口：后续由 `memory_set_metadata`（人工/管理员）把胜方置 `ACTIVE`、败方置 `HISTORICAL`；AI 无仲裁权。

#### 2.3.8 `memory_delete`

```json
{
  "name": "memory_delete",
  "description": "Soft-delete a memory (status=DELETED). The row is retained for audit; the vector projection is removed. DELETED is terminal for agents.",
  "inputSchema": {
    "type": "object",
    "properties": {
      "memory_id":       { "type": "string" },
      "reason":          { "type": "string", "minLength": 4, "description": "Mandatory deletion justification (audit)." },
      "expected_version":{ "type": "integer", "description": "Optional optimistic guard." }
    },
    "required": ["memory_id", "reason"],
    "additionalProperties": false
  }
}
```
返回：`{"ok":true,"data":{"memory_id":"mem_…","previous_status":"ACTIVE","status":"DELETED","deleted_at":…}}`

#### 2.3.9 `memory_reconcile`

```json
{
  "name": "memory_reconcile",
  "description": "Inspect (and optionally trigger) the reconciliation of TEMPORARY memories. Returns PROPOSALS only - it never applies changes itself. Applying proposals beyond TEMPORARY->PENDING_VERIFY requires an explicit human/admin approval.",
  "inputSchema": {
    "type": "object",
    "properties": {
      "action":     { "type": "string", "enum": ["status","run","list_candidates"], "default": "status" },
      "project_id": { "type": "string", "description": "Scope the run to one project." },
      "batch_id":   { "type": "string", "description": "For action=list_candidates." },
      "limit":      { "type": "integer", "minimum": 1, "maximum": 200, "default": 50 }
    },
    "required": [],
    "additionalProperties": false
  }
}
```
返回（`action=status`）：
```json
{ "ok": true, "data": {
    "temporary_backlog": 37,
    "oldest_temporary_age_hours": 29,
    "eligible_now": 22,
    "last_batch": { "batch_id": "recon_20261001_0230_ab12cd", "status": "COMPLETED",
                    "proposed": 22, "applied": 18, "created_at": 1790322600 },
    "next_scheduled_run": 1790409000 } }
```
**要点**：`run` 是**提案生成**，不是状态变更；`list_candidates` 回的是 `curation_candidates` 中的提案（含 `state`/`rejection_reason`/`evidence_snapshot`）。把"看"和"改"彻底分开，Agent 永远不能借这个工具一把梭改库。

#### 2.3.10 `session_ingest`

```json
{
  "name": "session_ingest",
  "description": "Ingest raw conversation messages into the lossless SQLite store for provenance. Idempotent on (session_id, message_id). Does not call any LLM and does not create memories.",
  "inputSchema": {
    "type": "object",
    "properties": {
      "session_id": { "type": "string" },
      "project_id": { "type": "string", "default": "global" },
      "messages": {
        "type": "array", "minItems": 1,
        "items": {
          "type": "object",
          "properties": {
            "message_id": { "type": "string" },
            "role":       { "type": "string", "enum": ["user","assistant","system","tool"] },
            "content":    { "type": "string" },
            "sequence":   { "type": "integer" },
            "timestamp":  { "type": "integer", "description": "Unix seconds. Mandatory - ISO strings are rejected." }
          },
          "required": ["message_id","role","content","timestamp"],
          "additionalProperties": false
        }
      }
    },
    "required": ["session_id","messages"],
    "additionalProperties": false
  }
}
```
**要点**：保留既有幂等语义（新插入 / revision 更新 / 忽略三分支），但把错误的 `timestamp` 从"静默丢弃"改为**显式报错 `-32602`**——静默丢消息是本系统历史上最隐蔽的坑（见 `amr-client-protocol` skill 的 Session Store Message Contract 条目）。`role` 增加 `tool`（工具输出是需要留痕的证据）。

### 2.4 Agent 典型调用时序（供 OpenCode 写集成测试直接照抄）

**A. 常规沉淀**
```
memory_create {content, project_id, type, source_refs} → memory_id, version=1
```

**B. 发现与既有记忆矛盾（冲突路径）**
```
memory_search {query}                      → 命中 mem_A (version=3, ACTIVE)
memory_conflict {conflicts_with: mem_A, content, evidence_refs, change_reason}
   → mem_A.status = CONFLICT, mem_B.status = CONFLICT
（仲裁）memory_set_metadata {memory_id: mem_B, status: ACTIVE}   ← 人工/管理员
         memory_set_metadata {memory_id: mem_A, status: HISTORICAL}
```

**C. 并发安全的内容修订**
```
memory_get    {memory_id: mem_A}                    → version = 3
memory_update {memory_id: mem_A, expected_version: 3, content, change_reason}
   ├─ 成功 → version = 4
   └─ -32001 → memory_get 重新取 version=4，套用变更重试
```

**D. 上下文切断暂存 → 次日整理**
```
memory_create {content, status: TEMPORARY, source_refs}      ← 交接前暂存
（次日 02:30 整理器扫描）→ 提案 TEMPORARY → PENDING_VERIFY
memory_get {memory_id}                                        → status = PENDING_VERIFY
```

### 2.5 Schema 下发方式（插件化落地）

`src/interfaces/mcp/tools.py` 保持**单一事实源**：所有 4 个 Agent 的插件（`plugins/hermes-amr`、`openclaw-amr`、`opencode-amr`、`dsh-amr`）**不得各自抄一份 schema**，一律通过 MCP `tools/list` 动态拉取。插件只负责：① 拉起 bridge 子进程 ② 把 tools 注册进宿主 ③ 做 prefetch 注入与 XML 转义。
**新增约束**：`tools/list` 返回值中应带 `x-amr-tool-contract: "v3.0"`，插件做版本握手，避免宿主缓存了旧 schema 而静默调错参数。

---
