# AI Memory Runtime - 接口协议与契约规范文档 (API)

**文档标识**：`DOC-AMR-04-API`  
**版本号**：`v1.0.0`  
**定案日期**：2026-09-26  
**编写方**：OpenClaw（架构团队）  
**审计方**：Hermes（架构专家）  
**执行方**：opencode（代码交付）

---

## 1. 协议层总则 (Protocol Overview)

- **传输层**：纯本地 Unix Domain Socket (UDS)。
- **包体格式**：Length-Prefixed JSON-RPC 2.0。
  - 前 4 字节：`uint32_be`（无符号 32 位大端整数），表示后随 JSON-RPC 字符串的字节长度。
  - 单包上限：`MAX_REQUEST_BYTES = 4,194,304` (4MB)。超过立即拒收断开。
- **Socket 分离**：
  - **业务 Socket**：`/run/user/1000/qdrant-bge.sock`（权限 `0600`，仅处理普通 Agent 工具调用）
  - **管理 Socket**：`/run/user/1000/qdrant-bge-admin.sock`（权限 `0600`，仅处理管理员 CLI 运维）

---

## 2. 业务接口规范（面向普通 Agent MCP 暴露）

### 2.1 `memory_search` (记忆语义检索)
- **方法名**：`memory.search`
- **MCP Tool 名称**：`memory_search`
- **入参规范**：
```json
{
  "query": "string (必填，检索文本)",
  "collections": ["string (可选，默认 ['ai_memory']，支持 ['crypto_standards', 'project_docs', 'all'])"],
  "project_id": "string (可选，限定项目范围，不传则检索该 collection 下全局)",
  "memory_type": "string (可选，fact / decision / rule / context)",
  "scope": "string (可选，global / project / agent / session，默认 global)",
  "limit": "integer (可选，默认 5，最大 20)"
}
```
- **出参规范**：
```json
{
  "results": [
    {
      "memory_id": "mem_20260926_01a2b3",
      "content": "SM4 GCM 模式下 IV 推荐为 12 字节，Tag 长度必须固定为 16 字节。",
      "memory_type": "decision",
      "score": 0.8842,
      "collection": "ai_memory",
      "project_id": "Reduction-Go",
      "source_agent": "opencode",
      "source_message_ids": ["msg_102", "msg_104"],
      "created_at": 1790352000
    }
  ],
  "total": 1
}
```
- **核心约束**：服务端底层强制注入 `status == "active"`，上层无法覆盖。

---

### 2.2 `memory_record` (显式沉淀记忆)
- **方法名**：`memory.record`
- **MCP Tool 名称**：`memory_record`
- **入参规范**：
```json
{
  "content": "string (必填，沉淀的事实、决策或规范内容)",
  "memory_type": "string (可选，默认 'fact'，可选 'decision' / 'rule' / 'context')",
  "scope": "string (可选，默认 'global'，可选 'project' / 'agent' / 'session')",
  "project_id": "string (可选，绑定具体项目，如 'Reduction-Go')",
  "source_agent": "string (由 MCP Bridge 自动硬编码注入，禁止客户端假冒)",
  "session_id": "string (可选，来源会话 ID)",
  "source_message_ids": ["string (可选，来源消息 ID 列表)"],
  "tags": ["string (可选，标签数组)"]
}
```
- **出参规范**：
```json
{
  "memory_id": "mem_20260926_01a2b3",
  "status": "active",
  "chunks_created": 1,
  "created_at": 1790352000
}
```
- **核心约束**：若 `content` 超过 8192 Token，内部自动分块（Overlap 128 Tokens），生成 `parent_memory_id` 关联记录，`chunks_created` 返回切片数。

---

### 2.3 `memory_get` (精准提取记忆与溯源)
- **方法名**：`memory.get`
- **MCP Tool 名称**：`memory_get`
- **入参规范**：
```json
{
  "memory_id": "string (必填，记忆 ID)"
}
```
- **出参规范**：
```json
{
  "memory_id": "mem_20260926_01a2b3",
  "content": "SM4 GCM 模式下 IV 推荐为 12 字节...",
  "memory_type": "decision",
  "status": "active",
  "superseded_by": null,
  "scope": "global",
  "project_id": "Reduction-Go",
  "source_agent": "opencode",
  "session_id": "sess_20260926_dj",
  "source_message_ids": ["msg_102", "msg_104"],
  "raw_messages": [
    {
      "message_id": "msg_102",
      "role": "user",
      "content": "SM4 GCM 的认证标签应该多长？",
      "timestamp": 1790351980
    },
    {
      "message_id": "msg_104",
      "role": "assistant",
      "content": "国密规范推荐 Tag 长度固定为 16 字节 (128位)。",
      "timestamp": 1790352000
    }
  ],
  "meta": { "tags": ["crypto", "sm4"] }
}
```

---

### 2.4 `memory_update_status` (记忆四态流转)
- **方法名**：`memory.update_status`
- **MCP Tool 名称**：`memory_update_status`
- **入参规范**：
```json
{
  "memory_id": "string (必填，目标记忆 ID)",
  "new_status": "string (必填，active / superseded / archived / deleted)",
  "superseded_by": "string (当 new_status 为 superseded 时必填，指向替代的新记忆 ID)"
}
```
- **出参规范**：
```json
{
  "memory_id": "mem_20260926_01a2b3",
  "previous_status": "active",
  "current_status": "superseded",
  "updated_at": 1790353000
}
```

---

### 2.5 `memory_ingest_session` (原始会话流水幂等摄取)
- **方法名**：`session.ingest`
- **MCP Tool 名称**：`memory_ingest_session`
- **入参规范**：
```json
{
  "session_id": "string (必填，会话唯一 ID)",
  "agent_id": "string (必填，调用方 Agent 标识)",
  "project_id": "string (可选，关联项目)",
  "messages": [
    {
      "message_id": "string (必填，消息 ID)",
      "role": "string (user / assistant / system)",
      "content": "string (消息正文)",
      "sequence": 1,
      "timestamp": 1790351980
    }
  ]
}
```
- **出参规范**：
```json
{
  "session_id": "sess_20260926_dj",
  "total_received": 5,
  "inserted": 3,
  "revision_updated": 1,
  "ignored": 1,
  "status": "success"
}
```
- **核心逻辑**：以 `(session_id, message_id)` 和 `content_hash` 对比。相同且 Hash 一致则 ignored；相同但 Hash 不一致则判定为 revision 并更新；不存在则新插入。不调用任何 LLM 提取。

---

## 3. 管理接口规范（仅面向 `admin-cli` 开放，走 Admin UDS）

- `admin.get_status` -> 获取 6 态状态机状态、GPU 显存指标（allocated_mb, reserved_mb, driver_used_mb）、队列深度、P50 耗时等。
- `admin.model_control` -> 手动触发 `load` 或 `unload`。
- `admin.collection_list` -> 列出当前配置文件生效的集合及其向量统计。
- `admin.snapshot_create` -> 创建 Qdrant 集合本地快照。
