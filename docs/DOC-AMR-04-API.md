# AI Memory Runtime - 接口协议与契约规范 (API)

**文档标识**：`DOC-AMR-04-API`  
**版本号**：`v2.2.0`  
**传输规范**：本地 Unix Domain Socket (UDS) + JSON-RPC 2.0 (单包 ≤ 4MB)

---

## 1. 业务接口清单

所有方法均通过业务 Socket (`$XDG_RUNTIME_DIR/qdrant-bge.sock`) 暴露，对应标准 MCP 工具族。

### 1.1 `memory.search` (记忆语义检索)
- **方法名**：`memory.search` / MCP 工具：`memory_search`
- **入参**：
```json
{
  "query": "string (必填，检索文本)",
  "project_id": "string (可选，限定项目范围)",
  "limit": 5,
  "score_threshold": 0.52
}
```
- **出参**：
```json
{
  "results": [
    {
      "memory_id": "mem_20261008_01a2b3",
      "content": "SM4 GCM 模式下 IV 推荐为 12 字节，Tag 长度必须固定为 16 字节。",
      "score": 0.8842,
      "project_id": "crypto-infrastructure",
      "created_at": 1791456000
    }
  ],
  "total": 1
}
```

---

### 1.2 `memory.create` / `memory.record` (记录记忆)
- **方法名**：`memory.create` / MCP 工具：`memory_create`
- **入参**：
```json
{
  "content": "string (必填，事实正文)",
  "project_id": "string (可选，关联项目)",
  "source_message_ids": ["msg_001"],
  "session_id": "sess_001"
}
```
- **出参**：
```json
{
  "memory_id": "mem_20261008_01a2b3",
  "status": "active",
  "created_at": 1791456000
}
```

---

### 1.3 `memory.get` (查询记忆详情)
- **方法名**：`memory.get` / MCP 工具：`memory_get`
- **入参**：`{ "memory_id": "mem_20261008_01a2b3" }`
- **出参**：返回该条记忆的完整元数据及关联的原始会话消息信息。

---

### 1.4 `memory.update` / `memory.update_status` (更新状态)
- **方法名**：`memory.update` / MCP 工具：`memory_update`
- **入参**：
```json
{
  "memory_id": "mem_20261008_01a2b3",
  "status": "archived",
  "change_reason": "配置已升级"
}
```

---

### 1.5 `session.ingest` (批量摄取会话流水)
- **方法名**：`session.ingest` / MCP 工具：`memory_ingest_session`
- **入参**：
```json
{
  "session_id": "sess_1001",
  "agent_id": "hermes",
  "project_id": "general",
  "messages": [
    {
      "message_id": "msg_001",
      "role": "user",
      "content": "请执行测试",
      "sequence": 1,
      "timestamp": 1791456000
    }
  ]
}
```
- **出参**：返回处理结果统计（`inserted`, `ignored`, `revision_updated`）。

---

## 2. 管理接口清单

通过管理 Socket (`$XDG_RUNTIME_DIR/qdrant-bge-admin.sock`) 开放给 CLI 工具：
- `admin.get_status`：查询服务运行健康状态、发件箱同步队列深度；
- `admin.collection_list`：查看当前配置的向量集合统计。
