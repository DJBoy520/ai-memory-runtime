# OpenCode 任务书：Hermes 原生 AMR 记忆插件实现

**任务标识**：`TASK-OPENCODE-IMPL-AMR-HERMES`  
**定案日期**：2026-09-26  
**委托方**：Hermes（方案与验收）  
**审计指导**：OpenClaw（架构审计定案）  
**执行方**：opencode（代码交付）  
**目标目录**：`/home/dj/.hermes/hermes-agent/plugins/memory/amr/`  
**依赖参考**：
- 设计方案：`/home/dj/WorkSpaces/ai-memory-runtime/docs/DOC-AMR-07-HERMES-INTEGRATION.md`
- 基类参考：`/home/dj/.hermes/hermes-agent/agent/memory_provider.py`

---

## 一、老板特别指示（最高优先级）
- **预取超时**：`prefetch()` 等待超时时间严格设定为 **0.3秒（300ms）**（`timeout=0.3`），确保历史记忆充分被召回，不盲目追求亚毫秒响应而丢弃上下文。

---

## 二、OpenClaw 审计规范要求（必须严格遵守）
1. **纯标准库轻量 UDS 客户端 (`_client.py`)**：
   - 严禁导入 `torch`, `cuda`, `transformers`，仅用 Python 标准库 (`socket`, `struct`, `json`, `os`, `time`, `logging`)；
   - 协议为 4 字节 Big-Endian `uint32` 长度前缀 + JSON-RPC 2.0；
   - 严格实现 `_recv_exact(sock, n)` 循环读取，防御粘包与半包；
   - 支持 Fail-open 优雅降级（连接异常或超时返回默认空结果，不抛未捕获异常中断主流程）。

2. **插件生命周期实现 (`__init__.py`)**：
   - 继承 `agent.memory_provider.MemoryProvider`；
   - `name`: 返回 `"amr"`；
   - `is_available()`: 快速检查 socket 文件是否存在；
   - `queue_prefetch(query)`: 异步在后台线程池发起检索；
   - `prefetch(query)`: 最多等待 **0.3 秒**（`future.result(timeout=0.3)`），提取命中内容格式化为 Markdown 事实上下文；
   - `sync_turn(user_content, assistant_content, session_id, messages)`: 在后台守护线程执行，调用 `session.ingest` 归档原始流水，并按需沉淀事实至 `memory.record`，Payload 必须带上 `agent_id="hermes"`, `source="hermes_plugin"`。

3. **插件描述 (`plugin.yaml`)**：
   - 标准 Hermes 插件配置，无额外 pip 依赖。

4. **单测套件 (`test_amr_plugin.py`)**：
   - 在插件目录或 tests 编写单测，测试 socket 编解码、超时容错、以及生命周期方法。

---

## 三、交付产物检查清单
1. `/home/dj/.hermes/hermes-agent/plugins/memory/amr/__init__.py`
2. `/home/dj/.hermes/hermes-agent/plugins/memory/amr/_client.py`
3. `/home/dj/.hermes/hermes-agent/plugins/memory/amr/plugin.yaml`
4. `/home/dj/.hermes/hermes-agent/plugins/memory/amr/test_amr_plugin.py`
