# AI Memory Runtime - opencode 任务拆解与分步执行方案 (TASK)

**文档标识**：`DOC-AMR-06-TASK`  
**版本号**：`v1.0.0`  
**定案日期**：2026-09-26  
**编写方**：OpenClaw（任务拆解与审计验收）  
**执行方**：opencode（代码编写与单步测试）  
**工程目标根目录**：`/home/dj/WorkSpaces/openclaw/ai-memory-runtime/`

---

## 协作工作流规则（老板定案准则）
1. **执行边界**：所有具体编码任务由 **opencode** 单步执行，严禁跨步骤一次性堆叠；
2. **审计边界**：每完成一个 Step，**OpenClaw** 进行代码审计与实测验证，验证通过后方可下发下一步；
3. **安全红线**：
   - MCP Bridge 进程严禁导入 `torch` / `transformers` / `cuda`；
   - 严禁开放任何 TCP 监听端口，通信一律收敛于 UDS；
   - 配置文件 `config.yaml` 严格权限 `0600`，绝不进入 Git。

---

## 任务拆分总览矩阵

| 任务步骤 | 模块/阶段 | 目标产物路径 | 核心目标与交付标准 |
| :--- | :--- | :--- | :--- |
| **STEP 1** | 工程脚手架与 UDS 协议层 | `config/`, `src/interfaces/ipc/` | 项目骨架、配置加载器、4MB 长度前缀 JSON-RPC UDS 服务端与客户端通信通道 |
| **STEP 2** | 会话存储与幂等引擎 | `src/core/session_store.py` | SQLite WAL 模式、sessions/messages/ingest_log 三表结构、联合 content_hash 识别 revision |
| **STEP 3** | BGE-M3 引擎与 6 态状态机 | `src/core/engine.py` | Tesla P4 FP16 推理、6 态流转（含 LOADING 排队）、300s Idle 自动卸载、线程池隔离 |
| **STEP 4** | Qdrant 适配与语义 Memory 服务 | `src/core/qdrant.py`, `src/service/` | 长连接复用、强制 status=active 过滤、四态流转、>8192 Token 自动分块 |
| **STEP 5** | 零显存 MCP 桥接与 Systemd 托管 | `src/interfaces/mcp/`, `admin/`, `systemd/` | 轻量 Stdio 桥接（<25MB）、admin-cli 运维工具、Systemd 用户守护与全量 pytest 验收 |

---

## 详细单步任务任务书

### STEP 1：工程脚手架与 UDS 协议层
- **输入参考**：`DOC-AMR-02-ADD` 第 3 节，`DOC-AMR-04-API` 第 1 节。
- **任务目标**：
  1. 初始化 `/home/dj/WorkSpaces/openclaw/ai-memory-runtime/` 目录；
  2. 编写 `requirements.txt`、`config/config.example.yaml` 及配置读取加载模块 `config/settings.py`；
  3. 编写 `src/interfaces/ipc/protocol.py`，实现前 4 字节 Big-Endian `uint32` 长度前缀的双向编码/解码器，严格校验 `MAX_REQUEST_BYTES = 4,194,304`；
  4. 编写 `src/interfaces/ipc/server.py`，实现基于 `asyncio.start_unix_server` 的双 Socket 监听（业务 socket 与管理 socket，权限 `0600`）。
- **验收准则**：
  - 编写单测 `tests/test_protocol.py`，测试大包拦截（>4MB 报错断开）、正常 JSON-RPC 消息回显通过。

---

### STEP 2：会话存储与幂等引擎
- **输入参考**：`DOC-AMR-03-DDD` 第 1.1 节，`DOC-AMR-04-API` 第 2.5 节。
- **任务目标**：
  1. 编写 `src/core/session_store.py`；
  2. 初始化 SQLite 数据库，配置 `WAL` 模式与 `busy_timeout=5000`；
  3. 创建 `sessions`, `messages`, `ingest_log` 三张表；
  4. 实现 `ingest_messages(session_id, agent_id, project_id, messages)` 方法：
     - 计算 `content_hash = sha256(role + content)`；
     - 判定相同 `(session_id, message_id)` 下 Hash 变动为 revision 并执行更新；完全一致则忽略；新记录则插入；全过程写入 `ingest_log`。
- **验收准则**：
  - 编写单测 `tests/test_session_store.py`，断言重复摄取无膨胀，修改内容正确触发 revision 更新并记录日志。

---

### STEP 3：BGE-M3 引擎与 6 态状态机
- **输入参考**：`DOC-AMR-03-DDD` 第 2 节，`DOC-AMR-04-API` 第 1 节。
- **任务目标**：
  1. 编写 `src/core/engine.py`；
  2. 实现 6 态状态机：`UNLOADED / LOADING / READY / IDLE / UNLOADING / ERROR`；
  3. 实现模型动态加载与显存回收：`torch.amp.autocast('cuda')`，回收时执行 `del` + `gc.collect()` + `torch.cuda.empty_cache()`；
  4. 启动 300 秒（5 分钟）Idle 卸载定时器；
  5. 实现单 Worker 排队机制（`asyncio.Queue` + `ThreadPoolExecutor`），切块批处理 `MAX_BATCH = 16`；
  6. 当状态为 `LOADING` 时，新请求自旋等待（最长 25 秒），队列超 64 报 503。
- **验收准则**：
  - 编写单测 `tests/test_engine.py`，模拟状态机流转，断言 LOADING 状态下并发请求不丢失，空闲 300 秒后触发 unload，allocated_mb 归零。

---

### STEP 4：Qdrant 适配与语义 Memory 服务
- **输入参考**：`DOC-AMR-03-DDD` 第 1.2 节，`DOC-AMR-04-API` 第 2 节。
- **任务目标**：
  1. 编写 `src/core/qdrant.py`，单例复用官方 `QdrantClient`；
  2. 编写 `src/service/memory_service.py`，实现核心业务逻辑：
     - `memory_search`：向 BGE 发起单次推理获取向量，调用 Qdrant 检索，**服务端底层强制注入 `status == active`**；
     - `memory_record`：检查 Token 长度，超过 8192 时按 128 Tokens 滑动重叠分块存储；
     - `memory_get`：按 ID 获取记忆，并关联 SQLite 查询返回关联的 `raw_messages`；
     - `memory_update_status`：支持状态四态流转（`active`, `superseded`, `archived`, `deleted`）。
- **验收准则**：
  - 编写单测 `tests/test_memory_service.py`，断言软删除后检索不可见，超长文本成功切片并携带 `parent_memory_id`。

---

### STEP 5：零显存 MCP 桥接与 Systemd 托管
- **输入参考**：`DOC-AMR-02-ADD` 第 1 节，`DOC-AMR-05-TST`。
- **任务目标**：
  1. 编写 `src/interfaces/mcp/bridge.py` 与 `tools.py`：
     - 标准 Stdio MCP 实现（可使用标准 `mcp` 纯 Python 库），**严禁 import torch**；
     - 将 5 个 `memory_*` 工具透明序列化为 UDS 协议发往 `qdrant-bge.sock`；
     - 硬编码注入 `source_agent` 标识。
  2. 编写 `src/admin/cli.py`：实现 `admin-cli status`、`admin-cli unload` 等运维指令（对接 `qdrant-bge-admin.sock`）；
  3. 编写 `systemd/amr.service`；
  4. 整合全量测试套件 `tests/`。
- **验收准则**：
  - 启动 systemd 用户服务，通过 MCP Stdio 模拟协议完整执行存取；
  - 运行 `pytest tests/` 达到 100% PASS；
  - 运行 `nvidia-smi` 验证显存稳保在 4.2GB 以内。
