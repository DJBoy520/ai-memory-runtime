# AI Memory Runtime (AMR) 可靠性重构与故障排查审计报告（供 GPT 外部独立审计）

- **审计目标**: 针对 AMR 项目近期在多智能体并发调用中的超时、错误崩溃与版本冲突，评估当前问题根因分析、架构重构方案（RFC v2）及契约设计的完备性、正确性与一致性。
- **项目工作区**: `/home/dj/WorkSpaces/ai-memory-runtime`
- **当前 Git 分支**: `fix/amr-reliability` (HEAD: `7e37671`)
- **生成时间**: 2026-11-08

---

## 目录
1. [系统背景与真实运行环境事实](#1-系统背景与真实运行环境事实)
2. [故障现象与现场日志核验证据](#2-故障现象与现场日志核验证据)
3. [多模型协同排查与审查演进（Gemini + DeepSeek 深度博弈）](#3-多模型协同排查与审查演进gemini--deepseek-深度博弈)
4. [核心根因剖析与架构缺陷（从表面现象到机制缺陷）](#4-核心根因剖析与架构缺陷从表面现象到机制缺陷)
5. [RFC v2 重构修复方案详细设计](#5-rfc-v2-重构修复方案详细设计)
6. [已落盘的代码与契约基础](#6-已落盘的代码与契约基础)
7. [外部审计要点与风险清单（供 GPT 重点核验）](#7-外部审计要点与风险清单供-gpt-重点核验)

---

## 1. 系统背景与真实运行环境事实

### 1.1 架构与技术栈
AMR 是本地私有运行的“多智能体统一语义记忆运行时”：
- **后端守护进程 (AMR Daemon)**: Python 3.12 异步守护进程，宿主为 `systemd --user` (`amr.service`)。
- **通信通道 (IPC)**: 本机 Unix Domain Socket (UDS) 双通道，权限严格 `0600`：
  - 业务端口: `/run/user/1000/qdrant-bge.sock` (JSON-RPC 2.0，4 字节 big-endian 长度前缀封包，帧上限 4MB)。
  - 管理端口: `/run/user/1000/qdrant-bge-admin.sock`。
- **客户端接入协议**:
  - **原生插件 (Fail-open)**: `dsh-amr` (DeepSeek Harness)、`opencode-amr`、`openclaw-amr`、`hermes-amr`，嵌入智能体运行时生命周期，执行预取 (Prefetch) 与会话归档 (Session Ingest)。
  - **MCP 桥接器**: `src/interfaces/mcp/bridge.py`，供 ZCode、Claude Code、Codex 等标准 MCP 客户端使用 stdio 桥接调用。
- **存储与索引体系**:
  - **单点真实事实来源 (SSOT)**: 本地 SQLite 3 (`data/sessions.db`)，配置 WAL 模式 (`busy_timeout=5000`)。
  - **向量检索库**: 外部独立 Qdrant 节点 (`http://192.168.30.161:6333`)，向量维度 1024。
- **本地硬件与嵌入模型**:
  - GPU: 本机 NVIDIA Tesla P4 (8GB 显存，驱动 580.178.04，CUDA 13.0)。
  - 嵌入模型: `BAAI/bge-m3` (FP16 加载，占用显存约 1.3GB)。

### 1.2 生产数据库现状审计 (2026-11-07 实测数据)
- `memories` 主表状态分布: `ACTIVE: 605`, `HISTORICAL: 522`, `DELETED: 28`（无未迁移的 legacy 脏值）。
- `qdrant_sync_queue`: 历史同步任务共 3587 条，当前状态全为 `synced`。
- `memory_projection_outbox`: 0 行（此前虽然建了表，但在写链路中并无消费者，仅对账引擎 `reconciliation` 尝试写入）。

---

## 2. 故障现象与现场日志核验证据

通过调取 systemd journald（近 7 天）及系统实时运行指标，定位到 Agent 调用频繁报错的现场日志：

### 2.1 故障日志 1：模型冷启动级联超时（发生率 > 80%）
```text
2026-11-03 19:35:42,097 [ERROR] [src.core.engine] Inference error in embed: Timed out waiting for READY (25.0s)
  File "/home/dj/WorkSpaces/ai-memory-runtime/src/core/engine.py", line 325, in embed
    await self._wait_for_ready(timeout=25.0)
  File "/home/dj/WorkSpaces/ai-memory-runtime/src/core/engine.py", line 271, in _wait_for_ready
    raise ModelLoadingTimeoutError(f"Timed out waiting for READY ({timeout}s)")
src.core.engine.ModelLoadingTimeoutError: Timed out waiting for READY (25.0s)
2026-11-03 19:35:42,515 [ERROR] [ipc.server] [business] Handler error executing method 'memory.search': Timed out waiting for READY (25.0s)
```
伴随大量客户端提前断开连接的日志：
```text
2026-11-07 18:30:37,725 [INFO] [ipc.server] [business] Client disconnected before response could be sent
```

### 2.2 故障日志 2：虚假 OCC 版本冲突（“已提交却返回失败”）
```text
2026-11-07 18:30:30,858 [ERROR] [ipc.server] [business] Handler error executing method 'memory.update': VERSION_CONFLICT: Expected version 2, but current version is 3.
  File "/home/dj/WorkSpaces/ai-memory-runtime/src/core/session_store.py", line 1545, in update_memory_v3
    raise ValueError(f"VERSION_CONFLICT: Expected version {expected_version}, but current version is {curr_version}.")
ValueError: VERSION_CONFLICT: Expected version 2, but current version is 3.
```

### 2.3 故障日志 3：客户端标识拒绝 (AGENT_UNAUTHORIZED)
```text
2026-11-07 12:52:20,300 [ERROR] [ipc.server] [business] Handler error executing method 'memory.search': AGENT_UNAUTHORIZED: Unknown or untrusted agent_id 'codex'
2026-10-10 21:59:44,265 [ERROR] [ipc.server] [business] Handler error executing method 'memory.search': AGENT_UNAUTHORIZED: Unknown or untrusted agent_id 'zcode-win'
2026-10-07 02:09:25,572 [ERROR] [ipc.server] [business] Handler error executing method 'memory.record': AGENT_UNAUTHORIZED: Unknown or untrusted agent_id 'zcode-overnight'
```

### 2.4 故障日志 4：状态枚举越界
```text
2026-10-25 11:54:04,445 [ERROR] [ipc.server] [business] Handler error executing method 'memory.update': Invalid status: 'SUPERSEDED'. Must be one of {'TEMPORARY', 'ACTIVE', 'CONFLICT', 'DELETED', 'HISTORICAL', 'PENDING_VERIFY'}
```

---

## 3. 多模型协同排查与审查演进（Gemini + DeepSeek 深度博弈）

在排查与制定修复方案的过程中，经历了由浅入深、双模型互相推翻与严苛交叉审查的演化：

### 阶段一：初查与直觉推断（Gemini 初步排查）
- **现象归纳**: 准确指出了 GPU 模型冷启动 25s 超时、白名单静态拦截未授权 agent、乐观锁冲突、以及日志全混在 systemd journald 无独立轮转文件。
- **初版建议**:
  1. 将 25s 等待调大到 60s，`idle_timeout_seconds` 改为 0；
  2. 增加文件轮转日志 `RotatingFileHandler`；
  3. 白名单允许通配符（如 `zcode*`）；
  4. 遇到乐观锁冲突自动获取最新版本重试合并。

### 阶段二：用户深度复核与纠偏（关键逻辑锁死）
用户复核后指出初版建议的多处致命缺陷：
1. **`idle_timeout_seconds: 0` 是严重暗坑**: `engine.py` 实现为 `threading.Timer(self.idle_timeout_seconds, ...)`，传 0 意味着 0 秒后立即卸载！必须从规范上将 `<= 0` 明确定义为“禁用卸载/永不卸载”。
2. **单纯 60s 仍兜不住并发冷启动**: 冷启动窗口在负载下可达 100s。必须从根上做 **Single-flight 并发收敛**（多请求共享同一加载任务），且保持客户端 80–150ms 预取预算的 Fail-open 特性不变。
3. **“假失败”制造了版本冲突**: `memory_update` 是先提交 SQLite 增加版本，再去同步计算 embedding 和写 Qdrant。一旦后面超时，客户端收到 Internal Error，重试时由于版本已在上轮提交中变成了新版本，直接触发 `VERSION_CONFLICT`。必须推行 **Transactional Outbox 单写者**，写请求只提交 SQLite 即返回。
4. **白名单不应盲目通配符**: `agent_id` 只是无认证的自声明字符串，通配符无安全性，应该分离 `agent_type`（授权）与 `instance_id`（审计），真实安全依赖 UDS 0600 文件属主。
5. **不建议再写一套文件日志**: systemd 已承担轮转，额外日志会双写浪费。应强化请求追踪与诊断工具。

### 阶段三：工作流流水线双向评审（RFC v1 遭拦截，拦截器生效）
依据用户思路起草 RFC v1 后，建立了 **“Gemini + DeepSeek 独立双向严格评审”** 门槛（任何一方判 revise 或存在 blocker，禁止进入编码）：
- **评审结果**: **两位模型全部给出 `revise`，合力斩获 22 项硬性 Blocker/缺陷**！
- **被评审拦截出的重大隐患**:
  1. **队列认错表**: v1 方案误将 `memory_projection_outbox` 作为当前生产队列，但经 DeepSeek 和代码审计查明，现有写链路消费的唯一队列是 `qdrant_sync_queue`，而该表**缺少 version、next_retry_at、dead_letter_at 等列**，直接建索引或当成规范表会使 SQL 报错。
  2. **`asyncio.Future` 跨事件循环竞争**: v1 企图用 `asyncio.Future` 做 single-flight，但 `engine` 实例同时被主线程 Loop 和 Outbox Worker 的独立线程+Loop 共享（`outbox_worker.py:161-171`），跨 Loop await Future 会抛 `attached to a different loop` 或死锁。
  3. **IPC Server 映射缺失**: v1 在 `server.py` 映射链中漏掉了 `RpcError` 直通分支，自定义业务异常依旧会被兜底为 `-32603 Internal Error`。
  4. **旧版本覆盖新版本**: 缺少陈旧投影行跳过规则，Worker 遇到重试时可能用旧快照覆盖新更新。
  5. **破坏既有测试契约**: v1 让自定义异常直接继承 `Exception`，导致现有 `tests/test_v3_contracts.py` 中预期的 `pytest.raises(ValueError)` 失败。

### 阶段四：方案闭环（诞生 RFC v2）
吸纳全部 22 条审计意见，将 single-flight 彻底改为线程原生锁与 epoch 代次方案，理顺了规范 Outbox 队列的升级迁移路径，形成目前的最终方案 RFC v2。

---

## 4. 核心根因剖析与架构缺陷（从表面现象到机制缺陷）

### 缺陷 1：模型加载缺乏并发收敛与跨线程冲突
- **源码位置**: `src/core/engine.py:209-232, 250`
- **机制漏洞**:
  - `load_model()` 没有防护并发进入状态：当第一个请求将状态变为 `LOADING` 并在后台线程池加载模型时，随后的请求进入发现不是 `READY`，在 25s 极短的硬编码自旋超时后抛出 `ModelLoadingTimeoutError`。
  - 模型在 Tesla P4 上的冷启动加载实测耗时在 28s ~ 35s 之间，硬编码 25s 必定导致系统在每次冷启动时引发大面积超时崩溃。
  - `idle_timeout_seconds` 定时器只要设为 0，就会产生 `Timer(0, unload)`，瞬间释放模型。

### 缺陷 2：破坏事务一致性的“同步双写”与假失败
- **源码位置**: `src/service/memory_service.py:728-769`
- **机制漏洞**:
  ```python
  # 1. 先提交 SQLite，版本从 2 递增为 3，落库 commit
  updated_item = await asyncio.to_thread(self.session_store.update_memory_v3, ...)

  # 2. 紧接着在请求主路径同步调用推理和 Qdrant
  embeddings = await self.engine.embed([content.strip()]) # <--- 此处若遇到 GPU 冷启动或抖动超时！
  await asyncio.to_thread(self.qdrant.upsert_points, ...)
  ```
  如果第 2 步发生任何网络抖动、模型超时或进程断开，异常会向外抛出给客户端。客户端认为更新失败，按照常理发起重试；但由于 SQLite 已经持久化并提交，内存卡片的版本已经是 3，客户端再次传入 `expected_version=2` 时，立即抛出 `VERSION_CONFLICT: Expected version 2, but current version is 3.`。

### 缺陷 3：两张队列表的历史割裂与 Worker 消费者缺陷
- **源码位置**: `src/core/session_store.py:156-188` vs `src/service/outbox_worker.py:63-141`
- **机制漏洞**:
  - 库内存在 `qdrant_sync_queue` 与 `memory_projection_outbox` 两张队列表。
  - `qdrant_sync_queue` 缺乏重试退避调度（无 `next_retry_at`），`retry_count >= 5` 直接丢弃不再扫描，且没有记录死信时间 `dead_letter_at`，排查极为困难。
  - 同时，如果一条记录先更新失败，后一条更新成功，失败的旧记录下次重试时，会用其携带的旧快照执行 `upsert`，将最新版本内容恶意回退覆盖！

### 缺陷 4：错误处理将语义信息完全吞没为 -32603
- **源码位置**: `src/interfaces/ipc/server.py:84-86`
- **机制漏洞**:
  `server.py` 内部简单粗暴地使用 `except Exception as ex: resp = make_jsonrpc_error(-32603, f"Internal error: {str(ex)}")`。导致客户端收到的全是无差别的 Internal Error，无法提取 `current_version` 进行乐观锁重试，也无法区分是授权失败、正在加载还是内部严重 Bug。

---

## 5. RFC v2 重构修复方案详细设计

完整方案详见 `docs/rfcs/RFC-AMR-RELIABILITY-FIXES.md`，核心设计概要如下：

### D1. 线程原语 Single-Flight 模型加载器与状态治理
彻底摒弃容易造成跨 Loop 死锁的 `asyncio.Future`，采用纯线程锁 + Epoch 机制：
1. **加载唯一性 (Single-Flight)**:
   - 使用 `threading.Lock()`（`_load_exec_lock`）非阻塞 `acquire(blocking=False)` 竞争加载权利。
   - 竞争成功者作为 Loader 派发后台任务到 `ThreadPoolExecutor(max_workers=1)`；竞争未成功者作为等待者。
2. **Epoch 防陈旧装载**:
   - 引入代次计数器 `_load_epoch`。`_load_model_sync` 改为返回 `(tokenizer, model)` 元组，不直接挂载实例属性。
   - 加载完成时核验代次：若代次一致才挂载并转为 `READY`；若等待者已因总体硬超时 (`load_timeout_seconds=120`) 推进了代次，完成线程自动释放丢弃产物 (`del + empty_cache`)，消除陈旧装载与竞态。
3. **Loop-Agnostic 等待协程**:
   - 任意异步事件循环（主 Loop 或 Outbox Worker 独立 Loop）只需以 50ms 周期异步轮询状态。
   - 单请求预算 (`request_load_wait_seconds=30`) 耗尽时抛出 `ModelLoadingPendingError`（对外暴露为 `MODEL_LOADING` 可重试状态），绝对不中断共享的加载过程。
4. **常驻模式明确化**:
   - `idle_timeout_seconds <= 0` 统一规定为**永不自动卸载**。
   - 在 `embed()` 的 `finally` 块中，当 `idle_timeout_seconds <= 0` 时，状态始终严格保持 `READY`，杜绝降级回 `IDLE` 或调用 `Timer(0)`。

### D2. 事务性 Outbox 单写者与严格幂等控制
1. **写请求同步截断**:
   - `memory_create`、`memory_update`、`memory_delete` 彻底移除主调用链路中的 `engine.embed` 和 `qdrant.*` 同步调用。
   - 请求只在 SQLite 事务内写入 `memories`、`memory_revisions` 以及规范发件箱 `memory_projection_outbox`，随后立即返回，响应附带 `"projection_status": "pending"`。
2. **规范化发件箱升级**:
   - 将 `memory_projection_outbox` 确立为标准规范队列（通过幂等 DDL 补充 `target_collection` 列以尊重传入的集合参数）。
   - Outbox Worker 切换为主消费 `memory_projection_outbox`，同时保留老队列 `qdrant_sync_queue` 排水逻辑保障兼容。
   - **防陈旧覆盖机制**: Worker 在处理每个 memory 的批量任务时，自动按 `memory_id` 归并只生效最新一条；执行 `upsert` 时增加守卫：若队列中任务的 `version < memories.version`，直接丢弃跳过，严防旧数据反弹。
   - **指数退避与死信**: 失败任务使用 `min(2^retry_count, 60)` 秒指数退避，超过 8 次标记 `dead_letter_at`，收归诊断监控。
3. **结构化幂等控制 (`idempotency_keys`)**:
   - 建立幂等底账: `PRIMARY KEY (agent_id_raw, op, idem_key)`。
   - 必须在事务内完成原子落库并生成快照 JSON。命中同一幂等键时返回带 `"idempotent_replay": true` 的原结果；若命中幂等键但目标 `memory_id` 发生篡改，拦截并抛出 `INVALID_ARGUMENT`。
4. **读路径一致性**:
   - `memory_get` 改为优先直接查询 SQLite SSOT 主表，不再硬读 Qdrant，消除在异步投影排队期间（几百毫秒窗口）`memory_get` 查出 null 的数据不一致。

### D3. 稳定业务错误码与全链路 Trace ID
1. **JSON-RPC 标准业务错误码体系**:
   - `-32001 AGENT_UNAUTHORIZED`: 身份不合法，返回 `{agent_id}`。
   - `-32002 VERSION_CONFLICT`: 乐观锁冲突，返回 `{memory_id, current_version, expected_version}`。
   - `-32003 MODEL_LOADING`: 模型排队加载中，返回 `{model_state, retry_after_ms}`。
   - `-32004 QUEUE_FULL`: 推理并发队列爆满，返回 `{active_requests, max_queue_size}`。
   - `-32005 NOT_FOUND`: 资源不存在。
   - `-32602 INVALID_ARGUMENT`: 参数非法。
   - `-32603 INTERNAL`: 兜底严重异常。
2. **向下兼容设计**:
   - 自定义领域异常 `RpcError`、`VersionConflictError` 均显式继承 Python 标准 `ValueError`，百分百保持现有测试断言（`pytest.raises(ValueError)`）的绿灯。
3. **Trace 贯穿**:
   - 提取请求头或生成的 `request_id` 存入 ContextVar，通过 `logging.Filter` 自动注入 Daemon 每条日志，串联 IPC、推理、SQLite 与 Outbox。

### D4. 状态枚举单一事实来源与无缝映射
- 确立 6 态标准集: `ACTIVE`, `HISTORICAL`, `DELETED`, `PENDING_VERIFY`, `CONFLICT`, `TEMPORARY`。
- Legacy 映射: 在所有输入入口通过 `normalize_status` 将历史出现的 `SUPERSEDED` / `ARCHIVED` 归一为 `HISTORICAL`。
- **解耦重定向逻辑**: 将 `memory_search` 中原本判断 `status == 'SUPERSEDED'` 的重定向规则，改为**只要卡片携带 `superseded_by` 关联指针即执行重定向**，避免状态收敛为 HISTORICAL 后导致记忆重定向追踪断裂。

---

## 6. 已落盘的代码与契约基础

在 `fix/amr-reliability` 分支上，已先行提交了无副作用的标准化基座与共享契约代码：

1. **`docs/rfcs/RFC-AMR-RELIABILITY-FIXES.md`**:
   - 完备的 RFC v2 设计全案，包含 22 项评审反馈修正对比、文件归属互斥矩阵与 10 条验收标准。
2. **`src/core/errors.py`**:
   - 跨模块领域异常定义：`VersionConflictError(ValueError)`、`ModelLoadingPendingError(Exception)`、`InvalidStatusError(ValueError)`。
3. **`src/interfaces/ipc/errors.py`**:
   - 结构化 JSON-RPC 异常契约：`RpcError(ValueError)` 及其子类 `RpcAgentUnauthorized`, `RpcVersionConflict`, `RpcModelLoading`, `RpcQueueFull`, `RpcNotFound`, `RpcInvalidArgument`。
4. **`src/core/request_context.py`**:
   - 基于 `contextvars.ContextVar` 的请求级跟踪上下文。
5. **`src/service/metrics.py`**:
   - 单进程轻量指标收集器 `Metrics`，内存环形无锁统计 P50/P95 延迟与各维度计数。

---

## 7. 外部审计要点与风险清单（供 GPT 重点核验）

请 GPT 重点围绕以下几个核心技术决策进行严苛推演，审查是否存在遗漏或潜在隐患：

1. **并发与线程安全模型**:
   - 在 Python 3.12 环境下，`engine.py` 同时面对主事件循环协程和 Outbox Worker 线程调用，基于 `threading.Lock` + 代次 `epoch` 的 Single-flight 加载与抛弃策略，是否存在死锁、假死或泄漏显存的边界场景？
2. **最终一致性与 Outbox 事务边界**:
   - `memory_create/update/delete` 将 Qdrant 写入彻底从前台解耦移入 Outbox 后，API 返回 `projection_status: pending`。对于智能体“写完立即检索（Search）”的行为，是否会在几十毫秒的时延内产生“刚写的内容搜不到”的弱一致性体验？现有设计在 `memory_get` 上用 SQLite SSOT 兜底是否足够充分？
3. **陈旧更新覆盖 (Stale Upsert) 规避算法**:
   - 在 Worker 异步消费 `memory_projection_outbox` 时，利用“按 memory_id 分组只消费最新 ID + 校验 version 必须 >= 当前主表 version”的规则，是否能够百分之百杜绝在网络重试、乱序场景下旧快照覆盖新数据的 Bug？
4. **幂等性方案设计**:
   - 基于 `(agent_id_raw, op, idem_key)` 的联合主键，结合直接持久化返回体 JSON 快照的重放策略，在边界情况（如带幂等重试时网络异常中断）下是否完全安全？
5. **异常与兼容性**:
   - 将业务 RPC 异常继承自 `ValueError` 来兼容既有 pytest 测试用例的设计，是否存在被上层普通 `try...except ValueError` 错误拦截进而破坏 JSON-RPC 错误码映射的风险？

---
*(报告完毕，请 GPT 基于以上真实环境、问题成因与详细 RFC 开展代码与架构审计)*
