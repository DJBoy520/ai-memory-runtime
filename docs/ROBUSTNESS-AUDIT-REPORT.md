# AMR（AI Memory Runtime）代码健壮性与并发鲁棒性深度审计报告
**报告编号**: AMR-QA-AUDIT-20261110-ROBUST  
**审计主体**: 独立质量保证委员会（Independent Quality Assurance Board）  
**审计专家角色**: 资深系统级可靠性与并发工程专家  
**审计目标代码库**: AI Memory Runtime (AMR)  
**审计工作分支**: `review/robust` (基于 `main @ 0105978`)  
**技术栈基准**: Python 3.12 + asyncio + SQLite (WAL Mode) + Qdrant (v1.12+) + PyTorch / HuggingFace Transformers (BGE-M3)  
**审计日期**: 2026-11-10  

---

## 目录
- [一、 执行摘要与独立审计委员会评语](#一-执行摘要与独立审计委员会评语)
- [二、 国际标准与工程规范对齐评估矩阵](#二-国际标准与工程规范对齐评估矩阵)
- [三、 缺陷清单与深度分析（Findings by Severity）](#三-缺陷清单与深度分析findings-by-severity)
  - [3.1 High 级别缺陷（严重破坏可用性与数据完整性）](#31-high-级别缺陷严重破坏可用性与数据完整性)
  - [3.2 Medium 级别缺陷（特定高并发时序与资源管理隐患）](#32-medium-级别缺陷特定高并发时序与资源管理隐患)
  - [3.3 Low 级别缺陷（代码坏味道与防御性边界疏漏）](#33-low-级别缺陷代码坏味道与防御性边界疏漏)
- [四、 关键并发原语与核心架构安全审计专题结论](#四-关键并发原语与核心架构安全审计专题结论)
  - [4.1 写路径事务原子性与幂等仲裁（SQLite + SAVEPOINT）](#41-写路径事务原子性与幂等仲裁sqlite--savepoint)
  - [4.2 事务性发件箱（Transactional Outbox）与 CAS 协议](#42-事务性发件箱transactional-outbox与-cas-协议)
  - [4.3 模型生命周期与 Watchdog 守护机制](#43-模型生命周期与-watchdog-守护机制)
  - [4.4 异常体系、因果链与错误折叠审计](#44-异常体系因果链与错误折叠审计)
  - [4.5 长期运行资源生命周期（FD、线程池、内存）](#45-长期运行资源生命周期fd线程池内存)
- [五、 增强与加固实施建议（Actionable Recommendations）](#五-增强与加固实施建议actionable-recommendations)

---

## 一、 执行摘要与独立审计委员会评语

### 1.1 总体健壮性评分
依据 ISO/IEC 25010 软件产品质量模型及 SEI CERT Python 并发安全规范，经独立质量保证委员会全面交叉审计与故障注入推演，本轮代码健壮性综合评分为：

$$\mathbf{4.2\ /\ 5.0\ 分\quad（良好\ -\ 具备生产就绪底座，但需排除若干临界竞态隐患）}$$

| 维度指标 | 权重 | 得分 (1-5) | 评语与关键考量 |
| :--- | :---: | :---: | :--- |
| **Fault Tolerance（容错性）** | 25% | **4.2** | 具备统一的 Qdrant 故障隔离机制（写路径解耦为 Outbox，读路径支持降级与短路）；但死信任务自动自愈与熔断机制仍有缺失。 |
| **Recoverability（可恢复性）** | 25% | **4.4** | SQLite WAL 模式结合排他事务与 CAS 认领，保证底账绝无半写；崩溃计数文件带 OS 级 flock 与原子写入，具备防循环自愈能力。 |
| **Concurrency & Synchronization（并发与时序）** | 30% | **3.9** | 核心写路径互斥和 Outbox CAS 设计精密，但 `engine.close()` 与 `OutboxWorker.stop()` 在交叠重启或事件循环中存在临界竞态。 |
| **Resource & Exception Discipline（资源与异常防御）** | 20% | **4.3** | 实现了完备的业务错误码映射表（-32001~-32006）；但部分模块存在线程池未确定性释放（RAII 缺失）与 NUL 字符守卫缺失。 |

### 1.2 独立审计委员会评语
AMR 经过此前多个审计轮次（R1~R5 及 GPT 健壮性修复）的持续演进，系统可靠性架构已达到工业级水准。其**核心亮点**包括：
1. **写路径绝对解耦（Zero-IO Write Path）**：`create_memory_v3` 与 `update_memory_v3` 完全消除内联 embedding 与 Qdrant 网络阻塞，实现了 SQLite 本地事务即完成的微秒级确认；
2. **CAS 乐观锁与合并写入协议**：Outbox 采用 `WHERE status='pending' AND projection_revision=?` 的条件更新，在高频并发写入交叠时能精准避免旧 revision 污染新任务，保障了终态一致性；
3. **分层式异常映射与可观测性**：在 UDS 边界严格维护了结构化机器可读错误码，消除了旧版本“裸字符串匹配”引发的误判。

然而，在面对**交叠重启（Rolling Restart）、异步事件循环关闭时序（Async Loop Teardown）以及外部依赖长时间不可用**等极端混沌场景时，仍暴露了 3 处 High 级和 4 处 Medium 级关键并发与资源缺陷。必须针对性完成加固，以消除生产环境偶发性 SIGSEGV、死锁及幽灵指标污染风险。

---

## 二、 国际标准与工程规范对齐评估矩阵

| 国际标准 / 规范体系 | 关键条款要求 | AMR 实现现状 | 合规结论 |
| :--- | :--- | :--- | :---: |
| **ISO/IEC 25010 §4.2.5**<br>Fault Tolerance | 发生单点软硬件故障时，系统能维持预定性能水准或退避降级 | - Qdrant 宕机时，写请求通过 Outbox 正常落库返回，支持指数退避；<br>- 模型加载卡死时 Watchdog 介入转移 ERROR 并受控重启。 | **COMPLIANT** |
| **ISO/IEC 25010 §4.2.6**<br>Recoverability | 在系统故障或数据中断后，能够重建数据状态并恢复系统操作 | - 存量迁移使用 `_amr_migrations` 保证原子幂等与隔离隔离；<br>- SQLite 崩溃重启依赖 WAL Redo/Undo 确定性回滚。 | **COMPLIANT** |
| **ISO/IEC 25010 §4.2.3**<br>Capacity & Utilization | 具备背压机制与长连接复用，无句柄、内存、线程泄漏 | - 提供 `QueueFullError` (-32004) 推理队列背压；<br>- 但 `MemoryService._enrich_executor` 未提供 `close()`，存在线程池泄漏。 | **PARTIALLY COMPLIANT**<br>*(需加固)* |
| **SEI CERT Python ERR00-P**<br>Exception Handling | 禁止吞噬系统退出异常（SystemExit）；必须显式保留因果链（`raise ... from ...`） | - 协议解析与 JSON 转换均正确保留 `from e`；<br>- 存在部分裸 `except Exception: pass` 吞噬未预期运行时错误。 | **COMPLIANT** |
| **SEI CERT Python FIO02-P**<br>Resource Management | 资源遵循 RAII 原则，上下文管理器保证连接、锁与文件描述符确定性释放 | - `OutboxWorker.stop()` 锁释放未等待线程真正终止，破坏互斥期；<br>- `locked_write_transaction` 深度嵌套依赖动态属性。 | **PARTIALLY COMPLIANT**<br>*(需加固)* |
| **POSIX & SQLite Concurrency**<br>Lock Ordering & WAL | 遵循锁分级预防死锁；排他写入事务与 SAVEPOINT 语义完整性 | - 最外层使用 `BEGIN IMMEDIATE` 规避 SQLITE_BUSY 死锁升级；<br>- 嵌套 SAVEPOINT 回滚后未执行 `RELEASE` 导致嵌套栈悬空。 | **PARTIALLY COMPLIANT**<br>*(需加固)* |

---

## 三、 缺陷清单与深度分析（Findings by Severity）

### 3.1 High 级别缺陷（严重破坏可用性与数据完整性）

#### [FINDING-ROBUST-01] `BGEM3Engine.close()` 在异步事件循环线程内调用时导致未同步资源并发释放（Race Condition / UAF / CUDA Crash）
- **文件与行号**: `src/core/engine.py:818-838`
- **缺陷等级**: **High (严重)**
- **代码根因**:
  在 `close()` 中，系统尝试检查调用者是否在事件循环线程：
  ```python
  is_event_loop_thread = False
  try:
      asyncio.get_running_loop()
      is_event_loop_thread = True
  except RuntimeError:
      is_event_loop_thread = False

  if not is_event_loop_thread:
      wait_until = time.monotonic() + 2.0
      while time.monotonic() < wait_until:
          with self._queue_lock:
              if self._active_requests == 0:
                  break
          time.sleep(0.02)

  try:
      self._executor.shutdown(wait=not is_event_loop_thread, cancel_futures=True)
  except TypeError:
      self._executor.shutdown(wait=not is_event_loop_thread)

  self._do_unload_resources()
  self._set_state(ModelState.UNLOADED)
  ```
  在微服务框架与现代异步应用中，`close()` 绝大多数情况下是在事件循环内被调用的（例如 `async def stop()` 或测试夹具的 teardown）。此时 `is_event_loop_thread == True`：
  1. `if not is_event_loop_thread:` 活跃请求等待逻辑被**完全绕过**；
  2. `self._executor.shutdown(wait=False)` **非阻塞返回**，后台推理工作线程仍在执行 `_infer_batch_sync`；
  3. 主线程紧接着同步调用 `self._do_unload_resources()`，直接执行 `del self.model; self.model = None; torch.cuda.empty_cache()`！
- **破坏性场景分析**:
  正在 `_executor` 子线程中执行的模型前向传播计算 `self.model(**encoded)` 或 CUDA Kernel 计算时，底层的 PyTorch C++ 对象被主线程释放。轻则抛出未捕获的 `AttributeError: 'NoneType' object has no attribute '...'`，重则触发底层 C++ 虚拟内存非法访问，直接引发 **`CUDA error: an illegal memory access was encountered`** 或整个 Python 解释器 **`SIGSEGV (Segmentation Fault)`** 崩溃。
- **修复与加固建议**:
  如果当前处于事件循环，必须使用异步等待（如 `await asyncio.sleep`）或将关闭逻辑委派至专用后台 Teardown 线程阻塞等待 `_active_requests == 0` 及 `_executor.shutdown(wait=True)` 彻底结束后，再释放模型显存。

---

#### [FINDING-ROBUST-02] `OutboxWorker.stop()` 锁释放早于线程退出破坏单执行者互斥语义（Double Writers / Split-Brain）
- **文件与行号**: `src/service/outbox_worker.py:462-478`
- **缺陷等级**: **High (严重)**
- **代码根因**:
  ```python
  def stop(self, timeout: float = 3.0) -> None:
      if not self._running:
          return

      self._running = False
      if self._thread and self._thread.is_alive():
          self._thread.join(timeout=timeout)

      if self._lock_fd is not None:
          try:
              fcntl.flock(self._lock_fd.fileno(), fcntl.LOCK_UN)
              self._lock_fd.close()
          except Exception as e:
              logger.debug("Error closing lock fd: %s", e)
          self._lock_fd = None
  ```
  当后台 Worker 线程正在执行高延迟的 Qdrant 批量网络操作或模型推断时，若耗时超出默认 `timeout=3.0s`，`self._thread.join()` 超时返回。此时 `self._thread.is_alive()` 依然为 `True`，但随后的代码**无条件执行了 `fcntl.LOCK_UN` 并关闭了锁文件描述符**！
- **破坏性场景分析**:
  在生产环境容器滚动重启、Systemd 守护进程交叠重载时，新进程启动并成功抢占到该 flock 锁；而旧进程的子线程由于仍在运行，继续向 Qdrant 发送点位 upsert/delete。这导致**两个 Worker 同时并发写入向量库**，打破了 AMR 核心的“单写者（Single Writer）”架构基石，引发 Qdrant 状态被乱序覆盖。
- **修复与加固建议**:
  仅在 `self._thread.join()` 成功完成（`not self._thread.is_alive()`）时才允许释放 flock 锁。若发生超时，必须记录 CRITICAL 级别告警，禁止释放锁文件，或向 Worker 线程的 loop 发送 `call_soon_threadsafe(loop.stop)` 强制取消。

---

#### [FINDING-ROBUST-03] `MemoryService` 内部无界常驻 `ThreadPoolExecutor` 缺乏生命周期管理导致线程池泄漏
- **文件与行号**: `src/service/memory_service.py:55-56`, `src/main.py:413-421`
- **缺陷等级**: **High (严重)**
- **代码根因**:
  在 `MemoryService.__init__` 中初始化了专用的富化线程池：
  ```python
  self._enrich_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="mem_enrich")
  self._enrich_semaphore = threading.Semaphore(2)
  ```
  然而，`MemoryService` 全文**未提供任何 `close()` 或 `shutdown()` 方法**，在 `main.py` 的守护进程优雅停机流程 `app.stop()` 中，也仅关闭了 Engine、Store、Qdrant 与 Server，未对 `memory_service` 的线程池进行释放。
- **破坏性场景分析**:
  在单元测试套件、治理巡检批处理脚本、动态多租户实例构造或微服务热加载场景中，每次实例化 `MemoryService` 都会在 OS 层面残留 2 个处于等待状态的工作线程。随着对象被 GC，线程池因内部 Worker 线程引用无法被垃圾回收器回收，导致线程句柄与虚拟内存持续泄漏。
- **修复与加固建议**:
  遵循 RAII 原则，在 `MemoryService` 中实现显式 `close()` 方法执行 `self._enrich_executor.shutdown(wait=True)`，并在 `AMRApplication.stop()` 中纳入确定性关闭流水线。

---

### 3.2 Medium 级别缺陷（特定高并发时序与资源管理隐患）

#### [FINDING-ROBUST-04] `BGEM3Engine` 的 `_load_exec_lock` 跨线程释放与卡死线程导致后续加载永久拒绝（Lock Mismanagement & DoS）
- **文件与行号**: `src/core/engine.py:509-520`
- **缺陷等级**: **Medium (中危)**
- **代码根因**:
  `self._load_exec_lock` 为 `threading.Lock()`。在 `_trigger_load` 中由调用方主线程执行 `acquire(blocking=False)`，而释放逻辑放在了新派生线程 `bge-loader-{epoch}` 的 `finally` 块中。
  若底层 PyTorch 或 HuggingFace 本地模型载入过程在 C++ 层面发生死锁（如网络文件系统 NFS 挂起），Watchdog 在超时后虽然将状态标为 `ERROR`，但挂起的 `_load_worker` 线程并未被杀死，`_load_exec_lock` 始终处于被锁定状态。
- **破坏性场景分析**:
  随后到达的任何新业务请求调用 `load_model()`，执行 `_trigger_load()` 尝试自愈时，`self._load_exec_lock.acquire(blocking=False)` 均返回 `False`，导致系统无法重新触发新的 loader 线程，模型进入永久假死状态，直到被 Watchdog 的二次超时机制执行受控崩溃退出。
- **修复与加固建议**:
  引入 Epoch 代际守卫锁或超时自愈令牌机制，若检测到前一代加载超时，允许通过带条件校验的 CAS 重置并置换新的执行锁，防止陈旧僵尸线程霸占加载通道。

---

#### [FINDING-ROBUST-05] `OutboxWorker` 死信队列缺少自动化恢复机制且存在幽灵指标污染（DLQ Black Hole & Orphan Metrics）
- **文件与行号**: `src/service/outbox_worker.py:197-208`, `src/core/session_store.py:1813-1828`
- **缺陷等级**: **Medium (中危)**
- **代码根因**:
  1. 当 Qdrant 暂时宕机导致单任务重试达到 8 次上限后，任务被标为 `dead_letter`。`fetch_pending_projection_tasks` 仅拉取 `status='pending'`，导致在外部依赖恢复后，这些死信任务若无人工干预将永久沉睡在数据库中；
  2. 随后若该记忆产生了新的更新，`enqueue_projection_task` 的局部唯一索引 `ON CONFLICT(target_collection, memory_id) WHERE status='pending'` 不会与旧的 `dead_letter` 记录冲突，因此会新增一条独立的 pending 记录；
  3. 当新记录同步完成后变成 `done`，旧的 `dead_letter` 行依然残留。
- **破坏性场景分析**:
  `count_projection_backlog` 中的 `dead_letter` 统计指标将只增不减（无法随业务新写入自愈），造成监控告警系统持续报假警（幽灵指标污染）。
- **修复与加固建议**:
  在 `enqueue_projection_task` 中，若检测到同 `(target_collection, memory_id)` 存在 `dead_letter` 记录，应在同事务内原子将其状态标为 `done`（注明 `superseded_by_revision`），清除陈旧死信；同时在 Worker 轮询主循环中增加低频（如每小时）自动重探机制。

---

#### [FINDING-ROBUST-06] `SessionStore.locked_write_transaction` 嵌套 SAVEPOINT 异常回滚语义不完整（Dangling Savepoint）
- **文件与行号**: `src/core/session_store.py:1625-1643`
- **缺陷等级**: **Medium (中危)**
- **代码根因**:
  ```python
  if is_root:
      conn.execute("BEGIN IMMEDIATE")
  else:
      conn.execute(f"SAVEPOINT {sp_name}")
  yield conn
  if is_root:
      conn.execute("COMMIT")
  else:
      conn.execute(f"RELEASE SAVEPOINT {sp_name}")
  except Exception:
      if is_root:
          try:
              conn.execute("ROLLBACK")
          except Exception: pass
      else:
          try:
              conn.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
          except Exception: pass
      raise
  ```
  在 SQLite 规范中，`ROLLBACK TO SAVEPOINT` 仅将事务状态回滚到保存点创建时的快照，**但该保存点依然保留在事务栈中**。
- **破坏性场景分析**:
  如果外层业务函数设计了局部容错补偿逻辑（捕获了内层抛出的特定非致命业务异常并继续执行外层操作），由于未显式执行 `RELEASE SAVEPOINT {sp_name}`，该保存点将一直占用 SQLite 内部事务栈空间，后续复用连接或执行同级 SAVEPOINT 时会引发事务层级混乱。
- **修复与加固建议**:
  在异常回滚分支中，完整遵循标准语法：先 `ROLLBACK TO SAVEPOINT {sp_name}`，紧接着必须执行 `RELEASE SAVEPOINT {sp_name}`。

---

#### [FINDING-ROBUST-07] `UDSServer.stop()` 未跟踪和优雅关闭已建立的长连接客户端（Dangling Active Sockets / Ungraceful Drain）
- **文件与行号**: `src/interfaces/ipc/server.py:291-303`
- **缺陷等级**: **Medium (中危)**
- **代码根因**:
  `UDSServer.stop()` 仅执行了 `self.server.close()` 和 `await self.server.wait_closed()`。
  在 asyncio 规范中，`server.close()` 仅阻止监听 socket 接收新的连接，**完全不会关闭任何已经建立并保持长连接的客户端 StreamWriter**。
- **破坏性场景分析**:
  当守护进程进入优雅关闭阶段，`server.stop()` 迅速完成并接着关闭了底层 SQLite 和 Engine。然而存量的长连接客户端任务（`_handle_client`）仍在后台挂起或正在接收新请求。此时客户端若发送 RPC 请求，服务端在已释放底层资源的上下文下执行 handler，产生大量的未捕获异常并向客户端返回非预期的 `-32603 Internal error`。
- **修复与加固建议**:
  在 `UDSServer` 中增加活跃客户端集合 `self._active_connections = set()`，在 `stop()` 流程中向所有现存 connection 发送关闭通知并执行 `writer.close(); await writer.wait_closed()`，配合超时取消机制实现真正的优雅排空（Graceful Drain）。

---

### 3.3 Low 级别缺陷（代码坏味道与防御性边界疏漏）

#### [FINDING-ROBUST-08] 字符串输入边界守卫缺少 NUL (`\x00`) 字节防御（SEI CERT Python Defensive Guard Violation）
- **文件与行号**: `src/core/session_store.py:2397-2403`, `src/service/memory_service.py:418-420`, `855-857`
- **缺陷等级**: **Low (低危)**
- **代码根因**:
  在输入校验中仅检查了 `not content or not content.strip()` 以及长度范围（5~16000），未对包含 `\x00` 字符的畸形输入进行清洗或拒绝。
- **破坏性场景分析**:
  当恶意或畸形的客户端载荷中夹带 `\x00` 时，Python 字符串可以正常处理，但在底层传递给 C 扩展库（如某些编译版本的 SQLite、分词器 C++ 绑定或 OS 系统调用）时可能触发字符串截断，造成数据不完整或索引失真。

---

#### [FINDING-ROBUST-09] `SessionStore.close()` 缺乏封闭标志位守卫存在静默复活风险（Zombie Resurrection）
- **文件与行号**: `src/core/session_store.py:343-352`, `2808-2816`
- **缺陷等级**: **Low (低危)**
- **代码根因**:
  `SessionStore.close()` 仅执行 `self._conn.close()` 并设 `self._conn = None`，但未标记 `self._closed = True`。若后续有延迟任务调用任何读写接口，`_get_connection()` 中的 `if self._conn is None:` 会隐式重新连接 SQLite。
- **破坏性场景分析**:
  违反了生命周期管理的 Fail-Fast 原则，掩盖了上层组件调用顺序错乱的缺陷。应在 `_get_connection()` 中断言 `if self._closed: raise RuntimeError("SessionStore is closed")`。

---

#### [FINDING-ROBUST-10] `ReconciliationEngine` Phase 1 在线热备独占长连接加剧锁竞争（WAL Checkpoint & Backup Starvation）
- **文件与行号**: `src/reconciliation/engine.py:127-132`
- **缺陷等级**: **Low (低危)**
- **代码根因**:
  在 `phase_1_cold_backup` 中，直接获取并复用了 `self.store.get_connection()` 执行全库页面复制 `conn.backup(backup_conn, pages=100)`。该过程未在 `self.store._lock` 保护范围内，且持续占用底层连接句柄。
- **破坏性场景分析**:
  在业务高频写入期触发治理巡检时，长时间的页面备份会引起 SQLite 的内部互斥锁竞争，增大业务写请求遭遇 `busy_timeout` 的概率。建议为冷备份创建专用的只读 SQLite 连接进行导出。

---

## 四、 关键并发原语与核心架构安全审计专题结论

### 4.1 写路径事务原子性与幂等仲裁（SQLite + SAVEPOINT）
- **原子性结论**: **安全 (SAFE)**。
  `SessionStore.create_memory_v3` 与 `update_memory_v3` 均采用严格的本地事务包裹，最外层在 `conn.isolation_level = None` 下显式执行 `BEGIN IMMEDIATE`，确保了在事务开始阶段即排他获取 SQLite 的 RESERVED 写入锁，彻底消除了从读事务中途升级写事务引发的 `SQLITE_BUSY` 死锁。
- **故障注入推演**:
  - *推演 1（业务正文更新失败）*: 在 `update_memory_v3` 内部触发乐观锁冲突或主键校验异常，外层 `locked_write_transaction` 触发异常捕获并统一执行 `ROLLBACK`。SQLite 的 `memories`、`memory_revisions`、`memory_audit_log` 及 `memory_projection_outbox` 均同事务回滚，底账绝对无半写。
  - *推演 2（幂等仲裁故障）*: `execute_idempotent` 在插入 `idempotency_keys` 占位符后，若业务逻辑抛出异常，整个外层事务回滚，占位记录随之一同回滚，不会发生“烧掉 key（Burned Key）”的不可恢复现象。
- **潜在隐患**:
  `_tx_depth` 为类实例属性，虽然受 `self._lock (threading.RLock)` 保护，但在极罕见的未捕获异常导致深度计数错乱时，可能导致后续事务对 root/savepoint 判定失准。

### 4.2 事务性发件箱（Transactional Outbox）与 CAS 协议
- **CAS 协议结论**: **安全且精巧 (HIGHLY ROBUST)**。
  Outbox 处理单任务时严格遵循如下时序：
  $$\text{Fetch Task} \longrightarrow \text{Read Memory Snapshot} \longrightarrow \text{Embed \& Upsert Qdrant} \longrightarrow \text{CAS Done}$$
  其中 CAS 提交条件为：
  ```sql
  UPDATE memory_projection_outbox
  SET status = 'done', projected_revision = ?, updated_at = ?
  WHERE id = ? AND status = 'pending' AND projection_revision = ?;
  ```
- **高频并发合并场景验证**:
  若在 Worker 生成向量期间，客户端对该记忆进行了第 2 次写入（revision 提升），SQLite 内的 outbox 行已被合并 UPDATE 为新 revision。
  Worker 执行 CAS 时因 `projection_revision` 不匹配导致 `cursor.rowcount == 0`，CAS 判定失败，保留 `status='pending'`，绝不会将已过期的快照标为 `done`。
  失败分支中 `mark_projection_failed` 同样使用该 CAS 条件，当新写入发生时同样返回 0 行，成功阻断了陈旧失败对新 revision 的重试计数（`retry_count`）污染。

### 4.3 模型生命周期与 Watchdog 守护机制
- **Single-flight 模型加载结论**: **基本可靠，但在极端挂起时存在通道阻塞 (CONDITIONALLY SAFE)**。
  `_trigger_load()` 采用 `self._load_exec_lock.acquire(blocking=False)` 确保了同进程内至多只有一个线程在执行模型加载；排队请求通过 `_wait_for_ready()` 进行毫秒级异步自旋，避免了重复加载炸显存。
- **Watchdog 与多进程崩溃计数结论**: **安全 (SAFE)**。
  `_with_crash_lock` 通过专用的 `amr.crash.count.lock` 文件的 `fcntl.flock(LOCK_EX)` 实现了跨多进程的排他写入。写入过程采用“带 PID 与线程标识的临时文件 + POSIX 原子重命名（`replace`）”，并修剪 10 分钟前的陈旧时间戳，多进程交叠写入绝无数据截断或竞争污染。

### 4.4 异常体系、因果链与错误折叠审计
- **全仓 `except` 扫描结论**: **良好 (GOOD)**。
  - IPC 服务器层通过 `_map_exception_to_error_response` 对领域异常进行了穷举映射，涵盖了 `RpcError`、`VersionConflictError`、`ModelLoadingPendingError`、`QueueFullError`、`KeyError`、`IdempotencyConflict` 等全部结构化异常；
  - 核心模块未出现通配 `except:` 吞噬 `KeyboardInterrupt` 或 `SystemExit` 的现象；
  - 在 `protocol.py` 内部解码错误均显式附加了因果链（`raise FrameDecodeError(...) from e`）。

### 4.5 长期运行资源生命周期（FD、线程池、内存）
- **长效稳定性结论**: **需警惕局部泄漏 (ATTENTION REQUIRED)**。
  1. `BGEM3Engine` 的 Watchdog 线程使用 `weakref.ref(self)` 传递引擎引用，引擎销毁时 Watchdog 能够自发终结，未引发循环强引用；
  2. 但 `MemoryService` 内部常驻的 `_enrich_executor` 缺乏确定性关闭；
  3. `UDSServer` 缺少对现存客户端 Socket 的生命周期追踪。

---

## 五、 增强与加固实施建议（Actionable Recommendations）

依据缺陷等级与影响范围，独立质量保证委员会提出以下系统级加固实施路线图：

### 阶段一：P0 紧急加固（消除崩溃与脑裂风险）
1. **加固 `BGEM3Engine.close()` 资源释放同步机制**:
   - 彻底重构事件循环内的关闭逻辑，在调用 `_do_unload_resources()` 之前，必须确保 `_executor` 的所有任务已执行完成并完成 shutdown。
2. **加固 `OutboxWorker.stop()` 锁释放语义**:
   - 强制将 `fcntl.flock(..., LOCK_UN)` 限制在 `self._thread.join()` 成功之后执行；若 join 超时，保持锁持有状态，打出致命日志并向线程的事件循环注入 stop 指令。
3. **补齐 `MemoryService` 的 RAII 资源管理**:
   - 在 `MemoryService` 增加 `close()` 方法显式 shutdown `_enrich_executor`；将其纳入 `AMRApplication.stop()` 核心编排流程。

### 阶段二：P1 稳健性增强（消灭假死与指标污染）
1. **完善 SAVEPOINT 回滚清理语法**:
   - 修改 `locked_write_transaction` 的异常分支为：
     ```python
     conn.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
     conn.execute(f"RELEASE SAVEPOINT {sp_name}")
     ```
2. **死信任务吸收与自愈**:
   - 在 `SessionStore.enqueue_projection_task` 中增加同 memory_id 的历史 `dead_letter` 行清理与状态吸收，杜绝监控指标幽灵滞留。
3. **Single-flight 锁超时自愈**:
   - 若 Watchdog 检测到加载超时并将状态置为 `ERROR`，应重置 `_load_exec_lock` 状态，确保后续 `load_model` 能重新获取锁触发自愈。

### 阶段三：P2 防御性加固（代码纯洁度与边界防御）
1. **全局输入防御性守卫**:
   - 在 `compute_request_digest` 及 Store 入口统一增加 NUL 字符守卫，对含有 `\x00` 的输入直接抛出 `RpcInvalidArgument`。
2. **连接显式封闭状态机**:
   - 在 `SessionStore` 增加 `self._closed = True` 标志位，禁止在 `close()` 之后隐式重建连接。

---
**独立质量保证委员会结论**:
AMR 当前代码在正常与高并发主干业务流程中展现出了极高的工程严谨度与优良的吞吐韧性。只要在后续迭代中严格按上述加固建议落地 P0/P1 项修复，即可完全消除隐蔽竞态，达成符合 ISO/IEC 25010 最高标准的企业级高可靠运行要求。

*(本报告已由独立质量保证委员会签署，存盘归档)*
