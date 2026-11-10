# AMR v5 R4 引擎生命周期 / Single-Flight / Watchdog / 并发 红队审计报告

- **审计轨道**：引擎生命周期 / single-flight / watchdog / 并发 红队（R4）
- **审计目标**：`src/core/engine.py`（BGE-M3 向量嵌入引擎与 6 态状态机实现）
- **基线分支**：`audit/r4-engine`（基于 `main @ 1e30119` v5.0.0）
- **审计日期**：2026-11-10

---

## 1. 结论

- **总体可靠性/鲁棒性评分**：**2.5 / 5.0 分**
- **一句话风险判断**：
  引擎在核心 single-flight 冷启动防护与基础 watchdog stall 超时上实现了预期承诺，但在**卸载-加载竞态状态踩踏**、**close() 缺乏防御标志导致的僵尸复活崩溃**、**未 flock 保护的崩溃文件并发写丢失**，以及**未显式 close 时的永久线程与内存泄漏**上存在关键缺陷，高并发生产环境下存在引擎静默挂死与告警失灵的真实风险。
- **核心疑点明确答复**：
  1. **线程泄漏**：**真实存在**。`_watchdog_thread` 因 bound method 强引用环在未调用 `close()` 时阻止 GC 并永久泄漏；`bge-loader` 线程在 `close()` 时未被 join 造成后台悬挂。
  2. **死锁/假死**：**真实存在**。`_on_idle_timeout` 与并发 load 发生竞态时踩踏状态至 `UNLOADED`，导致新模型被工作线程误判丢弃，请求永久挂起直至超时；部分构造（如 Tokenizer 为 None）被挂载为 `READY`，导致引擎永久假死在破损态。
  3. **重复加载**：**在并发冷启动时受 single-flight 有效保护（20 线程严格加载 1 次）；但在 close 后发生恶性非预期重载**。`close()` 后的新请求未被拦截，绕过状态机触发全量重载并在关闭的线程池上报底层 RuntimeError。

---

## 2. 发现清单

### 【HIGH】缺陷清单

#### 1. `_on_idle_timeout` 卸载完成盲目强置 `UNLOADED`，踩踏并发 `LOADING` 状态致使模型丢弃与请求挂死超时
- **where**: `src/core/engine.py:360`
- **what**: 自动闲置卸载完成时无条件调用 `self._set_state(ModelState.UNLOADED)`。若在卸载资源期间有新请求介入触发 `_trigger_load()` 并进入 `LOADING`，该状态会被后完成的卸载线程盲目踩踏回 `UNLOADED`。工作线程完成后因 `state == UNLOADED != LOADING` 误判为作废代次而丢弃正常加载的模型，等待者在 `_wait_for_ready` 中死等至超时抛出 `ModelLoadingPendingError`。
- **repro**: `tests/audit_r4_lifecycle_matrix.py::test_race_idle_unload_stomps_loading_state`
- **observed vs expected**:
  - *Observed*: 等待者等待 0.4s 超时抛出 `ModelLoadingPendingError`，日志记录 `Model load result for epoch 1 discarded (current epoch=1, state=UNLOADED)`，引擎停留在 `UNLOADED` 且模型未挂载。
  - *Expected*: 卸载完成时应检查当前状态是否仍为 `UNLOADING`；若已被新加载抢占，不得踩踏 `LOADING` 状态，新请求应正常就绪并返回 embedding 结果。
- **root_cause**: `_on_idle_timeout` 在释放 `_do_unload_resources()` 之后未重新校验 `self._state == ModelState.UNLOADING`，直接无条件执行 `self._set_state(ModelState.UNLOADED)`。
- **status**: **已复现（测试失败）**
- **proposed_fix**:
  ```python
  # src/core/engine.py:360
  try:
      self._do_unload_resources()
      with self._state_lock:
          if self._state == ModelState.UNLOADING:
              self._set_state(ModelState.UNLOADED)
  except Exception as e:
  ```

---

#### 2. `crash_count_path` 缺乏跨进程/跨线程文件锁，导致并发 Watchdog 崩溃写入丢失更新
- **where**: `src/core/engine.py:175-195`, `204-225`
- **what**: `_record_crash_and_exit()` 与 `_check_and_prune_crash_counts()` 在读-改-写 `crash_count_path` 时仅依赖 `temp_file.replace` 的原子替换，缺少排他文件锁（`fcntl.flock`）。当多个 worker 因 stall 同时触发退出时，后写覆盖先写，造成崩溃时间戳严重丢失。
- **repro**: `tests/audit_r4_watchdog.py::test_crash_count_concurrent_writes_lost_updates`
- **observed vs expected**:
  - *Observed*: 5 个引擎实例因 stall 同时调用 `_record_crash_and_exit()` 后，最终文件中仅记录了 1 条时间戳（4 条更新被丢弃）。后续启动时无法达到 `>= 3` 阈值，导致 `repeated watchdog exits` 告警失灵。
  - *Expected*: 5 次崩溃均应被可靠追加保留；达到 `>= 3` 阈值时触发 `logger.critical("repeated watchdog exits")`。
- **root_cause**: 读原文件有效时间戳、在内存追加当前时间、写临时文件、原子替换，整个过程不是一个受保护的原子事务，缺少 OS 级 `fcntl.flock(fd, fcntl.LOCK_EX)`。
- **status**: **已复现（测试失败）**
- **proposed_fix**:
  对 `crash_count_path` 的读取与写入通过 `fcntl.flock` 进行排他锁保护，或以 `open(path, "a")` 单行追加配合缓冲刷新。

---

#### 3. `BGEM3Engine` 缺少 `_closed` 保护，`close()` 后发起请求异常复活并崩溃于底层 `RuntimeError`
- **where**: `src/core/engine.py:608-656`, `713-727`
- **what**: `close()` 后未维护 `self._closed = True` 标志。后续调用 `embed()` 时，引擎看到当前状态为 `UNLOADED`，盲目触发 `_trigger_load()` 重新反序列化加载权重，但在 `_executor` 执行推理时抛出非预期的底包 `RuntimeError: cannot schedule new futures after shutdown`，引擎被异常复活成了缺少 watchdog 与 executor 的破损僵尸状态。
- **repro**: `tests/audit_r4_lifecycle_matrix.py::test_request_after_close_raises_engine_closed`
- **observed vs expected**:
  - *Observed*: 抛出原生 `RuntimeError('cannot schedule new futures after shutdown')`，模型被加载进内存但 watchdog 已停止、线程池已关闭，引擎被异常推进至 `READY`/`IDLE` 僵尸状态。
  - *Expected*: `close()` 后的任何请求应在入口处被安全拦截，抛出明确的结构化 `EngineError("Engine is closed")`。
- **root_cause**: 引擎缺少明确的已关闭标志与状态防御机制，`close()` 只是把状态重置为 `UNLOADED`，而公有入口方法视 `UNLOADED` 为冷启动可触发状态。
- **status**: **已复现（测试失败）**
- **proposed_fix**:
  ```python
  # src/core/engine.py:713
  def close(self):
      with self._state_lock:
          self._closed = True
      ...

  # 在 embed() / load_model() / preload() 入口检查：
  if getattr(self, "_closed", False):
      raise EngineError("Engine has already been closed")
  ```

---

### 【MEDIUM】缺陷清单

#### 4. `close()` 在多批次推理中途中断正在执行的请求，直接抛出未捕获底包 `RuntimeError`
- **where**: `src/core/engine.py:724`, `649`
- **what**: `close()` 立即执行 `self._executor.shutdown(wait=False)` 并清空模型，未对当前活跃请求（`_active_requests`）做任何排空或优雅中断，正在执行后续 batch 的协程遭遇底包 `RuntimeError` 崩溃。
- **repro**: `tests/audit_r4_lifecycle_matrix.py::test_close_during_multi_batch_embed_raises_engine_closed`
- **observed vs expected**:
  - *Observed*: 活跃 `embed()` 任务在未完成的 batch 提交时遭遇 `RuntimeError: cannot schedule new futures after shutdown`。
  - *Expected*: `close()` 应当优雅等待活跃请求在限定超时内完成，或统一捕获底包异常映射为受管的 `EngineError`。
- **root_cause**: `close()` 不检查 `self._active_requests`，直接执行不等待的 `_executor.shutdown(wait=False)`。
- **status**: **已复现（测试失败）**
- **proposed_fix**:
  `close()` 增加最长优雅等待时间（例如 2.0s）：
  ```python
  wait_until = time.monotonic() + 2.0
  while time.monotonic() < wait_until:
      with self._queue_lock:
          if self._active_requests == 0:
              break
      time.sleep(0.02)
  self._executor.shutdown(wait=True, cancel_futures=True)
  ```

---

#### 5. `_queue_lock` 与 `_state_lock` 未原子闭合，前序请求 `finally` 将正在推理中的活跃引擎置为 `IDLE`
- **where**: `src/core/engine.py:664-673`
- **what**: `embed()` 的 `finally` 块中，在释放 `_queue_lock` 后再获取 `_state_lock`。若在锁间隙到达新请求，新请求使 `_active_requests` 变为 1，但前序请求仍依据局部的 `remaining == 0` 将引擎置为 `IDLE`。
- **repro**: `tests/audit_r4_lifecycle_matrix.py::test_race_queue_lock_and_state_lock_demotes_active_engine_to_idle`
- **observed vs expected**:
  - *Observed*: 状态变为 `IDLE`，但 `status["queue_depth"] == 1`，引擎在有活跃请求时被降级为闲置状态。
  - *Expected*: 只要有活跃请求（`_active_requests > 0`），引擎状态必须保持 `READY`。
- **root_cause**: 活跃计数读取判定与状态变更未处于同一互斥锁区间中。
- **status**: **已复现（测试失败）**
- **proposed_fix**:
  ```python
  # src/core/engine.py:664
  finally:
      with self._queue_lock, self._state_lock:
          self._active_requests -= 1
          if self._active_requests == 0 and self._state == ModelState.READY:
              if self.idle_timeout_seconds > 0:
                  self._state = ModelState.IDLE
                  self._schedule_idle_unload()
  ```

---

#### 6. 模型部分构造（如 Tokenizer 为 None）被无条件推进至 `READY`，导致引擎永久死锁在破损就绪态
- **where**: `src/core/engine.py:421-432`
- **what**: `_load_worker` 仅检查 loader 是否抛出异常（`if success:`），未校验 `tokenizer is not None and model is not None`。若 loader 返回 `(None, Model)`，引擎进入 `READY`，随后所有推理请求抛错；且因状态为 `READY`，`load_model()` 判定已就绪直接跳过，引擎永久处于无法自愈的破损就绪态。
- **repro**: `tests/audit_r4_cross_loop_exceptions.py::test_partial_loader_construction_wedges_engine_in_ready`
- **observed vs expected**:
  - *Observed*: `engine.state == ModelState.READY`，`engine.tokenizer is None`。
  - *Expected*: 任一关键组件为 None 时应判定加载失败，进入 `ERROR` 状态。
- **root_cause**: `_load_worker` 缺少对组件有效性的防御性校验。
- **status**: **已复现（测试失败）**
- **proposed_fix**:
  ```python
  # src/core/engine.py:421
  if success and tokenizer is not None and model is not None:
      self.tokenizer = tokenizer
      self.model = model
      ...
  else:
      self._set_state(ModelState.ERROR, error_msg=error_msg or "Model or tokenizer partially constructed (None)")
  ```

---

#### 7. 未显式调用 `close()` 的引擎因 Watchdog 线程强引用发生永久实例与线程泄漏
- **where**: `src/core/engine.py:122-128`
- **what**: `_watchdog_thread` 绑定 `target=self._watchdog_loop`，导致 `threading._active` 持有线程，线程持有 bound method，bound method 持有 `engine`。若调用方丢失 engine 引用而未调用 `close()`，engine 永久无法被 GC 回收，Watchdog 线程以 50ms 频率永久空转。
- **repro**: `tests/audit_r4_backpressure_resources.py::test_unclosed_engine_watchdog_thread_and_memory_leak`
- **observed vs expected**:
  - *Observed*: `del engine; gc.collect()` 后，`ref() is not None`，且 `wd_thread.is_alive() is True`。
  - *Expected*: 引擎应具备 `__del__` 终结器，或在守护线程中使用弱引用 `weakref.ref(self)`。
- **root_cause**: 强引用环与缺少 `__del__` 终结机制。
- **status**: **已复现（测试失败）**
- **proposed_fix**:
  ```python
  def __del__(self):
      try:
          self.close()
      except Exception:
          pass
  ```

---

#### 8. loader 线程在 `close()` 后未被 join，长期悬挂并持有 `_load_exec_lock` 产生线程泄漏
- **where**: `src/core/engine.py:478-484`, `713-727`
- **what**: `_trigger_load()` 启动的 `bge-loader-{epoch}` 线程在 `close()` 中未保留句柄亦未执行 join。当 loader 遭遇慢 I/O 时，线程在后台泄漏运行，并持续占用 `_load_exec_lock`。
- **repro**: `tests/audit_r4_backpressure_resources.py::test_loader_thread_not_joined_on_close`
- **observed vs expected**:
  - *Observed*: `engine.close()` 返回后，`bge-loader-1` 线程仍在活跃运行。
  - *Expected*: `close()` 应当安全记录并 join loader 线程，确保无线程泄漏。
- **root_cause**: `self._load_thread` 未作为成员变量持久化并在 `close()` 中 join。
- **status**: **已复现（测试失败）**
- **proposed_fix**:
  在 `self._current_loader_thread` 保存线程句柄，在 `close()` 中执行 `self._current_loader_thread.join(timeout=1.0)`。

---

### 【LOW】缺陷清单

#### 9. `last_error` 在故障恢复后未被清理，诊断接口 `get_model_status` 持续上报陈旧错误
- **where**: `src/core/engine.py:323`, `428`
- **what**: `_set_state` 仅在 `error_msg` 为非空时更新 `self._last_error`。当模型从 `ERROR` 成功恢复到 `READY` 时，`error_msg` 为 None，旧的错误文本被永久保留在 `last_error` 字段中。
- **repro**: `tests/audit_r4_backpressure_resources.py::test_stale_last_error_persists_after_recovery`
- **observed vs expected**:
  - *Observed*: 状态恢复为 `READY` 且 `unhealthy=False` 时，`status["last_error"]` 依然残留 `"Disk read failure"`。
  - *Expected*: 成功进入 `READY` 状态时，应当清理 `self._last_error = None`。
- **root_cause**: `_set_state` 在非错误流转时缺少对 `_last_error` 的重置逻辑。
- **status**: **已复现（测试失败）**
- **proposed_fix**:
  在 `_load_worker` 成功分支或 `_set_state` 的非 ERROR 转移中执行 `self._last_error = None`。

---

#### 10. 多个 `BGEM3Engine` 实例化时相互污染全局 Gauge 指标 `model_unhealthy`
- **where**: `src/core/engine.py:131`, `147`
- **what**: `__init__` 中无条件执行 `METRICS.set_gauge("model_unhealthy", 0.0)`。若已有引擎处于 `unhealthy=True`（gauge=1.0），新建另一个引擎实例将抹杀报警指标。
- **repro**: `tests/audit_r4_backpressure_resources.py::test_multiple_engine_metrics_pollution`
- **observed vs expected**:
  - *Observed*: Engine 1 处于故障时，新建 Engine 2 直接将全局 gauge 抹为 0.0。
  - *Expected*: 指标应按引擎实例区分 label，或仅在状态迁移时维护全局聚合 gauge。
- **root_cause**: 单例指标对象 METRICS 中的无标签 gauge 无法支撑多实例生命周期共存。
- **status**: **已复现（测试失败）**
- **proposed_fix**:
  避免在 `__init__` 中无条件清零 gauge，或对指标打上实例 tag。

---

## 3. 已验证的攻面（通过的测试覆盖的风险）

本轮红队新增 34 个测试用例，其中 **24 个用例全部通过**，证明以下设计机制在常规并发与正常路径下按预期运作：
1. **Single-flight 并发冷启动**：
   - 20 个独立线程各自 `asyncio.run(engine.embed(...))` 并发冷启动，底层 `_load_model_sync` 被单飞执行锁保护，**严格调用恰好 1 次**，20 个线程全部获得 1024 维 embedding 结果（`test_twenty_threads_cold_start_singleflight`）。
2. **多事件循环预算超时解耦**：
   - 两个事件循环并发执行，短预算请求超时抛出 `ModelLoadingPendingError(retry_after_ms=1000)`，共享加载不被终止且最终成功转入 `READY`（`test_concurrent_event_loops_timeout_budget_shared_load_completes`）。
3. **过期 Epoch 迟到结果安全丢弃**：
   - 加载期间主动 `unload_model()`，迟到返回的模型被丢弃，不挂载且状态保持 `UNLOADED`（`test_late_loader_discarded_after_unload`）。
   - 加载期间调用 `close()`，迟到返回的模型被丢弃，不挂载且状态保持 `UNLOADED`（`test_late_loader_discarded_after_close`）。
   - 加载期间 watchdog 超时转入 `ERROR`，迟到返回的模型被丢弃，状态保持 `ERROR` 且 `unhealthy=True`（`test_late_loader_discarded_after_watchdog_timeout`）。
4. **状态矩阵合法流转与等待者语义**：
   - `UNLOADED -> LOADING -> READY -> IDLE -> UNLOADING -> UNLOADED` 全生命周期正常流转（`test_state_matrix_happy_path`）。
   - 手工置 `LOADING` 时多个并发请求自旋排队，转入 `READY` 后全量成功唤醒（`test_manual_loading_waiter_semantics`）；超时抛出 `ModelLoadingPendingError`（`test_manual_loading_waiter_timeout`）。
   - `close()` 与 `preload()` 重复调用的幂等性得到验证（`test_idempotent_close_and_preload`）。
5. **Watchdog 超时与受控崩溃退出**：
   - 卡死 loader 超过 `load_timeout_seconds` 正确置 `ERROR`、`unhealthy=True` 并累加指标 `model_load_failures_total`（`test_loader_stall_watchdog_timeout_marks_error_and_unhealthy`）。
   - `watchdog_exit_enabled=True` 时卡死达到 `load_stall_exit_seconds`，注入的 `exit_fn` **恰好调用一次且 code=1**，测试进程不退出（`test_watchdog_exit_fn_called_exactly_once`）。
   - 启动时 crash 文件时间戳清理（保留 <= 600s，清理 > 600s，全过期置空）正确（`test_crash_count_window_and_pruning`, `test_crash_count_all_expired_becomes_empty`）。
   - 10 分钟内 >= 3 条记录触发 `CRITICAL repeated watchdog exits` 告警（`test_crash_count_repeated_exits_critical_alert`）。
   - `idle_timeout <= 0` 彻底禁用自动卸载，`embed` 后状态保持 `READY`；`idle_timeout > 0` 定时卸载转入 `UNLOADED`（`test_idle_timeout_zero_disabled`, `test_idle_timeout_negative_disabled`, `test_idle_timeout_positive_auto_unloads`）。
6. **背压队列与活跃计数准确性**：
   - `max_queue_size` 边界处恰好满正常执行，超出限制立即抛出 `QueueFullError(Inference queue full (N/N))`，完成后深度归零（`test_max_queue_size_exact_boundary_and_overload`）。
   - 并发活跃请求计数与 `total_inferences` 累加严格一致且归零（`test_concurrent_embed_active_requests_accounting_accuracy`）。
   - `get_model_status` 诊断字段完备存在（`test_get_model_status_diagnostics_fields`）。
7. **跨事件循环调用与异常自愈**：
   - 主 loop 与独立 Worker loop 反复交替调用，未出现 `Future attached to a different loop`（`test_alternating_independent_event_loops`）。
   - 多线程独立事件循环并发推理未发生交叉挂死（`test_concurrent_threads_independent_event_loops`）。
   - loader 同步异常正确映射为 `EngineError` 并置 `ERROR` 状态（`test_loader_sync_exception_transitions_to_error`）。
   - unload 抛错后再次加载能够自愈并恢复 `READY`（`test_unload_failure_transitions_to_error_and_recovers`）。

---

## 4. 未覆盖/无法验证

1. **真实 Tesla P4 GPU / CUDA 驱动锁与 Segfault**：
   受审计规则严格限制（绝不加载真实权重 / 禁止使用真实 GPU / 不得触发物理 CUDA 驱动崩溃），真实驱动层死锁（如 CUDA OOM 导致的内核层上下文损坏）仅通过在线程池中模拟驱动抛错与延迟来验证，未进行真实硬件加电破坏性测试。
2. **操作系统断电 / SIGKILL 极限制裁**：
   子进程内验证了受控退出与 `exit_fn(1)`，但物理断电时 OS Page Cache 刷盘延迟对 `crash_count_path` 的最终持久性未作硬件级断电复现。

---

## 5. 测试文件清单与运行结果

### 新增审计测试文件清单
- `tests/audit_r4_singleflight.py`（Single-flight 20 线程、多事件循环、过期代次丢弃）
- `tests/audit_r4_lifecycle_matrix.py`（6 态流转、手工 LOADING 等待、卸载-加载竞态、close 防御缺陷）
- `tests/audit_r4_watchdog.py`（Watchdog 超时与受控退出、crash_count 清理与并发覆盖缺陷、idle 超时）
- `tests/audit_r4_backpressure_resources.py`（背压边界、线程泄漏、指标污染、诊断字段）
- `tests/audit_r4_cross_loop_exceptions.py`（跨 loop 并发、异常处理、部分构造缺陷、自愈）

### 运行结果尾部摘要

#### 1. 独立审计套件运行结果
```
$ pytest tests/audit_r4_*.py -q
...FFFF...F.....FFFF..........F...                                       [100%]
=========================== short test summary info ============================
FAILED tests/audit_r4_backpressure_resources.py::test_stale_last_error_persists_after_recovery
FAILED tests/audit_r4_backpressure_resources.py::test_unclosed_engine_watchdog_thread_and_memory_leak
FAILED tests/audit_r4_backpressure_resources.py::test_multiple_engine_metrics_pollution
FAILED tests/audit_r4_backpressure_resources.py::test_loader_thread_not_joined_on_close
FAILED tests/audit_r4_cross_loop_exceptions.py::test_partial_loader_construction_wedges_engine_in_ready
FAILED tests/audit_r4_lifecycle_matrix.py::test_race_idle_unload_stomps_loading_state
FAILED tests/audit_r4_lifecycle_matrix.py::test_request_after_close_raises_engine_closed
FAILED tests/audit_r4_lifecycle_matrix.py::test_close_during_multi_batch_embed_raises_engine_closed
FAILED tests/audit_r4_lifecycle_matrix.py::test_race_queue_lock_and_state_lock_demotes_active_engine_to_idle
FAILED tests/audit_r4_watchdog.py::test_crash_count_concurrent_writes_lost_updates
10 failed, 24 passed in 12.58s
```

#### 2. 全量套件运行结果（原有 336 既有用例 100% 保持绿色，10 个缺陷失败用例精准定点复现）
```
$ pytest tests/ -o "python_files=test_*.py audit_*.py" -q
... [全量套件运行] ...
=========================== short test summary info ============================
FAILED tests/audit_r4_backpressure_resources.py::test_stale_last_error_persists_after_recovery
FAILED tests/audit_r4_backpressure_resources.py::test_unclosed_engine_watchdog_thread_and_memory_leak
FAILED tests/audit_r4_backpressure_resources.py::test_multiple_engine_metrics_pollution
FAILED tests/audit_r4_backpressure_resources.py::test_loader_thread_not_joined_on_close
FAILED tests/audit_r4_cross_loop_exceptions.py::test_partial_loader_construction_wedges_engine_in_ready
FAILED tests/audit_r4_lifecycle_matrix.py::test_race_idle_unload_stomps_loading_state
FAILED tests/audit_r4_lifecycle_matrix.py::test_request_after_close_raises_engine_closed
FAILED tests/audit_r4_lifecycle_matrix.py::test_close_during_multi_batch_embed_raises_engine_closed
FAILED tests/audit_r4_lifecycle_matrix.py::test_race_queue_lock_and_state_lock_demotes_active_engine_to_idle
FAILED tests/audit_r4_watchdog.py::test_crash_count_concurrent_writes_lost_updates
10 failed, 360 passed, 6 skipped, 7 warnings in 55.40s
```
*(注：全量既有 336 个测试用例全部 passed，无一受损；新增 34 个审计用例中，24 个 passed 验证安全边界，10 个 failed 精准锁死发现清单中报告的产品缺陷。)*
