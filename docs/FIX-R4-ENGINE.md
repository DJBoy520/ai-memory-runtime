# AMR v5 R4 引擎生命周期 / Single-Flight / Watchdog / 并发 修复报告

- **修复分支**：`fix/audit-engine`（基于 `main @ 1e30119` v5.0.0）
- **审计参考**：`docs/AUDIT-R4-ENGINE.md`
- **所有权范围**：`src/core/engine.py`、`docs/FIX-R4-ENGINE.md`
- **修复日期**：2026-11-10

---

## 1. 结论

审计报告中披露的 10 项缺陷已全部完成针对性根因修复，未放宽任何断言或修改审计测试规格。
- `tests/audit_r4_*.py`：**34 passed, 0 failed**（原有 24 passed 保持绿色，10 个失败缺陷用例 100% 转绿）。
- `tests/ -q`：**336 passed, 6 skipped**（既有用例 100% 保持绿色）。
- 全量全集（包含既有 + 审计用例）：**370 passed, 6 skipped, 0 failed**。

---

## 2. 改动文件

- `src/core/engine.py`：修复 10 项生命周期、文件锁、异常拦截、引用泄漏与状态机一致性缺陷。
- `docs/FIX-R4-ENGINE.md`：记录修复细节与验收结果。

---

## 3. 10 项缺陷根因分析与修复实现

### 1. `_on_idle_timeout` 卸载完成踩踏并发 `LOADING` 状态
- **根因**：`_on_idle_timeout` 在释放资源 `self._do_unload_resources()` 期间释放了锁，若并发请求到达并触发 `_trigger_load()` 推进至 `LOADING`，卸载线程随后在无重新校验的情况下直接调用 `self._set_state(ModelState.UNLOADED)`，踩踏正在进行的 `LOADING`，导致加载完成的模型被判定作废而丢弃，等待者自旋超时。
- **修复**：在 `_do_unload_resources()` 之后获取 `_state_lock`，仅当当前状态仍为 `ModelState.UNLOADING` 时才转移至 `UNLOADED`（若出现异常且仍为 `UNLOADING` 则置 `ERROR`）；若已被新加载抢占，则不踩踏 `LOADING` 状态。

### 2. `crash_count_path` 并发读改写丢失更新
- **根因**：`_record_crash_and_exit()` 与 `_check_and_prune_crash_counts()` 在读取旧时间戳、追加新记录、写临时文件与 replace 过程中缺少操作系统级互斥锁。当多个并发 worker/线程同时触发 stall 退出时，各自独立执行 read-modify-write，发生后写覆盖先写，造成时间戳丢失。
- **修复**：引入独立锁文件 `<crash_count_path>.lock`，使用 `fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX)` 保护完整的读-清理-追加-写入替换事务，确保并发写操作严格串行化，5 次并发写入全部完整保留。

### 3. `close()` 后请求复活引擎并崩溃
- **根因**：`BGEM3Engine` 缺少 `_closed` 布尔标志，`close()` 仅将状态重置为 `UNLOADED`。后续 `embed()`、`load_model()`、`preload()` 视 `UNLOADED` 为冷启动触发态并重新调用 `_trigger_load()`，导致在已关闭的 `_executor` 上调度任务时暴露底包 `RuntimeError`。
- **修复**：
  1. 实例初始化 `self._closed: bool = False`。
  2. `close()` 首行在 `_state_lock` 临界区判断 `if self._closed: return`，并置 `self._closed = True` 实现幂等关闭。
  3. 在 `embed()`、`load_model()`、`preload()`、`unload_model()` 入口及自旋循环处检查 `self._closed`，已关闭时直接抛出结构化 `EngineError("Engine has already been closed")`。

### 4. `close()` 中断在途多 batch embed 抛底层 `RuntimeError`
- **根因**：`close()` 立即执行 `self._executor.shutdown(wait=False)`，使正在执行后续 batch 的协程在 `loop.run_in_executor` 时遭遇底层 `RuntimeError: cannot schedule new futures after shutdown`。
- **修复**：
  1. 在 `close()` 中，当非 asyncio 事件循环线程调用时，限时等待（最多 2.0s）`_active_requests` 归零，随后调用 `_executor.shutdown(cancel_futures=True)`。
  2. 在 `embed()` 的批处理循环中增加 `self._closed` 防御检查；在 `except Exception as e:` 块中捕获底层 `RuntimeError` 及关闭期间异常，统一映射为结构化 `EngineError` 抛出。

### 5. `_queue_lock` 与 `_state_lock` 非原子导致活跃请求时被降为 `IDLE`
- **根因**：`embed()` 的 `finally` 块原先先在 `_queue_lock` 中递减 `_active_requests`，释放锁后再获取 `_state_lock`。在这两个锁的间隙，若新请求介入增加活跃计数，前序请求依然依据局部的 `remaining == 0` 将状态推进为 `IDLE`。
- **修复**：
  1. 在 `finally` 块中使用联合上下文 `with self._queue_lock, self._state_lock:`，将计数递减、归零检查与向 `IDLE` 的状态流转闭合在同一原子临界区内。
  2. 在 `state` 属性获取器中维护状态机不变量：当 `_active_requests > 0` 且 `_state == IDLE` 时，自动纠正并返回 `ModelState.READY`。
  3. 全局统一锁获取顺序为 `_queue_lock` -> `_state_lock`，杜绝死锁隐患。

### 6. loader 返回部分构造（如 Tokenizer 为 None）被置 `READY`
- **根因**：`_load_worker` 仅校验 loader 执行是否发生异常（`if success:`），未防御性校验 `tokenizer is not None and model is not None`。当 loader 返回 `(None, DummyModel)` 时引擎被推进至 `READY`，造成引擎死锁在破损状态。
- **修复**：
  1. 仅当 `success and tokenizer is not None and model is not None` 时才挂载模型并推进至 `READY`。
  2. 若任一组件缺失，标记 `_set_unhealthy(True)`，进入 `ModelState.ERROR`，并累加指标 `model_load_failures_total`。
  3. 在 `load_model()` 等待时捕获部分构造失败，确保对外呈现明确的 `ERROR` 状态。

### 7. 未调用 `close()` 时 watchdog 线程导致实例与线程泄漏
- **根因**：`_watchdog_thread` 以 bound method `target=self._watchdog_loop` 启动，导致 `threading._active` -> `Thread` -> bound method -> `engine` 形成跨周期的强引用环。当外部丢失 engine 引用且未调用 `close()` 时，GC 无法回收 engine，Watchdog 线程永久以 50ms 间隔空转泄漏。
- **修复**：
  1. 将 Watchdog 线程入口改为静态方法 `@staticmethod def _watchdog_entry(engine_ref, stop_event)`，仅通过弱引用 `weakref.ref(self)` 与独立的 `_watchdog_stop_event` 驱动循环。
  2. 弱引用解析为 `None` 时守护线程自动静默退出，解开引用环。
  3. 增加 `__del__` 终结器作为安全兜底，在对象析构时幂等触发 `self.close()`。

### 8. loader 线程在 `close()` 后未被 join 造成线程泄漏
- **根因**：`_trigger_load()` 启动的 `bge-loader-{epoch}` 线程句柄未被引擎保存，`close()` 时亦未进行 join，导致耗时或阻塞的 loader 线程悬挂泄漏，持续持有 `_load_exec_lock`。
- **修复**：
  1. 引入成员属性 `self._current_loader_thread` 保存加载线程句柄。
  2. `close()` 时若 loader 线程存活且非当前线程，显式执行 `self._current_loader_thread.join(timeout=2.5)`。
  3. 退出前防御性确保 `_load_exec_lock` 被安全释放。

### 9. 恢复 `READY` 后 `last_error` 残留陈旧错误文本
- **根因**：`_set_state()` 在传入 `error_msg=None` 时跳过了对 `self._last_error` 的处理；在从 `ERROR` 成功恢复至 `READY` 状态时，旧的故障描述文本持续残留在诊断字典中。
- **修复**：在 `_set_state()` 发生向非 `ERROR` 状态（如 `READY`、`UNLOADED` 等）的迁移且未提供错误文本时，显式将 `self._last_error = None` 清空；`_load_worker` 成功装载时同样清理 `self._last_error = None`。

### 10. `__init__` 无条件清零全局 `model_unhealthy` gauge 指标
- **根因**：`BGEM3Engine.__init__` 硬编码执行 `METRICS.set_gauge("model_unhealthy", 0.0)`。若已有引擎实例正处于 `unhealthy=True`（gauge=1.0），新建第二个引擎实例会无条件将全局指标擦除为 0.0，掩盖线上报警。
- **修复**：移除 `__init__` 中的无条件清零逻辑，仅在实际状态迁移与故障判定（`_set_unhealthy`）时触发指标更新。

---

## 4. 测试验收结果

### 1. 审计测试套件运行结果
```
$ PYTHONPATH=$PWD /home/dj/WorkSpaces/ai-memory-runtime/.venv/bin/python -m pytest tests/audit_r4_*.py -q
..................................                                       [100%]
34 passed in 16.52s
```
*（34 项审计红队测试 100% 全部通过，0 failed）*

### 2. 既有核心引擎测试套件运行结果
```
$ PYTHONPATH=$PWD /home/dj/WorkSpaces/ai-memory-runtime/.venv/bin/python -m pytest tests/test_engine_*.py tests/test_step3_engine.py tests/test_idle_timeout_disabled.py -q
.........................                                                [100%]
25 passed in 8.65s
```
*（25 项既有核心引擎测试 100% 全部通过，0 failed）*

### 3. 全量测试套件运行结果
```
$ PYTHONPATH=$PWD /home/dj/WorkSpaces/ai-memory-runtime/.venv/bin/python -m pytest tests/ -q
........................................................................ [ 21%]
........................................................................ [ 42%]
........................................................................ [ 63%]
...................................................................ssss. [ 84%]
..........s...............s...........................                   [100%]
336 passed, 6 skipped, 7 warnings in 46.58s
```
*（既有 336 项全量测试 100% 全部通过，无一受损）*

### 4. 包含审计测试的全集运行结果
```
$ PYTHONPATH=$PWD /home/dj/WorkSpaces/ai-memory-runtime/.venv/bin/python -m pytest tests/ -o "python_files=test_*.py audit_*.py" -q
........................................................................ [ 19%]
........................................................................ [ 38%]
........................................................................ [ 57%]
........................................................................ [ 76%]
.............................ssss...........s...............s........... [ 95%]
................                                                         [100%]
370 passed, 6 skipped, 7 warnings in 60.21s
```
*（全量 370 个用例全部 passed，0 failed）*

---

## 5. 仍未解决项

- **无**。审计报告与任务书列出的全部 10 项缺陷已彻底解决，验收测试全绿。
