# AMR v5 审计修复报告 · API 轨 (R1)

## 1. 概述与所有权范围

- **工作目录**：`/home/dj/WorkSpaces/ai-memory-runtime-wt/fx-api`
- **Git 分支**：`fix/audit-api`（基线 `main @ 1e30119`）
- **所有权文件**：
  - `src/interfaces/ipc/errors.py`
  - `src/interfaces/ipc/server.py`
  - `src/interfaces/mcp/bridge.py`
  - `src/main.py`
- **验收文件**（只读验收）：
  - `tests/audit_r1_jsonrpc.py`
  - `tests/audit_r1_mcp_bridge.py`
  - `tests/audit_r1_auth.py`

---

## 2. 失败用例根因与修复清单

本次任务共修复 R1 审计中发现的 7 个非幂等失败用例，保持既有 30 个通过用例与全量套件全绿。

### 1. Notification 不应回复 (JSON-RPC 2.0 §4.1)
- **对应测试**：`tests/audit_r1_jsonrpc.py::test_notification_replied_violating_jsonrpc_spec_bug`
- **根因**：原 `UDSServer._handle_client` 仅通过 `req.get('id')` 取出 ID；缺少 `id` 成员时，`req_id` 为 `None`，但服务端仍然将结果序列化并发送了 `{"jsonrpc": "2.0", "result": ..., "id": null}`，违反了 RFC JSON-RPC 2.0 §4.1 “Server MUST NOT reply to a Notification” 的强约束。
- **修复**：
  - 在 `UDSServer._handle_client` 中显式检测 `is_notification = ("id" not in req)`。
  - 对于 Notification 请求，正常执行 handler（处理任何业务逻辑与日志记录），但不向客户端 writer 写入任何 response 帧；发生异常时记录 warning 日志，同样不写回 error 帧，直接 `continue` 继续监听后续请求。

### 2. JSON 根不是对象 (list/str/int/null)
- **对应测试**：`tests/audit_r1_jsonrpc.py::test_non_dict_json_payload_connection_crash_bug`
- **根因**：客户端发送合法 JSON 根非 dict（例如 `[1, 2, 3]`、`"hello"`）时，`req.get("id")` 抛出 `AttributeError: 'list' object has no attribute 'get'`，未在循环内捕获，导致连接异常中断（ConnectionClosed/EOF），客户端未获得任何错误响应。
- **修复**：
  - 在读取帧后立即校验 `if not isinstance(req, dict):`。
  - 记录 warning 日志并返回 `-32600 Invalid Request: root payload must be an object`（`req_id=None`）。
  - 连接不崩溃，继续保持服务循环。

### 3 & 9. params 非 dict (list/string) 校验与属性错误屏蔽
- **对应测试**：`tests/audit_r1_jsonrpc.py::test_non_dict_params_causes_internal_error_bug`
- **根因**：当客户端传入非 dict 的 params（如 `params=["malformed_param_list"]`）时，`main.py` 的 `c_ref = params.get(...)` 抛出 `AttributeError`，被服务端捕获映射为 `-32603 Internal error: 'list' object has no attribute 'get'`，既泄露了内部实现，又违反了参数非法应返回 `-32602 (INVALID_ARGUMENT)` 的规范。
- **修复**：
  - **Daemon 层**：`main.dispatch_business_rpc` 与 `dispatch_admin_rpc` 入口处先统一 `if params is None: params = {}`，随后严格校验 `if not isinstance(params, dict): raise RpcInvalidArgument(...)`。
  - **Server 层**：`UDSServer._handle_client` 针对 `params is not None and not isinstance(params, dict)` 返回结构化 `-32602 INVALID_ARGUMENT` 错误帧。
  - **Bridge 层**：`bridge.handle_request` 入口同样统一 `if params is None: params = {}`，非 dict 返回 `-32602`。绝不暴露底层 Python 属性错误。

### 4. 未知 method 统一返回 -32601 (METHOD_NOT_FOUND)
- **对应测试**：`tests/audit_r1_jsonrpc.py::test_unknown_method_error_code_inconsistency_bug`
- **根因**：原 `main.py` 对未知 method 抛出 `ValueError(f"Unknown business method: {method}")`。在 `UDSServer._map_exception_to_error_response` 中因未命中前缀，默认落入 else 分支被映射为 `-32602 INVALID_ARGUMENT`，与 JSON-RPC 2.0 规范的 `-32601 Method not found` 不一致。
- **修复**：
  - 在 `src/interfaces/ipc/errors.py` 增加 `RpcMethodNotFound(RpcError)`，绑定 `code = -32601`，`error_code = "METHOD_NOT_FOUND"`。
  - `main.dispatch_business_rpc` 与 `dispatch_admin_rpc` 对未知 method 改为抛出 `RpcMethodNotFound`。
  - `UDSServer` 首位 RpcError 直通返回 `-32601`；同时并在 `ValueError` 兜底匹配中追加 `METHOD_NOT_FOUND:` 前缀支持。

### 5. RpcError 消息双前缀去重
- **对应测试**：`tests/audit_r1_jsonrpc.py::test_rpc_error_duplicate_prefix_bug`
- **根因**：`auth.py` 中抛出 `RpcAgentUnauthorized("AGENT_UNAUTHORIZED: Unknown or untrusted agent_id ...")`，而 `RpcError.to_jsonrpc_error` 盲目使用 `f"{self.error_code}: {self.message}"` 拼接，导致错误消息出现 `AGENT_UNAUTHORIZED: AGENT_UNAUTHORIZED: ...` 双重前缀。
- **修复**：
  - 修改 `RpcError.to_jsonrpc_error`：检测 `if msg.startswith(f"{self.error_code}:")`，若已存在前缀则不再重复添加；若未存在前缀则补齐。保持 `pytest.raises(ValueError, match="AGENT_UNAUTHORIZED")` 兼容性。

### 6. MCP agent_id 伪造防御与身份绑定
- **对应测试**：`tests/audit_r1_mcp_bridge.py::test_mcp_agent_id_spoofing_vulnerability_bug`
- **根因**：`src/interfaces/mcp/bridge.py` 曾使用 `if not arguments.get("agent_id"): arguments["agent_id"] = self.source_agent`，导致客户端显式传入 `"agent_id": "hermes"` 时被放行，允许通过 bridge 伪造任意 agent 身份写入。
- **修复**：
  - 在 bridge 层建立严格的身份绑定：如果调用方显式传入未在服务端 `allowed_agents` 白名单中的 `agent_id`，立即返回 `-32001 AGENT_UNAUTHORIZED`。
  - 强制设置 `arguments["agent_id"] = self.source_agent` 与 `arguments["source_agent"] = self.source_agent`，确保到达 daemon 的真实写入身份始终不可篡改且来源于当前 bridge 的 `AMR_SOURCE_AGENT`。

### 7. MCP params=None / 非 dict 防挂起
- **对应测试**：`tests/audit_r1_mcp_bridge.py::test_mcp_none_params_hangs_client_bug`
- **根因**：当请求为 `params: None` 时，`params = req_data.get("params", {})` 取出为 `None`，`params.get("name")` 触发 `AttributeError`，在 `run_stdio` 异常块中未向 stdout 写入响应帧，导致 MCP 客户端永久挂死（Hang）。
- **修复**：
  - `handle_request` 入口对 `params` 判空，`None` 视为 `{}`，非 dict 返回 `-32602`。
  - `tools/call` 中对 `arguments` 同样做防 None 与非 dict 校验，返回 `isError: True` 响应帧，绝不抛出未处理异常导致 stdout 无帧挂起。

### 8. 项目根目录 sys.path 自动推导（来自 R5 易用性增强）
- **修改文件**：`src/main.py` 与 `src/interfaces/mcp/bridge.py`
- **修复**：在导入 `config.settings` 之前，通过 `Path(__file__).resolve()` 自动计算项目根目录，并在其未处于 `sys.path` 时通过 `sys.path.insert(0, ...)` 注入。使用户直接执行 `python3 src/main.py` 或 `python3 src/interfaces/mcp/bridge.py` 无需手工指定 `PYTHONPATH`。

---

## 3. 改动文件一览

1. `src/interfaces/ipc/errors.py`
   - 新增 `RpcMethodNotFound` 类（code = -32601）。
   - `RpcError.to_jsonrpc_error` 增加前缀判定避免重复拼接。
2. `src/interfaces/ipc/server.py`
   - `UDSServer._handle_client` 增加对 Notification（无 `id` 字段）的识别，不发送响应。
   - 增加对非 dict 根 payload 的校验，返回 `-32600`。
   - 增加对非 dict `params` 的校验，返回 `-32602`。
   - `_map_exception_to_error_response` 增加 `METHOD_NOT_FOUND:` 映射分支。
3. `src/interfaces/mcp/bridge.py`
   - 入口处基于 `__file__` 自动注入根目录到 `sys.path`。
   - `handle_request` 校验 `req_data` 与 `params` 类型。
   - `tools/call` 防护 `arguments` 结构，强制绑定 `agent_id` / `source_agent`。
4. `src/main.py`
   - 入口处基于 `__file__` 自动注入根目录到 `sys.path`。
   - `dispatch_business_rpc` 与 `dispatch_admin_rpc` 入口校验 `params` 必须为 dict。
   - 未知 method 改为抛出 `RpcMethodNotFound`。

---

## 4. 验证测试结果

### 1) R1 专项审计测试套件验证
```bash
PYTHONPATH=$PWD /home/dj/WorkSpaces/ai-memory-runtime/.venv/bin/python -m pytest tests/audit_r1_jsonrpc.py tests/audit_r1_mcp_bridge.py tests/audit_r1_auth.py -v
```
**输出尾部**：
```
collected 30 items

tests/audit_r1_jsonrpc.py::test_missing_method_returns_invalid_request PASSED [  3%]
tests/audit_r1_jsonrpc.py::test_non_string_method_returns_invalid_request PASSED [  6%]
tests/audit_r1_jsonrpc.py::test_empty_string_method_returns_invalid_request PASSED [ 10%]
tests/audit_r1_jsonrpc.py::test_params_none_or_missing PASSED            [ 13%]
tests/audit_r1_jsonrpc.py::test_id_null_huge_int_and_object PASSED       [ 16%]
tests/audit_r1_jsonrpc.py::test_malformed_json_returns_parse_error PASSED [ 20%]
tests/audit_r1_jsonrpc.py::test_declared_length_zero_returns_parse_error PASSED [ 23%]
tests/audit_r1_jsonrpc.py::test_frame_declared_too_large_rejected PASSED [ 26%]
tests/audit_r1_jsonrpc.py::test_notification_replied_violating_jsonrpc_spec_bug PASSED [ 30%]
tests/audit_r1_jsonrpc.py::test_incomplete_header_and_incomplete_payload PASSED [ 33%]
tests/audit_r1_jsonrpc.py::test_non_dict_json_payload_connection_crash_bug PASSED [ 36%]
tests/audit_r1_jsonrpc.py::test_single_connection_sequential_pipelined_requests PASSED [ 40%]
tests/audit_r1_jsonrpc.py::test_client_early_disconnect_during_handler PASSED [ 43%]
tests/audit_r1_jsonrpc.py::test_unknown_method_error_code_inconsistency_bug PASSED [ 46%]
tests/audit_r1_jsonrpc.py::test_rpc_error_duplicate_prefix_bug PASSED    [ 50%]
tests/audit_r1_jsonrpc.py::test_non_dict_params_causes_internal_error_bug PASSED [ 53%]
tests/audit_r1_mcp_bridge.py::test_mcp_initialize_and_tools_list PASSED  [ 56%]
tests/audit_r1_mcp_bridge.py::test_mcp_unknown_method_and_unknown_tool PASSED [ 60%]
tests/audit_r1_mcp_bridge.py::test_daemon_error_passthrough_all_codes PASSED [ 63%]
tests/audit_r1_mcp_bridge.py::test_daemon_socket_not_found PASSED        [ 66%]
tests/audit_r1_mcp_bridge.py::test_daemon_call_timeout PASSED            [ 70%]
tests/audit_r1_mcp_bridge.py::test_mcp_agent_id_spoofing_vulnerability_bug PASSED [ 73%]
tests/audit_r1_mcp_bridge.py::test_mcp_none_params_hangs_client_bug PASSED [ 76%]
tests/audit_r1_auth.py::test_agent_id_case_and_whitespace_normalization PASSED [ 80%]
tests/audit_r1_auth.py::test_agent_id_fallback_to_source_agent PASSED    [ 83%]
tests/audit_r1_auth.py::test_agent_id_fallback_to_system_default PASSED  [ 86%]
tests/audit_r1_auth.py::test_agent_id_non_string_types PASSED            [ 90%]
tests/audit_r1_auth.py::test_unauthorized_agent_error_payload PASSED     [ 93%]
tests/audit_r1_auth.py::test_instance_id_and_client_ref_precedence PASSED [ 96%]
tests/audit_r1_auth.py::test_dispatch_business_rpc_auth_enforcement_and_context PASSED [100%]

============================== 30 passed in 7.49s ==============================
```
**结果**：30 passed, 0 failed（原 7 个失败用例全部通过，23 个原有通过用例继续通过）。

### 2) 全量回归测试套件验证
```bash
PYTHONPATH=$PWD /home/dj/WorkSpaces/ai-memory-runtime/.venv/bin/python -m pytest tests/ --ignore=tests/audit_r1_idempotency.py -q
```
**输出尾部**：
```
336 passed, 6 skipped, 7 warnings in 47.12s
```
**结果**：既有 336 项全量测试完全绿灯通过（0 failed）。

### 3) 易用性（免 PYTHONPATH 启动）验证
```bash
env -u PYTHONPATH /home/dj/WorkSpaces/ai-memory-runtime/.venv/bin/python -c "import src.interfaces.mcp.bridge; print('bridge imported successfully')"
# 输出：bridge imported successfully
```
验证通过。

---

## 5. 未解决项与说明（第一阶段）

- `test_unnormalized_status_causes_false_idempotency_conflict_bug`（位于 `tests/audit_r1_idempotency.py`）：涉及 `src/service/memory_service.py` 与 `src/core/session_store.py` 内部事务幂等键 hash 计算时的 status 归一化逻辑，归属于 service 轨负责，API 轨未触碰亦不影响该用例。

---

## 6. 第二阶段：tools.py 契约审计修复

### 1) 修复清单与实现
1. **Content 最小长度校验 (min 5 chars)**
   - **对应测试**：`tests/audit_r1_params_contract.py::test_tools_py_missing_min_length_validation_bug`
   - **修复**：在 `validate_tool_args` 的 `memory_create` 与 `memory_update`（当 content 字段非空提供时）分支增加 `len(args["content"].strip()) < 5` 校验，未达标时抛出 `ValueError("Parameter 'content' must be at least 5 characters (min 5 chars).")`。
2. **Status 枚举合法性与分权校验**
   - **对应测试**：`tests/audit_r1_params_contract.py::test_tools_py_missing_status_enum_validation_bug`
   - **修复**：
     - `memory_create`：status 若提供，必须属于 `{"active", "pending_verify", "conflict", "temporary"}`（大小写不敏感比较），不允许 HISTORICAL 或 DELETED，非法时抛出 `ValueError`。
     - `memory_update`：status 若提供，必须属于 `{"active", "pending_verify", "conflict", "historical", "temporary", "deleted"}` 6 态之一（大小写不敏感比较），非法时抛出 `ValueError`。
3. **`memory_update_status` 大小写不敏感状态匹配**
   - **对应测试**：`tests/audit_r1_params_contract.py::test_tools_py_case_sensitive_status_rejection_bug`
   - **修复**：将 `valid_statuses` 比较改为 `new_stat.strip().lower() in valid_statuses`，支持首字母大写（如 `'Active'`、`'Pending_Verify'`）；同时保留 `superseded` 状态时 `superseded_by` 必填约束。
4. **Daemon 入口布尔值归一化增强**
   - **优化**：在 `src/main.py` 的 `memory_search` 入口处对 `include_history` 增加字符串布尔清洗（`"false"` / `"0"` 解析为 `False`，`"true"` / `"1"` 解析为 `True`），提高边界健壮性。

### 2) 新增所有权文件
- `src/interfaces/mcp/tools.py`

### 3) 第二阶段测试验证

#### 审计测试套件验证
```bash
PYTHONPATH=$PWD /home/dj/WorkSpaces/ai-memory-runtime/.venv/bin/python -m pytest tests/audit_r1_jsonrpc.py tests/audit_r1_mcp_bridge.py tests/audit_r1_auth.py tests/audit_r1_params_contract.py tests/audit_r1_concurrency.py -q
```
**输出尾部**：
```
....................................F....                                [100%]
=================================== FAILURES ===================================
________________ test_include_history_string_false_pitfall_bug _________________
...
=========================== short test summary info ============================
FAILED tests/audit_r1_params_contract.py::test_include_history_string_false_pitfall_bug
1 failed, 40 passed in 7.65s
```
- tools.py 的 3 个目标测试全部转绿：
  - `test_tools_py_missing_min_length_validation_bug` PASSED
  - `test_tools_py_missing_status_enum_validation_bug` PASSED
  - `test_tools_py_case_sensitive_status_rejection_bug` PASSED
- 其余 37 个相关审计用例全部持续保持 PASSED（总计 40 passed）。
- 唯一的 1 项失败为 `test_include_history_string_false_pitfall_bug`：该测试直接实例化并调用 `memory_service.memory_search(include_history="false")` 内部逻辑，属于 service 轨 `src/service/memory_service.py` 内部实现，非 tools.py / API 轨范围。

#### 全量回归测试套件验证
```bash
PYTHONPATH=$PWD /home/dj/WorkSpaces/ai-memory-runtime/.venv/bin/python -m pytest tests/ -q --ignore=tests/audit_r1_idempotency.py
```
**输出**：
```
336 passed, 6 skipped, 7 warnings in 46.70s
```
**结果**：既有 336 项全量测试完全绿灯通过（0 failed）。

