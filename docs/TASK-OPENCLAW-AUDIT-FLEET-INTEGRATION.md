# OpenClaw 独立架构与安全审计任务书

- **任务编号**：AUDIT-AMR-FLEET-001
- **审计目标**：审查 Hermes 提交的《DOC-AMR-08-FLEET-INTEGRATION: OpenClaw 与 OpenCode 统一接入 AI Memory Runtime (AMR) 架构方案》
- **方案文档路径**：`docs/DOC-AMR-08-FLEET-INTEGRATION.md`
- **参考实现代码**：
  - AMR Stdio MCP Bridge: `src/interfaces/mcp/bridge.py`
  - AMR Tools 定义: `src/interfaces/mcp/tools.py`
  - 旧版参考实现 (已镜像入库): `docs/ref/memory_mcp_server.py`

---

## 审计要点与安全关注项 (Checklist)

1. **MCP 协议契约一致性**：
   - 验证 `src/interfaces/mcp/bridge.py` 暴露的 5 个核心工具（`memory_search`, `memory_record`, `memory_get`, `memory_update_status`, `memory_ingest_session`）是否能平滑覆盖原有 `memory_mcp_server.py` 的功能；
   - 检查 stdio 通信模式下，OpenClaw 和 OpenCode 解析 JSON-RPC 的兼容性。

2. **环境变量注入与进程沙箱安全性**：
   - 检查在 `openclaw.json` 的 `mcp.ai-memory.env` 中注入 `PYTHONPATH` 和 `AMR_SOURCE_AGENT` 的安全性，是否会引起子进程污染；
   - 检查 `opencode.jsonc` 的 `mcp` 与 `mcpServers` 双块配置规范是否准确。

3. **资源治理与鉴权隔离**：
   - 评估弃用旧脚本 `memory_mcp_server.py` 后，释放 1.26GB GPU 显存的有效性；
   - 评估接入 AMR 后对 Qdrant 401 强鉴权的透明适配能力。

4. **异常降级与故障隔离 (Fault Tolerance)**：
   - 当 AMR 守护进程异常或被关闭时，OpenClaw / OpenCode 是否会出现进程卡死或级联崩溃？stdio bridge 是否具备 Fail-open 降级返回？

5. **回滚机制可行性**：
   - 评估备份预案与恢复步骤的可靠性。

---

## 交付要求

请仔细阅读 `docs/DOC-AMR-08-FLEET-INTEGRATION.md`，基于代码和配置进行严谨审查，并出具正式的审计结论：
- **【通过】(Approved)**
- **【修改后通过】(Approved with Modifications)**：列出具体的修订条目
- **【否决】(Rejected)**：给出明确的否决原因与安全隐患
