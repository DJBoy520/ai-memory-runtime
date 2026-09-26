# OpenClaw 独立架构与安全审计任务书：OpenCode 与 DSH 接入 AMR

- **任务编号**：AUDIT-AMR-OPENCODE-DSH-001
- **审计目标**：审查 Hermes 提交的《DOC-AMR-10-OPENCODE-DSH-INTEGRATION: OpenCode 与 DSH 接入 AI Memory Runtime (AMR) 架构方案（无感取用与存入）》
- **方案文档路径**：`/home/dj/WorkSpaces/ai-memory-runtime/docs/DOC-AMR-10-OPENCODE-DSH-INTEGRATION.md`
- **参考代码与配置文件**：
  - AMR Stdio MCP Bridge: `/home/dj/WorkSpaces/ai-memory-runtime/src/interfaces/mcp/bridge.py`
  - OpenCode 配置文件: `/home/dj/.config/opencode/opencode.jsonc`
  - DSH Cordis 配置: `/home/dj/.dsh/profiles/web/cordis.patch.yml` 与 `settings.yaml`
  - DSH MCP Client: `/home/dj/.npm-global/lib/node_modules/@deepseek-ai/dsh/node_modules/@deepseek-ai/dsh-mcp-client/lib/index.js`
  - OpenCode 插件参考: `/home/dj/.config/opencode/node_modules/opencode-mem/dist/index.js`

---

## 审计要点与安全关注项 (Checklist)

1. **协议一致性与兼容性**：
   - OpenCode 与 DSH 采用 AMR 的 Stdio MCP Bridge 是否与各宿主的协议规范（JSON-RPC 2.0 / MCP 规范）完全一致？
   - DSH 挂载 `@deepseek-ai/dsh-mcp-client` 的 YAML 配置格式是否精确匹配 Cordis 运行时？
2. **Zero-VRAM 铁律与资源消耗**：
   - 该方案是否杜绝了在客户端加载任何 PyTorch/Transformers 模型？
   - 是否能解决现有 OpenCode 独立加载模型吃 1.2G+ 显存与 401 报错的痼疾？
3. **无感拦截与上下文注入安全性**：
   - OpenCode 在 `chat.message` 中基于 UDS 直连做语义预取的设计，其超时控制（100ms）与 Fail-open 兜底机制是否足以保证主对话完全不被阻塞？
   - 注入上下文所采用的 XML 标签隔离防护，是否能有效防范 Prompt 注入？
4. **渐进式实施路线合理性**：
   - “阶段一：工具层 MCP 对接（修复显存与 401）” + “阶段二：OpenCode 原生无感插件对接” + “阶段三：DSH 深度集成” 的拆解是否风险可控？
5. **故障隔离与回滚机制**：
   - 当 AMR 守护进程离线或异常时，是否会导致 OpenCode 或 DSH 崩溃或挂死？备份与回滚方案是否完备？

---

## 交付要求
请仔细审查方案文档，出具正式的审计意见与结论：
- **【通过】(Approved)**
- **【修改后通过】(Approved with Modifications)**（指出具体修改项）
- **【否决】(Rejected)**（列出阻断项与风险）
