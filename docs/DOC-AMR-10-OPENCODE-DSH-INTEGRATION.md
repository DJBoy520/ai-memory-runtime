# 方案说明书：OpenCode 与 DSH 接入 AI Memory Runtime (AMR) 架构方案（无感取用与存入）

- **设计方**：Hermes Agent
- **审核方**：OpenClaw Auditor
- **执行方**：OpenCode（待审核通过后下发）
- **定案时间**：2026-09-26
- **文档编号**：DOC-AMR-10-OPENCODE-DSH-INTEGRATION
- **工程归属原则**：为 OpenCode 和 DSH 开发的所有适配代码（轻量 UDS 客户端、Hook 拦截器、配置模板与端到端测试用例），统一收拢于 `/home/dj/WorkSpaces/ai-memory-runtime` 仓库（`src/adapters/opencode/` 与 `src/adapters/dsh/`），并在该仓库内统一执行 AEP L4 全局存证与链上锚定。

---

## 一、现状与痛点排查 (Problem Statement)

系统内统一语义记忆底座 **AI Memory Runtime (AMR)** 已经稳定运行（守护进程 `amr.service`，监听 UDS `/run/user/1000/qdrant-bge.sock`，具备 BGE-M3 动态加载、3600 秒空闲自动卸载 GPU 显存，且配置了 Qdrant 强鉴权密钥）。

当前系统的 AI Fleet 中：
1. **Hermes**：已完全实现原生无感介入（`plugins/memory/amr` 原生插件 UDS 直连，0.3s 预取超时 Fail-open，每轮对话后台异步入库）。
2. **OpenCode 现状**：
   - 配置文件 `~/.config/opencode/opencode.jsonc` 中配置的是旧单体脚本 `memory_mcp_server.py`。
   - **痛点 1（显存浪费）**：旧脚本私自启动 PyTorch 加载 BGE-M3，吃掉 1.2G+ 显存。
   - **痛点 2（鉴权阻断 401）**：旧脚本直连 Qdrant 时没有配置 API Key，调用必报错。
   - **痛点 3（依赖主动工具调用）**：目前纯靠 LLM 主动调用 Tool，无法做到像 Hermes 那样在用户提问前**无感自动检索注入**、提问后**无感异步持久化流水**。
3. **DSH (DeepSeek Harness) 现状**：
   - DSH 采用 Cordis 4.0 插件微内核架构，运行在 `~/.dsh`，Web 服务端口 3080。
   - 目前处于纯无状态模型交互状态，未接入任何记忆系统。
   - 架构优势：DSH 核心内置了官方插件 `@deepseek-ai/dsh-mcp-client`（支持标准 stdio/streamable-http 协议），原生支持挂载外部 MCP。

---

## 二、需求规格说明 (Requirements)

1. **功能需求**：
   - **FR-01 记忆共享与统一**：OpenCode 与 DSH 能够读写统一 AMR 记忆库，共享系统长期事实、架构决策与跨 Agent 协作记录。
   - **FR-02 无感取用 (Implicit Recall)**：
     - 在用户每轮输入后、大模型思考前，后台自动拦截并根据当前 Prompt 在 AMR 中执行语义检索（Top-K / 阈值过滤）；
     - 将召回的记忆作为合成上下文（`<amr_recalled_context>`）隐式注入系统提示词，用户与模型均无需手动打字调用工具。
   - **FR-03 无感存入 (Implicit Ingest)**：
     - 每一轮对话结束或会话空闲时，后台异步提取当前上下文流水，自动调用 `session.ingest` 或 `memory.record` 持久化，主会话零等待、零卡顿。
2. **非功能与安全需求（铁律）**：
   - **NFR-01 Zero-VRAM 铁律**：严禁在 OpenCode 或 DSH 进程中引入 `torch` / `onnx` / `transformers`。宿主进程附加显存为 0MB，所有计算统一交由系统的 `amr.service` 守护进程承载。
   - **NFR-02 硬截止与 Fail-open (Hard Deadline & Fail-open)**：预取检索超时阈值严格限制（≤150ms），遇超时、UDS 故障或无响应时立即放行，绝不阻断大模型正常对话。
   - **NFR-03 注入防护 (Context Isolation)**：注入的上下文必须经过 XML 标签隔离并附带只读语义提示，防止 Prompt Injection。

---

## 三、实施方案与技术路线 (Implementation & Technical Roadmap)

我们采取 **“分阶演进、轻量先跑通、无感渐进式增强”** 的实施路线：

### 阶段一：MCP 基线对接（消灭显存占用与 401 阻断，打通工具级读写）

#### 1. OpenCode MCP 迁移 (`~/.config/opencode/opencode.jsonc`)
将旧脚本替换为 AMR 官方 Stdio Bridge：
```jsonc
{
  "mcpServers": {
    "amr": {
      "type": "stdio",
      "command": "/usr/bin/python3",
      "args": [
        "/home/dj/WorkSpaces/ai-memory-runtime/src/interfaces/mcp/bridge.py"
      ],
      "env": {
        "PYTHONPATH": "/home/dj/WorkSpaces/ai-memory-runtime",
        "AMR_SOURCE_AGENT": "opencode"
      },
      "enabled": true
    }
  },
  "mcp": {
    "amr": {
      "type": "local",
      "command": [
        "/usr/bin/python3",
        "/home/dj/WorkSpaces/ai-memory-runtime/src/interfaces/mcp/bridge.py"
      ],
      "environment": {
        "PYTHONPATH": "/home/dj/WorkSpaces/ai-memory-runtime",
        "AMR_SOURCE_AGENT": "opencode"
      }
    }
  }
}
```

#### 2. DSH MCP 挂载 (`~/.dsh/profiles/web/cordis.patch.yml`)
利用 DSH 内置的 `@deepseek-ai/dsh-mcp-client` 扩展点，在 Cordis 补丁层追加一个 AMR client 实例：
```yaml
- id: mcp-amr
  name: '@deepseek-ai/dsh-mcp-client'
  config:
    serverName: amr
    transport: stdio
    command: /usr/bin/python3
    args:
      - /home/dj/WorkSpaces/ai-memory-runtime/src/interfaces/mcp/bridge.py
    env:
      PYTHONPATH: /home/dj/WorkSpaces/ai-memory-runtime
      AMR_SOURCE_AGENT: dsh
    toolCallTimeoutMs: 15000
```
- **技术要点**：
  - 零编译、零新增 npm 依赖，复用 DSH 自带的 `@deepseek-ai/dsh-mcp-client`。
  - DSH 自动暴露 `mcp__amr__memory_search`、`mcp__amr__memory_record` 等工具。

---

### 阶段二：OpenCode 原生无感拦截器 (`opencode-amr` Plugin)

OpenCode 采用 `@opencode-ai/plugin` 架构（代码审查已确认其支持 `chat.message` 拦截注入，同目录现有的 `opencode-mem` 提供了完整的成熟实现范式）。

#### 技术路线：
1. **轻量级 Node.js UDS 客户端（0 额外依赖，0 显存）**：
   - 使用 Node.js 原生 `node:net` 直连 `/run/user/1000/qdrant-bge.sock`。
   - 遵循 AMR 二进制帧协议：4 字节 Big-Endian uint32 长度前缀 + JSON Payload。
2. **无感取用 (`chat.message` Hook)**：
   - 接收用户输入后，提取最新 prompt 文本；
   - **拦截排除机制**：必须主动检测并跳过系统内部任务，包括 `isStructuredSummaryPromptMessage`、compaction 任务、以及 title/summary 生成等内部请求，避免递归预取；
   - 发起 `memory.search` 请求，设置硬超时 100ms；
   - 超过阈值（如 score ≥ 0.70）的结果，在 `output.parts` 前部注入合成文本块；
   - **严格防越狱转义**：除常规 XML 转义外，必须对记忆内容中的 `</amr_recalled_context>`、`<amr_recalled_context` 等标签名做中性化处理（替换为 `&lt;/amr_recalled_context&gt;`），防止 Prompt 越狱；
   - 注入格式：
     ```xml
     <amr_recalled_context source="opencode" hint="Authoritative memory facts">
     - [Decision]: ...
     </amr_recalled_context>
     ```
   - 若超时或 Socket 报错，直接 `catch` 记录 log 并 Fail-open 放行，主线程完全无感。
3. **无感存入 (`session.idle` / 消息响应后)**：
   - 轮次结束后，异步（`setImmediate` 或 detached Promise）发送 `session.ingest`，将该轮对话存入 AMR 会话数据库。

---

### 阶段三：DSH 原生无感拦截机制探索与对接

#### 技术路线：
1. **短期（Prompt 引导无感）**：
   - 在 DSH 的系统预设（`agent-presets`）或全局指令中，固化记忆检索指令（“在分析代码或项目历史时，自动调用 amr 工具检索上下文”）。
2. **长期（Cordis Hook 拦截器）**：
   - 编写轻量 Cordis 插件（如 `cordis-plugin-dsh-amr`），监听 `before-chat` 与 `after-chat` 事件，在进入 LLM 推理管道前通过 UDS 注入 Recall 上下文。

---

## 四、安全与风险控制 (Security & Risk Controls)

1. **备份与回滚保证**：
   - 修改前对 `~/.config/opencode/opencode.jsonc` 和 `~/.dsh/profiles/web/cordis.patch.yml` 进行带时间戳的 `.bak` 备份。
   - 若出现任何配置解析错误，1 秒内还原配置并恢复原有状态。
2. **进程隔离与权限安全**：
   - UDS 严格使用 `/run/user/1000/qdrant-bge.sock`（权限 `0600`，仅限当前用户 dj 访问）。
   - 环境变量仅向子进程传递 `PYTHONPATH` 与 `AMR_SOURCE_AGENT`，无提权风险。
3. **零显存硬断言**：
   - 变更后检查 `nvidia-smi`，确认各 Agent 启动后未产生新的 Python 显存常驻，显存仅由 `amr.service` 动态托管。

---

## 五、验收标准 (Acceptance Criteria)

1. **配置有效性**：
   - OpenCode 与 DSH 成功启动，无 JSONC/YAML 语法解析错误。
2. **工具可用性**：
   - OpenCode MCP 列表中正确展示 AMR 5 个工具，且调用 `memory_search` 成功返回 JSON（无 401 报错）。
   - DSH 启动成功且 Web 界面/headless 能正常识别并使用 `mcp__amr__*` 工具。
3. **零显存验证**：
   - 执行 `nvidia-smi` 验证无多余进程常驻。
4. **无感流转验证（阶段二）**：
   - 在 OpenCode 中输入涉及先前记忆的问题，无需显式调用 Tool，上下文能够自动带出并命中记忆。
