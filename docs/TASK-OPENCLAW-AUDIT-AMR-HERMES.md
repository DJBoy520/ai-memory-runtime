# OpenClaw 任务派发任务书：Hermes 统一 AMR 记忆插件方案审计

**任务标识**：`TASK-OPENCLAW-AUDIT-AMR-HERMES`  
**定案日期**：2026-09-26  
**委托方**：Hermes（方案设计）  
**审计方**：OpenClaw（架构与代码审计专家）  
**工作区**：`/home/dj/WorkSpaces/qdrant-bge-memory`  

---

## 一、审计背景与目标

老板要求实现记忆机制的**“自动感知与无感存取”**（避免纯 MCP 被动工具遗忘调用的缺陷），同时要求**“彻底摆脱旧 Mem0 库与异构端口残留，统一纳管至已开机自启的 Qdrant-BGE（AMR）服务”**。

Hermes 已完成解法 2 的需求规格说明与系统方案：
- 文档路径：`docs/DOC-AMR-07-HERMES-INTEGRATION.md`
- 参考基类：`docs/ref/memory_provider.py`（已拷贝到本工作区供安全读取）
- 核心思路：在 Hermes 内实现轻量原生 `plugins/memory/amr` 插件，在生命周期 Hook（`prefetch` / `sync_turn`）中通过 UDS 直接与 AMR 通信，实现毫秒级无感注入与后台自动归档。

---

## 二、OpenClaw 重点审计清单

请 OpenClaw 重点对以下维度进行严密审查：
1. **架构契约一致性**：
   - 方案是否严格符合 Hermes 官方 `MemoryProvider` 抽象类的生命周期语义？
   - 是否满足非阻塞要求（`sync_turn` 必须在后台线程处理，绝不卡死主会话）？
2. **通信与协议安全性**：
   - UDS 通信是否完整遵守 AMR 定义的 4 字节 Big-Endian `uint32` 长度前缀与 4MB 帧限制？
   - 是否做到 Fail-open（当 AMR 服务未就绪或临时卸载时，优雅降级，不阻断正常聊天）？
3. **资源红线审查**：
   - 插件是否保证零 PyTorch / CUDA 导入？
   - 显存回收与 3600 秒倒计时是否受到干扰？
4. **多 Agent 统一性**：
   - 提取的记忆格式是否与 OpenClaw、OpenCode 的 Qdrant Payload 规范严格兼容？

---

## 三、交付输出标准

请输出结构化的审计报告，包含：
1. **审计结论**：【通过】/【修改后通过】/【驳回】
2. **风险点与边缘特例分析**（如：并发会话隔离、超长 prompt 截断处理）
3. **给 OpenCode 编写代码时的具体实现指导与边界建议**。
