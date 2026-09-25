# AI Memory Runtime - 全量测试与质量验收文档 (TST)

**文档标识**：`DOC-AMR-05-TST`  
**版本号**：`v1.0.0`  
**定案日期**：2026-09-26  
**编写方**：OpenClaw（架构团队）  
**审计方**：Hermes（架构专家）  
**执行方**：opencode（代码交付）

---

## 1. 测试策略与质量目标

本方案测试聚焦于三大核心红线：
1. **显存安全红线**：验证 Tesla P4 在 5 分钟闲置后的显存自动释放能力，以及 MinerU 并发时显存不突破 4.5GB；
2. **状态机与流控防爆**：验证 6 态流转（特别是 LOADING 期间的请求自旋排队）与五层流控（4MB、超长切分、Batch 16、Queue 64）；
3. **数据一致性与幂等**：验证 `(session_id, message_id, content_hash)` 消息修订识别，以及检索底层强制注入 `status == active`。

---

## 2. 测试用例矩阵 (Test Cases Matrix)

| 用例 ID | 测试模块 | 测试场景与操作步骤 | 预期结果与断言标准 |
| :--- | :--- | :--- | :--- |
| **TC-MOD-01** | 模型引擎 | 首次调用 `memory_search` 触发模型冷启动载入 | 状态从 `UNLOADED` -> `LOADING` -> `READY`，响应耗时 ≤ 5s，Tesla P4 allocated_mb 升至 ~1180MB。 |
| **TC-MOD-02** | 模型引擎 | 模型就绪后连续发起 10 次推理 | 状态稳定在 `READY` / `IDLE`，单次推理耗时在 25~35ms 之间，无显存泄漏。 |
| **TC-MOD-03** | 模型引擎 | 闲置倒计时测试：停止所有请求 300 秒 | 触发卸载逻辑，状态变为 `UNLOADED`，显存释放，allocated_mb 归零。 |
| **TC-MOD-04** | 模型引擎 | 在 `LOADING` 状态期间突发 5 个并发检索请求 | 所有请求正常排队等待，在模型进入 `READY` 后全部成功返回结果，无请求丢失或报错 500。 |
| **TC-MEM-01** | 记忆检索 | 调用 `memory_search("SM4 GCM")` 检索 | 仅返回 `status == "active"` 的条目，按 score 降序排列。 |
| **TC-MEM-02** | 记忆状态 | 调用 `memory_update_status` 将某记忆置为 `superseded` | 再次调用 `memory_search` 无法搜出该条目；调用 `memory_get` 能查出且标明 `superseded_by`。 |
| **TC-MEM-03** | 超长切分 | 调用 `memory_record` 传入长达 15,000 字的技术文档 | 内部自动切分出 2~3 个 Chunks，均绑定同一 `parent_memory_id`，切片带 128 Token 重叠。 |
| **TC-SES-01** | 会话幂等 | 连续两次摄取完全相同的会话消息 | 第二次摄取返回 `ignored: N`，SQLite 记录总数不变。 |
| **TC-SES-02** | 会话更新 | 摄取相同 message_id 但 content 发生变动的消息 | 识别到 `content_hash` 变动，返回 `revision_updated: 1`，数据库内容被更新。 |
| **TC-NET-01** | 流控防御 | 发送单包大于 4MB 的非法数据包 | UDS 服务端直接切断连接，不消耗 GPU 与内存，系统正常运行。 |
| **TC-NET-02** | 流控防御 | 模拟极端突发：瞬间涌入 100 个推理请求 | 前 64 个进入队列排队，后续请求触发背压，返回明确的 503 错误。 |
| **TC-ADM-01** | 管理面 | 通过 `admin-cli` 连 `qdrant-bge-admin.sock` 执行 status | 正确输出 JSON 格式的显存、队列深度、P50 指标。普通 Agent 目录无此 socket。 |

---

## 3. 验收准则 (Acceptance Criteria)

1. **自动化测试通过率**：`pytest tests/` 全部用例（单元、集成与并发）100% 通过（PASS）。
2. **物理显存压测验证**：
   - 运行压测脚本，同时触发 MinerU 图像解析与 32 批次并发向量推理；
   - 执行 `nvidia-smi` 实时采样，显存峰值必须稳定在 **4.5GB 以下**，杜绝任何 CUDA OOM。
3. **安全隔离合规**：
   - 执行 `ss -tlnp` 确认宿主机**无任何 8100/8101 等 TCP 监听端口**。
   - 确认 Socket 文件权限为 `0600`。
