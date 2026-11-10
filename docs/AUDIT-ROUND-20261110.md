# AMR v5 独立红队审计与加固报告（2026-11-10）

> 5 条独立 Gemini 红队轨道并行审计 v5.0.0；编排方负责监督、复现、裁决与修复验收。
> 所有测试均在隔离环境（tmp SQLite / 内存 Qdrant / fake engine / tmp UDS）执行，未触碰生产。

## 1. 审计范围与评分（修复前）

| 轨道 | 范围 | 评分 | 结论 |
| --- | --- | --- | --- |
| R1 API/MCP | JSON-RPC、MCP Bridge、鉴权、幂等、tools 校验 | **3.5 / 5** | 核心事务可靠；外围协议有 DoS/挂起/身份伪造/错误码不一致 |
| R2 存储/队列 | SQLite、Outbox、Worker、并发、崩溃恢复 | **4.2 / 5** | 单写者/CAS/崩溃自愈坚固；legacy 终态、自指、事务嵌套等有缺口 |
| R3 迁移 | 旧库变体、分类、隔离区、迁移脚本 | **4.0 / 5** | 当前生产快照可平滑升级；非标旧库/重复任务/缺列场景脆弱 |
| R4 引擎 | single-flight、watchdog、close、并发 | **2.5 / 5** | 单飞与基础 watchdog 有效；卸载竞态、close 复活、泄漏、锁序有缺陷 |
| R5 易用性 | 安装/接入/API/错误/运维/文档 | **2.9 / 5** | 底层工业级可靠，但文档与开发者门面严重脱节 |

后续修复后 R5 文档/DX 评分提升至 **4.6 / 5**（`docs/FIX-R5-USABILITY.md`）。

## 2. 发现与修复汇总

共 **40+ 项**可复现缺陷/缺口，全部修复并转为回归测试：

### API / MCP（R1）
- 非对象 JSON 根导致连接崩溃 → 返回 `-32600`；Notification 被错误回复 → 不再回复。
- 非 dict params 泄露内部错误 → `-32602`；未知 method → 统一 `-32601`。
- `RpcError` 消息双前缀 → 去重。
- MCP 客户端可伪造 `agent_id` → Bridge 强制绑定 `AMR_SOURCE_AGENT`。
- `params=None` 导致客户端永久挂起 → 结构化错误响应。
- tools 缺少 content 最小长度/status 枚举校验、`memory_update_status` 大小写敏感 → 补齐。
- `include_history="false"` 被当真值导致历史泄露 → 显式布尔归一化。
- `src/main.py` / `bridge.py` 缺少 sys.path 引导 → 无需 PYTHONPATH 可启动。

### 存储 / Worker（R2）
- legacy `DELETED` 可被复活 → 终态校验。
- `update_memory_v3` 允许 `superseded_by` 自指 → 目标校验。
- 幂等重放遇到 `None`/非 dict 响应崩溃 → 类型安全。
- `locked_write_transaction` 嵌套提前 COMMIT → SAVEPOINT/深度计数。
- `conflicts_with` 非列表 JSON 崩溃 → 容错；`checkpoint(mode)` 无白名单 → 校验。

### 迁移（R3）
- 旧 outbox 同 key 多条任务触发唯一索引崩溃 → 去重保留最新。
- 旧表缺 `id` 列时隔离报告 KeyError → 安全 `.get`，隔离先独立 COMMIT。
- 极老 memories 缺 `status`/`qdrant_point_id` → 列兜底与索引顺序修复。
- 非 pending 旧任务被错误复活 → 状态映射。
- ambiguous 统计硬编码 0 → 真实统计；rehearse 临时目录泄漏 → 清理。

### 引擎（R4）
- idle 卸载踩踏并发 LOADING → 状态复核。
- crash_count 并发写丢更新 → flock 保护。
- close 后请求复活引擎/底层 RuntimeError → `_closed` 守卫 + 优雅等待 + 异常映射。
- 活跃请求时被误降 IDLE → 锁内原子判断。
- 部分构造（tokenizer=None）进入 READY → 有效性校验。
- watchdog 强引用泄漏、loader 未 join → weakref + join。
- 恢复后 last_error 残留、多实例指标污染 → 清理/移除无条件清零。

### 易用性（R5）
- README/DOC-AMR-02/03/04/06 与 v5 行为对齐；补错误码、幂等、弱一致、工具清单、排查手册。
- `scripts/mcp-bridge.sh` 去除个人路径，可在任意 cwd 运行。
- systemd 单元、requirements、config.example 对齐。
- **新增真实 `scripts/amr_doctor.py`**（只读、`--json`、降级、脱敏），修复文档虚构命令。
- search 结果补齐 `created_at`；v3 `superseded_by` 精确 ID 重定向修复。
- reconciliation `--dry-run` 真正做到零写库/零备份。
- `storage.idempotency_retention_days` 配置生效。

## 3. 验证结果

| 验证 | 结果 |
| --- | --- |
| 审计/回归专项（`tests/audit_*.py` + fix tests） | **181 passed** |
| main 全量测试 | **352 passed, 5 skipped** |
| 隔离端到端 smoke（create/get/replay/search/update/conflict/delete/legacy/admin） | **SMOKE_OK** |
| 生产快照迁移预演（修复后） | 1173 v3 / 3 legacy，quarantine=0，源库未改 |
| 生产部署（v5.0.1） | READY 136.5s；doctor healthy；outbox backlog/lag/dead=0 |
| 生产功能冒烟 | 身份伪造被拦截（created_by=test_runner）、大小写幂等重放、created_at 非空、删除投影成功 |

## 4. 明确边界（仍不承诺）

- 单机单 Worker；多机/多实例需分布式租约 + fencing（本地 flock 不能跨机）。
- 真实 Tesla P4 / CUDA 驱动级卡死、物理断电等硬件极端场景未做破坏性验证；watchdog 只保证可见与受控重启。
- `memory_search` 弱一致（通常 ≤1s）；`memory_get` 强一致。
- legacy `memory_record` 仍为直写 Qdrant 的已知例外。
- 未进行第三方依赖漏洞扫描与真实网络分区测试（本轮聚焦应用逻辑）。

## 5. 产物

- 审计报告：`tests/audit_r{1..5}_*.py`、`docs/AUDIT-R{1..5}-*.md`、`docs/USABILITY-AUDIT-AMR-V5.md`
- 修复报告：`docs/FIX-R{1,4,5}-*.md`、`docs/FIX-R3-MIGRATION.md`
- 发布标签：`v5.0.1`；部署记录：`docs/DEPLOYMENT-RECORD-20261110.md`
