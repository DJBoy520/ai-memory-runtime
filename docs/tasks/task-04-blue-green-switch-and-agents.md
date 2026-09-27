# Task 04 任务书：Qdrant 蓝绿热切换与多 Agent 端到端集成验证

## 一、 执行者与工作区
- **执行者**：OpenCode
- **工作区**：`/home/dj/WorkSpaces/ai-memory-runtime`
- **当前 Git 分支**：`feat/amr-engine-upgrade-v2`
- **规范基准**：`docs/proposals/AMR认知记忆引擎升级与全量离线提纯实施规格_v2.2终版.md` (第四节与第五节)

---

## 二、 目标与交付清单

实现 Qdrant 蓝绿平滑别名切换脚本与全量离线提纯执行，并验证四大 Agent（Hermes、OpenClaw、OpenCode、DSH）的端到端检索：

1. **蓝绿集合与原子别名切换脚本（`scripts/blue_green_switch.py`）**：
   - 连接 Qdrant（`http://192.168.30.161:6333`）；
   - 创建新版本目标集合 `ai_memory_v2`（1024 维 Cosine，载荷索引支持 `status`, `project_id`, `type`）；
   - 从 SQLite SSOT 触发 Outbox Worker 同步全量精炼记忆到 `ai_memory_v2`；
   - 验证 `ai_memory_v2` 点数与 SQLite `active` 记忆一致性；
   - 执行原子别名切换：调用 `client.update_collection_aliases(...)`，将别名 `ai_memory` 从旧集合瞬间重定向到 `ai_memory_v2`；
   - 原旧集合重命名保留或标记为 cold backup，支持 `--rollback` 参数一键回切。

2. **多 Agent 检索兼容层验证（Adapter Verification）**：
   - 确保 UDS 响应体契约中同时输出平铺字段（`content`, `score`, `memory_id`）与结构化三元组，向后兼容现有客户端；
   - 编写 `tests/test_v2_end_to_end.py`：
     - 测试通过 UDS 查询 `Tesla P4 显卡状态`、`AEP 架构原则`、`Sub2API 与模型配置`；
     - 断言检索耗时维持在 `< 40ms` 预算内；
     - 断言返回评分字段同时包含 `final_score` 与 `vector_score`。

---

## 三、 质量与审计门禁
1. **全量测试通过**：`pytest tests/` 保持 100% 全绿；
2. **端到端断言**：`pytest tests/test_v2_end_to_end.py` 全部 PASS；
3. **完成标准**：切换过程实现无缝热切，零报错、零脏数据。
