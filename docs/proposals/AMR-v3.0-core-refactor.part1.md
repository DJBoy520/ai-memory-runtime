# AMR v3.0 核心重构落地方案（四个关键工程实现细节）

**文档编号**：`PROP-AMR-v3.0-CORE-REFACTOR`
**版本**：v1.0（待 OpenClaw 独立审核）
**编制**：Hermes（方案与需求分析）
**审核**：OpenClaw（架构审计）
**执行**：OpenCode（编码交付）
**验收**：Hermes（深度断言）
**编制日期**：2026-10-01

---

## 0. 执行摘要

老板已裁定 AMR 记忆模型的三条根本性变更：**废除 SPO 三元组与强关系图、状态收敛为 6 态、Qdrant Payload 收敛为 9 字段一跳直出**。本方案只回答四个工程落地问题（MCP 工具定义 / UDS RPC 协议 / Outbox 同步 / 日间 AI 整理），并给出可直接编码的契约。

**结论先行**：

| 问题 | 核心裁决 |
|---|---|
| Q1 MCP 工具 | 收敛为 **10 个工具**，写路径按「内容变更 / 元数据变更 / 冲突打标 / 软删除」四权分离，废止 `memory_record` / `memory_update_status` 的模糊语义 |
| Q2 RPC 协议 | **方法名与工具名一一同名映射**（`memory.search` ↔ `memory_search`），保留 1 个 release 的旧名别名；错误码锁定 `-32001 ~ -32010` 段，附 `data.retryable` 让 Agent 可自愈 |
| Q3 Outbox | **SSOT 单写者 + 同事务写发件箱 + 幂等投影 + 租约消费 + 退避重试 + 死信** 六件套；关键补强是「payload 快照在提交时冻结」，杜绝 Worker 回读可变状态导致的丢更新 |
| Q4 AI 整理 | 扫 **TEMPORARY 且（会话已关闭 或 龄期 > 24h）**；模型**只有提案权、零写权**，schema 强制 `evidence_source_ids + verbatim_quotes`，服务端 verbatim 复核不过即整条丢弃；自动流转上限止于 `TEMPORARY → PENDING_VERIFY` |

**动笔前必须先拍板的 6 个分歧点见 §1.3**——其中 D1（字段数 10 还是 11）与 D4（存量集合 `status` 大小写）若不决策，编码阶段必然返工。

---

## 1. 契约基线（重构目标态）

### 1.1 记忆实体核心字段

```
memory_id          TEXT  PK        全局唯一，形如 mem_YYYYMMDD_xxxxxx
project_id         TEXT  默认 global
type               TEXT  默认 general，命名空间格式 namespace/name
status             TEXT  6 态枚举（见 1.2）
content            TEXT  NOT NULL  唯一语义载体
version            INTEGER 默认 1   内容版本，内容变更才自增
created_by_agent   TEXT  NOT NULL
updated_by_agent   TEXT  NOT NULL
created_at         INTEGER NOT NULL  Unix 秒
updated_at         INTEGER NOT NULL  Unix 秒
source_refs        TEXT  弱引用 JSON 数组，默认 '[]'
```

### 1.2 状态 6 态与允许迁移

| 状态 | 语义 | 可迁移至 | 触发方 |
|---|---|---|---|
| `ACTIVE` | 当前生效事实 | `CONFLICT`, `HISTORICAL`, `DELETED` | Agent / 管理员 |
| `PENDING_VERIFY` | 候选事实，未采信 | `ACTIVE`, `CONFLICT`, `DELETED`, `HISTORICAL` | 整理器提案 + 闸门放行 |
| `CONFLICT` | 争议态，双向打标 | `ACTIVE`, `HISTORICAL`, `DELETED` | 仅 `memory_conflict` 与仲裁 |
| `HISTORICAL` | 曾经有效、已被取代 | `ACTIVE`（回滚）, `DELETED` | 管理员 / 整理器提案 |
| `TEMPORARY` | 上下文切断暂存 | `PENDING_VERIFY`, `HISTORICAL`, `DELETED` | 客户端 park / 自动 park |
| `DELETED` | 软删除（终态） | *（终态，仅管理员可 `ACTIVE` 复归）* | Agent / 管理员 |

**不变式 I-1**：`DELETED` 为终态，任何 Agent 侧工具不得将其迁出；仅 Admin UDS 的方法可复归。
**不变式 I-2**：`ACTIVE ↔ HISTORICAL` 的批量互换必须走 Admin 通道，业务通道单条变更上限见 D5。
**不变式 I-3**：`CONFLICT` 必须成对出现——进入 `CONFLICT` 时双方记忆同时被置位，不存在单边 `CONFLICT`。

### 1.3 必须先拍板的 6 个分歧点（阻塞编码）

**D1｜「核心 10 个字段」实际枚举了 11 个。**
任务书正文写「保留核心 10 个字段」，随后列出 `memory_id, project_id, type, status, content, version, created_by_agent, updated_by_agent, created_at, updated_at, source_refs` —— 实为 **11 个**。
→ **建议**：以 **11 个**为准（`source_refs` 保留），文档与 DDL 统一按 11 字段表述。
→ **附加**：冲突双向打标所需的 `conflicts_with` 不在 11 字段内，但它必须落库（否则 `CONFLICT` 态无法表达争议对象）。建议作为**第 12 个「内部运维列」**存在，不进入面向 Agent 的 9 字段投影，仅在 `memory_get` / `memory_conflict` 返回中暴露。

**D2｜`source_refs` 的载荷格式与 `scope` 的归宿。**
现模型有 `scope`（global/project/agent/session）、`session_id`、`source_message_ids`、`tags`、`confidence`、`importance`、`protection_level`。新 11 字段只留下 `source_refs`。
→ **建议**：`source_refs` 统一承载**弱引用对象数组**，不承载置信度与保护级别：
```json
[{"kind":"message","id":"msg_102","session_id":"sess_x"},
 {"kind":"memory","id":"mem_20261001_a1b2c3"},
 {"kind":"file","id":"/path/x.py","hash":"sha256:..."},
 {"kind":"url","id":"https://..."}]
```
→ `scope` 概念**取消**：`scope=project` 直接编码为 `project_id`；`scope=agent/session` 由客户端在检索侧用 `created_by_agent` / `source_refs` 过滤实现。
→ `confidence` / `importance` / `protection_level` **降级为内部运维列**（PolicyGate 熔断与白名单仍需），不进入 Agent 契约。

**D3｜旧 5 态到新 6 态的映射与存量迁移。**
代码中旧状态字面量共 **139 处**，散布 11 个文件（含 `src/service/cognitive_engine.py`、`src/interfaces/mcp/tools.py`、`scripts/blue_green_switch.py`、`tests/test_reconciliation.py` 等）。
→ **建议映射**：
| 旧 | 新 |
|---|---|
| `candidate` | `PENDING_VERIFY` |
| `active` | `ACTIVE` |
| `superseded` | `HISTORICAL`（`superseded_by` 转入 `source_refs{kind:memory}`） |
| `archived` | `HISTORICAL`（`deleted_at` 为空者） / `DELETED`（已有 `deleted_at` 者） |
| `stale` | `HISTORICAL` |
| `deleted` | `DELETED` |
→ 历史 `superseded` 链条不再有强边，改为**弱引用单向可追溯**（HISTORICAL 记录的 `source_refs` 指向取代它的 memory_id）。

**D4｜存量知识集合的 `status` 大小写断层（高危，必炸）。**
AMR 核心层对向量检索强制注入 `status == "active"` 过滤；而 `crypto_standards`、`project_docs` 等**知识集合的 payload 没有 `status` 键**，靠 `adopt_orphan_points.py` 补写为小写 `active`。新契约改为大写 `ACTIVE` 后，**所有未迁移集合将整片查空（total: 0）且不报错**。
→ **建议**：迁移期检索侧采用**兼容过滤** `status IN ["ACTIVE","active"]`，同时提供一次性回填脚本把知识集合 payload 统一改写为 `ACTIVE`；回填完成并核对计数一致后，再收紧为严格 `ACTIVE`。**此项必须在验收用例中以「知识集合召回非空」硬断言**。

**D5｜整理器的自动权限上限。**
→ **建议**：整理器（AI）**最大自动权限止于 `TEMPORARY → PENDING_VERIFY` 与 `TEMPORARY → DELETED`（过期，且需满足龄期与证据双条件）**；`PENDING_VERIFY → ACTIVE` 必须由 Agent 显式调用或管理员批准，AI 不得自动升格为事实。

**D6｜`memory_get` 的读取源。**
现状 `memory_get` 读 Qdrant（`src/service/memory_service.py:333`），在 Outbox 异步下存在读写延迟窗口。
→ **建议**：`memory_get` / `memory_history` 改读 **SQLite SSOT**（强一致、强 RYOW），`memory_search` 继续读 Qdrant（最终一致、允许 ≤ 秒级延迟）。同一毫秒写入的记忆，`create` 后立刻 `get` 必须命中，此条作为验收硬断言。

---
