# AI Memory Runtime (AMR) 架构良好性与软件工程质量深度审计报告

**报告编号**：`AMR-ARCH-AUDIT-20261110-V5`  
**审计委员会**：独立软件工程与架构质量评审委员会（Independent Software Engineering Board）  
**基线版本**：`v5.0.1`（Commit: `0105978`，Worktree 分支: `review/arch`）  
**审计准则依据**：
- Clean Architecture (Robert C. Martin) / Hexagonal Architecture (Alistair Cockburn)
- Domain-Driven Design: Tackling Complexity in the Heart of Software (Eric Evans)
- Enterprise Integration Patterns / CQRS & Transactional Outbox (Martin Fowler)
- ISO/IEC 25010 软件产品质量模型（可靠性、可维护性、模块化、兼容性）
- The Twelve-Factor App 契约规范

---

## 目录
1. [执行摘要与总体评分](#1-执行摘要与总体评分)
2. [架构成熟度雷达分析](#2-架构成熟度雷达分析)
3. [核心分层与依赖拓扑分析 (Clean Architecture / DIP)](#3-核心分层与依赖拓扑分析-clean-architecture--dip)
4. [领域驱动设计 (DDD) 建模纯度审查](#4-领域驱动设计-ddd-建模纯度审查)
5. [CQRS 读写分离与 Transactional Outbox 模式深度剖析](#5-cqrs-读写分离与-transactional-outbox-模式深度剖析)
6. [跨语言多 Agent 接入与协议安全解耦](#6-跨语言多-agent-接入与协议安全解耦)
7. [软件度量与代码质量 (Coupling, Cohesion & Complexity)](#7-软件度量与代码质量-coupling-cohesion--complexity)
8. [架构坏味道与技术债务清单 (严重程度排序)](#8-架构坏味道与技术债务清单-严重程度排序)
9. [中长期架构重构与演进规划路线图](#9-中长期架构重构与演进规划路线图)

---

## 1. 执行摘要与总体评分

AI Memory Runtime（以下简称 AMR）作为面向多 Agent 协同体系（Hermes, OpenClaw, OpenCode, DSH 等）的中心化本地记忆基础设施，在经历了 v1~v3 的探索期与 v5 的可靠性加固后，已经建立起一套**极具生产韧性、面向边缘/单机工作站高度优化的运行时体系**。

### 1.1 总体架构评分：**3.85 / 5.00**（等级：**B+ / 工业生产良好，重构债务中等**）

| 评审维度 | 权重 | 得分 (1-5) | 核心评价依据 |
| :--- | :---: | :---: | :--- |
| **Clean Architecture 分层解耦度** | 20% | **3.5** | 分层大体清晰，但存在明显的层间倒置（Core 逆向依赖 Service）及入口层泄露 SQL。 |
| **Domain-Driven Design (DDD) 纯度** | 20% | **3.2** | 缺乏充血领域实体（严重贫血模型，字典导向编程），但状态流转与修订历史机制完备。 |
| **CQRS 与 Transactional Outbox 质量** | 25% | **4.6** | **架构最大亮点**：Commit-then-Project 零 I/O 阻塞、状态折叠 Outbox 与 CAS 乐观锁极其卓越。 |
| **代码内聚度与复杂度控制 (Cohesion/SRP)** | 15% | **2.8** | 存在显著的上帝类反模式（`SessionStore` 达 2816 行，方法复杂度高达 51），职责严重聚合。 |
| **跨语言多 Agent 协议与隔离** | 10% | **4.3** | UDS 二进制长度前缀帧 + Stdio MCP 桥接实现进程级解耦，显存隔离彻底；但租户鉴权依赖自声明。 |
| **12-Factor App 契约与生产工程度** | 10% | **4.0** | 优雅停机、单执行者 flock、背压队列与单飞机制极为扎实；缺少精细环境变量映射与导入期副作用隔离。 |

**加权综合得分**：$3.5 \times 0.20 + 3.2 \times 0.20 + 4.6 \times 0.25 + 2.8 \times 0.15 + 4.3 \times 0.10 + 4.0 \times 0.10 = \mathbf{3.845} \approx \mathbf{3.85}$。

---

## 2. 架构成熟度雷达分析

```text
                  CQRS / Outbox 模式 (4.6)
                        5.0 ┌─────▲─────┐
                            │    / \    │
                            │   /   \   │
    多 Agent 协议解耦 (4.3) ├──/─────\──┤ 12-Factor 生产就绪 (4.0)
                       \    │ /       \ │    /
                        \   │/         \│   /
                         \  ●           ●  /
                          \ │           │ /
                           \│     ●     │/
  Clean Architecture (3.5) ─●───────────●─ DDD 建模纯度 (3.2)
                            │\         /│
                            │ \       / │
                            │  \     /  │
                            │   \   /   │
                        1.0 └─── \-●-───┘
                           高内聚低耦合 / SRP (2.8)
```

### 成熟度综合诊断
- **超强项（领先工业标准）**：异步投影发件箱（Transactional Outbox）与读写双重一致性契约设计极其严谨，零外部 I/O 提交极大释放了写链路并发能力；单写者 `fcntl.flock` 配合 CAS 变更仲裁，从物理层杜绝了模型计算乱序与并发竞争。
- **薄弱项（主要技术债务）**：数据持久化层严重缺乏仓储（Repository）切分，`SessionStore` 膨胀为超级上帝类；领域模型全面缺位，业务逻辑充斥着无类型强度的 `dict` 传递与松散 SQL 查询；内层 Core 出现了向外层 Service 的逆向依赖。

---

## 3. 核心分层与依赖拓扑分析 (Clean Architecture / DIP)

### 3.1 理想整洁架构分层模型 vs 现状映射

依据 Clean Architecture 原则，系统的依赖流向必须严格由**外层（基础设施/接口）**指向**内层（领域模型/用例服务）**。

```text
【理想 Clean Architecture 流向】
┌────────────────────────────────────────────────────────┐
│ 接口层 (Interfaces): IPC Server, MCP Bridge, Admin CLI  │
└──────────────────────────┬─────────────────────────────┘
                           │ 依赖调用
┌──────────────────────────▼─────────────────────────────┐
│ 应用服务层 (Application/Service): MemoryService, Outbox │
└──────────────────────────┬─────────────────────────────┘
                           │ 依赖驱动
┌──────────────────────────▼─────────────────────────────┐
│ 核心领域层 (Domain/Core): Entities, Value Objects, Ports │
└──────────────────────────┬─────────────────────────────┘
                           │ 依赖倒置 (DIP) 实现
┌──────────────────────────▼─────────────────────────────┐
│ 基础设施层 (Infrastructure): SQLite Store, Qdrant, BGE │
└────────────────────────────────────────────────────────┘
```

### 3.2 现实依赖拓扑图（含架构违规违约点）

通过 AST 语法树对 `src/` 各子系统导入关系的自动化拓扑分析：

```text
                     ┌──────────────────┐
                     │   src/main.py    │
                     └──┬──────┬──────┬─┘
           ┌────────────┘      │      └────────────┐
           ▼                   ▼                   ▼
┌────────────────────┐ ┌───────────────┐ ┌────────────────────┐
│ src/interfaces/    │ │ src/service/  │ │ src/reconciliation/│
│  - ipc/server.py   │ │  - memory_svc │ │  - engine.py       │
│  - mcp/bridge.py   │ │  - outbox_wkr │ │  - policy_gate.py  │
└─────────┬──────────┘ └───┬───────▲───┘ └─────────┬──────────┘
          │                │       │ ⚠ 违规逆向导入 │
          │                │       │ (metrics.py,  │
          │                │       │  status.py)   │
          ▼                ▼       │               ▼
┌──────────────────────────────────┴──────────────────────────┐
│ src/core/                                                   │
│  - session_store.py  ──[import status]──► (src/service) ⚠   │
│  - engine.py         ──[import metrics]─► (src/service) ⚠   │
│  - qdrant.py                                                │
└─────────────────────────────────────────────────────────────┘
```

### 3.3 依赖拓扑审计发现

#### 1. 核心层对服务层的逆向跨层依赖（Dependency Inversion Violation）
- **代码位置**：
  - `src/core/engine.py:26`: `from src.service.metrics import METRICS`
  - `src/core/session_store.py:21`: `from src.service.memory_status import normalize_status`
- **架构缺陷分析**：
  `src/core/` 在架构定义中是底层基础设施与核心持久化引擎，而 `src/service/` 是上层业务编排服务。`engine.py` 与 `session_store.py` 反向导入 `service` 中的模块，构成了典型的**子系统间循环依赖（Subsystem Architectural Cycle）**：
  $$\text{service} \xrightarrow{\text{imports}} \text{core} \quad \text{且} \quad \text{core} \xrightarrow{\text{imports}} \text{service}$$
  这直接破坏了分层系统的单向无环图（DAG）属性，导致若要独立打包、测试或抽取 `src/core/` 作为独立库使用时，将不可避免地被拖入上层服务的业务代码。

#### 2. 入口装配层侵入底层存储并直接编写 SQL（Layer Leaking）
- **代码位置**：`src/main.py:288-316` 与 `src/main.py:335-375`
- **架构缺陷分析**：
  在 `dispatch_admin_rpc` 中，处理 `outbox.list_dead_letters` 与 `outbox.replay` 时，应用宿主进程直接调用 `self.session_store.locked_write_transaction()` 获取连接游标，并在 `main.py` 内硬编码了长达 50 行的 SQL 查询与表结构字段映射：
  ```python
  # src/main.py:288-298 架构泄露示例
  with self.session_store.locked_write_transaction() as conn:
      cur = conn.cursor()
      cur.execute("""
          SELECT id, target_collection, memory_id, projection_revision, ...
          FROM memory_projection_outbox WHERE status = 'dead_letter' ...
      """, (limit,))
  ```
  控制器（Entrypoint / Dispatcher）直接绕过了存储仓储抽象，穿透至物理 SQL 游标，严重违反了关注点分离原则（Separation of Concerns）。

#### 3. 认知子系统与对账子系统的存储直连穿透
- **代码位置**：
  - `src/cognitive/context_builder.py:51-53, 81-88, 100-107`
  - `src/cognitive/scheduler.py:57-60`
  - `src/reconciliation/engine.py:60-67`
- **架构缺陷分析**：
  `ContextBuilder` 和 `CognitiveScheduler` 接收 `SessionStore` 实例后，直接调用 `store.get_connection().cursor()` 执行多条自由 SQL，导致对 `memories` 表内部字段结构的耦合四处蔓延。此外，`ReconciliationEngine` 绕过了 `src/core/qdrant.py` 封装的 `QdrantManager`，自建了一套独立的 `QdrantClient` 连接，破坏了外部连接治理的单一来源。

---

## 4. 领域驱动设计 (DDD) 建模纯度审查

### 4.1 核心领域建模：贫血模型与字典导向编程 (Anemic Domain Model)

在 DDD 规范中，领域层应由充血的**聚合根（Aggregate Root）**、**实体（Entity）**、**值对象（Value Object）**以及**领域服务（Domain Service）**构成，业务不变量必须封装在实体内部。

**审计实况**：
审查整个 `src/` 模块，**未定义任何代表业务领域核心概念的类**（如 `Memory`、`Revision`、`Evidence`、`AuditRecord`、`Session`、`Message`）。
- 全系统的领域数据以原始 Python 字典 `Dict[str, Any]` 的形式在网络层、服务层、存储层与向量库之间流转。
- 所有数据字段校验（如 `status` 大小写规范化、版本自增校验、冲突判定、时间戳补充）均以松散的过程式代码（Procedural Scripting）零散分布于 `MemoryService` 和 `SessionStore` 的各处局部逻辑中。

```text
【当前实现的贫血字典模型】
RPC Payload (dict) ──► MemoryService (dict) ──► SessionStore (dict + SQL) ──► Qdrant Payload (dict)
        ▲                      ▲                       ▲
        └────── 没有任何类型安全实体与领域不变量校验约束 ───────┘
```

**工程负面后果**：
1. **类型退化与键名歧义**：例如记忆类型字段在代码中同时存在 `type`、`memory_type` 两种键名；作用域字段在各层传递中存在 `scope` 与 `project_id` 的边界重叠；调用方极易因为拼写笔误（如 `source_agent` vs `created_by_agent`）引发隐蔽的数据污染。
2. **状态不变量缺乏实体内聚保护**：状态流转规则（如从 `PENDING_VERIFY` 到 `ACTIVE`）没有内聚在实体状态机对象中，而是依赖 SQL `UPDATE memories SET status = ?` 驱动，外部甚至可以直接调用底层方法修改状态。

### 4.2 记忆生命周期建模（6 态 DAG 状态机）

AMR 规范定义了 6 种标准记忆生命周期状态：
$$\mathcal{S} = \{\text{ACTIVE}, \text{PENDING\_VERIFY}, \text{CONFLICT}, \text{HISTORICAL}, \text{TEMPORARY}, \text{DELETED}\}$$

#### 状态机实现现状分析
- **优点**：
  1. `src/service/memory_status.py` 提供了统一的 `normalize_status()`，具备对历史 legacy 小写状态（`active`, `superseded`, `archived`, `deleted`）的健壮收敛能力。
  2. `SessionStore.change_memory_status()` 严格锁死了 `DELETED` 终态不可逆规则（已删除记忆不可复活为其它状态）。
  3. `_validate_superseded_target()` 有效防御了 `HISTORICAL` 状态下 `superseded_by` 的自指环路（Self-referencing loop）与同族循环指向。
- **不足与坏味道**：
  1. **状态流转矩阵（Transition Matrix）不闭合**：除了 `DELETED` 终态限制之外，系统并未定义或校验完整的合法迁移路径矩阵。例如系统允许记忆直接从 `TEMPORARY` 变更为 `CONFLICT`，或从 `HISTORICAL` 逆向变更为 `PENDING_VERIFY`，缺乏领域状态转移矩阵的严密白名单拦截。
  2. **脏状态混杂历史残留**：在 `src/cognitive/context_builder.py:72, 84, 103` 中，依然遗留了历史原型的 `candidate` 伪状态判断（`LOWER(status) IN ('active', 'candidate')`），暴露出状态模型未在全局完成原子收敛。

### 4.3 不可变修订版本链与溯源证据链 (Immutability & Provenance)

AMR 在持久化层的数据建模上具备高度的工业级前瞻性：
1. **修订历史表 (`memory_revisions`)**：每次正文或核心元数据变更，均同事务追加版本记录（`version`, `content`, `change_reason`, `created_by_agent`），保证了知识事实的不可篡改历史快照。
2. **证据追溯表 (`memory_evidence`)**：记录记忆抽取所锚定的底层原始会话与消息 ID（`session_id`, `message_id`, `quote`），配合 `EvidenceValidator` 实现了防幻觉与防篡改溯源。
3. **审计日志表 (`memory_audit_log`)**：记录所有治理动作（`promote`, `supersede`, `delete`, `update`）及操作者，提供了完整的合规审查轨迹。

---

## 5. CQRS 读写分离与 Transactional Outbox 模式深度剖析

AMR 架构设计的最大亮点在于其**纯粹的命令查询职责分离（CQRS）与事务发件箱（Transactional Outbox）实现**。

### 5.1 架构实现对比分析矩阵

将 AMR 的发件箱设计与经典分布式模式进行系统性对照：

| 对比维度 | 经典 Martin Fowler 事务发件箱 | Debezium / WAL 日志 CDC 模式 | AMR 本地单写者发件箱 (v5) |
| :--- | :--- | :--- | :--- |
| **事件投递载荷** | 完整事件载荷（Event Payload）入表 | 直接解析 DB 物理变更日志（Binlog/WAL） | **状态快照引用（Revision Reference）入表** |
| **写队列增长** | 随更新操作线性递增（Append-Only） | 随事务大小产生连续流 | **UPSERT 覆盖合并（Coalescing Outbox）** |
| **GPU / 计算开销** | 每次中间态均投递并重复触发下游计算 | 必须捕获全量中间变更流 | **极优：折叠中间态，仅计算最终版本向量** |
| **基础设施依赖** | 需要消息中间件（Kafka/RabbitMQ） | 依赖 JVM、Debezium、Kafka 集群 | **零外部依赖：纯本地 SQLite WAL + POSIX 锁** |
| **并发乱序防护** | 依赖分区键有序消费与分布式锁 | 依赖 Kafka Partition 分区全局单调 | **单写者 `fcntl.flock` + Revision CAS** |
| **适用场景** | 大规模分布式微服务网络 | 异构数据库企业级数据同步 | **边缘智能体、本地多 Agent 核心运行时** |

### 5.2 AMR Commit-then-Project 流程拓扑

```text
[写操作: memory_create / memory_update]
   │
   ▼
┌────────────────────────────────────────────────────────┐
│ 1. 开启 SQLite 强一致独占事务 (locked_write_transaction)│
│    • 写入/更新 memories (SSOT) 事实表                   │
│    • 递增 projection_revision                          │
│    • 记录 memory_revisions 快照                         │
│    • 幂等写入 idempotency_keys                         │
│    • UPSERT 入队 memory_projection_outbox              │
└──────────────────────────┬─────────────────────────────┘
                           │ 事务提交（耗时 < 2ms）
                           ▼
               立即返回客户端: projection_status="pending"
               【写链路完全阻断外部网络/GPU I/O】
                           │
                           ▼ (异步解耦)
┌────────────────────────────────────────────────────────┐
│ 2. OutboxWorker (持有 amr-outbox.lock 独占执行)        │
│    • 拉取 pending 任务                                 │
│    • 读取 memories 当前最新快照 (claim_revision)       │
│    • 驱动 BGE-M3 生成向量 (GPU 推理 20~50ms)           │
│    • Upsert / Delete 写入 Qdrant 衍生投影              │
│    • CAS 更新 Outbox 状态:                             │
│      UPDATE memory_projection_outbox                   │
│      SET status='done'                                 │
│      WHERE id=? AND projection_revision=claim_revision │
│      (若处理期间有新写入导致 rev 变更，更新 0 行并重试)│
└────────────────────────────────────────────────────────┘
```

### 5.3 读一致性双重契约分析

1. **强一致点查 (`memory_get`)**：直接面向 SQLite 数据库主键与索引检索。只要本地事务提交，后续点查立即可见最新文本、版本与状态，满足强一致性（Strong Consistency）。
2. **弱一致检索 (`memory_search`)**：面向 Qdrant 向量数据库执行近邻搜索。依赖 OutboxWorker 异步投影上屏，通常在数十毫秒至数百毫秒内就绪，满足最终一致性（Eventual Consistency）。

### 5.4 关键架构优势
- **GPU 资源免于颠簸**：采用 `ON CONFLICT(target_collection, memory_id) WHERE status='pending' DO UPDATE`，若在 Worker 推理期间同一个记忆被快速更新了 5 次，Outbox 自动合并为最新 `projection_revision`，避免了为 5 个中间态重复做 5 次 1024 维 BGE-M3 昂贵计算。
- **CAS 乐观并发安全**：即使极端情况下 Worker 取出任务后由于 GC 或线程调度发生长延时，写回 Qdrant 后的 CAS `UPDATE ... WHERE projection_revision = claim_revision` 会敏锐感知到数据已被修改（`rowcount == 0`），并安全地将旧任务废弃或重新入队，杜绝了向量库“以旧盖新”的历史版本覆盖缺陷。

---

## 6. 跨语言多 Agent 接入与协议安全解耦

### 6.1 协议层物理隔离与双 UDS 架构

AMR 在通信层彻底废弃了高开销、易受局域网攻击的 HTTP/TCP 协议，全面采用原生 Unix Domain Socket（UDS）。

```text
┌────────────────────────────────────────────────────────┐
│ 业务 Socket: $XDG_RUNTIME_DIR/qdrant-bge.sock (0600)   │
│  - 客户端: Hermes, OpenClaw, OpenCode, DSH             │
│  - 承载方法: memory.*, session.ingest, embedding.*     │
└────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────┐
│ 管理 Socket: $XDG_RUNTIME_DIR/qdrant-bge-admin.sock(0600)│
│  - 客户端: Admin CLI, amr_doctor                       │
│  - 承载方法: admin.*, status.metrics, outbox.replay     │
└────────────────────────────────────────────────────────┘
```

- **物理通道隔离**：管理方法与业务方法监听在独立的物理 Socket 文件上，普通 Agent 即使发起业务请求也无法调用模型的强制卸载、死信重放或物理快照接口。
- **二进制安全帧**：采用 `[uint32_be 长度前缀 (4 Bytes)] + [JSON-RPC 2.0 Payload]` 格式，单包上限硬性限制为 4MB，有效防御内存缓冲区溢出攻击（DoS）。

### 6.2 MCP Bridge 进程隔离与硬件零依赖

AMR 提供了 `src/interfaces/mcp/bridge.py`，将标准 MCP（Model Context Protocol）协议转化为内部 UDS 调用。
- **卓越实践**：Bridge 进程**完全不导入 PyTorch、CUDA 或 Transformers**（纯 Python 标准库 + 协议轻量解析），内存开销仅数十 MB，极度轻量。
- **Agent 宿主保护**：Claude Code 或外部 Agent 通过 Stdio 启动 Bridge 时，不会因宿主缺少 GPU 驱动或 CUDA 库冲突而崩溃。

### 6.3 租户与鉴权模型的架构隐患

审查 `src/interfaces/ipc/auth.py`：
- **缺陷**：鉴权函数 `authorize_agent()` 仅仅从 RPC 参数字典中提取 `params.get("agent_id")`，并比对是否在 `config.server.allowed_agents` 白名单中。
- **安全边界模糊**：在同一 Linux 用户（UID）下，任何连入 Socket 的 Agent 都可以任意声称自己是 `"agent_id": "hermes"` 或 `"agent_id": "system"`。
- **租户/项目无访问控制列表（ACL）**：记忆的 `project_id` 和 `scope` 完全由调用方自愿声明。Agent A 可以肆意传参 `project_id="secret-project-b"` 读取或写入其他项目的记忆，系统缺乏 `(agent_id, project_id, action)` 维度的 RBAC / ACL 授权网关。

---

## 7. 软件度量与代码质量 (Coupling, Cohesion & Complexity)

### 7.1 不稳定性度量 ($I = Ce / (Ca + Ce)$)

在面向对象包度量体系中：
- $Ca$（Afferent Coupling，入度）：外部依赖本模块的数量（表征职责重要性与稳定性需求）。
- $Ce$（Efferent Coupling，出度）：本模块依赖外部的数量（表征受外界变化的影响程度）。
- $I = Ce / (Ca + Ce)$：不稳定性指标。$I \to 0$ 极为稳定（内层核心），$I \to 1$ 极为不稳定（外层边界）。

#### 子系统级别度量分析

| 子系统 (Subsystem) | 入度 ($Ca$) | 出度 ($Ce$) | 不稳定性 ($I$) | 架构期望 | 偏离度评价 |
| :--- | :---: | :---: | :---: | :---: | :--- |
| `src/core/` | 5 | 1 | **0.17** | $0.00$ | ⚠ 轻度失范：作为底层核心本应 $Ce=0$，但依赖了 `src/service/`。 |
| `src/service/` | 3 | 1 | **0.25** | $0.20 \sim 0.40$ | 符合预期，但被 core 逆向导入。 |
| `src/interfaces/` | 2 | 1 | **0.33** | $0.50 \sim 0.80$ | 表现优异，仅依赖 core 错误与协议类型。 |
| `src/reconciliation/`| 0 | 2 | **1.00** | $0.80 \sim 1.00$ | 完全符合预期（高层治理用例）。 |
| `src/cognitive/` | 0 | 1 | **1.00** | $0.80 \sim 1.00$ | 完全符合预期（高层认知流水线）。 |

### 7.2 圈复杂度与代码体量热点统计

对 codebase 圈复杂度（Cyclomatic Complexity）进行静态分析，发现高风险复杂度集中在少数关键方法：

```text
圈复杂度分布梯队:
  50+ [极高风险/严重异味]: SessionStore._migrate_schema_v5 (51)
                         MemoryService.memory_search (68)
  30+ [高风险/急需拆分]  : SessionStore.update_memory_v3 (33)
                         main.dispatch_admin_rpc (38)
                         MemoryService.memory_get (47)
  20+ [中风险/职责偏重]  : SessionStore.create_memory_v3 (27)
                         SessionStore.apply_status_change (20)
                         SessionStore.change_memory_status (20)
                         MemoryService.memory_update (20)
                         main.dispatch_business_rpc (21)
```

---

## 8. 架构坏味道与技术债务清单 (严重程度排序)

根据国际架构审计标准，将识别出的技术债务按严重等级划分为 **P0 (阻塞级/极端坏味道)**、**P1 (高风险/架构失范)**、**P2 (中度风险/设计瑕疵)**。

```text
┌────────────────────────────────────────────────────────────────────────┐
│ P0-1: SessionStore 上帝类反模式 (God Object Anti-Pattern)               │
├────────────────────────────────────────────────────────────────────────┤
│ P1-1: 核心与服务层逆向循环导入 (Architectural Cycle: Core ◄► Service)  │
│ P1-2: 领域模型严重贫血与字典导向编程 (Anemic Domain Model)             │
│ P1-3: CQRS 读路径 SSOT 契约被污染 (Polluted Read Fallback in Get)      │
│ P1-4: Entrypoint 控制器层严重泄露底层 SQL (Main Layer Leaking)        │
├────────────────────────────────────────────────────────────────────────┤
│ P2-1: 抽象检索接口悬空废弃 (Abandoned Interface: RetrievalCore)        │
│ P2-2: 文档与实现严重偏离 (Doc-Code Divergence on Outbox Schema)       │
│ P2-3: 缺乏基于端口与适配器的依赖倒置 (Missing Ports for Unit Testing)   │
│ P2-4: 多 Agent 身份自声明与租户越权隐患 (Implicit Multi-Agent Auth)    │
│ P2-5: 12-Factor 配置静态绑定与导入期副作用 (Import-Time Side Effects)  │
└────────────────────────────────────────────────────────────────────────┘
```

---

### [P0-1] `SessionStore` 超级上帝类反模式 (God Object Anti-Pattern)

- **精确位置**：`src/core/session_store.py`（单文件 2816 行，包含 45 个方法，覆盖 12 张数据表）
- **职责聚合清单**：
  1. 数据库物理 Schema 声明与历史 3 代数据迁移引擎（`_migrate_schema_v2`, `_migrate_schema_v5`，累计 ~550 行）；
  2. 原始会话流水摄取与消息明细审计（`ingest_messages`, `record_raw_session`，~310 行）；
  3. Legacy 历史记忆与冻结旧同步队列处理（`create_memory`, `fetch_pending_sync_tasks`，~340 行）；
  4. 幂等控制与锁事务上下文仲裁（`execute_idempotent`, `locked_write_transaction`，~150 行）；
  5. 统一投影发件箱队列管理与 CAS 状态更新（`enqueue_projection_task`, `mark_projection_done`，~530 行）；
  6. 修订版本不可变快照读写（`record_memory_revision`, `get_memory_revisions`，~50 行）；
  7. v3 标准记忆事实主表 CRUD 与族冲突分析（`create_memory_v3`, `update_memory_v3`，~460 行）。
- **失范分析**：
  严重违背单一职责原则（SRP）。`SessionStore` 成为了整个系统的“全能上帝”，任何功能的改动（无论是修改队列重试策略、调整会话日志字段、增减索引、修改幂等过期时间，还是调整记忆生命周期校验）都必须编辑同一个巨型文件。
- **重构方案蓝图**：
  拆分为 **仓储模式（Repository Pattern）** 体系：
  ```text
  src/storage/ (或 src/infrastructure/persistence/)
  ├── migrator/
  │   ├── runner.py                 # 迁移驱动引擎
  │   └── versions/                 # 独立版本迁移脚本 (v2_migration, v5_migration)
  ├── connection.py                 # SQLite 连接池、WAL 模式与事务管理器 (UnitOfWork)
  ├── repositories/
  │   ├── session_repository.py     # 专管 sessions, messages, raw_* 表
  │   ├── memory_repository.py      # 专管 memories SSOT 事实表
  │   ├── revision_repository.py    # 专管 memory_revisions 快照表
  │   ├── outbox_repository.py      # 专管 memory_projection_outbox 表与 CAS 状态
  │   └── idempotency_repository.py # 专管 idempotency_keys
  └── facade.py                     # 向后兼容的 SessionStore 薄包装外观（Facade）
  ```

---

### [P1-1] 核心与服务层逆向循环导入 (Architectural Cycle: Core ◄► Service)

- **精确位置**：
  - `src/core/engine.py:26` -> 导入 `src.service.metrics`
  - `src/core/session_store.py:21` -> 导入 `src.service.memory_status`
- **失范分析**：
  Clean Architecture 核心原则禁止内层代码感知外层存在。
  - `memory_status.py` 定义的是记忆实体的标准状态枚举（`CANONICAL_STATUSES`），这是最纯粹的**领域模型值对象（Domain Value Object）**，被错误地存放在了应用服务层（`src/service/`），迫使持久化层 `SessionStore` 向上依赖。
  - `metrics.py` 是进程内指标观测器，属于**跨领域通用基础设施（Cross-cutting Infrastructure）**，被错误放置在应用服务层，迫使计算引擎 `BGEM3Engine` 向上依赖。
- **重构方案蓝图**：
  1. 建立 `src/domain/` 目录，将 `src/service/memory_status.py` 下沉为 `src/domain/lifecycle.py`。
  2. 建立 `src/common/` 或移入 `src/core/metrics.py`，将 `metrics.py` 下沉为通用基础设施，彻底消除 `core -> service` 的逆向导入。

---

### [P1-2] 领域模型严重贫血与字典导向编程 (Anemic Domain Model)

- **精确位置**：`src/service/memory_service.py`、`src/core/session_store.py` 全链路
- **失范分析**：
  业务入参出参全盘使用 Python `dict`。字段的缺失、类型的错误只能在运行时暴露；复杂的业务不变量（例如 `version` 必须连续且单调递增、`HISTORICAL` 必须伴随 `superseded_by`、`DELETED` 不得再演进）被稀释在 SQL 拼接逻辑中。
- **重构方案蓝图**：
  引入 Python `dataclass(slots=True)` 或 `Pydantic` 定义富领域模型：
  ```python
  @dataclass
  class MemoryEntity:
      memory_id: str
      content: str
      status: MemoryStatus
      version: int
      project_id: str = "global"
      type: str = "general"
      superseded_by: Optional[str] = None
      
      def mark_superseded(self, target_id: str) -> None:
          if self.status == MemoryStatus.DELETED:
              raise IllegalTransitionError("DELETED memory cannot be superseded.")
          if target_id == self.memory_id:
              raise SelfReferenceError("Cannot supersede self.")
          self.status = MemoryStatus.HISTORICAL
          self.superseded_by = target_id
  ```

---

### [P1-3] CQRS 读路径 SSOT 契约被污染 (Polluted Read Fallback in Get)

- **精确位置**：`src/service/memory_service.py:537-587` 与 `src/service/memory_service.py:616-656`
- **失范分析**：
  在点查方法 `memory_get` 中：
  1. **幽灵回退破坏 SSOT**：当 SQLite 未检索到记录时，代码退回到 Qdrant 进行二次查询；若在 Qdrant 命中则包装为合法记忆返回。这意味着 Qdrant 中残留的孤儿数据（Orphan Points）会被伪装成真实事实，打破了“SQLite 为唯一权威真实源”的铁律！
  2. **读操作强行绑定网络 I/O**：即使 SQLite 命中，方法仍启动线程池和信号量并发访问 Qdrant 提取 `parent_memory_id`, `chunk_index`, `tags` 等字段（所谓的 best-effort 富化），将一个本应微秒级的本地 SQLite 点查变成了可能超时（0.2s 降级）的复合操作。
- **重构方案蓝图**：
  - 彻底切断 `memory_get` 对 Qdrant 的数据回退与字段富化，令 `memory_get` 成为 100% 纯粹的 SQLite 查询；
  - 存量数据库中若缺失 `parent_memory_id` 或 `tags`，由启动期的对账脚本或迁移脚本一次性回填至 SQLite，不得在实时读路径中跨库拉取。

---

### [P1-4] Entrypoint 控制器层严重泄露底层 SQL (Main Layer Leaking)

- **精确位置**：`src/main.py:288-375`
- **失范分析**：
  作为守护进程装配入口与 RPC 路由分发器，`main.py` 在 `dispatch_admin_rpc` 中直接书写 SQL 游标操作死信表与重放更新，越过了所有服务与仓储层抽象。
- **重构方案蓝图**：
  在 `SessionStore`（或重构后的 `OutboxRepository`）中正式暴露领域管理方法：
  `outbox_repo.list_dead_letters(limit)` 与 `outbox_repo.replay_dead_letters(task_ids)`，`main.py` 仅保留参数校验与方法委托。

---

### [P2-1] 抽象检索接口悬空废弃 (Abandoned Interface: RetrievalCore)

- **精确位置**：`src/service/retrieval_core.py`（定义了抽象基类 `RetrievalCore` 与候选体契约 `Candidate`）
- **失范分析**：
  `RetrievalCore` 自 2026 年 10 月冻结后，在整个代码库中**没有任何类实现它**，也没有任何服务调用它。而 `MemoryService.memory_search` 却冗长地手写了 268 行代码，圈复杂度高达 68。抽象与实现严重脱节。
- **重构方案蓝图**：
  以 `RetrievalCore` 为契约，将 `memory_search` 重构为模块化检索策略流水线：
  - `ExactIdRetriever`（精确 ID 重定向召回）；
  - `DenseVectorRetriever`（Qdrant 近邻向量召回）；
  - `CandidateRe-ranker`（基于状态可见性与作用域过滤候选集）。

---

### [P2-2] 文档与实现严重偏离 (Doc-Code Divergence on Outbox Schema)

- **精确位置**：`docs/DOC-AMR-02-ADD.md`、`docs/DOC-AMR-03-DDD.md` vs `src/core/session_store.py:791-806`
- **失范分析**：
  架构设计文档（DDD 与 ADD）明确宣称发件箱表字段包含 `claimed_by`, `claimed_at`，状态包含 `processing`，支持超时租约回收。然而真实代码实现全面采用了更轻量的 **单进程 `flock` + Revision CAS 机制**，表结构中根本不存在 `claimed_by`/`claimed_at` 字段，CHECK 约束仅包含 `('pending', 'done', 'dead_letter')`。
  真实代码的设计更优且更适配单机环境，但文档未能同步修订，造成维护者的认知鸿沟。
- **重构方案蓝图**：
  全面同步更新架构文档，抹除已废弃的分布式租约声明，确立基于 `flock` + CAS 乐观锁的架构正式地位。

---

### [P2-3] 缺乏基于端口与适配器的依赖倒置 (Missing Ports for Unit Testing)

- **精确位置**：`src/service/memory_service.py:43-56`
- **失范分析**：
  `MemoryService` 强类型绑定具体类：`engine: BGEM3Engine`, `qdrant_manager: QdrantManager`, `session_store: SessionStore`。
  由于没有定义抽象接口（Port），单元测试无法轻易传入内存版内存存储实现，必须依赖真实的 SQLite 文件或打猴子补丁（`unittest.mock.MagicMock`）。
- **重构方案蓝图**：
  在领域层声明 `MemoryStorePort`、`VectorStorePort`、`EmbeddingEnginePort`，服务层仅面向抽象 Port 编程。

---

### [P2-4] 多 Agent 身份自声明与租户越权隐患 (Implicit Multi-Agent Auth)

- **精确位置**：`src/interfaces/ipc/auth.py:6-48`
- **失范分析**：
  仅依赖 RPC 报文内的字符串参数 `agent_id` 进行白名单过滤，缺少基于 Linux 操作系统对等凭证（`SO_PEERCRED`，检查连入进程的有效 UID/GID/PID）的校验，且缺乏多 Agent 访问不同 `project_id` 的细粒度权限校验。
- **重构方案蓝图**：
  1. 在 UDS 服务端通过 `socket.getsockopt(SOL_SOCKET, SO_PEERCRED)` 获取对端真实 PID/UID；
  2. 建立 `agent_permissions` 规则集，限制特定 Agent 仅能读写授权的 `project_id`。

---

### [P2-5] 12-Factor 配置静态绑定与导入期副作用 (Import-Time Side Effects)

- **精确位置**：`config/settings.py:157` (`settings = load_config()`)
- **失范分析**：
  `config/settings.py` 在被任何模块 `import` 时即刻执行文件 I/O，检查并修改本地文件权限（`0600`）。同时，配置项无法通过细粒度环境变量（如 `AMR_STORAGE__SQLITE_PATH`）直接覆写，对容器化部署友好度不足。
- **重构方案蓝图**：
  采用惰性单例模式（Lazy Initialization），移除顶层自动执行逻辑，改用 `pydantic-settings` 支持环境变量自动映射。

---

## 9. 中长期架构重构与演进规划路线图

为了在保持现有 351 项回归测试 100% 通过的前提下完成架构升级，建议独立工程委员会指导团队分三阶段平滑推进：

```text
2026 Q4 (Phase 1)             2027 Q1 (Phase 2)             2027 Q2 (Phase 3)
┌──────────────────────┐      ┌──────────────────────┐      ┌──────────────────────┐
│  消除架构环路与下沉  │ ───► │  上帝类仓储群拆分    │ ───► │  检索流水线与领域充血│
│  - 下沉 Domain 层    │      │  - 拆分 5 大仓储     │      │  - 实体模型充血      │
│  - 消除 core->service│      │  - 独立 Schema 迁移器│      │  - 策略模式重构检索  │
│  - 收敛 main.py SQL  │      │  - 引入 UnitOfWork   │      │  - UDS PeerCred 强化 │
└──────────────────────┘      └──────────────────────┘      └──────────────────────┘
```

### 阶段一：消灭循环依赖与边界纠偏（立即实施 / 1-2 周）
1. **领域与工具下沉**：
   - 创建 `src/domain/lifecycle.py`，迁入 `CANONICAL_STATUSES` 与 `normalize_status`；
   - 创建 `src/core/metrics.py`，将指标器移入通用核心基础设施；
   - 彻底切断 `src/core/` 对 `src/service/` 的一切反向依赖。
2. **收敛 Main 入口 SQL**：
   - 在 `SessionStore` 增加 `list_dead_letters()` 与 `replay_dead_letters()`，移除 `main.py` 内的原生 SQL。
3. **修正架构文档偏离**：
   - 更新 `DOC-AMR-02-ADD.md` 与 `DOC-AMR-03-DDD.md`，与当前生产级的 `flock` + CAS Revision 发件箱代码对齐。

### 阶段二：`SessionStore` 上帝类渐进式拆解（中期规划 / 1 个月）
1. **抽离数据库迁移器（Migration Subsystem）**：
   - 将 `_migrate_schema_v2` 与 `_migrate_schema_v5`（~550 行）剥离至独立的 `src/storage/migrator/`；
2. **构建专门仓储（Repositories）**：
   - 提取 `OutboxRepository` 处理统一投影与死信队列；
   - 提取 `SessionMessageRepository` 处理原始会话日志；
   - 提取 `MemoryRepository` 处理记忆 CRUD；
3. **保留 Facade 兼容层**：
   - 原 `SessionStore` 改写为委托外观类（Facade），维持既有全部 public API 不变，底层逐项委托给子仓储，保障 351 项既有单元测试与集成测试无感过渡。

### 阶段三：领域充血、检索策略化与安全强化（长期演化 / 2-3 个月）
1. **领域充血实体化**：
   - 建立强类型的 `Memory` 聚合根与完整状态流转矩阵验证；
2. **检索模块策略化**：
   - 激活 `RetrievalCore` 抽象契约，将 `memory_search` 拆解为可插拔的召回与重排过滤器链（Chain of Responsibility）；
3. **纯粹 SSOT 读路径**：
   - 彻底废除 `memory_get` 向 Qdrant 的降级回退，保证本地点查 100% 强一致微秒级返回；
4. **多 Agent 零信任鉴权**：
   - 接入 Linux `SO_PEERCRED` 套接字身份审查，引入项目级权限映射矩阵。

---

## 结论

AI Memory Runtime（AMR）在**高可用本地单机运行时**、**读写双重一致性模型**、**Commit-then-Project 事务发件箱**以及**显存防爆与单写并发控制**等关键领域展现了卓越的工业级架构水准，成功解决了大型向量模型与实时多 Agent 系统交互时的 I/O 阻塞与竞争难题。

本审计报告所指出的代码结构性债务（上帝类膨胀、领域模型贫血、层间依赖倒置）属于项目高速演进期典型的工程技术债务。通过执行本报告提出的三阶段重构路线图，AMR 将从**“工程健壮的高可用运行时”**全面升维为**“具备纯粹 Clean Architecture 与充血 DDD 典范的下一代智能体记忆操作系统”**。
