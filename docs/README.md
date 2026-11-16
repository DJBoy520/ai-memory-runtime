# AMR 文档中心

AI Memory Runtime (AMR) 设计说明与参考文档。

---

## 核心设计与技术规范

| 文档编号 | 文档名称 | 内容概述 |
|:---|:---|:---|
| [DOC-AMR-01](DOC-AMR-01-PRD.md) | **需求说明书 (PRD)** | 背景、主要功能与基础指标 |
| [DOC-AMR-02](DOC-AMR-02-ADD.md) | **系统架构设计 (ADD)** | 系统架构图、核心设计与工程目录结构 |
| [DOC-AMR-03](DOC-AMR-03-DDD.md) | **详细设计规范 (DDD)** | SQLite 表结构、Payload Schema 与帧协议 |
| [DOC-AMR-04](DOC-AMR-04-API.md) | **接口与协议契约 (API)** | UDS / MCP 接口清单与请求示例 |
| [DOC-AMR-05](DOC-AMR-05-TST.md) | **测试验证基线 (TST)** | 测试策略、核心用例与运行方法 |
| [DOC-AMR-06](DOC-AMR-06-MULTI-AGENT-INTEGRATION.md) | **客户端接入指南** | Hermes、OpenClaw 等客户端配置方法 |

---

## 2.x 升级与交接

| 文档 | 内容 |
|:---|:---|
| [HANDOFF-20261116-AMR2-PROD](HANDOFF-20261116-AMR2-PROD.md) | **2.x 生产切换交接（最新，先读）**：线上状态、证书、鉴权、客户端接入待办、回滚 |
| [DEPLOYMENT-RECORD-20261116-AMR2](DEPLOYMENT-RECORD-20261116-AMR2.md) | 2.x 生产切换记录：拓扑、证书指纹、验证矩阵、备份与回滚 |
| [HANDOFF-20261116-AMR2](HANDOFF-20261116-AMR2.md) | **2.x 会话交接**：分支/提交、已完成模块、测试命令、未完成项、风险与后续顺序 |
| [DOC-AMR-08](DOC-AMR-08-VERSIONING.md) | 1.x / 2.x 版本线与 v5 历史标签的关系 |
| [DOC-AMR-09](DOC-AMR-09-CLIENT-2X.md) | 2.x 客户端接入契约、错误码、spool 语义 |
| [DOC-AMR-10](DOC-AMR-10-DEPLOY-2X.md) | 2.x 部署、systemd、迁移与回滚草案 |

---

## RFC 提案

- **正式提案**：[rfcs/](rfcs/)（RFC-003 至 RFC-007，设计决策记录）
