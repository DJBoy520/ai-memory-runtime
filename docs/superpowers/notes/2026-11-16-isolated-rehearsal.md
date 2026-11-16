# 2.x 隔离全栈演练记录（2026-11-16）

## 目的与边界

在不触碰 1.x 生产服务的前提下，验证 `DSH 插件 → UDS adapter → HTTPS MCP 服务
→ 2.x daemon` 的完整链路，以及 `source_revision` 水位、spool 确认删除、召回注入。

隔离方式：

- 生产 SQLite 用 `sqlite3.Connection.backup()`（只读源）复制到 `/tmp`，主库未被打开写入。
- 临时 CA + 服务端证书（RSA，SAN `localhost`/`127.0.0.1`），服务只监听 `127.0.0.1:18443`。
- daemon 用 `AMR_CONFIG_PATH` 指向临时配置，`XDG_RUNTIME_DIR` 指向临时目录
  （否则会与生产的 `amr-outbox.lock` 冲突而拒绝启动）。
- Qdrant 指向同一台服务器，但集合名改为 `*_rehearsal`；模型用 CPU、关闭预热。
- DSH 使用 `DSH_HOME=/tmp/...` 的临时 profile，插件配置 `spoolBackend: sqlite`。

## 结果

| 检查 | 结果 |
| --- | --- |
| `remote_main --check` | ok |
| `/healthz` | 200 |
| `/mcp` 无 token | 401 |
| token 工具列表 | 3 个（memory_create / memory_ingest_session / memory_search） |
| `memory_ingest_session` rev=17 新消息 | `inserted=1` |
| 同消息 rev=10 旧快照 | `ignored=1`，内容保持不变 |
| 同消息 rev=17 不同内容 | `-32602`，整批回滚 |
| 同消息 rev=18 新内容 | `revision_updated`，水位 18 |
| 直连 MCP 结果复核 | messages=“newer”，watermark=18，ingest_log=ingested/ignored/revision_updated |
| adapter `amr.ping` | `{"status":"ok"}` |
| adapter `session.ingest` | 成功；客户端伪造的 `agent_id` 被剥离，落库为 `dsh` |
| DSH 真实会话（无工具） | 模型回答 `REHEARSAL_OK`；归档 2 条消息（user+assistant），水位 19 |
| DSH 归档 spool | 投递确认后 SQLite spool 0 行 |
| DSH 召回注入 | 临时 profile 阈值 0.5 时，模型仅凭上下文答出生产记忆里的 `signalkeepers`；归档仍只有 user+assistant 2 条 |

## 发现

1. **Qdrant 投影不随 `qdrant.collections` 隔离。** `memory_create` 的
   `collection_name` 默认为 `ai_memory`，投影写入了**生产集合**，不是配置里的
   `*_rehearsal`。演练后已删除该 point，并删除三个临时集合。若要做真正隔离的
   演练/蓝绿切换，必须使用独立 Qdrant 实例或改投影目标，不能只改集合列表。
2. **默认召回阈值会漏掉 0.60 的相关项。** 插件默认 `scoreThreshold: 0.65`；
   中文问句对英文记忆的相似度实测 0.600，因此未注入。调低到 0.5 后注入成功。
   这不是缺陷，但切换前应按真实语料校准阈值。
3. **工具型会话会把中间步骤/推理写入记忆。** 使用文件工具的 DSH 会话归档了
   16 条 assistant 消息（含推理与检索过程），不是“只归档最终回答”。需要确认这是
   否符合产品预期，否则应在 `session_mapper` 过滤 reasoning/tool 步骤。
4. **第二个 daemon 必须换 `XDG_RUNTIME_DIR`。** 否则 outbox 单例锁
   `$XDG_RUNTIME_DIR/amr-outbox.lock` 会让新实例直接退出（退出码 3）。
5. **生产库已存在 `message_source_revisions` 表（0 行）。** 说明此前已有 2.x 代码
   路径打开过生产库；这是增量表，1.x 忽略它，但“生产数据未改”的表述需要修正。
6. 演练中 `llm-pi-ai` 因临时 profile 被 YAML 往返改写（`off:` 变成 `false:`）而
   校验失败，DSH 回退到内置 DeepSeek provider。模型是真实的，但正式验收应使用
   目标 provider 并保留配置校验失败即告警的行为。

## 清理

演练进程全部停止，`18443` 已关闭；生产 `amr.service` 仍为 active；`/tmp` 下的
副本、证书、spool、临时 profile 已删除；生产 `ai_memory` 中演练写入的 point 已删。

## 结论

2.x 的服务、适配器、插件链路在真实 DSH 会话中可用，水位与 spool 语义符合
DOC-AMR-09。尚不能据此宣称生产可切换：Qdrant 投影隔离、召回阈值、归档内容
范围三项需要在正式部署方案中明确。
