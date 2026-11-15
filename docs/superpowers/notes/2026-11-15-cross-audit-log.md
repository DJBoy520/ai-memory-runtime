# 2.x 交叉审计记录

日期：2026-11-15
参与方：DeepSeek（实现/主控审计）、Gemini（分块代码与设计审计）、DeepSeek 子 agent（实现）。
规则：Gemini 的每条发现必须对应具体位置与触发条件；主控逐条判定为
`accepted` / `rejected` / `false-positive`，并记录理由。只有 `accepted` 才改代码，
改完补测试并复审。

## Round 1 — M1 服务与鉴权（Gemini 设计评审）

| # | 发现 | 判定 | 处理 |
| --- | --- | --- | --- |
| 1 | uvicorn `Config(install_signal_handlers=False)` 可避免信号劫持 | **rejected** | uvicorn 0.54 无此参数；改用覆盖 `Server.capture_signals` |
| 2 | start 竞态：返回时可能尚未 listen | **accepted** | `start()` 轮询 `server.started`，测试立即请求 |
| 3 | 端口 0 真实端口提取 | **accepted** | `bound_port` 从 `server.servers[0].sockets[0]` 读取 |
| 4 | stop 等待长连接可能挂起 | **accepted** | `stop()` 10s 超时后 cancel；测试覆盖 |
| 5 | 证书过期/SAN 应检查 | **accepted（部分）** | 启动期检查有效期；SAN/主机名由真实 TLS 客户端测试覆盖 |
| 6 | 明文/弱 TLS 降级 | **accepted** | 仅 TLS 1.2+；明文测试必须失败 |

## Round 2 — 2.x 结构化错误与令牌加固

| # | 发现 | 判定 | 处理 |
| --- | --- | --- | --- |
| 1 | bool 是 int 子类，可能混入版本号字段 | **accepted** | `_amr_error_result` 显式排除 bool；补测试 |
| 2 | 令牌未校验最小长度 | **accepted（配置层）** | `load_config` 强制 >= 32 字符；`TokenAuthenticator` 保持通用 |
| 3 | `compare_digest` 长度不同时提前返回，泄露令牌长度 | **accepted** | 预计算 SHA-256 后再比较；补回归 |
| 4 | `TokenAuthenticator` 内未强制长度 | **rejected** | 配置层已强制；authenticator 允许测试/嵌入式使用短令牌，长度不等于熵 |
| 5 | `TokenIdentity \\| None` 在 Python 3.9 报错 | **rejected** | 项目要求 Python 3.10+；README 已声明 |

复审：修改后 Gemini 重新审查同一差分，返回 `{"findings":[]}`。

## Round 3 — 2.x 架构（Gemini 设计评审）

| # | 发现 | 判定 | 处理 |
| --- | --- | --- | --- |
| 1 | 多插件并发共享 UDS writer 会交织报文 | **accepted** | 架构文档明确每连接独立上下文、串行写、禁止跨任务共享 writer |
| 2 | 默认 `127.0.0.1` 无法支撑 Ubuntu→Unraid | **accepted（部署约束）** | 文档要求跨机显式 `bind_host` + 防火墙/私网 |
| 3 | 非归档写超时重试可能重复执行 | **accepted** | 适配器不自动重试；写操作依赖幂等键，超时返回“状态未知” |
| 4 | SQLite 并发写锁 | **rejected** | 1.x 已启用 WAL + busy_timeout；2.x 不改变写者模型 |

## Round 4 — 2.x UDS 适配器（Gemini 分块代码审计）

| # | 发现 | 判定 | 处理 |
| --- | --- | --- | --- |
| 1 | socket 创建到 chmod 之间有权限窗口 | **accepted** | 改为先 bind/chmod/listen，再交给 asyncio；不再改全局 umask |
| 2 | 连接任务未从集合移除 | **false-positive** | `finally` 中已有 `discard` |
| 3 | 响应帧误用请求大小上限 | **accepted** | 新增 `max_response_bytes`（默认 8MB） |
| 4 | 并发 `stop()` 竞争 | **accepted** | 生命周期 `asyncio.Lock`；并发 stop 测试通过 |
| 5 | `_remove_socket` TOCTOU | **rejected** | 父目录要求属主且不可 group/other 写；同用户威胁不在边界内 |
| 6 | 启动中误报 `is_started` | **accepted** | 全部绑定监听成功后才置位；失败回滚 |
| 7 | 重复 binding 静默覆盖 | **accepted** | 启动前 realpath 去重校验 |
| 8 | `shutdown` 可能取消自身 | **accepted** | 过滤 `asyncio.current_task()` |
| 9 | `_write` 未捕获序列化错误 | **accepted** | 捕获 TypeError/ValueError 并关闭连接 |
| 10 | 响应超限只记日志导致客户端悬挂 | **accepted** | 写失败返回 False，`_dispatch` 关闭连接；补测试 |
| 11 | `_prepare_parent_dir` 跟随符号链接 | **accepted** | 拒绝符号链接父目录 |
| 12 | HTTP 客户端未流式限流/未校验 id/未拒绝 SSE | **accepted** | 流式读取上限、JSON-RPC id 校验、拒绝 SSE、错误 data 白名单 |
| 13 | 配置读取 TOCTOU/无上限 | **accepted（部分）** | 按上限读取；同用户配置 TOCTOU 不作为安全边界 |
| 14 | 配置 `bindings` 缺键 KeyError | **false-positive** | 必填键在读取后已统一检查 |
| 15 | 映射一致性只单向 | **accepted** | 增加反向校验，新增工具未映射时启动失败 |

复审：修复后 Gemini 重审 server/client/config，剩余项均为已解释的误报或已修复。

### Round 4 补充（复审后修复）

- 客户端未校验 HTTPS/id/SSE → 已加显式校验与 MockTransport 测试。
- 适配器重复启动会抢占已有 socket → 启动前探测已有监听并拒绝；补测试。
- 映射一致性增加反向校验；配置读取改为按上限读取并校验端口。
- 服务端写失败改为关闭连接（不再静默让客户端悬挂）。

## Round 5 — DSH 插件 2.x

| # | 发现 | 判定 | 处理 |
| --- | --- | --- | --- |
| 1 | spool key 在无 message_id 时退化为 `sessionId::` | **accepted** | 改为消息内容哈希；补测试 |
| 2 | spool 读取失败仍允许 enqueue/rewrite，可能覆盖旧数据 | **accepted** | `_loadFailed` 阻断写入与 rewrite；补测试 |
| 3 | spool 容量计算未扣除已决定丢弃的前缀 | **accepted** | 从 `sizes.slice(dropCount)` 起算 |
| 4 | `/tmp` 回退路径固定 | **accepted** | 加 uid 后缀 |
| 5 | `markPermanent` 未持久化 / temp 文件未清理 | **false-positive** | 已有 `_rewrite()` 与 `rmSync` |
| 6 | turn/end 缺 turn 序号时都归为 "latest"，丢 turn | **accepted** | 消息哈希兜底；补测试 |
| 7 | 归档成功触发的 replay 与在途直发双发 | **accepted** | `inFlight` + `activeArchives` 门控；replay `skipKeys` |
| 8 | 两个 turn 监听器 key 前缀不一致 | **accepted** | 统一 `turn:<session>:<suffix>`；补测试 |
| 9 | 缺 session id 时伪造时间戳 id 拆散会话 | **accepted** | 跳过并记录，不用伪 id |
| 10 | 未处理 `dispose` / abort | **accepted** | `disposed` 标志 + 请求后 abort 检查 |
| 11 | `encodeFrame` 未校验 4MB 上限 | **accepted** | 编码前校验；补测试 |
| 12 | cleanup 移除 error 监听后 destroy 可能未捕获异常 | **accepted** | destroy 前保留空 error 监听 |
| 13 | `notify` 定时器未 unref / 无 close 清理 | **accepted** | unref + close 监听 |
| 14 | macOS 误用 `/run/user` | **accepted** | 仅 Linux 使用该回退 |

## Round 6 — OpenCode / OpenClaw / Hermes 插件 2.x

| 插件 | 发现与处理 |
| --- | --- |
| OpenCode | 超长 query 截断、重复 synthetic 注入检查已修复；`event` 异步异常已有外层 catch（误报）；spool 永久记录保留是设计（误报）；replay 与直发极端竞态保留低风险（服务端按 message_id 去重） |
| OpenClaw | 结构化 prompt 被误判 noise 已修复；`KeyedMutex` 锁泄漏已修复；损坏记录先备份再重建；`markSuccess/markFailed/markPermanent` 增加 sessionId 类型校验；同 session 并发已有 keyed mutex（误报）；未知错误默认 permanent 是保守策略（避免无限重试，接受） |
| Hermes | `connect_timeout` 未使用、error/id 校验顺序、`/tmp` 回退、prefetch future 竞态、client 使用未解析 socket 已修复；`AmrProtocolError` 已继承 `AmrRpcError`（误报）；`DEFAULT_SOCKET_PATH` 仅兼容导出（误报）；`call()` fail-open 是 1.x 兼容契约（接受） |
| bridge2 | `AMR2_AGENT` 路径字符清洗、连接超时、stdio 异常边界、非 Linux 回退已修复；非 dict 请求已有校验（误报）；Windows stdio 非目标平台 |

## Round 7 — 收尾

- 三个插件与 bridge2 的测试：OpenCode 15、OpenClaw 162、Hermes 23、Python 远程/适配器/bridge2 486。
- 待办：标准 MCP 客户端（Claude Code/Codex/ZCode/Doubao）配置与验证、部署物、迁移演练、最终全量回归。

## 审计工具

- 分块评审脚本：本地 `/tmp/amr2_gem_review.py`（读取网关配置，不写入仓库）。
- 每次复审必须附：命令、Gemini 原始 JSON、判定、修复提交/测试结果。
