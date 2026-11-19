# CPU Quickstart 验收记录

日期为 2026-11-19。本机 Linux x86_64，AMD Ryzen 9 3900。现有生产实例继续运行。以下测试使用独立临时目录和独立 Qdrant Local，模型常驻 CPU。运行环境中存在 NVIDIA 显卡，不能作为无 NVIDIA 干净机器验收。

## 检索样例

通过 Claude Code 身份的 MCP bridge 写入 20 条不同记忆，等待后台索引完成，再用 Codex 身份的 MCP bridge 对每条执行 Top 3 检索。两端使用各自的 `AMR_SOURCE_AGENT`。检索判断基于返回的 `memory_id`。下表是查询及命中结果。

| # | 查询 | Top 3 命中 |
|---|---|---|
| 1 | 订单数据存储在什么数据库？ | 是 |
| 2 | 我们如何保护用户密码？ | 是 |
| 3 | 图片上传到哪里保存？ | 是 |
| 4 | 热门商品的缓存使用什么？ | 是 |
| 5 | 文章全文搜索用哪个系统？ | 是 |
| 6 | 用户界面采用了什么框架？ | 是 |
| 7 | 数据库备份安排在什么时间？ | 是 |
| 8 | 支付失败应当重试多少次？ | 是 |
| 9 | 分页默认页面大小是多少？ | 是 |
| 10 | 测试的 CI 由什么服务运行？ | 是 |
| 11 | How are production containers orchestrated? | 是 |
| 12 | Which service sends outgoing emails? | 是 |
| 13 | How long do we keep application logs? | 是 |
| 14 | What deployment strategy enables rollback? | 是 |
| 15 | What collects service metrics? | 是 |
| 16 | Where does the team track defects? | 是 |
| 17 | Which port handles encrypted web traffic? | 是 |
| 18 | Which Python formatter is the standard? | 是 |
| 19 | What protocol is used for client authentication? | 是 |
| 20 | What language is the mobile app written in? | 是 |

结果 20/20。20 次热搜索耗时（毫秒）依次为 142.3、158.5、130.0、141.4、136.8、131.9、153.2、148.2、134.8、139.9、139.0、147.6、151.8、164.1、152.8、138.7、126.7、136.2、151.7、156.6。中位数 141.9 ms，最近秩 p95 158.5 ms。20 次创建中位数 0.7 ms。连续创建 20 条后，最后一条在约 5.19 秒后可搜索，包含排队。

CPU-only 虚拟环境中，单条演示记忆创建返回 3.4 ms，状态 `pending`。`memory_get` 立即读回同一 ID。第一次搜索在 0.23 秒时为空，等待 1 秒后的第二次搜索在 1.43 秒时命中。管理状态显示 `READY`、`device=cpu`、`idle_timeout_seconds=0`，在另一轮 CPU-only 检索后已完成 46 次推理。

以上性能和检索测试使用独立 Python 3.14 虚拟环境及 `torch 2.9.1+cpu`，复用了仓库中已有的模型文件。模型缓存和依赖就绪时，启动至 READY 耗时 4.949 秒。完整从零下载模型的耗时未测。CPU wheel 未加载 CUDA 动态库。20 条内容分别涉及 PostgreSQL、Argon2、S3、Redis、Elasticsearch、React、备份时间、支付重试、分页、GitHub Actions、Kubernetes、SES、日志保留、蓝绿部署、Prometheus、Jira、HTTPS 端口、Black、OAuth 和 Kotlin。失败项为零。

## 故障和自动测试

- 模型下载失败：空缓存并设置 `HF_HUB_OFFLINE=1`，输出 `模型下载失败` 和具体原因，退出非零。
- Qdrant 初始化失败：用普通文件占据试用 Qdrant 目录，输出 `Qdrant 初始化或读写自检失败`，退出非零。
- socket 不可达：构造超长 Unix socket 路径，输出 `业务 socket 不可达或自检失败: AF_UNIX path too long`，退出非零。
- 运行中的试用实例拒绝 `--clean`。停止后执行 `--clean`，试用数据和 socket 目录均已删除，生产服务仍为原 PID。
- `--download-only` 在模型已有时完成。CPU-only 环境导入后 `torch.version.cuda` 为 `None`，未加载 CUDA、cuDART 或 cuBLAS 动态库。试用目录权限 0700，业务 socket 权限 0600。
- 本地完整测试为 931 passed、15 skipped、9 warnings，与改动前 15 skipped、9 warnings 数量一致。警告包括 Qdrant 和 asyncio 提示。
- 本机真实 Codex 新会话通过 MCP 查回 `mem_20261119_fc05ae`。禁止审批的非交互模式会阻止 MCP 调用，允许工具调用后成功。Claude Code 非交互调用未能在限定时间内返回，因此这里的写入来自按 Claude Code 身份调用的 MCP bridge。

## 尚待完成的验收

- [ ] 无 NVIDIA 的干净 Linux 机器上，由陌生开发者按 README 完成 Claude Code 写入、全新 Codex 会话搜索并比较同一 ID，记录耗时。
- [ ] 记录包含模型下载和不包含模型下载的完整冷装耗时。当前模型复用了仓库中的本地权重。
- [ ] 一段真实 Claude Code 存储、等待索引、切换 Codex 搜索的终端录屏。未以模拟调用冒充真实客户端录屏。

## 验收清单

- [ ] 无 NVIDIA 干净机、陌生开发者完成真实双客户端闭环并计时。当前本机有 NVIDIA，Claude Code 实际调用两次未在限定时间内返回。
- [ ] 冷装耗时含模型下载与不含模型下载分别计时。模型沿用本地权重，首次 CPU wheel 下载过程因修正 Python 3.14 依赖而中断重试，不能当成完整冷装计时。已就绪依赖和模型的启动耗时为 4.949 秒。
- [x] 20 条中英文检索样例至少 18 条 Top 3 命中。结果 20/20，详见上表。
- [x] 热状态计时：创建中位 0.7 ms，单条首次可搜 1.43 秒，20 次搜索 p95 158.5 ms。首次可搜数据含一次立即搜索及 1 秒等待。
- [x] 模型常驻与二次搜索：`idle_timeout_seconds=0`，多次检索后仍为 `READY`。
- [x] 停止后 `--clean` 删除试用数据和 socket，生产服务保持同一 PID。
- [x] 三个失败场景输出可读原因，详见“故障和自动测试”。
- [ ] 真实 Claude Code 到 Codex 的终端录屏，受 Claude Code 非交互请求超时阻塞。
- [x] 完整测试 931 passed、15 skipped、9 warnings。skip 和 warning 数量与修改前一致。
