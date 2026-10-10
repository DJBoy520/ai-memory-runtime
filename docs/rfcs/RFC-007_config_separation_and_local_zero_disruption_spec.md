# RFC-007: AMR 模板与本地私有配置分离改造规范 (Zero-Disruption Architecture)

- **作者**: Hermes (方案架构师)
- **目标审查方**: OpenClaw (独立审计, DeepSeek 引擎)
- **状态**: PROPOSED (等待审计)
- **日期**: 2026-10-08

---

## 一、 背景与业务诉求

AMR (AI Memory Runtime) 拟作为开源项目向社区发布与推广。
然而目前代码库中存在配置与本地开发环境的深度耦合问题：
1. `config/config.yaml` 包含了开发机私网 IP (`<LOCAL_QDRANT_IP>`)、真实 API 密钥与专属本地路径；
2. 如果直接粗暴替换代码中的 IP 和路径，会导致正在运行的本地微服务集群（`amr.service`, `Hermes`, `OpenClaw`, `OpenCode`）瞬间网络中断或鉴权失败；
3. 必须通过**“配置分层 + 模板外置 + 渐进加载 + 本地免碰”**的工业级方案，实现：
   - **对外**：Git 仓库中只有纯净、合规的通用模板 `config.example.yaml`；
   - **对内**：本地生产服务继续以 100% 确定性读取本地私有配置，零重启故障、零环境变量依赖、零性能损耗。

---

## 二、 核心架构设计与四重防破损原则

### 1. 单一真相源与分层加载顺序 (SSOT Configuration Hierarchy)
改造核心配置加载模块 `config/settings.py`，使 `load_config(path=None)` 支持安全回退，且**对既有行为 100% 向后兼容**：
1. **优先度 1（显式路径）**：若调用者传参 `path`（如守护进程显式指定绝对路径），直接加载；
2. **优先度 2（本地生产私有配置）**：若存在 `config/config.yaml`（即本地当前正在使用的私有配置），**直接加载该文件**；
3. **优先度 3（缺省开源模板）**：若 `config/config.yaml` 不存在，且存在 `config/config.example.yaml`，按开源默认配置启动（默认监听 `127.0.0.1`，提示用户复制生成私有配置）。

### 2. Git 隔离策略与隐蔽泄露物剔除 (物理级防泄露)
- **创建开源模板**：`config/config.example.yaml`，填入规范的占位符（`host: 127.0.0.1`，`port: 6333`，`api_key: ""`），并确保 `search:`（`default_score_threshold: 0.52`）等全量字段 100% Schema 对齐；
- **私有文件安全门禁**：确认 `config/config.yaml` 处于未跟踪状态，在 `.gitignore` 严密锁死；
- **剔除历史二进制存证包**：彻底将 `docs/*.aep`（包含历史 Key 与签名的归档文件）从 Git 索引中移除（`git rm --cached`）；
- **门禁锁死**：在 `.gitignore` 明确添加：
  ```gitignore
  # 私有运行时配置与密钥
  config/config.yaml
  config/*.local.yaml
  .env*
  *.aep
  *.log
  !config/config.example.yaml
  ```
- **效果**：本地磁盘上的 `config/config.yaml` 完好无损，本地 `amr.service` 继续实时读取它；而 GitHub 仓库永远只能看到 `config.example.yaml`。

### 3. 路径抽象化与跨平台支持 (Path Normalization)
- **消除绝对路径硬编码**：
  - `systemd/*.service` 模板使用标准的 `%h`（主目录宏）和 `/run/user/%U`，替代写死的 `$HOME`；
  - 源码内部确保以 `storage_cfg.sqlite_path`（相对路径）或 `Path.home() / ".amr"` 锚定；
- **Systemd 单元零破损**：
  - 本地 `~/.config/systemd/user/amr.service` 已由 systemd 正确加载运行，仓库模板优化不影响运行中的实例。

---

## 三、 详细实施步骤 (Phase 1)

1. **Step 1: 提炼开源配置模板**
   - 提取 `config/config.yaml` 完整 Schema 骨架至 `config/config.example.yaml`；
   - 补齐 `search:` 段及所有字段，脱敏地址与密钥。
2. **Step 2: 剔除二进制隐蔽泄露与更新 .gitignore**
   - 执行 `git rm --cached docs/*.aep`；
   - 更新 `.gitignore` 固化 `config/config.yaml`, `*.aep`, `.env*` 规则；
   - 断言 `git check-ignore -v config/config.yaml` 命中规则。
3. **Step 3: 抽象化 Systemd 模板**
   - 将 `systemd/amr.service` 与 `systemd/amr-reconciliation.service` 中的路径更新为 `%h` 宏。
4. **Step 4: 本地回归测试与服务健康断言**
   - 验证 `systemctl --user status amr.service` 依然 Active (running)；
   - 验证 IPC 接口与 MCP 搜索通道畅通。

---

## 四、 安全边界与红线要求

1. **严禁物理删除**：严禁 `rm -f config/config.yaml`，必须使用 `git rm --cached`；
2. **零环境变量依赖**：遵循老板长期原则，所有核心配置收敛于配置文件，严禁依赖环境变量注入覆盖；
3. **双重防脱节**：`config.example.yaml` 必须与 `config.yaml` 保持字段命名、嵌套结构 100% 对齐。
