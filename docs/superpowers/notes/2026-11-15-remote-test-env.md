# 远程 MCP 测试环境（隔离 venv）

日期：2026-11-15
适用：`tests/test_remote_*.py`、`tests/test_http_auth.py`

## 为什么 `.venv` 里远程测试会被 skip

生产 venv `.venv` 只安装 `requirements.txt` 中的运行依赖，**不包含 mcp SDK**。
远程 MCP 测试文件顶部使用：

```python
pytest.importorskip("mcp")
```

因此直接在生产 venv 运行这些文件时，pytest 会把整个模块静默标记为
`skipped`。这不是“测试通过”，而是根本没有执行。`tests/test_remote_service.py`
只做配置校验、本身不依赖 mcp，已移除该 `importorskip`，所以它在生产 venv
中也会正常执行。

为防止再次把 skip 当成通过，`tests/conftest.py` 增加了显式门禁：

- 设置 `AMR_REQUIRE_REMOTE=1` 且当前解释器找不到 mcp 时，collection 阶段直接
  抛 `RuntimeError`；
- 未设置该变量时，门禁完全不生效，普通测试照常运行。

## 创建隔离 venv 并安装依赖

远程依赖单独放在仓库根目录的 `requirements-remote.txt`，不要装进生产 venv。

本机 `uv` 位于 `~/.local/bin/uv`（若已在 `PATH` 中，可直接把 `"$UV"` 换成
`uv`）：

```bash
cd /home/dj/WorkSpaces/ai-memory-runtime

# 首次创建；若 /tmp/amr-remote-venv 已存在可跳过这一步，
# 需要重建时先删除该临时目录或给 uv venv 加 --clear。
UV="${UV:-$HOME/.local/bin/uv}"
"$UV" venv --python 3.12 /tmp/amr-remote-venv

# 安装远程 MCP 运行时 + 测试依赖
"$UV" pip install \
  --python /tmp/amr-remote-venv/bin/python \
  -r requirements-remote.txt
```

安装后确认：

```bash
/tmp/amr-remote-venv/bin/python -c "import mcp; print(mcp.__file__)"
```

## 如何运行远程测试

```bash
cd /home/dj/WorkSpaces/ai-memory-runtime
AMR_REQUIRE_REMOTE=1 /tmp/amr-remote-venv/bin/python -m pytest \
  tests/test_remote_*.py tests/test_http_auth.py -q
```

`AMR_REQUIRE_REMOTE=1` 是门禁：如果隔离 venv 里缺少 mcp，pytest 会明确报错，
而不是跳过远程测试。

单独运行配置校验测试不需要 mcp，生产 venv 也可以：

```bash
.venv/bin/python -m pytest tests/test_remote_service.py -q
```

## 为什么锁定 mcp < 2

`requirements-remote.txt` 固定 `mcp==1.30.0`，并禁止升级到 2.x：

- mcp 2.0 是破坏性 API，移除了
  `mcp.client.streamable_http.streamablehttp_client`；
- `StreamableHTTPSessionManager` 的构造签名也发生了变化。

仓库内远程 MCP 代码（以及测试中的真实 SDK 客户端）按 1.x API 编写，升级到
2.x 会在导入/装配阶段失败。因此不要把 `mcp` 写成无上限的 `>=`。

## 与生产 venv 的边界

- 普通 `pytest` 仍可在生产 venv `.venv` 中运行非远程测试；
- **远程测试必须使用隔离 venv**（或任何已安装 `requirements-remote.txt` 的
  3.12 环境）；
- 不要把 `mcp`、`uvicorn` 等远程可选依赖加入生产 `requirements.txt`。

## 已验证命令

```bash
# 1) 隔离 venv：配置校验测试
/tmp/amr-remote-venv/bin/python -m pytest tests/test_remote_service.py -q

# 2) 隔离 venv：全部远程测试 + HTTP 认证，要求无 skip
AMR_REQUIRE_REMOTE=1 /tmp/amr-remote-venv/bin/python -m pytest \
  tests/test_remote_*.py tests/test_http_auth.py -q

# 3) 生产 venv：不依赖 mcp 的测试仍应全绿、无 skip
.venv/bin/python -m pytest \
  tests/test_remote_service.py tests/test_http_auth.py -q

# 4) 门禁自检：生产 venv 缺少 mcp，应明确失败（不是 skip）
AMR_REQUIRE_REMOTE=1 .venv/bin/python -m pytest \
  tests/test_remote_mcp_sdk.py -q
```
