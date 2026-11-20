# AI Memory Runtime (AMR) 容器镜像
# 构建（仓库根目录执行）：
#   docker build -f deploy/docker/Dockerfile -t djboy520/amr:2.1.0 .
#
# 设计要点：
#   * 不内置 Qdrant（生产使用外部 Qdrant 服务器，由 config.yaml 指向）；
#   * 不内置 BGE-M3 模型与任何数据/密钥（运行时以只读卷挂载 /models）；
#   * PyTorch 使用 CPU 轮子，与生产 venv 同版本（2.6.0），仅把 cu124 换成 cpu；
#   * 其余依赖版本与生产 venv 完全一致（transformers 5.22.2 等）。

FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# CPU 版 PyTorch（与生产 2.6.0 同版本；Unraid 宿主无 GPU）
RUN pip install --index-url https://download.pytorch.org/whl/cpu "torch==2.6.0+cpu" \
 && pip install \
      "transformers==5.22.2" \
      "tokenizers==0.23.3" \
      "safetensors==0.8.0" \
      "huggingface-hub==2.4.0" \
      "qdrant-client==1.20.0" \
      "pydantic==2.14.3" \
      "PyYAML==6.0.3" \
      "jsonschema==4.26.0" \
      "mcp==1.30.0" \
      "uvicorn==0.54.0"

WORKDIR /app
COPY src/ ./src/
COPY VERSION pytest.ini ./

# 与宿主约定 uid 1000（Unraid 侧 appdata 目录 chown 1000:1000）
RUN useradd --uid 1000 --no-create-home --home-dir /app amr \
 && mkdir -p /run/amr /data/amr \
 && chown -R amr:amr /run/amr /data/amr /app

USER amr

# business/admin UDS 由 daemon 建在共享卷 /run/amr；HTTPS 网关端口 8443
EXPOSE 8443

VOLUME ["/run/amr", "/data/amr"]

# daemon 默认进程；compose 的 amr-gateway 用独立 command 覆盖为网关
CMD ["python", "src/main.py"]

ARG AMR_VERSION=2.1.0
ARG AMR_REVISION=unknown
ARG AMR_SOURCE=https://github.com/DJBoy520/ai-memory-runtime
LABEL org.opencontainers.image.title="ai-memory-runtime" \
      org.opencontainers.image.description="Unified Multi-Agent Semantic Memory OS & SSOT (daemon + authenticated HTTPS MCP gateway)" \
      org.opencontainers.image.version="${AMR_VERSION}" \
      org.opencontainers.image.revision="${AMR_REVISION}" \
      org.opencontainers.image.source="${AMR_SOURCE}" \
      org.opencontainers.image.licenses="MIT"
