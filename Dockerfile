# ==============================================================================
# 多阶段构建：最小化镜像体积（~180MB vs 基础 Python ~900MB）
# ==============================================================================

# ── 阶段 1：构建依赖 ────────────────────────────────────────────────────────
FROM python:3.11-slim-bookworm AS builder

WORKDIR /build

# 安装构建依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libc-dev \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖清单并安装
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# 下载中文字体（文泉驿微米黑）
RUN apt-get update && apt-get install -y --no-install-recommends \
    fonts-wqy-microhei \
    && fc-cache -fv \
    && rm -rf /var/lib/apt/lists/*

# ── 阶段 2：运行时镜像 ──────────────────────────────────────────────────────
FROM python:3.11-slim-bookworm

# 元数据
LABEL maintainer="Agnes <agnes@sapiens.ai>"
LABEL description="Industrial Distributed Telegram Monitor Bot"
LABEL version="1.0.0"

# 创建工作目录
WORKDIR /app

# 从 builder 阶段复制已安装的 Python 包
COPY --from=builder /install /usr/local

# 复制字体文件
COPY --from=builder /usr/share/fonts /usr/share/fonts
RUN fc-cache -fv

# 复制应用代码
COPY monitor_bot_optimized.py .
COPY requirements.txt .

# 创建数据/日志目录并映射到卷
RUN mkdir -p /app/data /app/logs && \
    chmod +x monitor_bot_optimized.py

# 切换到数据目录，确保相对路径文件写入正确位置
WORKDIR /app/data

# 环境变量
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    DB_FILE=/app/data/monitor_history.db \
    LOG_FILE=/app/logs/bot.log \
    DATA_FILE=/app/data/monitored_sites.json \
    SERVICES_FILE=/app/data/monitored_services.json

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD pgrep -f monitor_bot_optimized.py || exit 1

# 启动命令
CMD ["python3", "monitor_bot_optimized.py"]
