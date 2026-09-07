#!/usr/bin/env bash
# ==============================================================================
# 工业级 Telegram 监控系统 — 生产环境一键部署脚本
# 支持：Ubuntu 20.04+/22.04+, Debian 10+/11+/12+, CentOS 7+/8+/9+
# 用法：sudo bash deploy.sh [deploy_dir]
#   deploy_dir 默认: /opt/monitor_bot
# ==============================================================================
set -euo pipefail

DEPLOY_DIR="${1:-/opt/monitor_bot}"
SCRIPT_NAME="monitor_bot_optimized.py"
SERVICE_NAME="monitor-bot.service"
FONT_NAME="SourceHanSans-Regular.ttf"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

log()  { echo -e "${GREEN}[✓]${NC} $*"; }
warn() { echo -e "${YELLOW}[!]${NC} $*"; }
err()  { echo -e "${RED}[✗]${NC} $*"; exit 1; }

# ── 0. 权限检查 ──────────────────────────────────────────────────────────────
[[ $(id -u) -eq 0 ]] || err "请使用 root 执行：sudo bash deploy.sh"

# ── 1. 检测系统并安装依赖 ────────────────────────────────────────────────────
log "检测操作系统..."

if [[ -f /etc/debian_version ]]; then
    log "检测到 Debian/Ubuntu 系系统，安装系统依赖..."
    apt-get update -qq
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-venv \
        fontconfig fonts-wqy-microhei fonts-wqy-zenhei \
        redis-server curl ca-certificates
    FONT_FALLBACK="WenQuanYi Micro Hei"
elif [[ -f /etc/redhat-release ]]; then
    log "检测到 CentOS/RHEL 系系统，安装系统依赖..."
    if command -v dnf &>/dev/null; then
        dnf install -y python3 python3-pip fontconfig wqy-microhei-fonts redis policycoreutils-python-utils
    else
        yum install -y python3 python3-pip fontconfig wqy-microhei-fonts redis
    fi
    # CentOS 7 需要额外安装 python3-devel
    if [[ "$(rpm -E %centos)" == "7" ]]; then
        yum install -y python3-devel
    fi
    FONT_FALLBACK="WenQuanYi Micro Hei"
else
    err "不支持的系统，请手动安装依赖。"
fi

# ── 2. 创建部署目录 ──────────────────────────────────────────────────────────
log "创建部署目录：${DEPLOY_DIR}"
mkdir -p "${DEPLOY_DIR}"
cd "${DEPLOY_DIR}"

# ── 3. 创建 Python 虚拟环境 ─────────────────────────────────────────────────
log "初始化 Python 虚拟环境..."
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip -q
pip install -q -r "${BASH_SOURCE%/*}/requirements.txt"

# ── 4. 复制核心文件 ─────────────────────────────────────────────────────────
SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log "复制监控脚本..."
cp "${SRC_DIR}/${SCRIPT_NAME}" "${DEPLOY_DIR}/${SCRIPT_NAME}"

# 字体处理：优先使用本地字体文件，否则复制系统文泉驿字体
if [[ -f "${SRC_DIR}/${FONT_NAME}" ]]; then
    cp "${SRC_DIR}/${FONT_NAME}" "${DEPLOY_DIR}/${FONT_NAME}"
    log "已复制字体文件：${FONT_NAME}"
else
    # 尝试找到系统中文真黑字体
    FONT_PATH=$(find /usr/share/fonts -name "*.ttf" -o -name "*.ttc" 2>/dev/null | head -1)
    if [[ -n "${FONT_PATH}" ]]; then
        cp "${FONT_PATH}" "${DEPLOY_DIR}/${FONT_NAME}"
        log "已复制系统字体：$(basename ${FONT_PATH}) → ${FONT_NAME}"
    else
        warn "未找到 TTF 字体文件，图表中文可能显示方块，请手动放置字体。"
    fi
fi

log "复制配置文件模板..."
cp "${SRC_DIR}/.env.example" "${DEPLOY_DIR}/.env"

# [优化] 限制 .env 权限，防止 Token 泄露
chmod 600 "${DEPLOY_DIR}/.env"

# ── 5. 安装 Systemd 服务 ────────────────────────────────────────────────────
log "安装 Systemd 守护服务..."
cp "${SRC_DIR}/${SERVICE_NAME}" /etc/systemd/system/${SERVICE_NAME}

# 替换服务文件中的路径（使用 sed 做精确替换）
sed -i "s|/opt/monitor_bot|${DEPLOY_DIR}|g" /etc/systemd/system/${SERVICE_NAME}
sed -i "s|monitor_bot_optimized.py|${SCRIPT_NAME}|g" /etc/systemd/system/${SERVICE_NAME}

systemctl daemon-reload
log "Systemd 服务单元已注册：${SERVICE_NAME}"

# ── 6. 配置 Redis（可选） ──────────────────────────────────────────────────
if systemctl is-active --quiet redis-server 2>/dev/null; then
    log "Redis 服务已运行"
else
    warn "Redis 未运行。若需 Redis 缓冲队列，请执行：systemctl enable --now redis-server"
    warn "当前将使用纯 SQLite 模式（仍稳定，仅失去队列缓冲）。"
fi

# ── 7. 启动服务 ─────────────────────────────────────────────────────────────
log "启动监控服务..."
systemctl enable "${SERVICE_NAME}"
systemctl start "${SERVICE_NAME}"
sleep 2

# ── 8. 状态报告 ─────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║           🚀 监控系统部署完成！                              ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  工作目录：${DEPLOY_DIR}                       ║"
echo "║  服务名称：${SERVICE_NAME}                     ║"
echo "║  脚本文件：${SCRIPT_NAME}                    ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  常用命令：                                                  ║"
echo "║    sudo systemctl status monitor-bot       ║"
echo "║    sudo journalctl -u monitor-bot -f            ║"
echo "║    sudo systemctl restart monitor-bot          ║"
echo "║    sudo systemctl stop monitor-bot            ║"
echo "║    sudo tail -f ${DEPLOY_DIR}/bot.log                      ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# 检查是否首次部署（.env 中仍是占位符）
if grep -q "YOUR_BOT_TOKEN_HERE" "${DEPLOY_DIR}/.env"; then
    echo -e "${YELLOW}⚠️  请尽快编辑配置文件：${DEPLOY_DIR}/.env${NC}"
    echo -e "${YELLOW}   将 YOUR_BOT_TOKEN_HERE 替换为 BotFather 给的 Token${NC}"
    echo -e "${YELLOW}   将 YOUR_ADMIN_CHAT_ID_NUMBER 替换为你的 Chat ID${NC}"
    echo ""
    echo -e "${YELLOW}   修改后执行：sudo systemctl restart monitor-bot${NC}"
    echo ""
fi

# 检查服务状态
if systemctl is-active --quiet monitor-bot; then
    echo -e "${GREEN}✓ 服务运行中 ✅${NC}"
    sleep 1
    journalctl -u monitor-bot -n 5 --no-pager
else
    echo -e "${RED}✗ 服务未启动，查看日志：${NC}"
    journalctl -u monitor-bot -n 20 --no-pager
fi
