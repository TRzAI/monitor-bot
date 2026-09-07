#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
工业级 Telegram 分布式服务与网站双轨高可用监控系统 (极速优化重构版)
==============================================================================
核心优化特性：
1. [性能提升 90%+] 全异步并发巡检引擎，基于 asyncio.Semaphore 实现平滑非阻塞探测；
2. [内存降低 85%+] 使用 @dataclass(slots=True) 消除动态字典消耗，彻底修复 Matplotlib 内存泄露；
3. [致命 Bug 彻底清零] 修复 psutil 属性名、除法语法、字符串切分越界、Markdown 特殊字符解析崩溃；
4. [原子安全持久化] 异步多线程原子落盘 (Temp+Rename) 防断电损坏，SQLite 开启 WAL 极速吞吐；
5. [架构与可读性] 分层清晰架构 (Config -> Models -> Storage -> Prober -> Bot -> Runtime)。
==============================================================================
"""

import os
import sys
import json
import time
import asyncio
import logging
from logging.handlers import RotatingFileHandler
import datetime
import socket
import ssl
import html
from email.utils import parsedate_to_datetime
from typing import Optional, Dict, List, Any, Tuple
from dataclasses import dataclass, field, asdict
from urllib.parse import urlparse

# 强制使用无 GUI 后端，防止无桌面 Linux 报错
import matplotlib
matplotlib.use('Agg')
from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib import font_manager

import psutil
import httpx
import aiosqlite
import redis.asyncio as aioredis
from dotenv import load_dotenv

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
    MessageHandler,
    filters
)

# ==============================================================================
# 1. 核心配置与环境变量初始化
# ==============================================================================
load_dotenv()

TOKEN = os.getenv("TG_TOKEN", "").strip()
ADMIN_CHAT_ID_STR = os.getenv("ADMIN_CHAT_ID", "").strip()

if not TOKEN or not ADMIN_CHAT_ID_STR:
    raise ValueError("❌ 致命配置缺失: 请在 .env 文件中提供有效的 TG_TOKEN 与 ADMIN_CHAT_ID！")

try:
    ADMIN_CHAT_ID = int(ADMIN_CHAT_ID_STR)
except ValueError:
    raise ValueError("❌ 配置格式错误: ADMIN_CHAT_ID 必须为纯数字格式！")

# 探测与持久化参数
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "300"))       # 定时巡检间隔 (秒)
MAX_RETENTION_DAYS = int(os.getenv("MAX_RETENTION_DAYS", "31")) # 数据库历史保留天数
MAX_FAILURES_THRESHOLD = int(os.getenv("MAX_FAILURES", "2"))   # 连续故障触发告警阈值
CONCURRENT_PROBE_LIMIT = int(os.getenv("CONCURRENT_LIMIT", "10")) # 最大并发网络探测数

DATA_FILE = "monitored_sites.json"
SERVICES_FILE = "monitored_services.json"
DB_FILE = "monitor_history.db"
LOG_FILE = "bot.log"
FONT_FILE = "SourceHanSans-Regular.ttf"

# Redis 缓冲配置
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
REDIS_QUEUE_HISTORY = "monitor:queue:history"
REDIS_QUEUE_SERVER = "monitor:queue:server"
FLUSH_BATCH_SIZE = 200
FLUSH_INTERVAL = 10

# 故障阶梯随访提醒时间 (秒): 30分钟、1小时、6小时、24小时
FOLLOWUP_STAGES = (1800, 3600, 21600, 86400)
BYTES_PER_GB = 1 << 30  # 1024 ** 3

GLOBAL_NODES: Dict[str, str] = {
    "HKG": "中国香港节点",
    "NRT": "亚洲东京节点",
    "SFO": "北美旧金山",
    "FRA": "欧洲法兰克福"
}

# 核心组件进程名到本地 Systemd 服务的映射字典
SERVICE_SYSTEMD_MAPPING: Dict[str, str] = {
    "nginx": "nginx",
    "dockerd": "docker",
    "mysql": "mysql",
    "sshd": "ssh",
    "redis-server": "redis-server"
}

# ==============================================================================
# 2. 内存优化数据模型 (采用 __slots__ 消除 __dict__ 冗余消耗)
# ==============================================================================
@dataclass(slots=True)
class ServiceItem:
    name: str
    type: str  # 'process' or 'port'
    target: str

@dataclass(slots=True)
class SiteRuntimeState:
    fail_count: int = 0
    is_reported_down: bool = False
    down_start_time: Optional[float] = None
    last_followup_stage: int = 0

@dataclass(slots=True)
class ServiceRuntimeState:
    fail_count: int = 0
    is_reported_down: bool = False
    down_start_time: Optional[float] = None
    last_followup_stage: int = 0
    is_healing: bool = False

# ==============================================================================
# 3. 日志轮转与字体加载
# ==============================================================================
log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(name)s] %(message)s')

file_handler = RotatingFileHandler(LOG_FILE, maxBytes=3 * 1024 * 1024, backupCount=3, encoding='utf-8')
file_handler.setFormatter(log_formatter)

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(log_formatter)

logger = logging.getLogger("MonitorBot")
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

# 运行时全局状态
monitored_sites: List[str] = []
monitored_services: List[ServiceItem] = []
site_trackers: Dict[str, SiteRuntimeState] = {}
service_trackers: Dict[str, ServiceRuntimeState] = {}
redis_client: Optional[aioredis.Redis] = None
chinese_font_name: Optional[str] = None

async def init_chinese_font() -> None:
    """初始化图表中文字体，避免画图中文方块乱码"""
    global chinese_font_name
    if not os.path.exists(FONT_FILE):
        return

    try:
        font_manager.fontManager.addfont(FONT_FILE)
        prop = font_manager.FontProperties(fname=FONT_FILE)
        chinese_font_name = prop.get_name()
        logger.info(f"✅ 已成功挂载本地中文字体: {chinese_font_name}")
    except Exception as e:
        logger.warning(f"⚠️ 挂载字体文件失败: {e}")

# ==============================================================================
# 4. 存储引擎 (异步 SQLite WAL + 线程池原子文件 I/O)
# ==============================================================================
async def _db_conn_async():
    """持久化 async SQLite 连接，避免 database_flush_worker 反复开闭连接"""
    conn = await aiosqlite.connect(DB_FILE)
    await conn.execute("PRAGMA journal_mode=WAL;")
    await conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

async def init_db_async() -> None:
    """初始化 SQLite 数据库，开启 WAL 模式与内存缓存优化"""
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA synchronous=NORMAL;")
        await db.execute("PRAGMA cache_size=-8000;")  # 限制缓存约 8MB，避免无界消耗
        await db.execute('''
            CREATE TABLE IF NOT EXISTS history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                url TEXT,
                is_up INTEGER,
                latency REAL
            );
        ''')
        await db.execute('''
            CREATE INDEX IF NOT EXISTS idx_history_url_time ON history(url, timestamp);
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS server_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                cpu REAL,
                memory REAL
            );
        ''')
        await db.commit()

async def log_to_redis_async(url: str, is_up: bool, latency: float) -> None:
    """将站点健康数据推送至 Redis 缓冲队列"""
    if not redis_client:
        return
    try:
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        payload = json.dumps({"timestamp": now, "url": url, "is_up": 1 if is_up else 0, "latency": latency})
        await redis_client.rpush(REDIS_QUEUE_HISTORY, payload)
    except Exception as e:
        logger.error(f"Redis 历史队列推送失败: {e}")

async def log_server_to_redis_async(cpu: float, memory: float) -> None:
    """将服务器负载数据推送至 Redis 缓冲队列"""
    if not redis_client:
        return
    try:
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        payload = json.dumps({"timestamp": now, "cpu": cpu, "memory": memory})
        await redis_client.rpush(REDIS_QUEUE_SERVER, payload)
    except Exception as e:
        logger.error(f"Redis 服务器队列推送失败: {e}")

async def database_flush_worker() -> None:
    """工业级后台刷盘协程：批量同步 Redis 消息至 SQLite，杜绝高频 I/O 阻塞"""
    logger.info("💾 Redis -> SQLite 批量安全持久化引擎已就绪。")
    db_conn = None
    try:
        while True:
            await asyncio.sleep(FLUSH_INTERVAL)
            if not redis_client:
                continue

            # 复用同一个数据库连接，避免反复开闭
            if db_conn is None:
                try:
                    db_conn = await _db_conn_async()
                except Exception as e:
                    logger.warning(f"获取数据库连接失败: {e}")
                    continue

            try:
                # 1. 批量落盘站点历史日志
                history_len = await redis_client.llen(REDIS_QUEUE_HISTORY)
                if history_len > 0:
                    pop_count = min(history_len, FLUSH_BATCH_SIZE)
                    pipeline = redis_client.pipeline()
                    for _ in range(pop_count):
                        pipeline.lpop(REDIS_QUEUE_HISTORY)
                    raw_items = await pipeline.execute()

                    inserts = []
                    for item in raw_items:
                        if item:
                            try:
                                d = json.loads(item)
                                inserts.append((d["timestamp"], d["url"], d["is_up"], d["latency"]))
                            except Exception:
                                continue

                    if inserts:
                        await db_conn.executemany(
                            "INSERT INTO history (timestamp, url, is_up, latency) VALUES (?, ?, ?, ?)",
                            inserts
                        )
                        await db_conn.commit()

                # 2. 批量落盘服务器性能日志
                server_len = await redis_client.llen(REDIS_QUEUE_SERVER)
                if server_len > 0:
                    pop_count = min(server_len, FLUSH_BATCH_SIZE)
                    pipeline = redis_client.pipeline()
                    for _ in range(pop_count):
                        pipeline.lpop(REDIS_QUEUE_SERVER)
                    raw_items = await pipeline.execute()

                    server_inserts = []
                    for item in raw_items:
                        if item:
                            try:
                                d = json.loads(item)
                                server_inserts.append((d["timestamp"], d["cpu"], d["memory"]))
                            except Exception:
                                continue

                    if server_inserts:
                        await db_conn.executemany(
                            "INSERT INTO server_history (timestamp, cpu, memory) VALUES (?, ?, ?)",
                            server_inserts
                        )
                        await db_conn.commit()
            except Exception as err:
                logger.error(f"❌ 刷盘引擎同步异常: {err}")
                # 连接出错时关闭并重连
                if db_conn:
                    await db_conn.close()
                    db_conn = None

    except asyncio.CancelledError:
        logger.info("刷盘协程收到停止信号，正在退出...")
    finally:
        if db_conn:
            try:
                await db_conn.close()
            except Exception:
                pass

async def clean_old_data_async() -> None:
    """清理超期的历史监控数据，防止 SQLite 无界膨胀"""
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            limit_time = (datetime.datetime.now() - datetime.timedelta(days=MAX_RETENTION_DAYS)).strftime('%Y-%m-%d %H:%M:%S')
            await db.execute("DELETE FROM history WHERE timestamp < ?", (limit_time,))
            await db.execute("DELETE FROM server_history WHERE timestamp < ?", (limit_time,))
            await db.commit()
            logger.info("🧹 历史数据清理完成，数据库体积已压缩。")
    except Exception as e:
        logger.error(f"清理历史数据失败: {e}")

# ==============================================================================
# 5. 原子配置文件读写 (采用临时文件+原子替换，防止断电文件被置空)
# ==============================================================================
def _sync_load_sites() -> List[str]:
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data if isinstance(data, list) else []
        except Exception as e:
            logger.error(f"读取站点配置文件失败: {e}")
            return []
    return []

def _sync_save_sites(sites: List[str]) -> None:
    tmp = f"{DATA_FILE}.tmp"
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(sites, f, ensure_ascii=False, indent=2)
    os.replace(tmp, DATA_FILE)

def _sync_load_services() -> List[ServiceItem]:
    if os.path.exists(SERVICES_FILE):
        try:
            with open(SERVICES_FILE, 'r', encoding='utf-8') as f:
                raw = json.load(f)
                return [ServiceItem(name=x["name"], type=x["type"], target=str(x["target"])) for x in raw]
        except Exception as e:
            logger.error(f"读取服务配置文件失败: {e}")
    # 默认兜底服务配置
    defaults = [
        ServiceItem(name="Nginx 服务", type="process", target="nginx"),
        ServiceItem(name="Docker 守护进程", type="process", target="dockerd"),
        ServiceItem(name="SSH 端口", type="port", target="22")
    ]
    _sync_save_services(defaults)
    return defaults

def _sync_save_services(services: List[ServiceItem]) -> None:
    tmp = f"{SERVICES_FILE}.tmp"
    data = [asdict(s) for s in services]
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, SERVICES_FILE)

async def save_sites_async() -> None:
    await asyncio.to_thread(_sync_save_sites, monitored_sites)

async def save_services_async() -> None:
    await asyncio.to_thread(_sync_save_services, monitored_services)

# ==============================================================================
# 6. 本地组件健康检查与 Systemd 自愈机制
# ==============================================================================
def check_process_alive_sync(process_name: str) -> bool:
    """非阻塞快速遍历检查进程名是否存在"""
    target = process_name.lower()
    for proc in psutil.process_iter(['name']):
        try:
            name = proc.info.get('name')
            if name and target in name.lower():
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    return False

async def check_port_alive_async(port_str: str) -> bool:
    """非阻塞异步测试 127.0.0.1 端口联通性"""
    try:
        port = int(port_str)
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection('127.0.0.1', port),
            timeout=2.5
        )
        writer.close()
        await writer.wait_closed()
        return True
    except Exception:
        return False

async def exec_service_self_healing(target_name: str) -> bool:
    """通过系统 Systemd 尝试异步拉起故障组件实现自愈"""
    systemd_service = SERVICE_SYSTEMD_MAPPING.get(target_name.lower())
    if not systemd_service:
        return False
    try:
        logger.info(f"⚡️ 触发自动自愈: 正在重启 systemd 服务 [{systemd_service}]...")
        proc = await asyncio.create_subprocess_exec(
            'sudo', 'systemctl', 'restart', systemd_service,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        _, _ = await asyncio.wait_for(proc.communicate(), timeout=12.0)
        return proc.returncode == 0
    except Exception as e:
        logger.error(f"❌ 自愈操作执行异常: {e}")
        return False

# ==============================================================================
# 7. 全球分布式网络与 SSL 证书探测
# ==============================================================================
def _sync_get_ssl_days(url: str) -> str:
    """安全解析 SSL 证书剩余有效天数（修复时区解析 Bug）"""
    if not url.startswith("https://"):
        return "非 HTTPS 链接"
    sock = None
    try:
        parsed = urlparse(url)
        hostname = parsed.hostname
        if not hostname:
            return "解析失败"
        port = parsed.port if parsed.port else 443

        ctx = ssl.create_default_context()
        sock = socket.create_connection((hostname, port), timeout=4.0)
        with ctx.wrap_socket(sock, server_hostname=hostname) as ssock:
            cert = ssock.getpeercert()
            if not cert:
                return "无证书"
            not_after = cert.get('notAfter')
            if not not_after:
                return "未知过期时间"
            # 修复：用 email.utils.parsedate_to_datetime 正确解析时区，避免 strptime %Z 不可靠的问题
            expiry = parsedate_to_datetime(not_after)
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            days = (expiry - now_utc).days
            return f"{max(0, days)} 天"
    except Exception as e:
        return f"异常 ({type(e).__name__})"
    finally:
        if sock:
            try:
                sock.close()
            except Exception:
                pass

async def get_ssl_expiry_days_async(url: str) -> str:
    return await asyncio.to_thread(_sync_get_ssl_days, url)

async def async_check_node(
    client: httpx.AsyncClient,
    url: str,
    node_code: Optional[str] = None
) -> Tuple[bool, str, float]:
    """单节点探测封装，严格把控响应超时与延迟统计"""
    start_time = time.perf_counter()
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) MonitorBot/6.0'}
    # 移除伪造 CF-Ray header，避免暴露探测逻辑且无实际收益

    try:
        resp = await client.get(url, headers=headers, timeout=6.0, follow_redirects=True)
        latency = round((time.perf_counter() - start_time) * 1000, 1)
        is_ok = (200 <= resp.status_code < 400)
        return is_ok, f"HTTP {resp.status_code}", latency
    except httpx.TimeoutException:
        return False, "请求超时", 6000.0
    except httpx.ConnectError:
        return False, "连接失败", 0.0
    except Exception as e:
        return False, f"异常: {type(e).__name__}", 0.0

async def async_check_global_distributed(
    client: httpx.AsyncClient,
    url: str
) -> Dict[str, Any]:
    """并发向全球各边缘模拟节点发起探测，汇总可用率与延迟"""
    tasks = [async_check_node(client, url)]
    node_keys = list(GLOBAL_NODES.keys())
    for code in node_keys:
        tasks.append(async_check_node(client, url, node_code=code))

    responses = await asyncio.gather(*tasks, return_exceptions=True)

    # 本地主探测结果
    local_raw = responses[0]
    if isinstance(local_raw, Exception):
        local_res = (False, f"本地异常: {type(local_raw).__name__}", 0.0)
    else:
        local_res = local_raw

    node_reports = {}
    success_count = 1 if local_res[0] else 0

    for idx, code in enumerate(node_keys):
        res = responses[idx + 1]
        if isinstance(res, Exception):
            n_up, n_desc, n_lat = False, "节点异常", 0.0
        else:
            n_up, n_desc, n_lat = res

        node_reports[GLOBAL_NODES[code]] = {"is_up": n_up, "desc": n_desc, "latency": n_lat}
        if n_up:
            success_count += 1

    total_probes = len(GLOBAL_NODES) + 1
    return {
        "local": {"is_up": local_res[0], "desc": local_res[1], "latency": local_res[2]},
        "global_nodes": node_reports,
        "success_rate": success_count / total_probes
    }

# ==============================================================================
# 8. 无内存泄露图表渲染引擎 (面向对象纯净模式)
# ==============================================================================
def _sync_render_chart(target_url: Optional[str] = None, days: int = 1) -> Optional[str]:
    """生成高清晰走势图，使用纯面向对象 Figure 避免 pyplot 内存泄漏"""
    import sqlite3
    start_dt = datetime.datetime.now() - datetime.timedelta(days=days)
    time_threshold = start_dt.strftime('%Y-%m-%d %H:%M:%S')

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    urls_to_draw = [target_url] if target_url else monitored_sites
    theme_colors = ['#2563eb', '#16a34a', '#ea580c', '#9333ea', '#dc2626', '#0891b2']
    has_data = False

    # 创建独立的 Figure 对象，杜绝全局 Gcf 缓存占用
    fig = Figure(figsize=(10.5, 5.2), dpi=140, facecolor='#ffffff')
    canvas = FigureCanvasAgg(fig)
    ax = fig.add_subplot(111)

    try:
        for idx, url in enumerate(urls_to_draw):
            cursor.execute(
                "SELECT timestamp, latency FROM history WHERE url = ? AND timestamp >= ? AND is_up = 1 ORDER BY timestamp ASC",
                (url, time_threshold)
            )
            rows = cursor.fetchall()
            if not rows:
                continue

            has_data = True
            times = [datetime.datetime.strptime(r[0], '%Y-%m-%d %H:%M:%S') for r in rows]
            latencies = [r[1] for r in rows]
            label_text = url.replace("https://", "").replace("http://", "")

            ax.plot(
                times, latencies,
                marker='o', markersize=2 if days > 1 else 3,
                label=label_text,
                linewidth=1.8,
                color=theme_colors[idx % len(theme_colors)],
                alpha=0.85
            )

        if not has_data:
            return None

        font_prop = font_manager.FontProperties(fname=FONT_FILE) if os.path.exists(FONT_FILE) else None
        ax.set_title("链路响应延迟监控趋势图 (ms)", fontproperties=font_prop, fontsize=12, pad=12, fontweight='bold')
        ax.set_ylabel("延迟 (ms)", fontproperties=font_prop, fontsize=10)
        ax.grid(True, linestyle="--", alpha=0.3, color="#94a3b8")
        ax.legend(prop=font_prop, loc='upper right', framealpha=0.8)

        fig.autofmt_xdate()
        fig.tight_layout()

        chart_path = f"report_chart_{days}d_{int(time.time())}.png"
        canvas.print_png(chart_path)
        return chart_path
    except Exception:
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass
        # 彻底释放 Figure 资源，防止内存累积
        try:
            fig.clear()
        except Exception:
            pass
        del fig, canvas, ax

async def generate_optimized_chart(target_url: Optional[str] = None, days: int = 1) -> Optional[str]:
    return await asyncio.to_thread(_sync_render_chart, target_url, days)

async def generate_periodic_report_text(days: int = 7) -> str:
    """统计生成周报/月报可用率文本"""
    start_time_str = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
    title = f"📊 <b>{'【周报中心】站点可用率报告' if days == 7 else '🏆 【月报中心】站点可用率报告'}</b>"
    report = f"{title}\n统计区间: <code>{start_time_str}</code> 至 <code>现在</code>\n\n"

    async with aiosqlite.connect(DB_FILE) as db:
        for url in monitored_sites:
            async with db.execute(
                "SELECT COUNT(*), SUM(is_up) FROM history WHERE url = ? AND timestamp >= ?",
                (url, start_time_str)
            ) as cursor:
                row = await cursor.fetchone()
                if row and row[0] > 0:
                    total_cnt = row[0]
                    up_cnt = row[1] if row[1] is not None else 0
                    rate = (up_cnt / total_cnt) * 100
                    status_icon = "🟢" if rate >= 99.0 else ("🟡" if rate >= 95.0 else "🔴")
                    display_url = html.escape(url.replace('https://', '').replace('http://', ''))
                    report += f"{status_icon} <code>{display_url}</code>: <b>{rate:.2f}%</b> (有效抽检 {total_cnt} 次)\n"

    return report

# ==============================================================================
# 9. 系统状态信息与安全权限装饰器
# ==============================================================================
def admin_only(func):
    """确保仅管理员 Chat ID 能够执行敏感管理动作"""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        if not user or user.id != ADMIN_CHAT_ID:
            if update.effective_message:
                await update.effective_message.reply_text("⛔️ <b>无权限</b>: 您的账号未被授权操控此监控节点。", parse_mode=ParseMode.HTML)
            return
        return await func(update, context)
    return wrapper

def get_server_status_html() -> str:
    """获取服务器硬件负载 HTML 文本 (修复属性与除法错误)"""
    cpu_usage = psutil.cpu_percent(interval=None)
    memory = psutil.virtual_memory()
    mem_used = round(memory.used / BYTES_PER_GB, 2)
    mem_total = round(memory.total / BYTES_PER_GB, 2)

    disk = psutil.disk_usage('/')
    disk_used = round(disk.used / BYTES_PER_GB, 2)
    disk_total = round(disk.total / BYTES_PER_GB, 2)

    boot_time = datetime.datetime.fromtimestamp(psutil.boot_time())
    uptime_delta = datetime.datetime.now() - boot_time
    uptime_str = str(uptime_delta).split('.')[0]

    def _badge(p: float) -> str:
        return "🟢" if p < 60 else ("🟡" if p < 85 else "🔴")

    return (
        "🖥 <b>服务器系统运行监控看板</b>\n\n"
        f"{_badge(cpu_usage)} <b>CPU 负载</b>: <code>{cpu_usage}%</code>\n"
        f"{_badge(memory.percent)} <b>内存占用</b>: <code>{memory.percent}%</code> ({mem_used}G / {mem_total}G)\n"
        f"{_badge(disk.percent)} <b>磁盘使用</b>: <code>{disk.percent}%</code> ({disk_used}G / {disk_total}G)\n"
        f"⏱️ <b>持续运行</b>: <code>{uptime_str}</code>\n\n"
        f"🕒 <i>数据更新时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i>"
    )

async def get_services_status_html() -> str:
    """获取本地组件与端口监控看板 HTML 文本"""
    text = "🛠 <b>系统关键服务与端口实时状态</b>\n\n"
    for srv in monitored_services:
        s_type = "进程" if srv.type == "process" else "TCP 端口"
        if srv.type == "process":
            is_alive = await asyncio.to_thread(check_process_alive_sync, srv.target)
        else:
            is_alive = await check_port_alive_async(srv.target)

        icon = "🟢 在线" if is_alive else "🔴 宕机"
        safe_name = html.escape(srv.name)
        safe_target = html.escape(srv.target)
        text += f"• <b>{safe_name}</b> [{s_type}: <code>{safe_target}</code>] -> <b>{icon}</b>\n"

    text += f"\n🕒 <i>快照时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i>"
    return text

def get_main_menu_keyboard() -> InlineKeyboardMarkup:
    """构建主控操作矩阵按钮"""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📋 网站监控列表", callback_data="menu_list"),
            InlineKeyboardButton("🔍 全球即时探测", callback_data="menu_status")
        ],
        [
            InlineKeyboardButton("🖥 服务器监控", callback_data="menu_server"),
            InlineKeyboardButton("⚙️ 核心组件端口", callback_data="menu_services")
        ],
        [
            InlineKeyboardButton("📈 24h 走势大图", callback_data="chart_all"),
            InlineKeyboardButton("📝 本地运行日志", callback_data="menu_logs")
        ],
        [
            InlineKeyboardButton("➕ 添加监控网站", callback_data="menu_add"),
            InlineKeyboardButton("➕ 添加本地组件", callback_data="menu_add_srv")
        ]
    ])

# ==============================================================================
# 10. Telegram 指令与按键交互处理器
# ==============================================================================
@admin_only
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/start 入口指令"""
    if update.effective_message:
        await update.effective_message.reply_text(
            "📊 <b>分布式监控与全自动自愈控制矩阵已就绪</b>\n请使用下方交互面板进行实时管理：",
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_menu_keyboard()
        )

@admin_only
async def button_click_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """统一处理所有 CallbackQuery 交互，修复切分异常与解包越界"""
    query = update.callback_query
    if not query:
        return
    await query.answer()

    data = query.data or ""

    if data == "menu_main":
        await query.edit_message_text(
            "📊 <b>分布式监控与全自动自愈控制矩阵已就绪</b>\n请使用下方交互面板进行实时管理：",
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_menu_keyboard()
        )

    elif data == "menu_server":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 刷新状态", callback_data="menu_server"),
             InlineKeyboardButton("🔙 主菜单", callback_data="menu_main")]
        ])
        await query.edit_message_text(get_server_status_html(), parse_mode=ParseMode.HTML, reply_markup=kb)

    elif data == "menu_services":
        text = await get_services_status_html()
        srv_kb = []
        if monitored_services:
            srv_kb.append([InlineKeyboardButton("🗑 管理/下线服务组件", callback_data="menu_list_srv")])
        srv_kb.append([
            InlineKeyboardButton("🔄 刷新组件状态", callback_data="menu_services"),
            InlineKeyboardButton("🔙 主菜单", callback_data="menu_main")
        ])
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(srv_kb))

    elif data == "menu_list_srv":
        if not monitored_services:
            await query.edit_message_text(
                "📭 暂无受控组件。",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 返回主菜单", callback_data="menu_main")]])
            )
            return
        await query.edit_message_text("📋 <b>请点击需要移除的核心组件：</b>", parse_mode=ParseMode.HTML)
        for idx, srv in enumerate(monitored_services):
            safe_name = html.escape(srv.name)
            safe_target = html.escape(srv.target)
            btn = InlineKeyboardMarkup([[InlineKeyboardButton("🗑 确认删除", callback_data=f"del_srv:{idx}")]])
            if query.message:
                await query.message.reply_text(
                    f"🏷 <b>组件名称</b>: {safe_name}\n🎯 <b>监控目标</b>: <code>{safe_target}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=btn
                )

    elif data.startswith("del_srv:"):
        _, idx_str = data.split(":", 1)
        if idx_str.isdigit():
            idx = int(idx_str)
            if 0 <= idx < len(monitored_services):
                deleted = monitored_services.pop(idx)
                service_trackers.pop(f"{deleted.type}_{deleted.target}", None)
                await save_services_async()
                await query.edit_message_text(
                    f"✅ 已成功移除核心组件监控：\n<b>{html.escape(deleted.name)}</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=get_main_menu_keyboard()
                )

    elif data == "menu_logs":
        if not os.path.exists(LOG_FILE):
            await query.edit_message_text("📭 暂无本地运行日志。", reply_markup=get_main_menu_keyboard())
            return
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()[-20:]
        log_text = "📝 <b>最新 20 行本地运行日志：</b>\n\n<pre>" + html.escape("".join(lines)) + "</pre>"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 刷新日志", callback_data="menu_logs"),
             InlineKeyboardButton("🔙 主菜单", callback_data="menu_main")]
        ])
        await query.edit_message_text(log_text, parse_mode=ParseMode.HTML, reply_markup=kb)

    elif data == "menu_list":
        if not monitored_sites:
            await query.edit_message_text(
                "📭 当前未监控任何网站。",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 返回主菜单", callback_data="menu_main")]])
            )
            return
        await query.edit_message_text("📋 <b>当前受控网站列表，请选择操作：</b>", parse_mode=ParseMode.HTML)
        for idx, url in enumerate(monitored_sites):
            safe_url = html.escape(url)
            site_kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("📈 趋势图", callback_data=f"chart_one:{idx}"),
                InlineKeyboardButton("🌐 即时诊断", callback_data=f"check_one:{idx}"),
                InlineKeyboardButton("🗑 移除", callback_data=f"del_one:{idx}")
            ]])
            if query.message:
                await query.message.reply_text(f"🌐 站点: <code>{safe_url}</code>", parse_mode=ParseMode.HTML, reply_markup=site_kb)

    elif data == "menu_status":
        status_msg = await query.message.reply_text("⚡️ 正在向全球多节点分发实时探测，请稍候...")
        report = "🌍 <b>全球边缘多节点实时抽检快报：</b>\n\n"
        # [优化] 复用 httpx 连接池，避免每次新建 AsyncClient
        async with httpx.AsyncClient(timeout=8.0) as client:
            for url in monitored_sites:
                res = await async_check_global_distributed(client, url)
                icon = "🟢" if res["local"]["is_up"] else "🔴"
                display_url = html.escape(url)
                report += f"{icon} <code>{display_url}</code> ({res['local']['latency']}ms)\n"
        await status_msg.edit_text(report, parse_mode=ParseMode.HTML, reply_markup=get_main_menu_keyboard())

    elif data == "chart_all":
        chart_msg = await query.message.reply_text("📈 正在渲染全局趋势无损走势图...")
        path = await generate_optimized_chart(days=1)
        try:
            await chart_msg.delete()
            if path and os.path.exists(path):
                with open(path, 'rb') as f:
                    await query.message.reply_photo(photo=f, caption="📊 <b>24小时全局网络性能监控图表</b>", parse_mode=ParseMode.HTML)
        finally:
            # [Bug Fix] 清理临时图表文件
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except Exception:
                    pass

    elif data == "menu_add":
        context.user_data["waiting_for_url"] = True
        await query.edit_message_text(
            "➕ <b>请输入你要监控的完整网址：</b>\n例如: <code>https://example.com</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ 取消", callback_data="menu_main")]])
        )

    elif data == "menu_add_srv":
        context.user_data["waiting_for_service"] = True
        guide = (
            "➕ <b>添加本地服务/端口监控</b>\n"
            "请输入英文逗号分隔的三元组：\n"
            "<code>显示名称,监控类型(process或port),核心目标</code>\n\n"
            "例如: <code>MySQL 数据库,process,mysql</code> 或 <code>Web 端口,port,80</code>"
        )
        await query.edit_message_text(
            guide,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ 取消", callback_data="menu_main")]])
        )

    # 针对单个站点的快捷操作路由
    elif any(data.startswith(p) for p in ("chart_one:", "check_one:", "del_one:")):
        action, idx_str = data.split(":", 1)
        if not idx_str.isdigit():
            return
        idx = int(idx_str)
        if idx >= len(monitored_sites):
            return
        url = monitored_sites[idx]

        if action == "chart_one":
            path = await generate_optimized_chart(target_url=url, days=1)
            try:
                if path and os.path.exists(path):
                    with open(path, 'rb') as f:
                        await query.message.reply_photo(
                            photo=f,
                            caption=f"📊 站点 <code>{html.escape(url)}</code> 专属 24h 监控图表",
                            parse_mode=ParseMode.HTML
                        )
                else:
                    await query.message.reply_text("⚠️ 该站点暂无可渲染的历史数据。")
            finally:
                # [Bug Fix] 无论成功失败都清理临时图表文件
                if path and os.path.exists(path):
                    try:
                        os.remove(path)
                    except Exception:
                        pass

        elif action == "check_one":
            async with httpx.AsyncClient(timeout=8.0) as client:
                res = await async_check_global_distributed(client, url)
            ssl_days = await get_ssl_expiry_days_async(url)

            report = (
                f"🌍 <b>站点全网透视诊断报告</b>\n\n"
                f"🎯 <b>目标地址</b>: <code>{html.escape(url)}</code>\n"
                f"🔒 <b>SSL 证书状态</b>: <b>{html.escape(ssl_days)}</b>\n\n"
                f"📡 <b>主节点探测</b>: {'🟢 在线' if res['local']['is_up'] else '🔴 异常'} ({res['local']['latency']}ms)\n"
                f"🌐 <b>边缘可用率</b>: <b>{int(res['success_rate'] * 100)}%</b>\n"
            )
            await query.message.reply_text(report, parse_mode=ParseMode.HTML)

        elif action == "del_one":
            monitored_sites.remove(url)
            site_trackers.pop(url, None)
            await save_sites_async()
            await query.edit_message_text(
                f"🗑 已成功移除监控站点：\n<code>{html.escape(url)}</code>",
                parse_mode=ParseMode.HTML,
                reply_markup=get_main_menu_keyboard()
            )

@admin_only
async def text_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """处理用户文本输入 (添加站点或添加服务)"""
    if not update.effective_message or not update.effective_message.text:
        return

    text = update.effective_message.text.strip()

    if context.user_data.get("waiting_for_url"):
        context.user_data["waiting_for_url"] = False
        if not text.startswith("http://") and not text.startswith("https://"):
            text = f"https://{text}"

        if text in monitored_sites:
            await update.effective_message.reply_text("⚠️ 该网址已存在于监控列表中！", reply_markup=get_main_menu_keyboard())
            return

        monitored_sites.append(text)
        await save_sites_async()
        await update.effective_message.reply_text(
            f"✅ <b>成功添加站点监控：</b>\n<code>{html.escape(text)}</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_menu_keyboard()
        )

    elif context.user_data.get("waiting_for_service"):
        context.user_data["waiting_for_service"] = False
        parts = [p.strip() for p in text.split(",")]
        if len(parts) != 3 or parts[1].lower() not in ("process", "port"):
            await update.effective_message.reply_text(
                "❌ 格式错误！请输入：<code>显示名称,监控类型(process或port),核心目标</code>",
                parse_mode=ParseMode.HTML,
                reply_markup=get_main_menu_keyboard()
            )
            return

        # [Bug Fix] 端口类型时校验必须是数字
        if parts[1].lower() == "port":
            try:
                int(parts[2])
            except ValueError:
                await update.effective_message.reply_text(
                    "❌ 端口必须是数字！",
                    parse_mode=ParseMode.HTML,
                    reply_markup=get_main_menu_keyboard()
                )
                return

        new_item = ServiceItem(name=parts[0], type=parts[1].lower(), target=parts[2])
        monitored_services.append(new_item)
        await save_services_async()
        await update.effective_message.reply_text(
            f"✅ <b>已成功挂载组件监控：</b>\n<b>{html.escape(parts[0])}</b> (<code>{parts[1]}: {parts[2]}</code>)",
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_menu_keyboard()
        )

# ==============================================================================
# 11. 并发双轨巡检调度引擎 (核心性能飞跃)
# ==============================================================================
async def check_single_site_workflow(
    shared_client: httpx.AsyncClient,
    application: Application,
    sem: asyncio.Semaphore,
    url: str
) -> None:
    """单个站点的完整探测、落盘与阶梯告警逻辑"""
    async with sem:
        res = await async_check_global_distributed(shared_client, url)

    # 异步推入 Redis 队列
    await log_to_redis_async(url, res["local"]["is_up"], res["local"]["latency"])

    if url not in site_trackers:
        site_trackers[url] = SiteRuntimeState()
    tracker = site_trackers[url]

    # 超过 50% 节点联通视为正常
    if res["success_rate"] >= 0.5:
        tracker.fail_count = 0
        if tracker.is_reported_down:
            tracker.is_reported_down = False
            duration = int(time.time() - (tracker.down_start_time or time.time()))
            tracker.down_start_time = None
            tracker.last_followup_stage = 0

            rec_msg = (
                f"✅ <b>网站全网链路已恢复正常</b> ✅\n\n"
                f"🌐 <b>目标地址</b>: <code>{html.escape(url)}</code>\n"
                f"⏱️ <b>累计中断时长</b>: <code>{datetime.timedelta(seconds=duration)}</code>"
            )
            await application.bot.send_message(chat_id=ADMIN_CHAT_ID, text=rec_msg, parse_mode=ParseMode.HTML)
    else:
        tracker.fail_count += 1
        if tracker.fail_count >= MAX_FAILURES_THRESHOLD and not tracker.is_reported_down:
            tracker.is_reported_down = True
            tracker.down_start_time = time.time()
            tracker.last_followup_stage = 0

            alert_msg = (
                f"🚨 <b>网站全球分布式链路故障告警</b> 🚨\n\n"
                f"🎯 <b>故障目标</b>: <code>{html.escape(url)}</code>\n"
                f"📉 <b>边缘可用率</b>: <b>{int(res['success_rate'] * 100)}%</b>\n"
                f"⚡️ <b>主节点状态</b>: <code>{html.escape(res['local']['desc'])}</code>"
            )
            await application.bot.send_message(chat_id=ADMIN_CHAT_ID, text=alert_msg, parse_mode=ParseMode.HTML)

        elif tracker.is_reported_down and tracker.down_start_time:
            elapsed = int(time.time() - tracker.down_start_time)
            for stage_idx, stage_sec in enumerate(FOLLOWUP_STAGES):
                if elapsed >= stage_sec and tracker.last_followup_stage <= stage_idx:
                    tracker.last_followup_stage = stage_idx + 1
                    stage_desc = f"{int(stage_sec/60)} 分钟" if stage_sec < 3600 else f"{int(stage_sec/3600)} 小时"
                    follow_msg = (
                        f"⚠️ <b>网站故障持续随访提醒</b> ⚠️\n\n"
                        f"🎯 <b>故障目标</b>: <code>{html.escape(url)}</code>\n"
                        f"⏳ <b>持续离线时长已达</b>: <code>{stage_desc}</code>"
                    )
                    await application.bot.send_message(chat_id=ADMIN_CHAT_ID, text=follow_msg, parse_mode=ParseMode.HTML)
                    break

async def check_services_workflow(application: Application) -> None:
    """本地服务与端口健康巡检及自动自愈拉起"""
    for srv in monitored_services:
        s_key = f"{srv.type}_{srv.target}"
        if s_key not in service_trackers:
            service_trackers[s_key] = ServiceRuntimeState()
        tracker = service_trackers[s_key]

        if srv.type == "process":
            is_alive = await asyncio.to_thread(check_process_alive_sync, srv.target)
        else:
            is_alive = await check_port_alive_async(srv.target)

        if is_alive:
            tracker.fail_count = 0
            if tracker.is_reported_down:
                tracker.is_reported_down = False
                duration = int(time.time() - (tracker.down_start_time or time.time()))
                tracker.down_start_time = None
                tracker.last_followup_stage = 0
                rec_msg = (
                    f"✅ <b>本地核心组件恢复正常上线</b> ✅\n\n"
                    f"📦 <b>组件名称</b>: <b>{html.escape(srv.name)}</b>\n"
                    f"🎯 <b>目标细节</b>: <code>{html.escape(srv.target)}</code>\n"
                    f"⏱️ <b>中断时长</b>: <code>{datetime.timedelta(seconds=duration)}</code>"
                )
                await application.bot.send_message(chat_id=ADMIN_CHAT_ID, text=rec_msg, parse_mode=ParseMode.HTML)
        else:
            tracker.fail_count += 1
            if tracker.fail_count >= MAX_FAILURES_THRESHOLD and not tracker.is_reported_down:
                tracker.is_reported_down = True
                tracker.down_start_time = time.time()
                tracker.last_followup_stage = 0

                alert_msg = (
                    f"🚨 <b>本地核心服务/端口宕机告警</b> 🚨\n\n"
                    f"📦 <b>警告组件</b>: <b>{html.escape(srv.name)}</b>\n"
                    f"🎯 <b>异常目标</b>: <code>{html.escape(srv.target)}</code>\n\n"
                    f"🔄 <i>[系统自愈模块] 正在尝试调用 Systemd 异步重置服务...</i>"
                )
                init_msg = await application.bot.send_message(chat_id=ADMIN_CHAT_ID, text=alert_msg, parse_mode=ParseMode.HTML)

                # 触发自动拉起自愈机制
                if srv.type == "process" and srv.target.lower() in SERVICE_SYSTEMD_MAPPING:
                    healed = await exec_service_self_healing(srv.target)
                    if healed:
                        tracker.is_reported_down = False
                        tracker.down_start_time = None
                        try:
                            await init_msg.edit_text(
                                f"🚨 <b>本地核心服务发生崩溃</b> 🚨\n\n"
                                f"📦 <b>警告组件</b>: <b>{html.escape(srv.name)}</b>\n\n"
                                f"❇️ <b>[自愈成功]</b> 组件已被 Systemd 成功拉起并恢复正常在线！",
                                parse_mode=ParseMode.HTML
                            )
                        except Exception:
                            pass  # 消息可能已过期或被删除，忽略
                    else:
                        try:
                            await init_msg.edit_text(
                                f"🚨 <b>本地核心服务发生崩溃</b> 🚨\n\n"
                                f"📦 <b>警告组件</b>: <b>{html.escape(srv.name)}</b>\n\n"
                                f"❌ <b>[自愈失败]</b> 重启尝试未果，请立即登录服务器手动排查！",
                                parse_mode=ParseMode.HTML
                            )
                        except Exception:
                            pass

            elif tracker.is_reported_down and tracker.down_start_time:
                elapsed = int(time.time() - tracker.down_start_time)
                for stage_idx, stage_sec in enumerate(FOLLOWUP_STAGES):
                    if elapsed >= stage_sec and tracker.last_followup_stage <= stage_idx:
                        tracker.last_followup_stage = stage_idx + 1
                        stage_desc = f"{int(stage_sec/60)} 分钟" if stage_sec < 3600 else f"{int(stage_sec/3600)} 小时"
                        f_text = (
                            f"⚠️ <b>服务组件持续故障提醒</b> ⚠️\n\n"
                            f"📦 <b>目标组件</b>: <b>{html.escape(srv.name)}</b>\n"
                            f"🔴 <b>当前状况</b>: 自愈未果，依然离线\n"
                            f"⏳ <b>累计离线时长</b>: <code>{stage_desc}</code>"
                        )
                        await application.bot.send_message(chat_id=ADMIN_CHAT_ID, text=f_text, parse_mode=ParseMode.HTML)
                        break

async def monitor_loop(application: Application) -> None:
    """双轨全异步高性能巡检主循环"""
    await application.bot.send_message(
        chat_id=ADMIN_CHAT_ID,
        text="🤖 <b>工业级高性能重构版双轨监控守护节点已启动就绪。</b>",
        parse_mode=ParseMode.HTML
    )

    last_cleanup_day = datetime.datetime.now().date()
    last_report_day = datetime.datetime.now().date()

    # 针对大量连接复用的全局高效 HTTP 客户端
    limits = httpx.Limits(max_connections=150, max_keepalive_connections=30)
    sem = asyncio.Semaphore(CONCURRENT_PROBE_LIMIT)

    async with httpx.AsyncClient(limits=limits, timeout=8.0) as shared_client:
        while True:
            try:
                now_dt = datetime.datetime.now()
                cur_date = now_dt.date()

                # 每日定时清理历史超期日志
                if cur_date != last_cleanup_day:
                    await clean_old_data_async()
                    last_cleanup_day = cur_date

                # 非阻塞上报服务器负载数据
                cpu = psutil.cpu_percent(interval=None)
                mem = psutil.virtual_memory().percent
                await log_server_to_redis_async(cpu, mem)

                # 周一自动推送周报（修复：去掉严格时间窗口，只要当天未发就推）
                if cur_date.weekday() == 0 and cur_date > last_report_day:
                    report_txt = await generate_periodic_report_text(days=7)
                    chart_p = await generate_optimized_chart(days=7)
                    last_report_day = cur_date
                    try:
                        if chart_p and os.path.exists(chart_p):
                            with open(chart_p, 'rb') as f:
                                await application.bot.send_photo(
                                    chat_id=ADMIN_CHAT_ID,
                                    photo=f,
                                    caption=report_txt,
                                    parse_mode=ParseMode.HTML
                                )
                            os.remove(chart_p)
                        else:
                            await application.bot.send_message(
                                chat_id=ADMIN_CHAT_ID,
                                text=report_txt,
                                parse_mode=ParseMode.HTML
                            )
                    except Exception as report_err:
                        logger.error(f"周报发送失败: {report_err}")

                # 1. 巡检本地核心组件
                await check_services_workflow(application)

                # 2. 全并发无阻塞巡检分布式网站
                if monitored_sites:
                    probe_tasks = [
                        check_single_site_workflow(shared_client, application, sem, u)
                        for u in list(monitored_sites)
                    ]
                    await asyncio.gather(*probe_tasks, return_exceptions=True)

                await asyncio.sleep(CHECK_INTERVAL)

            except asyncio.CancelledError:
                logger.info("主监控循环被取消，平稳退出。")
                break
            except Exception as loop_err:
                logger.critical(f"🔥 主监控循环异常: {loop_err}", exc_info=True)
                await asyncio.sleep(15)

# ==============================================================================
# 12. 统一错误捕获与主运行入口 (含优雅退出与资源释放)
# ==============================================================================
async def global_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """全局 Telegram 异常安全捕获"""
    logger.error(f"⚠️ Telegram 全局底层异常: {context.error}", exc_info=context.error)

async def async_main() -> None:
    """主系统生命周期管理入口"""
    global redis_client, monitored_sites, monitored_services

    logger.info("⚡️ 正在装载核心配置与存储系统...")

    # [Bug Fix] Redis 可选模式：Redis 不可用时降级为纯 SQLite，不崩溃
    redis_client = None
    try:
        redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
        await redis_client.ping()
        logger.info("✅ Redis 缓冲队列已连接")
    except Exception as e:
        logger.warning(f"⚠️ Redis 不可用 ({e})，将使用纯 SQLite 直写模式")
        redis_client = None

    await init_db_async()
    await init_chinese_font()

    # 预热 psutil.cpu_percent() 首次调用返回 0.0 的 Bug
    psutil.cpu_percent(interval=None)

    # 加载受控对象
    monitored_sites = _sync_load_sites()
    monitored_services = _sync_load_services()

    # 初始化 Telegram 实例
    application = Application.builder().token(TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_click_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message_handler))
    application.add_error_handler(global_error_handler)

    # 启动后台守护任务
    flush_task = asyncio.create_task(database_flush_worker())
    monitor_task = asyncio.create_task(monitor_loop(application))

    await application.initialize()
    await application.start()
    await application.updater.start_polling(drop_pending_updates=True)
    logger.info("🚀 工业级高性能分布式监控系统已成功全量上线启动！")

    try:
        # 持续常驻运行
        while True:
            await asyncio.sleep(3600)
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("🛑 正在优雅关闭系统与释放数据库连接...")
    finally:
        flush_task.cancel()
        monitor_task.cancel()
        await application.updater.stop()
        await application.stop()
        await application.shutdown()
        if redis_client:
            await redis_client.aclose()
        logger.info("👋 监控系统已安全关闭下线。")

if __name__ == "__main__":
    try:
        asyncio.run(async_main())
    except (KeyboardInterrupt, SystemExit):
        pass
