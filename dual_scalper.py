import asyncio
import json
import logging
import math
import time
import os
import sys
from collections import deque
from decimal import Decimal
from enum import Enum
from typing import Optional, Dict, Any, List

from config import (
    COIN_PRESETS, DEFAULT_COIN,
    MARKET, ORDER_SIZE, MIN_ORDER_SIZE, SIZE_DECIMALS,
    MAX_CYCLES, PARADEX_ENV,
    MAX_CONSECUTIVE_FAILURES, EMERGENCY_STOP_FILE,
    ACCOUNT_GROUPS, RATE_LIMITS_FILE,
    ZERO_SPREAD_THRESHOLD, ENTRY_ZERO_SPREAD_MS, DEPTH_SAFETY_FACTOR,
    MAX_HOLD_SECONDS,
    BURST_ZERO_SPREAD_MS, BURST_MIN_DEPTH,
    MAX_ROUNDS_PER_BURST,
    TG_BOT_TOKEN, TG_CHAT_ID, TG_NOTIFY_INTERVAL, TG_ENABLED,
    BBO_RECORD_ENABLED, BBO_RECORD_DIR, BBO_RECORD_BUFFER_SIZE,
)

# Runtime overrides (set by select_coin → apply_coin_preset)
COIN_SYMBOL = DEFAULT_COIN

from paradex_py import ParadexSubkey
from paradex_py.api.ws_client import ParadexWebsocketChannel
from paradex_py.common.order import Order, OrderType, OrderSide


# ─── Logging ───
LOG_FILE = "dual_scalper.log"

file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)
console_handler.setFormatter(logging.Formatter('%(message)s'))

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

logging.getLogger('websockets').setLevel(logging.WARNING)
logging.getLogger('paradex_py').setLevel(logging.WARNING)


# ─── Rate limits (Retail profile) ───
MAX_ORDERS_PER_MINUTE = 30
MAX_ORDERS_PER_HALF_HOUR = 300
MAX_ORDERS_PER_DAY = 1000


# ─── Rate Persistence ───
class RatePersistence:
    """Persists per-account order timestamps to JSON file. Survives restarts."""

    def __init__(self, filepath: str = RATE_LIMITS_FILE):
        self.filepath = filepath
        self._data: Dict[str, List[float]] = self._load()

    def _load(self) -> Dict[str, List[float]]:
        """Load from disk, clean expired entries (>24h)."""
        if not os.path.exists(self.filepath):
            return {}
        try:
            with open(self.filepath, "r") as f:
                raw = json.load(f)
            now = time.time()
            cleaned = {}
            for addr, timestamps in raw.items():
                valid = [t for t in timestamps if now - t < 86400]
                if valid:
                    cleaned[addr] = valid
            return cleaned
        except Exception as e:
            logger.warning(f"速率文件加载失败, 将重新创建: {e}")
            return {}

    def _save(self):
        """Atomic write: write to .tmp then rename."""
        tmp = self.filepath + ".tmp"
        try:
            with open(tmp, "w") as f:
                json.dump(self._data, f)
            os.replace(tmp, self.filepath)
        except Exception as e:
            logger.error(f"速率文件保存失败: {e}")

    def get_orders(self, l2_address: str) -> List[float]:
        """Get valid (non-expired) timestamps for an account."""
        now = time.time()
        timestamps = self._data.get(l2_address, [])
        valid = [t for t in timestamps if now - t < 86400]
        self._data[l2_address] = valid
        return valid

    def record(self, l2_address: str, timestamp: float):
        """Append a new order timestamp and persist to disk."""
        if l2_address not in self._data:
            self._data[l2_address] = []
        self._data[l2_address].append(timestamp)
        self._save()

    def can_trade(self, l2_address: str) -> tuple[bool, float, str]:
        """Check if an account can trade based on persisted history."""
        now = time.time()
        timestamps = self.get_orders(l2_address)

        minute_orders = [t for t in timestamps if now - t < 60]
        half_hour_orders = [t for t in timestamps if now - t < 1800]
        day_orders = timestamps  # already cleaned to <24h

        if len(minute_orders) >= MAX_ORDERS_PER_MINUTE:
            return False, 60 - (now - minute_orders[0]), "分钟"
        if len(half_hour_orders) >= MAX_ORDERS_PER_HALF_HOUR:
            return False, 1800 - (now - half_hour_orders[0]), "30分钟"
        if len(day_orders) >= MAX_ORDERS_PER_DAY:
            return False, 86400 - (now - day_orders[0]), "24h"
        return True, 0, ""

    def get_counts(self, l2_address: str) -> tuple[int, int, int]:
        """Returns (minute_count, half_hour_count, day_count) for display."""
        now = time.time()
        timestamps = self.get_orders(l2_address)
        minute = sum(1 for t in timestamps if now - t < 60)
        half_hour = sum(1 for t in timestamps if now - t < 1800)
        day = len(timestamps)
        return minute, half_hour, day

    def earliest_unlock(self, l2_address: str) -> float:
        """Returns seconds until the earliest rate limit unlocks for this account."""
        now = time.time()
        timestamps = self.get_orders(l2_address)

        waits = []
        minute_orders = [t for t in timestamps if now - t < 60]
        half_hour_orders = [t for t in timestamps if now - t < 1800]

        if len(minute_orders) >= MAX_ORDERS_PER_MINUTE:
            waits.append(60 - (now - minute_orders[0]))
        if len(half_hour_orders) >= MAX_ORDERS_PER_HALF_HOUR:
            waits.append(1800 - (now - half_hour_orders[0]))
        if len(timestamps) >= MAX_ORDERS_PER_DAY:
            waits.append(86400 - (now - timestamps[0]))

        return min(waits) if waits else 0


# ─── State ───
class StrategyState(Enum):
    IDLE = "IDLE"
    HOLDING = "HOLDING"


# ─── Rate Limiter ───
class RateLimiter:
    """Sliding-window rate limiter (minute / 30min / day) with persistence."""

    def __init__(self, per_minute: int, per_half_hour: int, per_day: int,
                 l2_address: str = "", persistence: Optional[RatePersistence] = None):
        self.per_minute = per_minute
        self.per_half_hour = per_half_hour
        self.per_day = per_day
        self.l2_address = l2_address
        self.persistence = persistence
        self.minute_orders: deque = deque()
        self.half_hour_orders: deque = deque()
        self.day_orders: deque = deque()

        # Restore history from persistence file on startup
        if persistence and l2_address:
            self._restore_from_persistence()

    def _restore_from_persistence(self):
        """Load historical timestamps from persistence into deques."""
        timestamps = self.persistence.get_orders(self.l2_address)
        now = time.time()
        for t in timestamps:
            age = now - t
            if age < 86400:
                self.day_orders.append(t)
            if age < 1800:
                self.half_hour_orders.append(t)
            if age < 60:
                self.minute_orders.append(t)
        if timestamps:
            logger.info(f"[{self.l2_address[:10]}...] 恢复历史下单记录: "
                        f"{len(self.minute_orders)}m/{len(self.half_hour_orders)}h/{len(self.day_orders)}d")

    def can_place_order(self) -> tuple[bool, float, str]:
        now = time.time()
        while self.minute_orders and now - self.minute_orders[0] > 60:
            self.minute_orders.popleft()
        while self.half_hour_orders and now - self.half_hour_orders[0] > 1800:
            self.half_hour_orders.popleft()
        while self.day_orders and now - self.day_orders[0] > 86400:
            self.day_orders.popleft()

        if len(self.minute_orders) >= self.per_minute:
            return False, 60 - (now - self.minute_orders[0]), "分钟"
        if len(self.half_hour_orders) >= self.per_half_hour:
            return False, 1800 - (now - self.half_hour_orders[0]), "30分钟"
        if len(self.day_orders) >= self.per_day:
            return False, 86400 - (now - self.day_orders[0]), "24h"
        return True, 0, ""

    def record_order(self):
        now = time.time()
        self.minute_orders.append(now)
        self.half_hour_orders.append(now)
        self.day_orders.append(now)
        # Persist to disk
        if self.persistence and self.l2_address:
            self.persistence.record(self.l2_address, now)

    def get_counts(self) -> tuple[int, int, int]:
        """Returns (minute_count, half_hour_count, day_count)."""
        return len(self.minute_orders), len(self.half_hour_orders), len(self.day_orders)


# ─── Latency Tracker ───
class LatencyTracker:
    """Tracks recent cycle and WebSocket latencies."""

    def __init__(self, max_records: int = 5):
        self.recent_latencies: deque = deque(maxlen=max_records)
        self.current_ws_latency: float = 0.0

    def record_cycle_latency(self, latency_ms: float):
        self.recent_latencies.append(latency_ms)

    def update_ws_latency(self, latency_ms: float):
        self.current_ws_latency = latency_ms

    def get_stats(self) -> dict:
        if not self.recent_latencies:
            return {"recent": [], "avg": 0, "min": 0, "max": 0, "ws": self.current_ws_latency}
        latencies = list(self.recent_latencies)
        return {
            "recent": latencies,
            "avg": sum(latencies) / len(latencies),
            "min": min(latencies),
            "max": max(latencies),
            "ws": self.current_ws_latency,
        }

    def format_recent(self) -> str:
        if not self.recent_latencies:
            return "-"
        return "/".join([f"{lat:.0f}" for lat in self.recent_latencies])


# ─── Telegram Notifier ───
class TelegramNotifier:
    """Async Telegram alerts + background /stop command listener."""

    def __init__(self, bot_token: str, chat_id: str, enabled: bool = True):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.enabled = enabled and bool(bot_token) and bool(chat_id)
        self._stop_requested = False
        self._last_update_id = 0
        self._poll_task: Optional[asyncio.Task] = None
        self._poll_failures = 0
        if self.enabled:
            logger.info("Telegram 通知已启用")
            self._init_update_offset()
        else:
            logger.info("Telegram 通知未启用 (未配置 Token/ChatID 或已关闭)")

    def _init_update_offset(self):
        """Delete webhook (fixes 409) and skip stale updates on startup."""
        import urllib.request
        import json

        # Step 1: Delete any existing webhook to avoid 409 conflict
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/deleteWebhook"
            req = urllib.request.Request(url)
            urllib.request.urlopen(req, timeout=5)
            logger.info("TG webhook cleared")
        except Exception:
            pass

        # Step 2: Skip all pending updates
        try:
            url = (f"https://api.telegram.org/bot{self.bot_token}/getUpdates"
                   f"?offset=-1&limit=1&timeout=0")
            req = urllib.request.Request(url)
            resp = urllib.request.urlopen(req, timeout=5)
            data = json.loads(resp.read().decode("utf-8"))
            if data.get("ok") and data.get("result"):
                self._last_update_id = data["result"][-1]["update_id"] + 1
        except Exception as e:
            logger.debug(f"TG init offset failed (non-critical): {e}")

    def _send_sync(self, text: str):
        """Blocking send (runs in thread pool)."""
        import urllib.request
        import json

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = json.dumps({
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
        }).encode("utf-8")

        req = urllib.request.Request(
            url, data=payload,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)

    def _poll_commands_sync(self) -> list[str]:
        """Blocking poll for new commands. Short timeout to minimize blocking."""
        import urllib.request
        import json

        url = (f"https://api.telegram.org/bot{self.bot_token}/getUpdates"
               f"?offset={self._last_update_id}&limit=10&timeout=0")
        req = urllib.request.Request(url)
        resp = urllib.request.urlopen(req, timeout=3)
        data = json.loads(resp.read().decode("utf-8"))

        commands = []
        if data.get("ok"):
            for update in data.get("result", []):
                self._last_update_id = update["update_id"] + 1
                msg = update.get("message", {})
                chat_id = str(msg.get("chat", {}).get("id", ""))
                text = msg.get("text", "").strip().lower()
                if chat_id == str(self.chat_id) and text.startswith("/"):
                    commands.append(text)
        return commands

    @property
    def stop_requested(self) -> bool:
        return self._stop_requested

    def start_polling(self):
        """Start background task to poll TG commands. Non-blocking."""
        if self.enabled and self._poll_task is None:
            self._poll_task = asyncio.create_task(self._poll_loop())
            logger.info("TG 指令轮询已启动 (后台)")

    def stop_polling(self):
        """Cancel the background polling task."""
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()

    async def _poll_loop(self):
        """Background loop: polls TG every 5s, backs off on repeated failures."""
        poll_interval = 5
        max_interval = 60

        while True:
            try:
                await asyncio.sleep(poll_interval)
                commands = await asyncio.to_thread(self._poll_commands_sync)
                self._poll_failures = 0
                poll_interval = 5  # Reset on success

                for cmd in commands:
                    if cmd.startswith("/stop"):
                        self._stop_requested = True
                        logger.info("收到 Telegram /stop 指令")
                        await self.send("🛑 <b>收到 /stop 指令, 正在安全停止...</b>")
                        return
                    elif cmd == "/status":
                        await self.send("✅ 脚本运行中")

            except asyncio.CancelledError:
                return
            except Exception as e:
                self._poll_failures += 1
                # Exponential backoff: 5 → 10 → 20 → 40 → 60 (cap)
                poll_interval = min(5 * (2 ** self._poll_failures), max_interval)
                if self._poll_failures <= 3:
                    logger.debug(f"TG poll failed ({e}), retry in {poll_interval}s")
                elif self._poll_failures == 4:
                    logger.warning(f"TG poll 连续失败 {self._poll_failures} 次, "
                                   f"降频至 {poll_interval}s 轮询")

    async def send(self, text: str):
        """Async send, swallows exceptions."""
        if not self.enabled:
            return
        try:
            await asyncio.to_thread(self._send_sync, text)
        except Exception as e:
            logger.error(f"TG 发送失败: {e}")

    async def notify_startup(self, bal_a: float, bal_b: float,
                             group_name: str = "", total_groups: int = 1):
        """策略启动通知"""
        group_text = f" | 组: {group_name} ({total_groups}组)" if group_name else ""
        msg = (
            "🚀 <b>Paradex 双账户对冲套利已启动</b>\n"
            "\n"
            f"📊 市场: {MARKET} | 最大单量: {ORDER_SIZE} {COIN_SYMBOL} (动态){group_text}\n"
            f"🚦 限速: {MAX_ORDERS_PER_MINUTE}/分 | {MAX_ORDERS_PER_HALF_HOUR}/30分 | {MAX_ORDERS_PER_DAY}/日 (每账户)\n"
            f"💰 Long 余额: ${bal_a:.4f}\n"
            f"💰 Short 余额: ${bal_b:.4f}\n"
            f"💰 合计: ${bal_a + bal_b:.4f}\n"
            "\n"
            f"📡 发送 /stop 可远程停止脚本\n"
        )
        await self.send(msg)

    async def notify_progress(self, cycle: int, stats: dict,
                              account_a: 'AccountTrader', account_b: 'AccountTrader',
                              elapsed_min: float):
        """周期性进度报告"""
        pnl_a = account_a.get_pnl()
        pnl_b = account_b.get_pnl()
        _, half_a, day_a = account_a.rate_limiter.get_counts()
        _, half_b, day_b = account_b.rate_limiter.get_counts()

        pnl_emoji = "📈" if stats['pnl_total'] >= 0 else "📉"

        msg = (
            f"📊 <b>进度报告 — 第 {cycle}/{MAX_CYCLES} 轮</b>\n"
            "\n"
            f"🔄 成交笔数: {cycle * 4} 笔 (每轮4笔)\n"
            f"📈 累计交易量: ${stats['volume']:,.0f}\n"
            f"{pnl_emoji} 合计盈亏: ${stats['pnl_total']:+.4f}\n"
            f"📊 每万收益: ${stats['per_10k']:.4f}\n"
            "\n"
            f"🅰️ A: PnL ${pnl_a:+.4f} | 30m: {half_a}/{MAX_ORDERS_PER_HALF_HOUR} | 24h: {day_a}/{MAX_ORDERS_PER_DAY}\n"
            f"🅱️ B: PnL ${pnl_b:+.4f} | 30m: {half_b}/{MAX_ORDERS_PER_HALF_HOUR} | 24h: {day_b}/{MAX_ORDERS_PER_DAY}\n"
            f"⏰ 运行: {elapsed_min:.1f} 分钟\n"
        )
        await self.send(msg)

    async def notify_burst(self, zero_ms: float, bid_size: float, ask_size: float):
        """冲刺模式触发通知"""
        msg = (
            "🔥 <b>冲刺模式触发!</b>\n"
            "\n"
            f"⏱️ 0差持续: {zero_ms:.0f}ms\n"
            f"📈 深度: 买 {bid_size:.4f} | 卖 {ask_size:.4f}\n"
            f"🚀 开始高频循环 (最多 {MAX_ROUNDS_PER_BURST} 轮)\n"
        )
        await self.send(msg)

    async def notify_error(self, reason: str, stats: dict):
        """异常/停止通知"""
        msg = (
            "⚠️ <b>策略异常停止!</b>\n"
            "\n"
            f"❌ 原因: {reason}\n"
            f"🔄 已完成循环: {stats['cycles']}\n"
            f"💵 合计盈亏: ${stats['pnl_total']:+.4f}\n"
            f"📈 交易量: ${stats['volume']:,.0f}\n"
        )
        await self.send(msg)

    async def notify_shutdown(self, cycle: int, stats: dict,
                              account_a: 'AccountTrader', account_b: 'AccountTrader',
                              elapsed_min: float):
        """策略结束最终报告"""
        pnl_a = account_a.get_pnl()
        pnl_b = account_b.get_pnl()
        result_emoji = "✅" if stats['pnl_total'] >= 0 else "⚠️"

        msg = (
            f"{result_emoji} <b>策略运行结束</b>\n"
            "\n"
            f"🔄 总循环: {cycle} | 成交笔数: {cycle * 4}\n"
            f"📈 总交易量: ${stats['volume']:,.0f}\n"
            f"💵 合计盈亏: ${stats['pnl_total']:+.4f} USDC\n"
            f"📊 每万收益: ${stats['per_10k']:.4f}\n"
            "\n"
            f"🅰️ A: ${account_a.initial_balance:.2f} → ${account_a.current_balance:.2f} "
            f"({pnl_a:+.4f})\n"
            f"🅱️ B: ${account_b.initial_balance:.2f} → ${account_b.current_balance:.2f} "
            f"({pnl_b:+.4f})\n"
            f"⏰ 运行时长: {elapsed_min:.1f} 分钟\n"
        )
        await self.send(msg)


# ─── BBO Data Recorder ───
class BboDataRecorder:
    """Writes BBO snapshots to daily CSV files for offline analysis."""

    HEADER = "timestamp,bid,ask,bid_size,ask_size,spread_pct,zero_ms,mid_price\n"

    def __init__(self, data_dir: str, buffer_size: int, enabled: bool):
        self.data_dir = data_dir
        self.buffer_size = buffer_size
        self.enabled = enabled
        self.current_date: str = ""
        self.file = None
        self.buffer: list[str] = []
        self.total_records: int = 0

        if self.enabled:
            os.makedirs(data_dir, exist_ok=True)
            logger.info(f"BBO 数据记录已启用 → {data_dir}/")

    def record(self, now: float, bid: float, ask: float,
               bid_size: float, ask_size: float,
               spread_pct: float, zero_ms: float, mid_price: float):
        """记录一条 BBO 快照"""
        if not self.enabled:
            return

        # 按日切分文件
        date_str = time.strftime("%Y-%m-%d", time.localtime(now))
        if date_str != self.current_date:
            self._rotate_file(date_str)

        self.buffer.append(
            f"{now:.3f},{bid},{ask},{bid_size},{ask_size},"
            f"{spread_pct:.6f},{zero_ms:.1f},{mid_price:.2f}\n"
        )
        self.total_records += 1

        if len(self.buffer) >= self.buffer_size:
            self._flush()

    def _rotate_file(self, date_str: str):
        """切换到新日期的文件"""
        self._flush()
        if self.file:
            self.file.close()

        filepath = os.path.join(self.data_dir, f"{date_str}.csv")
        is_new = not os.path.exists(filepath)
        self.file = open(filepath, "a", encoding="utf-8")
        if is_new:
            self.file.write(self.HEADER)
        self.current_date = date_str
        logger.info(f"BBO 数据文件切换: {filepath}")

    def _flush(self):
        """把缓冲写入磁盘"""
        if self.buffer and self.file:
            self.file.writelines(self.buffer)
            self.file.flush()
            self.buffer.clear()

    def close(self):
        """关闭文件, 刷出剩余缓冲"""
        self._flush()
        if self.file:
            self.file.close()
            self.file = None


# ─── Market Observer ───
class MarketObserver:
    """Real-time BBO monitor: spread tracking, zero-gap timing, burst detection."""

    def __init__(self):
        self.current_bbo: Dict[str, Any] = {
            "bid": 0.0, "ask": 0.0,
            "bid_size": 0.0, "ask_size": 0.0,
            "spread": 100.0, "mid_price": 0.0,
            "last_update": 0,
        }

        # 0 点差追踪
        self.zero_spread_start: float = 0       # 本次 0 点差开始的 time.time()
        self.zero_spread_duration_ms: float = 0  # 当前 0 点差已持续毫秒数

        # 模式
        self.mode: str = "normal"   # "normal" 或 "burst"

        # BBO 数据记录器
        self.recorder = BboDataRecorder(
            data_dir=BBO_RECORD_DIR,
            buffer_size=BBO_RECORD_BUFFER_SIZE,
            enabled=BBO_RECORD_ENABLED,
        )

    async def on_bbo_update(self, channel, message):
        """WebSocket BBO callback — updates spread, zero-gap timer, burst mode."""
        try:
            data = message.get("params", {}).get("data", {})
            if not data:
                return

            bid = float(data.get("bid", 0))
            ask = float(data.get("ask", 0))
            bid_size = float(data.get("bid_size", 0))
            ask_size = float(data.get("ask_size", 0))

            if bid <= 0 or ask <= 0:
                return

            mid = (bid + ask) / 2
            spread_pct = round((ask - bid) / mid * 100, 6)
            now = time.time()

            self.current_bbo = {
                "bid": bid, "ask": ask,
                "bid_size": bid_size, "ask_size": ask_size,
                "spread": spread_pct, "mid_price": mid,
                "last_update": now,
            }

            # 追踪 0 点差持续时间 (≤ 阈值视为 0)
            if spread_pct <= ZERO_SPREAD_THRESHOLD:
                if self.zero_spread_start == 0:
                    self.zero_spread_start = now
                self.zero_spread_duration_ms = (now - self.zero_spread_start) * 1000
            else:
                self.zero_spread_start = 0
                self.zero_spread_duration_ms = 0

            # 记录 BBO 数据 (用于离线分析, 在 0 差计算之后)
            self.recorder.record(
                now, bid, ask, bid_size, ask_size,
                spread_pct, self.zero_spread_duration_ms, mid,
            )

            # 检测冲刺模式
            self._detect_burst_mode()

        except Exception as e:
            logger.error(f"BBO 解析错误: {e}")

    def _detect_burst_mode(self):
        """Enter burst mode when zero-gap persists and depth is thick on both sides."""
        bbo = self.current_bbo

        if (self.zero_spread_duration_ms >= BURST_ZERO_SPREAD_MS
                and bbo["bid_size"] >= BURST_MIN_DEPTH
                and bbo["ask_size"] >= BURST_MIN_DEPTH):
            if self.mode != "burst":
                logger.info(
                    f"🔥 进入冲刺模式! 0差持续 {self.zero_spread_duration_ms:.0f}ms, "
                    f"深度 买:{bbo['bid_size']:.4f} 卖:{bbo['ask_size']:.4f}"
                )
            self.mode = "burst"
        else:
            if self.mode == "burst":
                logger.info("📉 退出冲刺模式")
            self.mode = "normal"

    def is_spread_ready(self, min_ms: float) -> bool:
        """True if spread ≤ threshold for at least min_ms (ignores depth)."""
        bbo = self.current_bbo

        # 数据不能太旧 (>1s 视为过期)
        if time.time() - bbo["last_update"] > 1.0:
            return False

        # 必须 0 点差 (≤ 阈值)
        if bbo["spread"] > ZERO_SPREAD_THRESHOLD:
            return False

        # 0 点差持续 >= min_ms
        if self.zero_spread_duration_ms < min_ms:
            return False

        return True

    def calc_safe_size(self) -> float:
        """Dynamic order size = min(ORDER_SIZE, thin_side × safety_factor). Returns 0 if below minimum."""
        bbo = self.current_bbo

        if time.time() - bbo["last_update"] > 1.0:
            return 0

        thin_side = min(bbo["bid_size"], bbo["ask_size"])
        safe = min(ORDER_SIZE, thin_side * DEPTH_SAFETY_FACTOR)

        if safe < MIN_ORDER_SIZE:
            return 0

        # 向下取整到下单精度, 避免被交易所拒绝
        factor = 10 ** SIZE_DECIMALS
        safe = math.floor(safe * factor) / factor

        return safe

    def can_fill_close(self, size: float) -> bool:
        """True if both sides have enough depth to fill a close order of given size."""
        bbo = self.current_bbo
        if time.time() - bbo["last_update"] > 1.0:
            return False
        return bbo["bid_size"] >= size and bbo["ask_size"] >= size


# ─── Account Trader ───
class AccountTrader:
    """Single Paradex account: auth, market orders, balance, rate limiting."""

    def __init__(self, name: str, l2_address: str, l2_private_key: str,
                 persistence: Optional[RatePersistence] = None):
        self.name = name
        self.l2_address = l2_address
        self.l2_private_key = l2_private_key
        self.paradex: Optional[ParadexSubkey] = None
        self.rate_limiter = RateLimiter(
            MAX_ORDERS_PER_MINUTE, MAX_ORDERS_PER_HALF_HOUR, MAX_ORDERS_PER_DAY,
            l2_address=l2_address, persistence=persistence,
        )
        self.last_auth_time: float = 0
        self.initial_balance: float = 0.0
        self.current_balance: float = 0.0
        self.order_count: int = 0

    async def connect(self) -> bool:
        """Connect to Paradex and obtain interactive token."""
        try:
            env = "prod" if PARADEX_ENV == "MAINNET" else "testnet"
            self.paradex = ParadexSubkey(
                env=env,
                l2_private_key=self.l2_private_key,
                l2_address=self.l2_address,
            )
            await self.paradex.init_account()
            await self.auth_interactive()
            return True
        except Exception as e:
            logger.error(f"[{self.name}] 连接失败: {e}")
            return False

    def _auth_interactive_sync(self):
        """Blocking interactive token request (runs in thread pool)."""
        import time as time_module
        from paradex_py.api.models import AuthSchema

        api_client = self.paradex.api_client
        account = self.paradex.account

        headers = account.auth_headers()
        path = f"auth/{hex(account.l2_public_key)}?token_usage=interactive"
        res = api_client.post(api_url=api_client.api_url, path=path, headers=headers)

        data = AuthSchema().load(res, unknown="exclude", partial=True)
        api_client.auth_timestamp = int(time_module.time())
        account.set_jwt_token(data.jwt_token)
        api_client.client.headers.update({"Authorization": f"Bearer {data.jwt_token}"})

        self.last_auth_time = time_module.time()
        logger.info(f"[{self.name}] Interactive Token 获取成功")

    async def auth_interactive(self):
        """Async interactive token request — non-blocking."""
        await asyncio.to_thread(self._auth_interactive_sync)

    async def refresh_token_if_needed(self, max_age: int = 240):
        """Auto-refresh token before expiry (token TTL ~5min, refresh at 4min)."""
        if time.time() - self.last_auth_time >= max_age:
            await self.auth_interactive()

    def _place_order_sync(self, side: str, size: float) -> dict:
        """Blocking market order (runs in thread pool)."""
        order_side = OrderSide.Buy if side == "BUY" else OrderSide.Sell
        logger.info(f"[{self.name}] SUBMIT {side} {size} {MARKET} (OrderSide={order_side})")
        order = Order(
            market=MARKET,
            order_type=OrderType.Market,
            order_side=order_side,
            size=Decimal(str(size)),
        )
        result = self.paradex.api_client.submit_order(order)
        self.order_count += 1
        logger.info(f"[{self.name}] FILLED {side} {size} — result: {result}")
        return result

    async def place_order_async(self, side: str, size: float) -> dict:
        """Async market order — non-blocking, parallelizable via gather."""
        return await asyncio.to_thread(self._place_order_sync, side, size)

    def _get_balance_sync(self) -> float:
        """Blocking balance fetch."""
        try:
            summary = self.paradex.api_client.fetch_account_summary()
            if hasattr(summary, 'account_value') and summary.account_value:
                return float(summary.account_value)
            if hasattr(summary, 'equity') and summary.equity:
                return float(summary.equity)
            if hasattr(summary, 'free_collateral') and summary.free_collateral:
                return float(summary.free_collateral)
            return -1
        except Exception:
            return -1

    async def get_balance_async(self) -> float:
        """Async balance fetch."""
        return await asyncio.to_thread(self._get_balance_sync)

    def can_trade(self) -> tuple[bool, float, str]:
        """Check if rate limits allow placing an order."""
        return self.rate_limiter.can_place_order()

    def get_pnl(self) -> float:
        """Realized PnL based on balance delta."""
        return self.current_balance - self.initial_balance


# ─── PnL Tracker ───
class DualPnLTracker:
    """Combined PnL and volume tracker for both accounts."""

    def __init__(self):
        self.total_volume_usd: float = 0.0
        self.cycle_count: int = 0

    def record_cycle(self, price: float, size: float):
        """
        记录一个完整循环的成交量:
        每循环 = A开 + A平 + B开 + B平 = 4 笔成交
        Volume = price * size * 4
        """
        self.total_volume_usd += price * size * 4
        self.cycle_count += 1

    def get_stats(self, account_a: AccountTrader, account_b: AccountTrader) -> dict:
        pnl_a = account_a.get_pnl()
        pnl_b = account_b.get_pnl()
        pnl_total = pnl_a + pnl_b

        per_10k = 0.0
        if self.total_volume_usd > 0:
            # 正值 = 每万元成交赚多少, 负值 = 每万元成交亏多少
            per_10k = pnl_total / self.total_volume_usd * 10000

        return {
            "pnl_a": pnl_a,
            "pnl_b": pnl_b,
            "pnl_total": pnl_total,
            "volume": self.total_volume_usd,
            "per_10k": per_10k,
            "cycles": self.cycle_count,
        }


# ─── ANSI Colors ───
class C:
    """ANSI escape sequences for terminal styling."""
    RST  = "\033[0m"
    BOLD = "\033[1m"
    DIM  = "\033[2m"
    RED    = "\033[31m"
    GREEN  = "\033[32m"
    YELLOW = "\033[33m"
    BLUE   = "\033[34m"
    PURPLE = "\033[35m"
    CYAN   = "\033[36m"
    WHITE  = "\033[37m"
    # 亮色
    BRED   = "\033[91m"
    BGREEN = "\033[92m"
    BYELLOW = "\033[93m"
    BCYAN  = "\033[96m"
    BWHITE = "\033[97m"

    @staticmethod
    def pnl(val: float) -> str:
        """PnL 上色: 绿正红负"""
        if val > 0:
            return f"{C.BGREEN}+{val:.4f}{C.RST}"
        elif val < 0:
            return f"{C.BRED}{val:.4f}{C.RST}"
        return f"{C.DIM}0.0000{C.RST}"

    @staticmethod
    def spread_color(spread: float, threshold: float) -> str:
        """价差上色: 绿=0差 黄=接近 红=远"""
        if spread < threshold:
            return f"{C.BGREEN}{spread:.5f}%{C.RST}"
        elif spread < threshold * 5:
            return f"{C.BYELLOW}{spread:.5f}%{C.RST}"
        return f"{C.DIM}{spread:.5f}%{C.RST}"

    @staticmethod
    def bar(current: int, maximum: int, width: int = 10) -> str:
        """进度条: ████░░░░"""
        ratio = min(current / maximum, 1.0) if maximum > 0 else 0
        filled = int(ratio * width)
        empty = width - filled
        if ratio >= 0.9:
            color = C.BRED
        elif ratio >= 0.7:
            color = C.BYELLOW
        else:
            color = C.BCYAN
        return f"{color}{'█' * filled}{C.DIM}{'░' * empty}{C.RST}"

    @staticmethod
    def state_badge(state_val: str) -> str:
        """状态标签上色"""
        if state_val == "IDLE":
            return f"{C.BCYAN}{C.BOLD} IDLE {C.RST}"
        elif state_val == "HOLDING":
            return f"{C.BYELLOW}{C.BOLD} HOLD {C.RST}"
        return f"{C.DIM} {state_val} {C.RST}"

    @staticmethod
    def mode_badge(mode: str) -> str:
        """模式标签"""
        if mode == "burst":
            return f"{C.BRED}{C.BOLD}⚡BURST{C.RST}"
        return f"{C.DIM}NORMAL{C.RST}"


# ─── Display Panel ───
class FixedPanel:
    """Fixed-position terminal panel with ANSI overwrite refresh."""

    PANEL_LINES = 15

    def __init__(self):
        self.initialized = False

    def init_panel(self):
        if not self.initialized:
            print("\n" * self.PANEL_LINES, end="")
            self.initialized = True

    def update(self, lines: list[str]):
        sys.stdout.write(f"\033[{self.PANEL_LINES}A")
        sys.stdout.write("\033[J")
        for i, line in enumerate(lines):
            if i < self.PANEL_LINES:
                print(line)
        for _ in range(self.PANEL_LINES - len(lines)):
            print()
        sys.stdout.flush()


# ─── Strategy Controller ───
class DualAccountController:
    """Core state machine: IDLE ⇄ HOLDING, with multi-group rotation and persistent rate limits."""

    def __init__(self):
        self.observer = MarketObserver()
        self.account_a: Optional[AccountTrader] = None
        self.account_b: Optional[AccountTrader] = None
        self.pnl_tracker = DualPnLTracker()
        self.latency_tracker = LatencyTracker()
        self.panel = FixedPanel()
        self.tg = TelegramNotifier(TG_BOT_TOKEN, TG_CHAT_ID, TG_ENABLED)

        # Persistence & groups
        self.persistence = RatePersistence(RATE_LIMITS_FILE)
        self.groups: List[dict] = ACCOUNT_GROUPS
        self.current_group_idx: int = -1
        self.current_group_name: str = ""

        # WS account (shared across group switches, uses first group's long account)
        self.ws_account: Optional[AccountTrader] = None

        # 状态
        self.state = StrategyState.IDLE
        self.running = False
        self.start_time: Optional[float] = None

        # 循环计数
        self.cycle_count = 0
        self.successful_cycles = 0
        self.failed_cycles = 0
        self.consecutive_failures = 0

        # 方向控制 (每轮交替)
        self.current_direction = "A_LONG"   # "A_LONG" 或 "A_SHORT"

        # 持仓计时 & 动态单量
        self.hold_start_time: float = 0
        self.current_position_size: float = 0  # 当前持仓单量 (平仓时用)

        # 冲刺模式
        self.burst_rounds: int = 0

        # TG 通知控制
        self._last_tg_cycle: int = 0          # 上次发 TG 时的循环数
        self._burst_notified: bool = False     # 本次冲刺窗口是否已通知

    # ─── Group Management ───

    def _validate_groups(self) -> bool:
        """Validate ACCOUNT_GROUPS config: at least 1 group with all required fields."""
        if not self.groups:
            print(f"{C.BRED}❌ ACCOUNT_GROUPS 为空! 请在 config.py 中配置至少一组账户{C.RST}")
            return False

        required = ["name", "l2_address_long", "l2_private_key_long",
                     "l2_address_short", "l2_private_key_short"]
        for i, g in enumerate(self.groups):
            for field in required:
                if not g.get(field):
                    print(f"{C.BRED}❌ 账户组 {i} ({g.get('name', '?')}) 缺少字段: {field}{C.RST}")
                    return False
        return True

    def _find_available_group(self) -> int:
        """Find the first group where both accounts can trade. Returns index or -1."""
        for i, g in enumerate(self.groups):
            can_long, _, _ = self.persistence.can_trade(g["l2_address_long"])
            can_short, _, _ = self.persistence.can_trade(g["l2_address_short"])
            if can_long and can_short:
                return i
        return -1

    def _calc_wait_time(self) -> float:
        """Calculate seconds until any group becomes available again."""
        min_wait = float("inf")
        for g in self.groups:
            wait_long = self.persistence.earliest_unlock(g["l2_address_long"])
            wait_short = self.persistence.earliest_unlock(g["l2_address_short"])
            group_wait = max(wait_long, wait_short)  # both must be free
            min_wait = min(min_wait, group_wait)
        return min_wait if min_wait != float("inf") else 0

    async def _connect_group(self, group_idx: int) -> bool:
        """Connect a specific group's two accounts (long + short)."""
        g = self.groups[group_idx]
        env = "prod" if PARADEX_ENV == "MAINNET" else "testnet"
        name = g["name"]

        print(f"🔌 连接组 {C.BOLD}{name}{C.RST} — 做多账户 ({env})...")
        self.account_a = AccountTrader(
            f"{name}-Long", g["l2_address_long"], g["l2_private_key_long"],
            persistence=self.persistence,
        )
        if not await self.account_a.connect():
            print(f"❌ 组 {name} 做多账户连接失败!")
            return False
        print(f"✅ 组 {name} 做多账户连接成功 (Interactive Token)")

        print(f"🔌 连接组 {C.BOLD}{name}{C.RST} — 做空账户 ({env})...")
        self.account_b = AccountTrader(
            f"{name}-Short", g["l2_address_short"], g["l2_private_key_short"],
            persistence=self.persistence,
        )
        if not await self.account_b.connect():
            print(f"❌ 组 {name} 做空账户连接失败!")
            return False
        print(f"✅ 组 {name} 做空账户连接成功 (Interactive Token)")

        self.current_group_idx = group_idx
        self.current_group_name = name

        # First group also serves as WS provider
        if self.ws_account is None:
            self.ws_account = self.account_a

        return True

    async def _switch_group(self) -> bool:
        """Close current positions if holding, then switch to next available group."""
        old_name = self.current_group_name

        # 1. Close positions if holding
        if self.state == StrategyState.HOLDING:
            logger.info(f"组 {old_name} 限额满, 先平仓当前持仓...")
            await self._close_both(emergency=True)

        # 2. Find next available group
        new_idx = self._find_available_group()
        if new_idx < 0:
            return False

        logger.info(f"切换: 组 {old_name} → 组 {self.groups[new_idx]['name']}")

        # 3. Connect new group
        if not await self._connect_group(new_idx):
            return False

        # 4. Init balances for new group
        if not await self._init_balances():
            return False

        # 5. TG notification
        await self.tg.send(
            f"🔄 <b>组切换: {old_name} → {self.current_group_name}</b>\n"
            f"💰 Long: ${self.account_a.current_balance:.2f} | "
            f"Short: ${self.account_b.current_balance:.2f}"
        )

        # Reset state for new group
        self.state = StrategyState.IDLE
        self.consecutive_failures = 0
        self.current_position_size = 0

        return True

    # ─── Startup ───

    async def start(self):
        W = 74
        BAR = f"{C.BCYAN}{'━' * W}{C.RST}"
        print()
        print(BAR)
        print(f"  {C.BOLD}{C.BWHITE}PARADEX DUAL HEDGE v2{C.RST}"
              f"  {C.DIM}RPI Negative Spread Arbitrage{C.RST}")
        print(BAR)
        print(f"  {C.BOLD}MARKET{C.RST}  {C.BWHITE}{MARKET}{C.RST}"
              f"    {C.BOLD}SIZE{C.RST}  {ORDER_SIZE} {COIN_SYMBOL} {C.DIM}(dynamic){C.RST}"
              f"    {C.BOLD}MAX{C.RST}  {MAX_CYCLES} cycles")
        print(f"  {C.BOLD}ENTRY{C.RST}   0-gap ≥{ENTRY_ZERO_SPREAD_MS}ms"
              f"    {C.BOLD}SAFETY{C.RST}  ×{DEPTH_SAFETY_FACTOR}"
              f"    {C.BOLD}MIN{C.RST}  {MIN_ORDER_SIZE} {COIN_SYMBOL}")
        print(f"  {C.BOLD}BURST{C.RST}   0-gap ≥{BURST_ZERO_SPREAD_MS}ms"
              f"    {C.BOLD}DEPTH{C.RST}   ≥{BURST_MIN_DEPTH} {COIN_SYMBOL}"
              f"    {C.BOLD}MAX{C.RST}  {MAX_ROUNDS_PER_BURST} rounds")
        print(f"  {C.BOLD}RATE{C.RST}    {MAX_ORDERS_PER_MINUTE}/min"
              f"  {MAX_ORDERS_PER_HALF_HOUR}/30min"
              f"  {MAX_ORDERS_PER_DAY}/day {C.DIM}(per account){C.RST}")
        group_names = ", ".join(g["name"] for g in self.groups)
        print(f"  {C.BOLD}GROUPS{C.RST}  {C.BWHITE}{len(self.groups)}{C.RST}"
              f" [{group_names}]"
              f"    {C.BOLD}PERSIST{C.RST}  {C.DIM}{RATE_LIMITS_FILE}{C.RST}")
        print(BAR)
        print()

        # Validate groups
        if not self._validate_groups():
            return

        # Load persistence & find first available group
        start_idx = self._find_available_group()
        if start_idx < 0:
            wait = self._calc_wait_time()
            print(f"⏳ 所有账户组都已达到限额! 最快可用时间: {wait:.0f}s 后")
            print(f"   等待中...")
            await self._wait_for_available_group()
            start_idx = self._find_available_group()
            if start_idx < 0:
                print("❌ 无可用账户组, 退出")
                return

        # Show persisted counts
        for g in self.groups:
            m_l, h_l, d_l = self.persistence.get_counts(g["l2_address_long"])
            m_s, h_s, d_s = self.persistence.get_counts(g["l2_address_short"])
            avail = "✅" if self._group_available(g) else "⛔"
            print(f"  {avail} 组 {C.BOLD}{g['name']}{C.RST}"
                  f"  Long: {m_l}m/{h_l}h/{d_l}d"
                  f"  Short: {m_s}m/{h_s}h/{d_s}d")

        print()

        if not await self._connect_group(start_idx):
            return
        if not await self._subscribe_bbo():
            return
        if not await self._init_balances():
            return

        # TG: 启动通知 + 后台指令轮询
        await self.tg.notify_startup(
            self.account_a.current_balance,
            self.account_b.current_balance,
            group_name=self.current_group_name,
            total_groups=len(self.groups),
        )
        self.tg.start_polling()

        print()
        self.running = True
        self.start_time = time.time()
        self.panel.init_panel()

        try:
            await self.main_loop()
        except KeyboardInterrupt:
            pass
        finally:
            self.tg.stop_polling()
            await self.shutdown()

    def _group_available(self, g: dict) -> bool:
        """Check if both accounts in a group can trade."""
        can_l, _, _ = self.persistence.can_trade(g["l2_address_long"])
        can_s, _, _ = self.persistence.can_trade(g["l2_address_short"])
        return can_l and can_s

    async def _wait_for_available_group(self):
        """Block until at least one group is available, showing countdown."""
        while True:
            idx = self._find_available_group()
            if idx >= 0:
                return
            wait = self._calc_wait_time()
            if wait <= 0:
                return
            print(f"\r  ⏳ 所有组限额满, 等待 {wait:.0f}s...", end="", flush=True)
            await asyncio.sleep(min(wait, 5))

    async def _subscribe_bbo(self) -> bool:
        """Subscribe to BBO via WS account (shared across group switches)."""
        try:
            ws_src = self.ws_account or self.account_a
            print("📡 连接 WebSocket...")
            await ws_src.paradex.ws_client.connect()

            print(f"📊 订阅 {MARKET} BBO...")
            await ws_src.paradex.ws_client.subscribe(
                ParadexWebsocketChannel.BBO,
                callback=self.observer.on_bbo_update,
                params={"market": MARKET},
            )

            print("⏳ 等待 BBO 数据...")
            for _ in range(50):
                await asyncio.sleep(0.1)
                if self.observer.current_bbo["last_update"] > 0:
                    print(f"✅ 收到 BBO: ${self.observer.current_bbo['mid_price']:.0f}")
                    return True

            print("❌ 等待 BBO 超时!")
            return False
        except Exception as e:
            print(f"❌ WebSocket 连接失败: {e}")
            return False

    async def _init_balances(self) -> bool:
        """Fetch initial balances for both accounts in parallel."""
        bal_a, bal_b = await asyncio.gather(
            self.account_a.get_balance_async(),
            self.account_b.get_balance_async(),
        )

        if bal_a <= 0:
            print(f"❌ 做多账户余额获取失败: {bal_a}")
            return False
        if bal_b <= 0:
            print(f"❌ 做空账户余额获取失败: {bal_b}")
            return False

        self.account_a.initial_balance = bal_a
        self.account_a.current_balance = bal_a
        self.account_b.initial_balance = bal_b
        self.account_b.current_balance = bal_b

        print(f"💰 做多账户余额: ${bal_a:.4f} USDC")
        print(f"💰 做空账户余额: ${bal_b:.4f} USDC")
        print(f"💰 合计: ${bal_a + bal_b:.4f} USDC")
        return True

    # ─── Main Loop ───

    async def main_loop(self):
        last_balance_check: float = 0

        while self.running and self.cycle_count < MAX_CYCLES:
            # ── Telegram /stop (后台轮询, 这里只读 bool, 零开销) ──
            if self.tg.stop_requested:
                logger.info("Telegram /stop 指令触发停止")
                stats = self.pnl_tracker.get_stats(self.account_a, self.account_b)
                await self.tg.notify_error("Telegram /stop 指令", stats)
                break

            # 安全检查
            if os.path.exists(EMERGENCY_STOP_FILE):
                logger.info("检测到紧急停止文件, 退出")
                stats = self.pnl_tracker.get_stats(self.account_a, self.account_b)
                await self.tg.notify_error("检测到 STOP 文件", stats)
                break
            if self.consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                logger.error(f"连续失败 {self.consecutive_failures} 次, 停止策略")
                stats = self.pnl_tracker.get_stats(self.account_a, self.account_b)
                await self.tg.notify_error(
                    f"连续失败 {self.consecutive_failures} 次", stats
                )
                break

            try:
                # 刷新两个账户的 Token (每 240s, 并行)
                await asyncio.gather(
                    self.account_a.refresh_token_if_needed(240),
                    self.account_b.refresh_token_if_needed(240),
                )

                # 周期性更新余额 (每 10s)
                now = time.time()
                if now - last_balance_check > 10:
                    await self._update_balances()
                    last_balance_check = now

                # 更新 WS 延迟
                bbo = self.observer.current_bbo
                if bbo["last_update"] > 0:
                    ws_age_ms = (now - bbo["last_update"]) * 1000
                    self.latency_tracker.update_ws_latency(ws_age_ms)

                # ── 检查当前组是否还有额度, 否则切换 ──
                if self.state == StrategyState.IDLE:
                    can_a, _, _ = self.account_a.can_trade()
                    can_b, _, _ = self.account_b.can_trade()
                    if not can_a or not can_b:
                        logger.info(f"组 {self.current_group_name} 限额满, 尝试切换...")
                        if not await self._try_switch_or_wait():
                            break  # all exhausted and user stopped
                        continue

                # 状态机
                if self.state == StrategyState.IDLE:
                    await self._handle_idle()
                elif self.state == StrategyState.HOLDING:
                    await self._handle_holding()

                # 更新显示
                self._update_display()

            except Exception as e:
                logger.error(f"主循环错误: {e}")
                self.consecutive_failures += 1

            await asyncio.sleep(0.05)

    async def _try_switch_or_wait(self) -> bool:
        """Try to switch group, or wait if all groups are full. Returns False to stop."""
        if await self._switch_group():
            return True

        # All groups full — wait
        while self.running:
            if self.tg.stop_requested:
                return False
            if os.path.exists(EMERGENCY_STOP_FILE):
                return False

            wait = self._calc_wait_time()
            if wait <= 0:
                break

            # Show wait info on panel
            mins = wait / 60
            self._update_display_waiting(wait)

            if wait > 10:
                logger.info(f"所有组限额满, 等待 {mins:.1f}m")
                await self.tg.send(
                    f"⏳ 所有账户组限额满, 等待 {mins:.1f} 分钟后自动恢复"
                )

            await asyncio.sleep(min(wait, 5))

        # Try again after waiting
        if await self._switch_group():
            return True

        # Still no group — shouldn't normally happen
        logger.warning("等待后仍无可用组")
        return True  # keep running, loop will re-check

    # ─── State Handlers ───

    async def _handle_idle(self):
        """IDLE → check zero-gap + dynamic size → open both."""
        # 1. 价差条件
        if not self.observer.is_spread_ready(ENTRY_ZERO_SPREAD_MS):
            return

        # 2. 动态计算安全单量 (根据薄边深度)
        safe_size = self.observer.calc_safe_size()
        if safe_size <= 0:
            return

        # 3. 两个账户都要有下单额度
        can_a, _, _ = self.account_a.can_trade()
        can_b, _, _ = self.account_b.can_trade()
        if not can_a or not can_b:
            return

        await self._open_both(safe_size)

    async def _handle_holding(self):
        """HOLDING → wait for zero-gap to close, or force-close on timeout."""
        # 超时强制平仓 (不管深度, 必须平)
        hold_time = time.time() - self.hold_start_time
        if hold_time > MAX_HOLD_SECONDS:
            logger.warning(f"持仓超时 ({hold_time:.1f}s > {MAX_HOLD_SECONDS}s), 强制平仓")
            await self._close_both(emergency=True)
            return

        # 平仓条件: 0差等待时间减半 + 双边深度能填平仓单量
        exit_min_ms = ENTRY_ZERO_SPREAD_MS / 2

        if not self.observer.is_spread_ready(exit_min_ms):
            return

        if not self.observer.can_fill_close(self.current_position_size):
            return

        # 两个账户都要有下单额度
        can_a, _, _ = self.account_a.can_trade()
        can_b, _, _ = self.account_b.can_trade()
        if not can_a or not can_b:
            return

        await self._close_both()

    # ─── Open / Close ───

    async def _open_both(self, size: float):
        """Place opposing market orders on A and B simultaneously."""
        cycle_start = time.time()

        if self.current_direction == "A_LONG":
            a_side, b_side = "BUY", "SELL"
        else:
            a_side, b_side = "SELL", "BUY"

        dir_text = "A多B空" if self.current_direction == "A_LONG" else "A空B多"
        bbo = self.observer.current_bbo
        logger.info(f"[{self.current_group_name}] 开仓: {dir_text} | {size} {COIN_SYMBOL} "
                     f"(薄边:{min(bbo['bid_size'], bbo['ask_size']):.4f})")

        # 并行下单 (asyncio.to_thread 让两个 HTTP 同时发出)
        results = await asyncio.gather(
            self.account_a.place_order_async(a_side, size),
            self.account_b.place_order_async(b_side, size),
            return_exceptions=True,
        )

        a_ok = not isinstance(results[0], Exception)
        b_ok = not isinstance(results[1], Exception)

        if a_ok and b_ok:
            # ✅ 两边都成功 → 记录持仓单量
            self.account_a.rate_limiter.record_order()
            self.account_b.rate_limiter.record_order()
            self.current_position_size = size
            self.state = StrategyState.HOLDING
            self.hold_start_time = time.time()
            self.consecutive_failures = 0

            latency_ms = (time.time() - cycle_start) * 1000
            logger.info(f"开仓成功 | {dir_text} | {size} {COIN_SYMBOL} | {latency_ms:.0f}ms")

        elif a_ok and not b_ok:
            # ⚠️ A 成功 B 失败 → 立刻回撤 A
            logger.error(f"[B] 开仓失败: {results[1]}, 回撤 A...")
            self.account_a.rate_limiter.record_order()
            try:
                reverse = "SELL" if a_side == "BUY" else "BUY"
                await self.account_a.place_order_async(reverse, size)
                self.account_a.rate_limiter.record_order()
                logger.info("[A] 回撤成功")
            except Exception as e:
                logger.error(f"[A] 回撤失败: {e} — 请手动检查 A 的持仓!")
            self.state = StrategyState.IDLE
            self.consecutive_failures += 1
            self.failed_cycles += 1

        elif not a_ok and b_ok:
            # ⚠️ B 成功 A 失败 → 立刻回撤 B
            logger.error(f"[A] 开仓失败: {results[0]}, 回撤 B...")
            self.account_b.rate_limiter.record_order()
            try:
                reverse = "BUY" if b_side == "SELL" else "SELL"
                await self.account_b.place_order_async(reverse, size)
                self.account_b.rate_limiter.record_order()
                logger.info("[B] 回撤成功")
            except Exception as e:
                logger.error(f"[B] 回撤失败: {e} — 请手动检查 B 的持仓!")
            self.state = StrategyState.IDLE
            self.consecutive_failures += 1
            self.failed_cycles += 1

        else:
            # ❌ 两边都失败
            logger.error(f"开仓全部失败: A={results[0]}, B={results[1]}")
            self.state = StrategyState.IDLE
            self.consecutive_failures += 1
            self.failed_cycles += 1

    async def _close_both(self, emergency: bool = False):
        """Close both positions. On success, may trigger burst re-open."""
        cycle_start = time.time()
        close_size = self.current_position_size  # 用开仓时的单量平仓

        # 平仓方向: 和开仓相反
        if self.current_direction == "A_LONG":
            a_side, b_side = "SELL", "BUY"    # A 平多, B 平空
        else:
            a_side, b_side = "BUY", "SELL"    # A 平空, B 平多

        tag = " (超时强制)" if emergency else ""
        logger.info(f"[{self.current_group_name}] 平仓{tag} | {close_size} {COIN_SYMBOL}")

        # 并行平仓
        results = await asyncio.gather(
            self.account_a.place_order_async(a_side, close_size),
            self.account_b.place_order_async(b_side, close_size),
            return_exceptions=True,
        )

        a_ok = not isinstance(results[0], Exception)
        b_ok = not isinstance(results[1], Exception)

        if a_ok:
            self.account_a.rate_limiter.record_order()
        if b_ok:
            self.account_b.rate_limiter.record_order()

        if a_ok and b_ok:
            # ✅ 一个完整循环 (开+平) 完成
            self.cycle_count += 1
            self.successful_cycles += 1
            self.consecutive_failures = 0

            # 记录成交量 & 延迟 (使用实际平仓单量)
            price = self.observer.current_bbo["mid_price"]
            self.pnl_tracker.record_cycle(price, close_size)
            latency_ms = (time.time() - cycle_start) * 1000
            self.latency_tracker.record_cycle_latency(latency_ms)
            logger.info(f"✅ 循环 {self.cycle_count} 完成 | {close_size} {COIN_SYMBOL} | {latency_ms:.0f}ms")

            # 更新余额 (知道真实盈亏)
            await self._update_balances()

            # TG: 周期性进度报告
            if (self.cycle_count - self._last_tg_cycle) >= TG_NOTIFY_INTERVAL:
                self._last_tg_cycle = self.cycle_count
                elapsed = time.time() - self.start_time if self.start_time else 0
                stats = self.pnl_tracker.get_stats(self.account_a, self.account_b)
                await self.tg.notify_progress(
                    self.cycle_count, stats,
                    self.account_a, self.account_b,
                    elapsed / 60,
                )

            # 交替方向
            self.current_direction = (
                "A_SHORT" if self.current_direction == "A_LONG" else "A_LONG"
            )

            # ── 冲刺模式: 平仓后立即重新开仓 ──
            if (self.observer.mode == "burst"
                    and self.burst_rounds < MAX_ROUNDS_PER_BURST
                    and self.cycle_count < MAX_CYCLES
                    and not emergency):

                # 冲刺时放宽条件: 只要当前仍是 0 差就行, 动态算单量
                if self.observer.is_spread_ready(0):
                    burst_size = self.observer.calc_safe_size()
                    if burst_size > 0:
                        can_a, _, _ = self.account_a.can_trade()
                        can_b, _, _ = self.account_b.can_trade()
                        if can_a and can_b:
                            self.burst_rounds += 1
                            # TG: 冲刺模式首次触发时通知
                            if self.burst_rounds == 1 and not self._burst_notified:
                                self._burst_notified = True
                                bbo = self.observer.current_bbo
                                await self.tg.notify_burst(
                                    self.observer.zero_spread_duration_ms,
                                    bbo["bid_size"], bbo["ask_size"],
                                )
                            logger.info(f"🔥 冲刺连续开仓 (第 {self.burst_rounds} 轮) | {burst_size} {COIN_SYMBOL}")
                            await self._open_both(burst_size)
                            return  # state 已在 _open_both 中设为 HOLDING 或 IDLE

            # 非冲刺 / 冲刺结束 → 回到 IDLE
            self.burst_rounds = 0
            self._burst_notified = False
            self.state = StrategyState.IDLE

        elif a_ok and not b_ok:
            # ⚠️ A 平了, B 没平 → 重试 B
            logger.error(f"[B] 平仓失败: {results[1]}, 开始重试...")
            if await self._retry_close("B", self.account_b, b_side):
                self._on_close_success()
            else:
                logger.error("⛔ [B] 平仓重试耗尽! B 仍有持仓, 策略停止, 请手动处理")
                stats = self.pnl_tracker.get_stats(self.account_a, self.account_b)
                await self.tg.notify_error("B 平仓重试耗尽, B 仍有持仓!", stats)
                self.running = False
                self.state = StrategyState.IDLE

        elif not a_ok and b_ok:
            # ⚠️ B 平了, A 没平 → 重试 A
            logger.error(f"[A] 平仓失败: {results[0]}, 开始重试...")
            if await self._retry_close("A", self.account_a, a_side):
                self._on_close_success()
            else:
                logger.error("⛔ [A] 平仓重试耗尽! A 仍有持仓, 策略停止, 请手动处理")
                stats = self.pnl_tracker.get_stats(self.account_a, self.account_b)
                await self.tg.notify_error("A 平仓重试耗尽, A 仍有持仓!", stats)
                self.running = False
                self.state = StrategyState.IDLE

        else:
            # ❌ 两边都失败 → 仍持仓, 下轮重试
            logger.error(f"平仓全部失败: A={results[0]}, B={results[1]}")
            self.consecutive_failures += 1
            # state 保持 HOLDING, 下次循环会再尝试平仓

    async def _retry_close(self, name: str, account: AccountTrader, side: str) -> bool:
        """Retry a failed close up to 3 times using the stored position size."""
        close_size = self.current_position_size
        for attempt in range(1, 4):
            try:
                await account.place_order_async(side, close_size)
                account.rate_limiter.record_order()
                logger.info(f"[{name}] 重试平仓成功 (第{attempt}次) | {close_size} {COIN_SYMBOL}")
                return True
            except Exception as e:
                logger.error(f"[{name}] 重试平仓失败 (第{attempt}次): {e}")
                await asyncio.sleep(0.5)
        return False

    def _on_close_success(self):
        """Common post-close bookkeeping (cycle count, PnL, direction flip)."""
        self.cycle_count += 1
        self.successful_cycles += 1
        price = self.observer.current_bbo["mid_price"]
        self.pnl_tracker.record_cycle(price, self.current_position_size)
        self.current_direction = (
            "A_SHORT" if self.current_direction == "A_LONG" else "A_LONG"
        )
        self.burst_rounds = 0
        self.current_position_size = 0
        self.state = StrategyState.IDLE

    # ─── Helpers ───

    async def _update_balances(self):
        """Fetch balances for both accounts in parallel."""
        bal_a, bal_b = await asyncio.gather(
            self.account_a.get_balance_async(),
            self.account_b.get_balance_async(),
        )
        if bal_a > 0:
            self.account_a.current_balance = bal_a
        if bal_b > 0:
            self.account_b.current_balance = bal_b

    def _update_display(self):
        """Refresh the terminal monitoring panel."""
        bbo = self.observer.current_bbo
        now = time.time()

        ws_age = (now - bbo["last_update"]) * 1000 if bbo["last_update"] > 0 else 0
        elapsed = now - self.start_time if self.start_time else 0
        elapsed_min = elapsed / 60
        elapsed_hr = elapsed / 3600

        pnl_a = self.account_a.get_pnl()
        pnl_b = self.account_b.get_pnl()
        pnl_total = pnl_a + pnl_b
        stats = self.pnl_tracker.get_stats(self.account_a, self.account_b)

        min_a, half_a, day_a = self.account_a.rate_limiter.get_counts()
        min_b, half_b, day_b = self.account_b.rate_limiter.get_counts()

        dir_text = f"{C.CYAN}A多B空{C.RST}" if self.current_direction == "A_LONG" else f"{C.PURPLE}A空B多{C.RST}"
        zero_ms = self.observer.zero_spread_duration_ms
        zero_color = C.BGREEN if zero_ms > 0 else C.DIM

        # 动态单量
        if self.state == StrategyState.HOLDING:
            size_text = f"{C.BYELLOW}{self.current_position_size}{C.RST}"
        else:
            safe = self.observer.calc_safe_size()
            size_text = f"{C.BCYAN}{safe}{C.RST}" if safe > 0 else f"{C.DIM}--{C.RST}"

        # 运行时间格式
        if elapsed_hr >= 1:
            time_text = f"{elapsed_hr:.1f}h"
        else:
            time_text = f"{elapsed_min:.1f}m"

        # 组信息
        grp_text = (f"{C.BOLD}GRP{C.RST} {C.BWHITE}{self.current_group_name}{C.RST}"
                    f"/{len(self.groups)}")

        W = 74
        BAR = f"{C.BCYAN}{'━' * W}{C.RST}"

        lines = [
            BAR,
            f"  {C.BOLD}{C.BWHITE}PARADEX DUAL HEDGE{C.RST}"
            f"  {C.state_badge(self.state.value)}"
            f"  {C.mode_badge(self.observer.mode)}"
            f"  {C.DIM}{MARKET}{C.RST}"
            f"  {grp_text}",
            BAR,
            # ── 行情 ──
            f"  {C.BOLD}PRICE{C.RST}  {C.BWHITE}${bbo['mid_price']:,.2f}{C.RST}"
            f"    {C.BOLD}SPREAD{C.RST}  {C.spread_color(bbo['spread'], ZERO_SPREAD_THRESHOLD)}"
            f"    {C.BOLD}0-GAP{C.RST}  {zero_color}{zero_ms:.0f}ms{C.RST}",
            f"  {C.BOLD}DEPTH{C.RST}  {C.CYAN}BID {bbo['bid_size']:.4f}{C.RST}"
            f"   {C.PURPLE}ASK {bbo['ask_size']:.4f}{C.RST}"
            f"    {C.BOLD}SIZE{C.RST}  {size_text}"
            f"    {C.BOLD}NEXT{C.RST}  {dir_text}",
            BAR,
            # ── 账户 ──
            f"  {C.BOLD}{C.CYAN}L{C.RST}"
            f"  ${self.account_a.current_balance:>8.2f}"
            f"  {C.pnl(pnl_a)}"
            f"  {C.bar(min_a, MAX_ORDERS_PER_MINUTE, 6)} {min_a:>2}/{MAX_ORDERS_PER_MINUTE}m"
            f"  {C.bar(half_a, MAX_ORDERS_PER_HALF_HOUR, 6)} {half_a:>3}/{MAX_ORDERS_PER_HALF_HOUR}h"
            f"  {C.bar(day_a, MAX_ORDERS_PER_DAY, 6)} {day_a:>4}/{MAX_ORDERS_PER_DAY}d",
            f"  {C.BOLD}{C.PURPLE}S{C.RST}"
            f"  ${self.account_b.current_balance:>8.2f}"
            f"  {C.pnl(pnl_b)}"
            f"  {C.bar(min_b, MAX_ORDERS_PER_MINUTE, 6)} {min_b:>2}/{MAX_ORDERS_PER_MINUTE}m"
            f"  {C.bar(half_b, MAX_ORDERS_PER_HALF_HOUR, 6)} {half_b:>3}/{MAX_ORDERS_PER_HALF_HOUR}h"
            f"  {C.bar(day_b, MAX_ORDERS_PER_DAY, 6)} {day_b:>4}/{MAX_ORDERS_PER_DAY}d",
            BAR,
            # ── 统计 ──
            f"  {C.BOLD}CYCLES{C.RST}  {C.BWHITE}{self.cycle_count}{C.RST}/{MAX_CYCLES}"
            f"   {C.GREEN}✓{self.successful_cycles}{C.RST}"
            f" {C.RED}✗{self.failed_cycles}{C.RST}"
            f"   {C.BOLD}BURST{C.RST} {self.burst_rounds}"
            f"    {C.BOLD}PnL{C.RST}  {C.pnl(pnl_total)} U",
            f"  {C.BOLD}VOL{C.RST}  ${stats['volume'] / 1000:.1f}K"
            f"   {C.BOLD}PER10K{C.RST}  {C.pnl(stats['per_10k'])}"
            f"    {C.BOLD}WS{C.RST} {ws_age:.0f}ms"
            f"   {C.BOLD}LAT{C.RST} [{self.latency_tracker.format_recent()}]"
            f"   {C.DIM}{time_text}{C.RST}",
            BAR,
        ]

        self.panel.update(lines)

    def _update_display_waiting(self, wait_seconds: float):
        """Show a waiting panel when all groups are rate-limited."""
        W = 74
        BAR = f"{C.BCYAN}{'━' * W}{C.RST}"
        mins = wait_seconds / 60

        lines_info = []
        for g in self.groups:
            m_l, h_l, d_l = self.persistence.get_counts(g["l2_address_long"])
            m_s, h_s, d_s = self.persistence.get_counts(g["l2_address_short"])
            avail = f"{C.BGREEN}✅{C.RST}" if self._group_available(g) else f"{C.BRED}⛔{C.RST}"
            lines_info.append(
                f"  {avail} {C.BOLD}{g['name']}{C.RST}"
                f"  L:{m_l}m/{h_l}h/{d_l}d"
                f"  S:{m_s}m/{h_s}h/{d_s}d"
            )

        lines = [
            BAR,
            f"  {C.BOLD}{C.BYELLOW}⏳ ALL GROUPS RATE LIMITED{C.RST}"
            f"  {C.DIM}{MARKET}{C.RST}",
            BAR,
            f"  {C.BOLD}WAIT{C.RST}  {C.BYELLOW}{mins:.1f}m{C.RST} remaining",
            BAR,
        ] + lines_info + [
            BAR,
            "",  # padding
            "",
            "",
            "",
            "",
            "",
            BAR,
        ]

        # Trim to PANEL_LINES
        lines = lines[:self.panel.PANEL_LINES]
        self.panel.update(lines)

    # ─── Shutdown ───

    async def shutdown(self):
        """Graceful shutdown: final stats, TG report, cleanup."""
        self.running = False

        # 关闭 BBO 数据记录器 (刷出剩余缓冲)
        self.observer.recorder.close()
        if self.observer.recorder.total_records > 0:
            print(f"📝 BBO 数据已保存: {self.observer.recorder.total_records} 条 → {BBO_RECORD_DIR}/")

        # 最终余额
        try:
            await self._update_balances()
        except Exception:
            pass

        elapsed = time.time() - self.start_time if self.start_time else 0
        stats = self.pnl_tracker.get_stats(self.account_a, self.account_b)
        latency = self.latency_tracker.get_stats()

        print("\n" * 2)
        print("=" * 72)
        print(f"📊 双账户对冲策略 - 最终统计 (组 {self.current_group_name})")
        print("=" * 72)
        print(f"   循环: {self.cycle_count} "
              f"(成功: {self.successful_cycles}, 失败: {self.failed_cycles})")
        print(f"   运行: {elapsed / 60:.1f} 分钟")
        print("-" * 72)
        print(f"  做多账户 ({self.account_a.name}):")
        print(f"   初始: ${self.account_a.initial_balance:.4f} → "
              f"当前: ${self.account_a.current_balance:.4f}")
        print(f"   盈亏: ${self.account_a.get_pnl():+.4f} USDC | "
              f"下单: {self.account_a.order_count} 单")
        print(f"  做空账户 ({self.account_b.name}):")
        print(f"   初始: ${self.account_b.initial_balance:.4f} → "
              f"当前: ${self.account_b.current_balance:.4f}")
        print(f"   盈亏: ${self.account_b.get_pnl():+.4f} USDC | "
              f"下单: {self.account_b.order_count} 单")
        print("-" * 72)
        print(f"💵 合计盈亏: ${stats['pnl_total']:+.4f} USDC")
        print(f"📈 总交易量: ${stats['volume']:,.2f} USD")
        if stats['volume'] > 0:
            print(f"📊 每万成交: ${stats['per_10k']:.4f}")
        print("-" * 72)
        if latency["recent"]:
            print(f"⏱️  延迟: 平均 {latency['avg']:.0f}ms | "
                  f"最小 {latency['min']:.0f}ms | 最大 {latency['max']:.0f}ms")
        print("=" * 72)

        # TG: 最终报告
        await self.tg.notify_shutdown(
            self.cycle_count, stats,
            self.account_a, self.account_b,
            elapsed / 60,
        )

        # 关闭 WebSocket
        try:
            ws_src = self.ws_account or self.account_a
            await ws_src.paradex.ws_client.close()
        except Exception:
            pass

        print("👋 已退出")


# ─── Coin Selection ───

def select_coin() -> str:
    """Interactive coin selector. Also accepts --coin BTC from CLI."""
    # 命令行参数: python3 dual_scalper.py --coin BTC
    if "--coin" in sys.argv:
        idx = sys.argv.index("--coin")
        if idx + 1 < len(sys.argv):
            coin = sys.argv[idx + 1].upper()
            if coin in COIN_PRESETS:
                return coin
            else:
                print(f"❌ 无效币种: {coin}，可选: {', '.join(COIN_PRESETS.keys())}")
                sys.exit(1)

    # 交互式菜单
    coins = list(COIN_PRESETS.keys())
    BAR = f"{C.BCYAN}{'━' * 56}{C.RST}"
    print()
    print(BAR)
    print(f"  {C.BOLD}{C.BWHITE}PARADEX DUAL HEDGE{C.RST}  {C.DIM}Select Trading Pair{C.RST}")
    print(BAR)
    for i, coin in enumerate(coins, 1):
        preset = COIN_PRESETS[coin]
        print(f"  {C.BOLD}{C.BWHITE}[{i}]{C.RST}"
              f"  {C.BCYAN}{coin:<4}{C.RST}"
              f"  {C.DIM}→{C.RST}  {preset['market']:<18}"
              f"  {C.DIM}size:{C.RST} {preset['order_size']}")
    print(BAR)

    while True:
        try:
            choice = input(f"\n  {C.BOLD}>{C.RST} ").strip()
            num = int(choice)
            if 1 <= num <= len(coins):
                selected = coins[num - 1]
                print(f"\n  {C.BGREEN}✓{C.RST} {C.BOLD}{selected}{C.RST} {C.DIM}({COIN_PRESETS[selected]['market']}){C.RST}\n")
                return selected
            else:
                print(f"  {C.BYELLOW}!{C.RST} 输入 1~{len(coins)}")
        except ValueError:
            upper = choice.upper()
            if upper in COIN_PRESETS:
                print(f"\n  {C.BGREEN}✓{C.RST} {C.BOLD}{upper}{C.RST} {C.DIM}({COIN_PRESETS[upper]['market']}){C.RST}\n")
                return upper
            print(f"  {C.BYELLOW}!{C.RST} 输入 1~{len(coins)} 或 {'/'.join(coins)}")
        except (EOFError, KeyboardInterrupt):
            print(f"\n  {C.DIM}cancelled{C.RST}")
            sys.exit(0)


def apply_coin_preset(coin: str):
    """Override runtime globals with the selected coin's preset."""
    global MARKET, ORDER_SIZE, MIN_ORDER_SIZE, SIZE_DECIMALS
    global BURST_MIN_DEPTH, COIN_SYMBOL

    preset = COIN_PRESETS[coin]
    COIN_SYMBOL = coin
    MARKET = preset["market"]
    ORDER_SIZE = preset["order_size"]
    MIN_ORDER_SIZE = preset["min_order_size"]
    SIZE_DECIMALS = preset["size_decimals"]
    BURST_MIN_DEPTH = preset["burst_min_depth"]


# ─── Entry Point ───

async def main():
    controller = DualAccountController()
    await controller.start()


if __name__ == "__main__":
    try:
        # 1. 选择币种
        selected_coin = select_coin()
        # 2. 应用预设
        apply_coin_preset(selected_coin)
        # 3. 启动策略
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️  已中断")
    except Exception as e:
        print(f"❌ 错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
