"""
Paradex BTC 双账户对冲套利脚本 v1 - RPI 负点差套利版

策略逻辑:
1. WebSocket 实时监控 BTC-USD-PERP 盘口
2. 当 spread == 0 稳定 >= Nms 且深度足够时:
   - 账户 A 和账户 B 同时下反向市价单
   - 至少一边吃到 RPI 改善价 → 超低磨损 / 正收益
3. 持仓等待下一个 0 点差窗口，然后平仓
4. 交替方向，循环刷量
5. 检测到 "厚深度 + 持续0差" 时进入冲刺模式，加速循环

限制 (保持免费 Retail 档):
- 每账户: 30 单/min, 300 单/hr, 1000 单/24h
- Retail 模式 ~500ms speed bump
"""

import asyncio
import logging
import time
import os
import sys
from collections import deque
from decimal import Decimal
from enum import Enum
from typing import Optional, Dict, Any

from config import (
    MARKET, ORDER_SIZE_ETH, MAX_CYCLES, PARADEX_ENV,
    MAX_CONSECUTIVE_FAILURES, EMERGENCY_STOP_FILE,
    ACCOUNT_A_L2_ADDRESS, ACCOUNT_A_L2_PRIVATE_KEY,
    ACCOUNT_B_L2_ADDRESS, ACCOUNT_B_L2_PRIVATE_KEY,
    ZERO_SPREAD_THRESHOLD, ENTRY_ZERO_SPREAD_MS, MIN_DEPTH_MULTIPLIER,
    MAX_HOLD_SECONDS,
    BURST_ZERO_SPREAD_MS, BURST_MIN_DEPTH_ETH,
    MAX_ROUNDS_PER_BURST,
    TG_BOT_TOKEN, TG_CHAT_ID, TG_NOTIFY_INTERVAL, TG_ENABLED,
    BBO_RECORD_ENABLED, BBO_RECORD_DIR, BBO_RECORD_BUFFER_SIZE,
)

from paradex_py import ParadexSubkey
from paradex_py.api.ws_client import ParadexWebsocketChannel
from paradex_py.common.order import Order, OrderType, OrderSide


# ==================== 日志配置 ====================
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


# ==================== 常量 ====================
MAX_ORDERS_PER_MINUTE = 30
MAX_ORDERS_PER_HOUR = 300
MAX_ORDERS_PER_DAY = 1000


# ==================== 枚举 ====================
class StrategyState(Enum):
    IDLE = "IDLE"         # 无仓位，等待机会
    HOLDING = "HOLDING"   # 双向持仓中，等待平仓机会


# ==================== 速率限制器 ====================
class RateLimiter:
    """三级速率限制器: 分钟/小时/24小时"""

    def __init__(self, per_minute: int, per_hour: int, per_day: int):
        self.per_minute = per_minute
        self.per_hour = per_hour
        self.per_day = per_day
        self.minute_orders: deque = deque()
        self.hour_orders: deque = deque()
        self.day_orders: deque = deque()

    def can_place_order(self) -> tuple[bool, float, str]:
        now = time.time()
        while self.minute_orders and now - self.minute_orders[0] > 60:
            self.minute_orders.popleft()
        while self.hour_orders and now - self.hour_orders[0] > 3600:
            self.hour_orders.popleft()
        while self.day_orders and now - self.day_orders[0] > 86400:
            self.day_orders.popleft()

        if len(self.minute_orders) >= self.per_minute:
            return False, 60 - (now - self.minute_orders[0]), "分钟"
        if len(self.hour_orders) >= self.per_hour:
            return False, 3600 - (now - self.hour_orders[0]), "小时"
        if len(self.day_orders) >= self.per_day:
            return False, 86400 - (now - self.day_orders[0]), "24h"
        return True, 0, ""

    def record_order(self):
        now = time.time()
        self.minute_orders.append(now)
        self.hour_orders.append(now)
        self.day_orders.append(now)

    def get_counts(self) -> tuple[int, int, int]:
        return len(self.minute_orders), len(self.hour_orders), len(self.day_orders)


# ==================== 延迟追踪器 ====================
class LatencyTracker:
    """记录每轮开平仓耗时"""

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


# ==================== Telegram 通知 ====================
class TelegramNotifier:
    """
    Telegram Bot 通知器:
    - 纯 stdlib 实现 (urllib), 无额外依赖
    - 异步发送, 不阻塞主循环
    - 发送失败只记日志, 不影响策略运行
    """

    def __init__(self, bot_token: str, chat_id: str, enabled: bool = True):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.enabled = enabled and bool(bot_token) and bool(chat_id)
        if self.enabled:
            logger.info("Telegram 通知已启用")
        else:
            logger.info("Telegram 通知未启用 (未配置 Token/ChatID 或已关闭)")

    def _send_sync(self, text: str):
        """同步发送 (在线程中调用)"""
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

    async def send(self, text: str):
        """异步发送, 失败不抛异常"""
        if not self.enabled:
            return
        try:
            await asyncio.to_thread(self._send_sync, text)
        except Exception as e:
            logger.error(f"TG 发送失败: {e}")

    async def notify_startup(self, bal_a: float, bal_b: float):
        """策略启动通知"""
        msg = (
            "🚀 <b>Paradex 双账户对冲套利已启动</b>\n"
            "\n"
            f"📊 单量: {ORDER_SIZE_ETH} ETH\n"
            f"🚦 限速: {MAX_ORDERS_PER_MINUTE}/分 | {MAX_ORDERS_PER_DAY}/日 (每账户)\n"
            f"💰 A 余额: ${bal_a:.4f}\n"
            f"💰 B 余额: ${bal_b:.4f}\n"
            f"💰 合计: ${bal_a + bal_b:.4f}\n"
        )
        await self.send(msg)

    async def notify_progress(self, cycle: int, stats: dict,
                              account_a: 'AccountTrader', account_b: 'AccountTrader',
                              elapsed_min: float):
        """周期性进度报告"""
        pnl_a = account_a.get_pnl()
        pnl_b = account_b.get_pnl()
        _, _, day_a = account_a.rate_limiter.get_counts()
        _, _, day_b = account_b.rate_limiter.get_counts()

        pnl_emoji = "📈" if stats['pnl_total'] >= 0 else "📉"

        msg = (
            f"📊 <b>进度报告 — 第 {cycle}/{MAX_CYCLES} 轮</b>\n"
            "\n"
            f"🔄 成交笔数: {cycle * 4} 笔 (每轮4笔)\n"
            f"📈 累计交易量: ${stats['volume']:,.0f}\n"
            f"{pnl_emoji} 合计盈亏: ${stats['pnl_total']:+.4f}\n"
            f"📊 每万收益: ${stats['per_10k']:.4f}\n"
            "\n"
            f"🅰️ A: PnL ${pnl_a:+.4f} | 24h单数: {day_a}/{MAX_ORDERS_PER_DAY}\n"
            f"🅱️ B: PnL ${pnl_b:+.4f} | 24h单数: {day_b}/{MAX_ORDERS_PER_DAY}\n"
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


# ==================== BBO 数据记录器 ====================
class BboDataRecorder:
    """
    BBO 盘口数据记录器 — 用于离线分析 0 点差规律
    - 每天一个 CSV 文件: bbo_data/2026-02-09.csv
    - 带写入缓冲, 减少磁盘 IO
    - 记录字段: timestamp, bid, ask, bid_size, ask_size, spread_pct, zero_ms, mid_price
    """

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


# ==================== 市场观察器 ====================
class MarketObserver:
    """
    WebSocket 实时盘口监控:
    - 追踪 BBO (买一/卖一/深度)
    - 追踪 0 点差持续时长
    - 检测 "冲刺模式" (厚深度 + 持续0差)
    """

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
        """WebSocket BBO 消息回调"""
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
            spread_pct = (ask - bid) / mid * 100
            now = time.time()

            self.current_bbo = {
                "bid": bid, "ask": ask,
                "bid_size": bid_size, "ask_size": ask_size,
                "spread": spread_pct, "mid_price": mid,
                "last_update": now,
            }

            # 追踪 0 点差持续时间 (低于阈值视为 0)
            if spread_pct < ZERO_SPREAD_THRESHOLD:
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
        """
        冲刺模式判定:
        - 0 点差持续 >= BURST_ZERO_SPREAD_MS
        - 双边深度 >= BURST_MIN_DEPTH_BTC
        """
        bbo = self.current_bbo

        if (self.zero_spread_duration_ms >= BURST_ZERO_SPREAD_MS
                and bbo["bid_size"] >= BURST_MIN_DEPTH_ETH
                and bbo["ask_size"] >= BURST_MIN_DEPTH_ETH):
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

    def is_entry_ready(self, min_ms: float, min_depth: float) -> bool:
        """检查是否满足开/平仓条件"""
        bbo = self.current_bbo

        # 数据不能太旧 (>1s 视为过期)
        if time.time() - bbo["last_update"] > 1.0:
            return False

        # 必须 0 点差
        if bbo["spread"] >= ZERO_SPREAD_THRESHOLD:
            return False

        # 0 点差持续 >= min_ms
        if self.zero_spread_duration_ms < min_ms:
            return False

        # 双边深度足够
        if bbo["bid_size"] < min_depth or bbo["ask_size"] < min_depth:
            return False

        return True


# ==================== 单账户交易器 ====================
class AccountTrader:
    """
    封装单个 Paradex 账户:
    - 连接 / 认证 (Interactive Token)
    - 下市价单 (同步 + 异步)
    - 查余额
    - 独立速率限制
    """

    def __init__(self, name: str, l2_address: str, l2_private_key: str):
        self.name = name
        self.l2_address = l2_address
        self.l2_private_key = l2_private_key
        self.paradex: Optional[ParadexSubkey] = None
        self.rate_limiter = RateLimiter(MAX_ORDERS_PER_MINUTE, MAX_ORDERS_PER_HOUR, MAX_ORDERS_PER_DAY)
        self.last_auth_time: float = 0
        self.initial_balance: float = 0.0
        self.current_balance: float = 0.0
        self.order_count: int = 0

    async def connect(self) -> bool:
        """连接并初始化账户"""
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

    async def auth_interactive(self):
        """获取 Interactive Token (免费交易, 有 500ms speed bump)"""
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

    async def refresh_token_if_needed(self, max_age: int = 240):
        """Token 快过期时自动刷新 (默认 4 分钟刷新, token 5 分钟过期)"""
        if time.time() - self.last_auth_time >= max_age:
            await self.auth_interactive()

    def _place_order_sync(self, side: str, size: float) -> dict:
        """同步下市价单 (会阻塞线程)"""
        order = Order(
            market=MARKET,
            order_type=OrderType.Market,
            order_side=OrderSide.Buy if side == "BUY" else OrderSide.Sell,
            size=Decimal(str(size)),
        )
        result = self.paradex.api_client.submit_order(order)
        self.order_count += 1
        return result

    async def place_order_async(self, side: str, size: float) -> dict:
        """异步下市价单 (不阻塞事件循环, 可并行调用 A/B)"""
        return await asyncio.to_thread(self._place_order_sync, side, size)

    def _get_balance_sync(self) -> float:
        """同步获取账户余额"""
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
        """异步获取账户余额"""
        return await asyncio.to_thread(self._get_balance_sync)

    def can_trade(self) -> tuple[bool, float, str]:
        """检查速率限制是否允许下单"""
        return self.rate_limiter.can_place_order()

    def get_pnl(self) -> float:
        """当前盈亏 (基于真实余额变化)"""
        return self.current_balance - self.initial_balance


# ==================== 双账户盈亏统计 ====================
class DualPnLTracker:
    """双账户合并盈亏 & 成交量统计"""

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


# ==================== 固定面板显示 ====================
class FixedPanel:
    """终端固定面板 (覆盖式刷新, 不滚动)"""

    PANEL_LINES = 11

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


# ==================== 双账户策略控制器 ====================
class DualAccountController:
    """
    核心状态机:
      IDLE  ──(0差+深度+限速OK)──▶  HOLDING
               ◀──(0差+深度+限速OK / 超时强平)──
    冲刺模式: CLOSING 后立即重新 OPENING, 不回 IDLE
    """

    def __init__(self):
        self.observer = MarketObserver()
        self.account_a: Optional[AccountTrader] = None
        self.account_b: Optional[AccountTrader] = None
        self.pnl_tracker = DualPnLTracker()
        self.latency_tracker = LatencyTracker()
        self.panel = FixedPanel()
        self.tg = TelegramNotifier(TG_BOT_TOKEN, TG_CHAT_ID, TG_ENABLED)

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

        # 持仓计时
        self.hold_start_time: float = 0

        # 冲刺模式
        self.burst_rounds: int = 0

        # TG 通知控制
        self._last_tg_cycle: int = 0          # 上次发 TG 时的循环数
        self._burst_notified: bool = False     # 本次冲刺窗口是否已通知

    # ────────────────── 启动流程 ──────────────────

    async def start(self):
        print("=" * 72)
        print("🚀 Paradex BTC 双账户对冲套利 v1 - RPI 负点差套利版")
        print("=" * 72)
        print(f"📊 单量: {ORDER_SIZE_ETH} ETH | 最大循环: {MAX_CYCLES}")
        print(f"⏱️  触发: 0差≥{ENTRY_ZERO_SPREAD_MS}ms | "
              f"深度≥{ORDER_SIZE_ETH * MIN_DEPTH_MULTIPLIER:.3f} ETH")
        print(f"🔥 冲刺: 0差≥{BURST_ZERO_SPREAD_MS}ms | "
              f"深度≥{BURST_MIN_DEPTH_ETH} ETH | 每窗口≤{MAX_ROUNDS_PER_BURST}轮")
        print(f"🚦 限速: {MAX_ORDERS_PER_MINUTE}/分 | "
              f"{MAX_ORDERS_PER_HOUR}/时 | {MAX_ORDERS_PER_DAY}/24h (每账户)")
        print("=" * 72)

        # 检查配置
        if not ACCOUNT_A_L2_ADDRESS or not ACCOUNT_A_L2_PRIVATE_KEY:
            print("❌ 未配置账户 A 的 L2 密钥! 请编辑 config.py")
            return
        if not ACCOUNT_B_L2_ADDRESS or not ACCOUNT_B_L2_PRIVATE_KEY:
            print("❌ 未配置账户 B 的 L2 密钥! 请编辑 config.py")
            return

        if not await self._connect_accounts():
            return
        if not await self._subscribe_bbo():
            return
        if not await self._init_balances():
            return

        # TG: 启动通知
        await self.tg.notify_startup(
            self.account_a.current_balance,
            self.account_b.current_balance,
        )

        print()
        self.running = True
        self.start_time = time.time()
        self.panel.init_panel()

        try:
            await self.main_loop()
        except KeyboardInterrupt:
            pass
        finally:
            await self.shutdown()

    async def _connect_accounts(self) -> bool:
        """连接两个账户 (串行, 因为各自要做 L2 认证)"""
        env = "prod" if PARADEX_ENV == "MAINNET" else "testnet"

        print(f"🔌 连接账户 A ({env})...")
        self.account_a = AccountTrader("A", ACCOUNT_A_L2_ADDRESS, ACCOUNT_A_L2_PRIVATE_KEY)
        if not await self.account_a.connect():
            print("❌ 账户 A 连接失败!")
            return False
        print("✅ 账户 A 连接成功 (Interactive Token)")

        print(f"🔌 连接账户 B ({env})...")
        self.account_b = AccountTrader("B", ACCOUNT_B_L2_ADDRESS, ACCOUNT_B_L2_PRIVATE_KEY)
        if not await self.account_b.connect():
            print("❌ 账户 B 连接失败!")
            return False
        print("✅ 账户 B 连接成功 (Interactive Token)")

        return True

    async def _subscribe_bbo(self) -> bool:
        """通过账户 A 的 WebSocket 订阅 BBO"""
        try:
            print("📡 连接 WebSocket...")
            await self.account_a.paradex.ws_client.connect()

            print(f"📊 订阅 {MARKET} BBO...")
            await self.account_a.paradex.ws_client.subscribe(
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
        """并行查询两个账户的初始余额"""
        bal_a, bal_b = await asyncio.gather(
            self.account_a.get_balance_async(),
            self.account_b.get_balance_async(),
        )

        if bal_a <= 0:
            print(f"❌ 账户 A 余额获取失败: {bal_a}")
            return False
        if bal_b <= 0:
            print(f"❌ 账户 B 余额获取失败: {bal_b}")
            return False

        self.account_a.initial_balance = bal_a
        self.account_a.current_balance = bal_a
        self.account_b.initial_balance = bal_b
        self.account_b.current_balance = bal_b

        print(f"💰 账户 A 余额: ${bal_a:.4f} USDC")
        print(f"💰 账户 B 余额: ${bal_b:.4f} USDC")
        print(f"💰 合计: ${bal_a + bal_b:.4f} USDC")
        return True

    # ────────────────── 主循环 ──────────────────

    async def main_loop(self):
        last_balance_check: float = 0

        while self.running and self.cycle_count < MAX_CYCLES:
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
                # 刷新两个账户的 Token (每 240s)
                await self.account_a.refresh_token_if_needed(240)
                await self.account_b.refresh_token_if_needed(240)

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

    # ────────────────── 状态处理 ──────────────────

    async def _handle_idle(self):
        """IDLE: 等待 0 点差窗口开仓"""
        min_depth = ORDER_SIZE_ETH * MIN_DEPTH_MULTIPLIER

        if not self.observer.is_entry_ready(ENTRY_ZERO_SPREAD_MS, min_depth):
            return

        # 两个账户都要有下单额度
        can_a, _, _ = self.account_a.can_trade()
        can_b, _, _ = self.account_b.can_trade()
        if not can_a or not can_b:
            return

        await self._open_both()

    async def _handle_holding(self):
        """HOLDING: 等待 0 点差窗口平仓, 或超时强平"""
        # 超时强制平仓
        hold_time = time.time() - self.hold_start_time
        if hold_time > MAX_HOLD_SECONDS:
            logger.warning(f"持仓超时 ({hold_time:.1f}s > {MAX_HOLD_SECONDS}s), 强制平仓")
            await self._close_both(emergency=True)
            return

        # 平仓条件比开仓宽松: 0 差等待时间减半
        min_depth = ORDER_SIZE_ETH * MIN_DEPTH_MULTIPLIER
        exit_min_ms = ENTRY_ZERO_SPREAD_MS / 2

        if not self.observer.is_entry_ready(exit_min_ms, min_depth):
            return

        # 两个账户都要有下单额度
        can_a, _, _ = self.account_a.can_trade()
        can_b, _, _ = self.account_b.can_trade()
        if not can_a or not can_b:
            return

        await self._close_both()

    # ────────────────── 开仓 / 平仓 ──────────────────

    async def _open_both(self):
        """同时开仓: A 和 B 下反向市价单"""
        cycle_start = time.time()

        if self.current_direction == "A_LONG":
            a_side, b_side = "BUY", "SELL"
        else:
            a_side, b_side = "SELL", "BUY"

        dir_text = "A多B空" if self.current_direction == "A_LONG" else "A空B多"
        logger.info(f"开仓: {dir_text} | {ORDER_SIZE_ETH} ETH")

        # 并行下单 (asyncio.to_thread 让两个 HTTP 同时发出)
        results = await asyncio.gather(
            self.account_a.place_order_async(a_side, ORDER_SIZE_ETH),
            self.account_b.place_order_async(b_side, ORDER_SIZE_ETH),
            return_exceptions=True,
        )

        a_ok = not isinstance(results[0], Exception)
        b_ok = not isinstance(results[1], Exception)

        if a_ok and b_ok:
            # ✅ 两边都成功
            self.account_a.rate_limiter.record_order()
            self.account_b.rate_limiter.record_order()
            self.state = StrategyState.HOLDING
            self.hold_start_time = time.time()
            self.consecutive_failures = 0

            latency_ms = (time.time() - cycle_start) * 1000
            logger.info(f"开仓成功 | {dir_text} | {latency_ms:.0f}ms")

        elif a_ok and not b_ok:
            # ⚠️ A 成功 B 失败 → 立刻回撤 A
            logger.error(f"[B] 开仓失败: {results[1]}, 回撤 A...")
            self.account_a.rate_limiter.record_order()
            try:
                reverse = "SELL" if a_side == "BUY" else "BUY"
                await self.account_a.place_order_async(reverse, ORDER_SIZE_ETH)
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
                await self.account_b.place_order_async(reverse, ORDER_SIZE_ETH)
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
        """同时平仓, 成功后可触发冲刺模式连续开仓"""
        cycle_start = time.time()

        # 平仓方向: 和开仓相反
        if self.current_direction == "A_LONG":
            a_side, b_side = "SELL", "BUY"    # A 平多, B 平空
        else:
            a_side, b_side = "BUY", "SELL"    # A 平空, B 平多

        tag = " (超时强制)" if emergency else ""
        logger.info(f"平仓{tag}")

        # 并行平仓
        results = await asyncio.gather(
            self.account_a.place_order_async(a_side, ORDER_SIZE_ETH),
            self.account_b.place_order_async(b_side, ORDER_SIZE_ETH),
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

            # 记录成交量 & 延迟
            price = self.observer.current_bbo["mid_price"]
            self.pnl_tracker.record_cycle(price, ORDER_SIZE_ETH)
            latency_ms = (time.time() - cycle_start) * 1000
            self.latency_tracker.record_cycle_latency(latency_ms)
            logger.info(f"✅ 循环 {self.cycle_count} 完成 | {latency_ms:.0f}ms")

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

                min_depth = ORDER_SIZE_ETH * MIN_DEPTH_MULTIPLIER

                # 冲刺时放宽条件: 只要当前仍是 0 差 + 深度够就行
                if self.observer.is_entry_ready(0, min_depth):
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
                        logger.info(f"🔥 冲刺连续开仓 (第 {self.burst_rounds} 轮)")
                        await self._open_both()
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
        """重试平仓, 最多 3 次"""
        for attempt in range(1, 4):
            try:
                await account.place_order_async(side, ORDER_SIZE_ETH)
                account.rate_limiter.record_order()
                logger.info(f"[{name}] 重试平仓成功 (第{attempt}次)")
                return True
            except Exception as e:
                logger.error(f"[{name}] 重试平仓失败 (第{attempt}次): {e}")
                await asyncio.sleep(0.5)
        return False

    def _on_close_success(self):
        """平仓成功的公共收尾逻辑 (含重试成功)"""
        self.cycle_count += 1
        self.successful_cycles += 1
        price = self.observer.current_bbo["mid_price"]
        self.pnl_tracker.record_cycle(price, ORDER_SIZE_ETH)
        self.current_direction = (
            "A_SHORT" if self.current_direction == "A_LONG" else "A_LONG"
        )
        self.burst_rounds = 0
        self.state = StrategyState.IDLE

    # ────────────────── 辅助方法 ──────────────────

    async def _update_balances(self):
        """并行更新两个账户余额"""
        bal_a, bal_b = await asyncio.gather(
            self.account_a.get_balance_async(),
            self.account_b.get_balance_async(),
        )
        if bal_a > 0:
            self.account_a.current_balance = bal_a
        if bal_b > 0:
            self.account_b.current_balance = bal_b

    def _update_display(self):
        """刷新终端固定面板"""
        bbo = self.observer.current_bbo
        now = time.time()

        ws_age = (now - bbo["last_update"]) * 1000 if bbo["last_update"] > 0 else 0
        elapsed = now - self.start_time if self.start_time else 0
        elapsed_min = elapsed / 60

        pnl_a = self.account_a.get_pnl()
        pnl_b = self.account_b.get_pnl()
        pnl_total = pnl_a + pnl_b
        stats = self.pnl_tracker.get_stats(self.account_a, self.account_b)

        min_a, _, day_a = self.account_a.rate_limiter.get_counts()
        min_b, _, day_b = self.account_b.rate_limiter.get_counts()

        dir_text = "A多B空" if self.current_direction == "A_LONG" else "A空B多"
        mode_text = "🔥冲刺" if self.observer.mode == "burst" else "常态"
        zero_ms = self.observer.zero_spread_duration_ms

        pnl_sign = "+" if pnl_total >= 0 else ""

        lines = [
            "═" * 72,
            f"  📊 Paradex 双账户对冲套利 v1 | {self.state.value} | {mode_text}",
            "═" * 72,
            f"  💰 BTC: ${bbo['mid_price']:,.0f}  |  "
            f"价差: {bbo['spread']:.5f}%  |  0差: {zero_ms:.0f}ms",
            f"  📈 深度: 买 {bbo['bid_size']:.4f}  |  "
            f"卖 {bbo['ask_size']:.4f}  |  下次: {dir_text}",
            f"  🅰️ A: ${self.account_a.current_balance:.2f} | "
            f"PnL:{pnl_a:+.4f} | {min_a}/{MAX_ORDERS_PER_MINUTE}分 {day_a}/{MAX_ORDERS_PER_DAY}日",
            f"  🅱️ B: ${self.account_b.current_balance:.2f} | "
            f"PnL:{pnl_b:+.4f} | {min_b}/{MAX_ORDERS_PER_MINUTE}分 {day_b}/{MAX_ORDERS_PER_DAY}日",
            f"  🔄 循环: {self.cycle_count}/{MAX_CYCLES} | "
            f"成功:{self.successful_cycles} 失败:{self.failed_cycles} | 冲刺:{self.burst_rounds}轮",
            f"  💵 合计: {pnl_sign}{pnl_total:.4f} U | "
            f"量: ${stats['volume'] / 1000:.1f}K | 每万: ${stats['per_10k']:.4f}",
            f"  ⏱️  WS:{ws_age:.0f}ms | "
            f"近5:[{self.latency_tracker.format_recent()}]ms | 运行:{elapsed_min:.1f}分",
            "═" * 72,
        ]

        self.panel.update(lines)

    # ────────────────── 关闭 ──────────────────

    async def shutdown(self):
        """关闭策略, 输出最终统计"""
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
        print("📊 双账户对冲策略 - 最终统计")
        print("=" * 72)
        print(f"   循环: {self.cycle_count} "
              f"(成功: {self.successful_cycles}, 失败: {self.failed_cycles})")
        print(f"   运行: {elapsed / 60:.1f} 分钟")
        print("-" * 72)
        print("🅰️  账户 A:")
        print(f"   初始: ${self.account_a.initial_balance:.4f} → "
              f"当前: ${self.account_a.current_balance:.4f}")
        print(f"   盈亏: ${self.account_a.get_pnl():+.4f} USDC | "
              f"下单: {self.account_a.order_count} 单")
        print("🅱️  账户 B:")
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
            await self.account_a.paradex.ws_client.close()
        except Exception:
            pass

        print("👋 已退出")


# ==================== 入口 ====================
async def main():
    controller = DualAccountController()
    await controller.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️  已中断")
    except Exception as e:
        print(f"❌ 错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
