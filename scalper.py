"""
Paradex BTC 秒开关脚本 v6 - 双向智能版 (WebSocket 实时推送)

特点:
1. WebSocket 实时接收 BBO 价格 (~10-50ms 延迟)
2. 双向开平仓：根据买一/卖一厚度决定方向
3. 通过账户余额变化计算真实盈亏
4. 速率限制：30单/分钟, 300单/小时, 1000单/24小时
5. 延迟监控：实时延迟 + 近5单延迟统计
6. 固定面板显示，不滚动
"""

import asyncio
import logging
import time
import os
import sys
from collections import deque
from typing import Optional, Dict, Any

from config import (
    ORDER_SIZE_BTC, MAX_SPREAD_PERCENT, MAX_CYCLES,
    CYCLE_INTERVAL_SEC, LOG_FILE, LOG_LEVEL,
    MAX_CONSECUTIVE_FAILURES, EMERGENCY_STOP_FILE,
    L2_ADDRESS, L2_PRIVATE_KEY, PARADEX_ENV
)

from paradex_py import ParadexSubkey
from paradex_py.api.ws_client import ParadexWebsocketChannel
from paradex_py.common.order import Order, OrderType, OrderSide

# ==================== 日志配置 ====================
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


# ==================== 配置 ====================
MARKET = "BTC-USD-PERP"
MAX_ORDERS_PER_MINUTE = 30
MAX_ORDERS_PER_HOUR = 300
MAX_ORDERS_PER_DAY = 1000
MIN_DEPTH_BTC = 0.006


class RateLimiter:
    """三级速率限制器"""
    def __init__(self, per_minute: int, per_hour: int, per_day: int):
        self.per_minute = per_minute
        self.per_hour = per_hour
        self.per_day = per_day
        self.minute_orders = deque()
        self.hour_orders = deque()
        self.day_orders = deque()
    
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


class LatencyTracker:
    """延迟追踪器"""
    def __init__(self, max_records: int = 5):
        self.recent_latencies = deque(maxlen=max_records)
        self.current_ws_latency = 0.0
    
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
            "ws": self.current_ws_latency
        }
    
    def format_recent(self) -> str:
        if not self.recent_latencies:
            return "-"
        return "/".join([f"{l:.0f}" for l in self.recent_latencies])


class BalancePnLTracker:
    """盈亏追踪器"""
    def __init__(self):
        self.initial_balance = 0.0
        self.current_balance = 0.0
        self.total_volume_usd = 0.0
        self.last_valid_balance = 0.0
        self.long_count = 0
        self.short_count = 0
    
    def set_initial_balance(self, balance: float):
        if balance <= 0:
            return False
        self.initial_balance = balance
        self.current_balance = balance
        self.last_valid_balance = balance
        return True
    
    def update_balance(self, balance: float) -> bool:
        if balance <= 0:
            return False
        self.current_balance = balance
        self.last_valid_balance = balance
        return True
    
    def record_cycle_volume(self, price: float, size: float, direction: str):
        self.total_volume_usd += price * size * 2
        if direction == "LONG":
            self.long_count += 1
        else:
            self.short_count += 1
    
    def get_real_pnl(self) -> float:
        return self.current_balance - self.initial_balance
    
    def get_stats(self) -> dict:
        real_pnl = self.get_real_pnl()
        if self.total_volume_usd == 0:
            return {
                "pnl": real_pnl, "volume": 0,
                "per_10k": 0, "per_100k": 0, "per_million": 0,
                "initial": self.initial_balance, "current": self.current_balance,
                "long": self.long_count, "short": self.short_count,
            }
        cost_rate = abs(real_pnl) / self.total_volume_usd
        return {
            "pnl": real_pnl, "volume": self.total_volume_usd,
            "per_10k": cost_rate * 10000,
            "per_100k": cost_rate * 100000,
            "per_million": cost_rate * 1000000,
            "initial": self.initial_balance, "current": self.current_balance,
            "long": self.long_count, "short": self.short_count,
        }


class FixedPanel:
    """固定面板显示器 - 不滚动"""
    
    PANEL_LINES = 10  # 面板行数
    
    def __init__(self):
        self.initialized = False
    
    def init_panel(self):
        """初始化面板（打印空行占位）"""
        if not self.initialized:
            print("\n" * self.PANEL_LINES, end="")
            self.initialized = True
    
    def update(self, lines: list[str]):
        """更新整个面板"""
        # 移动光标到面板顶部
        sys.stdout.write(f"\033[{self.PANEL_LINES}A")  # 向上移动N行
        sys.stdout.write("\033[J")  # 清除从光标到屏幕底部
        
        # 打印所有行
        for i, line in enumerate(lines):
            if i < self.PANEL_LINES:
                print(line)
        
        # 补足剩余行
        for _ in range(self.PANEL_LINES - len(lines)):
            print()
        
        sys.stdout.flush()


class WebSocketScalper:
    """WebSocket 实时价格的 BTC 双向秒开关策略"""
    
    def __init__(self):
        self.paradex: Optional[ParadexSubkey] = None
        self.rate_limiter = RateLimiter(MAX_ORDERS_PER_MINUTE, MAX_ORDERS_PER_HOUR, MAX_ORDERS_PER_DAY)
        self.pnl_tracker = BalancePnLTracker()
        self.latency_tracker = LatencyTracker()
        self.panel = FixedPanel()
        
        self.cycle_count = 0
        self.successful_cycles = 0
        self.failed_cycles = 0
        self.consecutive_failures = 0
        self.running = False
        self.start_time = None
        self.last_auth_time = 0
        self.last_direction = "-"
        
        self.current_bbo: Dict[str, Any] = {
            "bid": 0.0, "ask": 0.0,
            "bid_size": 0.0, "ask_size": 0.0,
            "spread": 100.0, "mid_price": 0.0,
            "last_update": 0,
        }
        
        self.recent_cycle_times = deque(maxlen=5)
    
    def update_display(self, status: str = "监控中"):
        """更新固定面板显示"""
        bbo = self.current_bbo
        stats = self.pnl_tracker.get_stats()
        latency = self.latency_tracker.get_stats()
        min_o, hr_o, day_o = self.rate_limiter.get_counts()
        
        now = time.time()
        ws_age = (now - bbo["last_update"]) * 1000 if bbo["last_update"] > 0 else 0
        elapsed = now - self.start_time if self.start_time else 0
        elapsed_min = elapsed / 60
        
        direction = "🟢多" if bbo["bid_size"] >= bbo["ask_size"] else "🔴空"
        pnl_color = "+" if stats['pnl'] >= 0 else ""
        
        lines = [
            "═" * 70,
            f"  📊 Paradex BTC 双向秒开关 v6 | 状态: {status}",
            "═" * 70,
            f"  💰 价格: ${bbo['mid_price']:.0f}  |  价差: {bbo['spread']:.5f}%  |  方向: {direction}",
            f"  📈 深度: 买一 {bbo['bid_size']:.4f} BTC  |  卖一 {bbo['ask_size']:.4f} BTC",
            f"  🔄 循环: {self.cycle_count}/{MAX_CYCLES} (多:{stats['long']} 空:{stats['short']})  |  上次: {self.last_direction}",
            f"  💵 盈亏: {pnl_color}{stats['pnl']:.4f} U  |  成交量: ${stats['volume']/1000:.1f}K",
            f"  🚦 限速: {min_o}/{MAX_ORDERS_PER_MINUTE}分 | {hr_o}/{MAX_ORDERS_PER_HOUR}时 | {day_o}/{MAX_ORDERS_PER_DAY}日",
            f"  ⏱️ 延迟: WS {ws_age:.0f}ms  |  近5单: [{self.latency_tracker.format_recent()}]ms",
            f"  ⏰ 运行: {elapsed_min:.1f}分钟  |  磨损: ¥{stats['per_10k']:.2f}/万",
        ]
        
        self.panel.update(lines)
    
    async def on_bbo_update(self, channel, message):
        try:
            data = message.get("params", {}).get("data", {})
            if data:
                bid = float(data.get("bid", 0))
                ask = float(data.get("ask", 0))
                bid_size = float(data.get("bid_size", 0))
                ask_size = float(data.get("ask_size", 0))
                
                if bid > 0 and ask > 0:
                    mid = (bid + ask) / 2
                    spread_pct = (ask - bid) / mid * 100
                    
                    self.current_bbo = {
                        "bid": bid, "ask": ask,
                        "bid_size": bid_size, "ask_size": ask_size,
                        "spread": spread_pct, "mid_price": mid,
                        "last_update": time.time(),
                    }
        except Exception as e:
            logger.error(f"BBO 解析错误: {e}")
    
    async def connect(self) -> bool:
        try:
            env = "prod" if PARADEX_ENV == "MAINNET" else "testnet"
            print(f"🔌 连接 Paradex ({env})...")
            
            self.paradex = ParadexSubkey(
                env=env,
                l2_private_key=L2_PRIVATE_KEY,
                l2_address=L2_ADDRESS
            )
            
            await self.paradex.init_account()
            await self._auth_with_interactive_token()
            
            print("📡 连接 WebSocket...")
            await self.paradex.ws_client.connect()
            
            print(f"📊 订阅 {MARKET} BBO...")
            await self.paradex.ws_client.subscribe(
                ParadexWebsocketChannel.BBO,
                callback=self.on_bbo_update,
                params={"market": MARKET}
            )
            
            print("⏳ 等待 BBO 数据...")
            for _ in range(50):
                await asyncio.sleep(0.1)
                if self.current_bbo["last_update"] > 0:
                    print(f"✅ 收到 BBO: ${self.current_bbo['mid_price']:.0f}")
                    break
            
            return True
        except Exception as e:
            print(f"❌ 连接失败: {e}")
            return False
    
    async def _auth_with_interactive_token(self):
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
        print("🆓 Interactive Token 获取成功")
    
    async def refresh_token_if_needed(self, max_age: int = 240):
        elapsed = time.time() - self.last_auth_time
        if elapsed >= max_age:
            await self._auth_with_interactive_token()
    
    def get_account_balance(self) -> float:
        try:
            summary = self.paradex.api_client.fetch_account_summary()
            if hasattr(summary, 'account_value') and summary.account_value:
                return float(summary.account_value)
            if hasattr(summary, 'equity') and summary.equity:
                return float(summary.equity)
            if hasattr(summary, 'free_collateral') and summary.free_collateral:
                return float(summary.free_collateral)
            return -1
        except:
            return -1
    
    def place_market_order(self, side: str, size: float) -> dict:
        from decimal import Decimal
        order = Order(
            market=MARKET,
            order_type=OrderType.Market,
            order_side=OrderSide.Buy if side == "BUY" else OrderSide.Sell,
            size=Decimal(str(size))
        )
        return self.paradex.api_client.submit_order(order)
    
    def decide_direction(self, bid_size: float, ask_size: float) -> str:
        return "LONG" if bid_size >= ask_size else "SHORT"
    
    async def start(self):
        print("=" * 70)
        print("🚀 Paradex BTC 秒开关策略 v6 - 双向智能版")
        print("=" * 70)
        print(f"📊 配置: {ORDER_SIZE_BTC} BTC | 价差≤{MAX_SPREAD_PERCENT}%")
        print(f"🚦 限速: {MAX_ORDERS_PER_MINUTE}/分 | {MAX_ORDERS_PER_HOUR}/时 | {MAX_ORDERS_PER_DAY}/24h")
        print("=" * 70)
        
        if not L2_ADDRESS or not L2_PRIVATE_KEY:
            print("❌ 未配置 L2 密钥!")
            return
        
        if not await self.connect():
            return
        
        initial_balance = self.get_account_balance()
        if initial_balance <= 0:
            print(f"❌ 获取余额失败: {initial_balance}")
            return
        if not self.pnl_tracker.set_initial_balance(initial_balance):
            print("❌ 设置初始余额失败")
            return
        print(f"💰 初始余额: ${initial_balance:.4f} USDC")
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
    
    async def main_loop(self):
        last_balance_check = 0
        
        while self.running and self.cycle_count < MAX_CYCLES:
            if os.path.exists(EMERGENCY_STOP_FILE):
                break
            if self.consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                break
            
            try:
                await self.refresh_token_if_needed(240)
                
                now = time.time()
                if now - last_balance_check > 10:
                    balance = self.get_account_balance()
                    if balance > 0:
                        self.pnl_tracker.update_balance(balance)
                        last_balance_check = now
                
                can_trade, wait_sec, limit_reason = self.rate_limiter.can_place_order()
                
                bbo = self.current_bbo
                spread = bbo["spread"]
                price = bbo["mid_price"]
                age = now - bbo["last_update"]
                self.latency_tracker.update_ws_latency(age * 1000)
                
                # 更新显示
                if can_trade:
                    self.update_display("监控中")
                else:
                    self.update_display(f"{limit_reason}限速 {wait_sec:.0f}s")
                
                if not can_trade:
                    await asyncio.sleep(min(wait_sec, 2))
                    continue
                
                if age > 1.0:
                    await asyncio.sleep(0.05)
                    continue
                
                if spread <= MAX_SPREAD_PERCENT:
                    bid_size = bbo["bid_size"]
                    ask_size = bbo["ask_size"]
                    if bid_size < MIN_DEPTH_BTC or ask_size < MIN_DEPTH_BTC:
                        await asyncio.sleep(0.05)
                        continue
                    
                    direction = self.decide_direction(bid_size, ask_size)
                    
                    cycle_start = time.time()
                    success = await self.execute_cycle(price, direction)
                    cycle_time = time.time() - cycle_start
                    cycle_latency_ms = cycle_time * 1000
                    
                    if success:
                        self.successful_cycles += 1
                        self.consecutive_failures = 0
                        self.cycle_count += 1
                        self.recent_cycle_times.append(cycle_time)
                        self.latency_tracker.record_cycle_latency(cycle_latency_ms)
                        self.last_direction = "多" if direction == "LONG" else "空"
                        
                        await asyncio.sleep(0.2)
                        balance = self.get_account_balance()
                        if balance > 0:
                            self.pnl_tracker.update_balance(balance)
                            last_balance_check = time.time()
                        
                        logger.info(f"循环 {self.cycle_count} | {self.last_direction} | {cycle_latency_ms:.0f}ms")
                    else:
                        self.failed_cycles += 1
                        self.consecutive_failures += 1
                
            except Exception as e:
                logger.error(f"错误: {e}")
                self.consecutive_failures += 1
            
            await asyncio.sleep(0.05)
    
    async def execute_cycle(self, price: float, direction: str) -> bool:
        try:
            if direction == "LONG":
                self.place_market_order("BUY", ORDER_SIZE_BTC)
                self.rate_limiter.record_order()
                await asyncio.sleep(0.1)
                self.place_market_order("SELL", ORDER_SIZE_BTC)
                self.rate_limiter.record_order()
            else:
                self.place_market_order("SELL", ORDER_SIZE_BTC)
                self.rate_limiter.record_order()
                await asyncio.sleep(0.1)
                self.place_market_order("BUY", ORDER_SIZE_BTC)
                self.rate_limiter.record_order()
            
            self.pnl_tracker.record_cycle_volume(price, ORDER_SIZE_BTC, direction)
            return True
        except Exception as e:
            logger.error(f"循环失败: {e}")
            return False
    
    async def shutdown(self):
        self.running = False
        
        final_balance = self.get_account_balance()
        if final_balance > 0:
            self.pnl_tracker.update_balance(final_balance)
        
        elapsed = time.time() - self.start_time if self.start_time else 0
        stats = self.pnl_tracker.get_stats()
        latency = self.latency_tracker.get_stats()
        
        # 清屏后打印最终统计
        print("\n" * 2)
        print("=" * 70)
        print("📊 策略统计")
        print("=" * 70)
        print(f"   循环: {self.cycle_count} (成功: {self.successful_cycles}, 失败: {self.failed_cycles})")
        print(f"   方向: 多{stats['long']}次 | 空{stats['short']}次")
        print(f"   运行: {elapsed/60:.1f} 分钟")
        print("-" * 70)
        print(f"💰 余额:")
        print(f"   初始: ${stats['initial']:.4f} USDC")
        print(f"   当前: ${stats['current']:.4f} USDC")
        print(f"   盈亏: ${stats['pnl']:+.4f} USDC")
        print("-" * 70)
        print(f"📈 交易量: ${stats['volume']:,.2f} USD")
        print("-" * 70)
        if latency["recent"]:
            print(f"⏱️ 延迟: 平均 {latency['avg']:.0f}ms | 最小 {latency['min']:.0f}ms | 最大 {latency['max']:.0f}ms")
        print("=" * 70)
        
        try:
            await self.paradex.ws_client.close()
        except:
            pass
        
        print("👋 已退出")


async def main():
    scalper = WebSocketScalper()
    await scalper.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️ 已中断")
    except Exception as e:
        print(f"❌ 错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
