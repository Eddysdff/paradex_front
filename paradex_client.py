"""
Paradex API 客户端封装 (L2-Only 认证)

功能:
1. 使用 L2 私钥进行认证 (ParadexSubkey)
2. 获取 BBO 数据
3. 下市价单
4. 获取持仓信息
"""

import asyncio
import logging
from decimal import Decimal
from typing import Optional, Dict, Any

from paradex_py import ParadexSubkey
from paradex_py.environment import Environment
from paradex_py.common.order import Order, OrderType, OrderSide

from config import (
    PARADEX_ENV, L2_ADDRESS, L2_PRIVATE_KEY,
    MARKET, ORDER_SIZE_BTC
)

logger = logging.getLogger(__name__)


class ParadexClient:
    """Paradex API 客户端 (L2-Only 认证)"""
    
    def __init__(self):
        self.paradex: Optional[ParadexSubkey] = None
        self.connected = False
        self.use_interactive = True
        self.last_auth_time = 0  # 跟踪上次认证时间
        
    async def connect(self, use_interactive_token: bool = True) -> bool:
        """连接并初始化 Paradex 客户端
        
        Args:
            use_interactive_token: 使用 interactive token 实现免费交易 (有 500ms 延迟)
        """
        try:
            # 确定环境 - paradex-py 使用字符串: 'prod', 'testnet', 'nightly'
            env = "prod" if PARADEX_ENV == "MAINNET" else "testnet"
            
            logger.info(f"正在连接 Paradex ({env})...")
            logger.info(f"L2 地址: {L2_ADDRESS[:10]}...{L2_ADDRESS[-6:]}")
            
            if use_interactive_token:
                logger.info("🆓 使用 Interactive Token (免费交易, 500ms 延迟)")
            
            # 使用 L2-Only 认证 (Subkey)
            # 注意: 设置 auto_auth=False，我们手动调用带 token_usage 参数的 auth
            self.paradex = ParadexSubkey(
                env=env,
                l2_private_key=L2_PRIVATE_KEY,
                l2_address=L2_ADDRESS
            )
            
            # 初始化账户 (这会调用默认的 auth)
            await self.paradex.init_account()
            
            # 如果需要 interactive token，重新认证
            if use_interactive_token:
                self._auth_with_interactive_token()
            
            # 验证连接 - 获取账户信息
            account_info = self.paradex.api_client.fetch_account_info()
            logger.info(f"✅ 连接成功! 账户: {account_info.get('account', 'N/A')}")
            
            # 获取账户 profile 确认是否是 Retail
            try:
                profile = self.paradex.api_client.fetch_account_profile()
                fee_tier = profile.get("fee_tier", "unknown")
                logger.info(f"📊 账户 Profile: {fee_tier}")
            except:
                pass
            
            self.connected = True
            return True
            
        except Exception as e:
            logger.error(f"❌ 连接失败: {e}")
            self.connected = False
            return False
    
    def _auth_with_interactive_token(self):
        """使用 interactive token 重新认证 (免费交易)"""
        import time as time_module
        from paradex_py.api.models import AuthSchema
        
        if not self.paradex or not self.paradex.account:
            raise RuntimeError("Account not initialized")
        
        api_client = self.paradex.api_client
        account = self.paradex.account
        
        # 构建带 token_usage=interactive 参数的认证请求
        headers = account.auth_headers()
        
        # 关键: 添加 token_usage=interactive 查询参数
        path = f"auth/{hex(account.l2_public_key)}?token_usage=interactive"
        
        logger.info(f"🔑 发送 interactive token 认证请求...")
        res = api_client.post(api_url=api_client.api_url, path=path, headers=headers)
        
        # 解析响应
        data = AuthSchema().load(res, unknown="exclude", partial=True)
        api_client.auth_timestamp = int(time_module.time())
        account.set_jwt_token(data.jwt_token)
        api_client.client.headers.update({"Authorization": f"Bearer {data.jwt_token}"})
        
        # 记录认证时间
        self.last_auth_time = time_module.time()
        
        logger.info("✅ Interactive Token 认证成功!")
    
    def refresh_token_if_needed(self, max_age_seconds: int = 240) -> bool:
        """检查并刷新 token (每4分钟刷新一次，token 5分钟过期)
        
        Args:
            max_age_seconds: 最大 token 年龄（秒），默认240秒（4分钟）
            
        Returns:
            是否进行了刷新
        """
        import time as time_module
        
        if not self.use_interactive:
            return False
        
        elapsed = time_module.time() - self.last_auth_time
        
        if elapsed >= max_age_seconds:
            logger.info(f"🔄 Token 已使用 {elapsed:.0f}s，正在刷新...")
            try:
                self._auth_with_interactive_token()
                return True
            except Exception as e:
                logger.error(f"❌ Token 刷新失败: {e}")
                return False
        
        return False
    
    def get_bbo(self) -> Dict[str, Any]:
        """获取 BTC-USD-PERP 的买一卖一价格
        
        Returns:
            {
                "bid": float,      # 买一价
                "ask": float,      # 卖一价
                "bid_size": float, # 买一量
                "ask_size": float, # 卖一量
                "spread": float,   # 价差百分比
            }
        """
        if not self.paradex:
            raise RuntimeError("Client not connected")
        
        try:
            bbo = self.paradex.api_client.fetch_bbo(market=MARKET)
            
            bid = float(bbo.get("bid", 0))
            ask = float(bbo.get("ask", 0))
            bid_size = float(bbo.get("bid_size", 0))
            ask_size = float(bbo.get("ask_size", 0))
            
            # 计算价差百分比
            spread = 0.0
            if bid > 0 and ask > 0:
                mid_price = (bid + ask) / 2
                spread = ((ask - bid) / mid_price) * 100
            
            return {
                "bid": bid,
                "ask": ask,
                "bid_size": bid_size,
                "ask_size": ask_size,
                "spread": spread,
                "mid_price": (bid + ask) / 2 if bid > 0 and ask > 0 else 0
            }
            
        except Exception as e:
            logger.error(f"获取 BBO 失败: {e}")
            raise
    
    def place_market_order(self, side: str, size: float = ORDER_SIZE_BTC) -> Dict[str, Any]:
        """下市价单
        
        Args:
            side: "BUY" 或 "SELL"
            size: 下单数量 (BTC)
            
        Returns:
            订单响应
        """
        if not self.paradex:
            raise RuntimeError("Client not connected")
        
        try:
            order_side = OrderSide.Buy if side.upper() == "BUY" else OrderSide.Sell
            
            order = Order(
                market=MARKET,
                order_type=OrderType.Market,
                order_side=order_side,
                size=Decimal(str(size)),
            )
            
            response = self.paradex.api_client.submit_order(order=order)
            
            order_id = response.get("id", "N/A")
            status = response.get("status", "N/A")
            logger.info(f"{'🟢 买入' if side.upper() == 'BUY' else '🔴 卖出'} {size} BTC | 订单: {order_id} | 状态: {status}")
            
            return response
            
        except Exception as e:
            logger.error(f"下单失败 ({side} {size} BTC): {e}")
            raise
    
    def get_position(self) -> Dict[str, Any]:
        """获取 BTC-USD-PERP 持仓
        
        Returns:
            {
                "size": float,        # 持仓数量 (正=多头, 负=空头)
                "entry_price": float, # 入场均价
                "unrealized_pnl": float, # 未实现盈亏
            }
        """
        if not self.paradex:
            raise RuntimeError("Client not connected")
        
        try:
            positions = self.paradex.api_client.fetch_positions()
            
            for pos in positions.get("results", []):
                if pos.get("market") == MARKET:
                    return {
                        "size": float(pos.get("size", 0)),
                        "entry_price": float(pos.get("average_entry_price", 0)),
                        "unrealized_pnl": float(pos.get("unrealized_pnl", 0)),
                    }
            
            # 无持仓
            return {"size": 0.0, "entry_price": 0.0, "unrealized_pnl": 0.0}
            
        except Exception as e:
            logger.error(f"获取持仓失败: {e}")
            raise
    
    def get_account_balance(self) -> float:
        """获取账户 USDC 余额"""
        if not self.paradex:
            raise RuntimeError("Client not connected")
        
        try:
            balances = self.paradex.api_client.fetch_balances()
            
            for bal in balances.get("results", []):
                if bal.get("token") == "USDC":
                    return float(bal.get("size", 0))
            
            return 0.0
            
        except Exception as e:
            logger.error(f"获取余额失败: {e}")
            raise
    
    async def close(self):
        """关闭客户端"""
        if self.paradex:
            try:
                await self.paradex.close()
            except:
                pass
        self.connected = False
        logger.info("客户端已关闭")
