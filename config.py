# Paradex BTC 秒开关配置

import os

# ==================== API 配置 ====================
# Paradex 环境
PARADEX_ENV = "MAINNET"  # MAINNET 或 TESTNET

# L2 认证 - 直接填写密钥
L2_ADDRESS = ""
L2_PRIVATE_KEY = ""

# Paradex API URLs
API_BASE_URL = "https://api.prod.paradex.trade"
WS_URL = "wss://ws.api.prod.paradex.trade/v1"

# ==================== 交易配置 ====================
MARKET = "ETH-USD-PERP"

# 每单大小 (根据 MARKET 调整)
# BTC-USD-PERP: 建议 0.006~0.01
# ETH-USD-PERP: 建议 0.05~0.2
ORDER_SIZE_ETH = 0.1

# 价差阈值 (百分比)
# 当价差 <= 此值时触发开仓
MAX_SPREAD_PERCENT = 0.001  # 0.0006%

# 最大循环次数 (一开一关为一个循环)
# 每循环下2单，500循环 = 1000单 = Retail 24h 上限
MAX_CYCLES = 500

# 循环间隔 (秒)
# 考虑到 500ms speed bump，实际每单延迟约 1.5s
CYCLE_INTERVAL_SEC = 1.0

# ==================== 日志配置 ====================
LOG_FILE = "scalper.log"
LOG_LEVEL = "INFO"

# ==================== 安全配置 ====================
# 最大连续失败次数 (超过则暂停)
MAX_CONSECUTIVE_FAILURES = 5

# 紧急停止文件 (存在此文件则停止运行)
EMERGENCY_STOP_FILE = "STOP"

# ==================== 双账户对冲配置 ====================
# 账户 A (L2 Subkey)
ACCOUNT_A_L2_ADDRESS = ""
ACCOUNT_A_L2_PRIVATE_KEY = ""

# 账户 B (L2 Subkey)
ACCOUNT_B_L2_ADDRESS = ""
ACCOUNT_B_L2_PRIVATE_KEY = ""

# ==================== 对冲策略参数 ====================
# 视为 "0 点差" 的阈值 (百分比)
# Paradex 显示精度为 0.001%，低于此值视为 0 点差
ZERO_SPREAD_THRESHOLD = 0.001

# 0 点差需持续多久才触发开/平仓 (毫秒)
ENTRY_ZERO_SPREAD_MS = 300

# 盘口深度需为单量的多少倍才允许交易
MIN_DEPTH_MULTIPLIER = 2.0

# 单轮最长持仓时间 (秒)，超时强制平仓
MAX_HOLD_SECONDS = 30

# ==================== 冲刺模式配置 ====================
# 0 点差持续多久判定为冲刺窗口 (毫秒)
BURST_ZERO_SPREAD_MS = 2000

# 冲刺模式最低双边深度
# BTC: 建议 0.03  |  ETH: 建议 0.5
BURST_MIN_DEPTH_ETH = 1

# 每次冲刺窗口内最多连续循环数
MAX_ROUNDS_PER_BURST = 5

# ==================== Telegram 通知配置 ====================
# Telegram Bot Token (通过 @BotFather 获取)
TG_BOT_TOKEN = ""

# 你的 Chat ID (通过 @userinfobot 获取)
TG_CHAT_ID = ""

# 每隔多少个循环发送一次进度报告
TG_NOTIFY_INTERVAL = 10

# 是否启用 Telegram 通知 (填好 Token 和 Chat ID 后自动启用)
TG_ENABLED = True
