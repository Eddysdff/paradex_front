import os

# ─── API ───
PARADEX_ENV = "MAINNET"
L2_ADDRESS = ""
L2_PRIVATE_KEY = ""
API_BASE_URL = "https://api.prod.paradex.trade"
WS_URL = "wss://ws.api.prod.paradex.trade/v1"

# ─── 币种预设 (启动时选择) ───
COIN_PRESETS = {
    "BTC": {
        "market": "BTC-USD-PERP",
        "order_size": 0.004,       # 每单量 (建议 0.004~0.01)
        "burst_min_depth": 0.03,   # 冲刺模式最低深度
        "min_order_size": 0.001,   # 最小下单量
        "size_decimals": 3,        # 精度位数
    },
    "ETH": {
        "market": "ETH-USD-PERP",
        "order_size": 0.1,         # 每单量 (建议 0.05~0.2)
        "burst_min_depth": 1,      # 冲刺模式最低深度
        "min_order_size": 0.01,    # 最小下单量
        "size_decimals": 2,        # 精度位数
    },
    "SOL": {
        "market": "SOL-USD-PERP",
        "order_size": 2.0,         # 每单量 (建议 1~5)
        "burst_min_depth": 20,     # 冲刺模式最低深度
        "min_order_size": 0.1,     # 最小下单量
        "size_decimals": 1,        # 精度位数
    },
}

# ─── 交易 ───
DEFAULT_COIN = "ETH"                                       # 默认币种
MARKET = COIN_PRESETS[DEFAULT_COIN]["market"]               # 自动设置
ORDER_SIZE = COIN_PRESETS[DEFAULT_COIN]["order_size"]       # 自动设置
MIN_ORDER_SIZE = COIN_PRESETS[DEFAULT_COIN]["min_order_size"]
SIZE_DECIMALS = COIN_PRESETS[DEFAULT_COIN]["size_decimals"]
MAX_SPREAD_PERCENT = 0.0005    # 价差阈值 (%)
MAX_CYCLES = 500               # 最大循环次数 (开+平=1循环, 500循环=1000单)
CYCLE_INTERVAL_SEC = 1.0       # 循环间隔 (秒)

# ─── 日志 ───
LOG_FILE = "scalper.log"
LOG_LEVEL = "INFO"

# ─── 安全 ───
MAX_CONSECUTIVE_FAILURES = 5   # 连续失败几次后暂停
EMERGENCY_STOP_FILE = "STOP"   # 创建此文件可紧急停止

# ─── 双账户密钥 ───
ACCOUNT_A_L2_ADDRESS = ""
ACCOUNT_A_L2_PRIVATE_KEY = ""
ACCOUNT_B_L2_ADDRESS = ""
ACCOUNT_B_L2_PRIVATE_KEY = ""

# ─── 策略参数 ───
ZERO_SPREAD_THRESHOLD = 0.0005  # 低于此值视为 0 点差 (%)
ENTRY_ZERO_SPREAD_MS = 300     # 0 差持续多久才开/平仓 (ms)
DEPTH_SAFETY_FACTOR = 0.8      # 取薄边深度的 80% 作为单量
MAX_HOLD_SECONDS = 600         # 持仓超时强平 (秒)

# ─── 冲刺模式 (0差持续+深度厚时自动加速) ───
BURST_ZERO_SPREAD_MS = 2000    # 0 差持续多久触发冲刺 (ms)
BURST_MIN_DEPTH = COIN_PRESETS[DEFAULT_COIN]["burst_min_depth"]
MAX_ROUNDS_PER_BURST = 5       # 每次冲刺最多连续几轮

# ─── Telegram 通知 ───
TG_BOT_TOKEN = ""              # @BotFather 获取
TG_CHAT_ID = ""                # @userinfobot 获取
TG_NOTIFY_INTERVAL = 10        # 每几个循环推一次
TG_ENABLED = True              # 总开关

# ─── BBO 数据记录 (离线分析用) ───
BBO_RECORD_ENABLED = True      # 是否记录
BBO_RECORD_DIR = "bbo_data"    # 存储目录
BBO_RECORD_BUFFER_SIZE = 100   # 缓冲条数 (越大性能越好, 断电丢越多)
