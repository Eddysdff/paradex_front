# Paradex 双账户可选多币种对冲脚本

双账户 RPI 负点差对冲套利，零手续费刷量。

## 原理

Paradex 的 Retail Profile 提供 0% maker/taker 费率。当 orderbook 出现 0 点差时，做市商的 RPI (Retail Price Improvement) 会提供比可见盘口更优的成交价，产生"负点差"。

本策略用两个账户同时开反向仓位（一多一空），利用 RPI 实现超低磨损甚至正收益的刷量循环。

## 工作流程

```
监控 BBO → 检测 0 差窗口 → 动态算单量 → 双账户反向开仓
    ↑                                          ↓
  交替方向 ← 回到 IDLE ← 检测 0 差窗口 ← 双账户同时平仓
```

- **动态单量**：根据盘口薄边深度自动调整，不穿透 orderbook
- **冲刺模式**：检测到持续 0 差 + 厚深度时自动加速循环
- **多币种**：启动时选择 BTC / ETH / SOL

## 限制

| 约束 | 值 |
|------|------|
| Retail 费率 | 0% maker / 0% taker |
| Speed Bump | ~500ms |
| 下单频率 | 30/min · 300/hr · 1000/day (每账户) |

## 快速开始

```bash
# 安装
pip install -r requirements.txt

# 配置密钥
# 编辑 config.py，填入两个账户的 L2 地址和私钥

# 运行
python3 dual_scalper.py

# 指定币种跳过菜单
python3 dual_scalper.py --coin ETH
```

## 文件结构

| 文件 | 用途 |
|------|------|
| `dual_scalper.py` | 主策略脚本 |
| `config.py` | 所有配置参数 |
| `bbo_analysis.py` | BBO 数据离线分析 |
| `scalper.py` | 旧版单账户脚本 (参考) |

## 紧急停止

创建 `STOP` 文件即可停止：

```bash
touch STOP
```

## 数据分析

脚本运行时自动记录 BBO 到 `bbo_data/` 目录，用分析脚本查看 0 差规律：

```bash
python3 bbo_analysis.py bbo_data/2026-02-09.csv
```
