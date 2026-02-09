# Paradex BTC 秒开关策略

纯 Python 后台 BTC 秒开秒关策略，使用 Paradex Retail Profile 免费交易。

## 策略逻辑

1. 监控 BTC-USD-PERP 价差
2. 当价差 ≤ 0.0006% 时：
   - 开仓 (市价买入 0.006 BTC)
   - 立即平仓 (市价卖出 0.006 BTC)
3. 每个循环间隔 1 秒
4. 完成 500 个循环后自动停止

## 特点

- ✅ **免手续费**: 使用 Retail Profile，0% maker/taker 费用
- ✅ **纯后台运行**: 无需浏览器，无需 Tampermonkey
- ✅ **L2-Only 认证**: 只需 Starknet 私钥，更安全
- ⚠️ **500ms 延迟**: Retail 模式有 speed bump

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置 API 密钥

编辑 `config.py`，填入你的 L2 地址和私钥：

```python
L2_ADDRESS = "0x你的L2地址"
L2_PRIVATE_KEY = "0x你的L2私钥"
```

**获取方式**: 在 Paradex 网页端导出 Subkey

### 3. 运行

双击 `启动.bat` 或命令行运行：

```bash
python scalper.py
```

## 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `ORDER_SIZE_BTC` | 0.006 | 每单大小 |
| `MAX_SPREAD_PERCENT` | 0.0006 | 价差阈值 (%) |
| `MAX_CYCLES` | 500 | 最大循环次数 |
| `CYCLE_INTERVAL_SEC` | 1.0 | 循环间隔 (秒) |

## 紧急停止

在脚本目录创建名为 `STOP` 的文件即可停止运行。

## 日志

运行日志保存在 `scalper.log`。
