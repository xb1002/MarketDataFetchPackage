# MarketDataFetchPackage
这尝试通过codex写一个下载各个交易所市场数据的包。

## Binance U 本位合约示例

Binance U 本位永续实现位于 `market_data_fetch.exchanges.binance` 模块中，导入该模块即可完成注册：

```python
import market_data_fetch.exchanges.binance  # 注册 Binance 数据源

from market_data_fetch import Exchange, HistoricalWindow, Interval, MarketDataClient, Symbol

client = MarketDataClient()
symbol = Symbol("BTC", "USDT")
window = HistoricalWindow(symbol=symbol, interval=Interval.MINUTE_1, limit=200)

klines = client.get_price_klines(Exchange.BINANCE, window)
last_price, price_ts = client.get_latest_price(Exchange.BINANCE, symbol)  # 成交价
mark_price = client.get_latest_mark_price(Exchange.BINANCE, symbol)  # 标记价格 tuple

# K 线返回 List[Tuple]，顺序为 (open_time_ms, open, high, low, close, volume)
open_time_ms, open_price, *_ = klines[0]

# 其他返回结构：
# - Funding 历史/最新值: (funding_time_ms, funding_rate)
# - Index price 最新值: (index_price, timestamp_ms)
# - Premium index 最新值: (premium_index_value, timestamp_ms)
# - Mark price 快照: (mark_price, index_price, last_funding_rate, next_funding_time_ms)
# - Open interest: (timestamp_ms, value)
```

示例中演示了如何下载历史 K 线以及分别获取最新成交价与最新 Mark Price。其他如指数 K 线、标记价格 K 线、溢价指数、资金费率历史与未平仓量均通过同一个 `MarketDataClient` 入口暴露。

## Bybit U 本位合约示例

Bybit 的实现位于 `market_data_fetch.exchanges.bybit` 模块。导入后同样会自动注册数据源：

```python
import market_data_fetch.exchanges.bybit  # 注册 Bybit 数据源

from market_data_fetch import Exchange, MarketDataClient, Symbol

client = MarketDataClient()
symbol = Symbol("ETH", "USDT")

latest_mark_price = client.get_latest_mark_price(Exchange.BYBIT, symbol)
premium_index = client.get_latest_premium_index(Exchange.BYBIT, symbol)
open_interest = client.get_open_interest(Exchange.BYBIT, symbol)
```

由于 tuple 型返回结构保持一致，切换到 Bybit 仅需调整 `exchange` 枚举即可。
