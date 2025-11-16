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
latest = client.get_latest_price(Exchange.BINANCE, symbol)
```

示例中演示了如何下载历史 K 线以及获取最新的 Mark Price。其他如指数 K 线、溢价指数、资金费率历史与未平仓量均通过同一个 `MarketDataClient` 入口暴露。
