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
price_ts, last_price = client.get_latest_price(Exchange.BINANCE, symbol)  # 成交价
mark_price = client.get_latest_mark_price(Exchange.BINANCE, symbol)  # 标记价格 tuple

# K 线返回 List[Tuple]，顺序为 (open_time_ms, open, high, low, close, volume)
open_time_ms, open_price, *_ = klines[0]

# 其他返回结构：
# - Funding 历史/最新值: (funding_time_ms, funding_rate)
# - Index price 最新值: (timestamp_ms, index_price)
# - Premium index 最新值: (timestamp_ms, premium_index_value)
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

## Bitget U 本位合约示例

Bitget 的实现位于 `market_data_fetch.exchanges.bitget`，导入即可注册：

```python
import market_data_fetch.exchanges.bitget  # 注册 Bitget 数据源

from market_data_fetch import Exchange, MarketDataClient, Symbol

client = MarketDataClient()
symbol = Symbol("BTC", "USDT")

mark_price, index_price, funding_rate, next_funding = client.get_latest_mark_price(Exchange.BITGET, symbol)
```

Bitget 集成现已全面切换到 UTA 的 V3 行情接口：

- `/api/v3/market/history-candles`（`category=USDT-FUTURES`，`type=MARKET/INDEX/MARK/PREMIUM`）用于获取价格、指数、标记价与溢价 K 线；
- `/api/v3/market/history-fund-rate` 与 `/api/v3/market/current-fund-rate` 分别用于资金费率历史与下一次结算时间；
- `/api/v3/market/tickers`、`/api/v3/market/open-interest` 则返回最新成交价、Mark Price、指数价与未平仓量。

因此 `get_*_klines`、`get_latest_mark_price`、`get_latest_premium_index` 等方法都直接消费上述官方端点，输出与 Binance、Bybit 相同的 tuple 结构。

## 合约（Instrument）信息

三家交易所均实现了 `get_instruments` 接口，可通过 `MarketDataClient` 统一获取：

```python
from market_data_fetch import Exchange, MarketDataClient

client = MarketDataClient()

instruments = client.get_instruments(Exchange.BINANCE)
symbol, base, quote, tick_size, step_size, min_qty, max_qty, status = instruments[0]
```

返回结构同样是 tuple，以便在批量存储/序列化场景中保持较低的内存占用：

- `symbol`：交易所合约符号（如 `BTCUSDT`）。
- `base`/`quote`：基础币与计价币。
- `tick_size`：价格精度（最小价格变动）。
- `step_size`：数量精度（下单步长）。
- `min_qty`/`max_qty`：合约允许的下单数量区间。
- `status`：交易所原始状态字符串（如 `TRADING`、`online` 等）。
