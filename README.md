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
ticker = client.get_latest_price(Exchange.BINANCE, symbol)  # 成交价 dict
mark_price = client.get_latest_mark_price(Exchange.BINANCE, symbol)  # (timestamp, mark_price)

# K 线返回 List[Tuple]，顺序为 (open_time_ms, open, high, low, close, volume)
open_time_ms, open_price, *_ = klines[0]

# 其他返回结构：
# - 最新 ticker: {"timestamp": int, "last_price": Decimal, "bid_price": Decimal, "ask_price": Decimal}
# - Funding 历史/最新值: (funding_time_ms, funding_rate)
# - Index price 最新值: (timestamp_ms, index_price)
# - Premium index 最新值: (timestamp_ms, premium_index_value)
# - Mark price 快照: (timestamp_ms, mark_price)
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

mark_timestamp, mark_price = client.get_latest_mark_price(Exchange.BITGET, symbol)
```

Bitget 集成现已全面切换到 UTA 的 V3 行情接口：

- `/api/v3/market/history-candles`（`category=USDT-FUTURES`，`type=MARKET/INDEX/MARK/PREMIUM`）用于获取价格、指数、标记价与溢价 K 线；
- `/api/v3/market/history-fund-rate` 与 `/api/v3/market/current-fund-rate` 分别用于资金费率历史与下一次结算时间；
- `/api/v3/market/tickers`、`/api/v3/market/open-interest` 则返回最新成交价、Mark Price、指数价与未平仓量。

因此 `get_*_klines`、`get_latest_mark_price`、`get_latest_premium_index` 等方法都直接消费上述官方端点，输出与 Binance、Bybit 相同的 tuple 结构。

## OKX U 本位合约示例

OKX 的实现位于 `market_data_fetch.exchanges.okx`，导入后即可注册：

```python
import market_data_fetch.exchanges.okx  # 注册 OKX 数据源

from market_data_fetch import Exchange, HistoricalWindow, Interval, MarketDataClient, Symbol

client = MarketDataClient()
symbol = Symbol("BTC", "USDT")

price_klines = client.get_price_klines(
    Exchange.OKX,
    HistoricalWindow(symbol=symbol, interval=Interval.MINUTE_1, limit=200),
)
premium_klines = client.get_premium_index_klines(
    Exchange.OKX,
    HistoricalWindow(symbol=symbol, interval=Interval.MINUTE_1, limit=50),
)

mark_price = client.get_latest_mark_price(Exchange.OKX, symbol)
```

OKX 集成覆盖以下端点：

- `/api/v5/market/candles`、`/api/v5/market/index-candles`、`/api/v5/market/mark-price-candles` 下载价格/指数/标记价 K 线（官方最大 limit 分别为 300/100/100 条）。
- `/api/v5/public/premium-history` 返回溢价指数离散点，代码会将每条记录转换成 `USDTPerpKline` 并让 OHLC 值完全一致、成交量固定为 0，以满足统一的接口契约。
- `/api/v5/public/funding-rate-history` 与 `/api/v5/public/funding-rate` 提供资金费率历史与最新值。
- `/api/v5/market/tickers`、`/api/v5/market/index-tickers`、`/api/v5/public/mark-price`、`/api/v5/public/open-interest`、`/api/v5/public/instruments` 分别用于最新成交价、指数价、标记价、未平仓量与合约信息。

即便 OKX 官方没有直接提供溢价指数 K 线，也能通过 `premium-history` 合成出符合 tuple 契约的结果，其余接口行为与 Binance/Bybit/Bitget 保持一致。

## 合约（Instrument）信息

三家交易所均实现了 `get_instruments` 接口，可通过 `MarketDataClient` 统一获取：

```python
from market_data_fetch import Exchange, MarketDataClient

client = MarketDataClient()

instruments = client.get_instruments(Exchange.BINANCE)
instrument = instruments[0]
symbol = instrument["symbol"]
base = instrument["base_asset"]
tick_size = instrument["tick_size"]
```

返回结构改为 `dict`，字段含义如下，便于基于键名读取并避免符号顺序差异导致错误：

- `symbol`：交易所合约符号（如 `BTCUSDT`）。
- `base_asset`/`quote_asset`：基础币与计价币。
- `tick_size`：价格精度（最小价格变动）。
- `step_size`：数量精度（下单步长）。
- `min_qty`/`max_qty`：合约允许的下单数量区间。
- `status`：交易所原始状态字符串（如 `TRADING`、`online` 等）。

## CCXT 数据正确性校验

`tests/test_ccxt_parity.py` 会借助 [CCXT](https://github.com/ccxt/ccxt) 再次从 Binance/Bybit/Bitget/OKX 下载行情，并与本项目的接口返回逐一对齐，
覆盖：

- 价格/指数/标记/溢价 K 线；
- 最新成交价、指数价、溢价指数与 Mark Price 快照；
- 资金费率历史与最新值；
- 未平仓量与合约（instrument）元数据。

运行这些校验测试前，请先安装可选依赖：

```bash
pip install .[test]
```

随后可以根据需要筛选交易所，例如只对 Binance 做 CCXT 校验：

```bash
pytest tests/test_ccxt_parity.py -k binance
```

若 CCXT 无法访问目标交易所（例如被限流或地理限制），fixture 会自动 `skip`，因此不会影响其它测试结果。
