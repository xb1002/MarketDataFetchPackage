# PLAN

## 背景
为满足当前阶段仅支持 U 本位合约数据下载、同时预留币本位与杠杆扩展能力的目标，需要先冻结接口契约并规划落地步骤。以下内容覆盖接口组织、领域模型、方法签名和实施计划。

## 接口组织设计
- **包结构**：
  - `market_data_fetch/contracts/usdt_perp/interface.py`：定义 `USDTPerpMarketDataSource` 协议，面向历史/最新行情读取能力。
  - `market_data_fetch/models/usdt_perp.py`：以轻量 tuple（避免 dataclass 开销）描述所有返回数据模型（K 线、指数、资金费率、未平仓量等），合约信息则使用 `TypedDict` 保证字段语义清晰。
  - `market_data_fetch/models/shared.py`：时间粒度、交易对、分页游标等基础类型，未来币本位/杠杆可复用。
  - `market_data_fetch/core/queries.py`：封装查询参数对象，支持 start/end/limit 组合校验。
  - `market_data_fetch/exchanges/<exchange>/usdt_perp.py`：交易所实现；仅依赖接口与模型，确保扩展性。

## 领域模型
```python
# market_data_fetch/models/shared.py
class Interval(StrEnum): ...
@dataclass(frozen=True)
class Symbol:
    base: str
    quote: str
    contract_type: Literal["perpetual"]
```
```python
# market_data_fetch/models/usdt_perp.py
# Tuple 布局全部以毫秒时间戳+ Decimal 描述，避免 dataclass 带来的额外开销。
USDTPerpKline = tuple[int, Decimal, Decimal, Decimal, Decimal, Decimal]
USDTPerpFundingRatePoint = tuple[int, Decimal]
class USDTPerpFundingRate(TypedDict):
    funding_rate: Decimal
    next_funding_time: int
USDTPerpMarkPrice = tuple[int, Decimal]
USDTPerpOpenInterest = tuple[int, Decimal]

class USDTPerpTicker(TypedDict):
    timestamp: int
    last_price: Decimal
    index_price: Decimal
    mark_price: Decimal
USDTPerpIndexPricePoint = tuple[int, Decimal]
USDTPerpPremiumIndexPoint = tuple[int, Decimal]

class USDTPerpInstrument(TypedDict):
    symbol: str
    base_asset: str
    quote_asset: str
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal
    max_qty: Decimal
    status: bool
```

## 查询对象
```python
@dataclass(frozen=True)
class HistoricalWindow:
    symbol: Symbol
    interval: Interval
    start_time: datetime | None = None
    end_time: datetime | None = None
    limit: int | None = None  # 默认 500，由具体交易所/接口约束最大值
```
- `HistoricalWindow` 由协调层在调用前执行校验（起止时间、limit）。
- 对于资金费率历史，可另外定义 `FundingRateWindow`（仅 start/end/limit，无 interval）。

## `USDTPerpMarketDataSource` 协议
```python
class USDTPerpMarketDataSource(Protocol):
    exchange: ClassVar[Exchange]

    # 历史序列
    def get_price_klines(self, query: HistoricalWindow) -> Sequence[USDTPerpKline]: ...
    def get_index_price_klines(self, query: HistoricalWindow) -> Sequence[USDTPerpKline]: ...
    def get_premium_index_klines(self, query: HistoricalWindow) -> Sequence[USDTPerpKline]: ...
    def get_funding_rate_history(
        self, query: FundingRateWindow
    ) -> Sequence[USDTPerpFundingRatePoint]: ...

    # 最新值
    def get_latest_ticker(self, symbol: Symbol) -> USDTPerpTicker: ...
    def get_latest_index_price(self, symbol: Symbol) -> USDTPerpIndexPricePoint: ...
    def get_latest_premium_index(self, symbol: Symbol) -> USDTPerpPremiumIndexPoint: ...
    def get_latest_funding_rate(self, symbol: Symbol) -> USDTPerpFundingRate: ...
    def get_open_interest(self, symbol: Symbol) -> USDTPerpOpenInterest: ...
    def get_instruments(self) -> Sequence[USDTPerpInstrument]: ...
```
- 最新价格/指数/溢价指数均复用轻量 tuple 表达，指数类仅包含“数值+时间戳”而非 K 线结构。
- 通过统一的 `USDTPerpKline`，避免三类 K 线重复字段定义。
- `get_open_interest` 仅返回最新未平仓量；未来若需历史序列，可在协议中新增 `get_open_interest_history`，保持对现有实现向后兼容。
- `get_instruments` 统一暴露合约精度、下单限制与状态，方便对不同交易所做一致性校验。

## 错误处理契约
- 所有方法抛出 `MarketDataError` 子类：
  - `SymbolNotSupportedError`（交易所不支持该交易对）。
  - `IntervalNotSupportedError`。
  - `ExchangeTransientError`（网络/限频）。
- `core/coordinator.py` 负责捕获并重试 transient 错误，向上抛出语义化异常。

## 实施计划
1. **搭建包骨架**：创建 `market_data_fetch/` 目录及上述子模块；在 `__init__.py` 中导出关键协议和模型，方便外部引用。
2. **定义基础类型与模型**：在 `models/shared.py`、`models/usdt_perp.py` 和 `core/queries.py` 中实现 Enum/tuple type alias，并添加字段/顺序说明文档。
3. **实现接口协议**：在 `contracts/usdt_perp/interface.py` 中定义 `USDTPerpMarketDataSource`、相关异常类型注释，以及方法 docstring（说明参数、返回值、错误）。
4. **注册与发现机制**：实现 `core/registry.py`，允许通过 `register_usdt_perp_source(exchange, cls)` 注册；协调层通过 `get_usdt_perp_source(exchange)` 实例化。
5. **示例交易所实现**：以 Binance 为首个数据源，实现 `BinanceUSDTPerpFetcher`，覆盖全部接口方法并写单测，确保协议可行。
6. **编排客户端与 CLI**：在 `core/coordinator.py` 中封装 `MarketDataClient`，提供同步 API，内部调用注册的 fetcher；补充 README 使用示例。
7. **测试与文档**：
   - 为模型与查询对象编写验证测试。
   - 使用 fixture 模拟交易所响应，确保 fetcher 对齐协议。
   - 在 README 与 API 文档中列出接口方法及参数说明。
   - 通过 `tests/test_ccxt_parity.py` 借助 CCXT 对真实交易所数据做交叉校验，验证价格/指数/溢价 K 线、最新行情、资金费率、未平仓量与合约信息是否与官方返回保持一致。

通过上述接口与计划，可在专注 U 本位合约的同时，为未来的币本位和杠杆模块提供一致的扩展点。

## 交易所实现进度

- **Binance**：已落地全部 U 本位接口，使用官方 Futures REST API (`/fapi/v1/*`)，并在 tests 中连通 testnet 覆盖所有方法。
- **Bybit**：新增 `BybitUSDTPerpDataSource`，覆盖 `/v5/market/kline`、`/v5/market/index-price-kline`、`/v5/market/mark-price-kline`、`/v5/market/premium-index-price-kline`、`/v5/market/funding/history`、`/v5/market/tickers`、`/v5/market/premium-index-price` 与 `/v5/market/open-interest`。同时提供 live 测试（遇到 CloudFront 403 会自动以 `ExchangeTransientError` 跳过），保证接口契约在真实网络环境下验证。
- **Bitget**：新增 `BitgetUSDTPerpDataSource`，全面使用 UTA V3 行情接口：`/api/v3/market/history-candles`（`category=USDT-FUTURES`，`type=MARKET/INDEX/MARK/PREMIUM`）负责所有历史 K 线，`/api/v3/market/history-fund-rate` 与 `/api/v3/market/current-fund-rate` 提供资金费率历史及下一次结算时间，`/api/v3/market/tickers`、`/api/v3/market/open-interest` 提供最新成交价、标记价、指数价与未平仓量，`/api/v3/market/instruments` 则返回合约信息，保证所有数据下载都来自统一账户官方端点。
- **OKX**：新增 `OkxUSDTPerpDataSource`，历史价格/指数/标记价 K 线分别走 `/api/v5/market/candles`、`/api/v5/market/index-candles`、`/api/v5/market/mark-price-candles`（对应 limit 300/100/100），溢价指数历史由 `/api/v5/public/premium-history` 提供离散点，代码会将其合成为 OHLC 均等、成交量为 0 的 `USDTPerpKline`；资金费率来自 `/api/v5/public/funding-rate-history` 与 `/api/v5/public/funding-rate`，最新行情/未平仓/合约信息依赖 `/api/v5/market/tickers`、`/api/v5/market/index-tickers`、`/api/v5/public/mark-price`、`/api/v5/public/open-interest` 与 `/api/v5/public/instruments`。
- **CCXT 正确性验证**：新增 `tests/test_ccxt_parity.py`，为 Binance/Bybit/Bitget/OKX 建立统一的参数化 fixture，并通过 CCXT 重新抓取行情，与本项目返回做一对一比对（网络异常会自动 `skip`），确保真实数据与实现逻辑保持一致。
