"""Protocols describing USDT perpetual data sources."""

from __future__ import annotations

from typing import ClassVar, Protocol, Sequence, runtime_checkable

from ...core.queries import FundingRateWindow, HistoricalWindow
from ...models.shared import Exchange, Symbol
from ...models.usdt_perp import (
    USDTPerpFundingRatePoint,
    USDTPerpKline,
    USDTPerpMarkPrice,
    USDTPerpOpenInterest,
)


@runtime_checkable
class USDTPerpMarketDataSource(Protocol):
    """Data source capable of serving USDT-margined perpetual market data."""

    exchange: ClassVar[Exchange]

    # Historical series -------------------------------------------------
    def get_price_klines(self, query: HistoricalWindow) -> Sequence[USDTPerpKline]:
        """Return standard price klines for the specified symbol and window."""

    def get_index_price_klines(self, query: HistoricalWindow) -> Sequence[USDTPerpKline]:
        """Return index price klines sharing the same schema as price klines."""

    def get_premium_index_klines(self, query: HistoricalWindow) -> Sequence[USDTPerpKline]:
        """Return premium index klines."""

    def get_funding_rate_history(self, query: FundingRateWindow) -> Sequence[USDTPerpFundingRatePoint]:
        """Return historical funding rate points for the requested symbol."""

    # Latest snapshots --------------------------------------------------
    def get_latest_price(self, symbol: Symbol) -> USDTPerpMarkPrice:
        """Return the latest mark price snapshot containing price and funding details."""

    def get_latest_index_price(self, symbol: Symbol) -> USDTPerpKline:
        """Return the latest index price encapsulated in the generic kline structure."""

    def get_latest_premium_index(self, symbol: Symbol) -> USDTPerpKline:
        """Return the latest premium index encapsulated in the generic kline structure."""

    def get_latest_funding_rate(self, symbol: Symbol) -> USDTPerpFundingRatePoint:
        """Return the latest funding rate measurement."""

    def get_open_interest(self, symbol: Symbol) -> USDTPerpOpenInterest:
        """Return the current open interest value for the requested symbol."""
