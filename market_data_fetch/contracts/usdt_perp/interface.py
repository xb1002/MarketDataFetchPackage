"""Protocols describing USDT perpetual data sources."""

from __future__ import annotations

from typing import ClassVar, Protocol, Sequence, runtime_checkable

from ...core.queries import FundingRateWindow, HistoricalWindow
from ...models.shared import Exchange, Symbol
from ...models.usdt_perp import (
    USDTPerpFundingRate,
    USDTPerpFundingRatePoint,
    USDTPerpIndexPricePoint,
    USDTPerpInstrument,
    USDTPerpKline,
    USDTPerpMarkPrice,
    USDTPerpOpenInterest,
    USDTPerpTicker,
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

    def get_mark_price_klines(self, query: HistoricalWindow) -> Sequence[USDTPerpKline]:
        """Return mark price klines using the generic OHLCV schema."""

    def get_premium_index_klines(self, query: HistoricalWindow) -> Sequence[USDTPerpKline]:
        """Return premium index klines."""

    def get_funding_rate_history(self, query: FundingRateWindow) -> Sequence[USDTPerpFundingRatePoint]:
        """Return historical funding rate points for the requested symbol."""

    # Latest snapshots --------------------------------------------------
    def get_latest_ticker(self, symbol: Symbol) -> USDTPerpTicker:
        """Return the latest ticker snapshot."""

    def get_latest_mark_price(self, symbol: Symbol) -> USDTPerpMarkPrice:
        """Return the latest mark price snapshot."""

    def get_latest_index_price(self, symbol: Symbol) -> USDTPerpIndexPricePoint:
        """Return the latest index price value plus timestamp."""

    def get_latest_funding_rate(self, symbol: Symbol) -> USDTPerpFundingRate:
        """Return the latest funding rate measurement."""

    def get_open_interest(self, symbol: Symbol) -> USDTPerpOpenInterest:
        """Return the current open interest value for the requested symbol."""

    # Instruments -------------------------------------------------------
    def get_instruments(self) -> Sequence[USDTPerpInstrument]:
        """Return contract metadata for all tradable USDT-perpetual symbols."""
