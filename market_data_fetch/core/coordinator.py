"""High-level coordinator that routes calls to concrete data sources."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence

from ..contracts.usdt_perp.interface import USDTPerpMarketDataSource
from ..models.shared import Exchange, Symbol
from ..models.usdt_perp import (
    USDTPerpFundingRate,
    USDTPerpFundingRatePoint,
    USDTPerpIndexPricePoint,
    USDTPerpInstrument,
    USDTPerpKline,
    USDTPerpMarkPrice,
    USDTPerpOpenInterest,
    USDTPerpPremiumIndexPoint,
    USDTPerpTicker,
)
from .queries import FundingRateWindow, HistoricalWindow
from .registry import create_usdt_perp_source

SourceResolver = Callable[[Exchange], USDTPerpMarketDataSource]


class MarketDataClient:
    """Entry point consumed by SDK/CLI callers."""

    def __init__(
        self,
        *,
        source_overrides: Mapping[Exchange, USDTPerpMarketDataSource] | None = None,
        resolver: SourceResolver = create_usdt_perp_source,
    ) -> None:
        self._resolver = resolver
        self._sources: dict[Exchange, USDTPerpMarketDataSource] = {}
        if source_overrides:
            self._sources.update(source_overrides)

    # Historical --------------------------------------------------------
    def get_price_klines(self, exchange: Exchange, query: HistoricalWindow) -> Sequence[USDTPerpKline]:
        """Route to the registered price kline provider."""

        return self._get_source(exchange).get_price_klines(query)

    def get_index_price_klines(self, exchange: Exchange, query: HistoricalWindow) -> Sequence[USDTPerpKline]:
        """Route to the registered index price kline provider."""

        return self._get_source(exchange).get_index_price_klines(query)

    def get_mark_price_klines(self, exchange: Exchange, query: HistoricalWindow) -> Sequence[USDTPerpKline]:
        """Route to the registered mark price kline provider."""

        return self._get_source(exchange).get_mark_price_klines(query)

    def get_premium_index_klines(self, exchange: Exchange, query: HistoricalWindow) -> Sequence[USDTPerpKline]:
        """Route to the registered premium index kline provider."""

        return self._get_source(exchange).get_premium_index_klines(query)

    def get_funding_rate_history(self, exchange: Exchange, query: FundingRateWindow) -> Sequence[USDTPerpFundingRatePoint]:
        """Route to the registered funding rate provider."""

        return self._get_source(exchange).get_funding_rate_history(query)

    # Latest ------------------------------------------------------------
    def get_latest_ticker(self, exchange: Exchange, symbol: Symbol) -> USDTPerpTicker:
        """Return the latest ticker snapshot from the underlying source."""

        return self._get_source(exchange).get_latest_ticker(symbol)

    def get_latest_mark_price(self, exchange: Exchange, symbol: Symbol) -> USDTPerpMarkPrice:
        """Return the latest mark price snapshot from the underlying source."""

        return self._get_source(exchange).get_latest_mark_price(symbol)

    def get_latest_index_price(self, exchange: Exchange, symbol: Symbol) -> USDTPerpIndexPricePoint:
        """Return the latest index price snapshot."""

        return self._get_source(exchange).get_latest_index_price(symbol)

    def get_latest_premium_index(
        self, exchange: Exchange, symbol: Symbol
    ) -> USDTPerpPremiumIndexPoint:
        """Return the latest premium index snapshot."""

        return self._get_source(exchange).get_latest_premium_index(symbol)

    def get_latest_funding_rate(self, exchange: Exchange, symbol: Symbol) -> USDTPerpFundingRate:
        """Return the latest funding rate measurement."""

        return self._get_source(exchange).get_latest_funding_rate(symbol)

    def get_open_interest(self, exchange: Exchange, symbol: Symbol) -> USDTPerpOpenInterest:
        """Return the latest open interest value."""

        return self._get_source(exchange).get_open_interest(symbol)

    # Instruments -------------------------------------------------------
    def get_instruments(self, exchange: Exchange) -> Sequence[USDTPerpInstrument]:
        """Return instrument metadata for the selected exchange."""

        return self._get_source(exchange).get_instruments()

    # Internal ----------------------------------------------------------
    def _get_source(self, exchange: Exchange) -> USDTPerpMarketDataSource:
        try:
            return self._sources[exchange]
        except KeyError:
            source = self._resolver(exchange)
            self._sources[exchange] = source
            return source
