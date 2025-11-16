"""Market data fetching package skeleton.

This module exposes the public API for abstract interfaces, models, and helper
utilities required to build market data downloaders.
"""

from .contracts.usdt_perp.interface import USDTPerpMarketDataSource
from .core.coordinator import MarketDataClient
from .core.errors import ExchangeTransientError, IntervalNotSupportedError, MarketDataError, SymbolNotSupportedError
from .core.queries import FundingRateWindow, HistoricalWindow
from .core.registry import create_usdt_perp_source, register_usdt_perp_source
from .models.shared import Exchange, Interval, Symbol
from .models.usdt_perp import (
    USDTPerpFundingRatePoint,
    USDTPerpIndexPricePoint,
    USDTPerpKline,
    USDTPerpMarkPrice,
    USDTPerpOpenInterest,
    USDTPerpPremiumIndexPoint,
    USDTPerpPriceTicker,
)

__all__ = [
    "USDTPerpMarketDataSource",
    "MarketDataClient",
    "FundingRateWindow",
    "HistoricalWindow",
    "Exchange",
    "Interval",
    "Symbol",
    "USDTPerpFundingRatePoint",
    "USDTPerpIndexPricePoint",
    "USDTPerpKline",
    "USDTPerpMarkPrice",
    "USDTPerpOpenInterest",
    "USDTPerpPremiumIndexPoint",
    "USDTPerpPriceTicker",
    "register_usdt_perp_source",
    "create_usdt_perp_source",
    "MarketDataError",
    "SymbolNotSupportedError",
    "IntervalNotSupportedError",
    "ExchangeTransientError",
]
