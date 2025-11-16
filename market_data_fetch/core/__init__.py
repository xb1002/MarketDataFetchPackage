"""Core utilities for market data fetching."""

from .coordinator import MarketDataClient
from .errors import (
    ExchangeTransientError,
    IntervalNotSupportedError,
    MarketDataError,
    SymbolNotSupportedError,
)
from .queries import FundingRateWindow, HistoricalWindow
from .registry import create_usdt_perp_source, register_usdt_perp_source

__all__ = [
    "MarketDataClient",
    "FundingRateWindow",
    "HistoricalWindow",
    "register_usdt_perp_source",
    "create_usdt_perp_source",
    "MarketDataError",
    "SymbolNotSupportedError",
    "IntervalNotSupportedError",
    "ExchangeTransientError",
]
