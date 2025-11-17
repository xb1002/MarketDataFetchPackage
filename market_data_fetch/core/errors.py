"""Custom exception hierarchy for market data fetching."""

from __future__ import annotations


class MarketDataError(RuntimeError):
    """Base class for all domain-specific exceptions."""


class SymbolNotSupportedError(MarketDataError):
    """Raised when an exchange does not list the requested symbol."""


class IntervalNotSupportedError(MarketDataError):
    """Raised when an unsupported candlestick interval is requested."""


class ExchangeTransientError(MarketDataError):
    """Represents temporary issues such as rate limiting or network failures."""
