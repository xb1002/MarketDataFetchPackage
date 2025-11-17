"""Core utilities for market data fetching."""

from __future__ import annotations

from importlib import import_module
from typing import Any

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

_lazy_targets = {
    "MarketDataClient": ("coordinator", "MarketDataClient"),
    "FundingRateWindow": ("queries", "FundingRateWindow"),
    "HistoricalWindow": ("queries", "HistoricalWindow"),
    "register_usdt_perp_source": ("registry", "register_usdt_perp_source"),
    "create_usdt_perp_source": ("registry", "create_usdt_perp_source"),
    "MarketDataError": ("errors", "MarketDataError"),
    "SymbolNotSupportedError": ("errors", "SymbolNotSupportedError"),
    "IntervalNotSupportedError": ("errors", "IntervalNotSupportedError"),
    "ExchangeTransientError": ("errors", "ExchangeTransientError"),
}


def __getattr__(name: str) -> Any:
    try:
        module_name, attr_name = _lazy_targets[name]
    except KeyError as exc:  # pragma: no cover - defensive
        raise AttributeError(f"module 'market_data_fetch.core' has no attribute {name!r}") from exc
    module = import_module(f"{__name__}.{module_name}")
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
