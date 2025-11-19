"""Tuple-based data contracts specific to USDT-margined perpetual markets."""

from __future__ import annotations

from decimal import Decimal
from typing import TypeAlias, TypedDict

# The tuple layouts intentionally avoid dataclasses to minimize memory overhead
# when processing very large payloads (millions of rows) before persisting them.

# ``(open_time_ms, open, high, low, close, volume)``
USDTPerpKline: TypeAlias = tuple[int, Decimal, Decimal, Decimal, Decimal, Decimal]

# ``(funding_time_ms, funding_rate)``
USDTPerpFundingRatePoint: TypeAlias = tuple[int, Decimal]

# ``(timestamp_ms, mark_price)``
USDTPerpMarkPrice: TypeAlias = tuple[int, Decimal]

# ``(timestamp_ms, open_interest_value)``
USDTPerpOpenInterest: TypeAlias = tuple[int, Decimal]



class USDTPerpTicker(TypedDict):
    """Ticker snapshot bundling traded, index, and mark prices."""

    timestamp: int
    last_price: Decimal
    index_price: Decimal
    mark_price: Decimal

# ``(timestamp_ms, index_price)``
USDTPerpIndexPricePoint: TypeAlias = tuple[int, Decimal]

# ``(timestamp_ms, premium_index_value)``
USDTPerpPremiumIndexPoint: TypeAlias = tuple[int, Decimal]



class USDTPerpInstrument(TypedDict):
    """Instrument metadata for a USDT-margined perpetual contract."""

    symbol: str
    base_asset: str
    quote_asset: str
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal
    max_qty: Decimal
    status: str
