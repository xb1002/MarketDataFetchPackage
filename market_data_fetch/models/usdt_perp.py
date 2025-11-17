"""Tuple-based data contracts specific to USDT-margined perpetual markets."""

from __future__ import annotations

from decimal import Decimal
from typing import TypeAlias

# The tuple layouts intentionally avoid dataclasses to minimize memory overhead
# when processing very large payloads (millions of rows) before persisting them.

# ``(open_time_ms, open, high, low, close, volume)``
USDTPerpKline: TypeAlias = tuple[int, Decimal, Decimal, Decimal, Decimal, Decimal]

# ``(funding_time_ms, funding_rate)``
USDTPerpFundingRatePoint: TypeAlias = tuple[int, Decimal]

# ``(mark_price, index_price, last_funding_rate, next_funding_time_ms)``
USDTPerpMarkPrice: TypeAlias = tuple[Decimal, Decimal, Decimal, int]

# ``(timestamp_ms, open_interest_value)``
USDTPerpOpenInterest: TypeAlias = tuple[int, Decimal]

# ``(timestamp_ms, last_price)``
USDTPerpPriceTicker: TypeAlias = tuple[int, Decimal]

# ``(timestamp_ms, index_price)``
USDTPerpIndexPricePoint: TypeAlias = tuple[int, Decimal]

# ``(timestamp_ms, premium_index_value)``
USDTPerpPremiumIndexPoint: TypeAlias = tuple[int, Decimal]

# ``(symbol, base_asset, quote_asset, tick_size, step_size, min_qty, max_qty, status)``
USDTPerpInstrument: TypeAlias = tuple[
    str,
    str,
    str,
    Decimal,
    Decimal,
    Decimal,
    Decimal,
    str,
]
