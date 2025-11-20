"""Domain models for market data fetching."""

from .shared import Exchange, Interval, Symbol
from .usdt_perp import (
    USDTPerpFundingRate,
    USDTPerpFundingRatePoint,
    USDTPerpIndexPricePoint,
    USDTPerpInstrument,
    USDTPerpKline,
    USDTPerpMarkPrice,
    USDTPerpOpenInterest,
    USDTPerpTicker,
)

__all__ = [
    "Exchange",
    "Interval",
    "Symbol",
    "USDTPerpFundingRatePoint",
    "USDTPerpFundingRate",
    "USDTPerpIndexPricePoint",
    "USDTPerpInstrument",
    "USDTPerpKline",
    "USDTPerpMarkPrice",
    "USDTPerpOpenInterest",
    "USDTPerpTicker",
]
