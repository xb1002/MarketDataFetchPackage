"""Domain models for market data fetching."""

from .shared import Exchange, Interval, Symbol
from .usdt_perp import (
    USDTPerpFundingRatePoint,
    USDTPerpIndexPricePoint,
    USDTPerpKline,
    USDTPerpMarkPrice,
    USDTPerpOpenInterest,
    USDTPerpPremiumIndexPoint,
    USDTPerpPriceTicker,
)

__all__ = [
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
]
