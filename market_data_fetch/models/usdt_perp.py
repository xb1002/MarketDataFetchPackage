"""Data models specific to USDT-margined perpetual contracts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from .shared import Symbol


@dataclass(frozen=True, slots=True)
class USDTPerpKline:
    """Generic OHLCV representation shared by price, index price, and premium feeds."""

    symbol: Symbol
    open_time: datetime
    close_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    quote_volume: Decimal

    def to_dict(self) -> dict[str, Any]:
        """Serialize the kline into a JSON-friendly dictionary."""

        return {
            "symbol": self.symbol.pair,
            "open_time": self.open_time.isoformat(),
            "close_time": self.close_time.isoformat(),
            "open": str(self.open),
            "high": str(self.high),
            "low": str(self.low),
            "close": str(self.close),
            "volume": str(self.volume),
            "quote_volume": str(self.quote_volume),
        }


@dataclass(frozen=True, slots=True)
class USDTPerpFundingRatePoint:
    """Represents one historical or latest funding rate measurement."""

    symbol: Symbol
    timestamp: datetime
    rate: Decimal
    predicted_rate: Decimal | None


@dataclass(frozen=True, slots=True)
class USDTPerpMarkPrice:
    """Snapshot of mark/index price and associated funding metadata."""

    symbol: Symbol
    price: Decimal
    index_price: Decimal
    last_funding_rate: Decimal
    next_funding_time: datetime


@dataclass(frozen=True, slots=True)
class USDTPerpOpenInterest:
    """Latest open interest value for a perpetual contract."""

    symbol: Symbol
    timestamp: datetime
    value: Decimal
