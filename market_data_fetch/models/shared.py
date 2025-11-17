"""Shared domain models used across multiple market segments."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Literal


class Exchange(StrEnum):
    """Supported exchanges.

    The enum is intentionally small for now; additional exchanges can be appended
    without breaking callers because the values are string based.
    """

    BINANCE = "binance"
    OKX = "okx"
    BYBIT = "bybit"
    BITGET = "bitget"


class Interval(StrEnum):
    """Standardized candlestick intervals."""

    MINUTE_1 = "1m"
    MINUTE_3 = "3m"
    MINUTE_5 = "5m"
    MINUTE_15 = "15m"
    MINUTE_30 = "30m"
    HOUR_1 = "1h"
    HOUR_2 = "2h"
    HOUR_4 = "4h"
    HOUR_6 = "6h"
    HOUR_12 = "12h"
    DAY_1 = "1d"
    DAY_3 = "3d"
    WEEK_1 = "1w"
    MONTH_1 = "1M"


ContractType = Literal["perpetual"]


@dataclass(frozen=True, slots=True)
class Symbol:
    """Represents a derivative contract symbol."""

    base: str
    quote: str
    contract_type: ContractType = "perpetual"

    def __post_init__(self) -> None:
        if not self.base or not self.quote:
            raise ValueError("Symbol base and quote must be non-empty strings.")

    @property
    def pair(self) -> str:
        """Return the canonical pair string (e.g., ``BTCUSDT``)."""

        return f"{self.base}{self.quote}"
