"""Query helper objects shared across data sources."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from ..models.shared import Interval, Symbol

DEFAULT_LIMIT = 500


@dataclass(frozen=True, slots=True)
class HistoricalWindow:
    """Represents an OHLCV historical query window."""

    symbol: Symbol
    interval: Interval
    start_time: datetime | None = None
    end_time: datetime | None = None
    limit: int = DEFAULT_LIMIT

    def __post_init__(self) -> None:
        if self.limit <= 0:
            raise ValueError("limit must be a positive integer")
        if self.start_time and self.end_time and self.start_time >= self.end_time:
            raise ValueError("start_time must be earlier than end_time")


@dataclass(frozen=True, slots=True)
class FundingRateWindow:
    """Represents a historical funding rate query window."""

    symbol: Symbol
    start_time: datetime | None = None
    end_time: datetime | None = None
    limit: int = DEFAULT_LIMIT

    def __post_init__(self) -> None:
        if self.limit <= 0:
            raise ValueError("limit must be a positive integer")
        if self.start_time and self.end_time and self.start_time >= self.end_time:
            raise ValueError("start_time must be earlier than end_time")
