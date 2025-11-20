"""Shared helpers for manual provider-vs-CCXT comparisons."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Callable, Iterable, Sequence

import ccxt  # type: ignore

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from market_data_fetch.contracts.usdt_perp.interface import USDTPerpMarketDataSource
from market_data_fetch.exchanges.binance.usdt_perp import BinanceUSDTPerpDataSource
from market_data_fetch.exchanges.bitget.usdt_perp import BitgetUSDTPerpDataSource
from market_data_fetch.exchanges.bybit.usdt_perp import BybitUSDTPerpDataSource
from market_data_fetch.exchanges.okx.usdt_perp import OkxUSDTPerpDataSource
from market_data_fetch.models.shared import Symbol


@dataclass(slots=True)
class ProviderCase:
    name: str
    source_factory: Callable[[], USDTPerpMarketDataSource]
    ccxt_factory: Callable[[dict[str, object]], ccxt.Exchange]
    ccxt_symbol: str


CASES: Sequence[ProviderCase] = (
    ProviderCase(
        name="binance",
        source_factory=BinanceUSDTPerpDataSource,
        ccxt_factory=ccxt.binanceusdm,
        ccxt_symbol="BTC/USDT:USDT",
    ),
    ProviderCase(
        name="bybit",
        source_factory=BybitUSDTPerpDataSource,
        ccxt_factory=ccxt.bybit,
        ccxt_symbol="BTC/USDT:USDT",
    ),
    ProviderCase(
        name="bitget",
        source_factory=BitgetUSDTPerpDataSource,
        ccxt_factory=ccxt.bitget,
        ccxt_symbol="BTC/USDT:USDT",
    ),
    ProviderCase(
        name="okx",
        source_factory=OkxUSDTPerpDataSource,
        ccxt_factory=ccxt.okx,
        ccxt_symbol="BTC/USDT:USDT",
    ),
)


def iter_cases(targets: Iterable[str] | None = None) -> Iterable[ProviderCase]:
    if not targets:
        yield from CASES
        return
    selected = {t.lower() for t in targets}
    for case in CASES:
        if case.name.lower() in selected:
            yield case


def iso_ms(ts_ms: int | float | None) -> str:
    if ts_ms is None:
        return "<missing>"
    return datetime.fromtimestamp(float(ts_ms) / 1000, tz=timezone.utc).isoformat()


SYMBOL = Symbol("BTC", "USDT")
