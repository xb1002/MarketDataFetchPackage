"""Compare latest funding rates with CCXT outputs for reference."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Callable, Sequence

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
class FundingComparator:
    name: str
    source_factory: Callable[[], USDTPerpMarketDataSource]
    ccxt_factory: Callable[[dict[str, object]], ccxt.Exchange]
    ccxt_symbol: str


CASES: Sequence[FundingComparator] = (
    FundingComparator(
        name="binance",
        source_factory=BinanceUSDTPerpDataSource,
        ccxt_factory=ccxt.binanceusdm,
        ccxt_symbol="BTC/USDT:USDT",
    ),
    FundingComparator(
        name="bybit",
        source_factory=BybitUSDTPerpDataSource,
        ccxt_factory=ccxt.bybit,
        ccxt_symbol="BTC/USDT:USDT",
    ),
    FundingComparator(
        name="bitget",
        source_factory=BitgetUSDTPerpDataSource,
        ccxt_factory=ccxt.bitget,
        ccxt_symbol="BTC/USDT:USDT",
    ),
    FundingComparator(
        name="okx",
        source_factory=OkxUSDTPerpDataSource,
        ccxt_factory=ccxt.okx,
        ccxt_symbol="BTC/USDT:USDT",
    ),
)


def main() -> None:
    symbol = Symbol("BTC", "USDT")
    for case in CASES:
        print(f"\n=== {case.name} ===")
        source = case.source_factory()
        ours: str
        try:
            snapshot = source.get_latest_funding_rate(symbol)
        except Exception as exc:  # pragma: no cover - manual tool
            ours = f"provider error: {exc}"
        else:
            ours = (
                "provider funding_rate="
                f"{snapshot['funding_rate']} | next_funding_time={snapshot['next_funding_time']}"
            )
        finally:
            try:
                source.close()
            except Exception:  # pragma: no cover - defensive cleanup
                pass

        exchange = case.ccxt_factory({"enableRateLimit": True})
        ccxt_result: str
        try:
            exchange.load_markets()
            funding = exchange.fetchFundingRate(case.ccxt_symbol)
            ccxt_result = (
                "ccxt fundingRate="
                f"{funding.get('fundingRate')} | nextFundingTime={funding.get('fundingDatetime')}"
            )
        except ccxt.BaseError as exc:  # pragma: no cover - manual tool
            ccxt_result = f"ccxt error: {exc}"
        finally:
            try:
                exchange.close()
            except Exception:  # pragma: no cover - cleanup best-effort
                pass

        print(ours)
        print(ccxt_result)


if __name__ == "__main__":  # pragma: no cover - manual script
    main()
