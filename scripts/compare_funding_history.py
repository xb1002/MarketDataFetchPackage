"""Compare funding rate history with CCXT outputs."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable

import sys

import ccxt  # type: ignore

from compare_utils import iso_ms, iter_cases, SYMBOL
from market_data_fetch.core.queries import FundingRateWindow


def main(targets: Iterable[str] | None = None) -> None:
    end = datetime.now(tz=timezone.utc)
    start = end - timedelta(hours=4)
    window = FundingRateWindow(symbol=SYMBOL, start_time=start, end_time=end, limit=20)
    for case in iter_cases(targets):
        print(f"\n=== {case.name} funding history ===")
        source = case.source_factory()
        try:
            rows = list(source.get_funding_rate_history(window))
            for ts, rate in rows[:5]:
                print(f"provider {iso_ms(ts)} rate={rate}")
        except Exception as exc:
            print(f"provider error: {exc}")
        finally:
            try:
                source.close()
            except Exception:
                pass

        exchange = case.ccxt_factory({"enableRateLimit": True})
        try:
            exchange.load_markets()
            history = exchange.fetchFundingRateHistory(case.ccxt_symbol, limit=20)
            for entry in history[:5]:
                print(f"ccxt {iso_ms(entry.get('timestamp'))} rate={entry.get('fundingRate')}")
        except ccxt.BaseError as exc:
            print(f"ccxt error: {exc}")
        finally:
            try:
                exchange.close()
            except Exception:
                pass


if __name__ == "__main__":  # pragma: no cover
    main(sys.argv[1:] or None)
