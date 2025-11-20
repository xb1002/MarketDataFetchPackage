"""Compare index price klines from providers against CCXT when available."""
from __future__ import annotations

from typing import Iterable

import sys

import ccxt  # type: ignore

from compare_utils import iso_ms, iter_cases, SYMBOL
from market_data_fetch.core.queries import HistoricalWindow
from market_data_fetch.models.shared import Interval


def _print_series(label: str, series: list[tuple]) -> None:
    print(label)
    for ts, *_rest, close, vol in series[:5]:
        print(f"  {iso_ms(ts)} close={close} vol={vol}")


def main(targets: Iterable[str] | None = None) -> None:
    window = HistoricalWindow(symbol=SYMBOL, interval=Interval.MINUTE_1, limit=5)
    for case in iter_cases(targets):
        print(f"\n=== {case.name} index klines ===")
        source = case.source_factory()
        try:
            ours = list(source.get_index_price_klines(window))
            _print_series("provider", ours)
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
            if not hasattr(exchange, "fetchIndexOHLCV"):
                raise ccxt.NotSupported("fetchIndexOHLCV not supported")
            klines = exchange.fetchIndexOHLCV(case.ccxt_symbol, timeframe="1m", limit=5)
            formatted = [(k[0], k[1], k[2], k[3], k[4], k[5] if len(k) > 5 else 0) for k in klines]
            _print_series("ccxt", formatted)
        except ccxt.BaseError as exc:
            print(f"ccxt error: {exc}")
        finally:
            try:
                exchange.close()
            except Exception:
                pass


if __name__ == "__main__":  # pragma: no cover
    main(sys.argv[1:] or None)
