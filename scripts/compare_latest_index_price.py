"""Compare latest index price snapshots against CCXT."""
from __future__ import annotations

from typing import Iterable

import sys

import ccxt  # type: ignore

from compare_utils import iso_ms, iter_cases, SYMBOL


def main(targets: Iterable[str] | None = None) -> None:
    for case in iter_cases(targets):
        print(f"\n=== {case.name} latest index price ===")
        source = case.source_factory()
        try:
            ts, value = source.get_latest_index_price(SYMBOL)
            print(f"provider ts={iso_ms(ts)} value={value}")
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
            kline = exchange.fetchIndexOHLCV(case.ccxt_symbol, timeframe="1m", limit=1)[-1]
            print(f"ccxt ts={iso_ms(kline[0])} value={kline[4]}")
        except ccxt.BaseError as exc:
            print(f"ccxt error: {exc}")
        finally:
            try:
                exchange.close()
            except Exception:
                pass


if __name__ == "__main__":  # pragma: no cover
    main(sys.argv[1:] or None)
