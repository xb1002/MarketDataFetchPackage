"""Compare latest ticker snapshots against CCXT tickers."""
from __future__ import annotations

from typing import Iterable

import sys

import ccxt  # type: ignore

from compare_utils import iso_ms, iter_cases, SYMBOL


def main(targets: Iterable[str] | None = None) -> None:
    for case in iter_cases(targets):
        print(f"\n=== {case.name} latest ticker ===")
        source = case.source_factory()
        try:
            ticker = source.get_latest_ticker(SYMBOL)
            print(
                "provider",
                f"ts={iso_ms(ticker['timestamp'])}",
                f"last={ticker['last_price']}",
                f"index={ticker['index_price']}",
                f"mark={ticker['mark_price']}",
            )
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
            ccxt_ticker = exchange.fetch_ticker(case.ccxt_symbol)
            print(
                "ccxt",
                f"ts={ccxt_ticker.get('datetime')}",
                f"last={ccxt_ticker.get('last')}",
                f"index={ccxt_ticker.get('info', {}).get('indexPrice')}",
                f"mark={ccxt_ticker.get('info', {}).get('markPrice')}",
            )
        except ccxt.BaseError as exc:
            print(f"ccxt error: {exc}")
        finally:
            try:
                exchange.close()
            except Exception:
                pass


if __name__ == "__main__":  # pragma: no cover
    main(sys.argv[1:] or None)
