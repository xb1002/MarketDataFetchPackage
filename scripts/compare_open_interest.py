"""Compare open interest snapshots against CCXT outputs."""
from __future__ import annotations

from typing import Iterable

import sys

import ccxt  # type: ignore

from compare_utils import iso_ms, iter_cases, SYMBOL


def main(targets: Iterable[str] | None = None) -> None:
    for case in iter_cases(targets):
        print(f"\n=== {case.name} open interest ===")
        source = case.source_factory()
        try:
            ts, value = source.get_open_interest(SYMBOL)
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
            snapshot = exchange.fetchOpenInterest(case.ccxt_symbol)
            print(
                "ccxt",
                f"ts={snapshot.get('timestamp') and iso_ms(snapshot.get('timestamp'))}",
                f"value={snapshot.get('openInterestAmount') or snapshot.get('openInterestValue')}",
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
