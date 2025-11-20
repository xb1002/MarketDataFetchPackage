"""Compare latest funding rates with CCXT outputs for reference."""
from __future__ import annotations

from typing import Iterable

import sys

import ccxt  # type: ignore

from compare_utils import ProviderCase, iso_ms, iter_cases, SYMBOL


def _print_header(case: ProviderCase) -> None:
    print(f"\n=== {case.name} ===")


def main(targets: Iterable[str] | None = None) -> None:
    for case in iter_cases(targets):
        _print_header(case)
        source = case.source_factory()
        try:
            snapshot = source.get_latest_funding_rate(SYMBOL)
            ours = (
                f"provider funding_rate={snapshot['funding_rate']} "
                f"| next_funding_time={iso_ms(snapshot['next_funding_time'])}"
            )
        except Exception as exc:  # pragma: no cover - manual script
            ours = f"provider error: {exc}"
        finally:
            try:
                source.close()
            except Exception:  # pragma: no cover - defensive cleanup
                pass

        exchange = case.ccxt_factory({"enableRateLimit": True})
        try:
            exchange.load_markets()
            funding = exchange.fetchFundingRate(case.ccxt_symbol)
            ccxt_result = (
                f"ccxt fundingRate={funding.get('fundingRate')} "
                f"| nextFundingTime={funding.get('fundingDatetime')}"
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
    main(sys.argv[1:] or None)
