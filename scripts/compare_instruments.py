"""Compare instrument metadata against CCXT market definitions."""
from __future__ import annotations

from typing import Iterable

import sys

import ccxt  # type: ignore

from compare_utils import iter_cases, SYMBOL


def main(targets: Iterable[str] | None = None) -> None:
    for case in iter_cases(targets):
        print(f"\n=== {case.name} instruments ===")
        source = case.source_factory()
        try:
            instruments = source.get_instruments()
            ours = next((item for item in instruments if item["symbol"] == SYMBOL.pair), None)
            print(f"provider {ours}")
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
            market = exchange.market(case.ccxt_symbol)
            print(
                "ccxt",
                {
                    "symbol": market.get("symbol"),
                    "base": market.get("base"),
                    "quote": market.get("quote"),
                    "tick_size": market.get("limits", {})
                    .get("price", {})
                    .get("min"),
                    "step_size": market.get("limits", {})
                    .get("amount", {})
                    .get("min"),
                    "status": market.get("active"),
                },
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
