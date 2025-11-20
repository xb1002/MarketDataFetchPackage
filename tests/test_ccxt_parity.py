from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Sequence

import ccxt
from ccxt.base.errors import BaseError as CCXTBaseError
import pytest

from market_data_fetch.contracts.usdt_perp.interface import USDTPerpMarketDataSource
from market_data_fetch.core.errors import ExchangeTransientError
from market_data_fetch.core.queries import FundingRateWindow, HistoricalWindow
from market_data_fetch.exchanges.binance.usdt_perp import BinanceUSDTPerpDataSource
from market_data_fetch.exchanges.bitget.usdt_perp import BitgetUSDTPerpDataSource
from market_data_fetch.exchanges.bybit.usdt_perp import BybitUSDTPerpDataSource
from market_data_fetch.exchanges.okx.usdt_perp import OkxUSDTPerpDataSource
from market_data_fetch.models.shared import Interval, Symbol
from market_data_fetch.models.usdt_perp import (
    USDTPerpFundingRatePoint,
    USDTPerpInstrument,
    USDTPerpKline,
    USDTPerpOpenInterest,
)

PRICE_REL_TOL = Decimal("1e-3")
VOLUME_REL_TOL = Decimal("5e-2")
FUNDING_REL_TOL = Decimal("5e-2")
FUNDING_ABS_TOL = Decimal("5e-6")
OPEN_INTEREST_REL_TOL = Decimal("5e-2")
SNAPSHOT_TIME_TOL_MS = 120_000
OPEN_INTEREST_TIME_TOL_MS = 600_000
FUNDING_NEXT_TIME_TOL_MS = 600_000


@dataclass(slots=True)
class CCXTProviderCase:
    name: str
    factory: Callable[[], USDTPerpMarketDataSource]
    symbol: Symbol
    ccxt_id: str
    ccxt_symbol: str
    history_limit: int = 20
    funding_limit: int = 20
    ccxt_options: dict[str, Any] = field(default_factory=dict)
    params: dict[str, dict[str, Any]] = field(default_factory=dict)
    supports_mark_snapshot: bool = True
    supports_premium_series: bool = True

    def params_for(self, key: str) -> dict[str, Any]:
        base = self.params.get(key, {})
        return dict(base)


CCXT_CASES: tuple[CCXTProviderCase, ...] = (
    CCXTProviderCase(
        name="binance",
        factory=BinanceUSDTPerpDataSource,
        symbol=Symbol("BTC", "USDT"),
        ccxt_id="binanceusdm",
        ccxt_symbol="BTC/USDT:USDT",
        ccxt_options={"options": {"defaultType": "future", "defaultSubType": "linear"}},
    ),
    CCXTProviderCase(
        name="bybit",
        factory=BybitUSDTPerpDataSource,
        symbol=Symbol("BTC", "USDT"),
        ccxt_id="bybit",
        ccxt_symbol="BTC/USDT:USDT",
        ccxt_options={"options": {"defaultType": "swap", "defaultSubType": "linear", "defaultSettle": "USDT"}},
        supports_mark_snapshot=False,
    ),
    CCXTProviderCase(
        name="bitget",
        factory=BitgetUSDTPerpDataSource,
        symbol=Symbol("BTC", "USDT"),
        ccxt_id="bitget",
        ccxt_symbol="BTC/USDT:USDT",
        ccxt_options={"options": {"defaultType": "swap"}},
        supports_premium_series=False,
    ),
    CCXTProviderCase(
        name="okx",
        factory=OkxUSDTPerpDataSource,
        symbol=Symbol("BTC", "USDT"),
        ccxt_id="okx",
        ccxt_symbol="BTC/USDT:USDT",
        ccxt_options={"options": {"defaultType": "swap", "defaultSettle": "USDT"}},
    ),
)


@dataclass(slots=True)
class CCXTProviderContext:
    case: CCXTProviderCase
    source: USDTPerpMarketDataSource
    ccxt: ccxt.Exchange


@pytest.fixture(scope="module", params=CCXT_CASES, ids=lambda c: c.name)
def parity_provider(request: pytest.FixtureRequest) -> CCXTProviderContext:
    case: CCXTProviderCase = request.param
    source = case.factory()
    exchange_cls = getattr(ccxt, case.ccxt_id)
    exchange_options = {"enableRateLimit": True}
    exchange_options.update(case.ccxt_options)
    exchange = exchange_cls(exchange_options)
    try:
        exchange.load_markets()
    except CCXTBaseError as exc:  # pragma: no cover - depends on remote API
        source.close()
        if hasattr(exchange, "close"):
            exchange.close()
        pytest.skip(f"ccxt {case.ccxt_id} unavailable: {exc}")
    context = CCXTProviderContext(case=case, source=source, ccxt=exchange)
    yield context
    source.close()
    if hasattr(exchange, "close"):
        exchange.close()


def _call_provider(context: CCXTProviderContext, fn: Callable[[], Any]) -> Any:
    try:
        return fn()
    except ExchangeTransientError as exc:  # pragma: no cover - depends on remote API
        pytest.skip(f"{context.case.name} provider unavailable: {exc}")


def _call_ccxt(context: CCXTProviderContext, fn: Callable[[], Any]) -> Any:
    try:
        return fn()
    except CCXTBaseError as exc:  # pragma: no cover - depends on remote API
        pytest.skip(f"ccxt {context.case.ccxt_id} unavailable: {exc}")


def _decimal_from(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if value is None:
        return Decimal("0")
    return Decimal(str(value))


def _first_not_none(*values: Any) -> Any:
    for entry in values:
        if entry not in (None, ""):
            return entry
    return None


def _assert_decimal_close(
    actual: Decimal,
    expected: Decimal,
    *,
    rel: Decimal = PRICE_REL_TOL,
    abs_tol: Decimal = Decimal("0.0001"),
    context: str,
) -> None:
    diff = abs(actual - expected)
    allowed = max(abs_tol, abs(expected) * rel)
    assert diff <= allowed, f"{context}: {actual} != {expected} (diff {diff}, allowed {allowed})"


def _convert_ccxt_klines(data: Sequence[Sequence[Any]]) -> list[USDTPerpKline]:
    klines: list[USDTPerpKline] = []
    for entry in data:
        if len(entry) < 5:
            continue
        ts = int(entry[0])
        open_px = _decimal_from(entry[1])
        high = _decimal_from(entry[2])
        low = _decimal_from(entry[3])
        close = _decimal_from(entry[4])
        volume_raw = entry[5] if len(entry) > 5 else 0
        volume = _decimal_from(volume_raw)
        klines.append((ts, open_px, high, low, close, volume))
    return klines


def _convert_ccxt_funding(data: Sequence[dict[str, Any]]) -> list[USDTPerpFundingRatePoint]:
    points: list[USDTPerpFundingRatePoint] = []
    for entry in data:
        ts = entry.get("timestamp") or entry.get("fundingRateTimestamp")
        rate = entry.get("fundingRate")
        if ts is None or rate is None:
            continue
        points.append((int(ts), _decimal_from(rate)))
    return points


def _convert_ccxt_open_interest(payload: dict[str, Any]) -> USDTPerpOpenInterest | None:
    ts = payload.get("timestamp") or payload.get("time")
    if ts is None:
        info = payload.get("info") or {}
        ts = info.get("timestamp") or info.get("time")
    value = payload.get("openInterestValue") or payload.get("quoteVolume")
    if value is None:
        value = payload.get("openInterestAmount") or payload.get("baseVolume")
    if ts is None or value is None:
        return None
    return (int(ts), _decimal_from(value))


def _match_on_timestamp(
    ours: Sequence[tuple[int, Any]],
    ccxt_series: Sequence[tuple[int, Any]],
    *,
    context: str,
) -> list[tuple[tuple[int, Any], tuple[int, Any]]]:
    ccxt_map = {row[0]: row for row in ccxt_series}
    overlaps: list[tuple[tuple[int, Any], tuple[int, Any]]] = []
    for row in ours:
        match = ccxt_map.get(row[0])
        if match:
            overlaps.append((row, match))
    if not overlaps:
        pytest.skip(f"No overlapping timestamps for {context}")
    return overlaps


def _fetch_ccxt_kline(
    context: CCXTProviderContext,
    *,
    method_name: str,
    params_key: str,
    limit: int,
) -> list[USDTPerpKline]:
    method = getattr(context.ccxt, method_name)
    params = context.case.params_for(params_key)
    raw = _call_ccxt(
        context,
        lambda: method(context.case.ccxt_symbol, timeframe=Interval.MINUTE_1.value, limit=limit, params=params),
    )
    if not raw:
        pytest.skip(f"ccxt {context.case.ccxt_id} returned empty {method_name}")
    return _convert_ccxt_klines(raw)


def _assert_kline_series(
    context: CCXTProviderContext,
    ours: Sequence[USDTPerpKline],
    ccxt_series: Sequence[USDTPerpKline],
    *,
    label: str,
) -> None:
    overlaps = _match_on_timestamp(ours, ccxt_series, context=label)
    overlaps = sorted(overlaps, key=lambda row: row[0][0])
    if len(overlaps) > 1:
        # The newest candle can still be forming between the provider and CCXT
        # requests, so drop it and compare only fully-settled entries.
        overlaps = overlaps[:-1]
    for ours_row, ccxt_row in overlaps:
        for idx, rel_tol in zip(range(1, 5), [PRICE_REL_TOL] * 4, strict=True):
            _assert_decimal_close(
                ours_row[idx],
                ccxt_row[idx],
                rel=rel_tol,
                context=f"{context.case.name} {label} field {idx}",
            )
        _assert_decimal_close(
            ours_row[5],
            ccxt_row[5],
            rel=VOLUME_REL_TOL,
            context=f"{context.case.name} {label} volume",
        )


def _maybe_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (ArithmeticError, ValueError, InvalidOperation):
        return None


def _precision_to_increment(value: Any) -> Decimal | None:
    maybe = _maybe_decimal(value)
    if maybe is None or maybe == 0:
        return None
    if maybe < 1:
        return maybe
    return Decimal("1").scaleb(-int(maybe))


def _extract_filter_value(filters: Any, filter_type: str, key: str) -> Any:
    if isinstance(filters, Sequence):
        for entry in filters:
            if isinstance(entry, dict) and entry.get("filterType") == filter_type:
                return entry.get(key)
    return None


def _expected_tick_size(market: dict[str, Any]) -> Decimal | None:
    info = market.get("info")
    if isinstance(info, dict):
        tick = (
            _maybe_decimal(info.get("priceTick"))
            or _maybe_decimal(info.get("tickSize"))
        )
        if tick is None:
            price_filter = info.get("priceFilter")
            if isinstance(price_filter, dict):
                tick = _maybe_decimal(price_filter.get("tickSize"))
        if tick is None:
            filters = info.get("filters")
            tick = _maybe_decimal(
                _extract_filter_value(filters, "PRICE_FILTER", "tickSize")
            )
        if tick is None:
            tick = _precision_to_increment(info.get("pricePrecision"))
        if tick is not None:
            return tick
    precision = market.get("precision")
    if isinstance(precision, dict):
        tick = _precision_to_increment(precision.get("price"))
        if tick is not None:
            return tick
    return None


def _expected_step_size(market: dict[str, Any]) -> Decimal | None:
    info = market.get("info")
    if isinstance(info, dict):
        lot_filter = info.get("lotSizeFilter")
        step = (
            _maybe_decimal(info.get("sizeIncrement"))
            or _maybe_decimal(info.get("quantityStep"))
        )
        if step is None and isinstance(lot_filter, dict):
            step = _maybe_decimal(lot_filter.get("qtyStep") or lot_filter.get("step"))
        if step is None:
            filters = info.get("filters")
            step = _maybe_decimal(
                _extract_filter_value(filters, "LOT_SIZE", "stepSize")
            )
        if step is None:
            step = _precision_to_increment(info.get("quantityPrecision"))
        if step is not None:
            return step
    precision = market.get("precision")
    if isinstance(precision, dict):
        step = _precision_to_increment(precision.get("amount"))
        if step is not None:
            return step
    return None


@pytest.mark.network
@pytest.mark.integration
def test_price_klines_match_ccxt(parity_provider: CCXTProviderContext) -> None:
    window = HistoricalWindow(
        symbol=parity_provider.case.symbol,
        interval=Interval.MINUTE_1,
        limit=parity_provider.case.history_limit,
    )
    ours = _call_provider(parity_provider, lambda: parity_provider.source.get_price_klines(window))
    assert ours
    ccxt_series = _fetch_ccxt_kline(
        parity_provider,
        method_name="fetch_ohlcv",
        params_key="price",
        limit=parity_provider.case.history_limit,
    )
    _assert_kline_series(parity_provider, ours, ccxt_series, label="price klines")


@pytest.mark.network
@pytest.mark.integration
def test_index_klines_match_ccxt(parity_provider: CCXTProviderContext) -> None:
    ccxt_series = _fetch_ccxt_kline(
        parity_provider,
        method_name="fetchIndexOHLCV",
        params_key="index",
        limit=parity_provider.case.history_limit,
    )
    window = HistoricalWindow(
        symbol=parity_provider.case.symbol,
        interval=Interval.MINUTE_1,
        limit=parity_provider.case.history_limit,
    )
    ours = _call_provider(
        parity_provider, lambda: parity_provider.source.get_index_price_klines(window)
    )
    assert ours
    _assert_kline_series(parity_provider, ours, ccxt_series, label="index klines")


@pytest.mark.network
@pytest.mark.integration
def test_mark_price_klines_match_ccxt(parity_provider: CCXTProviderContext) -> None:
    ccxt_series = _fetch_ccxt_kline(
        parity_provider,
        method_name="fetchMarkOHLCV",
        params_key="mark",
        limit=parity_provider.case.history_limit,
    )
    window = HistoricalWindow(
        symbol=parity_provider.case.symbol,
        interval=Interval.MINUTE_1,
        limit=parity_provider.case.history_limit,
    )
    ours = _call_provider(
        parity_provider, lambda: parity_provider.source.get_mark_price_klines(window)
    )
    assert ours
    _assert_kline_series(parity_provider, ours, ccxt_series, label="mark klines")


@pytest.mark.network
@pytest.mark.integration
def test_premium_index_klines_match_ccxt(parity_provider: CCXTProviderContext) -> None:
    if not parity_provider.case.supports_premium_series:
        pytest.skip("ccxt exchange lacks premium index endpoint")
    ccxt_series = _fetch_ccxt_kline(
        parity_provider,
        method_name="fetchPremiumIndexOHLCV",
        params_key="premium",
        limit=parity_provider.case.history_limit,
    )
    window = HistoricalWindow(
        symbol=parity_provider.case.symbol,
        interval=Interval.MINUTE_1,
        limit=parity_provider.case.history_limit,
    )
    ours = _call_provider(
        parity_provider, lambda: parity_provider.source.get_premium_index_klines(window)
    )
    assert ours
    _assert_kline_series(parity_provider, ours, ccxt_series, label="premium index klines")


@pytest.mark.network
@pytest.mark.integration
def test_funding_history_matches_ccxt(parity_provider: CCXTProviderContext) -> None:
    window = FundingRateWindow(symbol=parity_provider.case.symbol, limit=parity_provider.case.funding_limit)
    ours = _call_provider(
        parity_provider, lambda: parity_provider.source.get_funding_rate_history(window)
    )
    assert ours
    params = parity_provider.case.params_for("funding")
    ccxt_history = _call_ccxt(
        parity_provider,
        lambda: parity_provider.ccxt.fetchFundingRateHistory(
            parity_provider.case.ccxt_symbol,
            limit=parity_provider.case.funding_limit,
            params=params,
        ),
    )
    if not ccxt_history:
        pytest.skip("ccxt returned empty funding history")
    ccxt_points = _convert_ccxt_funding(ccxt_history)
    overlaps = _match_on_timestamp(ours, ccxt_points, context="funding history")
    for ours_point, ccxt_point in overlaps:
        _assert_decimal_close(
            ours_point[1],
            ccxt_point[1],
            rel=FUNDING_REL_TOL,
            abs_tol=FUNDING_ABS_TOL,
            context=f"{parity_provider.case.name} funding rate",
        )


@pytest.mark.network
@pytest.mark.integration
def test_latest_ticker_matches_ccxt(parity_provider: CCXTProviderContext) -> None:
    latest = _call_provider(
        parity_provider, lambda: parity_provider.source.get_latest_ticker(parity_provider.case.symbol)
    )
    ticker = _call_ccxt(
        parity_provider,
        lambda: parity_provider.ccxt.fetch_ticker(parity_provider.case.ccxt_symbol),
    )
    ccxt_price = ticker.get("last") or ticker.get("close")
    ccxt_timestamp = ticker.get("timestamp") or ticker.get("datetime") or ticker.get("closeTime")
    if ccxt_price is None or ccxt_timestamp is None:
        pytest.skip("ccxt ticker missing price or timestamp")
    ts = latest["timestamp"]
    _assert_decimal_close(latest["last_price"], _decimal_from(ccxt_price), context="latest price")
    index_snapshot = _call_provider(
        parity_provider, lambda: parity_provider.source.get_latest_index_price(parity_provider.case.symbol)
    )
    _assert_decimal_close(
        latest["index_price"],
        index_snapshot[1],
        context=f"{parity_provider.case.name} ticker index parity",
    )
    mark_snapshot = _call_provider(
        parity_provider, lambda: parity_provider.source.get_latest_mark_price(parity_provider.case.symbol)
    )
    _assert_decimal_close(
        latest["mark_price"],
        mark_snapshot[1],
        context=f"{parity_provider.case.name} ticker mark parity",
    )
    assert abs(ts - int(ccxt_timestamp)) <= SNAPSHOT_TIME_TOL_MS


@pytest.mark.network
@pytest.mark.integration
def test_latest_index_price_matches_ccxt(parity_provider: CCXTProviderContext) -> None:
    latest = _call_provider(
        parity_provider,
        lambda: parity_provider.source.get_latest_index_price(parity_provider.case.symbol),
    )
    ccxt_series = _fetch_ccxt_kline(
        parity_provider,
        method_name="fetchIndexOHLCV",
        params_key="index",
        limit=1,
    )
    ccxt_point = ccxt_series[-1]
    assert abs(latest[0] - ccxt_point[0]) <= SNAPSHOT_TIME_TOL_MS
    _assert_decimal_close(latest[1], ccxt_point[4], context="latest index price")



@pytest.mark.network
@pytest.mark.integration
def test_latest_mark_price_snapshot_matches_ccxt(parity_provider: CCXTProviderContext) -> None:
    if not parity_provider.case.supports_mark_snapshot:
        pytest.skip("ccxt exchange lacks mark price snapshot endpoint")
    snapshot = _call_provider(
        parity_provider,
        lambda: parity_provider.source.get_latest_mark_price(parity_provider.case.symbol),
    )
    params = parity_provider.case.params_for("mark")
    ccxt_snapshot = _call_ccxt(
        parity_provider,
        lambda: parity_provider.ccxt.fetchMarkPrice(parity_provider.case.ccxt_symbol, params=params),
    )
    info = ccxt_snapshot.get("info", {})
    ccxt_mark = ccxt_snapshot.get("last") or info.get("markPrice")
    ccxt_time = ccxt_snapshot.get("timestamp") or info.get("time") or info.get("timestamp")
    if None in (ccxt_mark, ccxt_time):
        pytest.skip("ccxt mark price snapshot missing mark value")
    ts, mark_price = snapshot
    _assert_decimal_close(mark_price, _decimal_from(ccxt_mark), context="mark price value")
    assert abs(ts - int(ccxt_time)) <= SNAPSHOT_TIME_TOL_MS


@pytest.mark.network
@pytest.mark.integration
def test_latest_funding_rate_matches_ccxt(parity_provider: CCXTProviderContext) -> None:
    if not hasattr(parity_provider.ccxt, "fetchFundingRate"):
        pytest.skip("ccxt exchange lacks fetchFundingRate")
    latest = _call_provider(
        parity_provider,
        lambda: parity_provider.source.get_latest_funding_rate(parity_provider.case.symbol),
    )
    params = parity_provider.case.params_for("funding")
    try:
        ccxt_snapshot = _call_ccxt(
            parity_provider,
            lambda: parity_provider.ccxt.fetchFundingRate(
                parity_provider.case.ccxt_symbol, params=params
            ),
        )
    except AttributeError:  # pragma: no cover - depends on ccxt version
        pytest.skip("ccxt exchange lacks fetchFundingRate")
    info = ccxt_snapshot.get("info", {}) if isinstance(ccxt_snapshot, dict) else {}
    ccxt_rate = _first_not_none(
        ccxt_snapshot.get("fundingRate") if isinstance(ccxt_snapshot, dict) else None,
        info.get("fundingRate"),
        info.get("estFundingRate"),
        info.get("lastFundingRate"),
        info.get("nextFundingRate"),
    )
    ccxt_next_time = _first_not_none(
        ccxt_snapshot.get("nextFundingTime") if isinstance(ccxt_snapshot, dict) else None,
        ccxt_snapshot.get("nextFundingTimestamp") if isinstance(ccxt_snapshot, dict) else None,
        info.get("nextFundingTime"),
        info.get("nextFundingTimestamp"),
        info.get("nextFundingRateTime"),
    )
    if ccxt_rate in (None, "") or ccxt_next_time in (None, ""):
        pytest.skip("ccxt funding snapshot missing fields")
    _assert_decimal_close(
        latest["funding_rate"],
        _decimal_from(ccxt_rate),
        rel=FUNDING_REL_TOL,
        abs_tol=FUNDING_ABS_TOL,
        context="latest funding rate",
    )
    assert (
        abs(latest["next_funding_time"] - int(ccxt_next_time)) <= FUNDING_NEXT_TIME_TOL_MS
    ), "next funding timestamps diverged"


@pytest.mark.network
@pytest.mark.integration
def test_open_interest_matches_ccxt(parity_provider: CCXTProviderContext) -> None:
    ours = _call_provider(
        parity_provider, lambda: parity_provider.source.get_open_interest(parity_provider.case.symbol)
    )
    params = parity_provider.case.params_for("open_interest")
    ccxt_snapshot = _call_ccxt(
        parity_provider,
        lambda: parity_provider.ccxt.fetchOpenInterest(parity_provider.case.ccxt_symbol, params=params),
    )
    parsed = _convert_ccxt_open_interest(ccxt_snapshot)
    if parsed is None:
        pytest.skip("ccxt open interest payload missing value")
    assert abs(ours[0] - parsed[0]) <= OPEN_INTEREST_TIME_TOL_MS
    _assert_decimal_close(
        ours[1],
        parsed[1],
        rel=OPEN_INTEREST_REL_TOL,
        context="open interest value",
    )


@pytest.mark.network
@pytest.mark.integration
def test_instrument_metadata_matches_ccxt(parity_provider: CCXTProviderContext) -> None:
    instruments = _call_provider(parity_provider, parity_provider.source.get_instruments)
    assert instruments
    target_symbol = parity_provider.case.symbol.pair
    ours = next((item for item in instruments if item["symbol"] == target_symbol), None)
    assert ours is not None, "instrument not found in provider payload"
    market = parity_provider.ccxt.market(parity_provider.case.ccxt_symbol)
    assert ours["base_asset"] == market["base"]
    assert ours["quote_asset"] == market["quote"]
    tick_size = _expected_tick_size(market)
    if tick_size is not None:
        _assert_decimal_close(
            ours["tick_size"],
            tick_size,
            context="instrument tick size",
        )
    step_size = _expected_step_size(market)
    if step_size is not None:
        _assert_decimal_close(
            ours["step_size"],
            step_size,
            context="instrument step size",
        )
    limits = market.get("limits", {})
    min_qty = limits.get("amount", {}).get("min")
    if min_qty is not None:
        _assert_decimal_close(
            ours["min_qty"],
            _decimal_from(min_qty),
            context="instrument min qty",
        )
    max_qty = limits.get("amount", {}).get("max")
    if max_qty is not None:
        _assert_decimal_close(
            ours["max_qty"],
            _decimal_from(max_qty),
            context="instrument max qty",
        )
