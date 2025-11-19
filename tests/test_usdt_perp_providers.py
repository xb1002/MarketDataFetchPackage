from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from market_data_fetch.contracts.usdt_perp.interface import USDTPerpMarketDataSource
from market_data_fetch.core.errors import ExchangeTransientError
from market_data_fetch.core.queries import FundingRateWindow, HistoricalWindow
from market_data_fetch.models.shared import Interval
from tests.provider_cases import PROVIDERS, ProviderCase


@dataclass(slots=True)
class ProviderContext:
    case: ProviderCase
    source: USDTPerpMarketDataSource


@pytest.fixture(scope="module", params=PROVIDERS, ids=lambda case: case.name)
def provider(request: pytest.FixtureRequest) -> ProviderContext:
    case: ProviderCase = request.param
    source = case.factory()
    context = ProviderContext(case=case, source=source)
    yield context
    source.close()


def _call_or_skip(context: ProviderContext, fn):
    try:
        return fn()
    except ExchangeTransientError as exc:  # pragma: no cover - depends on external service
        pytest.skip(f"{context.case.name} API unavailable: {exc}")


@pytest.mark.network
@pytest.mark.integration
def test_get_price_klines_live(provider: ProviderContext) -> None:
    window = HistoricalWindow(symbol=provider.case.symbol, interval=Interval.MINUTE_1, limit=5)
    klines = _call_or_skip(provider, lambda: provider.source.get_price_klines(window))

    assert len(klines) > 0
    ts, open_price, *_ = klines[0]
    assert isinstance(ts, int)
    assert isinstance(open_price, Decimal)


@pytest.mark.network
@pytest.mark.integration
def test_get_index_price_klines_live(provider: ProviderContext) -> None:
    window = HistoricalWindow(symbol=provider.case.symbol, interval=Interval.MINUTE_1, limit=3)
    klines = _call_or_skip(provider, lambda: provider.source.get_index_price_klines(window))

    assert len(klines) > 0
    assert isinstance(klines[0][0], int)


@pytest.mark.network
@pytest.mark.integration
def test_get_mark_price_klines_live(provider: ProviderContext) -> None:
    window = HistoricalWindow(symbol=provider.case.symbol, interval=Interval.MINUTE_1, limit=3)
    klines = _call_or_skip(provider, lambda: provider.source.get_mark_price_klines(window))

    assert len(klines) > 0
    assert isinstance(klines[0][4], Decimal)


@pytest.mark.network
@pytest.mark.integration
def test_get_premium_index_klines_live(provider: ProviderContext) -> None:
    window = HistoricalWindow(symbol=provider.case.symbol, interval=Interval.MINUTE_1, limit=3)
    klines = _call_or_skip(provider, lambda: provider.source.get_premium_index_klines(window))

    assert len(klines) > 0
    assert isinstance(klines[0][5], Decimal)


@pytest.mark.network
@pytest.mark.integration
def test_get_funding_rate_history_live(provider: ProviderContext) -> None:
    end_time = datetime.now(tz=timezone.utc)
    start_time = end_time - timedelta(days=3)
    window = FundingRateWindow(
        symbol=provider.case.symbol,
        start_time=start_time,
        end_time=end_time,
        limit=5,
    )
    points = _call_or_skip(provider, lambda: provider.source.get_funding_rate_history(window))

    assert len(points) > 0
    funding_time, funding_rate = points[0]
    assert isinstance(funding_time, int)
    assert isinstance(funding_rate, Decimal)


@pytest.mark.network
@pytest.mark.integration
def test_get_latest_price_live(provider: ProviderContext) -> None:
    ticker = _call_or_skip(
        provider, lambda: provider.source.get_latest_price(provider.case.symbol)
    )

    assert ticker["timestamp"] > 0
    assert isinstance(ticker["timestamp"], int)
    assert isinstance(ticker["last_price"], Decimal)
    assert isinstance(ticker["bid_price"], Decimal)
    assert isinstance(ticker["ask_price"], Decimal)


@pytest.mark.network
@pytest.mark.integration
def test_get_latest_mark_price_live(provider: ProviderContext) -> None:
    snapshot = _call_or_skip(provider, lambda: provider.source.get_latest_mark_price(provider.case.symbol))

    timestamp, mark_price = snapshot
    assert isinstance(timestamp, int)
    assert isinstance(mark_price, Decimal)


@pytest.mark.network
@pytest.mark.integration
def test_get_latest_index_price_live(provider: ProviderContext) -> None:
    ts, value = _call_or_skip(
        provider, lambda: provider.source.get_latest_index_price(provider.case.symbol)
    )

    assert isinstance(ts, int)
    assert isinstance(value, Decimal)


@pytest.mark.network
@pytest.mark.integration
def test_get_latest_premium_index_live(provider: ProviderContext) -> None:
    ts, value = _call_or_skip(
        provider, lambda: provider.source.get_latest_premium_index(provider.case.symbol)
    )

    assert isinstance(ts, int)
    assert isinstance(value, Decimal)


@pytest.mark.network
@pytest.mark.integration
def test_get_latest_funding_rate_point_live(provider: ProviderContext) -> None:
    point = _call_or_skip(provider, lambda: provider.source.get_latest_funding_rate(provider.case.symbol))

    assert isinstance(point[0], int)
    assert isinstance(point[1], Decimal)


@pytest.mark.network
@pytest.mark.integration
def test_get_open_interest_snapshot_live(provider: ProviderContext) -> None:
    snapshot = _call_or_skip(provider, lambda: provider.source.get_open_interest(provider.case.symbol))

    assert isinstance(snapshot[0], int)
    assert isinstance(snapshot[1], Decimal)


@pytest.mark.network
@pytest.mark.integration
def test_get_instruments(provider: ProviderContext) -> None:
    instruments = _call_or_skip(provider, lambda: provider.source.get_instruments())

    assert len(instruments) > 0
    target = provider.case.symbol.pair
    match = next((item for item in instruments if item["symbol"] == target), None)
    assert match is not None
    assert isinstance(match["base_asset"], str)
    assert isinstance(match["quote_asset"], str)
    assert isinstance(match["tick_size"], Decimal)
    assert isinstance(match["step_size"], Decimal)
    assert isinstance(match["min_qty"], Decimal)
    assert isinstance(match["max_qty"], Decimal)
    assert isinstance(match["status"], str)


def test_price_kline_limit_validation(provider: ProviderContext) -> None:
    window = HistoricalWindow(
        symbol=provider.case.symbol,
        interval=Interval.MINUTE_1,
        limit=provider.case.price_limit + 1,
    )

    with pytest.raises(ValueError):
        provider.source.get_price_klines(window)


def test_funding_limit_validation(provider: ProviderContext) -> None:
    window = FundingRateWindow(symbol=provider.case.symbol, limit=provider.case.funding_limit + 1)

    with pytest.raises(ValueError):
        provider.source.get_funding_rate_history(window)
