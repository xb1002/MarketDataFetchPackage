from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pytest

from market_data_fetch.contracts.usdt_perp.interface import USDTPerpMarketDataSource
from market_data_fetch.core.errors import ExchangeTransientError
from market_data_fetch.core.queries import HistoricalWindow
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
    except ExchangeTransientError as exc:  # pragma: no cover - depends on live API
        pytest.skip(f"{context.case.name} API unavailable: {exc}")


def _ms(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


@pytest.mark.network
@pytest.mark.integration
def test_price_klines_respect_start_end(provider: ProviderContext) -> None:
    now = datetime.now(timezone.utc)
    end_time = now - timedelta(minutes=40)
    start_time = end_time - timedelta(minutes=5)
    window = HistoricalWindow(
        symbol=provider.case.symbol,
        interval=Interval.MINUTE_1,
        limit=5,
        start_time=start_time,
        end_time=end_time,
    )
    klines = _call_or_skip(provider, lambda: provider.source.get_price_klines(window))
    assert klines
    start_ms = _ms(start_time)
    end_ms = _ms(end_time)
    for ts, *_ in klines:
        assert start_ms <= ts <= end_ms


@pytest.mark.network
@pytest.mark.integration
def test_price_klines_with_start_only(provider: ProviderContext) -> None:
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(hours=3)
    window = HistoricalWindow(
        symbol=provider.case.symbol,
        interval=Interval.MINUTE_1,
        limit=10,
        start_time=start_time,
    )
    klines = _call_or_skip(provider, lambda: provider.source.get_price_klines(window))
    assert klines
    start_ms = _ms(start_time)
    for ts, *_ in klines:
        assert ts >= start_ms


@pytest.mark.network
@pytest.mark.integration
def test_price_klines_with_end_only(provider: ProviderContext) -> None:
    now = datetime.now(timezone.utc)
    end_time = now - timedelta(hours=1)
    window = HistoricalWindow(
        symbol=provider.case.symbol,
        interval=Interval.MINUTE_1,
        limit=10,
        end_time=end_time,
    )
    klines = _call_or_skip(provider, lambda: provider.source.get_price_klines(window))
    assert klines
    end_ms = _ms(end_time)
    for ts, *_ in klines:
        assert ts <= end_ms


@pytest.mark.network
@pytest.mark.integration
def test_price_klines_without_explicit_limit(provider: ProviderContext) -> None:
    now = datetime.now(timezone.utc)
    end_time = now - timedelta(minutes=20)
    start_time = end_time - timedelta(minutes=3)
    window = HistoricalWindow(
        symbol=provider.case.symbol,
        interval=Interval.MINUTE_1,
        start_time=start_time,
        end_time=end_time,
    )
    klines = _call_or_skip(provider, lambda: provider.source.get_price_klines(window))
    assert klines
    start_ms = _ms(start_time)
    end_ms = _ms(end_time)
    for ts, *_ in klines:
        assert start_ms <= ts <= end_ms
