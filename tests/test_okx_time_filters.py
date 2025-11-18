from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from market_data_fetch.core.errors import ExchangeTransientError
from market_data_fetch.core.queries import HistoricalWindow
from market_data_fetch.exchanges.okx.usdt_perp import OkxUSDTPerpDataSource
from market_data_fetch.models.shared import Interval, Symbol


@pytest.fixture(scope="module")
def okx_source() -> OkxUSDTPerpDataSource:
    source = OkxUSDTPerpDataSource()
    yield source
    source.close()


def _call_or_skip(source: OkxUSDTPerpDataSource, fn):
    try:
        return fn()
    except ExchangeTransientError as exc:  # pragma: no cover - network guarded
        pytest.skip(f"okx API unavailable: {exc}")


def _ms(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


@pytest.mark.network
@pytest.mark.integration
def test_okx_price_klines_respect_start_end(okx_source: OkxUSDTPerpDataSource) -> None:
    now = datetime.now(timezone.utc)
    end_time = now - timedelta(minutes=40)
    start_time = end_time - timedelta(minutes=5)
    window = HistoricalWindow(
        symbol=Symbol("BTC", "USDT"),
        interval=Interval.MINUTE_1,
        limit=5,
        start_time=start_time,
        end_time=end_time,
    )
    klines = _call_or_skip(okx_source, lambda: okx_source.get_price_klines(window))
    assert klines
    start_ms = _ms(start_time)
    end_ms = _ms(end_time)
    for ts, *_ in klines:
        assert start_ms <= ts <= end_ms


@pytest.mark.network
@pytest.mark.integration
def test_okx_price_klines_with_start_only(okx_source: OkxUSDTPerpDataSource) -> None:
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(hours=3)
    window = HistoricalWindow(
        symbol=Symbol("BTC", "USDT"),
        interval=Interval.MINUTE_1,
        limit=10,
        start_time=start_time,
    )
    klines = _call_or_skip(okx_source, lambda: okx_source.get_price_klines(window))
    assert klines
    start_ms = _ms(start_time)
    for ts, *_ in klines:
        assert ts >= start_ms


@pytest.mark.network
@pytest.mark.integration
def test_okx_price_klines_with_end_only(okx_source: OkxUSDTPerpDataSource) -> None:
    now = datetime.now(timezone.utc)
    end_time = now - timedelta(hours=1)
    window = HistoricalWindow(
        symbol=Symbol("BTC", "USDT"),
        interval=Interval.MINUTE_1,
        limit=10,
        end_time=end_time,
    )
    klines = _call_or_skip(okx_source, lambda: okx_source.get_price_klines(window))
    assert klines
    end_ms = _ms(end_time)
    for ts, *_ in klines:
        assert ts <= end_ms
