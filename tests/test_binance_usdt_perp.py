from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from market_data_fetch.core.errors import ExchangeTransientError
from market_data_fetch.core.queries import FundingRateWindow, HistoricalWindow
from market_data_fetch.exchanges.binance.usdt_perp import BinanceUSDTPerpDataSource
from market_data_fetch.models.shared import Interval, Symbol

TESTNET_BASE_URL = "https://testnet.binancefuture.com"


@pytest.fixture(scope="module")
def symbol() -> Symbol:
    return Symbol("BTC", "USDT")


@pytest.fixture(scope="module")
def source() -> BinanceUSDTPerpDataSource:
    client = BinanceUSDTPerpDataSource(base_url=TESTNET_BASE_URL)
    yield client
    client.close()


def _call_or_skip(fn):
    try:
        return fn()
    except ExchangeTransientError as exc:  # pragma: no cover - depends on external service
        pytest.skip(f"Binance API unavailable: {exc}")


@pytest.mark.network
@pytest.mark.integration
def test_get_price_klines_live(source: BinanceUSDTPerpDataSource, symbol: Symbol) -> None:
    window = HistoricalWindow(symbol=symbol, interval=Interval.MINUTE_1, limit=5)
    klines = _call_or_skip(lambda: source.get_price_klines(window))

    assert len(klines) > 0
    assert all(k.symbol == symbol for k in klines)
    assert all(k.close_time >= k.open_time for k in klines)


@pytest.mark.network
@pytest.mark.integration
def test_get_index_price_klines_live(source: BinanceUSDTPerpDataSource, symbol: Symbol) -> None:
    window = HistoricalWindow(symbol=symbol, interval=Interval.MINUTE_1, limit=3)
    klines = _call_or_skip(lambda: source.get_index_price_klines(window))

    assert len(klines) > 0
    assert all(k.symbol == symbol for k in klines)


@pytest.mark.network
@pytest.mark.integration
def test_get_premium_index_klines_live(source: BinanceUSDTPerpDataSource, symbol: Symbol) -> None:
    window = HistoricalWindow(symbol=symbol, interval=Interval.MINUTE_1, limit=3)
    klines = _call_or_skip(lambda: source.get_premium_index_klines(window))

    assert len(klines) > 0
    assert all(k.symbol == symbol for k in klines)


@pytest.mark.network
@pytest.mark.integration
def test_get_funding_rate_history_live(source: BinanceUSDTPerpDataSource, symbol: Symbol) -> None:
    end_time = datetime.now(tz=timezone.utc)
    start_time = end_time - timedelta(days=3)
    window = FundingRateWindow(symbol=symbol, start_time=start_time, end_time=end_time, limit=5)
    points = _call_or_skip(lambda: source.get_funding_rate_history(window))

    assert len(points) > 0
    assert all(point.symbol == symbol for point in points)
    assert all(isinstance(point.rate, Decimal) for point in points)


@pytest.mark.network
@pytest.mark.integration
def test_get_latest_price_snapshot_live(source: BinanceUSDTPerpDataSource, symbol: Symbol) -> None:
    snapshot = _call_or_skip(lambda: source.get_latest_price(symbol))

    assert snapshot.symbol == symbol
    assert isinstance(snapshot.price, Decimal)
    assert isinstance(snapshot.index_price, Decimal)


@pytest.mark.network
@pytest.mark.integration
def test_get_latest_index_price_live(source: BinanceUSDTPerpDataSource, symbol: Symbol) -> None:
    kline = _call_or_skip(lambda: source.get_latest_index_price(symbol))

    assert kline.symbol == symbol
    assert isinstance(kline.close, Decimal)


@pytest.mark.network
@pytest.mark.integration
def test_get_latest_premium_index_live(source: BinanceUSDTPerpDataSource, symbol: Symbol) -> None:
    kline = _call_or_skip(lambda: source.get_latest_premium_index(symbol))

    assert kline.symbol == symbol
    assert isinstance(kline.close, Decimal)


@pytest.mark.network
@pytest.mark.integration
def test_get_latest_funding_rate_point_live(source: BinanceUSDTPerpDataSource, symbol: Symbol) -> None:
    point = _call_or_skip(lambda: source.get_latest_funding_rate(symbol))

    assert point.symbol == symbol
    assert isinstance(point.rate, Decimal)


@pytest.mark.network
@pytest.mark.integration
def test_get_open_interest_snapshot_live(source: BinanceUSDTPerpDataSource, symbol: Symbol) -> None:
    snapshot = _call_or_skip(lambda: source.get_open_interest(symbol))

    assert snapshot.symbol == symbol
    assert isinstance(snapshot.value, Decimal)
