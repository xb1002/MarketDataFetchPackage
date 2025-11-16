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
    ts, open_price, *_ = klines[0]
    assert isinstance(ts, int)
    assert isinstance(open_price, Decimal)


@pytest.mark.network
@pytest.mark.integration
def test_get_index_price_klines_live(source: BinanceUSDTPerpDataSource, symbol: Symbol) -> None:
    window = HistoricalWindow(symbol=symbol, interval=Interval.MINUTE_1, limit=3)
    klines = _call_or_skip(lambda: source.get_index_price_klines(window))

    assert len(klines) > 0
    ts, _, _, _, close, _ = klines[0]
    assert isinstance(ts, int)
    assert isinstance(close, Decimal)


@pytest.mark.network
@pytest.mark.integration
def test_get_mark_price_klines_live(source: BinanceUSDTPerpDataSource, symbol: Symbol) -> None:
    window = HistoricalWindow(symbol=symbol, interval=Interval.MINUTE_1, limit=3)
    klines = _call_or_skip(lambda: source.get_mark_price_klines(window))

    assert len(klines) > 0
    assert isinstance(klines[0][0], int)


@pytest.mark.network
@pytest.mark.integration
def test_get_premium_index_klines_live(source: BinanceUSDTPerpDataSource, symbol: Symbol) -> None:
    window = HistoricalWindow(symbol=symbol, interval=Interval.MINUTE_1, limit=3)
    klines = _call_or_skip(lambda: source.get_premium_index_klines(window))

    assert len(klines) > 0
    assert isinstance(klines[0][4], Decimal)


@pytest.mark.network
@pytest.mark.integration
def test_get_funding_rate_history_live(source: BinanceUSDTPerpDataSource, symbol: Symbol) -> None:
    end_time = datetime.now(tz=timezone.utc)
    start_time = end_time - timedelta(days=3)
    window = FundingRateWindow(symbol=symbol, start_time=start_time, end_time=end_time, limit=5)
    points = _call_or_skip(lambda: source.get_funding_rate_history(window))

    assert len(points) > 0
    funding_time, funding_rate = points[0]
    assert isinstance(funding_time, int)
    assert isinstance(funding_rate, Decimal)


@pytest.mark.network
@pytest.mark.integration
def test_get_latest_price_live(source: BinanceUSDTPerpDataSource, symbol: Symbol) -> None:
    price, ts = _call_or_skip(lambda: source.get_latest_price(symbol))

    assert isinstance(price, Decimal)
    assert isinstance(ts, int)


@pytest.mark.network
@pytest.mark.integration
def test_get_latest_mark_price_live(source: BinanceUSDTPerpDataSource, symbol: Symbol) -> None:
    snapshot = _call_or_skip(lambda: source.get_latest_mark_price(symbol))

    mark_price, index_price, last_funding_rate, next_funding_ts = snapshot
    assert isinstance(mark_price, Decimal)
    assert isinstance(index_price, Decimal)
    assert isinstance(last_funding_rate, Decimal)
    assert isinstance(next_funding_ts, int)


@pytest.mark.network
@pytest.mark.integration
def test_get_latest_index_price_live(source: BinanceUSDTPerpDataSource, symbol: Symbol) -> None:
    value, ts = _call_or_skip(lambda: source.get_latest_index_price(symbol))

    assert isinstance(value, Decimal)
    assert isinstance(ts, int)


@pytest.mark.network
@pytest.mark.integration
def test_get_latest_premium_index_live(source: BinanceUSDTPerpDataSource, symbol: Symbol) -> None:
    value, ts = _call_or_skip(lambda: source.get_latest_premium_index(symbol))

    assert isinstance(value, Decimal)
    assert isinstance(ts, int)


@pytest.mark.network
@pytest.mark.integration
def test_get_latest_funding_rate_point_live(source: BinanceUSDTPerpDataSource, symbol: Symbol) -> None:
    point = _call_or_skip(lambda: source.get_latest_funding_rate(symbol))

    assert isinstance(point[0], int)
    assert isinstance(point[1], Decimal)


@pytest.mark.network
@pytest.mark.integration
def test_get_open_interest_snapshot_live(source: BinanceUSDTPerpDataSource, symbol: Symbol) -> None:
    snapshot = _call_or_skip(lambda: source.get_open_interest(symbol))

    assert isinstance(snapshot[0], int)
    assert isinstance(snapshot[1], Decimal)


def test_price_kline_limit_validation(symbol: Symbol) -> None:
    client = BinanceUSDTPerpDataSource(base_url=TESTNET_BASE_URL)
    window = HistoricalWindow(symbol=symbol, interval=Interval.MINUTE_1, limit=2000)

    with pytest.raises(ValueError):
        client.get_price_klines(window)

    client.close()


def test_funding_limit_validation(symbol: Symbol) -> None:
    client = BinanceUSDTPerpDataSource(base_url=TESTNET_BASE_URL)
    window = FundingRateWindow(symbol=symbol, limit=1500)

    with pytest.raises(ValueError):
        client.get_funding_rate_history(window)

    client.close()
