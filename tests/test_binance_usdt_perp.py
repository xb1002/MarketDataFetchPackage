from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from market_data_fetch.core.queries import FundingRateWindow, HistoricalWindow
from market_data_fetch.exchanges.binance import usdt_perp as binance_module
from market_data_fetch.exchanges.binance.usdt_perp import BinanceUSDTPerpDataSource
from market_data_fetch.models.shared import Interval, Symbol


class StubResponse:
    def __init__(self, payload, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload


class StubSession:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self._responses: list[StubResponse] = []

    def queue(self, payload, status_code: int = 200) -> None:
        self._responses.append(StubResponse(payload, status_code))

    def get(self, url, params=None, timeout=0):
        if params is None:
            params = {}
        self.calls.append({"url": url, "params": dict(params), "timeout": timeout})
        if not self._responses:
            raise AssertionError("No queued response left for stub session")
        return self._responses.pop(0)


@pytest.fixture()
def symbol() -> Symbol:
    return Symbol("BTC", "USDT")


@pytest.fixture()
def session_and_source():
    session = StubSession()
    source = BinanceUSDTPerpDataSource(session=session)
    return session, source


def _ts(value: datetime) -> int:
    return int(value.replace(tzinfo=timezone.utc).timestamp() * 1000)


def test_get_price_klines_parses_entries(session_and_source, symbol):
    session, source = session_and_source
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 1, 2, tzinfo=timezone.utc)
    window = HistoricalWindow(symbol=symbol, interval=Interval.MINUTE_1, start_time=start, end_time=end, limit=2)
    session.queue(
        [
            [_ts(start), "10", "11", "9", "10.5", "100", _ts(start) + 60_000, "200"],
            [_ts(start) + 60_000, "10.5", "12", "10", "11", "120", _ts(start) + 120_000, "300"],
        ]
    )

    klines = source.get_price_klines(window)

    assert len(klines) == 2
    assert klines[0].symbol == symbol
    assert klines[0].open == Decimal("10")
    assert session.calls[0]["url"].endswith(binance_module.PRICE_KLINES_ENDPOINT)
    assert session.calls[0]["params"]["symbol"] == symbol.pair
    assert session.calls[0]["params"]["startTime"] == _ts(start)
    assert session.calls[0]["params"]["endTime"] == _ts(end)


def test_get_index_price_klines_uses_pair_key(session_and_source, symbol):
    session, source = session_and_source
    window = HistoricalWindow(symbol=symbol, interval=Interval.HOUR_1, limit=1)
    session.queue([[1700000000000, "1", "1", "1", "1", "0", 1700000005000, "0"]])

    result = source.get_index_price_klines(window)

    assert result[0].close == Decimal("1")
    assert session.calls[-1]["url"].endswith(binance_module.INDEX_KLINES_ENDPOINT)
    assert session.calls[-1]["params"]["pair"] == symbol.pair


def test_get_premium_index_klines(session_and_source, symbol):
    session, source = session_and_source
    window = HistoricalWindow(symbol=symbol, interval=Interval.HOUR_1, limit=1)
    session.queue([[1700000100000, "2", "2", "2", "2", "0", 1700000105000, "0"]])

    result = source.get_premium_index_klines(window)

    assert result[0].close == Decimal("2")
    assert session.calls[-1]["url"].endswith(binance_module.PREMIUM_KLINES_ENDPOINT)
    assert session.calls[-1]["params"]["pair"] == symbol.pair


def test_get_funding_rate_history(session_and_source, symbol):
    session, source = session_and_source
    start = datetime(2024, 2, 1, tzinfo=timezone.utc)
    window = FundingRateWindow(symbol=symbol, start_time=start, limit=1)
    session.queue([
        {
            "fundingTime": _ts(start),
            "fundingRate": "0.0001",
            "predictedFundingRate": "0.0002",
        }
    ])

    points = source.get_funding_rate_history(window)

    assert points[0].rate == Decimal("0.0001")
    assert points[0].predicted_rate == Decimal("0.0002")
    assert session.calls[-1]["url"].endswith(binance_module.FUNDING_HISTORY_ENDPOINT)
    assert session.calls[-1]["params"]["symbol"] == symbol.pair


def test_get_latest_price_snapshot(session_and_source, symbol):
    session, source = session_and_source
    session.queue(
        {
            "markPrice": "100",
            "indexPrice": "101",
            "lastFundingRate": "0.00025",
            "nextFundingTime": 1700001000000,
        }
    )

    snapshot = source.get_latest_price(symbol)

    assert snapshot.price == Decimal("100")
    assert snapshot.index_price == Decimal("101")
    assert snapshot.last_funding_rate == Decimal("0.00025")
    assert session.calls[-1]["url"].endswith(binance_module.PREMIUM_INDEX_ENDPOINT)


def test_get_latest_index_price_uses_minute_interval(session_and_source, symbol):
    session, source = session_and_source
    session.queue([[1700002000000, "5", "5", "5", "5", "0", 1700002005000, "0"]])

    kline = source.get_latest_index_price(symbol)

    assert kline.close == Decimal("5")
    call = session.calls[-1]
    assert call["url"].endswith(binance_module.INDEX_KLINES_ENDPOINT)
    assert call["params"]["pair"] == symbol.pair
    assert call["params"]["limit"] == 1


def test_get_latest_premium_index(session_and_source, symbol):
    session, source = session_and_source
    session.queue([[1700003000000, "6", "6", "6", "6", "0", 1700003005000, "0"]])

    kline = source.get_latest_premium_index(symbol)

    assert kline.close == Decimal("6")
    assert session.calls[-1]["url"].endswith(binance_module.PREMIUM_KLINES_ENDPOINT)


def test_get_latest_funding_rate_point(session_and_source, symbol):
    session, source = session_and_source
    session.queue(
        [
            {
                "fundingTime": 1700004000000,
                "fundingRate": "0.0003",
                "predictedFundingRate": "0.0004",
            }
        ]
    )

    point = source.get_latest_funding_rate(symbol)

    assert point.rate == Decimal("0.0003")
    assert session.calls[-1]["params"]["limit"] == 1


def test_get_open_interest_snapshot(session_and_source, symbol):
    session, source = session_and_source
    session.queue({"time": 1700005000000, "openInterest": "123.45"})

    snapshot = source.get_open_interest(symbol)

    assert snapshot.value == Decimal("123.45")
    assert session.calls[-1]["url"].endswith(binance_module.OPEN_INTEREST_ENDPOINT)
