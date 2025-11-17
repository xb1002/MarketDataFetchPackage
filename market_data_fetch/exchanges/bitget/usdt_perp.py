"""Bitget USDT perpetual market data implementation."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterable, Sequence

import requests

from ...contracts.usdt_perp.interface import USDTPerpMarketDataSource
from ...core.errors import ExchangeTransientError, IntervalNotSupportedError, MarketDataError
from ...core.queries import FundingRateWindow, HistoricalWindow
from ...core.registry import register_usdt_perp_source
from ...models.shared import Exchange, Interval, Symbol
from ...models.usdt_perp import (
    USDTPerpFundingRatePoint,
    USDTPerpIndexPricePoint,
    USDTPerpKline,
    USDTPerpMarkPrice,
    USDTPerpOpenInterest,
    USDTPerpPremiumIndexPoint,
    USDTPerpPriceTicker,
)

BASE_URL = "https://api.bitget.com"
CANDLES_ENDPOINT = "/api/mix/v1/market/candles"
FUNDING_HISTORY_ENDPOINT = "/api/mix/v1/market/history-fundRate"
TICKER_ENDPOINT = "/api/mix/v1/market/ticker"
MARK_PRICE_ENDPOINT = "/api/mix/v1/market/mark-price"
FUNDING_TIME_ENDPOINT = "/api/mix/v1/market/funding-time"
OPEN_INTEREST_ENDPOINT = "/api/mix/v1/market/open-interest"
PRODUCT_SUFFIX = "_UMCBL"
DEFAULT_TIMEOUT = 10.0
KLINE_MAX_LIMIT = 1000
FUNDING_MAX_LIMIT = 100
PREMIUM_KLINE_TYPE = "premium"

INTERVAL_MAP: dict[Interval, str] = {
    Interval.MINUTE_1: "1m",
    Interval.MINUTE_3: "3m",
    Interval.MINUTE_5: "5m",
    Interval.MINUTE_15: "15m",
    Interval.MINUTE_30: "30m",
    Interval.HOUR_1: "1H",
    Interval.HOUR_2: "2H",
    Interval.HOUR_4: "4H",
    Interval.HOUR_6: "6H",
    Interval.HOUR_12: "12H",
    Interval.DAY_1: "1D",
    Interval.DAY_3: "3D",
    Interval.WEEK_1: "1W",
    Interval.MONTH_1: "1M",
}

INTERVAL_MILLISECONDS: dict[Interval, int] = {
    Interval.MINUTE_1: 60_000,
    Interval.MINUTE_3: 180_000,
    Interval.MINUTE_5: 300_000,
    Interval.MINUTE_15: 900_000,
    Interval.MINUTE_30: 1_800_000,
    Interval.HOUR_1: 3_600_000,
    Interval.HOUR_2: 7_200_000,
    Interval.HOUR_4: 14_400_000,
    Interval.HOUR_6: 21_600_000,
    Interval.HOUR_12: 43_200_000,
    Interval.DAY_1: 86_400_000,
    Interval.DAY_3: 259_200_000,
    Interval.WEEK_1: 604_800_000,
    Interval.MONTH_1: 2_592_000_000,
}


class BitgetUSDTPerpDataSource(USDTPerpMarketDataSource):
    """Bitget requests-backed implementation."""

    exchange = Exchange.BITGET

    def __init__(
        self,
        *,
        session: requests.Session | None = None,
        base_url: str = BASE_URL,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        self._session = session or requests.Session()
        self._owns_session = session is None
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    # ------------------------------------------------------------------
    # Historical series
    def get_price_klines(self, query: HistoricalWindow) -> Sequence[USDTPerpKline]:
        entries = self._fetch_kline_series(query, endpoint_name="price klines")
        return [self._parse_kline(row) for row in entries]

    def get_index_price_klines(self, query: HistoricalWindow) -> Sequence[USDTPerpKline]:
        entries = self._fetch_kline_series(query, endpoint_name="index price klines", kline_type="index")
        return [self._parse_kline(row) for row in entries]

    def get_mark_price_klines(self, query: HistoricalWindow) -> Sequence[USDTPerpKline]:
        entries = self._fetch_kline_series(query, endpoint_name="mark price klines", kline_type="mark")
        return [self._parse_kline(row) for row in entries]

    def get_premium_index_klines(self, query: HistoricalWindow) -> Sequence[USDTPerpKline]:
        entries = self._fetch_kline_series(
            query,
            endpoint_name="premium index klines",
            kline_type=PREMIUM_KLINE_TYPE,
        )
        return [self._parse_kline(row) for row in entries]

    def get_funding_rate_history(self, query: FundingRateWindow) -> Sequence[USDTPerpFundingRatePoint]:
        params = self._funding_params(query)
        payload = self._request_wrapped(FUNDING_HISTORY_ENDPOINT, params)
        entries = payload.get("data")
        if not isinstance(entries, Iterable):
            raise MarketDataError("Bitget returned malformed funding rate history payload")
        return [self._parse_funding_point(item) for item in entries]

    # ------------------------------------------------------------------
    # Latest snapshots
    def get_latest_price(self, symbol: Symbol) -> USDTPerpPriceTicker:
        ticker, server_time = self._fetch_ticker(symbol)
        price = self._to_decimal(ticker.get("last"))
        timestamp = self._infer_timestamp(ticker, server_time)
        return (price, timestamp)

    def get_latest_mark_price(self, symbol: Symbol) -> USDTPerpMarkPrice:
        mark_price, _ = self._fetch_mark_price(symbol)
        ticker, _ = self._fetch_ticker(symbol)
        index_price = self._to_decimal(ticker.get("indexPrice"))
        funding_rate = self._to_decimal(ticker.get("fundingRate"))
        next_funding = self._fetch_next_funding_time(symbol)
        return (mark_price, index_price, funding_rate, next_funding)

    def get_latest_index_price(self, symbol: Symbol) -> USDTPerpIndexPricePoint:
        ticker, server_time = self._fetch_ticker(symbol)
        index_price = self._to_decimal(ticker.get("indexPrice"))
        timestamp = self._infer_timestamp(ticker, server_time)
        return (index_price, timestamp)

    def get_latest_premium_index(self, symbol: Symbol) -> USDTPerpPremiumIndexPoint:
        window = HistoricalWindow(symbol=symbol, interval=Interval.MINUTE_1, limit=1)
        entries = self._fetch_kline_series(
            window,
            endpoint_name="premium index klines",
            kline_type=PREMIUM_KLINE_TYPE,
            limit_override=1,
        )
        if not entries:
            raise MarketDataError("Bitget returned empty premium index klines payload")
        return self._parse_snapshot_from_kline(entries[0], endpoint_name="premium index")

    def get_latest_funding_rate(self, symbol: Symbol) -> USDTPerpFundingRatePoint:
        window = FundingRateWindow(symbol=symbol, limit=1)
        history = self.get_funding_rate_history(window)
        if not history:
            raise MarketDataError("Bitget returned empty funding rate history")
        return history[0]

    def get_open_interest(self, symbol: Symbol) -> USDTPerpOpenInterest:
        params = {"symbol": self._symbol_id(symbol)}
        payload = self._request_wrapped(OPEN_INTEREST_ENDPOINT, params)
        data = payload.get("data")
        if not isinstance(data, dict):
            raise MarketDataError("Bitget returned malformed open interest payload")
        timestamp = int(data.get("timestamp") or payload.get("requestTime") or 0)
        amount = self._to_decimal(data.get("amount"))
        return (timestamp, amount)

    # ------------------------------------------------------------------
    # Internal helpers
    def close(self) -> None:
        if self._owns_session:
            self._session.close()

    def _fetch_kline_series(
        self,
        query: HistoricalWindow,
        *,
        endpoint_name: str,
        kline_type: str | None = None,
        limit_override: int | None = None,
        time_range: tuple[int, int] | None = None,
    ) -> Sequence[Sequence[Any]]:
        params = self._historical_params(
            query,
            endpoint_name=endpoint_name,
            kline_type=kline_type,
            limit_override=limit_override,
            time_range=time_range,
        )
        payload = self._request_json(CANDLES_ENDPOINT, params)
        if not isinstance(payload, list) or not payload:
            raise MarketDataError(f"Bitget returned empty {endpoint_name}")
        return payload

    def _historical_params(
        self,
        query: HistoricalWindow,
        *,
        endpoint_name: str,
        kline_type: str | None,
        limit_override: int | None = None,
        time_range: tuple[int, int] | None = None,
    ) -> dict[str, Any]:
        interval = self._map_interval(query.interval)
        limit = limit_override or self._enforce_limit(query.limit, KLINE_MAX_LIMIT, endpoint_name=endpoint_name)
        start_ms, end_ms = time_range or self._derive_time_range(query, limit)
        params: dict[str, Any] = {
            "symbol": self._symbol_id(query.symbol),
            "granularity": interval,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": limit,
        }
        if kline_type:
            params["kLineType"] = kline_type
        return params

    def _funding_params(self, query: FundingRateWindow) -> dict[str, Any]:
        limit = self._enforce_limit(query.limit, FUNDING_MAX_LIMIT, endpoint_name="funding history")
        params: dict[str, Any] = {
            "symbol": self._symbol_id(query.symbol),
            "pageSize": limit,
        }
        return params

    def _fetch_ticker(self, symbol: Symbol) -> tuple[dict[str, Any], int]:
        params = {"symbol": self._symbol_id(symbol)}
        payload = self._request_wrapped(TICKER_ENDPOINT, params)
        data = payload.get("data")
        if not isinstance(data, dict):
            raise MarketDataError("Bitget returned malformed ticker payload")
        server_time = int(payload.get("requestTime") or data.get("timestamp") or 0)
        return data, server_time

    def _fetch_mark_price(self, symbol: Symbol) -> tuple[Decimal, int]:
        params = {"symbol": self._symbol_id(symbol)}
        payload = self._request_wrapped(MARK_PRICE_ENDPOINT, params)
        data = payload.get("data")
        if not isinstance(data, dict):
            raise MarketDataError("Bitget returned malformed mark price payload")
        price = self._to_decimal(data.get("markPrice"))
        timestamp = int(data.get("timestamp") or payload.get("requestTime") or 0)
        return price, timestamp

    def _fetch_next_funding_time(self, symbol: Symbol) -> int:
        params = {"symbol": self._symbol_id(symbol)}
        payload = self._request_wrapped(FUNDING_TIME_ENDPOINT, params)
        data = payload.get("data")
        if not isinstance(data, dict):
            raise MarketDataError("Bitget returned malformed funding time payload")
        return int(data.get("fundingTime") or payload.get("requestTime") or 0)

    def _derive_time_range(self, query: HistoricalWindow, limit: int) -> tuple[int, int]:
        interval_ms = INTERVAL_MILLISECONDS.get(query.interval)
        if interval_ms is None:
            raise IntervalNotSupportedError(f"Interval {query.interval} is not supported by Bitget")
        end_ms = _datetime_to_ms(query.end_time) if query.end_time else _now_ms()
        start_ms = _datetime_to_ms(query.start_time) if query.start_time else end_ms - interval_ms * limit
        if start_ms >= end_ms:
            start_ms = max(end_ms - interval_ms * limit, 0)
        return int(start_ms), int(end_ms)

    def _map_interval(self, interval: Interval) -> str:
        try:
            return INTERVAL_MAP[interval]
        except KeyError as exc:  # pragma: no cover - validated via INTERVAL_MILLISECONDS
            raise IntervalNotSupportedError(f"Interval {interval} is not supported by Bitget") from exc

    def _enforce_limit(self, requested: int, max_limit: int, *, endpoint_name: str) -> int:
        if requested > max_limit:
            raise ValueError(f"Bitget {endpoint_name} limit cannot exceed {max_limit} entries")
        return requested

    def _parse_kline(self, raw: Sequence[Any]) -> USDTPerpKline:
        if len(raw) < 6:
            raise MarketDataError("Unexpected Bitget kline payload structure")
        open_time = int(raw[0])
        open_price = self._to_decimal(raw[1])
        high = self._to_decimal(raw[2])
        low = self._to_decimal(raw[3])
        close = self._to_decimal(raw[4])
        volume = self._to_decimal(raw[5])
        return (open_time, open_price, high, low, close, volume)

    def _parse_snapshot_from_kline(
        self, raw: Sequence[Any], *, endpoint_name: str
    ) -> tuple[Decimal, int]:
        if len(raw) < 5:
            raise MarketDataError(f"Unexpected Bitget {endpoint_name} kline payload structure")
        close_price = self._to_decimal(raw[4])
        timestamp = int(raw[0])
        return (close_price, timestamp)

    def _parse_funding_point(self, raw: Any) -> USDTPerpFundingRatePoint:
        if not isinstance(raw, dict):
            raise MarketDataError("Bitget returned malformed funding rate entry")
        timestamp = int(raw.get("settleTime") or raw.get("fundingTime") or 0)
        rate = self._to_decimal(raw.get("fundingRate"))
        return (timestamp, rate)

    def _symbol_id(self, symbol: Symbol) -> str:
        return f"{symbol.pair}{PRODUCT_SUFFIX}"

    def _infer_timestamp(self, ticker: dict[str, Any], server_time: int) -> int:
        candidate = ticker.get("timestamp")
        if candidate:
            return int(candidate)
        if server_time:
            return server_time
        return _now_ms()

    def _request_wrapped(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        payload = self._request_json(path, params)
        if not isinstance(payload, dict):
            raise MarketDataError("Bitget returned an unexpected payload")
        code = payload.get("code")
        if code not in (None, "00000"):
            message = payload.get("msg") or f"Bitget endpoint {path} returned error code {code}"
            raise MarketDataError(message)
        return payload

    def _request_json(self, path: str, params: dict[str, Any]) -> Any:
        url = f"{self._base_url}{path}"
        try:
            response = self._session.get(url, params=params, timeout=self._timeout)
        except requests.RequestException as exc:  # pragma: no cover - network failure
            raise ExchangeTransientError(f"Failed to call Bitget endpoint {path}: {exc}") from exc

        if response.status_code == 403:
            raise ExchangeTransientError("Bitget denied the request with HTTP 403")
        if response.status_code >= 500:
            raise ExchangeTransientError(f"Bitget endpoint {path} unavailable (HTTP {response.status_code})")

        payload = self._decode_response(response)
        if response.status_code >= 400:
            message = self._extract_message(payload) or f"Bitget endpoint {path} returned HTTP {response.status_code}"
            raise MarketDataError(message)
        return payload

    def _decode_response(self, response: requests.Response) -> Any:
        try:
            return response.json()
        except ValueError as exc:  # pragma: no cover - defensive branch
            raise MarketDataError("Bitget returned a non-JSON payload") from exc

    def _extract_message(self, payload: Any) -> str | None:
        if isinstance(payload, dict):
            msg = payload.get("msg")
            if isinstance(msg, str) and msg:
                return msg
        return None

    def _to_decimal(self, value: Any) -> Decimal:
        if value in (None, ""):
            return Decimal("0")
        return Decimal(str(value))


def _datetime_to_ms(value: datetime | None) -> int:
    if value is None:
        return _now_ms()
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


def _now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def register(*, replace: bool = False) -> None:
    """Register the Bitget data source in the global registry."""

    register_usdt_perp_source(Exchange.BITGET, lambda: BitgetUSDTPerpDataSource(), replace=replace)


register()
