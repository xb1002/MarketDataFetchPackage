"""Bitget USDT perpetual market data implementation."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Sequence

import requests

from ...contracts.usdt_perp.interface import USDTPerpMarketDataSource
from ...core.errors import ExchangeTransientError, IntervalNotSupportedError, MarketDataError
from ...core.queries import DEFAULT_LIMIT, FundingRateWindow, HistoricalWindow
from ...core.registry import register_usdt_perp_source
from ...models.shared import Exchange, Interval, Symbol
from ...models.usdt_perp import (
    USDTPerpFundingRatePoint,
    USDTPerpIndexPricePoint,
    USDTPerpInstrument,
    USDTPerpKline,
    USDTPerpMarkPrice,
    USDTPerpOpenInterest,
    USDTPerpPremiumIndexPoint,
    USDTPerpTicker,
)

BASE_URL = "https://api.bitget.com"
HISTORY_CANDLES_ENDPOINT = "/api/v3/market/history-candles"
FUNDING_HISTORY_ENDPOINT = "/api/v3/market/history-fund-rate"
CURRENT_FUNDING_ENDPOINT = "/api/v3/market/current-fund-rate"
TICKER_ENDPOINT = "/api/v3/market/tickers"
OPEN_INTEREST_ENDPOINT = "/api/v3/market/open-interest"
INSTRUMENTS_ENDPOINT = "/api/v3/market/instruments"
CATEGORY = "USDT-FUTURES"
DEFAULT_TIMEOUT = 10.0
KLINE_MAX_LIMIT = 100
FUNDING_MAX_LIMIT = 200
KLINE_TYPE_MARKET = "MARKET"
KLINE_TYPE_MARK = "MARK"
KLINE_TYPE_INDEX = "INDEX"
KLINE_TYPE_PREMIUM = "PREMIUM"

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
        entries = self._fetch_kline_series(
            query, endpoint_name="price klines", kline_type=KLINE_TYPE_MARKET
        )
        return [self._parse_kline(row) for row in entries]

    def get_index_price_klines(self, query: HistoricalWindow) -> Sequence[USDTPerpKline]:
        entries = self._fetch_kline_series(
            query, endpoint_name="index price klines", kline_type=KLINE_TYPE_INDEX
        )
        return [self._parse_kline(row, zero_volume=True) for row in entries]

    def get_mark_price_klines(self, query: HistoricalWindow) -> Sequence[USDTPerpKline]:
        entries = self._fetch_kline_series(
            query, endpoint_name="mark price klines", kline_type=KLINE_TYPE_MARK
        )
        return [self._parse_kline(row, zero_volume=True) for row in entries]

    def get_premium_index_klines(self, query: HistoricalWindow) -> Sequence[USDTPerpKline]:
        entries = self._fetch_kline_series(
            query,
            endpoint_name="premium index klines",
            kline_type=KLINE_TYPE_PREMIUM,
        )
        return [self._parse_kline(row, zero_volume=True) for row in entries]

    def get_funding_rate_history(self, query: FundingRateWindow) -> Sequence[USDTPerpFundingRatePoint]:
        params = self._funding_params(query)
        payload = self._request_wrapped(FUNDING_HISTORY_ENDPOINT, params)
        data = payload.get("data")
        if not isinstance(data, dict):
            raise MarketDataError("Bitget returned malformed funding rate history payload")
        entries = data.get("resultList")
        if not isinstance(entries, Sequence):
            raise MarketDataError("Bitget returned malformed funding rate history payload")
        points = [self._parse_funding_point(item) for item in entries]
        start_ms = _datetime_to_ms(query.start_time) if query.start_time else None
        end_ms = _datetime_to_ms(query.end_time) if query.end_time else None
        if start_ms or end_ms:
            filtered: list[USDTPerpFundingRatePoint] = []
            for timestamp, rate in points:
                if start_ms and timestamp < start_ms:
                    continue
                if end_ms and timestamp > end_ms:
                    continue
                filtered.append((timestamp, rate))
            return filtered
        return points

    # ------------------------------------------------------------------
    # Latest snapshots
    def get_latest_price(self, symbol: Symbol) -> USDTPerpTicker:
        ticker, timestamp = self._fetch_ticker(symbol)
        return {
            "timestamp": timestamp,
            "last_price": self._to_decimal(ticker.get("lastPrice") or ticker.get("lastPr")),
            "index_price": self._to_decimal(
                ticker.get("indexPrice") or ticker.get("indexPr") or ticker.get("indexPx")
            ),
            "mark_price": self._to_decimal(
                ticker.get("markPrice") or ticker.get("markPr") or ticker.get("markPx")
            ),
        }

    def get_latest_mark_price(self, symbol: Symbol) -> USDTPerpMarkPrice:
        ticker, timestamp = self._fetch_ticker(symbol)
        mark_price = self._to_decimal(ticker.get("markPrice") or ticker.get("markPr"))
        return (timestamp, mark_price)

    def get_latest_index_price(self, symbol: Symbol) -> USDTPerpIndexPricePoint:
        ticker, timestamp = self._fetch_ticker(symbol)
        index_price = self._to_decimal(ticker.get("indexPrice"))
        return (timestamp, index_price)

    def get_latest_premium_index(self, symbol: Symbol) -> USDTPerpPremiumIndexPoint:
        window = HistoricalWindow(symbol=symbol, interval=Interval.MINUTE_1, limit=1)
        entries = self._fetch_kline_series(
            window,
            endpoint_name="premium index klines",
            kline_type=KLINE_TYPE_PREMIUM,
            limit_override=1,
        )
        if not entries:
            raise MarketDataError("Bitget returned empty premium index klines payload")
        return self._parse_snapshot_from_kline(entries[0], endpoint_name="premium index")

    def get_latest_funding_rate(self, symbol: Symbol) -> USDTPerpFundingRatePoint:
        timestamp, rate = self._fetch_current_funding(symbol)
        return (timestamp, rate)

    def get_open_interest(self, symbol: Symbol) -> USDTPerpOpenInterest:
        params = {"category": CATEGORY, "symbol": self._symbol_pair(symbol)}
        payload = self._request_wrapped(OPEN_INTEREST_ENDPOINT, params)
        data = payload.get("data")
        if not isinstance(data, dict):
            raise MarketDataError("Bitget returned malformed open interest payload")
        entries = data.get("list")
        if not isinstance(entries, Sequence) or not entries:
            raise MarketDataError("Bitget returned empty open interest payload")
        entry = entries[0]
        timestamp = int(data.get("ts") or payload.get("requestTime") or 0)
        amount = self._to_decimal(entry.get("openInterest"))
        return (timestamp, amount)

    def get_instruments(self) -> Sequence[USDTPerpInstrument]:
        payload = self._request_wrapped(INSTRUMENTS_ENDPOINT, {"category": CATEGORY})
        data = payload.get("data")
        if not isinstance(data, Sequence) or not data:
            raise MarketDataError("Bitget returned empty instruments payload")
        return [self._parse_instrument(entry) for entry in data]

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
    ) -> Sequence[Sequence[Any]]:
        params = self._historical_params(
            query,
            endpoint_name=endpoint_name,
            kline_type=kline_type,
            limit_override=limit_override,
        )
        payload = self._request_wrapped(HISTORY_CANDLES_ENDPOINT, params)
        data = payload.get("data")
        if not isinstance(data, Sequence) or not data:
            raise MarketDataError(f"Bitget returned empty {endpoint_name}")
        entries: list[Sequence[Any]] = list(data)
        if query.start_time or query.end_time:
            start_ms = _datetime_to_ms(query.start_time) if query.start_time else None
            end_ms = _datetime_to_ms(query.end_time) if query.end_time else None
            filtered: list[Sequence[Any]] = []
            for row in entries:
                timestamp = int(row[0]) if row else 0
                if start_ms and timestamp < start_ms:
                    continue
                if end_ms and timestamp > end_ms:
                    continue
                filtered.append(row)
            if not filtered:
                raise MarketDataError(f"Bitget returned no {endpoint_name} entries within requested window")
            entries = filtered
        return entries

    def _historical_params(
        self,
        query: HistoricalWindow,
        *,
        endpoint_name: str,
        kline_type: str | None,
        limit_override: int | None = None,
    ) -> dict[str, Any]:
        interval = self._map_interval(query.interval)
        limit = limit_override or self._enforce_limit(query.limit, KLINE_MAX_LIMIT, endpoint_name=endpoint_name)
        params: dict[str, Any] = {
            "category": CATEGORY,
            "symbol": self._symbol_pair(query.symbol),
            "interval": interval,
            "limit": limit,
        }
        if query.start_time or query.end_time:
            start_ms, end_ms = self._derive_time_range(query, limit)
            params["startTime"] = start_ms
            params["endTime"] = end_ms
        if kline_type:
            params["type"] = kline_type
        return params

    def _funding_params(self, query: FundingRateWindow) -> dict[str, Any]:
        limit = self._enforce_limit(query.limit, FUNDING_MAX_LIMIT, endpoint_name="funding history")
        params: dict[str, Any] = {
            "category": CATEGORY,
            "symbol": self._symbol_pair(query.symbol),
            "limit": limit,
        }
        return params

    def _fetch_ticker(self, symbol: Symbol) -> tuple[dict[str, Any], int]:
        params = {"category": CATEGORY, "symbol": self._symbol_pair(symbol)}
        payload = self._request_wrapped(TICKER_ENDPOINT, params)
        data = payload.get("data")
        if not isinstance(data, Sequence) or not data:
            raise MarketDataError("Bitget returned malformed ticker payload")
        entry = data[0]
        timestamp = int(entry.get("ts") or payload.get("requestTime") or 0)
        return entry, timestamp

    def _fetch_current_funding(self, symbol: Symbol) -> tuple[int, Decimal]:
        params = {"symbol": self._symbol_pair(symbol)}
        payload = self._request_wrapped(CURRENT_FUNDING_ENDPOINT, params)
        data = payload.get("data")
        if not isinstance(data, Sequence) or not data:
            raise MarketDataError("Bitget returned malformed current funding payload")
        entry = data[0]
        rate = self._to_decimal(entry.get("fundingRate"))
        timestamp = int(entry.get("timestamp") or entry.get("fundingTime") or payload.get("requestTime") or 0)
        return timestamp, rate

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
            if requested == DEFAULT_LIMIT:
                return max_limit
            raise ValueError(f"Bitget {endpoint_name} limit cannot exceed {max_limit} entries")
        return requested

    def _parse_kline(self, raw: Sequence[Any], *, zero_volume: bool = False) -> USDTPerpKline:
        if len(raw) < 6:
            raise MarketDataError("Unexpected Bitget kline payload structure")
        open_time = int(raw[0])
        open_price = self._to_decimal(raw[1])
        high = self._to_decimal(raw[2])
        low = self._to_decimal(raw[3])
        close = self._to_decimal(raw[4])
        volume = Decimal("0") if zero_volume else self._to_decimal(raw[5])
        return (open_time, open_price, high, low, close, volume)

    def _parse_snapshot_from_kline(
        self, raw: Sequence[Any], *, endpoint_name: str
    ) -> tuple[int, Decimal]:
        if len(raw) < 5:
            raise MarketDataError(f"Unexpected Bitget {endpoint_name} kline payload structure")
        close_price = self._to_decimal(raw[4])
        timestamp = int(raw[0])
        return (timestamp, close_price)

    def _parse_funding_point(self, raw: Any) -> USDTPerpFundingRatePoint:
        if not isinstance(raw, dict):
            raise MarketDataError("Bitget returned malformed funding rate entry")
        timestamp = int(raw.get("fundingRateTimestamp") or 0)
        rate = self._to_decimal(raw.get("fundingRate"))
        return (timestamp, rate)

    def _parse_instrument(self, raw: dict[str, Any]) -> USDTPerpInstrument:
        symbol = str(raw.get("symbol") or "")
        base_coin = str(raw.get("baseCoin") or "")
        quote_coin = str(raw.get("quoteCoin") or "")
        status = str(raw.get("status") or "")
        tick_size = self._derive_precision(raw.get("priceMultiplier"), raw.get("pricePrecision"))
        step_size = self._derive_precision(raw.get("quantityMultiplier"), raw.get("quantityPrecision"))
        min_qty = self._to_decimal(raw.get("minOrderQty"))
        max_qty = self._to_decimal(raw.get("maxOrderQty"))
        return {
            "symbol": symbol,
            "base_asset": base_coin,
            "quote_asset": quote_coin,
            "tick_size": tick_size,
            "step_size": step_size,
            "min_qty": min_qty,
            "max_qty": max_qty,
            "status": status,
        }

    def _derive_precision(self, multiplier: Any, precision: Any) -> Decimal:
        value = self._to_decimal(multiplier)
        if value == 0 and precision not in (None, ""):
            try:
                digits = int(precision)
                if digits >= 0:
                    return Decimal("1") / (Decimal(10) ** digits)
            except ValueError:
                pass
        return value

    def _symbol_pair(self, symbol: Symbol) -> str:
        return symbol.pair

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
