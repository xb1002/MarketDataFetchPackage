"""Bybit USDT perpetual market data implementation."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Sequence

import requests

from ...contracts.usdt_perp.interface import USDTPerpMarketDataSource
from ...core.errors import ExchangeTransientError, IntervalNotSupportedError, MarketDataError
from ...core.queries import FundingRateWindow, HistoricalWindow
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
    USDTPerpPriceTicker,
)

BASE_URL = "https://api.bybit.com"
CATEGORY = "linear"
PRICE_KLINES_ENDPOINT = "/v5/market/kline"
INDEX_PRICE_KLINES_ENDPOINT = "/v5/market/index-price-kline"
MARK_PRICE_KLINES_ENDPOINT = "/v5/market/mark-price-kline"
PREMIUM_INDEX_KLINES_ENDPOINT = "/v5/market/premium-index-price-kline"
FUNDING_HISTORY_ENDPOINT = "/v5/market/funding/history"
TICKERS_ENDPOINT = "/v5/market/tickers"
PREMIUM_INDEX_ENDPOINT = "/v5/market/premium-index-price"
INSTRUMENTS_ENDPOINT = "/v5/market/instruments-info"
OPEN_INTEREST_INTERVAL = "5min"
DEFAULT_TIMEOUT = 10.0
PRICE_KLINES_MAX_LIMIT = 1000
INDEX_KLINES_MAX_LIMIT = 1000
MARK_PRICE_KLINES_MAX_LIMIT = 1000
PREMIUM_INDEX_KLINES_MAX_LIMIT = 1000
FUNDING_RATE_MAX_LIMIT = 200

INTERVAL_MAP: dict[Interval, str] = {
    Interval.MINUTE_1: "1",
    Interval.MINUTE_3: "3",
    Interval.MINUTE_5: "5",
    Interval.MINUTE_15: "15",
    Interval.MINUTE_30: "30",
    Interval.HOUR_1: "60",
    Interval.HOUR_2: "120",
    Interval.HOUR_4: "240",
    Interval.HOUR_6: "360",
    Interval.HOUR_12: "720",
    Interval.DAY_1: "D",
    Interval.WEEK_1: "W",
    Interval.MONTH_1: "M",
}


class BybitUSDTPerpDataSource(USDTPerpMarketDataSource):
    """Requests-backed Bybit implementation."""

    exchange = Exchange.BYBIT

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
        data = self._fetch_kline_series(
            PRICE_KLINES_ENDPOINT,
            query,
            max_limit=PRICE_KLINES_MAX_LIMIT,
            endpoint_name="price klines",
        )
        return [self._parse_kline(entry) for entry in data]

    def get_index_price_klines(self, query: HistoricalWindow) -> Sequence[USDTPerpKline]:
        data = self._fetch_kline_series(
            INDEX_PRICE_KLINES_ENDPOINT,
            query,
            max_limit=INDEX_KLINES_MAX_LIMIT,
            endpoint_name="index price klines",
        )
        return [self._parse_kline(entry) for entry in data]

    def get_mark_price_klines(self, query: HistoricalWindow) -> Sequence[USDTPerpKline]:
        data = self._fetch_kline_series(
            MARK_PRICE_KLINES_ENDPOINT,
            query,
            max_limit=MARK_PRICE_KLINES_MAX_LIMIT,
            endpoint_name="mark price klines",
        )
        return [self._parse_kline(entry) for entry in data]

    def get_premium_index_klines(self, query: HistoricalWindow) -> Sequence[USDTPerpKline]:
        data = self._fetch_kline_series(
            PREMIUM_INDEX_KLINES_ENDPOINT,
            query,
            max_limit=PREMIUM_INDEX_KLINES_MAX_LIMIT,
            endpoint_name="premium index klines",
        )
        return [self._parse_kline(entry) for entry in data]

    def get_funding_rate_history(self, query: FundingRateWindow) -> Sequence[USDTPerpFundingRatePoint]:
        payload = self._request(
            FUNDING_HISTORY_ENDPOINT,
            self._funding_params(query, max_limit=FUNDING_RATE_MAX_LIMIT),
        )
        entries = self._extract_list(payload, endpoint_name="funding history")
        return [self._parse_funding_point(item) for item in entries]

    # ------------------------------------------------------------------
    # Latest snapshots
    def get_latest_price(self, symbol: Symbol) -> USDTPerpPriceTicker:
        ticker, server_time = self._fetch_ticker(symbol)
        price = self._to_decimal(ticker.get("lastPrice"))
        timestamp = self._infer_timestamp(ticker, server_time)
        return (timestamp, price)

    def get_latest_mark_price(self, symbol: Symbol) -> USDTPerpMarkPrice:
        ticker, _ = self._fetch_ticker(symbol)
        mark_price = self._to_decimal(ticker.get("markPrice"))
        index_price = self._to_decimal(ticker.get("indexPrice"))
        funding_rate = self._to_decimal(ticker.get("fundingRate"))
        next_funding_time = int(ticker.get("nextFundingTime") or 0)
        return (mark_price, index_price, funding_rate, next_funding_time)

    def get_latest_index_price(self, symbol: Symbol) -> USDTPerpIndexPricePoint:
        ticker, server_time = self._fetch_ticker(symbol)
        index_price = self._to_decimal(ticker.get("indexPrice"))
        timestamp = self._infer_timestamp(ticker, server_time)
        return (timestamp, index_price)

    def get_latest_premium_index(self, symbol: Symbol) -> USDTPerpPremiumIndexPoint:
        # The dedicated premium index snapshot endpoint frequently returns HTTP
        # 4xx/5xx outside of approved regions. Fall back to the kline feed with
        # a single-entry request which is globally available.
        window = HistoricalWindow(symbol=symbol, interval=Interval.MINUTE_1, limit=1)
        data = self._fetch_kline_series(
            PREMIUM_INDEX_KLINES_ENDPOINT,
            window,
            max_limit=PREMIUM_INDEX_KLINES_MAX_LIMIT,
            endpoint_name="premium index klines",
        )
        latest = self._parse_kline(data[0])
        # close price reflects the latest premium index value and open_time is
        # the corresponding timestamp.
        return (latest[0], latest[4])

    def get_latest_funding_rate(self, symbol: Symbol) -> USDTPerpFundingRatePoint:
        window = FundingRateWindow(symbol=symbol, limit=1)
        history = self.get_funding_rate_history(window)
        if not history:
            raise MarketDataError("Bybit returned empty funding rate history")
        return history[0]

    def get_open_interest(self, symbol: Symbol) -> USDTPerpOpenInterest:
        # Bybit's dedicated open-interest endpoint can trail the latest ticker
        # reading by tens of minutes depending on the configured bucket size.
        # Reuse the ticker snapshot instead so the timestamp lines up with the
        # freshest market data (which is also what CCXT exposes).
        ticker, server_time = self._fetch_ticker(symbol)
        timestamp = self._infer_timestamp(ticker, server_time)
        value = ticker.get("openInterestValue") or ticker.get("openInterest")
        open_interest = self._to_decimal(value)
        return (timestamp, open_interest)

    def get_instruments(self) -> Sequence[USDTPerpInstrument]:
        payload = self._request(INSTRUMENTS_ENDPOINT, {"category": CATEGORY})
        entries = self._extract_list(payload, endpoint_name="instruments")
        return [self._parse_instrument(entry) for entry in entries]

    # ------------------------------------------------------------------
    # Internal helpers
    def close(self) -> None:
        if self._owns_session:
            self._session.close()

    def _fetch_kline_series(
        self,
        endpoint: str,
        query: HistoricalWindow,
        *,
        max_limit: int,
        endpoint_name: str,
    ) -> Sequence[Sequence[Any]]:
        params = self._historical_params(query, max_limit=max_limit, endpoint_name=endpoint_name)
        payload = self._request(endpoint, params)
        return self._extract_list(payload, endpoint_name=endpoint_name)

    def _fetch_ticker(self, symbol: Symbol) -> tuple[dict[str, Any], int]:
        params = {"category": CATEGORY, "symbol": symbol.pair}
        payload = self._request(TICKERS_ENDPOINT, params)
        entries = self._extract_list(payload, endpoint_name="ticker")
        ticker = entries[0]
        server_time = int(payload.get("time") or 0)
        return ticker, server_time

    def _historical_params(
        self,
        query: HistoricalWindow,
        *,
        max_limit: int,
        endpoint_name: str,
    ) -> dict[str, Any]:
        interval = self._map_interval(query.interval)
        limit = self._enforce_limit(query.limit, max_limit, endpoint_name=endpoint_name)
        params: dict[str, Any] = {
            "category": CATEGORY,
            "symbol": query.symbol.pair,
            "interval": interval,
            "limit": limit,
        }
        if query.start_time:
            params["start"] = _to_milliseconds(query.start_time)
        if query.end_time:
            params["end"] = _to_milliseconds(query.end_time)
        return params

    def _funding_params(self, query: FundingRateWindow, *, max_limit: int) -> dict[str, Any]:
        limit = self._enforce_limit(query.limit, max_limit, endpoint_name="funding history")
        params: dict[str, Any] = {
            "category": CATEGORY,
            "symbol": query.symbol.pair,
            "limit": limit,
        }
        if query.start_time:
            params["start"] = _to_milliseconds(query.start_time)
        if query.end_time:
            params["end"] = _to_milliseconds(query.end_time)
        return params

    def _enforce_limit(self, requested: int, max_limit: int, *, endpoint_name: str) -> int:
        if requested > max_limit:
            raise ValueError(f"Bybit {endpoint_name} limit cannot exceed {max_limit} entries")
        return requested

    def _map_interval(self, interval: Interval) -> str:
        try:
            return INTERVAL_MAP[interval]
        except KeyError as exc:
            raise IntervalNotSupportedError(f"Interval {interval} is not supported by Bybit") from exc

    def _extract_list(self, payload: dict[str, Any], *, endpoint_name: str) -> Sequence[Any]:
        result = payload.get("result")
        if not isinstance(result, dict):
            raise MarketDataError(f"Bybit returned malformed {endpoint_name} payload")
        entries = result.get("list")
        if not entries:
            raise MarketDataError(f"Bybit returned empty {endpoint_name} payload")
        return entries

    def _parse_kline(self, raw: Sequence[Any]) -> USDTPerpKline:
        if len(raw) < 5:
            raise MarketDataError("Unexpected Bybit kline payload structure")
        open_time = int(raw[0])
        open_price = self._to_decimal(raw[1])
        high = self._to_decimal(raw[2])
        low = self._to_decimal(raw[3])
        close = self._to_decimal(raw[4])
        volume_source = raw[5] if len(raw) > 5 else "0"
        volume = self._to_decimal(volume_source)
        return (open_time, open_price, high, low, close, volume)

    def _parse_funding_point(self, raw: dict[str, Any]) -> USDTPerpFundingRatePoint:
        timestamp = int(raw.get("fundingRateTimestamp") or raw.get("timestamp") or 0)
        rate = self._to_decimal(raw.get("fundingRate"))
        return (timestamp, rate)

    def _parse_instrument(self, raw: dict[str, Any]) -> USDTPerpInstrument:
        symbol = str(raw.get("symbol") or "")
        base_coin = str(raw.get("baseCoin") or "")
        quote_coin = str(raw.get("quoteCoin") or "")
        status = str(raw.get("status") or "")
        price_filter = raw.get("priceFilter") or {}
        lot_filter = raw.get("lotSizeFilter") or {}
        tick_size = self._to_decimal(price_filter.get("tickSize"))
        step_size = self._to_decimal(lot_filter.get("qtyStep"))
        min_qty = self._to_decimal(lot_filter.get("minOrderQty"))
        max_qty = self._to_decimal(lot_filter.get("maxOrderQty"))
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

    def _infer_timestamp(self, ticker: dict[str, Any], server_time: int) -> int:
        candidate = ticker.get("timestamp") or ticker.get("ts")
        if candidate:
            return int(candidate)
        if server_time:
            return server_time
        return int(datetime.now(tz=timezone.utc).timestamp() * 1000)

    def _to_decimal(self, value: Any) -> Decimal:
        if value in ("", None):
            return Decimal("0")
        return Decimal(str(value))

    def _request(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self._base_url}{path}"
        try:
            response = self._session.get(url, params=params, timeout=self._timeout)
        except requests.RequestException as exc:  # pragma: no cover - network failure
            raise ExchangeTransientError(f"Failed to call Bybit endpoint {path}: {exc}") from exc

        if response.status_code == 403:
            raise ExchangeTransientError("Bybit denied the request with HTTP 403")
        if response.status_code >= 500:
            raise ExchangeTransientError(f"Bybit endpoint {path} unavailable (HTTP {response.status_code})")

        payload = self._decode_response(response)
        if response.status_code >= 400:
            message = self._extract_message(payload) or f"Bybit endpoint {path} returned HTTP {response.status_code}"
            raise MarketDataError(message)

        ret_code = payload.get("retCode")
        if ret_code not in (0, None):
            message = payload.get("retMsg") or f"Bybit error code {ret_code}"
            raise MarketDataError(message)
        return payload

    def _decode_response(self, response: requests.Response) -> dict[str, Any]:
        try:
            data = response.json()
        except ValueError as exc:  # pragma: no cover - defensive branch
            raise MarketDataError("Bybit returned a non-JSON payload") from exc
        if not isinstance(data, dict):  # pragma: no cover - defensive branch
            raise MarketDataError("Bybit returned an unexpected payload")
        return data

    def _extract_message(self, payload: dict[str, Any]) -> str | None:
        msg = payload.get("retMsg") or payload.get("retExtInfo")
        if isinstance(msg, str) and msg:
            return msg
        return None


def _to_milliseconds(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


def register(*, replace: bool = False) -> None:
    """Register the Bybit data source in the global registry."""

    register_usdt_perp_source(Exchange.BYBIT, lambda: BybitUSDTPerpDataSource(), replace=replace)


register()
