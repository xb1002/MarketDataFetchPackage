"""Binance USDT perpetual market data implementation."""

from __future__ import annotations

from datetime import datetime, timezone
import time
from decimal import Decimal
from typing import Any, Sequence

import requests

from ...contracts.usdt_perp.interface import USDTPerpMarketDataSource
from ...core.errors import (
    ExchangeTransientError,
    IntervalNotSupportedError,
    MarketDataError,
    SymbolNotSupportedError,
)
from ...core.queries import FundingRateWindow, HistoricalWindow
from ...core.registry import register_usdt_perp_source
from ...models.shared import Exchange, Interval, Symbol
from ...models.usdt_perp import (
    USDTPerpFundingRate,
    USDTPerpFundingRatePoint,
    USDTPerpIndexPricePoint,
    USDTPerpInstrument,
    USDTPerpKline,
    USDTPerpMarkPrice,
    USDTPerpOpenInterest,
    USDTPerpTicker,
)

BASE_URL = "https://fapi.binance.com"
PRICE_KLINES_ENDPOINT = "/fapi/v1/klines"
INDEX_KLINES_ENDPOINT = "/fapi/v1/indexPriceKlines"
MARK_PRICE_KLINES_ENDPOINT = "/fapi/v1/markPriceKlines"
PREMIUM_KLINES_ENDPOINT = "/fapi/v1/premiumIndexKlines"
FUNDING_HISTORY_ENDPOINT = "/fapi/v1/fundingRate"
PREMIUM_INDEX_ENDPOINT = "/fapi/v1/premiumIndex"
TICKER_24H_ENDPOINT = "/fapi/v1/ticker/24hr"
OPEN_INTEREST_ENDPOINT = "/fapi/v1/openInterest"
EXCHANGE_INFO_ENDPOINT = "/fapi/v1/exchangeInfo"
DEFAULT_TIMEOUT = 10.0
# Binance Futures REST API limits documented at
# https://binance-docs.github.io/apidocs/futures/en/#change-log
PRICE_KLINES_MAX_LIMIT = 1500
INDEX_PRICE_KLINES_MAX_LIMIT = 1500
MARK_PRICE_KLINES_MAX_LIMIT = 1500
PREMIUM_INDEX_KLINES_MAX_LIMIT = 1500
FUNDING_RATE_MAX_LIMIT = 1000


class BinanceUSDTPerpDataSource(USDTPerpMarketDataSource):
    """Requests-backed implementation of :class:`USDTPerpMarketDataSource`."""

    exchange = Exchange.BINANCE

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
        payload = self._request(
            PRICE_KLINES_ENDPOINT,
            self._historical_params(
                query,
                key="symbol",
                max_limit=PRICE_KLINES_MAX_LIMIT,
                endpoint_name="price klines",
            ),
        )
        klines = [self._parse_kline(raw) for raw in payload]
        return self._sort_klines(klines)

    def get_index_price_klines(self, query: HistoricalWindow) -> Sequence[USDTPerpKline]:
        payload = self._request(
            INDEX_KLINES_ENDPOINT,
            self._historical_params(
                query,
                key="pair",
                max_limit=INDEX_PRICE_KLINES_MAX_LIMIT,
                endpoint_name="index price klines",
            ),
        )
        klines = [self._parse_kline(raw) for raw in payload]
        return self._sort_klines(klines)

    def get_premium_index_klines(self, query: HistoricalWindow) -> Sequence[USDTPerpKline]:
        payload = self._request(
            PREMIUM_KLINES_ENDPOINT,
            self._historical_params(
                query,
                key="symbol",
                max_limit=PREMIUM_INDEX_KLINES_MAX_LIMIT,
                endpoint_name="premium index klines",
            ),
        )
        klines = [self._parse_kline(raw) for raw in payload]
        return self._sort_klines(klines)

    def get_mark_price_klines(self, query: HistoricalWindow) -> Sequence[USDTPerpKline]:
        payload = self._request(
            MARK_PRICE_KLINES_ENDPOINT,
            self._historical_params(
                query,
                key="symbol",
                max_limit=MARK_PRICE_KLINES_MAX_LIMIT,
                endpoint_name="mark price klines",
            ),
        )
        klines = [self._parse_kline(raw) for raw in payload]
        return self._sort_klines(klines)

    def get_funding_rate_history(self, query: FundingRateWindow) -> Sequence[USDTPerpFundingRatePoint]:
        limit = self._enforce_limit(
            query.limit,
            FUNDING_RATE_MAX_LIMIT,
            endpoint_name="funding rate history",
        )
        params = {
            "symbol": query.symbol.pair,
            "limit": limit,
        }
        if query.start_time:
            params["startTime"] = _to_milliseconds(query.start_time)
        if query.end_time:
            params["endTime"] = _to_milliseconds(query.end_time)
        payload = self._request(FUNDING_HISTORY_ENDPOINT, params)
        points = [self._parse_funding_point(entry) for entry in payload]
        return sorted(points, key=lambda item: item[0])

    # ------------------------------------------------------------------
    # Latest snapshots
    def get_latest_ticker(self, symbol: Symbol) -> USDTPerpTicker:
        ticker = self._request(TICKER_24H_ENDPOINT, {"symbol": symbol.pair})
        premium = self._request(PREMIUM_INDEX_ENDPOINT, {"symbol": symbol.pair})
        timestamp = int(
            ticker.get("closeTime")
            or ticker.get("time")
            or premium.get("time")
            or int(time.time() * 1000)
        )
        return {
            "timestamp": timestamp,
            "last_price": Decimal(ticker.get("lastPrice") or "0"),
            "index_price": Decimal(premium.get("indexPrice") or "0"),
            "mark_price": Decimal(premium.get("markPrice") or "0"),
        }

    def get_latest_mark_price(self, symbol: Symbol) -> USDTPerpMarkPrice:
        payload = self._request(PREMIUM_INDEX_ENDPOINT, {"symbol": symbol.pair})
        return (int(payload.get("time") or 0), Decimal(payload.get("markPrice") or "0"))

    def get_latest_index_price(self, symbol: Symbol) -> USDTPerpIndexPricePoint:
        raw = self._latest_closed_kline(
            INDEX_KLINES_ENDPOINT,
            symbol,
            key="pair",
            endpoint_name="index price",
        )
        return self._parse_snapshot_from_kline(raw, endpoint_name="index price")

    def get_latest_funding_rate(self, symbol: Symbol) -> USDTPerpFundingRate:
        payload = self._request(PREMIUM_INDEX_ENDPOINT, {"symbol": symbol.pair})
        rate = Decimal(payload.get("lastFundingRate") or "0")
        next_time = int(payload.get("nextFundingTime") or 0)
        return {"funding_rate": rate, "next_funding_time": next_time}

    def get_open_interest(self, symbol: Symbol) -> USDTPerpOpenInterest:
        payload = self._request(OPEN_INTEREST_ENDPOINT, {"symbol": symbol.pair})
        return (int(payload["time"]), Decimal(payload["openInterest"]))

    def get_instruments(self) -> Sequence[USDTPerpInstrument]:
        payload = self._request(EXCHANGE_INFO_ENDPOINT, {})
        symbols = payload.get("symbols")
        if not isinstance(symbols, Sequence) or not symbols:
            raise MarketDataError("Binance returned empty exchange info payload")
        instruments: list[USDTPerpInstrument] = []
        for entry in symbols:
            if not isinstance(entry, dict):
                continue
            if entry.get("contractType") != "PERPETUAL":
                continue
            instruments.append(self._parse_instrument(entry))
        if not instruments:
            raise MarketDataError("Binance did not return any USDT perpetual instruments")
        return instruments

    # ------------------------------------------------------------------
    # Internal helpers
    def close(self) -> None:
        if self._owns_session:
            self._session.close()

    def _sort_klines(self, klines: Sequence[USDTPerpKline]) -> list[USDTPerpKline]:
        return sorted(klines, key=lambda entry: entry[0])

    def _historical_params(
        self,
        query: HistoricalWindow,
        *,
        key: str,
        max_limit: int,
        endpoint_name: str,
    ) -> dict[str, Any]:
        limit = self._enforce_limit(query.limit, max_limit, endpoint_name=endpoint_name)
        params: dict[str, Any] = {
            key: query.symbol.pair,
            "interval": query.interval.value,
            "limit": limit,
        }
        if query.start_time:
            params["startTime"] = _to_milliseconds(query.start_time)
        if query.end_time:
            params["endTime"] = _to_milliseconds(query.end_time)
        return params

    def _enforce_limit(self, requested: int, max_limit: int, *, endpoint_name: str) -> int:
        if requested > max_limit:
            raise ValueError(
                f"Binance {endpoint_name} limit cannot exceed {max_limit} entries"
            )
        return requested

    def _request(self, path: str, params: dict[str, Any]) -> Any:
        url = f"{self._base_url}{path}"
        try:
            response = self._session.get(url, params=params, timeout=self._timeout)
        except requests.RequestException as exc:  # pragma: no cover - network failure
            raise ExchangeTransientError(f"Failed to call Binance endpoint {path}: {exc}") from exc

        payload = self._decode_response(response)
        if isinstance(payload, dict) and "code" in payload and payload["code"] not in (0, None):
            self._raise_api_error(int(payload["code"]), payload.get("msg"))
        if response.status_code >= 400:
            self._raise_http_error(response.status_code, payload)
        return payload

    def _decode_response(self, response: requests.Response) -> Any:
        try:
            return response.json()
        except ValueError as exc:  # pragma: no cover - defensive branch
            raise MarketDataError("Binance returned a non-JSON payload") from exc

    def _raise_http_error(self, status_code: int, payload: Any) -> None:
        message = self._extract_message(payload) or f"HTTP {status_code}"
        if status_code in {418, 429, 451} or status_code >= 500:
            raise ExchangeTransientError(message)
        raise MarketDataError(message)

    def _raise_api_error(self, code: int, message: str | None) -> None:
        msg = message or f"Binance error code {code}"
        if code == -1121:
            raise SymbolNotSupportedError(msg)
        if code == -1120:
            raise IntervalNotSupportedError(msg)
        raise MarketDataError(msg)

    def _parse_kline(self, raw: Sequence[Any]) -> USDTPerpKline:
        if len(raw) < 6:
            raise MarketDataError("Unexpected Binance kline payload structure")
        open_time = int(raw[0])
        open_price = Decimal(str(raw[1]))
        high = Decimal(str(raw[2]))
        low = Decimal(str(raw[3]))
        close = Decimal(str(raw[4]))
        volume_value = Decimal(str(raw[5])) if len(raw) > 5 else Decimal("0")
        return (open_time, open_price, high, low, close, volume_value)

    def _parse_funding_point(self, raw: dict[str, Any]) -> USDTPerpFundingRatePoint:
        return (int(raw["fundingTime"]), Decimal(raw["fundingRate"]))

    def _parse_snapshot_from_kline(
        self, raw: Sequence[Any], *, endpoint_name: str
    ) -> tuple[int, Decimal]:
        if len(raw) < 5:
            raise MarketDataError(
                f"Unexpected Binance {endpoint_name} kline payload structure"
            )
        close_price = Decimal(str(raw[4]))
        timestamp = int(raw[6]) if len(raw) > 6 else int(raw[0])
        return (timestamp, close_price)

    def _latest_closed_kline(
        self,
        endpoint: str,
        symbol: Symbol,
        *,
        key: str,
        endpoint_name: str,
        only_closed: bool = True,
    ) -> Sequence[Any]:
        params = {
            key: symbol.pair,
            "interval": Interval.MINUTE_1.value,
            "limit": 2,
        }
        payload = self._request(endpoint, params)
        if not payload:
            raise MarketDataError(f"Binance returned empty {endpoint_name} payload")
        candidate = payload[-1]
        close_time = self._extract_close_time(candidate)
        now = int(time.time() * 1000)
        if only_closed and close_time > now and len(payload) > 1:
            candidate = payload[-2]
        return candidate

    def _extract_close_time(self, raw: Sequence[Any]) -> int:
        if len(raw) > 6:
            return int(raw[6])
        # close time is not present on the premium index feed; infer it from the
        # open timestamp plus the one-minute interval length.
        return int(raw[0]) + 60_000

    def _extract_message(self, payload: Any) -> str | None:
        if isinstance(payload, dict):
            msg = payload.get("msg") or payload.get("message")
            if isinstance(msg, str):
                return msg
        return None

    def _parse_instrument(self, raw: dict[str, Any]) -> USDTPerpInstrument:
        symbol = str(raw.get("symbol") or "")
        base_asset = str(raw.get("baseAsset") or "")
        quote_asset = str(raw.get("quoteAsset") or "")
        status = str(raw.get("status") or "")
        is_active = status.upper() == "TRADING"
        price_filter = self._find_filter(raw, "PRICE_FILTER")
        lot_filter = self._find_filter(raw, "LOT_SIZE")
        tick_size = Decimal(price_filter.get("tickSize", "0"))
        step_size = Decimal(lot_filter.get("stepSize", "0"))
        min_qty = Decimal(lot_filter.get("minQty", "0"))
        max_qty = Decimal(lot_filter.get("maxQty", "0"))
        return {
            "symbol": symbol,
            "base_asset": base_asset,
            "quote_asset": quote_asset,
            "tick_size": tick_size,
            "step_size": step_size,
            "min_qty": min_qty,
            "max_qty": max_qty,
            "status": is_active,
        }

    def _find_filter(self, raw: dict[str, Any], filter_type: str) -> dict[str, Any]:
        filters = raw.get("filters")
        if isinstance(filters, Sequence):
            for flt in filters:
                if isinstance(flt, dict) and flt.get("filterType") == filter_type:
                    return flt
        raise MarketDataError(f"Binance instrument missing {filter_type} filter")


def _to_milliseconds(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


def register(*, replace: bool = False) -> None:
    """Register the Binance data source in the global registry."""

    register_usdt_perp_source(Exchange.BINANCE, lambda: BinanceUSDTPerpDataSource(), replace=replace)


register()
