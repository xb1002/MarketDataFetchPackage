"""Binance USDT perpetual market data implementation."""

from __future__ import annotations

from datetime import datetime, timezone
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
    USDTPerpFundingRatePoint,
    USDTPerpKline,
    USDTPerpMarkPrice,
    USDTPerpOpenInterest,
)

BASE_URL = "https://fapi.binance.com"
PRICE_KLINES_ENDPOINT = "/fapi/v1/klines"
INDEX_KLINES_ENDPOINT = "/fapi/v1/indexPriceKlines"
PREMIUM_KLINES_ENDPOINT = "/fapi/v1/premiumIndexKlines"
FUNDING_HISTORY_ENDPOINT = "/fapi/v1/fundingRate"
PREMIUM_INDEX_ENDPOINT = "/fapi/v1/premiumIndex"
OPEN_INTEREST_ENDPOINT = "/fapi/v1/openInterest"
DEFAULT_TIMEOUT = 10.0


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
            self._historical_params(query, key="symbol"),
        )
        return [self._parse_kline(query.symbol, raw) for raw in payload]

    def get_index_price_klines(self, query: HistoricalWindow) -> Sequence[USDTPerpKline]:
        payload = self._request(
            INDEX_KLINES_ENDPOINT,
            self._historical_params(query, key="pair"),
        )
        return [self._parse_kline(query.symbol, raw) for raw in payload]

    def get_premium_index_klines(self, query: HistoricalWindow) -> Sequence[USDTPerpKline]:
        payload = self._request(
            PREMIUM_KLINES_ENDPOINT,
            self._historical_params(query, key="symbol"),
        )
        return [self._parse_kline(query.symbol, raw) for raw in payload]

    def get_funding_rate_history(self, query: FundingRateWindow) -> Sequence[USDTPerpFundingRatePoint]:
        params = {
            "symbol": query.symbol.pair,
            "limit": query.limit,
        }
        if query.start_time:
            params["startTime"] = _to_milliseconds(query.start_time)
        if query.end_time:
            params["endTime"] = _to_milliseconds(query.end_time)
        payload = self._request(FUNDING_HISTORY_ENDPOINT, params)
        return [self._parse_funding_point(entry, query.symbol) for entry in payload]

    # ------------------------------------------------------------------
    # Latest snapshots
    def get_latest_price(self, symbol: Symbol) -> USDTPerpMarkPrice:
        payload = self._request(PREMIUM_INDEX_ENDPOINT, {"symbol": symbol.pair})
        return USDTPerpMarkPrice(
            symbol=symbol,
            price=Decimal(payload["markPrice"]),
            index_price=Decimal(payload["indexPrice"]),
            last_funding_rate=Decimal(payload["lastFundingRate"]),
            next_funding_time=_from_milliseconds(payload["nextFundingTime"]),
        )

    def get_latest_index_price(self, symbol: Symbol) -> USDTPerpKline:
        params = {
            "pair": symbol.pair,
            "interval": Interval.MINUTE_1.value,
            "limit": 1,
        }
        payload = self._request(INDEX_KLINES_ENDPOINT, params)
        if not payload:
            raise MarketDataError("Binance returned empty index kline payload")
        return self._parse_kline(symbol, payload[0])

    def get_latest_premium_index(self, symbol: Symbol) -> USDTPerpKline:
        params = {
            "symbol": symbol.pair,
            "interval": Interval.MINUTE_1.value,
            "limit": 1,
        }
        payload = self._request(PREMIUM_KLINES_ENDPOINT, params)
        if not payload:
            raise MarketDataError("Binance returned empty premium index payload")
        return self._parse_kline(symbol, payload[0])

    def get_latest_funding_rate(self, symbol: Symbol) -> USDTPerpFundingRatePoint:
        payload = self._request(
            FUNDING_HISTORY_ENDPOINT,
            {"symbol": symbol.pair, "limit": 1},
        )
        if not payload:
            raise MarketDataError("Binance returned empty funding rate payload")
        return self._parse_funding_point(payload[0], symbol)

    def get_open_interest(self, symbol: Symbol) -> USDTPerpOpenInterest:
        payload = self._request(OPEN_INTEREST_ENDPOINT, {"symbol": symbol.pair})
        return USDTPerpOpenInterest(
            symbol=symbol,
            timestamp=_from_milliseconds(payload["time"]),
            value=Decimal(payload["openInterest"]),
        )

    # ------------------------------------------------------------------
    # Internal helpers
    def close(self) -> None:
        if self._owns_session:
            self._session.close()

    def _historical_params(self, query: HistoricalWindow, *, key: str) -> dict[str, Any]:
        params: dict[str, Any] = {
            key: query.symbol.pair,
            "interval": query.interval.value,
            "limit": query.limit,
        }
        if query.start_time:
            params["startTime"] = _to_milliseconds(query.start_time)
        if query.end_time:
            params["endTime"] = _to_milliseconds(query.end_time)
        return params

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

    def _parse_kline(self, symbol: Symbol, raw: Sequence[Any]) -> USDTPerpKline:
        if len(raw) < 6:
            raise MarketDataError("Unexpected Binance kline payload structure")
        open_time = _from_milliseconds(raw[0])
        open_price = Decimal(str(raw[1]))
        high = Decimal(str(raw[2]))
        low = Decimal(str(raw[3]))
        close = Decimal(str(raw[4]))
        if len(raw) >= 8:
            volume_value = Decimal(str(raw[5]))
            close_time = _from_milliseconds(raw[6])
            quote_volume_value = Decimal(str(raw[7]))
        else:
            volume_value = Decimal("0")
            close_time = _from_milliseconds(raw[-1])
            quote_volume_value = Decimal("0")
        return USDTPerpKline(
            symbol=symbol,
            open_time=open_time,
            close_time=close_time,
            open=open_price,
            high=high,
            low=low,
            close=close,
            volume=volume_value,
            quote_volume=quote_volume_value,
        )

    def _parse_funding_point(self, raw: dict[str, Any], symbol: Symbol) -> USDTPerpFundingRatePoint:
        predicted_raw = raw.get("predictedFundingRate") or raw.get("estimatedSettlePrice")
        predicted_rate = Decimal(predicted_raw) if predicted_raw is not None else None
        return USDTPerpFundingRatePoint(
            symbol=symbol,
            timestamp=_from_milliseconds(raw["fundingTime"]),
            rate=Decimal(raw["fundingRate"]),
            predicted_rate=predicted_rate,
        )

    def _extract_message(self, payload: Any) -> str | None:
        if isinstance(payload, dict):
            msg = payload.get("msg") or payload.get("message")
            if isinstance(msg, str):
                return msg
        return None


def _to_milliseconds(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


def _from_milliseconds(value: int | float | str) -> datetime:
    return datetime.fromtimestamp(float(value) / 1000, tz=timezone.utc)


def register(*, replace: bool = False) -> None:
    """Register the Binance data source in the global registry."""

    register_usdt_perp_source(Exchange.BINANCE, lambda: BinanceUSDTPerpDataSource(), replace=replace)


register()
