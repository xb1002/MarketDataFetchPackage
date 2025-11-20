"""OKX USDT perpetual market data implementation."""

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
from ...core.queries import DEFAULT_LIMIT, FundingRateWindow, HistoricalWindow
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
    USDTPerpPremiumIndexPoint,
    USDTPerpTicker,
)

BASE_URL = "https://www.okx.com"
PRICE_KLINES_ENDPOINT = "/api/v5/market/candles"
INDEX_KLINES_ENDPOINT = "/api/v5/market/index-candles"
MARK_PRICE_KLINES_ENDPOINT = "/api/v5/market/mark-price-candles"
PREMIUM_HISTORY_ENDPOINT = "/api/v5/public/premium-history"
FUNDING_HISTORY_ENDPOINT = "/api/v5/public/funding-rate-history"
FUNDING_LATEST_ENDPOINT = "/api/v5/public/funding-rate"
TICKERS_ENDPOINT = "/api/v5/market/tickers"
INDEX_TICKERS_ENDPOINT = "/api/v5/market/index-tickers"
MARK_PRICE_ENDPOINT = "/api/v5/public/mark-price"
OPEN_INTEREST_ENDPOINT = "/api/v5/public/open-interest"
INSTRUMENTS_ENDPOINT = "/api/v5/public/instruments"
DEFAULT_TIMEOUT = 10.0
PRICE_KLINES_MAX_LIMIT = 300
INDEX_KLINES_MAX_LIMIT = 100
MARK_PRICE_KLINES_MAX_LIMIT = 100
PREMIUM_HISTORY_MAX_LIMIT = 100
FUNDING_HISTORY_MAX_LIMIT = 400

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

RATE_LIMIT_ERROR_CODES = {"50011", "50012", "50013"}
SUCCESS_CODES = {"0", "00000"}


class OkxUSDTPerpDataSource(USDTPerpMarketDataSource):
    """Requests-backed implementation for OKX USDT perpetual markets."""

    exchange = Exchange.OKX

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
        entries = self._fetch_candles(
            PRICE_KLINES_ENDPOINT,
            query,
            inst_id=self._contract_inst_id(query.symbol),
            endpoint_name="price klines",
            max_limit=PRICE_KLINES_MAX_LIMIT,
        )
        klines = [self._parse_kline(row) for row in entries]
        return self._sort_klines(klines)

    def get_index_price_klines(self, query: HistoricalWindow) -> Sequence[USDTPerpKline]:
        entries = self._fetch_candles(
            INDEX_KLINES_ENDPOINT,
            query,
            inst_id=self._index_inst_id(query.symbol),
            endpoint_name="index price klines",
            max_limit=INDEX_KLINES_MAX_LIMIT,
        )
        klines = [self._parse_kline(row, zero_volume=True) for row in entries]
        return self._sort_klines(klines)

    def get_mark_price_klines(self, query: HistoricalWindow) -> Sequence[USDTPerpKline]:
        entries = self._fetch_candles(
            MARK_PRICE_KLINES_ENDPOINT,
            query,
            inst_id=self._contract_inst_id(query.symbol),
            endpoint_name="mark price klines",
            max_limit=MARK_PRICE_KLINES_MAX_LIMIT,
        )
        klines = [self._parse_kline(row, zero_volume=True) for row in entries]
        return self._sort_klines(klines)

    def get_premium_index_klines(self, query: HistoricalWindow) -> Sequence[USDTPerpKline]:
        entries = self._fetch_premium_history(
            query,
            endpoint_name="premium index history",
        )
        klines = [self._build_flat_kline(item) for item in entries]
        return self._sort_klines(klines)

    def get_funding_rate_history(self, query: FundingRateWindow) -> Sequence[USDTPerpFundingRatePoint]:
        params = {
            "instId": self._contract_inst_id(query.symbol),
        }
        limit = self._enforce_limit(
            query.limit,
            FUNDING_HISTORY_MAX_LIMIT,
            endpoint_name="funding rate history",
        )
        params["limit"] = str(limit)
        payload = self._request(FUNDING_HISTORY_ENDPOINT, params)
        entries = self._extract_sequence(payload, endpoint_name="funding rate history")
        return [self._parse_funding_point(item) for item in entries]

    # ------------------------------------------------------------------
    # Latest snapshots
    def get_latest_ticker(self, symbol: Symbol) -> USDTPerpTicker:
        ticker = self._fetch_ticker(symbol)
        index_ticker = self._fetch_index_ticker(symbol)
        mark_entry = self._fetch_mark_snapshot(symbol)
        timestamp = int(ticker.get("ts") or index_ticker.get("ts") or mark_entry.get("ts") or 0)
        return {
            "timestamp": timestamp,
            "last_price": self._to_decimal(ticker.get("last")),
            "index_price": self._to_decimal(index_ticker.get("idxPx")),
            "mark_price": self._to_decimal(mark_entry.get("markPx")),
        }

    def get_latest_mark_price(self, symbol: Symbol) -> USDTPerpMarkPrice:
        mark_entry = self._fetch_mark_snapshot(symbol)
        mark_price = self._to_decimal(mark_entry.get("markPx"))
        timestamp = int(mark_entry.get("ts") or 0)
        return (timestamp, mark_price)

    def get_latest_index_price(self, symbol: Symbol) -> USDTPerpIndexPricePoint:
        entry = self._fetch_index_ticker(symbol)
        value = self._to_decimal(entry.get("idxPx"))
        timestamp = int(entry.get("ts") or 0)
        return (timestamp, value)

    def get_latest_premium_index(self, symbol: Symbol) -> USDTPerpPremiumIndexPoint:
        entries = self._fetch_premium_history(
            HistoricalWindow(symbol=symbol, interval=Interval.MINUTE_1, limit=1),
            endpoint_name="premium index history",
        )
        if not entries:
            raise MarketDataError("OKX returned empty premium history payload")
        timestamp, _, _, _, close, _ = self._build_flat_kline(entries[0])
        return (timestamp, close)

    def get_latest_funding_rate(self, symbol: Symbol) -> USDTPerpFundingRate:
        entry = self._fetch_latest_funding(symbol)
        rate = self._to_decimal(entry.get("fundingRate") or entry.get("nextFundingRate"))
        next_time = int(entry.get("fundingTime") or entry.get("nextFundingTime") or entry.get("ts") or 0)
        return {"funding_rate": rate, "next_funding_time": next_time}

    def get_open_interest(self, symbol: Symbol) -> USDTPerpOpenInterest:
        params = {"instId": self._contract_inst_id(symbol)}
        payload = self._request(OPEN_INTEREST_ENDPOINT, params)
        entries = self._extract_sequence(payload, endpoint_name="open interest")
        entry = entries[0]
        timestamp = int(entry.get("ts") or 0)
        value = self._to_decimal(entry.get("oiUsd") or entry.get("oi"))
        return (timestamp, value)

    def get_instruments(self) -> Sequence[USDTPerpInstrument]:
        payload = self._request(INSTRUMENTS_ENDPOINT, {"instType": "SWAP"})
        entries = self._extract_sequence(payload, endpoint_name="instruments")
        instruments: list[USDTPerpInstrument] = []
        for entry in entries:
            if entry.get("settleCcy") != "USDT":
                continue
            instruments.append(self._parse_instrument(entry))
        if not instruments:
            raise MarketDataError("OKX did not return any USDT perpetual instruments")
        return instruments

    # ------------------------------------------------------------------
    # Internal helpers
    def close(self) -> None:
        if self._owns_session:
            self._session.close()

    def _sort_klines(self, klines: Sequence[USDTPerpKline]) -> list[USDTPerpKline]:
        return sorted(klines, key=lambda entry: entry[0])

    def _fetch_candles(
        self,
        endpoint: str,
        query: HistoricalWindow,
        *,
        inst_id: str,
        endpoint_name: str,
        max_limit: int,
    ) -> Sequence[Sequence[Any]]:
        params = {
            "instId": inst_id,
            "bar": self._map_interval(query.interval),
        }
        self._apply_time_filters(params, query.start_time, query.end_time)
        limit = self._enforce_limit(query.limit, max_limit, endpoint_name=endpoint_name)
        params["limit"] = str(limit)
        payload = self._request(endpoint, params)
        return self._extract_sequence(payload, endpoint_name=endpoint_name)

    def _fetch_premium_history(
        self,
        query: HistoricalWindow,
        *,
        endpoint_name: str,
    ) -> Sequence[dict[str, Any]]:
        params = {"instId": self._contract_inst_id(query.symbol)}
        self._apply_time_filters(params, query.start_time, query.end_time)
        limit = self._enforce_limit(query.limit, PREMIUM_HISTORY_MAX_LIMIT, endpoint_name=endpoint_name)
        params["limit"] = str(limit)
        payload = self._request(PREMIUM_HISTORY_ENDPOINT, params)
        return self._extract_sequence(payload, endpoint_name=endpoint_name)

    def _fetch_ticker(self, symbol: Symbol) -> dict[str, Any]:
        params = {"instType": "SWAP", "instId": self._contract_inst_id(symbol)}
        payload = self._request(TICKERS_ENDPOINT, params)
        entries = self._extract_sequence(payload, endpoint_name="tickers")
        inst_id = self._contract_inst_id(symbol)
        match = next((item for item in entries if item.get("instId") == inst_id), None)
        if not match:
            raise MarketDataError(f"OKX ticker payload missing {inst_id}")
        return match

    def _fetch_index_ticker(self, symbol: Symbol) -> dict[str, Any]:
        params = {"instId": self._index_inst_id(symbol)}
        payload = self._request(INDEX_TICKERS_ENDPOINT, params)
        entries = self._extract_sequence(payload, endpoint_name="index tickers")
        inst_id = self._index_inst_id(symbol)
        match = next((item for item in entries if item.get("instId") == inst_id), None)
        if not match:
            raise MarketDataError(f"OKX index ticker payload missing {inst_id}")
        return match

    def _fetch_mark_snapshot(self, symbol: Symbol) -> dict[str, Any]:
        params = {"instType": "SWAP", "uly": self._underlying(symbol)}
        payload = self._request(MARK_PRICE_ENDPOINT, params)
        entries = self._extract_sequence(payload, endpoint_name="mark price snapshot")
        match = next((item for item in entries if item.get("instId") == self._contract_inst_id(symbol)), None)
        return match or entries[0]

    def _fetch_latest_funding(self, symbol: Symbol) -> dict[str, Any]:
        params = {"instId": self._contract_inst_id(symbol)}
        payload = self._request(FUNDING_LATEST_ENDPOINT, params)
        entries = self._extract_sequence(payload, endpoint_name="latest funding rate")
        return entries[0]

    def _fetch_recent_funding(self, symbol: Symbol) -> dict[str, Any]:
        window = FundingRateWindow(symbol=symbol, limit=1)
        params = {"instId": self._contract_inst_id(window.symbol)}
        limit = self._enforce_limit(window.limit, FUNDING_HISTORY_MAX_LIMIT, endpoint_name="funding rate history")
        params["limit"] = str(limit)
        payload = self._request(FUNDING_HISTORY_ENDPOINT, params)
        entries = self._extract_sequence(payload, endpoint_name="funding rate history")
        return entries[0]

    def _parse_kline(self, raw: Sequence[Any], *, zero_volume: bool = False) -> USDTPerpKline:
        if len(raw) < 5:
            raise MarketDataError("Unexpected OKX kline payload structure")
        timestamp = int(raw[0])
        open_price = self._to_decimal(raw[1])
        high = self._to_decimal(raw[2])
        low = self._to_decimal(raw[3])
        close = self._to_decimal(raw[4])
        if zero_volume:
            volume_source = Decimal("0")
        elif len(raw) > 6 and raw[6] not in (None, ""):
            volume_source = self._to_decimal(raw[6])
        else:
            volume_source = self._to_decimal(raw[5] if len(raw) > 5 else "0")
        return (timestamp, open_price, high, low, close, volume_source)

    def _build_flat_kline(self, raw: dict[str, Any]) -> USDTPerpKline:
        timestamp = int(raw.get("ts") or 0)
        value = self._to_decimal(raw.get("premium"))
        return (timestamp, value, value, value, value, Decimal("0"))

    def _parse_funding_point(self, raw: dict[str, Any]) -> USDTPerpFundingRatePoint:
        timestamp = int(raw.get("fundingTime") or raw.get("ts") or 0)
        rate = self._to_decimal(raw.get("fundingRate"))
        return (timestamp, rate)

    def _parse_instrument(self, raw: dict[str, Any]) -> USDTPerpInstrument:
        inst_id = str(raw.get("instId") or "")
        base, quote = self._split_underlying(str(raw.get("uly") or ""))
        tick_size = self._to_decimal(raw.get("tickSz"))
        step_size = self._to_decimal(raw.get("lotSz"))
        min_qty = self._to_decimal(raw.get("minSz"))
        max_qty = self._to_decimal(raw.get("maxLmtSz") or raw.get("maxMktSz") or "0")
        symbol = f"{base}{quote or 'USDT'}"
        status = str(raw.get("state") or "")
        return {
            "symbol": symbol,
            "base_asset": base,
            "quote_asset": quote or "USDT",
            "tick_size": tick_size,
            "step_size": step_size,
            "min_qty": min_qty,
            "max_qty": max_qty,
            "status": status,
        }

    def _extract_sequence(self, payload: dict[str, Any], *, endpoint_name: str) -> Sequence[Any]:
        data = payload.get("data")
        if not isinstance(data, Sequence) or not data:
            raise MarketDataError(f"OKX returned empty {endpoint_name} payload")
        return data

    def _apply_time_filters(
        self,
        params: dict[str, Any],
        start_time: datetime | None,
        end_time: datetime | None,
    ) -> None:
        """Attach OKX-specific before/after filters.

        OKX uses a somewhat counter-intuitive pagination contract:
        * ``after`` returns candles **older** than the supplied timestamp.
        * ``before`` returns candles **newer** than the supplied timestamp.

        ``HistoricalWindow`` follows the conventional semantics where
        ``start_time`` is the *oldest* bound and ``end_time`` is the *newest*
        bound. To keep the public interface consistent across exchanges we
        map the parameters so callers can continue to supply either (or both)
        timestamps without worrying about OKX's inverted behaviour.
        """

        if start_time:
            params["before"] = str(_datetime_to_ms(start_time))
        if end_time:
            params["after"] = str(_datetime_to_ms(end_time))

    def _enforce_limit(self, requested: int, max_limit: int, *, endpoint_name: str) -> int:
        if requested > max_limit:
            if requested == DEFAULT_LIMIT:
                return max_limit
            raise ValueError(f"OKX {endpoint_name} limit cannot exceed {max_limit} entries")
        return requested

    def _map_interval(self, interval: Interval) -> str:
        try:
            return INTERVAL_MAP[interval]
        except KeyError as exc:
            raise IntervalNotSupportedError(f"Interval {interval} is not supported by OKX") from exc

    def _request(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self._base_url}{path}"
        try:
            response = self._session.get(url, params=params, timeout=self._timeout)
        except requests.RequestException as exc:  # pragma: no cover - network failure
            raise ExchangeTransientError("OKX request failed") from exc
        if response.status_code in {429, 500, 502, 503, 504}:
            raise ExchangeTransientError(f"OKX temporary HTTP error: {response.status_code}")
        try:
            payload = response.json()
        except ValueError as exc:
            raise MarketDataError("OKX returned a non-JSON payload") from exc
        code = str(payload.get("code") or "0")
        if code not in SUCCESS_CODES:
            msg = payload.get("msg") or payload.get("error_message") or "unknown error"
            if code in RATE_LIMIT_ERROR_CODES:
                raise ExchangeTransientError(f"OKX rate limited the request: {msg}")
            if code == "51001":
                raise SymbolNotSupportedError(f"OKX symbol error: {msg}")
            raise MarketDataError(f"OKX error ({code}): {msg}")
        return payload

    def _contract_inst_id(self, symbol: Symbol) -> str:
        base = symbol.base.upper()
        quote = symbol.quote.upper()
        return f"{base}-{quote}-SWAP"

    def _index_inst_id(self, symbol: Symbol) -> str:
        base = symbol.base.upper()
        quote = symbol.quote.upper()
        return f"{base}-{quote}"

    def _underlying(self, symbol: Symbol) -> str:
        base = symbol.base.upper()
        quote = symbol.quote.upper()
        return f"{base}-{quote}"

    def _split_underlying(self, underlying: str) -> tuple[str, str]:
        if "-" in underlying:
            parts = underlying.split("-")
            if len(parts) >= 2:
                return parts[0], parts[1]
        return underlying, ""

    def _to_decimal(self, value: Any) -> Decimal:
        if value in ("", None):
            return Decimal("0")
        return Decimal(str(value))


def _datetime_to_ms(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


def register(*, replace: bool = False) -> None:
    """Register the OKX data source with the global registry."""

    register_usdt_perp_source(Exchange.OKX, lambda: OkxUSDTPerpDataSource(), replace=replace)


register()
