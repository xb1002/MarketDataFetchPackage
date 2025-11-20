from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from market_data_fetch.contracts.usdt_perp.interface import USDTPerpMarketDataSource
from market_data_fetch.exchanges.binance.usdt_perp import BinanceUSDTPerpDataSource
from market_data_fetch.exchanges.bitget.usdt_perp import BitgetUSDTPerpDataSource
from market_data_fetch.exchanges.bybit.usdt_perp import BybitUSDTPerpDataSource
from market_data_fetch.exchanges.okx.usdt_perp import OkxUSDTPerpDataSource
from market_data_fetch.models.shared import Symbol

BINANCE_TESTNET_BASE_URL = "https://testnet.binancefuture.com"
BYBIT_BASE_URL = "https://api.bybit.com"
BITGET_BASE_URL = "https://api.bitget.com"
OKX_BASE_URL = "https://www.okx.com"


@dataclass(slots=True)
class ProviderCase:
    name: str
    factory: Callable[[], USDTPerpMarketDataSource]
    symbol: Symbol
    price_limit: int
    funding_limit: int


PROVIDERS: tuple[ProviderCase, ...] = (
    ProviderCase(
        name="binance",
        factory=lambda: BinanceUSDTPerpDataSource(base_url=BINANCE_TESTNET_BASE_URL),
        symbol=Symbol("BTC", "USDT"),
        price_limit=1500,
        funding_limit=1000,
    ),
    ProviderCase(
        name="bybit",
        factory=lambda: BybitUSDTPerpDataSource(base_url=BYBIT_BASE_URL),
        symbol=Symbol("BTC", "USDT"),
        price_limit=1000,
        funding_limit=200,
    ),
    ProviderCase(
        name="bitget",
        factory=lambda: BitgetUSDTPerpDataSource(base_url=BITGET_BASE_URL),
        symbol=Symbol("BTC", "USDT"),
        price_limit=100,
        funding_limit=200,
    ),
    ProviderCase(
        name="okx",
        factory=lambda: OkxUSDTPerpDataSource(base_url=OKX_BASE_URL),
        symbol=Symbol("BTC", "USDT"),
        price_limit=300,
        funding_limit=400,
    ),
)
