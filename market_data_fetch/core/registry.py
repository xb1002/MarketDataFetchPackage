"""Registry utilities for mapping exchanges to data source factories."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import MutableMapping

from ..contracts.usdt_perp.interface import USDTPerpMarketDataSource
from ..models.shared import Exchange

USDTPerpSourceFactory = Callable[[], USDTPerpMarketDataSource]


class USDTPerpRegistry:
    """In-memory registry for USDT perpetual data sources."""

    def __init__(self) -> None:
        self._factories: MutableMapping[Exchange, USDTPerpSourceFactory] = {}

    def register(self, exchange: Exchange, factory: USDTPerpSourceFactory, *, replace: bool = False) -> None:
        """Register a factory for the given exchange."""

        if not replace and exchange in self._factories:
            raise ValueError(f"USDT perp source for {exchange} already registered")
        self._factories[exchange] = factory

    def create(self, exchange: Exchange) -> USDTPerpMarketDataSource:
        """Instantiate a source for the given exchange."""

        try:
            factory = self._factories[exchange]
        except KeyError as exc:  # pragma: no cover - defensive branch
            raise ValueError(f"No USDT perp source registered for {exchange}") from exc
        return factory()

    def snapshot(self) -> Mapping[Exchange, USDTPerpSourceFactory]:
        """Return a copy of registered factories."""

        return dict(self._factories)


_registry = USDTPerpRegistry()


def register_usdt_perp_source(exchange: Exchange, factory: USDTPerpSourceFactory, *, replace: bool = False) -> None:
    """Register a factory globally."""

    _registry.register(exchange, factory, replace=replace)


def create_usdt_perp_source(exchange: Exchange) -> USDTPerpMarketDataSource:
    """Create a source instance for the specified exchange."""

    return _registry.create(exchange)


def registered_usdt_perp_sources() -> Mapping[Exchange, USDTPerpSourceFactory]:
    """Expose the underlying factory mapping (primarily for debugging/tests)."""

    return _registry.snapshot()
