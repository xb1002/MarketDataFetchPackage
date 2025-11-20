"""OKX exchange integration."""

from .usdt_perp import OkxUSDTPerpDataSource, register

__all__ = ["OkxUSDTPerpDataSource", "register"]
