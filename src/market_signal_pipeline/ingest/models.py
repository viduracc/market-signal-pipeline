"""Pydantic models for market data."""

from datetime import date
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class DailyBar(BaseModel):
    """One trading day's OHLCV record."""

    model_config = ConfigDict(frozen=True)

    bar_date: date = Field(alias="date")
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int


class DailySeries(BaseModel):
    """A ticker symbol with its time series of daily bars."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    last_refreshed: date
    bars: tuple[DailyBar, ...]
