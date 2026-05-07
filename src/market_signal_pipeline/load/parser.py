"""Parses bronze JSON blobs into DailySeries. Handles Alpha Vantage and yfinance shapes."""

import json
from datetime import date
from decimal import Decimal
from typing import Any

from market_signal_pipeline.ingest.exceptions import MalformedResponseError
from market_signal_pipeline.ingest.models import DailyBar, DailySeries


def parse_blob(raw_bytes: bytes, ticker: str) -> tuple[DailySeries, str]:
    """Parse raw blob bytes into a DailySeries and source label.

    Returns (series, source) where source is 'alpha_vantage' or 'yahoo_finance'.
    """
    try:
        payload: dict[str, Any] = json.loads(raw_bytes)
    except ValueError as exc:
        raise MalformedResponseError(f"Blob for {ticker} is not valid JSON") from exc

    if "Time Series (Daily)" in payload:
        return _parse_alpha_vantage(payload, ticker), "alpha_vantage"
    if payload.get("source") == "yahoo_finance":
        return _parse_yahoo_finance(payload, ticker), "yahoo_finance"

    raise MalformedResponseError(f"Unrecognised blob shape for {ticker}")


def _parse_alpha_vantage(payload: dict[str, Any], ticker: str) -> DailySeries:
    try:
        meta = payload["Meta Data"]
        time_series = payload["Time Series (Daily)"]
        symbol: str = meta["2. Symbol"]
        last_refreshed = date.fromisoformat(meta["3. Last Refreshed"])
    except KeyError as exc:
        raise MalformedResponseError(
            f"Missing key in Alpha Vantage blob for {ticker}: {exc}"
        ) from exc

    bars: list[DailyBar] = []
    for date_str, values in time_series.items():
        try:
            bars.append(
                DailyBar(
                    date=date.fromisoformat(date_str),
                    open=Decimal(values["1. open"]),
                    high=Decimal(values["2. high"]),
                    low=Decimal(values["3. low"]),
                    close=Decimal(values["4. close"]),
                    volume=int(values["5. volume"]),
                )
            )
        except (KeyError, ValueError) as exc:
            raise MalformedResponseError(
                f"Malformed Alpha Vantage bar for {ticker} at {date_str}: {exc}"
            ) from exc

    bars.sort(key=lambda b: b.bar_date, reverse=True)
    return DailySeries(symbol=symbol, last_refreshed=last_refreshed, bars=tuple(bars))


def _parse_yahoo_finance(payload: dict[str, Any], ticker: str) -> DailySeries:
    try:
        last_refreshed = date.fromisoformat(payload["last_refreshed"])
        records: list[dict[str, Any]] = payload["bars"]
    except KeyError as exc:
        raise MalformedResponseError(f"Missing key in yfinance blob for {ticker}: {exc}") from exc

    bars: list[DailyBar] = []
    for record in records:
        try:
            bars.append(
                DailyBar(
                    date=date.fromisoformat(record["date"]),
                    open=Decimal(str(record["open"])),
                    high=Decimal(str(record["high"])),
                    low=Decimal(str(record["low"])),
                    close=Decimal(str(record["close"])),
                    volume=int(record["volume"]),
                )
            )
        except (KeyError, ValueError) as exc:
            raise MalformedResponseError(
                f"Malformed yfinance bar for {ticker} at {record.get('date')!r}: {exc}"
            ) from exc

    bars.sort(key=lambda b: b.bar_date, reverse=True)
    return DailySeries(symbol=ticker, last_refreshed=last_refreshed, bars=tuple(bars))
