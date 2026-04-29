"""HTTP client for Yahoo Finance via yfinance. Used for historical bulk pulls."""

import json
from datetime import date
from decimal import Decimal
from typing import Any

import pandas as pd
import structlog
import yfinance as yf  # pyright: ignore[reportMissingTypeStubs]

from market_signal_pipeline.ingest.exceptions import (
    IngestError,
    MalformedResponseError,
)
from market_signal_pipeline.ingest.models import DailyBar, DailySeries

log = structlog.get_logger()

DEFAULT_PERIOD = "max"


class YahooFinanceClient:
    """Wrapper around yfinance for fetching daily OHLCV history."""

    def __repr__(self) -> str:
        return "YahooFinanceClient()"

    def fetch_daily(
        self,
        ticker: str,
        period: str = DEFAULT_PERIOD,
    ) -> tuple[DailySeries, bytes]:
        """Fetch daily OHLCV bars for a ticker. Returns parsed series and JSON-serialized raw bytes.

        period: yfinance period string (e.g. 'max', '5y', '1y').
        """
        log.info("yahoo_finance.fetch_daily.attempt", ticker=ticker, period=period)

        try:
            yf_ticker = yf.Ticker(ticker)
            df = yf_ticker.history(period=period, auto_adjust=False, actions=False)  # pyright: ignore[reportUnknownMemberType]
        except Exception as exc:
            raise IngestError(f"yfinance call failed for {ticker}: {exc}") from exc

        if df.empty:
            raise MalformedResponseError(f"yfinance returned empty data for {ticker}")

        bars: list[DailyBar] = []
        records: list[dict[str, Any]] = []

        for ts, row in df.iterrows():  # pyright: ignore[reportUnknownVariableType]
            try:
                if not isinstance(ts, pd.Timestamp):
                    raise MalformedResponseError(
                        f"Unexpected timestamp type for {ticker}: {type(ts).__name__}"
                    )
                bar_date: date = ts.date()
                bar = DailyBar(
                    date=bar_date,
                    open=Decimal(str(row["Open"])),
                    high=Decimal(str(row["High"])),
                    low=Decimal(str(row["Low"])),
                    close=Decimal(str(row["Close"])),
                    volume=int(row["Volume"]),
                )
                bars.append(bar)
                records.append(
                    {
                        "date": bar_date.isoformat(),
                        "open": str(row["Open"]),
                        "high": str(row["High"]),
                        "low": str(row["Low"]),
                        "close": str(row["Close"]),
                        "volume": int(row["Volume"]),
                    }
                )
            except (KeyError, ValueError, TypeError) as exc:
                raise MalformedResponseError(
                    f"Malformed bar for {ticker} at {ts!r}: {exc}"
                ) from exc

        bars.sort(key=lambda b: b.bar_date, reverse=True)

        last_refreshed: date = bars[0].bar_date

        series = DailySeries(
            symbol=ticker,
            last_refreshed=last_refreshed,
            bars=tuple(bars),
        )

        raw_payload: dict[str, Any] = {
            "source": "yahoo_finance",
            "ticker": ticker,
            "period": period,
            "last_refreshed": last_refreshed.isoformat(),
            "bars": records,
        }
        raw_bytes = json.dumps(raw_payload, indent=2).encode("utf-8")

        log.info(
            "yahoo_finance.fetch_daily.success",
            ticker=ticker,
            bars=len(bars),
            bytes=len(raw_bytes),
        )

        return series, raw_bytes
