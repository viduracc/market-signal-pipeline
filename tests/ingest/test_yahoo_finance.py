"""Tests for the Yahoo Finance client."""

import json
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from market_signal_pipeline.ingest.exceptions import IngestError, MalformedResponseError
from market_signal_pipeline.ingest.yahoo_finance import YahooFinanceClient


def _make_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Open": [100.0, 102.0],
            "High": [105.0, 108.0],
            "Low": [99.0, 101.0],
            "Close": [104.0, 107.0],
            "Volume": [1000000, 1200000],
        },
        index=pd.to_datetime(["2026-04-22", "2026-04-23"]),
    )


@pytest.fixture
def client() -> YahooFinanceClient:
    return YahooFinanceClient()


def test_fetch_daily_happy_path(client: YahooFinanceClient) -> None:
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = _make_dataframe()

    with patch(
        "market_signal_pipeline.ingest.yahoo_finance.yf.Ticker",
        return_value=mock_ticker,
    ):
        series, raw = client.fetch_daily("MSFT")

    assert series.symbol == "MSFT"
    assert len(series.bars) == 2
    assert series.bars[0].bar_date == date(2026, 4, 23)
    assert series.bars[0].close == Decimal("107.0")
    assert series.bars[0].volume == 1200000

    payload = json.loads(raw)
    assert payload["source"] == "yahoo_finance"
    assert payload["ticker"] == "MSFT"
    assert payload["period"] == "max"
    assert len(payload["bars"]) == 2


def test_fetch_daily_uses_specified_period(client: YahooFinanceClient) -> None:
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = _make_dataframe()

    with patch(
        "market_signal_pipeline.ingest.yahoo_finance.yf.Ticker",
        return_value=mock_ticker,
    ):
        client.fetch_daily("MSFT", period="5y")

    mock_ticker.history.assert_called_once_with(period="5y", auto_adjust=False, actions=False)


def test_fetch_daily_raises_on_empty_dataframe(client: YahooFinanceClient) -> None:
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = pd.DataFrame()

    with (
        patch(
            "market_signal_pipeline.ingest.yahoo_finance.yf.Ticker",
            return_value=mock_ticker,
        ),
        pytest.raises(MalformedResponseError, match="empty data"),
    ):
        client.fetch_daily("INVALID")


def test_fetch_daily_raises_on_yfinance_exception(client: YahooFinanceClient) -> None:
    mock_ticker = MagicMock()
    mock_ticker.history.side_effect = ConnectionError("network down")

    with (
        patch(
            "market_signal_pipeline.ingest.yahoo_finance.yf.Ticker",
            return_value=mock_ticker,
        ),
        pytest.raises(IngestError, match="yfinance call failed"),
    ):
        client.fetch_daily("MSFT")


def test_fetch_daily_sorts_bars_newest_first(client: YahooFinanceClient) -> None:
    df = pd.DataFrame(
        {
            "Open": [100.0, 102.0, 101.0],
            "High": [105.0, 108.0, 106.0],
            "Low": [99.0, 101.0, 100.0],
            "Close": [104.0, 107.0, 105.0],
            "Volume": [1000000, 1200000, 1100000],
        },
        index=pd.to_datetime(["2026-04-22", "2026-04-24", "2026-04-23"]),
    )
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = df

    with patch(
        "market_signal_pipeline.ingest.yahoo_finance.yf.Ticker",
        return_value=mock_ticker,
    ):
        series, _ = client.fetch_daily("MSFT")

    assert series.bars[0].bar_date == date(2026, 4, 24)
    assert series.bars[1].bar_date == date(2026, 4, 23)
    assert series.bars[2].bar_date == date(2026, 4, 22)
