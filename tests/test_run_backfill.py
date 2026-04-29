"""Tests for the backfill entrypoint orchestration."""

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

from market_signal_pipeline.ingest.bronze import BronzeWriter
from market_signal_pipeline.ingest.exceptions import IngestError
from market_signal_pipeline.ingest.models import DailyBar, DailySeries
from market_signal_pipeline.ingest.yahoo_finance import YahooFinanceClient
from scripts.run_backfill import BackfillResult, run_backfill


def _make_series(symbol: str) -> DailySeries:
    return DailySeries(
        symbol=symbol,
        last_refreshed=date(2026, 4, 27),
        bars=(
            DailyBar(
                date=date(2026, 4, 27),
                open=Decimal("100"),
                high=Decimal("110"),
                low=Decimal("95"),
                close=Decimal("105"),
                volume=1000,
            ),
        ),
    )


def test_run_backfill_all_succeed() -> None:
    client = MagicMock(spec=YahooFinanceClient)

    def fetch_side_effect(ticker: str, period: str = "max") -> tuple[DailySeries, bytes]:
        return _make_series(ticker), b'{"raw": "historical data"}'

    client.fetch_daily.side_effect = fetch_side_effect

    writer = MagicMock(spec=BronzeWriter)
    writer.write_historical.return_value = "historical/MSFT.json"

    result = run_backfill(
        client=client,
        writer=writer,
        tickers=("MSFT", "AAPL"),
    )

    assert result.successes == ["MSFT", "AAPL"]
    assert result.failures == {}
    assert result.any_failed is False
    assert client.fetch_daily.call_count == 2
    assert writer.write_historical.call_count == 2


def test_run_backfill_uses_max_period() -> None:
    client = MagicMock(spec=YahooFinanceClient)
    client.fetch_daily.return_value = (_make_series("MSFT"), b"{}")

    writer = MagicMock(spec=BronzeWriter)
    writer.write_historical.return_value = "historical/MSFT.json"

    run_backfill(client=client, writer=writer, tickers=("MSFT",))

    client.fetch_daily.assert_called_once_with("MSFT", period="max")


def test_run_backfill_partial_failure() -> None:
    client = MagicMock(spec=YahooFinanceClient)

    def fetch_side_effect(ticker: str, period: str = "max") -> tuple[DailySeries, bytes]:
        if ticker == "AAPL":
            raise IngestError("network failure")
        return _make_series(ticker), b"{}"

    client.fetch_daily.side_effect = fetch_side_effect

    writer = MagicMock(spec=BronzeWriter)
    writer.write_historical.return_value = "historical/path.json"

    result = run_backfill(
        client=client,
        writer=writer,
        tickers=("MSFT", "AAPL", "GOOGL"),
    )

    assert result.successes == ["MSFT", "GOOGL"]
    assert "AAPL" in result.failures
    assert "IngestError" in result.failures["AAPL"]
    assert result.any_failed is True


def test_backfill_result_any_failed_property() -> None:
    empty = BackfillResult()
    assert empty.any_failed is False

    with_success = BackfillResult(successes=["MSFT"])
    assert with_success.any_failed is False

    with_failure = BackfillResult(failures={"MSFT": "error"})
    assert with_failure.any_failed is True
