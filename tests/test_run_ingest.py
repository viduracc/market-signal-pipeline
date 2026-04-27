"""Tests for the ingestion entrypoint orchestration."""

from datetime import UTC, datetime
from unittest.mock import MagicMock

from market_signal_pipeline.ingest.alpha_vantage import AlphaVantageClient
from market_signal_pipeline.ingest.bronze import BronzeWriter
from market_signal_pipeline.ingest.exceptions import RateLimitError
from market_signal_pipeline.ingest.models import DailyBar, DailySeries
from scripts.run_ingest import IngestionResult, run_ingestion


def _make_series(symbol: str) -> DailySeries:
    from datetime import date
    from decimal import Decimal

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


def test_run_ingestion_all_succeed() -> None:
    client = MagicMock(spec=AlphaVantageClient)

    def _fetch_side_effect(ticker: str) -> tuple[DailySeries, bytes]:
        return _make_series(ticker), b'{"raw": "data"}'

    client.fetch_daily.side_effect = _fetch_side_effect

    writer = MagicMock(spec=BronzeWriter)
    writer.write.return_value = "2026/04/27/MSFT.json"

    result = run_ingestion(
        client=client,
        writer=writer,
        tickers=("MSFT", "AAPL"),
        ingest_date=datetime(2026, 4, 27, tzinfo=UTC),
    )

    assert result.successes == ["MSFT", "AAPL"]
    assert result.failures == {}
    assert result.any_failed is False
    assert client.fetch_daily.call_count == 2
    assert writer.write.call_count == 2


def test_run_ingestion_partial_failure() -> None:
    client = MagicMock(spec=AlphaVantageClient)

    def fetch_side_effect(ticker: str) -> tuple[DailySeries, bytes]:
        if ticker == "AAPL":
            raise RateLimitError("rate limited")
        return _make_series(ticker), b'{"raw": "data"}'

    client.fetch_daily.side_effect = fetch_side_effect

    writer = MagicMock(spec=BronzeWriter)
    writer.write.return_value = "blob/path"

    result = run_ingestion(
        client=client,
        writer=writer,
        tickers=("MSFT", "AAPL", "GOOGL"),
        ingest_date=datetime(2026, 4, 27, tzinfo=UTC),
    )

    assert result.successes == ["MSFT", "GOOGL"]
    assert "AAPL" in result.failures
    assert "RateLimitError" in result.failures["AAPL"]
    assert result.any_failed is True


def test_run_ingestion_all_fail() -> None:
    client = MagicMock(spec=AlphaVantageClient)
    client.fetch_daily.side_effect = RateLimitError("api down")

    writer = MagicMock(spec=BronzeWriter)

    result = run_ingestion(
        client=client,
        writer=writer,
        tickers=("MSFT", "AAPL"),
        ingest_date=datetime(2026, 4, 27, tzinfo=UTC),
    )

    assert result.successes == []
    assert len(result.failures) == 2
    assert result.any_failed is True
    writer.write.assert_not_called()


def test_ingestion_result_any_failed_property() -> None:
    empty = IngestionResult()
    assert empty.any_failed is False

    with_success = IngestionResult(successes=["MSFT"])
    assert with_success.any_failed is False

    with_failure = IngestionResult(failures={"MSFT": "error"})
    assert with_failure.any_failed is True
