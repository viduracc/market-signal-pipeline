"""Tests for the bronze loader entrypoint orchestration."""

from unittest.mock import MagicMock

from market_signal_pipeline.ingest.exceptions import MalformedResponseError
from market_signal_pipeline.load.bronze_reader import BronzeReader
from market_signal_pipeline.load.postgres_writer import PostgresWriter
from scripts.run_load_bronze import LoadResult, _ticker_from_path, run_load


def _mock_reader(blobs: list[str], blob_content: bytes = b"{}") -> MagicMock:
    reader = MagicMock(spec=BronzeReader)
    reader.list_blobs.return_value = blobs
    reader.read_blob.return_value = blob_content
    return reader


def _mock_writer(upsert_return: int = 1) -> MagicMock:
    writer = MagicMock(spec=PostgresWriter)
    writer.upsert_bars.return_value = upsert_return
    return writer


def test_ticker_from_daily_path() -> None:
    assert _ticker_from_path("2026/05/01/AAPL.json") == "AAPL"


def test_ticker_from_historical_path() -> None:
    assert _ticker_from_path("historical/MSFT.json") == "MSFT"


def test_run_load_all_succeed() -> None:
    from datetime import date
    from decimal import Decimal
    from unittest.mock import patch

    from market_signal_pipeline.ingest.models import DailyBar, DailySeries

    series = DailySeries(
        symbol="AAPL",
        last_refreshed=date(2026, 5, 1),
        bars=(
            DailyBar(
                date=date(2026, 5, 1),
                open=Decimal("278"),
                high=Decimal("280"),
                low=Decimal("275"),
                close=Decimal("279"),
                volume=1000,
            ),
        ),
    )

    reader = _mock_reader(["2026/05/01/AAPL.json", "2026/05/01/MSFT.json"])
    writer = _mock_writer()

    with patch(
        "scripts.run_load_bronze.parse_blob",
        return_value=(series, "alpha_vantage"),
    ):
        result = run_load(reader=reader, writer=writer)

    assert len(result.successes) == 2
    assert result.failures == {}
    assert result.any_failed is False


def test_run_load_partial_failure() -> None:
    from datetime import date
    from decimal import Decimal
    from unittest.mock import patch

    from market_signal_pipeline.ingest.models import DailyBar, DailySeries

    series = DailySeries(
        symbol="AAPL",
        last_refreshed=date(2026, 5, 1),
        bars=(
            DailyBar(
                date=date(2026, 5, 1),
                open=Decimal("278"),
                high=Decimal("280"),
                low=Decimal("275"),
                close=Decimal("279"),
                volume=1000,
            ),
        ),
    )

    reader = _mock_reader(["2026/05/01/AAPL.json", "2026/05/01/MSFT.json"])
    writer = _mock_writer()

    def _parse_side_effect(raw: bytes, ticker: str):  # type: ignore[no-untyped-def]
        if ticker == "MSFT":
            raise MalformedResponseError("bad shape")
        return series, "alpha_vantage"

    with patch("scripts.run_load_bronze.parse_blob", side_effect=_parse_side_effect):
        result = run_load(reader=reader, writer=writer)

    assert len(result.successes) == 1
    assert "2026/05/01/MSFT.json" in result.failures
    assert result.any_failed is True


def test_load_result_any_failed_property() -> None:
    assert LoadResult().any_failed is False
    assert LoadResult(successes=["a"]).any_failed is False
    assert LoadResult(failures={"a": "err"}).any_failed is True
