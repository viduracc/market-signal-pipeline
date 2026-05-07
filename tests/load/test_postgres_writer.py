"""Tests for PostgresWriter."""

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from market_signal_pipeline.ingest.models import DailyBar
from market_signal_pipeline.load.postgres_writer import PostgresWriteError, PostgresWriter


def _make_bars() -> tuple[DailyBar, ...]:
    return (
        DailyBar(
            date=date(2026, 5, 1),
            open=Decimal("278.855"),
            high=Decimal("287.22"),
            low=Decimal("278.37"),
            close=Decimal("280.14"),
            volume=77938859,
        ),
    )


def _make_writer() -> PostgresWriter:
    return PostgresWriter(
        host="pg-msp-dev.postgres.database.azure.com",
        port=5432,
        dbname="market_signals",
        user="pgadmin",
        password="s3cr3t!",
    )


def test_repr_does_not_leak_credentials() -> None:
    writer = _make_writer()
    r = repr(writer)
    assert "s3cr3t" not in r
    assert "pgadmin" not in r
    assert "pg-msp-dev" in r
    assert "market_signals" in r


def test_empty_host_raises() -> None:
    with pytest.raises(ValueError, match="host"):
        PostgresWriter(host="", port=5432, dbname="market_signals", user="pgadmin", password="x")


def test_empty_user_raises() -> None:
    with pytest.raises(ValueError, match="user"):
        PostgresWriter(host="host", port=5432, dbname="market_signals", user="", password="x")


def test_empty_password_raises() -> None:
    with pytest.raises(ValueError, match="password"):
        PostgresWriter(host="host", port=5432, dbname="market_signals", user="pgadmin", password="")


def test_ensure_table_without_context_manager_raises() -> None:
    writer = _make_writer()
    with pytest.raises(PostgresWriteError):
        writer.ensure_table()


def test_upsert_without_context_manager_raises() -> None:
    writer = _make_writer()
    with pytest.raises(PostgresWriteError):
        writer.upsert_bars("AAPL", _make_bars(), "alpha_vantage")


def test_upsert_bars_returns_row_count() -> None:
    writer = _make_writer()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch("psycopg.connect", return_value=mock_conn), writer:
        count = writer.upsert_bars("AAPL", _make_bars(), "alpha_vantage")

    assert count == 1
    mock_conn.commit.assert_called()
