"""Postgres writer. Upserts parsed bars into the bronze_raw table."""

from datetime import UTC, datetime
from typing import Any

import psycopg
import structlog

from market_signal_pipeline.ingest.models import DailyBar

log = structlog.get_logger()

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS bronze_raw (
    ticker      VARCHAR(10)    NOT NULL,
    bar_date    DATE           NOT NULL,
    open        NUMERIC(18, 6) NOT NULL,
    high        NUMERIC(18, 6) NOT NULL,
    low         NUMERIC(18, 6) NOT NULL,
    close       NUMERIC(18, 6) NOT NULL,
    volume      BIGINT         NOT NULL,
    source      VARCHAR(20)    NOT NULL,
    loaded_at   TIMESTAMPTZ    NOT NULL,
    PRIMARY KEY (ticker, bar_date)
);
"""

_UPSERT = """
INSERT INTO bronze_raw (ticker, bar_date, open, high, low, close, volume, source, loaded_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (ticker, bar_date) DO UPDATE SET
    open      = EXCLUDED.open,
    high      = EXCLUDED.high,
    low       = EXCLUDED.low,
    close     = EXCLUDED.close,
    volume    = EXCLUDED.volume,
    source    = EXCLUDED.source,
    loaded_at = EXCLUDED.loaded_at;
"""


class PostgresWriteError(Exception):
    """Failure writing to Postgres."""


class PostgresWriter:
    """Upserts OHLCV bars into the bronze_raw table."""

    def __init__(
        self,
        host: str,
        port: int,
        dbname: str,
        user: str,
        password: str,
    ) -> None:
        if not host:
            raise ValueError("host must be a non-empty string")
        if not user:
            raise ValueError("user must be a non-empty string")
        if not password:
            raise ValueError("password must be a non-empty string")
        self._host = host
        self._dbname = dbname
        self._connect_kwargs: dict[str, Any] = {
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password": password,
            "sslmode": "require",
        }
        self._conn: psycopg.Connection[Any] | None = None

    def __repr__(self) -> str:
        return f"PostgresWriter(host={self._host!r}, dbname={self._dbname!r})"

    def __enter__(self) -> "PostgresWriter":
        self._conn = psycopg.connect(**self._connect_kwargs)
        return self

    def __exit__(self, *_: object) -> None:
        if self._conn:
            self._conn.close()

    def ensure_table(self) -> None:
        """Create bronze_raw if it does not exist."""
        if self._conn is None:
            raise PostgresWriteError("PostgresWriter must be used as a context manager")
        with self._conn.cursor() as cur:
            cur.execute(_CREATE_TABLE)
        self._conn.commit()
        log.info("postgres.ensure_table.done")

    def upsert_bars(self, ticker: str, bars: tuple[DailyBar, ...], source: str) -> int:
        """Upsert bars for a ticker. Returns number of rows affected."""
        if self._conn is None:
            raise PostgresWriteError("PostgresWriter must be used as a context manager")

        now = datetime.now(UTC)
        rows = [
            (
                ticker,
                bar.bar_date,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.volume,
                source,
                now,
            )
            for bar in bars
        ]

        try:
            with self._conn.cursor() as cur:
                cur.executemany(_UPSERT, rows)
            self._conn.commit()
        except psycopg.Error as exc:
            self._conn.rollback()
            raise PostgresWriteError(f"Upsert failed for {ticker}: {exc}") from exc

        log.info("postgres.upsert.success", ticker=ticker, rows=len(rows), source=source)
        return len(rows)
