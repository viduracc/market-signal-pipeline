"""Entrypoint for bronze loader. Reads blobs, parses, upserts to Postgres."""

import sys
from dataclasses import dataclass, field

import structlog

from market_signal_pipeline.config import get_settings
from market_signal_pipeline.ingest.exceptions import MalformedResponseError
from market_signal_pipeline.load.bronze_reader import BronzeReader, BronzeReadError
from market_signal_pipeline.load.parser import parse_blob
from market_signal_pipeline.load.postgres_writer import PostgresWriteError, PostgresWriter

log = structlog.get_logger()


@dataclass(frozen=True)
class LoadResult:
    successes: list[str] = field(default_factory=lambda: [])
    failures: dict[str, str] = field(default_factory=lambda: {})

    @property
    def any_failed(self) -> bool:
        return len(self.failures) > 0


def ticker_from_path(blob_path: str) -> str:
    return blob_path.rsplit("/", 1)[-1].removesuffix(".json")


def run_load(
    reader: BronzeReader,
    writer: PostgresWriter,
) -> LoadResult:
    """Read all bronze blobs and upsert to Postgres. Best-effort per blob."""
    result = LoadResult()

    blob_paths = reader.list_blobs()

    for blob_path in blob_paths:
        ticker = ticker_from_path(blob_path)
        try:
            raw = reader.read_blob(blob_path)
            series, source = parse_blob(raw, ticker)
            rows = writer.upsert_bars(ticker, series.bars, source)
            result.successes.append(blob_path)
            log.info("load.blob.success", blob_path=blob_path, ticker=ticker, rows=rows)
        except (BronzeReadError, MalformedResponseError, PostgresWriteError, Exception) as exc:
            result.failures[blob_path] = f"{type(exc).__name__}: {exc}"
            log.error("load.blob.failure", blob_path=blob_path, ticker=ticker, error=str(exc))

    return result


def main() -> int:
    settings = get_settings()
    account_url = f"https://{settings.azure_storage_account}.blob.core.windows.net"

    log.info("load.run.start")

    reader = BronzeReader(
        account_url=account_url,
        account_key=settings.azure_storage_account_key,
        container_name=settings.azure_storage_container,
    )

    with PostgresWriter(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
    ) as writer:
        writer.ensure_table()
        result = run_load(reader=reader, writer=writer)

    log.info(
        "load.run.complete",
        successes=len(result.successes),
        failures=result.failures,
    )

    return 1 if result.any_failed else 0


if __name__ == "__main__":
    sys.exit(main())
