"""Entrypoint for scheduled bronze ingestion. Run via cron or manually."""

import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime

import structlog

from market_signal_pipeline.config import get_settings
from market_signal_pipeline.ingest.alpha_vantage import AlphaVantageClient
from market_signal_pipeline.ingest.bronze import BronzeWriter

log = structlog.get_logger()

TICKERS: tuple[str, ...] = ("MSFT", "AAPL", "GOOGL", "AMZN", "NVDA")


@dataclass(frozen=True)
class IngestionResult:
    successes: list[str] = field(default_factory=lambda: [])
    failures: dict[str, str] = field(default_factory=lambda: {})

    @property
    def any_failed(self) -> bool:
        return len(self.failures) > 0


def run_ingestion(
    client: AlphaVantageClient,
    writer: BronzeWriter,
    tickers: tuple[str, ...],
    ingest_date: "datetime",
) -> IngestionResult:
    """Fetch each ticker and write to bronze. Best-effort per ticker."""
    result = IngestionResult()

    for ticker in tickers:
        try:
            _, raw = client.fetch_daily(ticker)
            blob_path = writer.write(
                ticker=ticker,
                series_date=ingest_date.date(),
                raw_bytes=raw,
            )
            result.successes.append(ticker)
            log.info("ingestion.ticker.success", ticker=ticker, blob_path=blob_path)
        except Exception as exc:
            result.failures[ticker] = f"{type(exc).__name__}: {exc}"
            log.error("ingestion.ticker.failure", ticker=ticker, error=str(exc))

    return result


def main() -> int:
    settings = get_settings()
    account_url = f"https://{settings.azure_storage_account}.blob.core.windows.net"

    log.info("ingestion.run.start", tickers=list(TICKERS))

    with AlphaVantageClient(api_key=settings.alpha_vantage_api_key) as client:
        writer = BronzeWriter(
            account_url=account_url,
            account_key=settings.azure_storage_account_key,
            container_name=settings.azure_storage_container,
        )
        result = run_ingestion(
            client=client,
            writer=writer,
            tickers=TICKERS,
            ingest_date=datetime.now(UTC),
        )

    log.info(
        "ingestion.run.complete",
        successes=result.successes,
        failures=result.failures,
    )

    return 1 if result.any_failed else 0


if __name__ == "__main__":
    sys.exit(main())
