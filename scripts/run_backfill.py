"""Entrypoint for one-time historical backfill. Run via workflow_dispatch."""

import sys
from dataclasses import dataclass, field

import structlog

from market_signal_pipeline.config import get_settings
from market_signal_pipeline.ingest.bronze import BronzeWriter
from market_signal_pipeline.ingest.yahoo_finance import YahooFinanceClient

log = structlog.get_logger()

TICKERS: tuple[str, ...] = ("MSFT", "AAPL", "GOOGL", "AMZN", "NVDA")


@dataclass(frozen=True)
class BackfillResult:
    successes: list[str] = field(default_factory=lambda: [])
    failures: dict[str, str] = field(default_factory=lambda: {})

    @property
    def any_failed(self) -> bool:
        return len(self.failures) > 0


def run_backfill(
    client: YahooFinanceClient,
    writer: BronzeWriter,
    tickers: tuple[str, ...],
) -> BackfillResult:
    """Fetch full history for each ticker and write to bronze. Best-effort per ticker."""
    result = BackfillResult()

    for ticker in tickers:
        try:
            _, raw = client.fetch_daily(ticker, period="max")
            blob_path = writer.write_historical(ticker=ticker, raw_bytes=raw)
            result.successes.append(ticker)
            log.info("backfill.ticker.success", ticker=ticker, blob_path=blob_path, bytes=len(raw))
        except Exception as exc:
            result.failures[ticker] = f"{type(exc).__name__}: {exc}"
            log.error("backfill.ticker.failure", ticker=ticker, error=str(exc))

    return result


def main() -> int:
    settings = get_settings()
    account_url = f"https://{settings.azure_storage_account}.blob.core.windows.net"

    log.info("backfill.run.start", tickers=list(TICKERS))

    client = YahooFinanceClient()
    writer = BronzeWriter(
        account_url=account_url,
        account_key=settings.azure_storage_account_key,
        container_name=settings.azure_storage_container,
    )
    result = run_backfill(client=client, writer=writer, tickers=TICKERS)

    log.info(
        "backfill.run.complete",
        successes=result.successes,
        failures=result.failures,
    )

    return 1 if result.any_failed else 0


if __name__ == "__main__":
    sys.exit(main())
