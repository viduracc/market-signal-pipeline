"""Bronze layer writer. Persists raw API responses to Azure Blob Storage."""

from datetime import date

import structlog
from azure.core.exceptions import AzureError
from azure.storage.blob import ContainerClient, ContentSettings

log = structlog.get_logger()


class BronzeWriteError(Exception):
    """Failure writing to bronze layer."""


class BronzeWriter:
    """Writes raw OHLCV API responses to Azure Blob Storage."""

    def __init__(
        self,
        account_url: str,
        account_key: str,
        container_name: str,
    ) -> None:
        if not account_key:
            raise ValueError("account_key must be a non-empty string")
        self._container_name = container_name
        self._container = ContainerClient(
            account_url=account_url,
            container_name=container_name,
            credential=account_key,
        )

    def __repr__(self) -> str:
        return f"BronzeWriter(container={self._container_name!r})"

    def write(self, ticker: str, series_date: date, raw_bytes: bytes) -> str:
        """Write raw bytes to bronze. Returns the blob path written."""
        blob_path = self._build_path(ticker, series_date)
        log.info("bronze.write.attempt", ticker=ticker, blob_path=blob_path)

        try:
            blob_client = self._container.get_blob_client(blob_path)
            blob_client.upload_blob(
                raw_bytes,
                overwrite=True,
                content_settings=ContentSettings(content_type="application/json"),
            )
        except AzureError as exc:
            raise BronzeWriteError(
                f"Failed to write {ticker} for {series_date.isoformat()}: {exc}"
            ) from exc

        log.info("bronze.write.success", ticker=ticker, blob_path=blob_path)
        return blob_path

    @staticmethod
    def _build_path(ticker: str, series_date: date) -> str:
        return f"{series_date.year:04d}/{series_date.month:02d}/{series_date.day:02d}/{ticker}.json"
