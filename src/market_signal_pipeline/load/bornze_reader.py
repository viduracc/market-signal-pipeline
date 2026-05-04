"""Bronze layer reader. Fetches raw blobs from Azure Blob Storage."""

import structlog
from azure.core.exceptions import AzureError
from azure.storage.blob import ContainerClient

log = structlog.getLogger()


class BronzeReadError(Exception):
    """Failure reading from bronze layer."""


class BronzeReader:
    """Reads raw blob from the bronze container."""

    def __init__(
        self,
        account_url: str,
        account_key: str,
        container_name: str,
    ) -> None:
        if not account_key:
            raise ValueError("account_key must be a none-empty string")
        self._container_name = container_name
        self._container = ContainerClient(
            account_url=account_url, container_name=container_name, credential=account_key
        )

    def __repr__(self) -> str:
        return f"BronzeReader(container={self._container_name!r})"

    def list_blobs(self, prefix: str = "") -> list[str]:
        """Return blob names under the given prefix."""
        try:
            return [
                blob.name
                for blob in self._container.list_blobs(name_starts_with=prefix or None)
                if blob.name and blob.name.endswith(".json")
            ]
        except AzureError as exc:
            raise BronzeReadError(f"Failed to list blobs under {prefix!r}: {exc}") from exc

    def read_blob(self, blob_path: str) -> bytes:
        """Download and return raw bytes for a blob."""
        log.info("bronze.read.attempt", blob_path=blob_path)
        try:
            blob_client = self._container.get_blob_client(blob_path)
            data: bytes = blob_client.download_blob().readall()
        except AzureError as exc:
            raise BronzeReadError(f"Failed to read blob {blob_path!r}: {exc}") from exc
        log.info("bronze.read.success", blob_path=blob_path, bytes=len(data))
        return data
