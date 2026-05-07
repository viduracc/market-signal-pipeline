"""Tests for BronzeReader."""

from unittest.mock import MagicMock

import pytest
from azure.core.exceptions import AzureError

from market_signal_pipeline.load.bronze_reader import BronzeReader, BronzeReadError


def _make_reader() -> BronzeReader:
    return BronzeReader(
        account_url="https://stmspdev.blob.core.windows.net",
        account_key="dummykey==",
        container_name="bronze-raw",
    )


def test_repr_does_not_leak_credentials() -> None:
    reader = _make_reader()
    assert "dummykey" not in repr(reader)
    assert "bronze-raw" in repr(reader)


def test_empty_account_key_raises() -> None:
    with pytest.raises(ValueError, match="account_key"):
        BronzeReader(
            account_url="https://stmspdev.blob.core.windows.net",
            account_key="",
            container_name="bronze-raw",
        )


def test_list_blobs_returns_json_names() -> None:
    reader = _make_reader()

    blob_a = MagicMock()
    blob_a.name = "2026/05/01/AAPL.json"
    blob_b = MagicMock()
    blob_b.name = "historical/MSFT.json"
    blob_c = MagicMock()
    blob_c.name = "2026/05/01/AAPL.parquet"

    reader._container.list_blobs = MagicMock(  # pyright: ignore[reportPrivateUsage]
        return_value=[blob_a, blob_b, blob_c]
    )

    result = reader.list_blobs()
    assert result == ["2026/05/01/AAPL.json", "historical/MSFT.json"]


def test_list_blobs_azure_error_raises_read_error() -> None:
    reader = _make_reader()
    reader._container.list_blobs = MagicMock(  # pyright: ignore[reportPrivateUsage]
        side_effect=AzureError("connection failed")
    )

    with pytest.raises(BronzeReadError):
        reader.list_blobs()


def test_read_blob_returns_bytes() -> None:
    reader = _make_reader()

    mock_blob_client = MagicMock()
    mock_blob_client.download_blob.return_value.readall.return_value = b'{"data": 1}'
    reader._container.get_blob_client = MagicMock(  # pyright: ignore[reportPrivateUsage]
        return_value=mock_blob_client
    )

    result = reader.read_blob("2026/05/01/AAPL.json")
    assert result == b'{"data": 1}'


def test_read_blob_azure_error_raises_read_error() -> None:
    reader = _make_reader()

    mock_blob_client = MagicMock()
    mock_blob_client.download_blob.side_effect = AzureError("not found")
    reader._container.get_blob_client = MagicMock(  # pyright: ignore[reportPrivateUsage]
        return_value=mock_blob_client
    )

    with pytest.raises(BronzeReadError):
        reader.read_blob("2026/05/01/AAPL.json")
