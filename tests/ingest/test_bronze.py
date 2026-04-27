"""Tests for the bronze layer writer."""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from azure.core.exceptions import ServiceRequestError

from market_signal_pipeline.ingest.bronze import BronzeWriteError, BronzeWriter


@pytest.fixture
def writer() -> BronzeWriter:
    with patch("market_signal_pipeline.ingest.bronze.ContainerClient"):
        return BronzeWriter(
            account_url="https://test.blob.core.windows.net",
            account_key="test-key",
            container_name="bronze-raw",
        )


def test_write_happy_path(writer: BronzeWriter) -> None:
    mock_blob_client = MagicMock()
    writer._container.get_blob_client = MagicMock(return_value=mock_blob_client)  # pyright: ignore[reportPrivateUsage]

    blob_path = writer.write(
        ticker="MSFT",
        series_date=date(2026, 4, 26),
        raw_bytes=b'{"Meta Data": {"2. Symbol": "MSFT"}}',
    )

    assert blob_path == "2026/04/26/MSFT.json"
    mock_blob_client.upload_blob.assert_called_once()
    call_kwargs = mock_blob_client.upload_blob.call_args.kwargs
    assert call_kwargs["overwrite"] is True


def test_write_zero_pads_single_digit_months_and_days(writer: BronzeWriter) -> None:
    writer._container.get_blob_client = MagicMock(return_value=MagicMock())  # pyright: ignore[reportPrivateUsage]

    blob_path = writer.write(
        ticker="AAPL",
        series_date=date(2026, 1, 5),
        raw_bytes=b"{}",
    )

    assert blob_path == "2026/01/05/AAPL.json"


def test_write_raises_on_azure_error(writer: BronzeWriter) -> None:
    mock_blob_client = MagicMock()
    mock_blob_client.upload_blob.side_effect = ServiceRequestError(message="connection failed")
    writer._container.get_blob_client = MagicMock(return_value=mock_blob_client)  # pyright: ignore[reportPrivateUsage]

    with pytest.raises(BronzeWriteError, match="Failed to write MSFT"):
        writer.write(
            ticker="MSFT",
            series_date=date(2026, 4, 26),
            raw_bytes=b"{}",
        )


def test_constructor_rejects_empty_key() -> None:
    with (
        patch("market_signal_pipeline.ingest.bronze.ContainerClient"),
        pytest.raises(ValueError, match="account_key must be a non-empty string"),
    ):
        BronzeWriter(
            account_url="https://test.blob.core.windows.net",
            account_key="",
            container_name="bronze-raw",
        )


def test_repr_does_not_leak_key() -> None:
    with patch("market_signal_pipeline.ingest.bronze.ContainerClient"):
        writer = BronzeWriter(
            account_url="https://test.blob.core.windows.net",
            account_key="super-secret-key",
            container_name="bronze-raw",
        )
        assert "super-secret-key" not in repr(writer)
        assert "bronze-raw" in repr(writer)
