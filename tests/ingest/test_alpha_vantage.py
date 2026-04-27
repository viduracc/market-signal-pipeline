"""Tests for the Alpha Vantage HTTP client."""

from decimal import Decimal
from typing import Any

import httpx
import pytest
import respx

from market_signal_pipeline.ingest.alpha_vantage import AlphaVantageClient
from market_signal_pipeline.ingest.exceptions import (
    ClientError,
    MalformedResponseError,
    ServerError,
)

VALID_PAYLOAD: dict[str, Any] = {
    "Meta Data": {
        "1. Information": "Daily Prices",
        "2. Symbol": "MSFT",
        "3. Last Refreshed": "2026-04-24",
        "4. Output Size": "Compact",
        "5. Time Zone": "US/Eastern",
    },
    "Time Series (Daily)": {
        "2026-04-24": {
            "1. open": "412.50",
            "2. high": "415.30",
            "3. low": "410.10",
            "4. close": "414.20",
            "5. volume": "18234500",
        },
        "2026-04-23": {
            "1. open": "408.00",
            "2. high": "413.00",
            "3. low": "407.50",
            "4. close": "412.00",
            "5. volume": "20000000",
        },
    },
}


@pytest.fixture
def client() -> AlphaVantageClient:
    return AlphaVantageClient(api_key="test-key", base_url="https://test.example")


@respx.mock
def test_fetch_daily_happy_path(client: AlphaVantageClient) -> None:
    respx.get("https://test.example/query").mock(
        return_value=httpx.Response(200, json=VALID_PAYLOAD)
    )

    series, raw = client.fetch_daily("MSFT")

    assert series.symbol == "MSFT"
    assert len(series.bars) == 2
    assert series.bars[0].bar_date.isoformat() == "2026-04-24"
    assert series.bars[0].open == Decimal("412.50")
    assert series.bars[0].volume == 18234500
    assert isinstance(raw, bytes)
    assert b"Meta Data" in raw


@respx.mock
def test_fetch_daily_retries_on_rate_limit(client: AlphaVantageClient) -> None:
    route = respx.get("https://test.example/query").mock(
        side_effect=[
            httpx.Response(429),
            httpx.Response(200, json=VALID_PAYLOAD),
        ]
    )

    series, _ = client.fetch_daily("MSFT")

    assert route.call_count == 2
    assert series.symbol == "MSFT"


@respx.mock
def test_fetch_daily_retries_on_soft_rate_limit(client: AlphaVantageClient) -> None:
    soft_limit_payload = {"Note": "Thank you for using Alpha Vantage..."}
    route = respx.get("https://test.example/query").mock(
        side_effect=[
            httpx.Response(200, json=soft_limit_payload),
            httpx.Response(200, json=VALID_PAYLOAD),
        ]
    )

    series, _ = client.fetch_daily("MSFT")

    assert route.call_count == 2
    assert series.symbol == "MSFT"


@respx.mock
def test_fetch_daily_raises_on_4xx_without_retry(client: AlphaVantageClient) -> None:
    route = respx.get("https://test.example/query").mock(
        return_value=httpx.Response(401, json={"error": "invalid api key"})
    )

    with pytest.raises(ClientError):
        client.fetch_daily("MSFT")

    assert route.call_count == 1


@respx.mock
def test_fetch_daily_raises_on_malformed_json(client: AlphaVantageClient) -> None:
    respx.get("https://test.example/query").mock(
        return_value=httpx.Response(200, text="not valid json")
    )

    with pytest.raises(MalformedResponseError):
        client.fetch_daily("MSFT")


@respx.mock
def test_fetch_daily_raises_after_retries_exhausted(client: AlphaVantageClient) -> None:
    route = respx.get("https://test.example/query").mock(return_value=httpx.Response(500))

    with pytest.raises(ServerError):
        client.fetch_daily("MSFT")

    assert route.call_count == 3


def test_constructor_rejects_empty_api_key() -> None:
    with pytest.raises(ValueError, match="api_key must be a non-empty string"):
        AlphaVantageClient(api_key="")


def test_repr_does_not_leak_api_key() -> None:
    client = AlphaVantageClient(api_key="super-secret-key", base_url="https://test.example")
    assert "super-secret-key" not in repr(client)
    assert "https://test.example" in repr(client)
