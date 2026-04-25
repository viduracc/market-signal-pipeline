"""HTTP client for the Alpha Vantage market data API."""

from datetime import date
from decimal import Decimal
from typing import Any

import httpx
import structlog
from tenacity import (
    RetryError,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from market_signal_pipeline.ingest.exceptions import (
    AlphaVantageError,
    ClientError,
    MalformedResponseError,
    RateLimitError,
    ServerError,
)
from market_signal_pipeline.ingest.models import DailyBar, DailySeries

log = structlog.get_logger()

DEFAULT_BASE_URL = "https://www.alphavantage.co"
DEFAULT_TIMEOUT_SECONDS = 30.0
MAX_RETRY_ATTEMPTS = 3


class AlphaVantageClient:
    """Sync HTTP client for the Alpha Vantage daily time series endpoint."""

    def __init__(
        self,
        api_key: str,
        base_url: str = DEFAULT_BASE_URL,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        if not api_key:
            raise ValueError("api_key must be a non-empty string")
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._client = httpx.Client(timeout=timeout_seconds)

    def __repr__(self) -> str:
        return f"AlphaVantageClient(base_url={self._base_url!r})"

    def fetch_daily(self, ticker: str) -> DailySeries:
        """Fetch daily OHLCV bars for a ticker. Returns a clean DailySeries."""
        try:
            result: DailySeries = self._fetch_daily_with_retry(ticker)
            return result
        except RetryError as exc:
            last_exception = exc.last_attempt.exception()
            if last_exception is None:
                raise AlphaVantageError("Retry failed with no underlying exception") from exc
            raise last_exception from exc

    @retry(
        stop=stop_after_attempt(MAX_RETRY_ATTEMPTS),
        wait=wait_exponential_jitter(initial=1, max=8),
        retry=retry_if_exception_type(
            (RateLimitError, ServerError, httpx.ConnectError, httpx.ReadTimeout)
        ),
        reraise=False,
    )
    def _fetch_daily_with_retry(self, ticker: str) -> DailySeries:
        log.info("alpha_vantage.fetch_daily.attempt", ticker=ticker)
        response = self._client.get(
            f"{self._base_url}/query",
            params={
                "function": "TIME_SERIES_DAILY",
                "symbol": ticker,
                "outputsize": "compact",
                "apikey": self._api_key,
            },
        )
        return self._handle_response(response, ticker)

    def _handle_response(self, response: httpx.Response, ticker: str) -> DailySeries:
        if response.status_code == 429:
            raise RateLimitError(f"Rate limited (HTTP 429) for {ticker}")
        if 500 <= response.status_code < 600:
            raise ServerError(f"Server error (HTTP {response.status_code}) for {ticker}")
        if 400 <= response.status_code < 500:
            raise ClientError(f"Client error (HTTP {response.status_code}) for {ticker}")

        try:
            payload: dict[str, Any] = response.json()
        except ValueError as exc:
            raise MalformedResponseError(f"Response was not valid JSON for {ticker}") from exc

        if "Note" in payload or "Information" in payload:
            raise RateLimitError(f"Soft rate limit response for {ticker}")

        return self._parse_payload(payload, ticker)

    @staticmethod
    def _parse_payload(payload: dict[str, Any], ticker: str) -> DailySeries:
        try:
            meta = payload["Meta Data"]
            time_series = payload["Time Series (Daily)"]
            symbol = meta["2. Symbol"]
            last_refreshed_str = meta["3. Last Refreshed"]
        except KeyError as exc:
            raise MalformedResponseError(
                f"Missing expected key in response for {ticker}: {exc}"
            ) from exc

        bars: list[DailyBar] = []
        for date_str, values in time_series.items():
            try:
                bars.append(
                    DailyBar(
                        date=date.fromisoformat(date_str),
                        open=Decimal(values["1. open"]),
                        high=Decimal(values["2. high"]),
                        low=Decimal(values["3. low"]),
                        close=Decimal(values["4. close"]),
                        volume=int(values["5. volume"]),
                    )
                )
            except (KeyError, ValueError) as exc:
                raise MalformedResponseError(
                    f"Malformed bar for {ticker} at {date_str}: {exc}"
                ) from exc

        bars.sort(key=lambda b: b.bar_date, reverse=True)

        return DailySeries(
            symbol=symbol,
            last_refreshed=date.fromisoformat(last_refreshed_str),
            bars=tuple(bars),
        )

    def __enter__(self) -> "AlphaVantageClient":
        return self

    def __exit__(self, *_: object) -> None:
        self._client.close()
