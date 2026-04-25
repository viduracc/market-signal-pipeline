"""Exception types raised by the Alpha Vantage client."""


class AlphaVantageError(Exception):
    """Base class for all Alpha Vantage client failures."""


class RateLimitError(AlphaVantageError):
    """Rate limit hit. HTTP 429 or a soft-rate-limit response body. Retryable."""


class ServerError(AlphaVantageError):
    """Server-side failure. HTTP 5xx. Retryable."""


class ClientError(AlphaVantageError):
    """Client-side failure. HTTP 4xx (excluding 429). Not retryable."""


class MalformedResponseError(AlphaVantageError):
    """Response body did not match expected shape. Not retryable."""
