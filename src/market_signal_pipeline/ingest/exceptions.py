class IngestError(Exception):
    """Base class for all data ingestion failures."""


class RateLimitError(IngestError):
    """Rate limit hit. Retryable."""


class ServerError(IngestError):
    """Server-side failure. HTTP 5xx. Retryable."""


class ClientError(IngestError):
    """Client-side failure. HTTP 4xx (excluding 429). Not retryable."""


class MalformedResponseError(IngestError):
    """Response body did not match expected shape. Not retryable."""
