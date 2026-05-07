"""Tests for bronze blob parser."""

import json
from datetime import date
from decimal import Decimal

import pytest

from market_signal_pipeline.ingest.exceptions import MalformedResponseError
from market_signal_pipeline.load.parser import parse_blob

_AV_PAYLOAD = {
    "Meta Data": {
        "1. Information": "Daily Prices",
        "2. Symbol": "AAPL",
        "3. Last Refreshed": "2026-05-01",
        "4. Output Size": "Compact",
        "5. Time Zone": "US/Eastern",
    },
    "Time Series (Daily)": {
        "2026-05-01": {
            "1. open": "278.8550",
            "2. high": "287.2200",
            "3. low": "278.3700",
            "4. close": "280.1400",
            "5. volume": "77938859",
        },
    },
}

_YF_PAYLOAD = {
    "source": "yahoo_finance",
    "ticker": "AAPL",
    "period": "max",
    "last_refreshed": "2026-05-01",
    "bars": [
        {
            "date": "2026-05-01",
            "open": "278.855",
            "high": "287.22",
            "low": "278.37",
            "close": "280.14",
            "volume": 77938859,
        }
    ],
}


def test_parse_alpha_vantage_shape() -> None:
    series, source = parse_blob(json.dumps(_AV_PAYLOAD).encode(), "AAPL")
    assert source == "alpha_vantage"
    assert series.symbol == "AAPL"
    assert series.last_refreshed == date(2026, 5, 1)
    assert len(series.bars) == 1
    assert series.bars[0].bar_date == date(2026, 5, 1)
    assert series.bars[0].close == Decimal("280.1400")


def test_parse_yahoo_finance_shape() -> None:
    series, source = parse_blob(json.dumps(_YF_PAYLOAD).encode(), "AAPL")
    assert source == "yahoo_finance"
    assert series.last_refreshed == date(2026, 5, 1)
    assert len(series.bars) == 1
    assert series.bars[0].volume == 77938859


def test_parse_invalid_json_raises() -> None:
    with pytest.raises(MalformedResponseError):
        parse_blob(b"not json", "AAPL")


def test_parse_unknown_shape_raises() -> None:
    payload = json.dumps({"unknown": "shape"}).encode()
    with pytest.raises(MalformedResponseError):
        parse_blob(payload, "AAPL")


def test_parse_alpha_vantage_missing_key_raises() -> None:
    broken = {**_AV_PAYLOAD, "Meta Data": {}}
    with pytest.raises(MalformedResponseError):
        parse_blob(json.dumps(broken).encode(), "AAPL")


def test_parse_yahoo_finance_missing_key_raises() -> None:
    broken = {**_YF_PAYLOAD}
    del broken["last_refreshed"]
    with pytest.raises(MalformedResponseError):
        parse_blob(json.dumps(broken).encode(), "AAPL")
