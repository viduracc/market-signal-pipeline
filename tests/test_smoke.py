"""Smoke test to ensure the package imports and CI runs."""

from market_signal_pipeline import __version__


def test_version() -> None:
    assert __version__ == "0.1.0"
