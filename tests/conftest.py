import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def sample_ohlcv() -> pd.DataFrame:
    """Generate a realistic OHLCV DataFrame for testing."""
    np.random.seed(42)
    n = 100
    dates = pd.bdate_range("2024-01-01", periods=n)
    close = 100 + np.cumsum(np.random.randn(n) * 2)
    high = close + np.abs(np.random.randn(n))
    low = close - np.abs(np.random.randn(n))
    open_ = close + np.random.randn(n) * 0.5
    volume = np.random.randint(1_000_000, 10_000_000, size=n)

    return pd.DataFrame(
        {"Open": open_, "High": high, "Low": low, "Close": close, "Volume": volume},
        index=dates,
    )


@pytest.fixture
def sample_tickers() -> list[str]:
    return ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "JPM", "V", "JNJ"]


@pytest.fixture
def small_ohlcv() -> pd.DataFrame:
    """Minimal OHLCV for edge-case testing."""
    dates = pd.bdate_range("2024-01-01", periods=5)
    return pd.DataFrame(
        {
            "Open": [100, 102, 101, 103, 104],
            "High": [103, 104, 103, 105, 106],
            "Low": [99, 100, 99, 101, 102],
            "Close": [102, 101, 103, 104, 105],
            "Volume": [1e6, 1.1e6, 9e5, 1.2e6, 1e6],
        },
        index=dates,
    )
