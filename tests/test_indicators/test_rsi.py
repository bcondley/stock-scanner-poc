import numpy as np
import pandas as pd
import pytest

from src.indicators.rsi import RSIIndicator


class TestRSI:
    def test_rsi_column_added(self, sample_ohlcv):
        indicator = RSIIndicator()
        result = indicator.compute(sample_ohlcv)
        assert "rsi" in result.columns

    def test_rsi_range(self, sample_ohlcv):
        indicator = RSIIndicator()
        result = indicator.compute(sample_ohlcv)
        rsi_valid = result["rsi"].dropna()
        assert (rsi_valid >= 0).all()
        assert (rsi_valid <= 100).all()

    def test_rsi_custom_period(self, sample_ohlcv):
        indicator = RSIIndicator(params={"period": 7})
        result = indicator.compute(sample_ohlcv)
        # Shorter period should produce values sooner
        first_valid = result["rsi"].first_valid_index()
        assert first_valid is not None

    def test_rsi_all_up(self):
        """Monotonically increasing prices should give RSI near 100."""
        dates = pd.bdate_range("2024-01-01", periods=30)
        df = pd.DataFrame(
            {"Open": range(30), "High": range(1, 31), "Low": range(30), "Close": range(30), "Volume": [1e6] * 30},
            index=dates,
        )
        result = RSIIndicator().compute(df)
        last_rsi = result["rsi"].iloc[-1]
        assert last_rsi > 90

    def test_rsi_all_down(self):
        """Monotonically decreasing prices should give RSI near 0."""
        dates = pd.bdate_range("2024-01-01", periods=30)
        prices = list(range(100, 70, -1))
        df = pd.DataFrame(
            {"Open": prices, "High": prices, "Low": prices, "Close": prices, "Volume": [1e6] * 30},
            index=dates,
        )
        result = RSIIndicator().compute(df)
        last_rsi = result["rsi"].iloc[-1]
        assert last_rsi < 10

    def test_does_not_mutate_input(self, sample_ohlcv):
        original_cols = set(sample_ohlcv.columns)
        RSIIndicator().compute(sample_ohlcv)
        assert set(sample_ohlcv.columns) == original_cols
