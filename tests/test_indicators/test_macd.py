import pandas as pd
import pytest

from src.indicators.macd import MACDIndicator


class TestMACD:
    def test_columns_added(self, sample_ohlcv):
        result = MACDIndicator().compute(sample_ohlcv)
        assert "macd" in result.columns
        assert "macd_signal" in result.columns
        assert "macd_hist" in result.columns

    def test_histogram_is_diff(self, sample_ohlcv):
        result = MACDIndicator().compute(sample_ohlcv)
        diff = result["macd"] - result["macd_signal"]
        pd.testing.assert_series_equal(result["macd_hist"], diff, check_names=False)

    def test_custom_params(self, sample_ohlcv):
        indicator = MACDIndicator(params={"fast": 8, "slow": 21, "signal": 5})
        result = indicator.compute(sample_ohlcv)
        assert "macd" in result.columns

    def test_does_not_mutate_input(self, sample_ohlcv):
        original_cols = set(sample_ohlcv.columns)
        MACDIndicator().compute(sample_ohlcv)
        assert set(sample_ohlcv.columns) == original_cols

    def test_flat_prices(self):
        """Flat prices should yield MACD near zero."""
        dates = pd.bdate_range("2024-01-01", periods=50)
        df = pd.DataFrame(
            {"Open": [100] * 50, "High": [100] * 50, "Low": [100] * 50, "Close": [100] * 50, "Volume": [1e6] * 50},
            index=dates,
        )
        result = MACDIndicator().compute(df)
        assert abs(result["macd"].iloc[-1]) < 0.01
