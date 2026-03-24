import pandas as pd
import pytest

from src.indicators.bollinger import BollingerIndicator


class TestBollinger:
    def test_columns_added(self, sample_ohlcv):
        result = BollingerIndicator().compute(sample_ohlcv)
        assert "bb_mid" in result.columns
        assert "bb_upper" in result.columns
        assert "bb_lower" in result.columns
        assert "bb_width" in result.columns

    def test_upper_above_lower(self, sample_ohlcv):
        result = BollingerIndicator().compute(sample_ohlcv)
        valid = result.dropna(subset=["bb_upper", "bb_lower"])
        assert (valid["bb_upper"] >= valid["bb_lower"]).all()

    def test_mid_is_sma(self, sample_ohlcv):
        result = BollingerIndicator(params={"period": 20}).compute(sample_ohlcv)
        expected_sma = sample_ohlcv["Close"].rolling(20).mean()
        pd.testing.assert_series_equal(result["bb_mid"], expected_sma, check_names=False)

    def test_width_positive(self, sample_ohlcv):
        result = BollingerIndicator().compute(sample_ohlcv)
        valid = result["bb_width"].dropna()
        assert (valid >= 0).all()

    def test_custom_std(self, sample_ohlcv):
        narrow = BollingerIndicator(params={"num_std": 1.0}).compute(sample_ohlcv)
        wide = BollingerIndicator(params={"num_std": 3.0}).compute(sample_ohlcv)
        # Wider bands should have greater width
        n_valid = narrow["bb_width"].dropna()
        w_valid = wide["bb_width"].dropna()
        assert (w_valid.values > n_valid.values).all()

    def test_does_not_mutate_input(self, sample_ohlcv):
        original_cols = set(sample_ohlcv.columns)
        BollingerIndicator().compute(sample_ohlcv)
        assert set(sample_ohlcv.columns) == original_cols
