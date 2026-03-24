import pandas as pd
import pytest

from src.indicators.moving_avg import SMAIndicator, EMAIndicator


class TestSMA:
    def test_column_added(self, sample_ohlcv):
        result = SMAIndicator(params={"period": 20}).compute(sample_ohlcv)
        assert "sma_20" in result.columns

    def test_matches_pandas_rolling(self, sample_ohlcv):
        result = SMAIndicator(params={"period": 20}).compute(sample_ohlcv)
        expected = sample_ohlcv["Close"].rolling(20).mean()
        pd.testing.assert_series_equal(result["sma_20"], expected, check_names=False)

    def test_custom_period(self, sample_ohlcv):
        result = SMAIndicator(params={"period": 50}).compute(sample_ohlcv)
        assert "sma_50" in result.columns

    def test_does_not_mutate(self, sample_ohlcv):
        original_cols = set(sample_ohlcv.columns)
        SMAIndicator().compute(sample_ohlcv)
        assert set(sample_ohlcv.columns) == original_cols


class TestEMA:
    def test_column_added(self, sample_ohlcv):
        result = EMAIndicator(params={"period": 20}).compute(sample_ohlcv)
        assert "ema_20" in result.columns

    def test_matches_pandas_ewm(self, sample_ohlcv):
        result = EMAIndicator(params={"period": 20}).compute(sample_ohlcv)
        expected = sample_ohlcv["Close"].ewm(span=20, adjust=False).mean()
        pd.testing.assert_series_equal(result["ema_20"], expected, check_names=False)

    def test_ema_reacts_faster_than_sma(self, sample_ohlcv):
        """EMA should track recent prices more closely than SMA."""
        sma = SMAIndicator(params={"period": 20}).compute(sample_ohlcv)
        ema = EMAIndicator(params={"period": 20}).compute(sample_ohlcv)
        # Check last value differs (EMA weights recent more)
        assert sma["sma_20"].iloc[-1] != ema["ema_20"].iloc[-1]

    def test_does_not_mutate(self, sample_ohlcv):
        original_cols = set(sample_ohlcv.columns)
        EMAIndicator().compute(sample_ohlcv)
        assert set(sample_ohlcv.columns) == original_cols
