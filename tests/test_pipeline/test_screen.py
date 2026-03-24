import numpy as np
import pandas as pd
import pytest

from src.indicators.rsi import RSIIndicator
from src.indicators.macd import MACDIndicator
from src.indicators.bollinger import BollingerIndicator
from src.indicators.moving_avg import SMAIndicator
from src.pipeline.screen import apply_filters, rank_results, _check_macd_cross


@pytest.fixture
def enriched_data(sample_ohlcv):
    """OHLCV data with all indicators computed."""
    df = sample_ohlcv.copy()
    df = RSIIndicator(params={"period": 14}).compute(df)
    df = MACDIndicator().compute(df)
    df = BollingerIndicator().compute(df)
    df = SMAIndicator(params={"period": 20}).compute(df)
    return {"AAPL": df}


class TestApplyFilters:
    def test_no_filters(self, enriched_data):
        result = apply_filters(enriched_data, {})
        assert len(result) == 1

    def test_rsi_below_filter(self, enriched_data):
        # Set RSI threshold that should pass (high threshold)
        result = apply_filters(enriched_data, {"rsi_below": 100})
        assert "AAPL" in result

    def test_rsi_filter_excludes(self, enriched_data):
        # Set RSI threshold very low — should exclude
        result = apply_filters(enriched_data, {"rsi_below": 0})
        assert "AAPL" not in result

    def test_unknown_filter_skipped(self, enriched_data):
        result = apply_filters(enriched_data, {"nonexistent_filter": 42})
        assert len(result) == 1

    def test_missing_column_excludes(self):
        """Filter referencing missing column should exclude ticker."""
        df = pd.DataFrame({"Close": [100]}, index=[pd.Timestamp("2024-01-01")])
        result = apply_filters({"AAPL": df}, {"rsi_below": 50})
        assert "AAPL" not in result


class TestMACDCross:
    def test_bullish_cross(self):
        df = pd.DataFrame({"macd_hist": [-0.5, -0.2, 0.1]})
        assert _check_macd_cross(df, "bullish")

    def test_bearish_cross(self):
        df = pd.DataFrame({"macd_hist": [0.5, 0.2, -0.1]})
        assert _check_macd_cross(df, "bearish")

    def test_no_cross(self):
        df = pd.DataFrame({"macd_hist": [0.5, 0.6, 0.7]})
        assert not _check_macd_cross(df, "bullish")

    def test_too_short(self):
        df = pd.DataFrame({"macd_hist": [0.5]})
        assert not _check_macd_cross(df, "bullish")


class TestRankResults:
    def test_ranks_by_rsi(self, enriched_data):
        # Create two tickers with different RSI
        data = {}
        for ticker, rsi_override in [("LOW", 25), ("HIGH", 75)]:
            df = enriched_data["AAPL"].copy()
            df.loc[df.index[-1], "rsi"] = rsi_override
            data[ticker] = df

        ranked = rank_results(data, rank_by="rsi", ascending=True)
        assert ranked[0]["ticker"] == "LOW"
        assert ranked[1]["ticker"] == "HIGH"

    def test_result_contains_close(self, enriched_data):
        ranked = rank_results(enriched_data)
        assert "close" in ranked[0]
        assert "ticker" in ranked[0]
