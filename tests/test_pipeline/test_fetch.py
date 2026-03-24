from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from src.pipeline.fetch import fetch_ticker_data, fetch_chunk


class TestFetchTickerData:
    @patch("src.pipeline.fetch.yf.download")
    def test_successful_fetch(self, mock_download, sample_ohlcv):
        mock_download.return_value = sample_ohlcv
        result = fetch_ticker_data("AAPL", "2024-01-01", "2024-12-31")
        assert result is not None
        assert len(result) == len(sample_ohlcv)

    @patch("src.pipeline.fetch.yf.download")
    def test_empty_result(self, mock_download):
        mock_download.return_value = pd.DataFrame()
        result = fetch_ticker_data("INVALID", "2024-01-01", "2024-12-31")
        assert result is None

    @patch("src.pipeline.fetch.yf.download")
    def test_exception_returns_none(self, mock_download):
        mock_download.side_effect = Exception("network error")
        result = fetch_ticker_data("AAPL", "2024-01-01", "2024-12-31")
        assert result is None

    @patch("src.pipeline.fetch.yf.download")
    def test_redis_cache_hit(self, mock_download, sample_ohlcv):
        mock_redis = MagicMock()
        mock_redis.get.return_value = sample_ohlcv.to_json(orient="split", date_format="iso")
        result = fetch_ticker_data("AAPL", "2024-01-01", "2024-12-31", redis_client=mock_redis)
        assert result is not None
        mock_download.assert_not_called()

    @patch("src.pipeline.fetch.yf.download")
    def test_redis_cache_miss_stores(self, mock_download, sample_ohlcv):
        mock_download.return_value = sample_ohlcv
        mock_redis = MagicMock()
        mock_redis.get.return_value = None
        fetch_ticker_data("AAPL", "2024-01-01", "2024-12-31", redis_client=mock_redis, cache_ttl=600)
        mock_redis.setex.assert_called_once()
        args = mock_redis.setex.call_args
        assert args[0][0] == "ohlcv:AAPL:2024-01-01:2024-12-31"
        assert args[0][1] == 600


class TestFetchChunk:
    @patch("src.pipeline.fetch.yf.download")
    def test_fetch_multiple(self, mock_download, sample_ohlcv):
        mock_download.return_value = sample_ohlcv
        result = fetch_chunk(["AAPL", "MSFT", "GOOGL"], "2024-01-01", "2024-12-31")
        assert len(result) == 3
        assert "AAPL" in result

    @patch("src.pipeline.fetch.yf.download")
    def test_partial_failure(self, mock_download, sample_ohlcv):
        def side_effect(ticker, **kwargs):
            if ticker == "BAD":
                return pd.DataFrame()
            return sample_ohlcv
        mock_download.side_effect = side_effect
        result = fetch_chunk(["AAPL", "BAD", "MSFT"], "2024-01-01", "2024-12-31")
        assert len(result) == 2
        assert "BAD" not in result
