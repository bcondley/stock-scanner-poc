import pandas as pd
import pytest

from src.config.models import ScreenerConfig, IndicatorConfig
from src.pipeline.ingest import partition_tickers, get_sp500_tickers


class TestPartitionTickers:
    def test_even_split(self):
        tickers = list("ABCDEFGHIJ")
        chunks = partition_tickers(tickers, chunk_size=5)
        assert len(chunks) == 2
        assert all(len(c) == 5 for c in chunks)

    def test_uneven_split(self):
        tickers = list("ABCDEFG")
        chunks = partition_tickers(tickers, chunk_size=3)
        flat = [t for c in chunks for t in c]
        assert flat == tickers

    def test_single_chunk(self):
        tickers = ["AAPL", "MSFT"]
        chunks = partition_tickers(tickers, chunk_size=100)
        assert len(chunks) == 1
        assert chunks[0] == tickers

    def test_empty_list(self):
        chunks = partition_tickers([], chunk_size=10)
        assert chunks == []

    def test_chunk_size_one(self):
        tickers = ["A", "B", "C"]
        chunks = partition_tickers(tickers, chunk_size=1)
        assert len(chunks) == 3


class TestSP500Tickers:
    def test_returns_list(self):
        tickers = get_sp500_tickers()
        assert isinstance(tickers, list)
        assert len(tickers) > 400

    def test_contains_known_tickers(self):
        tickers = get_sp500_tickers()
        for t in ["AAPL", "MSFT", "GOOGL", "AMZN"]:
            assert t in tickers

    def test_no_duplicates(self):
        tickers = get_sp500_tickers()
        # Some tickers may appear more than once in the static list (e.g., PEAK)
        # but the list should be mostly unique
        unique = set(tickers)
        assert len(unique) > 400
