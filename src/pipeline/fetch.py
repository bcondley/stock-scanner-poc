from __future__ import annotations

import io
import json
import logging
from typing import TYPE_CHECKING

import pandas as pd
import yfinance as yf

if TYPE_CHECKING:
    import redis

logger = logging.getLogger(__name__)


def fetch_ticker_data(
    ticker: str,
    start_date: str,
    end_date: str,
    redis_client: redis.Redis | None = None,
    cache_ttl: int = 3600,
) -> pd.DataFrame | None:
    """Fetch OHLCV data for a single ticker, using Redis cache if available."""
    cache_key = f"ohlcv:{ticker}:{start_date}:{end_date}"

    if redis_client is not None:
        cached = redis_client.get(cache_key)
        if cached is not None:
            logger.debug("Cache hit for %s", ticker)
            data = cached if isinstance(cached, str) else cached.decode()
            return pd.read_json(io.StringIO(data), orient="split")

    try:
        df = yf.download(ticker, start=start_date, end=end_date, progress=False)
        if df.empty:
            logger.warning("No data returned for %s", ticker)
            return None

        # Flatten multi-level columns if present
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        if redis_client is not None:
            redis_client.setex(cache_key, cache_ttl, df.to_json(orient="split", date_format="iso"))

        return df
    except Exception:
        logger.exception("Failed to fetch data for %s", ticker)
        return None


def fetch_chunk(
    tickers: list[str],
    start_date: str,
    end_date: str,
    redis_client: redis.Redis | None = None,
    cache_ttl: int = 3600,
) -> dict[str, pd.DataFrame]:
    """Fetch OHLCV data for a chunk of tickers."""
    results: dict[str, pd.DataFrame] = {}
    for ticker in tickers:
        df = fetch_ticker_data(ticker, start_date, end_date, redis_client, cache_ttl)
        if df is not None:
            results[ticker] = df
    return results
