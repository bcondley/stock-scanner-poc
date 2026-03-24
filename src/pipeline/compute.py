from __future__ import annotations

import logging

import pandas as pd

from src.config.models import IndicatorConfig
from src.indicators.base import get_indicator

logger = logging.getLogger(__name__)


def compute_indicators(
    ticker: str,
    df: pd.DataFrame,
    indicator_configs: list[IndicatorConfig],
) -> pd.DataFrame:
    """Apply all configured indicators to a single ticker's OHLCV data."""
    for cfg in indicator_configs:
        indicator_cls = get_indicator(cfg.name)
        indicator = indicator_cls(params=cfg.params)
        try:
            df = indicator.compute(df)
        except Exception:
            logger.exception("Indicator %s failed for %s", cfg.name, ticker)
    return df


def compute_chunk(
    chunk: dict[str, pd.DataFrame],
    indicator_configs: list[IndicatorConfig],
) -> dict[str, pd.DataFrame]:
    """Compute indicators for all tickers in a chunk."""
    return {
        ticker: compute_indicators(ticker, df, indicator_configs)
        for ticker, df in chunk.items()
    }
