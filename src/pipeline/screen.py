from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)

FILTER_FUNCTIONS = {
    "rsi_below": lambda df, val: df["rsi"].iloc[-1] < val,
    "rsi_above": lambda df, val: df["rsi"].iloc[-1] > val,
    "macd_cross": lambda df, val: _check_macd_cross(df, val),
    "price_above_sma": lambda df, val: df["Close"].iloc[-1] > df[f"sma_{int(val)}"].iloc[-1],
    "price_below_sma": lambda df, val: df["Close"].iloc[-1] < df[f"sma_{int(val)}"].iloc[-1],
    "bb_squeeze": lambda df, val: df["bb_width"].iloc[-1] < val,
}


def _check_macd_cross(df: pd.DataFrame, direction: str) -> bool:
    if len(df) < 2:
        return False
    prev_hist = df["macd_hist"].iloc[-2]
    curr_hist = df["macd_hist"].iloc[-1]
    if direction == "bullish":
        return prev_hist < 0 and curr_hist >= 0
    elif direction == "bearish":
        return prev_hist > 0 and curr_hist <= 0
    return False


def apply_filters(
    ticker_data: dict[str, pd.DataFrame],
    filters: dict,
) -> dict[str, pd.DataFrame]:
    """Apply screening filters and return only tickers that pass all criteria."""
    if not filters:
        return ticker_data

    passed: dict[str, pd.DataFrame] = {}
    for ticker, df in ticker_data.items():
        if _passes_all_filters(ticker, df, filters):
            passed[ticker] = df
    return passed


def _passes_all_filters(ticker: str, df: pd.DataFrame, filters: dict) -> bool:
    for filter_name, filter_value in filters.items():
        fn = FILTER_FUNCTIONS.get(filter_name)
        if fn is None:
            logger.warning("Unknown filter: %s (skipping)", filter_name)
            continue
        try:
            if not fn(df, filter_value):
                return False
        except (KeyError, IndexError):
            logger.debug("Filter %s failed for %s (missing column/data)", filter_name, ticker)
            return False
    return True


def rank_results(
    ticker_data: dict[str, pd.DataFrame],
    rank_by: str = "rsi",
    ascending: bool = True,
) -> list[dict]:
    """Rank screened results and return summary rows."""
    rows = []
    for ticker, df in ticker_data.items():
        last = df.iloc[-1]
        row: dict = {"ticker": ticker, "close": last.get("Close")}
        for col in df.columns:
            if col not in ("Open", "High", "Low", "Close", "Volume", "Adj Close"):
                row[col] = last.get(col)
        rows.append(row)

    rows.sort(key=lambda r: r.get(rank_by, float("inf")), reverse=not ascending)
    return rows
