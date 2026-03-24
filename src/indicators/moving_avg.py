from __future__ import annotations

import pandas as pd

from src.indicators.base import Indicator


class SMAIndicator(Indicator):
    name = "sma"

    def compute(self, df: pd.DataFrame) -> pd.DataFrame:
        period = int(self.params.get("period", 20))
        df = df.copy()
        df[f"sma_{period}"] = df["Close"].rolling(window=period).mean()
        return df


class EMAIndicator(Indicator):
    name = "ema"

    def compute(self, df: pd.DataFrame) -> pd.DataFrame:
        period = int(self.params.get("period", 20))
        df = df.copy()
        df[f"ema_{period}"] = df["Close"].ewm(span=period, adjust=False).mean()
        return df
