from __future__ import annotations

import pandas as pd

from src.indicators.base import Indicator


class RSIIndicator(Indicator):
    name = "rsi"

    def compute(self, df: pd.DataFrame) -> pd.DataFrame:
        period = int(self.params.get("period", 14))
        delta = df["Close"].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
        rs = avg_gain / avg_loss
        df = df.copy()
        df["rsi"] = 100 - (100 / (1 + rs))
        return df
