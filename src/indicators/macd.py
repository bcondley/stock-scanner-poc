from __future__ import annotations

import pandas as pd

from src.indicators.base import Indicator


class MACDIndicator(Indicator):
    name = "macd"

    def compute(self, df: pd.DataFrame) -> pd.DataFrame:
        fast = int(self.params.get("fast", 12))
        slow = int(self.params.get("slow", 26))
        signal_period = int(self.params.get("signal", 9))

        df = df.copy()
        ema_fast = df["Close"].ewm(span=fast, adjust=False).mean()
        ema_slow = df["Close"].ewm(span=slow, adjust=False).mean()
        df["macd"] = ema_fast - ema_slow
        df["macd_signal"] = df["macd"].ewm(span=signal_period, adjust=False).mean()
        df["macd_hist"] = df["macd"] - df["macd_signal"]
        return df
