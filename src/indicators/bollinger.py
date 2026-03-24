from __future__ import annotations

import pandas as pd

from src.indicators.base import Indicator


class BollingerIndicator(Indicator):
    name = "bollinger"

    def compute(self, df: pd.DataFrame) -> pd.DataFrame:
        period = int(self.params.get("period", 20))
        num_std = float(self.params.get("num_std", 2.0))

        df = df.copy()
        df["bb_mid"] = df["Close"].rolling(window=period).mean()
        rolling_std = df["Close"].rolling(window=period).std()
        df["bb_upper"] = df["bb_mid"] + (rolling_std * num_std)
        df["bb_lower"] = df["bb_mid"] - (rolling_std * num_std)
        df["bb_width"] = df["bb_upper"] - df["bb_lower"]
        return df
