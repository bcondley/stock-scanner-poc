from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

INDICATOR_REGISTRY: dict[str, type[Indicator]] = {}


def get_indicator(name: str) -> type[Indicator]:
    if name not in INDICATOR_REGISTRY:
        raise KeyError(f"Unknown indicator: {name!r}. Available: {sorted(INDICATOR_REGISTRY)}")
    return INDICATOR_REGISTRY[name]


class Indicator(ABC):
    name: str

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        if hasattr(cls, "name") and cls.name:
            INDICATOR_REGISTRY[cls.name] = cls

    def __init__(self, params: dict | None = None) -> None:
        self.params = params or {}

    @abstractmethod
    def compute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute the indicator, adding columns to df. Expects OHLCV input."""

    def __repr__(self) -> str:
        return f"{type(self).__name__}(params={self.params})"
