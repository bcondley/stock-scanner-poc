from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ClusterConfig:
    provider: str = "aws"
    node_count: int = 3
    instance_type: str = "t3.medium"
    ttl_hours: int = 2
    region: str = "us-east-1"
    key_name: str = ""
    placement_group: str = "screener-pg"


@dataclass(frozen=True)
class IndicatorConfig:
    name: str
    params: dict = field(default_factory=dict)


@dataclass(frozen=True)
class ScreenerConfig:
    tickers: list[str]
    start_date: str
    end_date: str
    indicators: list[IndicatorConfig]
    filters: dict = field(default_factory=dict)
    cluster: ClusterConfig = field(default_factory=ClusterConfig)
    chunk_size: int = 100
    redis_url: str = "redis://localhost:6379/0"
    cache_ttl: int = 3600
