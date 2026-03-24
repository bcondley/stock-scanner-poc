from dataclasses import asdict

import dacite
import pytest

from src.config.models import ScreenerConfig, ClusterConfig, IndicatorConfig


class TestIndicatorConfig:
    def test_defaults(self):
        ic = IndicatorConfig(name="rsi")
        assert ic.name == "rsi"
        assert ic.params == {}

    def test_with_params(self):
        ic = IndicatorConfig(name="macd", params={"fast": 12, "slow": 26})
        assert ic.params["fast"] == 12

    def test_frozen(self):
        ic = IndicatorConfig(name="rsi")
        with pytest.raises(AttributeError):
            ic.name = "macd"


class TestClusterConfig:
    def test_defaults(self):
        cc = ClusterConfig()
        assert cc.provider == "aws"
        assert cc.node_count == 3
        assert cc.instance_type == "t3.medium"
        assert cc.ttl_hours == 2

    def test_custom(self):
        cc = ClusterConfig(node_count=5, instance_type="c5.xlarge")
        assert cc.node_count == 5
        assert cc.instance_type == "c5.xlarge"


class TestScreenerConfig:
    def test_minimal(self):
        cfg = ScreenerConfig(
            tickers=["AAPL"],
            start_date="2024-01-01",
            end_date="2024-12-31",
            indicators=[IndicatorConfig(name="rsi")],
        )
        assert len(cfg.tickers) == 1
        assert cfg.chunk_size == 100
        assert cfg.cluster.provider == "aws"

    def test_dacite_hydration(self):
        raw = {
            "tickers": ["AAPL", "MSFT"],
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "indicators": [{"name": "rsi", "params": {"period": 14}}],
            "filters": {"rsi_below": 30},
            "cluster": {"provider": "aws", "node_count": 5},
        }
        cfg = dacite.from_dict(data_class=ScreenerConfig, data=raw)
        assert cfg.tickers == ["AAPL", "MSFT"]
        assert cfg.indicators[0].name == "rsi"
        assert cfg.cluster.node_count == 5
        assert cfg.filters["rsi_below"] == 30
