import tempfile
from pathlib import Path

import pytest
from marshmallow import ValidationError

from src.config.schema import ScreenerConfigSchema, load_config


class TestScreenerConfigSchema:
    def test_valid_config(self):
        schema = ScreenerConfigSchema()
        data = {
            "tickers": ["AAPL", "MSFT"],
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "indicators": [{"name": "rsi"}],
        }
        result = schema.load(data)
        assert result["tickers"] == ["AAPL", "MSFT"]
        assert result["chunk_size"] == 100
        assert result["cluster"]["provider"] == "aws"

    def test_missing_tickers(self):
        schema = ScreenerConfigSchema()
        with pytest.raises(ValidationError) as exc_info:
            schema.load({"start_date": "2024-01-01", "end_date": "2024-12-31", "indicators": [{"name": "rsi"}]})
        assert "tickers" in exc_info.value.messages

    def test_empty_tickers(self):
        schema = ScreenerConfigSchema()
        with pytest.raises(ValidationError):
            schema.load({"tickers": [], "start_date": "2024-01-01", "end_date": "2024-12-31", "indicators": [{"name": "rsi"}]})

    def test_invalid_date_format(self):
        schema = ScreenerConfigSchema()
        with pytest.raises(ValidationError):
            schema.load({"tickers": ["AAPL"], "start_date": "01-01-2024", "end_date": "2024-12-31", "indicators": [{"name": "rsi"}]})

    def test_invalid_indicator(self):
        schema = ScreenerConfigSchema()
        with pytest.raises(ValidationError):
            schema.load({"tickers": ["AAPL"], "start_date": "2024-01-01", "end_date": "2024-12-31", "indicators": [{"name": "invalid"}]})

    def test_invalid_provider(self):
        schema = ScreenerConfigSchema()
        with pytest.raises(ValidationError):
            schema.load({
                "tickers": ["AAPL"],
                "start_date": "2024-01-01",
                "end_date": "2024-12-31",
                "indicators": [{"name": "rsi"}],
                "cluster": {"provider": "gcp"},
            })

    def test_node_count_range(self):
        schema = ScreenerConfigSchema()
        with pytest.raises(ValidationError):
            schema.load({
                "tickers": ["AAPL"],
                "start_date": "2024-01-01",
                "end_date": "2024-12-31",
                "indicators": [{"name": "rsi"}],
                "cluster": {"node_count": 100},
            })

    def test_all_indicators(self):
        schema = ScreenerConfigSchema()
        data = {
            "tickers": ["AAPL"],
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "indicators": [
                {"name": "rsi", "params": {"period": 14}},
                {"name": "macd"},
                {"name": "bollinger"},
                {"name": "sma", "params": {"period": 50}},
                {"name": "ema", "params": {"period": 20}},
            ],
        }
        result = schema.load(data)
        assert len(result["indicators"]) == 5


class TestLoadConfig:
    def test_load_yaml(self, tmp_path):
        config_file = tmp_path / "test.yaml"
        config_file.write_text("""
tickers:
  - AAPL
  - MSFT
start_date: "2024-01-01"
end_date: "2024-12-31"
indicators:
  - name: rsi
    params:
      period: 14
filters:
  rsi_below: 30
""")
        cfg = load_config(config_file)
        assert cfg.tickers == ["AAPL", "MSFT"]
        assert cfg.indicators[0].name == "rsi"
        assert cfg.indicators[0].params["period"] == 14
        assert cfg.filters["rsi_below"] == 30

    def test_load_invalid_yaml(self, tmp_path):
        config_file = tmp_path / "bad.yaml"
        config_file.write_text("tickers: []\nstart_date: bad\nend_date: bad\nindicators: []\n")
        with pytest.raises(ValidationError):
            load_config(config_file)
