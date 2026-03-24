from __future__ import annotations

from pathlib import Path

import dacite
import yaml
from marshmallow import Schema, fields, validate, validates, ValidationError

from src.config.models import ScreenerConfig, IndicatorConfig

VALID_INDICATORS = {"rsi", "macd", "bollinger", "sma", "ema"}
VALID_PROVIDERS = {"aws", "local"}


class IndicatorSchema(Schema):
    name = fields.String(required=True, validate=validate.OneOf(VALID_INDICATORS))
    params = fields.Dict(keys=fields.String(), values=fields.Raw(), load_default={})


class ClusterSchema(Schema):
    provider = fields.String(load_default="aws", validate=validate.OneOf(VALID_PROVIDERS))
    node_count = fields.Integer(load_default=3, validate=validate.Range(min=1, max=20))
    instance_type = fields.String(load_default="t3.medium")
    ttl_hours = fields.Integer(load_default=2, validate=validate.Range(min=1, max=24))
    region = fields.String(load_default="us-east-1")
    key_name = fields.String(load_default="")
    placement_group = fields.String(load_default="screener-pg")


class ScreenerConfigSchema(Schema):
    tickers = fields.List(fields.String(), required=True, validate=validate.Length(min=1))
    start_date = fields.String(required=True)
    end_date = fields.String(required=True)
    indicators = fields.List(fields.Nested(IndicatorSchema), required=True, validate=validate.Length(min=1))
    filters = fields.Dict(keys=fields.String(), values=fields.Raw(), load_default={})
    cluster = fields.Nested(ClusterSchema, load_default=ClusterSchema().load({}))
    chunk_size = fields.Integer(load_default=100, validate=validate.Range(min=1))
    redis_url = fields.String(load_default="redis://localhost:6379/0")
    cache_ttl = fields.Integer(load_default=3600, validate=validate.Range(min=0))

    @validates("start_date")
    def validate_start_date(self, value: str, **kwargs: object) -> None:
        import re
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", value):
            raise ValidationError("start_date must be YYYY-MM-DD format")

    @validates("end_date")
    def validate_end_date(self, value: str, **kwargs: object) -> None:
        import re
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", value):
            raise ValidationError("end_date must be YYYY-MM-DD format")


def load_config(path: str | Path) -> ScreenerConfig:
    """Load and validate a YAML config file, returning a typed ScreenerConfig."""
    raw = yaml.safe_load(Path(path).read_text())
    schema = ScreenerConfigSchema()
    validated = schema.load(raw)
    return dacite.from_dict(data_class=ScreenerConfig, data=validated)
