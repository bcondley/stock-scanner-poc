from __future__ import annotations

import io
import logging
from typing import TYPE_CHECKING

import ray

from src.config.models import ScreenerConfig
from src.pipeline.ingest import partition_tickers
from src.pipeline.fetch import fetch_chunk
from src.pipeline.compute import compute_chunk
from src.pipeline.screen import apply_filters, rank_results

if TYPE_CHECKING:
    import redis as redis_mod

logger = logging.getLogger(__name__)


@ray.remote
def _ray_fetch_chunk(
    tickers: list[str],
    start_date: str,
    end_date: str,
    redis_url: str | None,
    cache_ttl: int,
) -> dict:
    """Ray remote task: fetch OHLCV data for a chunk of tickers."""
    redis_client = None
    if redis_url:
        import redis
        redis_client = redis.from_url(redis_url)
    result = fetch_chunk(tickers, start_date, end_date, redis_client, cache_ttl)
    # Serialize DataFrames to JSON for Ray transfer
    return {t: df.to_json(orient="split", date_format="iso") for t, df in result.items()}


@ray.remote
def _ray_compute_chunk(
    chunk_json: dict,
    indicator_configs_raw: list[dict],
) -> dict:
    """Ray remote task: compute indicators for a chunk."""
    import pandas as pd
    from src.config.models import IndicatorConfig

    chunk = {t: pd.read_json(io.StringIO(j), orient="split") for t, j in chunk_json.items()}
    configs = [IndicatorConfig(**c) for c in indicator_configs_raw]
    result = compute_chunk(chunk, configs)
    return {t: df.to_json(orient="split", date_format="iso") for t, df in result.items()}


class ScreeningPipeline:
    """Orchestrates the 4-stage screening DAG."""

    def __init__(self, config: ScreenerConfig) -> None:
        self.config = config

    def run(self, use_ray: bool = True) -> list[dict]:
        """Execute the full pipeline, returning ranked results."""
        if use_ray:
            return self._run_distributed()
        return self._run_local()

    def _run_local(self) -> list[dict]:
        """Run pipeline without Ray (single-process)."""
        import redis as redis_mod

        logger.info("Stage 1: Ingesting %d tickers", len(self.config.tickers))
        chunks = partition_tickers(self.config.tickers, self.config.chunk_size)

        redis_client = None
        try:
            redis_client = redis_mod.from_url(self.config.redis_url)
            redis_client.ping()
        except Exception:
            logger.warning("Redis unavailable, proceeding without cache")
            redis_client = None

        logger.info("Stage 2: Fetching data (%d chunks)", len(chunks))
        all_data: dict = {}
        for chunk in chunks:
            result = fetch_chunk(
                chunk, self.config.start_date, self.config.end_date,
                redis_client, self.config.cache_ttl,
            )
            all_data.update(result)

        indicator_configs = list(self.config.indicators)
        logger.info("Stage 3: Computing %d indicators", len(indicator_configs))
        all_data = compute_chunk(all_data, indicator_configs)

        logger.info("Stage 4: Screening and ranking")
        filtered = apply_filters(all_data, self.config.filters)
        return rank_results(filtered)

    def _run_distributed(self) -> list[dict]:
        """Run pipeline distributed across Ray workers."""
        import pandas as pd

        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)

        logger.info("Stage 1: Ingesting %d tickers", len(self.config.tickers))
        chunks = partition_tickers(self.config.tickers, self.config.chunk_size)

        redis_url = self.config.redis_url
        cache_ttl = self.config.cache_ttl

        logger.info("Stage 2: Fetching data (%d chunks across Ray workers)", len(chunks))
        fetch_futures = [
            _ray_fetch_chunk.remote(chunk, self.config.start_date, self.config.end_date, redis_url, cache_ttl)
            for chunk in chunks
        ]
        fetched_chunks = ray.get(fetch_futures)

        indicator_dicts = [{"name": ic.name, "params": ic.params} for ic in self.config.indicators]

        logger.info("Stage 3: Computing %d indicators across Ray workers", len(indicator_dicts))
        compute_futures = [
            _ray_compute_chunk.remote(chunk_json, indicator_dicts)
            for chunk_json in fetched_chunks
        ]
        computed_chunks = ray.get(compute_futures)

        # Merge all chunks
        all_data: dict[str, pd.DataFrame] = {}
        for chunk_json in computed_chunks:
            for ticker, json_str in chunk_json.items():
                all_data[ticker] = pd.read_json(io.StringIO(json_str), orient="split")

        logger.info("Stage 4: Screening and ranking")
        filtered = apply_filters(all_data, self.config.filters)
        return rank_results(filtered)
