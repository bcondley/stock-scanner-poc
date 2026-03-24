# CLAUDE.md — Knowledge Transfer

## What Was Built

Full initial implementation of a distributed stock screening pipeline. Every file in the project was an empty stub — all source code, tests, configs, and scripts were written from scratch in a single session.

## Design Decisions

### Config Boundary (marshmallow + dacite)

The config layer is intentionally split into two concerns:
- **Marshmallow** validates raw input at the boundary (rejects bad tickers, invalid date formats, unknown indicators, out-of-range node counts) *before* any compute spins up
- **Dacite** hydrates the validated dict into frozen dataclasses that flow through the pipeline

This means `ScreenerConfig` is always valid by construction — no defensive checks needed downstream. The dataclasses are frozen (`frozen=True`) to prevent accidental mutation during parallel execution.

One gotcha: Marshmallow 4.x changed the `@validates` decorator to pass a `data_key` kwarg. The validators accept `**kwargs` to stay compatible.

The `ClusterSchema` nested field uses `load_default=ClusterSchema().load({})` rather than `load_default={}` because Marshmallow 4 doesn't auto-populate nested schema defaults from an empty dict.

### Indicator Registry Pattern

Indicators use `__init_subclass__` for auto-registration — any class that inherits from `Indicator` and sets a `name` class attribute gets added to `INDICATOR_REGISTRY` automatically. No manual registration or import-time side effects beyond the normal module import.

Adding a new indicator is: create a file, subclass `Indicator`, set `name`, implement `compute()`. The registry picks it up. The `__init__.py` imports ensure all indicators are loaded.

All indicator functions are pure: OHLCV DataFrame in, enriched DataFrame out. They `.copy()` the input to avoid mutation. This makes them safe for parallel execution and trivial to unit test.

### Pipeline DAG

The four stages map cleanly to the spec:

1. **Ingest** — `partition_tickers()` splits the ticker list into chunks sized for parallel workers
2. **Fetch** — `fetch_chunk()` pulls OHLCV via yfinance, caching in Redis. Cache key is `ohlcv:{ticker}:{start}:{end}`
3. **Compute** — `compute_chunk()` applies the indicator chain. Embarrassingly parallel per ticker
4. **Screen** — `apply_filters()` + `rank_results()` aggregates and filters

The `ScreeningPipeline` class has two execution paths:
- `_run_local()` — single-process, no Ray. Good for development and small runs
- `_run_distributed()` — fans out fetch and compute stages as Ray remote tasks

DataFrames are serialized to JSON (`orient="split"`) for Ray object transfer. This is not the fastest serialization but avoids pickle compatibility issues across nodes. `pd.read_json` requires `io.StringIO` wrapping in recent pandas versions (it tries to interpret raw strings as file paths).

### Infrastructure Layer

The `InfraProvider` abstract class uses the same `__init_subclass__` registry pattern as indicators. Currently only `AWSProvider` exists, but `get_provider()` dispatches by name so adding a local/mock provider is straightforward.

`AWSProvider` creates a placement group with `cluster` strategy for low-latency inter-node networking, then launches EC2 instances tagged with `screener-cluster: active` for easy identification and cleanup.

`bootstrap.py` uses Paramiko to SSH into each node, install dependencies, and start Ray. The head node starts `ray start --head`, workers join with `ray start --address={head_private_ip}:6379`. SSH has retry logic (5 attempts, 10s delay) since EC2 instances take time to accept connections after reaching "running" state.

### Autoscaler

The autoscaler is a simple threshold-based approach: it checks Ray's available vs total CPUs to estimate pending work, then requests more EC2 instances via the provider if needed. It's not a polling loop — it's called on-demand (e.g., between pipeline stages or by an external monitor).

### Cluster State

Redis stores three things:
- **Node registry** — hash per node with IP, role, status; set of active node IDs
- **Pipeline status** — current stage and metadata
- **Results** — final ranked output as JSON

This lets external tools query progress without touching the pipeline process.

## Test Architecture

79 tests covering all modules. Key fixtures in `conftest.py`:
- `sample_ohlcv` — 100-row realistic OHLCV with seeded random data (`np.random.seed(42)`)
- `small_ohlcv` — 5-row minimal dataset for edge cases

Tests are designed for parallel execution via `pytest-xdist` (`-n auto` in pyproject.toml). No shared state, no test ordering dependencies.

Infra tests mock Boto3 and Paramiko entirely — no AWS calls. Fetch tests mock `yfinance.download`. The AWS test for duplicate placement groups uses `botocore.exceptions.ClientError` directly rather than the client's exception class (which is dynamically generated and doesn't work well with mocks).

## Environment Notes

- **Python 3.12** required — Ray doesn't support 3.14 yet. The venv is created with `python3.12 -m venv .venv`
- **pyproject.toml** uses `setuptools.build_meta` as build backend
- The `.venv/` directory is at project root (add to `.gitignore` if not already there)

## What's Not Done Yet

- `.gitignore` should include `.venv/`, `results.json`, `__pycache__/`, `.pytest_cache/`
- No integration tests (would need Redis running + real yfinance calls)
- The `cluster_small.yaml` uses `tickers: "sp500"` as a special value but the pipeline doesn't resolve that — it expects a list. Would need a pre-processing step in `load_config` or the pipeline
- No TTL enforcement on the cluster (the `ttl_hours` config exists but nothing watches the clock and tears down)
- Autoscaler is callable but not wired into the pipeline DAG loop — would need a monitor thread or inter-stage check
- No Redis health check in distributed mode startup
- No CI/CD configuration
