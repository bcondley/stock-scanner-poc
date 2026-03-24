# Stock Screener

Distributed stock screening pipeline that screens ~500 S&P 500 tickers through a multi-stage analysis pipeline using Ray for parallel computation.

Pull price history, compute technical indicators, rank and filter based on configurable criteria — distributed across an EC2 cluster or run locally.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    CLI (click)                          │
├─────────────────────────────────────────────────────────┤
│              Config (marshmallow + dacite)              │
│         YAML → validate → typed dataclasses             │
├─────────────────────────────────────────────────────────┤
│                   Pipeline DAG                          │
│                                                         │
│  Stage 1: Ingest ──→ partition tickers into chunks      │
│  Stage 2: Fetch  ──→ yfinance OHLCV (Redis-cached)      │
│  Stage 3: Compute ─→ indicators (RSI, MACD, BB, MA)     │
│  Stage 4: Screen ──→ filter + rank results              │
│                                                         │
│  Stages 2-3 fan out across Ray workers                  │
├─────────────────────────────────────────────────────────┤
│              Infrastructure Layer                       │
│                                                         │
│  Boto3 ──→ EC2 cluster (placement group)                │
│  Paramiko ──→ SSH bootstrap (install Ray, join cluster) │
│  Redis ──→ cache, cluster state, result sink            │
│  Autoscaler ──→ monitors queue depth, adds workers      │
└─────────────────────────────────────────────────────────┘
```

## Quick Start

```bash
# Create venv and install
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Validate a config
stock-screener validate configs/default_screen.yaml

# Run locally (no cluster needed)
stock-screener run configs/default_screen.yaml --local -o results.json

# Or use the convenience script
./scripts/run_local.sh
```

## Distributed Mode

```bash
# Provision EC2 cluster and run
stock-screener provision configs/cluster_small.yaml --key-path ~/.ssh/my-key.pem
stock-screener run configs/cluster_small.yaml -o results.json

# Tear down when done
stock-screener teardown configs/cluster_small.yaml

# Or use the convenience script
./scripts/run_cluster.sh configs/cluster_small.yaml ~/.ssh/my-key.pem
```

**Prerequisites for distributed mode:**
- AWS credentials configured (`aws configure` or environment variables)
- An EC2 key pair for SSH access
- Redis (local or ElastiCache) for caching and state

## Configuration

Configs are YAML files validated by Marshmallow before any compute runs. See `configs/` for examples.

```yaml
tickers:
  - AAPL
  - MSFT
  - GOOGL

start_date: "2024-01-01"
end_date: "2024-12-31"

indicators:
  - name: rsi
    params:
      period: 14
  - name: macd
    params:
      fast: 12
      slow: 26
      signal: 9
  - name: bollinger
    params:
      period: 20
      num_std: 2.0
  - name: sma
    params:
      period: 50
  - name: ema
    params:
      period: 20

filters:
  rsi_below: 30          # RSI oversold
  macd_cross: bullish     # MACD bullish crossover
  price_above_sma: 50     # Price above 50-day SMA

cluster:
  provider: aws
  node_count: 3
  instance_type: t3.medium
  ttl_hours: 2
  region: us-east-1
  placement_group: screener-pg

chunk_size: 100
redis_url: "redis://localhost:6379/0"
cache_ttl: 3600
```

### Available Indicators

| Indicator | Name | Parameters |
|-----------|------|------------|
| RSI | `rsi` | `period` (default: 14) |
| MACD | `macd` | `fast` (12), `slow` (26), `signal` (9) |
| Bollinger Bands | `bollinger` | `period` (20), `num_std` (2.0) |
| Simple Moving Avg | `sma` | `period` (20) |
| Exponential Moving Avg | `ema` | `period` (20) |

### Available Filters

| Filter | Value | Description |
|--------|-------|-------------|
| `rsi_below` | number | RSI below threshold (oversold) |
| `rsi_above` | number | RSI above threshold (overbought) |
| `macd_cross` | `"bullish"` / `"bearish"` | MACD histogram crossover |
| `price_above_sma` | period | Price above SMA of given period |
| `price_below_sma` | period | Price below SMA of given period |
| `bb_squeeze` | number | Bollinger Band width below threshold |

## Project Structure

```
src/
├── cli.py                  # Click CLI (run, provision, teardown, validate)
├── config/
│   ├── models.py           # Frozen dataclasses (ScreenerConfig, etc.)
│   └── schema.py           # Marshmallow validation + YAML loader
├── indicators/
│   ├── base.py             # Abstract Indicator with auto-registry
│   ├── rsi.py              # Relative Strength Index
│   ├── macd.py             # Moving Average Convergence Divergence
│   ├── bollinger.py        # Bollinger Bands
│   └── moving_avg.py       # SMA and EMA
├── pipeline/
│   ├── dag.py              # ScreeningPipeline orchestrator (local + Ray)
│   ├── ingest.py           # Ticker partitioning + S&P 500 list
│   ├── fetch.py            # yfinance data fetching with Redis cache
│   ├── compute.py          # Indicator computation on chunks
│   └── screen.py           # Filter application + result ranking
├── infra/
│   ├── provider.py         # Abstract InfraProvider with auto-registry
│   ├── aws.py              # Boto3 EC2 provisioning + placement groups
│   └── bootstrap.py        # Paramiko SSH bootstrap (install Ray, join cluster)
└── cluster/
    ├── manager.py          # Cluster lifecycle (provision → teardown)
    ├── state.py            # Redis-backed cluster state tracking
    └── autoscaler.py       # Ray queue monitoring + EC2 scaling

tests/                      # 79 tests, parallel via pytest-xdist
configs/                    # YAML config examples
scripts/                    # Shell convenience scripts
```

## Testing

```bash
# Run all tests (parallel by default via pytest-xdist)
pytest

# Run sequentially with verbose output
pytest -o "addopts=" -v

# Run a specific module
pytest tests/test_indicators/ -v
```

## Key Dependencies

| Package | Role |
|---------|------|
| `ray` | Distributed task execution across workers |
| `boto3` | EC2 cluster provisioning |
| `paramiko` | SSH into nodes to install Ray and join cluster |
| `redis` | Cache layer, cluster state, result sink |
| `marshmallow` | Config validation at the boundary |
| `dacite` | Hydrate validated dicts into typed dataclasses |
| `yfinance` | Free OHLCV price data (no API key needed) |
| `pandas` / `numpy` | Data manipulation and indicator math |
| `click` | CLI interface |
| `pytest-xdist` | Parallel test execution |
