from __future__ import annotations

import json
import logging
import sys

import click

from src.config.schema import load_config
from src.pipeline.dag import ScreeningPipeline


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging")
def cli(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(name)s %(levelname)s %(message)s")


@cli.command()
@click.argument("config_path", type=click.Path(exists=True))
@click.option("--local", is_flag=True, help="Run locally without Ray distribution")
@click.option("--output", "-o", type=click.Path(), help="Write results to JSON file")
def run(config_path: str, local: bool, output: str | None) -> None:
    """Run the stock screening pipeline."""
    config = load_config(config_path)
    pipeline = ScreeningPipeline(config)
    results = pipeline.run(use_ray=not local)

    if output:
        with open(output, "w") as f:
            json.dump(results, f, indent=2, default=str)
        click.echo(f"Results written to {output}")
    else:
        click.echo(json.dumps(results, indent=2, default=str))

    click.echo(f"\n{len(results)} tickers passed screening")


@cli.command()
@click.argument("config_path", type=click.Path(exists=True))
@click.option("--key-path", required=True, help="Path to SSH private key")
def provision(config_path: str, key_path: str) -> None:
    """Provision an EC2 cluster for distributed screening."""
    from src.cluster.manager import ClusterManager

    config = load_config(config_path)
    manager = ClusterManager(config, key_path=key_path)
    nodes = manager.provision()
    click.echo(f"Cluster ready: {len(nodes)} nodes")
    for node in nodes:
        click.echo(f"  {node.instance_id} — {node.public_ip} ({node.state})")


@cli.command()
@click.argument("config_path", type=click.Path(exists=True))
def teardown(config_path: str) -> None:
    """Tear down the EC2 cluster."""
    from src.cluster.manager import ClusterManager

    config = load_config(config_path)
    manager = ClusterManager(config)
    # List existing nodes and terminate
    nodes = manager.provider.list_nodes()
    instance_ids = [n.instance_id for n in nodes]
    if instance_ids:
        manager.provider.terminate_nodes(instance_ids)
        click.echo(f"Terminated {len(instance_ids)} instances")
    else:
        click.echo("No active instances found")


@cli.command()
@click.argument("config_path", type=click.Path(exists=True))
def validate(config_path: str) -> None:
    """Validate a screening config file."""
    try:
        config = load_config(config_path)
        click.echo("Config is valid:")
        click.echo(f"  Tickers: {len(config.tickers)}")
        click.echo(f"  Date range: {config.start_date} to {config.end_date}")
        click.echo(f"  Indicators: {[i.name for i in config.indicators]}")
        click.echo(f"  Filters: {config.filters}")
    except Exception as e:
        click.echo(f"Invalid config: {e}", err=True)
        sys.exit(1)


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
