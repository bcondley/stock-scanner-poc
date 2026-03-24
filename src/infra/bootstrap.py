from __future__ import annotations

import logging
import time

import paramiko

from src.infra.provider import NodeInfo

logger = logging.getLogger(__name__)

INSTALL_SCRIPT = """
set -e
sudo apt-get update -qq
sudo apt-get install -y -qq python3-pip redis-tools
pip3 install -q 'ray[default]>=2.9' redis pandas numpy yfinance
""".strip()


def bootstrap_node(
    node: NodeInfo,
    key_path: str,
    head_ip: str | None = None,
    ray_port: int = 6379,
    username: str = "ubuntu",
    retries: int = 5,
    retry_delay: float = 10.0,
) -> None:
    """SSH into a node, install dependencies, and start/join Ray cluster."""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    for attempt in range(retries):
        try:
            client.connect(node.public_ip, username=username, key_filename=key_path, timeout=30)
            break
        except Exception:
            if attempt == retries - 1:
                raise
            logger.info("SSH attempt %d/%d failed for %s, retrying...", attempt + 1, retries, node.instance_id)
            time.sleep(retry_delay)

    try:
        _run_cmd(client, INSTALL_SCRIPT)

        if head_ip is None:
            # This is the head node
            _run_cmd(client, f"ray start --head --port={ray_port} --dashboard-host=0.0.0.0")
            logger.info("Head node started on %s", node.public_ip)
        else:
            # Worker node — join the cluster
            _run_cmd(client, f"ray start --address={head_ip}:{ray_port}")
            logger.info("Worker %s joined cluster at %s", node.instance_id, head_ip)
    finally:
        client.close()


def bootstrap_cluster(
    nodes: list[NodeInfo],
    key_path: str,
    ray_port: int = 6379,
) -> str:
    """Bootstrap all nodes: first as head, rest as workers. Returns head IP."""
    if not nodes:
        raise ValueError("No nodes to bootstrap")

    head = nodes[0]
    bootstrap_node(head, key_path, head_ip=None, ray_port=ray_port)

    for worker in nodes[1:]:
        bootstrap_node(worker, key_path, head_ip=head.private_ip, ray_port=ray_port)

    return head.public_ip


def _run_cmd(client: paramiko.SSHClient, command: str) -> str:
    _, stdout, stderr = client.exec_command(command, timeout=300)
    exit_code = stdout.channel.recv_exit_status()
    output = stdout.read().decode()
    if exit_code != 0:
        err = stderr.read().decode()
        raise RuntimeError(f"Command failed (exit {exit_code}): {err}")
    return output
