from __future__ import annotations

import logging

import ray

from src.config.models import ClusterConfig
from src.infra.provider import InfraProvider, get_provider
from src.infra.bootstrap import bootstrap_node

logger = logging.getLogger(__name__)


class Autoscaler:
    """Monitors Ray task queue depth and scales EC2 workers accordingly."""

    def __init__(
        self,
        provider: InfraProvider,
        config: ClusterConfig,
        key_path: str = "",
        head_ip: str = "",
        max_nodes: int = 10,
        tasks_per_node: int = 50,
    ) -> None:
        self.provider = provider
        self.config = config
        self.key_path = key_path
        self.head_ip = head_ip
        self.max_nodes = max_nodes
        self.tasks_per_node = tasks_per_node

    def check_and_scale(self) -> int:
        """Check pending tasks and scale up if needed. Returns number of nodes added."""
        pending = self._get_pending_task_count()
        current_nodes = len(self.provider.list_nodes())
        desired = min(
            self.max_nodes,
            max(current_nodes, (pending // self.tasks_per_node) + 1),
        )
        to_add = desired - current_nodes

        if to_add <= 0:
            logger.debug("No scaling needed: %d pending tasks, %d nodes", pending, current_nodes)
            return 0

        logger.info("Scaling up: adding %d nodes (pending=%d, current=%d)", to_add, pending, current_nodes)
        new_nodes = self.provider.launch_nodes(to_add)

        if self.key_path and self.head_ip:
            for node in new_nodes:
                try:
                    bootstrap_node(node, self.key_path, head_ip=self.head_ip)
                except Exception:
                    logger.exception("Failed to bootstrap node %s", node.instance_id)

        return len(new_nodes)

    def _get_pending_task_count(self) -> int:
        try:
            resources = ray.cluster_resources()
            available = ray.available_resources()
            total_cpus = resources.get("CPU", 0)
            free_cpus = available.get("CPU", 0)
            # Approximate pending tasks as used CPUs (tasks in flight)
            return int(total_cpus - free_cpus)
        except Exception:
            logger.debug("Could not query Ray cluster resources")
            return 0
