from __future__ import annotations

import logging

from src.config.models import ScreenerConfig
from src.infra.provider import InfraProvider, NodeInfo, get_provider
from src.infra.bootstrap import bootstrap_cluster
from src.cluster.state import ClusterState
from src.cluster.autoscaler import Autoscaler

logger = logging.getLogger(__name__)


class ClusterManager:
    """High-level cluster lifecycle: provision, bootstrap, run, teardown."""

    def __init__(self, config: ScreenerConfig, key_path: str = "") -> None:
        self.config = config
        self.key_path = key_path
        self.provider: InfraProvider = get_provider(config.cluster)
        self.state = ClusterState(redis_url=config.redis_url)
        self.nodes: list[NodeInfo] = []
        self.head_ip: str = ""

    def provision(self) -> list[NodeInfo]:
        """Launch EC2 instances and bootstrap the Ray cluster."""
        logger.info("Provisioning %d nodes (%s)", self.config.cluster.node_count, self.config.cluster.instance_type)
        self.nodes = self.provider.launch_nodes(self.config.cluster.node_count)

        # Bootstrap Ray on all nodes
        self.head_ip = bootstrap_cluster(self.nodes, self.key_path)

        # Register in state
        for i, node in enumerate(self.nodes):
            role = "head" if i == 0 else "worker"
            self.state.register_node(node.instance_id, node.public_ip, role)

        self.state.set_pipeline_status("provisioned", {"node_count": len(self.nodes)})
        return self.nodes

    def create_autoscaler(self, max_nodes: int = 10) -> Autoscaler:
        return Autoscaler(
            provider=self.provider,
            config=self.config.cluster,
            key_path=self.key_path,
            head_ip=self.head_ip,
            max_nodes=max_nodes,
        )

    def teardown(self) -> None:
        """Terminate all cluster nodes."""
        instance_ids = [n.instance_id for n in self.nodes]
        if instance_ids:
            self.provider.terminate_nodes(instance_ids)
            for nid in instance_ids:
                self.state.deregister_node(nid)
        self.state.set_pipeline_status("terminated")
        self.nodes = []
        logger.info("Cluster teardown complete")
