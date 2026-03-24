from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from src.config.models import ClusterConfig

PROVIDER_REGISTRY: dict[str, type[InfraProvider]] = {}


def get_provider(config: ClusterConfig) -> InfraProvider:
    cls = PROVIDER_REGISTRY.get(config.provider)
    if cls is None:
        raise KeyError(f"Unknown provider: {config.provider!r}. Available: {sorted(PROVIDER_REGISTRY)}")
    return cls(config)


@dataclass
class NodeInfo:
    instance_id: str
    public_ip: str
    private_ip: str
    state: str


class InfraProvider(ABC):
    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        if hasattr(cls, "provider_name") and cls.provider_name:
            PROVIDER_REGISTRY[cls.provider_name] = cls

    def __init__(self, config: ClusterConfig) -> None:
        self.config = config

    @abstractmethod
    def launch_nodes(self, count: int) -> list[NodeInfo]:
        """Launch EC2 instances (or equivalent) and return their info."""

    @abstractmethod
    def terminate_nodes(self, instance_ids: list[str]) -> None:
        """Terminate the given instances."""

    @abstractmethod
    def list_nodes(self) -> list[NodeInfo]:
        """List all active nodes in the cluster."""

    @abstractmethod
    def ensure_placement_group(self) -> str:
        """Create placement group if it doesn't exist, return its name."""
