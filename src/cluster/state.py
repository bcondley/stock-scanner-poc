from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict

import redis as redis_mod

logger = logging.getLogger(__name__)


@dataclass
class ClusterState:
    """Tracks cluster state in Redis."""

    redis_url: str = "redis://localhost:6379/0"
    _prefix: str = "cluster:"

    def _client(self) -> redis_mod.Redis:
        return redis_mod.from_url(self.redis_url)

    def register_node(self, instance_id: str, ip: str, role: str = "worker") -> None:
        client = self._client()
        key = f"{self._prefix}node:{instance_id}"
        client.hset(key, mapping={"ip": ip, "role": role, "status": "active"})
        client.sadd(f"{self._prefix}nodes", instance_id)
        logger.info("Registered node %s (%s) as %s", instance_id, ip, role)

    def deregister_node(self, instance_id: str) -> None:
        client = self._client()
        client.delete(f"{self._prefix}node:{instance_id}")
        client.srem(f"{self._prefix}nodes", instance_id)
        logger.info("Deregistered node %s", instance_id)

    def get_active_nodes(self) -> list[dict]:
        client = self._client()
        node_ids = client.smembers(f"{self._prefix}nodes")
        nodes = []
        for nid in node_ids:
            nid_str = nid.decode() if isinstance(nid, bytes) else nid
            data = client.hgetall(f"{self._prefix}node:{nid_str}")
            if data:
                decoded = {k.decode(): v.decode() for k, v in data.items()}
                decoded["instance_id"] = nid_str
                nodes.append(decoded)
        return nodes

    def set_pipeline_status(self, status: str, metadata: dict | None = None) -> None:
        client = self._client()
        payload = {"status": status}
        if metadata:
            payload["metadata"] = json.dumps(metadata)
        client.hset(f"{self._prefix}pipeline", mapping=payload)

    def get_pipeline_status(self) -> dict:
        client = self._client()
        raw = client.hgetall(f"{self._prefix}pipeline")
        return {k.decode(): v.decode() for k, v in raw.items()} if raw else {}

    def store_results(self, results: list[dict]) -> None:
        client = self._client()
        client.set(f"{self._prefix}results", json.dumps(results))

    def get_results(self) -> list[dict]:
        client = self._client()
        raw = client.get(f"{self._prefix}results")
        if raw is None:
            return []
        return json.loads(raw)

    def clear(self) -> None:
        client = self._client()
        keys = client.keys(f"{self._prefix}*")
        if keys:
            client.delete(*keys)
