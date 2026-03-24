from __future__ import annotations

import logging

import boto3
from botocore.exceptions import ClientError

from src.config.models import ClusterConfig
from src.infra.provider import InfraProvider, NodeInfo

logger = logging.getLogger(__name__)

# Ubuntu 22.04 AMI (us-east-1) — used as default for Ray workers
DEFAULT_AMI = "ami-0c7217cdde317cfec"


class AWSProvider(InfraProvider):
    provider_name = "aws"

    def __init__(self, config: ClusterConfig) -> None:
        super().__init__(config)
        self.ec2 = boto3.client("ec2", region_name=config.region)
        self.ec2_resource = boto3.resource("ec2", region_name=config.region)
        self._tag_key = "screener-cluster"
        self._tag_value = "active"

    def ensure_placement_group(self) -> str:
        name = self.config.placement_group
        try:
            self.ec2.create_placement_group(
                GroupName=name,
                Strategy="cluster",
            )
            logger.info("Created placement group: %s", name)
        except ClientError as e:
            if "InvalidPlacementGroup.Duplicate" not in str(e):
                raise
            logger.debug("Placement group %s already exists", name)
        return name

    def launch_nodes(self, count: int) -> list[NodeInfo]:
        pg = self.ensure_placement_group()
        kwargs: dict = {
            "ImageId": DEFAULT_AMI,
            "InstanceType": self.config.instance_type,
            "MinCount": count,
            "MaxCount": count,
            "Placement": {"GroupName": pg},
            "TagSpecifications": [
                {
                    "ResourceType": "instance",
                    "Tags": [
                        {"Key": self._tag_key, "Value": self._tag_value},
                        {"Key": "Name", "Value": "screener-worker"},
                    ],
                }
            ],
        }
        if self.config.key_name:
            kwargs["KeyName"] = self.config.key_name

        response = self.ec2.run_instances(**kwargs)
        instance_ids = [inst["InstanceId"] for inst in response["Instances"]]

        # Wait for running state
        waiter = self.ec2.get_waiter("instance_running")
        waiter.wait(InstanceIds=instance_ids)

        return self._describe_instances(instance_ids)

    def terminate_nodes(self, instance_ids: list[str]) -> None:
        if not instance_ids:
            return
        self.ec2.terminate_instances(InstanceIds=instance_ids)
        logger.info("Terminated %d instances", len(instance_ids))

    def list_nodes(self) -> list[NodeInfo]:
        response = self.ec2.describe_instances(
            Filters=[
                {"Name": f"tag:{self._tag_key}", "Values": [self._tag_value]},
                {"Name": "instance-state-name", "Values": ["running", "pending"]},
            ]
        )
        nodes = []
        for reservation in response["Reservations"]:
            for inst in reservation["Instances"]:
                nodes.append(NodeInfo(
                    instance_id=inst["InstanceId"],
                    public_ip=inst.get("PublicIpAddress", ""),
                    private_ip=inst.get("PrivateIpAddress", ""),
                    state=inst["State"]["Name"],
                ))
        return nodes

    def _describe_instances(self, instance_ids: list[str]) -> list[NodeInfo]:
        response = self.ec2.describe_instances(InstanceIds=instance_ids)
        nodes = []
        for reservation in response["Reservations"]:
            for inst in reservation["Instances"]:
                nodes.append(NodeInfo(
                    instance_id=inst["InstanceId"],
                    public_ip=inst.get("PublicIpAddress", ""),
                    private_ip=inst.get("PrivateIpAddress", ""),
                    state=inst["State"]["Name"],
                ))
        return nodes
