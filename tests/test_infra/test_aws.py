from unittest.mock import patch, MagicMock

import pytest
from botocore.exceptions import ClientError

from src.config.models import ClusterConfig
from src.infra.aws import AWSProvider
from src.infra.provider import NodeInfo


@pytest.fixture
def aws_config():
    return ClusterConfig(
        provider="aws",
        node_count=3,
        instance_type="t3.medium",
        region="us-east-1",
        placement_group="test-pg",
    )


class TestAWSProvider:
    @patch("src.infra.aws.boto3")
    def test_ensure_placement_group_creates(self, mock_boto3, aws_config):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_boto3.resource.return_value = MagicMock()

        provider = AWSProvider(aws_config)
        name = provider.ensure_placement_group()
        assert name == "test-pg"
        mock_client.create_placement_group.assert_called_once()

    @patch("src.infra.aws.boto3")
    def test_ensure_placement_group_already_exists(self, mock_boto3, aws_config):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_boto3.resource.return_value = MagicMock()

        # Simulate duplicate placement group error
        mock_client.create_placement_group.side_effect = ClientError(
            {"Error": {"Code": "InvalidPlacementGroup.Duplicate", "Message": "already exists"}},
            "CreatePlacementGroup",
        )

        provider = AWSProvider(aws_config)
        name = provider.ensure_placement_group()
        assert name == "test-pg"

    @patch("src.infra.aws.boto3")
    def test_list_nodes(self, mock_boto3, aws_config):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_boto3.resource.return_value = MagicMock()

        mock_client.describe_instances.return_value = {
            "Reservations": [
                {
                    "Instances": [
                        {
                            "InstanceId": "i-123",
                            "PublicIpAddress": "1.2.3.4",
                            "PrivateIpAddress": "10.0.0.1",
                            "State": {"Name": "running"},
                        }
                    ]
                }
            ]
        }

        provider = AWSProvider(aws_config)
        nodes = provider.list_nodes()
        assert len(nodes) == 1
        assert nodes[0].instance_id == "i-123"
        assert nodes[0].public_ip == "1.2.3.4"

    @patch("src.infra.aws.boto3")
    def test_terminate_nodes(self, mock_boto3, aws_config):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_boto3.resource.return_value = MagicMock()

        provider = AWSProvider(aws_config)
        provider.terminate_nodes(["i-123", "i-456"])
        mock_client.terminate_instances.assert_called_once_with(InstanceIds=["i-123", "i-456"])

    @patch("src.infra.aws.boto3")
    def test_terminate_empty_list(self, mock_boto3, aws_config):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_boto3.resource.return_value = MagicMock()

        provider = AWSProvider(aws_config)
        provider.terminate_nodes([])
        mock_client.terminate_instances.assert_not_called()
