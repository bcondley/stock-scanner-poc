from unittest.mock import patch, MagicMock, call

import pytest

from src.infra.provider import NodeInfo
from src.infra.bootstrap import bootstrap_node, bootstrap_cluster, _run_cmd


@pytest.fixture
def mock_node():
    return NodeInfo(instance_id="i-123", public_ip="1.2.3.4", private_ip="10.0.0.1", state="running")


@pytest.fixture
def mock_nodes():
    return [
        NodeInfo(instance_id="i-head", public_ip="1.1.1.1", private_ip="10.0.0.1", state="running"),
        NodeInfo(instance_id="i-w1", public_ip="2.2.2.2", private_ip="10.0.0.2", state="running"),
        NodeInfo(instance_id="i-w2", public_ip="3.3.3.3", private_ip="10.0.0.3", state="running"),
    ]


class TestRunCmd:
    def test_success(self):
        mock_client = MagicMock()
        mock_stdout = MagicMock()
        mock_stdout.read.return_value = b"ok"
        mock_stdout.channel.recv_exit_status.return_value = 0
        mock_stderr = MagicMock()
        mock_stderr.read.return_value = b""
        mock_client.exec_command.return_value = (None, mock_stdout, mock_stderr)

        result = _run_cmd(mock_client, "echo hello")
        assert result == "ok"

    def test_failure_raises(self):
        mock_client = MagicMock()
        mock_stdout = MagicMock()
        mock_stdout.read.return_value = b""
        mock_stdout.channel.recv_exit_status.return_value = 1
        mock_stderr = MagicMock()
        mock_stderr.read.return_value = b"error msg"
        mock_client.exec_command.return_value = (None, mock_stdout, mock_stderr)

        with pytest.raises(RuntimeError, match="Command failed"):
            _run_cmd(mock_client, "bad command")


class TestBootstrapNode:
    @patch("src.infra.bootstrap.paramiko.SSHClient")
    def test_head_node(self, mock_ssh_cls, mock_node):
        mock_client = MagicMock()
        mock_ssh_cls.return_value = mock_client
        mock_stdout = MagicMock()
        mock_stdout.read.return_value = b""
        mock_stdout.channel.recv_exit_status.return_value = 0
        mock_stderr = MagicMock()
        mock_stderr.read.return_value = b""
        mock_client.exec_command.return_value = (None, mock_stdout, mock_stderr)

        bootstrap_node(mock_node, "/path/to/key", head_ip=None)
        mock_client.connect.assert_called_once()
        # Should have called exec_command for install + ray start --head
        assert mock_client.exec_command.call_count == 2

    @patch("src.infra.bootstrap.paramiko.SSHClient")
    def test_worker_node(self, mock_ssh_cls, mock_node):
        mock_client = MagicMock()
        mock_ssh_cls.return_value = mock_client
        mock_stdout = MagicMock()
        mock_stdout.read.return_value = b""
        mock_stdout.channel.recv_exit_status.return_value = 0
        mock_stderr = MagicMock()
        mock_stderr.read.return_value = b""
        mock_client.exec_command.return_value = (None, mock_stdout, mock_stderr)

        bootstrap_node(mock_node, "/path/to/key", head_ip="10.0.0.1")
        assert mock_client.exec_command.call_count == 2


class TestBootstrapCluster:
    @patch("src.infra.bootstrap.bootstrap_node")
    def test_cluster_bootstrap(self, mock_bootstrap, mock_nodes):
        head_ip = bootstrap_cluster(mock_nodes, "/path/to/key")
        assert head_ip == "1.1.1.1"
        assert mock_bootstrap.call_count == 3
        # First call should be head (no head_ip)
        first_call = mock_bootstrap.call_args_list[0]
        assert first_call[1].get("head_ip") is None or first_call[0][2] is None
        # Subsequent calls should have head's private IP
        for worker_call in mock_bootstrap.call_args_list[1:]:
            assert "10.0.0.1" in str(worker_call)

    @patch("src.infra.bootstrap.bootstrap_node")
    def test_empty_nodes_raises(self, mock_bootstrap):
        with pytest.raises(ValueError):
            bootstrap_cluster([], "/path/to/key")
