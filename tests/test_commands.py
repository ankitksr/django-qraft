"""Tests for the qraftcluster management command (--name re-exec)."""

import os
import sys
from unittest.mock import patch

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError

ALT_Q_CLUSTER = {
    "name": "test",
    "orm": "default",
    "broker_class": "qraft.brokers.QraftOrmBroker",
    "ALT_CLUSTERS": {"io-workers": {}},
}


@pytest.fixture
def fake_argv(monkeypatch):
    """A manage.py-shaped argv, so the rebuilt command line is inspectable."""
    argv = ["manage.py", "qraftcluster", "--run-once", "--name", "io-workers"]
    monkeypatch.setattr(sys, "argv", argv)
    return argv


@pytest.fixture
def execve():
    with patch("qraft.management.commands.qraftcluster.os.execve") as mock:
        yield mock


@pytest.fixture
def no_cluster_env(monkeypatch):
    monkeypatch.delenv("Q_CLUSTER_NAME", raising=False)


@pytest.fixture
def alt_clusters(settings):
    """Q_CLUSTER that declares io-workers, as Django-Q2 requires."""
    settings.Q_CLUSTER = ALT_Q_CLUSTER


@pytest.fixture
def cluster():
    with patch("qraft.management.commands.qraftcluster.QraftCluster") as mock:
        yield mock


@pytest.mark.usefixtures("fake_argv", "no_cluster_env", "alt_clusters", "cluster")
class TestReexec:
    def test_name_reexecs_with_the_environment_variable_set(self, execve):
        call_command("qraftcluster", "--run-once", "--name", "io-workers")

        execve.assert_called_once()
        binary, _argv, env = execve.call_args[0]
        assert binary == sys.executable
        assert env["Q_CLUSTER_NAME"] == "io-workers"

    def test_reexec_preserves_every_other_argument(self, execve, fake_argv):
        call_command("qraftcluster", "--run-once", "--name", "io-workers")

        _binary, argv, _env = execve.call_args[0]
        assert argv == [sys.executable, *fake_argv]
        assert "--run-once" in argv

    def test_does_not_reexec_when_the_variable_already_matches(self, execve, cluster):
        """The no-loop property: the re-executed process takes the normal path."""
        with patch.dict(os.environ, {"Q_CLUSTER_NAME": "io-workers"}):
            call_command("qraftcluster", "--run-once", "--name", "io-workers")

        execve.assert_not_called()
        cluster.return_value.start.assert_called_once()

    def test_the_rebuilt_environment_terminates_a_second_pass(self, execve, cluster):
        """
        Feed the environment the re-exec builds back into the command.

        Proves termination without replacing the process image: the second
        pass must run the cluster, not exec a third one.
        """
        call_command("qraftcluster", "--run-once", "--name", "io-workers")
        _binary, _argv, env = execve.call_args[0]
        execve.reset_mock()

        with patch.dict(os.environ, env):
            call_command("qraftcluster", "--run-once", "--name", "io-workers")

        execve.assert_not_called()
        cluster.return_value.start.assert_called_once()

    def test_without_name_nothing_is_reexeced(self, execve, cluster):
        call_command("qraftcluster", "--run-once")

        execve.assert_not_called()
        cluster.return_value.start.assert_called_once()

    def test_the_default_cluster_name_needs_no_alt_clusters_entry(self, execve):
        call_command("qraftcluster", "--name", "test")

        execve.assert_called_once()


@pytest.mark.usefixtures("fake_argv", "no_cluster_env", "cluster")
class TestValidation:
    def test_unknown_name_errors_before_reexec(self, execve, alt_clusters):
        with pytest.raises(CommandError, match="Unknown cluster name 'typo'"):
            call_command("qraftcluster", "--name", "typo")

        execve.assert_not_called()

    def test_missing_q_cluster_alt_clusters_errors_before_reexec(self, execve):
        """
        django_q.conf.Conf pops ALT_CLUSTERS unguarded, so a name known only
        to QRAFT_CLUSTER would raise KeyError on import in the new process.
        """
        with pytest.raises(CommandError, match="no ALT_CLUSTERS key"):
            call_command("qraftcluster", "--name", "io-workers")

        execve.assert_not_called()
