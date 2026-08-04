"""Tests for qraft.cluster module (QraftSentinel/QraftCluster worker selection)."""

import uuid
from unittest.mock import Mock, patch

from django_q.worker import worker as q2_worker

from qraft.cluster import QraftCluster, QraftSentinel
from qraft.worker import threaded_worker


def make_sentinel(threads=1, max_inflight=2, grace_period=10.0):
    """Build a QraftSentinel without starting the guard loop or real broker."""
    conf = Mock(threads=threads, grace_period=grace_period)
    conf.get_max_inflight.return_value = max_inflight
    with patch("qraft.cluster.get_conf", return_value=conf):
        return QraftSentinel(
            stop_event=Mock(),
            start_event=Mock(),
            cluster_id=uuid.uuid4(),
            broker=Mock(),
            start=False,
        )


class TestQraftSentinelThreadingConfig:
    """Threading flag derivation from qraft config."""

    def test_standard_mode_when_threads_is_one(self):
        sentinel = make_sentinel(threads=1)
        assert sentinel.threading_enabled is False
        assert sentinel.threads == 1

    def test_threaded_mode_when_threads_greater_than_one(self):
        sentinel = make_sentinel(threads=8, max_inflight=16)
        assert sentinel.threading_enabled is True
        assert sentinel.threads == 8
        assert sentinel.max_inflight == 16


class TestSpawnWorkerSelection:
    """spawn_worker must pick threaded_worker vs the standard worker based on config."""

    def test_standard_mode_spawns_django_q_worker(self):
        sentinel = make_sentinel(threads=1)
        with patch.object(sentinel, "spawn_process") as mock_spawn:
            sentinel.spawn_worker()

        args, _ = mock_spawn.call_args
        assert args[0] is q2_worker
        # Standard worker takes (worker, task_queue, result_queue, timer, timeout)
        assert len(args) == 5

    def test_threaded_mode_spawns_threaded_worker_with_thread_config(self):
        sentinel = make_sentinel(threads=4, max_inflight=8, grace_period=5.0)
        with patch.object(sentinel, "spawn_process") as mock_spawn:
            sentinel.spawn_worker()

        args, _ = mock_spawn.call_args
        assert args[0] is threaded_worker
        # (threaded_worker, task_queue, result_queue, timer, timeout, threads,
        #  max_inflight, grace_period)
        assert args[5] == 4
        assert args[6] == 8
        assert args[7] == 5.0


class TestSpawnProcess:
    """spawn_process must register worker processes with the right daemon flag."""

    def _spawn(self, sentinel, target, timer_value=1.0, daemonize=False):
        fake_process = Mock()
        with (
            patch(
                "qraft.cluster.Process", return_value=fake_process
            ) as mock_process_cls,
            patch("django_q.conf.Conf.DAEMONIZE_WORKERS", daemonize),
        ):
            result = sentinel.spawn_process(target, "task_q", "result_q", timer_value)
        return result, fake_process, mock_process_cls

    def test_standard_worker_target_registers_in_pool(self):
        sentinel = make_sentinel()
        sentinel.pool = []
        result, fake_process, _ = self._spawn(sentinel, q2_worker, daemonize=True)

        assert result is fake_process
        assert fake_process in sentinel.pool
        assert fake_process.daemon is True
        assert fake_process.timer == 1.0
        fake_process.start.assert_called_once()

    def test_threaded_worker_target_registers_in_pool(self):
        sentinel = make_sentinel()
        sentinel.pool = []
        result, fake_process, _ = self._spawn(
            sentinel, threaded_worker, daemonize=False
        )

        assert result is fake_process
        assert fake_process in sentinel.pool
        assert fake_process.daemon is False
        assert fake_process.timer == 1.0

    def test_non_worker_target_not_registered_in_pool(self):
        sentinel = make_sentinel()
        sentinel.pool = []

        def monitor_fn():
            pass

        result, fake_process, _ = self._spawn(sentinel, monitor_fn)

        assert result is fake_process
        assert fake_process not in sentinel.pool
        # Non-worker targets keep the default daemon=True set unconditionally.
        assert fake_process.daemon is True


class TestQraftClusterStart:
    """QraftCluster.start() must spawn a QraftSentinel process, not base Sentinel."""

    def test_start_spawns_qraft_sentinel(self):
        cluster = QraftCluster()
        fake_process = Mock()
        fake_process.pid = 4242
        cluster.pid = 4242
        conf = Mock(threads=1)

        with (
            patch(
                "qraft.cluster.Process", return_value=fake_process
            ) as mock_process_cls,
            patch(
                "qraft.cluster.Event", return_value=Mock(is_set=Mock(return_value=True))
            ),
            patch("qraft.cluster.get_conf", return_value=conf),
        ):
            pid = cluster.start()

        _, kwargs = mock_process_cls.call_args
        assert kwargs["target"] is QraftSentinel
        fake_process.start.assert_called_once()
        assert pid == 4242
