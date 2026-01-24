"""
Custom cluster implementation for Django-Qraft.

Extends Django-Q2's Cluster and Sentinel to support optional multithreaded
worker execution while preserving all existing behavior through inheritance.
"""

import logging
import uuid
from multiprocessing import Event, Process, Value, current_process
from time import sleep

from django_q.cluster import Cluster, Sentinel
from django_q.conf import setproctitle
from django_q.worker import worker

from .conf import get_conf
from .worker import threaded_worker

_logger = logging.getLogger("django-q")


class QraftSentinel(Sentinel):
    """
    Sentinel subclass that supports optional multithreaded workers.

    When `qraft_conf.threads > 1`, spawns threaded workers instead of
    standard Django-Q2 workers. All other behavior is inherited from
    the base Sentinel class.
    """

    def __init__(
        self,
        stop_event,
        start_event,
        cluster_id,
        broker=None,
        timeout=None,
        start=True,
    ):
        # Load Qraft settings (respects Q_CLUSTER_NAME for ALT_CLUSTERS)
        qraft_conf = get_conf()

        # Store threading configuration before calling super().__init__
        # (which may call start() immediately)
        self.threads = qraft_conf.threads
        self.max_inflight = qraft_conf.get_max_inflight()
        self.grace_period = qraft_conf.grace_period
        self.threading_enabled = self.threads > 1

        if self.threading_enabled:
            _logger.info(
                "Qraft threading enabled: %d threads per worker, max_inflight=%d",
                self.threads,
                self.max_inflight,
            )

        # Call parent __init__ which sets up all the standard sentinel behavior
        super().__init__(
            stop_event=stop_event,
            start_event=start_event,
            cluster_id=cluster_id,
            broker=broker,
            timeout=timeout,
            start=start,
        )

    def spawn_worker(self):
        """
        Spawn a worker process.

        Overrides parent to spawn threaded workers when threading is enabled.
        Otherwise, delegates to the standard Django-Q2 worker spawning.
        """
        if self.threading_enabled:
            self.spawn_process(
                threaded_worker,
                self.task_queue,
                self.result_queue,
                Value("f", -1),
                self.timeout,
                self.threads,
                self.max_inflight,
                self.grace_period,
            )
        else:
            # Use standard Django-Q2 worker
            self.spawn_process(
                worker,
                self.task_queue,
                self.result_queue,
                Value("f", -1),
                self.timeout,
            )

    def spawn_process(self, target, *args) -> Process:
        """
        Override spawn_process to handle threaded_worker target detection.

        The parent class checks `target == worker` to set daemon mode and timer.
        We extend this to also recognize `threaded_worker`.
        """
        p = Process(target=target, args=args, name=f"Process-{uuid.uuid4().hex}")
        p.daemon = True

        # Check for both standard and threaded worker targets
        if target in (worker, threaded_worker):
            from django_q.conf import Conf

            p.daemon = Conf.DAEMONIZE_WORKERS
            p.timer = args[2]  # timer is the 3rd argument for both worker types
            self.pool.append(p)

        p.start()
        return p


class QraftCluster(Cluster):
    """
    Qraft-enhanced Cluster that uses QraftSentinel for optional threading support.

    Drop-in replacement for Django-Q2's Cluster. When threading is disabled
    (threads=1), behavior is identical to the standard cluster.
    """

    def start(self) -> int:
        """
        Start the Qraft cluster with optional threading support.

        Overrides parent to use QraftSentinel instead of standard Sentinel.
        """
        if setproctitle:
            setproctitle.setproctitle(f"qcluster {current_process().name} {self.name}")

        # Start QraftSentinel instead of standard Sentinel
        self.stop_event = Event()
        self.start_event = Event()
        self.sentinel = Process(
            target=QraftSentinel,
            name=f"Process-{uuid.uuid4().hex}",
            args=(
                self.stop_event,
                self.start_event,
                self.cluster_id,
                self.broker,
                self.timeout,
            ),
        )
        self.sentinel.start()

        # Log with threading info if enabled
        qraft_conf = get_conf()
        threading_info = ""
        if qraft_conf.threads > 1:
            threading_info = f" (threaded: {qraft_conf.threads}t per worker)"

        _logger.info("Q Cluster %s starting.%s", self.name, threading_info)

        while not self.start_event.is_set():
            sleep(0.1)

        return self.pid
