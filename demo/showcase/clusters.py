"""
Cluster lifecycle for the verification suite.

The suite boots the worker clusters it needs, waits until each one really
drains its own lane, and stops them again. Nobody opens three terminals.

A profile name is passed straight through as `Q_CLUSTER_NAME`. Both django-q2
and qraft read that variable and apply their own `ALT_CLUSTERS` entry, so the
name selects the broker lane and the worker settings together.
"""

import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / ".demo-logs"

# Every profile the scenarios can ask for. Names must exist in both
# Q_CLUSTER["ALT_CLUSTERS"] and QRAFT_CLUSTER["ALT_CLUSTERS"], except
# "default", which is the base configuration.
PROFILES = {
    "default": "3 process workers, async hooks",
    "baseline": "2 process workers, for the threading comparison",
    "threaded": "2 workers x 8 threads, for the threading comparison",
    "synchooks": "1 worker, sync_hooks=True",
    "throttle-a": "2 workers sharing a rate bucket with throttle-b",
    "throttle-b": "2 workers sharing a rate bucket with throttle-a",
    "lanes": "1 worker, drains high then default then low",
    "soak": "4 process workers, 30 min timeout, for long fake-API tasks",
}

# Seconds to wait for a cluster to execute its first probe task.
READY_TIMEOUT = 90.0
# Seconds to wait for a graceful stop before escalating to SIGKILL.
STOP_TIMEOUT = 25.0


def profile_summaries() -> list[dict]:
    """
    Worker process count and threads-per-worker for each profile.

    Reuses qraft's own ALT_CLUSTERS merge (the same one a `qraftcluster`
    process applies to itself) rather than re-deriving the precedence rules,
    so the dashboard cannot drift from what a cluster actually runs with.
    """
    from django.conf import settings

    from qraft.conf import _merge_alt_cluster

    return [
        {
            "name": name,
            "why": why,
            "workers": _merge_alt_cluster(settings.Q_CLUSTER, name).get("workers", 1),
            "threads": _merge_alt_cluster(settings.QRAFT_CLUSTER, name).get(
                "threads", 1
            ),
        }
        for name, why in PROFILES.items()
    ]


class ClusterManager:
    """Starts, probes and stops `manage.py qraftcluster` child processes."""

    def __init__(self, log=None):
        self._lock = threading.RLock()
        self._procs: dict[str, subprocess.Popen] = {}
        self._log = log or (lambda message: None)
        LOG_DIR.mkdir(exist_ok=True)

    # --- lifecycle ---

    def start(self, name: str) -> None:
        with self._lock:
            self._start(name)

    def _start(self, name: str) -> None:
        if name in self._procs and self._procs[name].poll() is None:
            return
        if name not in PROFILES:
            raise ValueError(f"Unknown cluster profile {name!r}")

        env = {**os.environ, "Q_CLUSTER_NAME": name, "PYTHONUNBUFFERED": "1"}
        log_path = LOG_DIR / f"{name}.log"
        handle = open(log_path, "w")
        proc = subprocess.Popen(
            [sys.executable, "manage.py", "qraftcluster"],
            cwd=BASE_DIR,
            env=env,
            stdout=handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        handle.close()
        self._procs[name] = proc
        self._log(f"cluster {name} starting (pid {proc.pid}, log {log_path.name})")

        if not self._probe(name):
            tail = self._tail(name)
            self.stop(name)
            raise RuntimeError(
                f"Cluster {name!r} did not execute a probe task within "
                f"{READY_TIMEOUT:.0f}s. Last log lines:\n{tail}"
            )
        self._log(f"cluster {name} ready")

    def ensure(self, names) -> None:
        for name in names:
            self.start(name)

    def stop(self, name: str) -> None:
        with self._lock:
            self._stop(name)

    def _stop(self, name: str) -> None:
        proc = self._procs.pop(name, None)
        if proc is None or proc.poll() is not None:
            return
        self._log(f"cluster {name} stopping")
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=STOP_TIMEOUT)
        except subprocess.TimeoutExpired:
            self._log(f"cluster {name} ignored SIGTERM, killing process group")
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.wait(timeout=10)

    def stop_all(self) -> None:
        for name in list(self._procs):
            self.stop(name)

    def running(self) -> list[str]:
        # Polling the UI must not wait for start()'s readiness probe.
        return [
            name for name, proc in tuple(self._procs.items()) if proc.poll() is None
        ]

    # --- probing ---

    def _probe(self, name: str) -> bool:
        """
        Wait until this cluster completes a task from its own lane.

        A process that is merely alive proves nothing: it may be pointed at
        the wrong lane, or stuck on a database it cannot reach. Executing a
        real task is the only honest readiness signal.
        """
        from django_q.models import Task as Q2Task
        from django_q.tasks import async_task as q2_async_task

        task_id = q2_async_task(
            "showcase.tasks.ping",
            f"probe-{name}",
            cluster=name,
            hook=None,
            ack_failure=True,
        )
        deadline = time.monotonic() + READY_TIMEOUT
        while time.monotonic() < deadline:
            if Q2Task.objects.filter(id=task_id).exists():
                return True
            if self._procs[name].poll() is not None:
                return False
            time.sleep(0.3)
        return False

    def _tail(self, name: str, lines: int = 25) -> str:
        path = LOG_DIR / f"{name}.log"
        if not path.exists():
            return "(no log)"
        return "\n".join(path.read_text(errors="replace").splitlines()[-lines:])

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.stop_all()
        return False
