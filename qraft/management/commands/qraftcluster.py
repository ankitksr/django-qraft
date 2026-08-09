"""
Management command to start a Django-Qraft cluster with optional threading support.

Supports running multiple clusters with different configurations via ALT_CLUSTERS,
enabling mixed worker pools (standard process workers + threaded workers).
"""

import os
import sys

from django.conf import settings as django_settings
from django.core.management.base import BaseCommand, CommandError

from qraft.cluster import QraftCluster
from qraft.conf import get_conf

ENV_VAR = "Q_CLUSTER_NAME"


def _alt_cluster_names(config: object) -> set[str]:
    """Names declared under ALT_CLUSTERS in a cluster settings dict."""
    if not isinstance(config, dict):
        return set()
    alt = config.get("ALT_CLUSTERS")
    return set(alt) if isinstance(alt, dict) else set()


class Command(BaseCommand):
    help = "Starts a Django-Qraft Cluster with optional multithreaded workers."

    def add_arguments(self, parser):
        parser.add_argument(
            "--run-once",
            action="store_true",
            dest="run_once",
            default=False,
            help="Run once and then stop.",
        )
        parser.add_argument(
            "-n",
            "--name",
            dest="cluster_name",
            default=None,
            help=(
                "Set alternative cluster name to select from ALT_CLUSTERS config. "
                "This enables running mixed worker pools (process + threaded). "
                "Can also be set via Q_CLUSTER_NAME environment variable."
            ),
        )

    def handle(self, *args, **options):
        cluster_name = options.get("cluster_name")
        if cluster_name and os.environ.get(ENV_VAR) != cluster_name:
            # django_q.conf.Conf is a module-level object built at import
            # time, which is already done by the time handle() runs. Setting
            # the variable here would give this process Qraft's ALT_CLUSTERS
            # threading config while Django-Q2 still drained the *default*
            # queue, stranding anything routed with cluster=<name>. Re-exec
            # so both configs are read from the same name.
            self._reexec(cluster_name)
            return  # unreachable: execve replaces the process image

        settings = get_conf()

        if cluster_name:
            self.stdout.write(f"Starting cluster: {cluster_name}")

        if settings.threads > 1:
            self.stdout.write(
                self.style.SUCCESS(
                    f"Worker mode: threaded ({settings.threads} threads per worker, "
                    f"max_inflight={settings.get_max_inflight()}, "
                    f"grace_period={settings.grace_period}s)"
                )
            )
        else:
            self.stdout.write(
                self.style.SUCCESS("Worker mode: standard (process-based)")
            )

        q = QraftCluster()
        q.start()

        if options.get("run_once", False):
            q.stop()

    def _reexec(self, cluster_name: str) -> None:
        """
        Restart this process with Q_CLUSTER_NAME set, so imports run fresh.

        The re-executed process sees the variable already matching --name and
        so takes the normal path: the guard in handle() is what makes this
        terminate rather than loop, and it holds even though --name is kept
        in argv (which is what lets the second process still report its name).
        """
        self._validate_cluster_name(cluster_name)

        self.stdout.write(f"Re-executing with {ENV_VAR}={cluster_name}")
        sys.stdout.flush()

        # sys.argv[0] is a Python file for every supported entry point
        # (manage.py, the django-admin console script, django/__main__.py),
        # so one form covers them all. Passing the environment explicitly
        # keeps the no-loop guarantee independent of putenv side effects.
        os.execve(
            sys.executable,
            [sys.executable, *sys.argv],
            {**os.environ, ENV_VAR: cluster_name},
        )

    def _validate_cluster_name(self, cluster_name: str) -> None:
        """
        Reject an unusable name before re-exec, not after.

        A typo would otherwise surface in the second process, where it either
        crashes inside django_q.conf or silently falls back to the default
        queue - both far harder to read than an error here.
        """
        q_cluster = getattr(django_settings, "Q_CLUSTER", {})
        if not isinstance(q_cluster, dict):
            q_cluster = {}
        qraft_cluster = getattr(django_settings, "QRAFT_CLUSTER", {})

        # Django-Q2 treats a name matching its own cluster as the default
        # cluster and never touches ALT_CLUSTERS, so nothing to check.
        if cluster_name in (q_cluster.get("name"), q_cluster.get("cluster_name")):
            return

        known = _alt_cluster_names(q_cluster) | _alt_cluster_names(qraft_cluster)
        if cluster_name not in known:
            listed = ", ".join(sorted(known)) or "none declared"
            raise CommandError(
                f"Unknown cluster name '{cluster_name}'. "
                f"Declare it under ALT_CLUSTERS in Q_CLUSTER or QRAFT_CLUSTER. "
                f"Known names: {listed}"
            )

        if "ALT_CLUSTERS" not in q_cluster:
            # django_q.conf.Conf does a bare conf.pop("ALT_CLUSTERS") whenever
            # Q_CLUSTER_NAME names a non-default cluster, so a Q_CLUSTER
            # without the key raises KeyError on import in the new process.
            raise CommandError(
                f"Cluster '{cluster_name}' is declared in QRAFT_CLUSTER but "
                f"Q_CLUSTER has no ALT_CLUSTERS key. Django-Q2 requires it to "
                f"select a cluster. Add ALT_CLUSTERS = {{'{cluster_name}': {{}}}} "
                f"to Q_CLUSTER."
            )
