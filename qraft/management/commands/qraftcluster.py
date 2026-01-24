"""
Management command to start a Django-Qraft cluster with optional threading support.

Supports running multiple clusters with different configurations via ALT_CLUSTERS,
enabling mixed worker pools (standard process workers + threaded workers).
"""

import os

from django.core.management.base import BaseCommand

from qraft.cluster import QraftCluster
from qraft.conf import get_conf


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
        # Set alternative cluster_name before loading config
        cluster_name = options.get("cluster_name")
        if cluster_name:
            os.environ["Q_CLUSTER_NAME"] = cluster_name

        # Reload settings to pick up ALT_CLUSTERS config for this cluster name
        settings = get_conf()

        # Display cluster configuration
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
