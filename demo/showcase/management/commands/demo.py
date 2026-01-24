"""
Unified demo command showcasing Qraft's features.

Usage:
    python manage.py demo hooks     # Dual-phase success/failure hooks
    python manage.py demo retry     # Retry policies with backoff
    python manage.py demo perf      # Multithreaded performance comparison
"""

import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from django.core.management.base import BaseCommand
from django_q.models import Task

from qraft.models import QraftTask
from qraft.tasks import async_task


class Command(BaseCommand):
    help = "Showcase Qraft features: hooks, retries, and performance"

    def add_arguments(self, parser):
        subparsers = parser.add_subparsers(dest="scenario", help="Demo scenario")

        # Hooks demo
        hooks = subparsers.add_parser("hooks", help="Dual-phase hooks demo")
        hooks.add_argument("-n", type=int, default=3, help="Number of tasks")

        # Retry demo
        retry = subparsers.add_parser("retry", help="Retry policies demo")
        retry.add_argument(
            "--fail-times", type=int, default=2, help="Failures before success"
        )
        retry.add_argument(
            "--max-attempts", type=int, default=4, help="Max retry attempts"
        )

        # Performance demo
        perf = subparsers.add_parser("perf", help="Threading performance demo")
        perf.add_argument("-n", type=int, default=20, help="Number of tasks")
        perf.add_argument(
            "--duration", type=float, default=1.0, help="Task duration (seconds)"
        )

    def handle(self, *args, **options):
        scenario = options.get("scenario")

        if not scenario:
            self.stdout.write(
                self.style.ERROR("Usage: python manage.py demo <scenario>")
            )
            self.stdout.write("\nAvailable scenarios:")
            self.stdout.write("  hooks  - Dual-phase success/failure hooks")
            self.stdout.write("  retry  - Retry policies with backoff")
            self.stdout.write("  perf   - Multithreaded performance")
            return

        self.stdout.write("")

        if scenario == "hooks":
            self._demo_hooks(options["n"])
        elif scenario == "retry":
            self._demo_retry(options["fail_times"], options["max_attempts"])
        elif scenario == "perf":
            self._demo_perf(options["n"], options["duration"])

    def _demo_hooks(self, count: int):
        """Demonstrate dual-phase hooks with mixed success/failure tasks."""
        self.stdout.write(self.style.HTTP_INFO("=== DUAL-PHASE HOOKS DEMO ===\n"))
        self.stdout.write(
            "Queueing tasks with 50% failure rate.\n"
            "Watch for SUCCESS HOOK and FAILURE HOOK messages in the cluster logs.\n"
        )

        for i in range(count):
            task_id = f"hooks-{uuid.uuid4().hex[:6]}"

            async_task(
                "showcase.tasks.flaky_task",
                fail_rate=0.5,
                qraft_options={
                    "success_hook": "showcase.tasks.on_success",
                    "success_args": [task_id],
                    "failure_hook": "showcase.tasks.on_failure",
                    "failure_args": [task_id],
                },
            )

            self.stdout.write(f"  Queued: {task_id}")

        self.stdout.write(self.style.SUCCESS(f"\n{count} tasks queued."))
        self._print_cluster_reminder()

    def _demo_retry(self, fail_times: int, max_attempts: int):
        """Demonstrate retry policies with countdown tasks."""
        self.stdout.write(self.style.HTTP_INFO("=== RETRY POLICIES DEMO ===\n"))

        will_succeed = fail_times < max_attempts
        outcome = "SUCCEED" if will_succeed else "EXHAUST retries"

        self.stdout.write(
            f"Task will fail {fail_times} times, max {max_attempts} attempts.\n"
            f"Expected outcome: {outcome}\n"
        )

        task_id = f"retry-{uuid.uuid4().hex[:6]}"

        async_task(
            "showcase.tasks.countdown_task",
            task_id,
            fail_times=fail_times,
            qraft_options={
                "max_attempts": max_attempts,
                "base_delay": 2.0,
                "backoff_strategy": "exponential",
                "jitter": False,
                "success_hook": "showcase.tasks.on_success",
                "success_args": [task_id],
                "failure_hook": "showcase.tasks.on_failure",
                "failure_args": [task_id],
            },
        )

        self.stdout.write(f"\n  Queued: {task_id}")
        self.stdout.write(f"  Retry delays: 2s, 4s, 8s... (exponential backoff)\n")
        self.stdout.write(self.style.SUCCESS("Task queued."))
        self._print_cluster_reminder()

    def _demo_perf(self, count: int, duration: float):
        """
        Side-by-side comparison: baseline Django-Q2 vs Qraft threading.

        Queues identical workloads to two clusters:
        - baseline: Standard Django-Q2 (2 workers, no threading)
        - qraft: Qraft-enhanced (2 workers x 4 threads = 8 concurrent tasks)
        """
        from django.utils import timezone

        self.stdout.write(self.style.HTTP_INFO("=== PERFORMANCE COMPARISON ===\n"))
        self.stdout.write(
            f"Queueing {count} tasks (each sleeps {duration}s) to BOTH clusters:\n"
            f"  - baseline: Standard Django-Q2 (2 workers)\n"
            f"  - qraft:    Qraft threading (2 workers x 4 threads)\n"
        )

        run_id = uuid.uuid4().hex[:6]

        # Capture start time BEFORE queueing any tasks
        start_dt = timezone.now()

        # Queue to baseline cluster using Qraft's async_task
        # This creates QraftTask entries for tracking
        self.stdout.write(self.style.WARNING("\nQueueing to BASELINE cluster..."))
        baseline_ids = []
        for i in range(count):
            task_id = async_task(
                "showcase.tasks.slow_task",
                duration,
                task_name=f"baseline-{run_id}-{i:03d}",
                cluster="baseline",  # Routes to Q_CLUSTER
            )
            baseline_ids.append(task_id)
        self.stdout.write(f"  {count} tasks queued to baseline")

        # Queue to Qraft cluster using Qraft's async_task
        # This creates QraftTask entries for tracking
        self.stdout.write(self.style.WARNING("\nQueueing to QRAFT cluster..."))
        qraft_ids = []
        for i in range(count):
            task_id = async_task(
                "showcase.tasks.slow_task",
                duration,
                task_name=f"qraft-{run_id}-{i:03d}",
                cluster="qraft",  # Routes to ALT_CLUSTERS["qraft"]
            )
            qraft_ids.append(task_id)
        self.stdout.write(f"  {count} tasks queued to qraft")

        # Ensure both clusters are running
        self.stdout.write(
            self.style.WARNING(
                "\nMake sure BOTH clusters are running in separate terminals:\n"
                "  Terminal 1: python manage.py qraftcluster\n"
                "  Terminal 2: Q_CLUSTER_NAME=qraft python manage.py qraftcluster\n"
            )
        )

        self.stdout.write(self.style.SUCCESS("\nWaiting for completion...\n"))

        # Record the queue time for accurate measurement
        queue_time = time.time()
        timeout = count * duration + 30

        # Wait and measure both clusters CONCURRENTLY using threads
        # This ensures accurate timing for both clusters
        results = {}
        output_lock = Lock()

        def measure_cluster(label: str, task_ids: list[str]):
            """Measure completion time for a cluster's tasks."""
            import django

            django.db.connection.close()  # Close connection before thread starts

            deadline = queue_time + timeout
            total = len(task_ids)

            while time.time() < deadline:
                completed = Task.objects.filter(
                    id__in=task_ids, stopped__isnull=False
                ).count()
                if completed >= total:
                    elapsed = time.time() - queue_time
                    with output_lock:
                        self.stdout.write(
                            f"  {label}: {completed}/{total} completed in {elapsed:.2f}s"
                        )
                    results[label] = elapsed
                    return
                time.sleep(0.5)

            # Timeout reached
            completed = Task.objects.filter(
                id__in=task_ids, stopped__isnull=False
            ).count()
            with output_lock:
                self.stdout.write(
                    self.style.WARNING(
                        f"  {label}: Only {completed}/{total} completed (timeout)"
                    )
                )
            results[label] = 0.0

        # Run both measurements concurrently
        with ThreadPoolExecutor(max_workers=2) as executor:
            executor.submit(measure_cluster, "BASELINE", baseline_ids)
            executor.submit(measure_cluster, "QRAFT", qraft_ids)

        baseline_time = results.get("BASELINE", 0.0)
        qraft_time = results.get("QRAFT", 0.0)

        # Show results
        self.stdout.write(self.style.HTTP_INFO("\n=== RESULTS ==="))
        self.stdout.write(f"  Baseline (no threading): {baseline_time:.2f}s")
        self.stdout.write(f"  Qraft (4 threads):       {qraft_time:.2f}s")

        if baseline_time > 0 and qraft_time > 0:
            speedup = baseline_time / qraft_time
            self.stdout.write(self.style.SUCCESS(f"  Speedup: {speedup:.2f}x\n"))
        else:
            self.stdout.write(
                self.style.ERROR(
                    "\n  Could not calculate speedup - check cluster logs\n"
                )
            )

    def _wait_and_measure(
        self, label: str, task_ids: list[str], start_dt, timeout: float
    ) -> float:
        """
        Wait for tasks to complete and return elapsed time.

        Args:
            label: Display label for progress messages
            task_ids: List of task IDs to monitor
            start_dt: Django timezone-aware datetime when benchmark started (unused, kept for API compatibility)
            timeout: Maximum seconds to wait

        Returns:
            Elapsed seconds from start to completion, or 0.0 if timeout reached.
        """
        deadline = time.time() + timeout
        start = time.time()
        total = len(task_ids)

        while time.time() < deadline:
            # Count completed tasks using their specific IDs
            # Note: No time-based filter needed since task_ids are unique to this run
            completed = Task.objects.filter(
                id__in=task_ids,
                stopped__isnull=False,  # Has completed
            ).count()
            if completed >= total:
                elapsed = time.time() - start
                self.stdout.write(
                    f"  {label}: {completed}/{total} completed in {elapsed:.2f}s"
                )
                return elapsed
            time.sleep(0.5)

        # Timeout reached
        completed = Task.objects.filter(id__in=task_ids, stopped__isnull=False).count()
        elapsed = time.time() - start
        self.stdout.write(
            self.style.WARNING(
                f"  {label}: Only {completed}/{total} completed (timeout)"
            )
        )
        return 0.0

    def _print_cluster_reminder(self):
        """Remind user to start the cluster if not running."""
        self.stdout.write(self.style.WARNING("\nMake sure the cluster is running:"))
        self.stdout.write("  python manage.py qraftcluster\n")
