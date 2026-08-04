"""
Unified demo command showcasing Qraft's features.

Usage:
    python manage.py demo hooks     # Dual-phase success/failure hooks
    python manage.py demo retry     # Retry policies with backoff
    python manage.py demo perf      # Multithreaded performance comparison
    python manage.py demo chain     # Sequential workflow execution
    python manage.py demo iter      # Parallel same-function workflow
    python manage.py demo batch     # Parallel multi-function workflow
    python manage.py demo cancel    # Workflow cancellation
"""

import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from django.core.management.base import BaseCommand
from django_q.models import Task

from qraft.batch import QraftBatch
from qraft.chain import QraftChain
from qraft.iter import QraftIter
from qraft.tasks import async_task


class Command(BaseCommand):
    help = "Showcase Qraft features: hooks, retries, performance, and workflows"

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

        # Workflow demos
        chain = subparsers.add_parser("chain", help="Chain workflow demo")
        chain.add_argument("-n", type=int, default=3, help="Number of steps")
        chain.add_argument(
            "--wait", type=int, default=0, metavar="MS",
            help="Wait for completion (milliseconds), 0=don't wait",
        )

        iter_parser = subparsers.add_parser("iter", help="Iter workflow demo")
        iter_parser.add_argument("-n", type=int, default=5, help="Number of items")
        iter_parser.add_argument(
            "--wait", type=int, default=0, metavar="MS",
            help="Wait for completion (milliseconds), 0=don't wait",
        )

        batch = subparsers.add_parser("batch", help="Batch workflow demo")
        batch.add_argument("-n", type=int, default=3, help="Number of tasks")
        batch.add_argument(
            "--wait", type=int, default=0, metavar="MS",
            help="Wait for completion (milliseconds), 0=don't wait",
        )

        # Cancel demo
        cancel = subparsers.add_parser("cancel", help="Workflow cancellation demo")

    def handle(self, *args, **options):
        scenario = options.get("scenario")

        if not scenario:
            self.stdout.write(
                self.style.ERROR("Usage: python manage.py demo <scenario>")
            )
            self.stdout.write("\nAvailable scenarios:")
            self.stdout.write("  hooks   - Dual-phase success/failure hooks")
            self.stdout.write("  retry   - Retry policies with backoff")
            self.stdout.write("  perf    - Multithreaded performance")
            self.stdout.write("  chain   - Sequential workflow execution")
            self.stdout.write("  iter    - Parallel same-function workflow")
            self.stdout.write("  batch   - Parallel multi-function workflow")
            self.stdout.write("  cancel  - Workflow cancellation")
            return

        self.stdout.write("")

        if scenario == "hooks":
            self._demo_hooks(options["n"])
        elif scenario == "retry":
            self._demo_retry(options["fail_times"], options["max_attempts"])
        elif scenario == "perf":
            self._demo_perf(options["n"], options["duration"])
        elif scenario == "chain":
            self._demo_chain(options["n"], options["wait"])
        elif scenario == "iter":
            self._demo_iter(options["n"], options["wait"])
        elif scenario == "batch":
            self._demo_batch(options["n"], options["wait"])
        elif scenario == "cancel":
            self._demo_cancel()

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

        # Queue to baseline cluster
        self.stdout.write(self.style.WARNING("\nQueueing to BASELINE cluster..."))
        baseline_ids = []
        for i in range(count):
            task_id = async_task(
                "showcase.tasks.slow_task",
                duration,
                task_name=f"baseline-{run_id}-{i:03d}",
                qraft_options={"cluster": "baseline"},
            )
            baseline_ids.append(task_id)
        self.stdout.write(f"  {count} tasks queued to baseline")

        # Queue to Qraft cluster
        self.stdout.write(self.style.WARNING("\nQueueing to QRAFT cluster..."))
        qraft_ids = []
        for i in range(count):
            task_id = async_task(
                "showcase.tasks.slow_task",
                duration,
                task_name=f"qraft-{run_id}-{i:03d}",
                qraft_options={"cluster": "qraft"},
            )
            qraft_ids.append(task_id)
        self.stdout.write(f"  {count} tasks queued to qraft")

        # Ensure both clusters are running
        self.stdout.write(
            self.style.WARNING(
                "\nMake sure BOTH clusters are running in separate terminals:\n"
                "  Terminal 1: uv run python manage.py qraftcluster\n"
                "  Terminal 2: Q_CLUSTER_NAME=qraft uv run python manage.py qraftcluster\n"
            )
        )

        self.stdout.write(self.style.SUCCESS("\nWaiting for completion...\n"))

        # Record the queue time for accurate measurement
        queue_time = time.time()
        timeout = count * duration + 30

        # Wait and measure both clusters CONCURRENTLY using threads
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

    def _demo_chain(self, count: int, wait_ms: int):
        """Demonstrate chain sequential workflow."""
        self.stdout.write(self.style.HTTP_INFO("=== CHAIN WORKFLOW DEMO ===\n"))
        self.stdout.write(
            f"Creating a {count}-step sequential workflow.\n"
            "Each step will execute only after the previous one succeeds.\n"
        )

        chain = QraftChain(
            on_success="showcase.tasks.on_success",
            success_args=["chain-complete"],
            on_failure="showcase.tasks.on_failure",
            failure_args=["chain-failed"],
            on_cancelled="showcase.tasks.on_cancelled",
        )

        for i in range(count):
            self.stdout.write(f"  Step {i + 1}: noop_task({i})")
            chain.append(
                "showcase.tasks.noop_task",
                i,
                qraft_options={"max_attempts": 2},
            )

        chain_id = chain.run()

        self.stdout.write(self.style.SUCCESS(f"\nChain {chain_id} started."))
        self.stdout.write(f"  Current step: {chain.current()}")
        self.stdout.write(f"  Status: {chain.status}")

        if wait_ms:
            self._wait_for_result(chain, wait_ms)
        else:
            self._print_cluster_reminder()

    def _demo_iter(self, count: int, wait_ms: int):
        """Demonstrate iter parallel workflow (same function, many inputs)."""
        self.stdout.write(self.style.HTTP_INFO("=== ITER WORKFLOW DEMO ===\n"))
        self.stdout.write(
            f"Creating an iter with {count} items (all execute in parallel).\n"
            "Same function applied to different inputs.\n"
        )

        iter_task = QraftIter(
            "showcase.tasks.noop_task",
            qraft_options={"max_attempts": 2},
            on_success="showcase.tasks.on_success",
            success_args=["iter-complete"],
            on_failure="showcase.tasks.on_failure",
            failure_args=["iter-partial"],
            on_cancelled="showcase.tasks.on_cancelled",
            progress_hook="showcase.tasks.on_progress",
        )

        for i in range(count):
            self.stdout.write(f"  Item {i + 1}: noop_task({i})")
            iter_task.append(i)

        iter_id = iter_task.run()

        self.stdout.write(self.style.SUCCESS(f"\nIter {iter_id} started."))
        self.stdout.write(f"  Total items: {iter_task.total_count}")
        self.stdout.write(f"  Status: {iter_task.status}")
        self.stdout.write("  Progress hook: showcase.tasks.on_progress")

        if wait_ms:
            self._wait_for_result(iter_task, wait_ms)
        else:
            self._print_cluster_reminder()

    def _demo_batch(self, count: int, wait_ms: int):
        """Demonstrate batch parallel workflow (different functions)."""
        self.stdout.write(self.style.HTTP_INFO("=== BATCH WORKFLOW DEMO ===\n"))
        self.stdout.write(
            f"Creating a batch with {count} different tasks (fork-join pattern).\n"
            "All tasks execute in parallel and join when complete.\n"
        )

        batch = QraftBatch(
            on_success="showcase.tasks.on_success",
            success_args=["batch-complete"],
            on_failure="showcase.tasks.on_failure",
            failure_args=["batch-partial"],
            on_cancelled="showcase.tasks.on_cancelled",
            progress_hook="showcase.tasks.on_progress",
        )

        # Add different tasks with different retry policies
        tasks = [
            ("showcase.tasks.slow_task", {"duration": 5.0}, {"max_attempts": 1}),
            ("showcase.tasks.flaky_task", {"fail_rate": 1.0}, {"max_attempts": 3}),
            ("showcase.tasks.noop_task", {}, {"max_attempts": 2}),
        ]

        for i in range(count):
            task_func, kwargs, qraft_opts = tasks[i % len(tasks)]
            self.stdout.write(f"  Task {i + 1}: {task_func}()")
            batch.append(task_func, **kwargs, qraft_options=qraft_opts)

        batch_id = batch.run()

        self.stdout.write(self.style.SUCCESS(f"\nBatch {batch_id} started."))
        self.stdout.write(f"  Total tasks: {batch.total_count}")
        self.stdout.write(f"  Status: {batch.status}")
        self.stdout.write("  Progress hook: showcase.tasks.on_progress")

        if wait_ms:
            self._wait_for_result(batch, wait_ms)
        else:
            self._print_cluster_reminder()

    def _demo_cancel(self):
        """Demonstrate workflow cancellation with on_cancelled hook."""
        self.stdout.write(self.style.HTTP_INFO("=== CANCELLATION DEMO ===\n"))
        self.stdout.write(
            "Creating a chain, then immediately cancelling it.\n"
            "The on_cancelled hook will fire.\n"
        )

        chain = QraftChain(
            on_success="showcase.tasks.on_success",
            success_args=["cancel-should-not-fire"],
            on_cancelled="showcase.tasks.on_cancelled",
        )

        for i in range(5):
            chain.append(
                "showcase.tasks.slow_task",
                5.0,
                qraft_options={"max_attempts": 1},
            )

        chain_id = chain.run()
        self.stdout.write(f"  Chain {chain_id} started with 5 slow steps.")
        self.stdout.write(f"  Status: {chain.status}")

        # Cancel immediately
        chain.cancel()
        self.stdout.write(f"  Status after cancel: {chain.status}")
        self.stdout.write(
            self.style.SUCCESS("\nChain cancelled. In-flight tasks will complete "
                               "but no further steps will be queued.")
        )
        self._print_cluster_reminder()

    def _wait_for_result(self, workflow, wait_ms: int):
        """Wait for workflow completion and display results."""
        self.stdout.write(
            self.style.WARNING(f"\nWaiting up to {wait_ms}ms for completion...")
        )
        try:
            result = workflow.result(wait=wait_ms)
            self.stdout.write(self.style.SUCCESS(f"\nWorkflow completed!"))
            self.stdout.write(f"  Final status: {workflow.status}")
            self.stdout.write(f"  Total results: {len(result)}")
            self.stdout.write(f"  Succeeded: {len(result.succeeded)}")
            self.stdout.write(f"  Failed: {len(result.failed_results)}")
            if result.succeeded:
                self.stdout.write("  Values:")
                for tr in result.succeeded[:5]:
                    self.stdout.write(f"    {tr.func}: {tr.result}")
                if len(result.succeeded) > 5:
                    self.stdout.write(f"    ... and {len(result.succeeded) - 5} more")
            if result.errors():
                self.stdout.write("  Errors:")
                for err in result.errors()[:3]:
                    self.stdout.write(
                        f"    {err['func']}: {err['exception']} "
                        f"({err['attempts']} attempts)"
                    )
        except TimeoutError:
            self.stdout.write(
                self.style.ERROR(
                    f"\nWorkflow did not complete within {wait_ms}ms."
                )
            )
            self.stdout.write(f"  Current status: {workflow.status}")

    def _print_cluster_reminder(self):
        """Remind user to start the cluster if not running."""
        self.stdout.write(self.style.WARNING("\nMake sure the cluster is running:"))
        self.stdout.write("  uv run python manage.py qraftcluster\n")
