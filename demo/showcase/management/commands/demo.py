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
    python manage.py demo approval  # Approval-gated chain step
    python manage.py demo ratelimit # Token bucket + rate-limit-aware retries
    python manage.py demo usage     # Token/cost accounting
    python manage.py demo idempotent# Idempotency keys
    python manage.py demo reaper    # Orphan detection and requeue
"""

import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from django.core.management.base import BaseCommand
from django_q.models import Schedule, Task

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
            "--wait",
            type=int,
            default=0,
            metavar="MS",
            help="Wait for completion (milliseconds), 0=don't wait",
        )

        iter_parser = subparsers.add_parser("iter", help="Iter workflow demo")
        iter_parser.add_argument("-n", type=int, default=5, help="Number of items")
        iter_parser.add_argument(
            "--wait",
            type=int,
            default=0,
            metavar="MS",
            help="Wait for completion (milliseconds), 0=don't wait",
        )

        batch = subparsers.add_parser("batch", help="Batch workflow demo")
        batch.add_argument("-n", type=int, default=3, help="Number of tasks")
        batch.add_argument(
            "--wait",
            type=int,
            default=0,
            metavar="MS",
            help="Wait for completion (milliseconds), 0=don't wait",
        )

        # Cancel demo
        subparsers.add_parser("cancel", help="Workflow cancellation demo")

        # AI-workload demos
        subparsers.add_parser("approval", help="Approval-gated chain step demo")
        subparsers.add_parser("ratelimit", help="Token bucket + rate-limit retry demo")
        subparsers.add_parser("usage", help="Token/cost accounting demo")
        subparsers.add_parser("idempotent", help="Idempotency key demo")

        reaper = subparsers.add_parser("reaper", help="Orphan reaper demo")
        reaper.add_argument(
            "--stale-after",
            type=float,
            default=1.0,
            help="Seconds before an unresolved attempt counts as orphaned",
        )

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
            self.stdout.write("  approval   - Approval-gated chain step")
            self.stdout.write("  ratelimit  - Token bucket + rate-limit-aware retries")
            self.stdout.write("  usage      - Token/cost accounting")
            self.stdout.write("  idempotent - Idempotency keys")
            self.stdout.write("  reaper     - Orphan detection and requeue")
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
        elif scenario == "approval":
            self._demo_approval()
        elif scenario == "ratelimit":
            self._demo_ratelimit()
        elif scenario == "usage":
            self._demo_usage()
        elif scenario == "idempotent":
            self._demo_idempotent()
        elif scenario == "reaper":
            self._demo_reaper(options["stale_after"])

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
        self.stdout.write("  Retry delays: 2s, 4s, 8s... (exponential backoff)\n")
        self.stdout.write(self.style.SUCCESS("Task queued."))
        self._print_cluster_reminder()

    def _demo_perf(self, count: int, duration: float):
        """
        Side-by-side comparison: baseline Django-Q2 vs Qraft threading.

        Queues identical workloads to two clusters:
        - baseline: Standard Django-Q2 (2 workers, no threading)
        - qraft: Qraft-enhanced (2 workers x 4 threads = 8 concurrent tasks)
        """
        self.stdout.write(self.style.HTTP_INFO("=== PERFORMANCE COMPARISON ===\n"))
        self.stdout.write(
            f"Queueing {count} tasks (each sleeps {duration}s) to BOTH clusters:\n"
            f"  - baseline: Standard Django-Q2 (2 workers)\n"
            f"  - qraft:    Qraft threading (2 workers x 4 threads)\n"
        )

        run_id = uuid.uuid4().hex[:6]

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
                "  Terminal 2: Q_CLUSTER_NAME=qraft uv run python manage.py "
                "qraftcluster\n"
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
                            f"  {label}: {completed}/{total} completed "
                            f"in {elapsed:.2f}s"
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
            self.style.SUCCESS(
                "\nChain cancelled. In-flight tasks will complete "
                "but no further steps will be queued."
            )
        )
        self._print_cluster_reminder()

    def _demo_approval(self):
        """Demonstrate a chain step that parks for human approval."""
        from qraft.models import WorkflowStatus

        self.stdout.write(self.style.HTTP_INFO("=== APPROVAL STEP DEMO ===\n"))
        self.stdout.write(
            "A two-step chain. Step 2 is gated: the chain parks in\n"
            "WAITING_APPROVAL at zero compute until approve() is called.\n"
        )

        document = f"Mockco-brief-{uuid.uuid4().hex[:6]}"
        chain = QraftChain(
            on_success="showcase.tasks.on_success",
            success_args=["approval-chain-complete"],
            on_cancelled="showcase.tasks.on_cancelled",
        )
        chain.append("showcase.tasks.review_task", document)
        chain.append("showcase.tasks.publish_task", document, requires_approval=True)

        chain_id = chain.run()
        self.stdout.write(f"  Chain {chain_id} started (document={document})")

        parked = self._poll(
            lambda: chain.status == WorkflowStatus.WAITING_APPROVAL, timeout=30
        )
        if not parked:
            self.stdout.write(
                self.style.ERROR(f"  Chain never parked (status: {chain.status})")
            )
            self._print_cluster_reminder()
            return

        self.stdout.write(self.style.WARNING(f"  Status: {chain.status}"))
        self.stdout.write(f"  Parked before step: {chain.current()}")

        # result() returns immediately on a parked chain rather than blocking
        # until the timeout: WAITING_APPROVAL won't clear without an external
        # decision.
        partial = chain.result(wait=5000)
        self.stdout.write(f"  Results so far: {len(partial)} step(s)")
        for task_result in partial.succeeded:
            self.stdout.write(f"    {task_result.func}: {task_result.result}")

        self.stdout.write(self.style.HTTP_INFO("\n  Approving..."))
        chain.approve()

        self._wait_for_result(chain, 30000)

    def _demo_ratelimit(self):
        """Demonstrate DB token-bucket backpressure and rate-limit-aware retries."""
        from qraft.models import QraftTaskAttempt, RateBucket
        from showcase.tasks import RATE_BUCKET_KEY

        self.stdout.write(self.style.HTTP_INFO("=== RATE LIMIT DEMO ===\n"))
        self.stdout.write(
            f"Bucket '{RATE_BUCKET_KEY}' holds 1 token and refills at 0.01/s.\n"
            "Task 1 takes the token. Task 2 finds the bucket empty, raises\n"
            "RateLimited, and is rescheduled with rate-limit backoff.\n"
        )

        # Reset the bucket so the scenario is repeatable.
        RateBucket.objects.filter(key=RATE_BUCKET_KEY).delete()

        run_id = uuid.uuid4().hex[:6]
        retry_options = {
            "max_attempts": 3,
            "base_delay": 3.0,
            "backoff_strategy": "exponential",
            "jitter": False,
        }

        first_id = async_task(
            "showcase.tasks.throttled_task",
            f"{run_id}-first",
            qraft_options=retry_options,
        )
        self.stdout.write(f"\n  Queued task 1 (q2: {first_id})")

        if not self._poll(lambda: self._attempt_resolved(first_id), timeout=30):
            self.stdout.write(self.style.ERROR("  Task 1 never completed"))
            self._print_cluster_reminder()
            return

        first = QraftTaskAttempt.objects.get(q2_task_id=first_id)
        self.stdout.write(
            self.style.SUCCESS(f"  Task 1 outcome: success={first.success}")
        )

        second_id = async_task(
            "showcase.tasks.throttled_task",
            f"{run_id}-second",
            qraft_options=retry_options,
        )
        self.stdout.write(f"\n  Queued task 2 (q2: {second_id})")

        if not self._poll(lambda: self._attempt_resolved(second_id), timeout=30):
            self.stdout.write(self.style.ERROR("  Task 2 never completed"))
            return

        second = QraftTaskAttempt.objects.select_related("qraft_task").get(
            q2_task_id=second_id
        )
        qraft_task = second.qraft_task
        self.stdout.write(
            self.style.WARNING(
                f"  Task 2 outcome: success={second.success} "
                f"exception={second.exception_class}"
            )
        )

        # The retry is a django_q Schedule row named qraft_retry:<id>:<attempt>.
        self._poll(
            lambda: Schedule.objects.filter(
                name__startswith=f"qraft_retry:{qraft_task.id}:"
            ).exists(),
            timeout=10,
        )
        for schedule in Schedule.objects.filter(
            name__startswith=f"qraft_retry:{qraft_task.id}:"
        ):
            self.stdout.write(
                f"  Rescheduled: {schedule.name} -> next_run={schedule.next_run}"
            )

        qraft_task.refresh_from_db()
        self.stdout.write(f"  QraftTask status: {qraft_task.status}")
        self.stdout.write(
            "\n  RateLimited is in RetryPolicy.RATE_LIMIT_EXCEPTIONS, so it "
            "backs off\n  exponentially even under a restrictive "
            "retry_exceptions allowlist."
        )

    def _demo_usage(self):
        """Demonstrate per-attempt token/cost accounting and progress reporting."""
        from qraft.context import aggregate_usage
        from qraft.models import QraftTaskAttempt

        self.stdout.write(self.style.HTTP_INFO("=== USAGE ACCOUNTING DEMO ===\n"))
        self.stdout.write(
            "llm_task makes 3 mock-llm calls, recording usage after each one.\n"
            "record_usage() accumulates numeric fields on the attempt.\n"
        )

        q2_task_id = async_task(
            "showcase.tasks.llm_task",
            "summarize the Mockco filing",
            calls=3,
        )
        self.stdout.write(f"\n  Queued (q2: {q2_task_id})")

        if not self._poll(lambda: self._attempt_resolved(q2_task_id), timeout=30):
            self.stdout.write(self.style.ERROR("  Task never completed"))
            self._print_cluster_reminder()
            return

        attempt = QraftTaskAttempt.objects.select_related("qraft_task").get(
            q2_task_id=q2_task_id
        )
        qraft_task = attempt.qraft_task

        self.stdout.write(self.style.SUCCESS("\n  Attempt usage:"))
        for key, value in sorted((attempt.usage or {}).items()):
            self.stdout.write(f"    {key}: {value}")

        self.stdout.write("\n  aggregate_usage(qraft_task):")
        for key, value in sorted(aggregate_usage(qraft_task).items()):
            self.stdout.write(f"    {key}: {value}")

        self.stdout.write(f"\n  Last reported progress: {qraft_task.progress}")

    def _demo_idempotent(self):
        """Demonstrate idempotency-key deduplication."""
        from qraft.models import QraftTask

        self.stdout.write(self.style.HTTP_INFO("=== IDEMPOTENCY KEY DEMO ===\n"))
        self.stdout.write(
            "charge_task is enqueued twice under one key. The second call is a\n"
            "no-op that returns the first call's task id - Mockco is charged once.\n"
        )

        key = f"charge-Mockco-{uuid.uuid4().hex[:8]}"
        options = {"idempotency_key": key}

        first_id = async_task(
            "showcase.tasks.charge_task", "Mockco", 42.50, qraft_options=options
        )
        second_id = async_task(
            "showcase.tasks.charge_task", "Mockco", 42.50, qraft_options=options
        )

        self.stdout.write(f"\n  Key: {key}")
        self.stdout.write(f"  1st enqueue -> {first_id}")
        self.stdout.write(f"  2nd enqueue -> {second_id}")
        self.stdout.write(f"  Same task id: {first_id == second_id}")
        self.stdout.write(
            self.style.SUCCESS(
                f"  QraftTask rows for this key: "
                f"{QraftTask.objects.filter(idempotency_key=key).count()}"
            )
        )
        self.stdout.write(
            "\n  The key is a permanent dedupe, not a 'retry if failed' signal.\n"
            "  Use a new key to run again."
        )

    def _demo_reaper(self, stale_after: float):
        """Demonstrate orphan detection for a task whose worker died mid-run."""
        from datetime import timedelta

        from django.utils import timezone

        from qraft.models import QraftTask, QraftTaskAttempt, TaskStatus
        from qraft.reaper import reap_orphans
        from qraft.retry import RetryPolicy

        self.stdout.write(self.style.HTTP_INFO("=== ORPHAN REAPER DEMO ===\n"))
        self.stdout.write(
            "Simulating a worker killed mid-task: a RUNNING attempt whose\n"
            "Django-Q2 Task row never appeared. The reaper resolves it through\n"
            "the normal retry path instead of leaving it stuck forever.\n"
        )

        policy = RetryPolicy(
            max_attempts=2, base_delay=1.0, backoff_strategy="fixed", jitter=False
        )
        qraft_task = QraftTask.objects.create(
            func="showcase.tasks.noop_task",
            task_args=["reaped"],
            task_kwargs={},
            retry_policy=policy.to_dict(),
            status=TaskStatus.RUNNING,
        )
        attempt = QraftTaskAttempt.objects.create(
            qraft_task=qraft_task,
            attempt_number=1,
            q2_task_id=uuid.uuid4().hex[:32],
        )
        # date_created is auto_now_add, so backdating needs a queryset update.
        QraftTaskAttempt.objects.filter(id=attempt.id).update(
            date_created=timezone.now() - timedelta(seconds=stale_after + 60)
        )

        self.stdout.write(f"\n  Orphan QraftTask {qraft_task.id}")
        self.stdout.write(f"  Attempt 1 q2_task_id={attempt.q2_task_id} (no Q2 row)")
        self.stdout.write(f"  Status before reap: {qraft_task.status}")

        reaped = reap_orphans(stale_after=stale_after)
        self.stdout.write(self.style.WARNING(f"\n  reap_orphans() -> {reaped}"))

        attempt.refresh_from_db()
        qraft_task.refresh_from_db()
        self.stdout.write(
            f"  Attempt 1: success={attempt.success} "
            f"exception={attempt.exception_class}"
        )
        self.stdout.write(f"  Status after reap: {qraft_task.status}")

        self.stdout.write("\n  Waiting for the requeued attempt...")
        completed = self._poll(
            lambda: self._task_status(qraft_task.id)
            in (TaskStatus.SUCCEEDED, TaskStatus.EXHAUSTED),
            timeout=60,
        )
        qraft_task.refresh_from_db()
        if completed:
            self.stdout.write(
                self.style.SUCCESS(f"  Final status: {qraft_task.status}")
            )
        else:
            self.stdout.write(
                self.style.ERROR(f"  Still {qraft_task.status} after 60s")
            )
            self._print_cluster_reminder()

        for retry_attempt in qraft_task.attempts.order_by("attempt_number"):
            self.stdout.write(
                f"    attempt {retry_attempt.attempt_number}: "
                f"success={retry_attempt.success} "
                f"exception={retry_attempt.exception_class}"
            )

    @staticmethod
    def _attempt_resolved(q2_task_id: str) -> bool:
        """Whether the attempt for a Q2 task id has recorded an outcome."""
        from qraft.models import QraftTaskAttempt

        return QraftTaskAttempt.objects.filter(
            q2_task_id=q2_task_id, success__isnull=False
        ).exists()

    @staticmethod
    def _task_status(qraft_task_id) -> str:
        from qraft.models import QraftTask

        return QraftTask.objects.values_list("status", flat=True).get(id=qraft_task_id)

    @staticmethod
    def _poll(predicate, timeout: float, interval: float = 0.5) -> bool:
        """Poll a predicate until it's true or the timeout expires."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return True
            time.sleep(interval)
        return predicate()

    def _wait_for_result(self, workflow, wait_ms: int):
        """Wait for workflow completion and display results."""
        self.stdout.write(
            self.style.WARNING(f"\nWaiting up to {wait_ms}ms for completion...")
        )
        try:
            result = workflow.result(wait=wait_ms)
            self.stdout.write(self.style.SUCCESS("\nWorkflow completed!"))
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
                self.style.ERROR(f"\nWorkflow did not complete within {wait_ms}ms.")
            )
            self.stdout.write(f"  Current status: {workflow.status}")

    def _print_cluster_reminder(self):
        """Remind user to start the cluster if not running."""
        self.stdout.write(self.style.WARNING("\nMake sure the cluster is running:"))
        self.stdout.write("  uv run python manage.py qraftcluster\n")
