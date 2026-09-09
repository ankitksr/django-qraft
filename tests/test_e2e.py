"""
End-to-end tests: enqueue -> broker -> worker -> monitor -> hook handler.

Every other test module mocks `q2_async_task` and calls the hook handler with
a fake Django-Q2 task. That leaves the seam Qraft actually depends on - a real
`OrmQ` row, a real worker executing a real dotted path, a real `Task` row whose
`post_save` fires `qraft_hook_handler` - untested. These tests drive that seam
with Django-Q2's own `pusher`/`worker`/`monitor` functions in-process, the same
three loops `qcluster` runs in separate processes, so nothing between
`async_task()` and the hook is stubbed.

What stays out of the loop is the sentinel: process spawning, recycling and
timeout kills cannot run inside a test process. `dispatch_due()` (normally the
dispatcher thread) is called directly instead of polled, which is also what
makes retry backoff instant here rather than real time.
"""

import signal
from datetime import timedelta
from multiprocessing import Event, Value
from queue import Queue

import pytest
from django.utils import timezone
from django_q.brokers import get_broker
from django_q.models import OrmQ, Task
from django_q.monitor import monitor
from django_q.pusher import pusher
from django_q.worker import worker as q2_worker

from qraft.models import (
    HookDispatch,
    QraftTask,
    QraftTaskAttempt,
    TaskStatus,
    WorkflowStatus,
)
from qraft.models.tasks import AttemptState
from qraft.scheduler import dispatch_due
from qraft.tasks import async_task
from qraft.worker import threaded_worker
from tests import e2e_tasks

# Bound on settle() passes, so a task that never stops re-queueing itself
# fails the test instead of hanging it.
MAX_PASSES = 12


@pytest.fixture
def clean_queue():
    """Drain the real broker and the recorded calls around each test."""
    get_broker().purge_queue()
    e2e_tasks.reset()
    yield
    get_broker().purge_queue()
    e2e_tasks.reset()


def _make_visible():
    """
    Move every queued message's visibility deadline into the past.

    `ORM.enqueue()` stamps `lock=now` and `dequeue()` matches `lock < now`, so
    a message is only invisible for as long as the clock takes to advance.
    Doing it explicitly keeps the pass deterministic rather than dependent on
    that gap.
    """
    OrmQ.objects.update(lock=timezone.now() - timedelta(seconds=1))


def _run_standard_worker(task_queue, result_queue):
    q2_worker(task_queue, result_queue, Value("f", -1), timeout=-1)


def _run_threaded_worker(task_queue, result_queue):
    """
    Drive qraft's own worker loop.

    It installs process-wide SIGINT/SIGTERM handlers on entry (the sentinel
    owns shutdown in production), which would outlive the test here.
    """
    original = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        threaded_worker(
            task_queue,
            result_queue,
            Value("f", -1),
            timeout=-1,
            threads=2,
            max_inflight=2,
            grace_period=10.0,
        )
    finally:
        for sig, handler in original.items():
            signal.signal(sig, handler)


def run_once(run_worker=_run_standard_worker) -> int:
    """
    Push, execute and monitor whatever is queued right now.

    Returns the number of messages executed, so callers can loop until the
    queue stops producing work.
    """
    _make_visible()
    task_queue = Queue()
    event = Event()
    # Set before the call: pusher checks it at the end of its first pass, so
    # this makes the loop do exactly one dequeue.
    event.set()
    pusher(task_queue, event, get_broker())

    executed = task_queue.qsize()
    if not executed:
        return 0

    result_queue = Queue()
    task_queue.put("STOP")
    run_worker(task_queue, result_queue)
    result_queue.put("STOP")
    monitor(result_queue, get_broker())
    return executed


def settle(run_worker=_run_standard_worker) -> None:
    """
    Run passes until nothing is left to execute or dispatch.

    `dispatch_due()` stands in for the dispatcher thread: it turns a SCHEDULED
    retry into a queued message, which the next pass then executes.
    """
    for _ in range(MAX_PASSES):
        if not run_once(run_worker) and not dispatch_due():
            return
    raise AssertionError(f"queue did not settle within {MAX_PASSES} passes")


@pytest.mark.django_db(transaction=True)
class TestTaskEndToEnd:
    """A task really executes, and its outcome really reaches the hook."""

    def test_success_runs_task_then_success_hook(self, clean_queue):
        q2_task_id = async_task(
            "tests.e2e_tasks.succeed",
            "alpha",
            qraft_options={
                "success_hook": "tests.e2e_tasks.on_success",
                "success_args": ["alpha"],
                "failure_hook": "tests.e2e_tasks.on_failure",
            },
        )

        assert run_once() == 1
        assert e2e_tasks.CALLS["succeed"] == ["alpha"]

        qraft_task = QraftTask.objects.get()
        assert qraft_task.status == TaskStatus.SUCCEEDED

        attempt = QraftTaskAttempt.objects.get()
        assert attempt.q2_task_id == q2_task_id
        assert attempt.success is True
        assert attempt.routed is True
        # Stamped by the execution lease at pre_execute, inside the worker.
        assert attempt.date_started is not None
        assert attempt.worker_pid is not None
        assert Task.objects.get(id=q2_task_id).success is True

        # The hook is dispatched as its own task, so it takes another pass.
        dispatch = HookDispatch.objects.get()
        assert dispatch.hook_type == "success"
        assert "on_success" not in e2e_tasks.CALLS

        assert run_once() == 1
        assert e2e_tasks.CALLS["on_success"] == ["alpha"]
        assert "on_failure" not in e2e_tasks.CALLS

    def test_retry_reruns_the_task_and_then_succeeds(self, clean_queue):
        e2e_tasks.FAIL_BUDGET["flaky"] = 1

        async_task(
            "tests.e2e_tasks.flaky",
            "beta",
            qraft_options={
                "max_attempts": 3,
                "base_delay": 0,
                "backoff_strategy": "fixed",
                "jitter": False,
                "success_hook": "tests.e2e_tasks.on_success",
                "success_args": ["beta"],
            },
        )

        assert run_once() == 1
        assert e2e_tasks.CALLS["flaky"] == ["beta"]

        # Attempt 1 failed and attempt 2 exists as a row, not a queued
        # message: it is the dispatcher that hands it to the broker.
        qraft_task = QraftTask.objects.get()
        assert qraft_task.status == TaskStatus.PENDING
        retry = QraftTaskAttempt.objects.get(attempt_number=2)
        assert retry.state == AttemptState.SCHEDULED
        assert retry.q2_task_id is None
        assert OrmQ.objects.count() == 0

        assert dispatch_due() == 1
        retry.refresh_from_db()
        assert retry.state == AttemptState.QUEUED
        assert retry.q2_task_id is not None

        assert run_once() == 1
        assert e2e_tasks.CALLS["flaky"] == ["beta", "beta"]

        qraft_task.refresh_from_db()
        assert qraft_task.status == TaskStatus.SUCCEEDED
        retry.refresh_from_db()
        assert retry.success is True
        assert retry.date_started is not None
        assert QraftTaskAttempt.objects.filter(attempt_number=1).get().success is False

        assert run_once() == 1
        assert e2e_tasks.CALLS["on_success"] == ["beta"]

    def test_exhaustion_runs_every_attempt_then_the_failure_hook(self, clean_queue):
        async_task(
            "tests.e2e_tasks.always_fails",
            "gamma",
            qraft_options={
                "max_attempts": 2,
                "base_delay": 0,
                "backoff_strategy": "fixed",
                "jitter": False,
                "success_hook": "tests.e2e_tasks.on_success",
                "failure_hook": "tests.e2e_tasks.on_failure",
                "failure_args": ["gamma"],
            },
        )

        settle()

        assert e2e_tasks.CALLS["always_fails"] == ["gamma", "gamma"]
        assert e2e_tasks.CALLS["on_failure"] == ["gamma"]
        assert "on_success" not in e2e_tasks.CALLS

        qraft_task = QraftTask.objects.get()
        assert qraft_task.status == TaskStatus.EXHAUSTED

        attempts = QraftTaskAttempt.objects.order_by("attempt_number")
        assert [a.attempt_number for a in attempts] == [1, 2]
        assert all(a.success is False for a in attempts)
        assert all(a.exception_class == "TransientError" for a in attempts)

    def test_threaded_worker_runs_the_same_path(self, clean_queue):
        async_task(
            "tests.e2e_tasks.succeed",
            "delta",
            qraft_options={
                "success_hook": "tests.e2e_tasks.on_success",
                "success_args": ["delta"],
            },
        )

        settle(run_worker=_run_threaded_worker)

        assert e2e_tasks.CALLS["succeed"] == ["delta"]
        assert e2e_tasks.CALLS["on_success"] == ["delta"]
        assert QraftTask.objects.get().status == TaskStatus.SUCCEEDED

        attempt = QraftTaskAttempt.objects.get()
        assert attempt.success is True
        # The threaded worker executes in a pool thread, and the lease records
        # which one - the standard worker leaves this null.
        assert attempt.worker_thread is not None

    def test_async_def_task_runs_retries_and_hooks_like_a_sync_one(self, clean_queue):
        """A coroutine target really executes: the reroute through run_task
        awaits it in the worker, and the retry takes the identical path."""
        e2e_tasks.FAIL_BUDGET["flaky_async"] = 1

        async_task(
            "tests.e2e_tasks.flaky_async",
            "zeta",
            qraft_options={
                "max_attempts": 3,
                "base_delay": 0,
                "backoff_strategy": "fixed",
                "jitter": False,
                "success_hook": "tests.e2e_tasks.on_success_async",
                "success_args": ["zeta"],
            },
        )

        settle()

        assert e2e_tasks.CALLS["flaky_async"] == ["zeta", "zeta"]
        # The hook is itself an `async def`, so this also proves hook
        # dispatch reroutes coroutine hooks through run_task.
        assert e2e_tasks.CALLS["on_success_async"] == ["zeta"]

        qraft_task = QraftTask.objects.get()
        assert qraft_task.status == TaskStatus.SUCCEEDED
        # The reroute is an enqueue detail; metadata records the real target.
        assert qraft_task.func == "tests.e2e_tasks.flaky_async"
        retry = QraftTaskAttempt.objects.get(attempt_number=2)
        assert retry.success is True
        assert Task.objects.get(id=retry.q2_task_id).result == "ok:zeta"

    def test_async_def_task_on_the_threaded_worker(self, clean_queue):
        async_task("tests.e2e_tasks.succeed_async", "eta")

        settle(run_worker=_run_threaded_worker)

        assert e2e_tasks.CALLS["succeed_async"] == ["eta"]
        assert QraftTask.objects.get().status == TaskStatus.SUCCEEDED

    def test_dead_task_requeues_and_continues_its_attempt_series(self, clean_queue):
        e2e_tasks.FAIL_BUDGET["flaky"] = 1
        async_task(
            "tests.e2e_tasks.flaky",
            "epsilon",
            qraft_options={"max_attempts": 1},
        )

        settle()
        qraft_task = QraftTask.objects.get()
        assert qraft_task.status == TaskStatus.EXHAUSTED

        from qraft.dlq import dead_letters, requeue

        assert list(dead_letters()) == [qraft_task]
        requeue(qraft_task)

        settle()

        assert e2e_tasks.CALLS["flaky"] == ["epsilon", "epsilon"]
        qraft_task.refresh_from_db()
        assert qraft_task.status == TaskStatus.SUCCEEDED
        assert QraftTaskAttempt.objects.count() == 2


@pytest.mark.django_db(transaction=True)
class TestWorkflowEndToEnd:
    """Workflow orchestration really advances on real completions."""

    def test_chain_runs_its_steps_in_order_then_fires_its_hook(self, clean_queue):
        from qraft.chain import QraftChain

        chain = QraftChain(
            on_success="tests.e2e_tasks.on_success",
            success_args=("chain",),
            on_failure="tests.e2e_tasks.on_failure",
        )
        chain.append("tests.e2e_tasks.succeed", "step-0")
        chain.append("tests.e2e_tasks.succeed", "step-1")
        chain.run()

        settle()

        # Order is the assertion that matters: step 1 is only queued once
        # step 0's completion reaches the chain dispatcher.
        assert e2e_tasks.CALLS["succeed"] == ["step-0", "step-1"]
        assert e2e_tasks.CALLS["on_success"] == ["chain"]
        assert "on_failure" not in e2e_tasks.CALLS

        chain._model.refresh_from_db()
        assert chain._model.status == WorkflowStatus.SUCCEEDED

    def test_iter_counts_every_member_before_firing_its_hook(self, clean_queue):
        from qraft.iter import QraftIter

        iter_workflow = QraftIter(
            "tests.e2e_tasks.succeed",
            on_success="tests.e2e_tasks.on_success",
            success_args=("iter",),
        )
        for label in ("one", "two", "three"):
            iter_workflow.append(label)
        iter_workflow.run()

        settle()

        assert sorted(e2e_tasks.CALLS["succeed"]) == ["one", "three", "two"]
        assert e2e_tasks.CALLS["on_success"] == ["iter"]

        iter_workflow._model.refresh_from_db()
        assert iter_workflow._model.status == WorkflowStatus.SUCCEEDED
        assert iter_workflow._model.completed_count == 3
        assert iter_workflow._model.success_count == 3


@pytest.mark.django_db(transaction=True)
class TestObservabilityEndToEnd:
    """Signals, hook context and trace propagation on the real path."""

    def test_task_settled_receiver_observes_the_committed_row(self, clean_queue):
        from qraft import signals

        seen = []

        def receiver(sender, payload, **kwargs):
            # Fires after commit, so a fresh read sees the resolution.
            seen.append(QraftTask.objects.get(id=payload["task_id"]).status)

        signals.task_settled.connect(receiver, weak=False)
        try:
            async_task(
                "tests.e2e_tasks.succeed",
                "theta",
                qraft_options={
                    "subject": ("worksheet", 4117),
                    "hook_context": True,
                    "success_hook": "tests.e2e_tasks.on_success_with_context",
                    "success_args": ["theta"],
                },
            )
            settle()
        finally:
            signals.task_settled.disconnect(receiver)

        assert seen == [TaskStatus.SUCCEEDED]
        ((label, context),) = e2e_tasks.CALLS["on_success_with_context"]
        assert label == "theta"
        assert context["outcome"] == "succeeded"
        assert context["subject_id"] == "4117"
        attempt = QraftTaskAttempt.objects.get()
        assert context["attempt_id"] == str(attempt.id)
        assert context["result_ref"] == attempt.q2_task_id
        assert Task.objects.get(id=context["result_ref"]).result == "ok:theta"

    def test_traceparent_rides_the_attempt_and_its_retry(self, clean_queue):
        from opentelemetry import context as otel_context
        from opentelemetry import trace

        e2e_tasks.FAIL_BUDGET["flaky"] = 1
        span_context = trace.SpanContext(
            trace_id=0x1234567890ABCDEF1234567890ABCDEF,
            span_id=0x1234567890ABCDEF,
            is_remote=False,
            trace_flags=trace.TraceFlags(trace.TraceFlags.SAMPLED),
        )
        token = otel_context.attach(
            trace.set_span_in_context(trace.NonRecordingSpan(span_context))
        )
        try:
            async_task(
                "tests.e2e_tasks.flaky",
                "iota",
                qraft_options={
                    "max_attempts": 2,
                    "base_delay": 0,
                    "backoff_strategy": "fixed",
                    "jitter": False,
                },
            )
        finally:
            otel_context.detach(token)

        settle()

        first, retry = QraftTaskAttempt.objects.order_by("attempt_number")
        assert first.trace_context.startswith("00-1234567890abcdef1234567890abcdef-")
        assert retry.trace_context == first.trace_context
        assert retry.success is True
        assert first.enqueued_at is not None and retry.enqueued_at is not None
        assert retry.enqueued_at >= first.enqueued_at

    def test_a_two_stage_run_settles_from_its_own_success_path(self, clean_queue):
        """
        The shape the run primitive exists for: stage two is enqueued by stage
        one's success hook, not by a static step list, and the run still
        settles once with a duration and one durable `on_settled` dispatch.
        """
        from qraft import runs
        from qraft.models import WorkflowHookDispatch
        from qraft.models.runs import QraftRun, RunStatus, StageStatus, UnitType

        started = timezone.now() - timedelta(seconds=5)
        run_id = runs.start(
            subject=("worksheet", 4117),
            stages=["one", "two"],
            kind="shadow",
            started_at=started,
            on_settled="tests.e2e_tasks.run_is_ready",
        )
        async_task(
            "tests.e2e_tasks.succeed",
            "kappa",
            qraft_options={
                "run": run_id,
                "stage": "one",
                "hook_context": True,
                "success_hook": "tests.e2e_tasks.bind_second_stage",
                "success_args": ["kappa"],
            },
        )

        settle()

        run = QraftRun.objects.get(id=run_id)
        assert run.status == RunStatus.SUCCEEDED
        assert run.settled_at is not None
        assert run.summary["duration_s"] >= 5
        stages = {stage["name"]: stage for stage in run.summary["stages"]}
        assert stages["one"]["unit_type"] == UnitType.TASK
        assert stages["two"]["unit_type"] == UnitType.BATCH
        assert {s["outcome"] for s in run.summary["stages"]} == {StageStatus.SUCCEEDED}

        assert (
            WorkflowHookDispatch.objects.filter(
                workflow_type="run", workflow_id=run_id, hook_type="settled"
            ).count()
            == 1
        )
        (context,) = e2e_tasks.CALLS["run_is_ready"]
        assert context["outcome"] == RunStatus.SUCCEEDED
        assert context["stages"] == {"one": "succeeded", "two": "succeeded"}
        assert context["duration_s"] >= 5

    def test_a_stalled_attempt_is_flagged_and_still_finishes(self, clean_queue):
        """
        Stall observation never touches the attempt: the task is flagged while
        it runs and then completes normally, with no retry and no second
        attempt.
        """
        from qraft.reaper import flag_stalls

        async_task(
            "tests.e2e_tasks.slow_scorer",
            "lambda",
            qraft_options={"stall_after": 0, "max_attempts": 3},
        )
        # Push and execute, but flag between the worker starting the attempt
        # and the monitor recording its result.
        _make_visible()
        task_queue = Queue()
        event = Event()
        event.set()
        pusher(task_queue, event, get_broker())
        result_queue = Queue()
        task_queue.put("STOP")
        _run_standard_worker(task_queue, result_queue)

        attempt = QraftTaskAttempt.objects.get()
        assert attempt.progress["current"] == 1
        assert flag_stalls() == 1
        attempt.refresh_from_db()
        assert attempt.stall_suspected_at is not None
        assert attempt.success is None

        result_queue.put("STOP")
        monitor(result_queue, get_broker())
        settle()

        task = QraftTask.objects.get()
        assert task.status == TaskStatus.SUCCEEDED
        # Flagged, never resolved: one attempt, and the flag survives as history.
        assert QraftTaskAttempt.objects.count() == 1
        assert QraftTaskAttempt.objects.get().stall_suspected_at is not None
        assert e2e_tasks.CALLS["slow_scorer"] == ["lambda"]
