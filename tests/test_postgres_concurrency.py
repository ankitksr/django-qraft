"""
Concurrency cases that need real row locks.

SQLite has no SKIP LOCKED and no row locks, so `select_for_update()` and the
compare-and-swaps that rely on it are unproven there. These run only when
QRAFT_TEST_DATABASE_URL points at Postgres (see conftest), each on its own
connection per thread, and turn "the lock serialises them" into a check.
"""

import threading
from unittest.mock import Mock, patch

import pytest
from django.db import connection, connections
from django.utils import timezone

from qraft.hooks import qraft_hook_handler
from qraft.models import HookDispatch, QraftTask, QraftTaskAttempt, TaskStatus

pytestmark = [pytest.mark.postgres, pytest.mark.django_db(transaction=True)]


def run_concurrently(*targets):
    """Run each target on its own thread and connection; re-raise any failure."""
    errors = []

    def wrap(target):
        def _run():
            try:
                target()
            except Exception as exc:  # pragma: no cover - surfaced below
                errors.append(exc)
            finally:
                connections.close_all()

        return _run

    threads = [threading.Thread(target=wrap(target)) for target in targets]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    if errors:
        raise errors[0]


def _q2(q2_task_id, success=True):
    task = Mock()
    task.id = q2_task_id
    task.success = success
    task.result = "ok" if success else "boom : Traceback\nValueError: x"
    task.stopped = timezone.now()
    return task


@pytest.fixture(autouse=True)
def _postgres_only():
    if connection.vendor != "postgresql":
        pytest.skip("Postgres only")


class TestDuplicateCompletionRace:
    def test_two_deliveries_resolve_once_and_dispatch_one_hook(self, signal_log):
        task = QraftTask.objects.create(
            func="app.tasks.crunch",
            status=TaskStatus.RUNNING,
            success_hook="x.hooks.done",
        )
        attempt = QraftTaskAttempt.objects.create(
            qraft_task=task, attempt_number=1, q2_task_id="q2-race-1"
        )
        with patch("qraft.hooks.q2_async_task", return_value="hook-race"):
            run_concurrently(
                lambda: qraft_hook_handler(_q2("q2-race-1")),
                lambda: qraft_hook_handler(_q2("q2-race-1")),
            )

        attempt.refresh_from_db()
        assert attempt.success is True and attempt.routed is True
        assert QraftTask.objects.get(id=task.id).status == TaskStatus.SUCCEEDED
        assert HookDispatch.objects.filter(qraft_task=task).count() == 1
        assert len(signal_log["attempt_finished"]) == 1
        assert len(signal_log["task_settled"]) == 1


class TestProgressRace:
    def test_concurrent_reports_lose_no_write_and_stamp_advanced_once(self):
        from qraft import context

        task = QraftTask.objects.create(func="app.tasks.crunch", status=TaskStatus.RUNNING)
        attempt = QraftTaskAttempt.objects.create(
            qraft_task=task, attempt_number=1, q2_task_id="q2-progress-race"
        )

        def report(**fields):
            def _run():
                context._current_q2_task_id.set("q2-progress-race")
                context.report_progress(**fields)

            return _run

        run_concurrently(
            report(current=3, total=10),
            report(message="chunk a"),
            report(stage="score"),
        )
        attempt.refresh_from_db()
        # All three merges survived: no reporter overwrote another's keys.
        assert attempt.progress == {
            "current": 3,
            "total": 10,
            "message": "chunk a",
            "stage": "score",
        }
        assert attempt.progress_advanced_at is not None
        task.refresh_from_db()
        assert task.progress["attempt_id"] == str(attempt.id)


class TestGraphSettlementRace:
    def test_two_nodes_settling_at_once_settle_the_graph_exactly_once(self, signal_log):
        """
        The last two nodes of a graph finish on different workers at the same
        moment. Without the graph's row lock both derivations read "one node
        still RUNNING" and neither settles, or both do; with it, exactly one
        wins the settled_at compare-and-swap.
        """
        from qraft import graphs
        from qraft.models.graphs import (
            GraphStatus,
            NodeStatus,
            QraftGraph,
            QraftGraphNode,
        )

        builder = graphs.Graph(subject=("worksheet", "race"))
        builder.node("root", "app.tasks.root", recovery="transactional")
        builder.node(
            "rules", "app.tasks.rules", after=("root",), recovery="transactional"
        )
        builder.node("ai", "app.tasks.ai", after=("root",), recovery="transactional")
        graph_id = builder.start()

        root = QraftGraphNode.objects.get(graph_id=graph_id, key="root")
        root_task = root.task
        root_task.status = TaskStatus.SUCCEEDED
        root_task.save(update_fields=["status"])
        root_attempt = root_task.attempts.order_by("-attempt_number").first()
        QraftTaskAttempt.objects.filter(pk=root_attempt.pk).update(
            success=True, date_completed=timezone.now()
        )
        graphs.handle_node_completion(root_task, root_attempt)
        nodes = {
            key: QraftGraphNode.objects.get(graph_id=graph_id, key=key)
            for key in ("rules", "ai")
        }
        for node in nodes.values():
            node.refresh_from_db()
            task = node.task
            task.status = TaskStatus.SUCCEEDED
            task.save(update_fields=["status"])
            attempt = task.attempts.order_by("-attempt_number").first()
            QraftTaskAttempt.objects.filter(pk=attempt.pk).update(
                success=True, date_completed=timezone.now()
            )

        run_concurrently(
            *(
                lambda node=node: graphs.handle_node_completion(
                    node.task, node.task.attempts.latest("attempt_number")
                )
                for node in nodes.values()
            )
        )

        graph = QraftGraph.objects.get(id=graph_id)
        assert graph.status == GraphStatus.SUCCEEDED
        assert graph.summary["outcome"] == GraphStatus.SUCCEEDED
        assert set(
            QraftGraphNode.objects.filter(graph_id=graph_id).values_list(
                "status", flat=True
            )
        ) == {NodeStatus.SUCCEEDED}
        assert len(signal_log["graph_settled"]) == 1

    def test_a_skip_racing_a_cancel_leaves_a_consistent_outcome(self):
        """
        Skip and cancel both take the graph's row lock. One commits first and
        the other either raises or records a consistent terminal state.
        """
        from qraft import graphs
        from qraft.models.graphs import GraphStatus, NodeStatus, QraftGraphNode

        builder = graphs.Graph(subject=("worksheet", "race2"))
        builder.node("ingest", "app.tasks.ingest", recovery="transactional")
        builder.node(
            "rules", "app.tasks.rules", after=("ingest",), recovery="transactional"
        )
        graph_id = builder.start()
        refused = []

        def skip_rules():
            try:
                graphs.skip(graph_id, "rules", reason="race")
            except graphs.GraphError:
                refused.append(True)

        run_concurrently(skip_rules, lambda: graphs.cancel(graph_id))

        from qraft.models.graphs import QraftGraph

        assert QraftGraph.objects.get(id=graph_id).status == GraphStatus.CANCELLED
        rules = QraftGraphNode.objects.get(graph_id=graph_id, key="rules")
        if refused:
            assert rules.status in (NodeStatus.PENDING, NodeStatus.SKIPPED)
        else:
            assert rules.status == NodeStatus.SKIPPED


class TestStallFlagRace:
    def test_a_stall_flag_racing_a_progress_advance_flags_at_most_once(self):
        """
        The reaper is deciding a task has stopped moving at the same moment the
        task reports that it moved. Either order is correct; what must not
        happen is two flags, or a flag whose compare-and-swap ran twice.
        """
        from qraft import context
        from qraft.reaper import flag_stalls

        now = timezone.now()
        task = QraftTask.objects.create(
            func="app.tasks.score", status=TaskStatus.RUNNING, stall_after=30
        )
        attempt = QraftTaskAttempt.objects.create(
            qraft_task=task, attempt_number=1, q2_task_id="q2-stall-race"
        )
        QraftTaskAttempt.objects.filter(id=attempt.id).update(
            date_started=now - timezone.timedelta(minutes=10),
            heartbeat_at=now,
            progress={"current": 1, "total": 9},
            progress_reported_at=now,
            progress_advanced_at=now - timezone.timedelta(minutes=10),
        )

        def advance():
            context._current_q2_task_id.set("q2-stall-race")
            context.report_progress(current=2, total=9)

        run_concurrently(advance, flag_stalls, flag_stalls)

        attempt.refresh_from_db()
        assert attempt.progress["current"] == 2
        # Flagged or not, the attempt is never resolved and never retried.
        assert attempt.success is None
        assert QraftTaskAttempt.objects.filter(qraft_task=task).count() == 1
        if attempt.stall_suspected_at:
            # The recovery reading the dashboard shows: advanced since flagged.
            assert attempt.progress_advanced_at >= attempt.stall_suspected_at


class TestProgressAttributionRace:
    def test_a_report_racing_the_next_attempt_is_not_attributed_to_the_old_one(self):
        """
        The stalled attempt reports while its retry row is being written. The
        task-level snapshot is the latest attempt's, so the report must find
        attempt 2 and refuse - which it only does if the two take the same
        lock, since a statement that started before the insert would otherwise
        keep its own snapshot of "no newer attempt".
        """
        import time

        from django.db import transaction

        from qraft import context
        from qraft.scheduler import schedule_attempt

        task = QraftTask.objects.create(func="app.tasks.crunch", status=TaskStatus.RUNNING)
        attempt = QraftTaskAttempt.objects.create(
            qraft_task=task, attempt_number=1, q2_task_id="q2-attr-1"
        )
        inserted, release = threading.Event(), threading.Event()
        reported = []

        def write_next_attempt():
            with transaction.atomic():
                schedule_attempt(task, 2, timezone.now())
                inserted.set()
                release.wait(10)

        def report():
            inserted.wait(10)
            context._current_q2_task_id.set("q2-attr-1")
            reported.append(context.report_progress(current=5, total=10))

        def unblock():
            inserted.wait(10)
            # Long enough for the reporter to be waiting on the task row.
            time.sleep(0.5)
            release.set()

        run_concurrently(write_next_attempt, report, unblock)

        attempt.refresh_from_db()
        task.refresh_from_db()
        assert reported == [True]
        # The attempt keeps its own progress; only the task-level snapshot,
        # which names one attempt, is refused to the superseded one.
        assert attempt.progress == {"current": 5, "total": 10}
        assert not task.progress


class TestDeliveryClaimRace:
    """Two deliveries of one attempt: exactly one may execute."""

    def test_only_one_of_two_concurrent_deliveries_is_admitted(self):
        from qraft import context, lease

        task = QraftTask.objects.create(
            func="tests.e2e_tasks.succeed",
            task_args=["x"],
            task_kwargs={},
            status=TaskStatus.RUNNING,
        )
        attempt = QraftTaskAttempt.objects.create(
            qraft_task=task, attempt_number=1, q2_task_id="q2-race-delivery"
        )

        verdicts = []
        barrier = threading.Barrier(2)

        def deliver():
            context.clear_context()
            barrier.wait(timeout=10)
            verdicts.append(lease.claim_delivery(attempt.q2_task_id))

        run_concurrently(deliver, deliver)

        assert sorted(verdicts) == ["first", "refused"]
        attempt.refresh_from_db()
        assert attempt.execution_count == 1


class TestSubjectBindRace:
    """A member created while the subject is being bound still gets it."""

    def test_a_concurrent_member_never_keeps_a_null_subject(self):
        """
        The losing interleaving, forced rather than hoped for: the enqueue
        reads the run's (still null) subject, the bind then completes with its
        member backfill, and only afterwards does the insert happen. Without
        the locked re-read inside `_create_and_enqueue` the task keeps a null
        subject that nothing ever fills in.
        """
        from qraft import runs
        from qraft.models.runs import QraftRun

        run_id = runs.start(None, ["ingest", "score"])
        labels_read = threading.Event()
        bind_done = threading.Event()
        real_member_labels = runs.member_labels

        def slow_member_labels(*args, **kwargs):
            labels = real_member_labels(*args, **kwargs)
            labels_read.set()
            bind_done.wait(timeout=10)
            return labels

        def bind():
            labels_read.wait(timeout=10)
            runs.bind_subject(run_id, ("worksheet", 4117))
            bind_done.set()

        def enqueue():
            with (
                patch("qraft.tasks.q2_async_task", return_value="q2-race-subject"),
                patch.object(runs, "member_labels", slow_member_labels),
            ):
                from qraft.tasks import async_task

                async_task(
                    "tests.e2e_tasks.succeed",
                    "x",
                    qraft_options={"run": run_id, "stage": "score"},
                )

        run_concurrently(bind, enqueue)

        assert QraftRun.objects.get(id=run_id).subject_type == "worksheet"
        task = QraftTask.objects.get(stage="score")
        # Either it landed before the bind and was backfilled, or it waited on
        # the run's lock and read the bound subject. Never null.
        assert (task.subject_type, task.subject_id) == ("worksheet", "4117")


class TestBudgetRace:
    """Two workers cannot both spend the last request."""

    def test_the_last_request_is_spent_once(self):
        from qraft import runs
        from qraft.context import BudgetExhausted, consume_budget, remaining_budget

        run_id = runs.start(("worksheet", 1), ["ai"], budgets={"openai_requests": 1})
        outcomes = []
        barrier = threading.Barrier(2)

        def spend():
            barrier.wait(timeout=10)
            try:
                outcomes.append(consume_budget("openai_requests", run_id=run_id))
            except BudgetExhausted:
                outcomes.append("exhausted")

        run_concurrently(spend, spend)

        assert sorted(outcomes, key=str) == [0, "exhausted"]
        assert remaining_budget("openai_requests", graph_id=graph_id) == 0


class TestParallelCounterRace:
    """
    Two members of an iter completing at once.

    `_atomic_increment` rejected F() expressions because the completion test
    has to read the counter back, so `select_for_update()` on the workflow row
    is the only thing serialising them. Without a real row lock both threads
    read `completed_count == 0`, both write 1, and the workflow hook that
    should fire on the last member never fires at all.
    """

    def test_two_members_finishing_at_once_settle_the_iter_once(self, signal_log):
        from qraft.dispatchers import ParallelDispatcher
        from qraft.models import (
            QraftIterModel,
            WorkflowHookDispatch,
            WorkflowStatus,
        )

        workflow = QraftIterModel.objects.create(
            func="tests.e2e_tasks.succeed",
            total_count=2,
            status=WorkflowStatus.RUNNING,
            success_hook="tests.e2e_tasks.succeed",
        )
        attempts = []
        for index in range(2):
            task = QraftTask.objects.create(
                func="tests.e2e_tasks.succeed",
                status=TaskStatus.SUCCEEDED,
                qraft_iter=workflow,
            )
            attempts.append(
                QraftTaskAttempt.objects.create(
                    qraft_task=task,
                    attempt_number=1,
                    q2_task_id=f"q2-parallel-race-{index}",
                    success=True,
                )
            )

        barrier = threading.Barrier(2)

        def complete(attempt):
            def _run():
                barrier.wait(timeout=10)
                with patch(
                    "qraft.dispatchers.q2_async_task",
                    return_value=f"hook-{attempt.q2_task_id}",
                ):
                    ParallelDispatcher(workflow, attempt).handle()

            return _run

        run_concurrently(*(complete(attempt) for attempt in attempts))

        workflow.refresh_from_db()
        assert (workflow.completed_count, workflow.success_count) == (2, 2)
        assert workflow.status == WorkflowStatus.SUCCEEDED
        assert workflow.settled_at is not None
        assert (
            WorkflowHookDispatch.objects.filter(
                workflow_type="iter", workflow_id=workflow.id
            ).count()
            == 1
        )
        assert len(signal_log["workflow_settled"]) == 1


class TestChainAdvanceRace:
    """
    One chain step's completion delivered twice at once.

    The advance is guarded by `current_step_index` under the chain's row lock.
    Without the lock both deliveries read the same index, both pass the guard,
    and step 2 is enqueued twice.
    """

    def test_a_duplicated_step_completion_queues_the_next_step_once(self):
        from qraft.dispatchers import ChainDispatcher
        from qraft.models import QraftChainModel, QraftChainStep, WorkflowStatus

        chain = QraftChainModel.objects.create(status=WorkflowStatus.RUNNING)
        steps = [
            QraftChainStep.objects.create(
                chain=chain, step_index=index, func="tests.e2e_tasks.succeed"
            )
            for index in range(2)
        ]
        task = QraftTask.objects.create(
            func="tests.e2e_tasks.succeed", status=TaskStatus.SUCCEEDED
        )
        steps[0].qraft_task = task
        steps[0].save(update_fields=["qraft_task"])
        attempt = QraftTaskAttempt.objects.create(
            qraft_task=task,
            attempt_number=1,
            q2_task_id="q2-chain-advance-race",
            success=True,
        )

        barrier = threading.Barrier(2)

        def advance():
            barrier.wait(timeout=10)
            with patch("qraft.tasks.q2_async_task", return_value="q2-next"):
                ChainDispatcher(chain, steps[0], attempt).handle()

        run_concurrently(advance, advance)

        chain.refresh_from_db()
        assert chain.current_step_index == 1
        steps[1].refresh_from_db()
        assert steps[1].qraft_task is not None
        # Step 0's task plus step 1's. A second advance past the guard would
        # create a third and orphan the one the step no longer points at.
        assert QraftTask.objects.count() == 2


class TestThrottleRace:
    """
    The token bucket is shared state; one token cannot be spent twice.

    `throttle.acquire` refills and withdraws under `select_for_update()`. On
    SQLite that lock is a no-op, so every existing throttle test proves the
    arithmetic and nothing about the cross-worker limit the bucket exists for.
    """

    def test_two_workers_and_one_token_admit_exactly_one(self):
        from qraft.models import RateBucket
        from qraft.throttle import acquire

        RateBucket.objects.create(key="provider-race", tokens=1.0)
        verdicts = []
        barrier = threading.Barrier(2)

        def withdraw():
            barrier.wait(timeout=10)
            # rate=0 so no refill can hide a lost update behind elapsed time.
            verdicts.append(acquire("provider-race", rate=0.0, capacity=1.0))

        run_concurrently(withdraw, withdraw)

        assert sorted(verdicts) == [False, True]
        assert RateBucket.objects.get(key="provider-race").tokens == pytest.approx(0.0)
