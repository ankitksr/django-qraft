"""Regression tests for workflow dispatchers (v1.1.1 race/idempotency fixes)."""

from unittest.mock import patch

import pytest

# Disable hook path import validation for tests using fake module paths
pytestmark = pytest.mark.usefixtures("_disable_hook_validation")

from qraft.dispatchers import (  # noqa: E402
    INCREMENT_COMPLETE,
    INCREMENT_NOOP,
    INCREMENT_PARTIAL,
    ChainDispatcher,
    ParallelDispatcher,
    _dispatch_workflow_hook,
)
from qraft.models import (  # noqa: E402
    QraftChainModel,
    QraftChainStep,
    QraftIterModel,
    QraftTask,
    QraftTaskAttempt,
    TaskStatus,
    WorkflowHookDispatch,
    WorkflowStatus,
)


@pytest.fixture
def iter_model(db):
    """A 2-task iter workflow, running, with a failure hook configured."""
    return QraftIterModel.objects.create(
        func="demo.showcase.tasks.noop_task",
        total_count=2,
        status=WorkflowStatus.RUNNING,
        success_hook="showcase.tasks.on_success",
        failure_hook="showcase.tasks.on_failure",
    )


def _make_attempt(workflow_fk_field, workflow, success):
    """Create a QraftTask + terminal QraftTaskAttempt linked to a parallel workflow."""
    task = QraftTask.objects.create(
        func="demo.showcase.tasks.noop_task",
        status=TaskStatus.SUCCEEDED if success else TaskStatus.EXHAUSTED,
        **{workflow_fk_field: workflow},
    )
    return QraftTaskAttempt.objects.create(
        qraft_task=task,
        attempt_number=1,
        q2_task_id=f"q2-{task.id}",
        success=success,
    )


def _chain_with_steps(n=2, **chain_fields):
    """Create a RUNNING chain with n steps."""
    chain = QraftChainModel.objects.create(
        status=WorkflowStatus.RUNNING, **chain_fields
    )
    steps = [
        QraftChainStep.objects.create(
            chain=chain,
            step_index=i,
            func="demo.showcase.tasks.noop_task",
        )
        for i in range(n)
    ]
    return chain, steps


def _chain_attempt(step, success, task_status):
    """Create and link a QraftTask + QraftTaskAttempt for a chain step."""
    task = QraftTask.objects.create(func=step.func, status=task_status)
    step.qraft_task = task
    step.save(update_fields=["qraft_task"])
    return QraftTaskAttempt.objects.create(
        qraft_task=task,
        attempt_number=1,
        q2_task_id=f"q2-{task.id}",
        success=success,
    )


class TestParallelDispatcherIdempotency:
    """Regression: double-firing the handler for one attempt must not double-count."""

    def test_already_counted_attempt_is_not_double_counted(self, db, iter_model):
        attempt = _make_attempt("qraft_iter", iter_model, success=True)

        with patch("qraft.dispatchers.q2_async_task"):
            ParallelDispatcher(iter_model, attempt).handle()
        iter_model.refresh_from_db()
        assert iter_model.completed_count == 1
        assert iter_model.success_count == 1

        # Handler fires again for the same attempt (e.g. duplicate hook delivery).
        with patch("qraft.dispatchers.q2_async_task"):
            ParallelDispatcher(iter_model, attempt).handle()
        iter_model.refresh_from_db()
        assert iter_model.completed_count == 1
        assert iter_model.success_count == 1

    def test_retrying_task_does_not_count_towards_workflow(self, db, iter_model):
        """A failed attempt whose task will still retry is ignored.

        schedule_retry() puts the task back to PENDING, so PENDING - not
        FAILED - is what a retry in flight actually looks like here.
        """
        task = QraftTask.objects.create(
            func="demo.showcase.tasks.noop_task",
            status=TaskStatus.PENDING,  # retry scheduled, waiting on backoff
            qraft_iter=iter_model,
        )
        attempt = QraftTaskAttempt.objects.create(
            qraft_task=task,
            attempt_number=1,
            q2_task_id="q2-retrying",
            success=False,
        )

        with patch("qraft.dispatchers.q2_async_task") as mock_async:
            ParallelDispatcher(iter_model, attempt).handle()

        iter_model.refresh_from_db()
        assert iter_model.completed_count == 0
        assert iter_model.failure_count == 0
        mock_async.assert_not_called()


class TestParallelDispatcherCompletion:
    """Completion detection and workflow-hook idempotency for iter/batch."""

    def test_hook_fires_once_when_last_task_completes(self, db, iter_model):
        attempt1 = _make_attempt("qraft_iter", iter_model, success=True)
        attempt2 = _make_attempt("qraft_iter", iter_model, success=True)

        with patch("qraft.dispatchers.q2_async_task") as mock_async:
            mock_async.return_value = "q2-hook-1"
            ParallelDispatcher(iter_model, attempt1).handle()
            mock_async.assert_not_called()

            ParallelDispatcher(iter_model, attempt2).handle()
            mock_async.assert_called_once()

        iter_model.refresh_from_db()
        assert iter_model.status == WorkflowStatus.SUCCEEDED
        assert (
            WorkflowHookDispatch.objects.filter(
                workflow_type="iter",
                workflow_id=iter_model.id,
                hook_type="success",
            ).count()
            == 1
        )

    def test_workflow_hook_dispatch_uniqueness_blocks_duplicate(self, db, iter_model):
        """Calling _dispatch_workflow_hook twice for the same workflow+hook_type
        must only queue the async hook once."""
        with patch("qraft.dispatchers.q2_async_task") as mock_async:
            mock_async.return_value = "q2-hook-a"
            _dispatch_workflow_hook(
                workflow_type="iter",
                workflow_id=iter_model.id,
                hook_type="success",
                hook_path="showcase.tasks.on_success",
                hook_args=[],
                hook_kwargs={},
            )
            _dispatch_workflow_hook(
                workflow_type="iter",
                workflow_id=iter_model.id,
                hook_type="success",
                hook_path="showcase.tasks.on_success",
                hook_args=[],
                hook_kwargs={},
            )
            mock_async.assert_called_once()

        assert (
            WorkflowHookDispatch.objects.filter(
                workflow_type="iter",
                workflow_id=iter_model.id,
                hook_type="success",
            ).count()
            == 1
        )

    def test_failure_count_marks_workflow_failed(self, db, iter_model):
        attempt1 = _make_attempt("qraft_iter", iter_model, success=True)
        attempt2 = _make_attempt("qraft_iter", iter_model, success=False)

        with patch("qraft.dispatchers.q2_async_task") as mock_async:
            mock_async.return_value = "q2-hook-fail"
            ParallelDispatcher(iter_model, attempt1).handle()
            ParallelDispatcher(iter_model, attempt2).handle()
            call_args = mock_async.call_args
            assert call_args[0][0] == "showcase.tasks.on_failure"

        iter_model.refresh_from_db()
        assert iter_model.status == WorkflowStatus.FAILED

    def test_cancelled_workflow_skips_processing(self, db, iter_model):
        iter_model.status = WorkflowStatus.CANCELLED
        iter_model.save(update_fields=["status"])
        attempt = _make_attempt("qraft_iter", iter_model, success=True)

        ParallelDispatcher(iter_model, attempt).handle()

        iter_model.refresh_from_db()
        assert iter_model.completed_count == 0
        attempt.refresh_from_db()
        assert attempt.counted is False

    def test_cancel_racing_final_completion_is_not_overwritten(self, db, iter_model):
        """Regression: a cancel landing between the outer status check (in
        `handle()`) and the locked increment (in `_atomic_increment()`) must
        not be clobbered back to SUCCEEDED/FAILED by the completing task."""
        attempt = _make_attempt("qraft_iter", iter_model, success=True)
        dispatcher = ParallelDispatcher(iter_model, attempt)

        # Simulate the interleaving directly: the outer `handle()` check saw
        # RUNNING, but by the time `_atomic_increment` takes its lock, a
        # concurrent cancel has already committed.
        iter_model.status = WorkflowStatus.CANCELLED
        iter_model.save(update_fields=["status"])

        assert dispatcher._atomic_increment() is INCREMENT_NOOP
        iter_model.refresh_from_db()
        assert iter_model.status == WorkflowStatus.CANCELLED
        assert iter_model.completed_count == 0
        attempt.refresh_from_db()
        assert attempt.counted is False


class TestChainDispatcher:
    """Sequential continuation, failure short-circuit, and retry-in-progress."""

    def _chain_with_steps(self, n=2):
        chain = QraftChainModel.objects.create(status=WorkflowStatus.RUNNING)
        steps = [
            QraftChainStep.objects.create(
                chain=chain,
                step_index=i,
                func="demo.showcase.tasks.noop_task",
            )
            for i in range(n)
        ]
        return chain, steps

    def _attempt_for_step(self, step, success, task_status):
        task = QraftTask.objects.create(func=step.func, status=task_status)
        step.qraft_task = task
        step.save(update_fields=["qraft_task"])
        return QraftTaskAttempt.objects.create(
            qraft_task=task,
            attempt_number=1,
            q2_task_id=f"q2-{task.id}",
            success=success,
        )

    def test_success_queues_next_step(self, db):
        chain, (step0, step1) = self._chain_with_steps(2)
        attempt = self._attempt_for_step(
            step0, success=True, task_status=TaskStatus.SUCCEEDED
        )

        with patch("qraft.tasks.q2_async_task") as mock_async:
            mock_async.return_value = "q2-next"
            ChainDispatcher(chain, step0, attempt).handle()

        chain.refresh_from_db()
        step1.refresh_from_db()
        assert chain.status == WorkflowStatus.RUNNING
        assert chain.current_step_index == 1
        assert step1.qraft_task is not None
        mock_async.assert_called_once()

    def test_last_step_success_marks_chain_succeeded(self, db):
        chain, (step0,) = self._chain_with_steps(1)
        attempt = self._attempt_for_step(
            step0, success=True, task_status=TaskStatus.SUCCEEDED
        )

        with patch("qraft.dispatchers.q2_async_task") as mock_async:
            ChainDispatcher(chain, step0, attempt).handle()
            mock_async.assert_not_called()  # no hook configured on this chain

        chain.refresh_from_db()
        assert chain.status == WorkflowStatus.SUCCEEDED

    def test_exhausted_failure_marks_chain_failed_and_stops(self, db):
        chain, (step0, step1) = self._chain_with_steps(2)
        attempt = self._attempt_for_step(
            step0, success=False, task_status=TaskStatus.EXHAUSTED
        )

        with patch("qraft.tasks.q2_async_task") as mock_next_step:
            ChainDispatcher(chain, step0, attempt).handle()
            mock_next_step.assert_not_called()

        chain.refresh_from_db()
        step1.refresh_from_db()
        assert chain.status == WorkflowStatus.FAILED
        assert step1.qraft_task is None  # never queued

    def test_retry_in_progress_does_not_fail_or_advance_chain(self, db):
        """A pending retry must not fail or advance the chain.

        schedule_retry() returns the task to PENDING, so that - not FAILED -
        is what a retry in flight looks like to the dispatcher.
        """
        chain, (step0, step1) = self._chain_with_steps(2)
        attempt = self._attempt_for_step(
            step0, success=False, task_status=TaskStatus.PENDING
        )

        with patch("qraft.tasks.q2_async_task") as mock_next_step:
            ChainDispatcher(chain, step0, attempt).handle()
            mock_next_step.assert_not_called()

        chain.refresh_from_db()
        step1.refresh_from_db()
        assert chain.status == WorkflowStatus.RUNNING
        assert chain.current_step_index == 0
        assert step1.qraft_task is None

    def test_duplicate_completion_delivery_queues_next_step_once(self, db):
        """Regression: two deliveries of the same successful step completion
        (e.g. a duplicate hook call) must queue the next step exactly once,
        not overwrite step1.qraft_task with a second QraftTask."""
        chain, (step0, step1) = self._chain_with_steps(2)
        attempt = self._attempt_for_step(
            step0, success=True, task_status=TaskStatus.SUCCEEDED
        )

        with patch("qraft.tasks.q2_async_task") as mock_async:
            mock_async.return_value = "q2-next-1"
            ChainDispatcher(chain, step0, attempt).handle()
            mock_async.return_value = "q2-next-2"
            ChainDispatcher(chain, step0, attempt).handle()

        chain.refresh_from_db()
        step1.refresh_from_db()
        assert chain.current_step_index == 1
        assert mock_async.call_count == 1
        # step0's original task + exactly one new task for step1
        assert QraftTask.objects.count() == 2

    def test_cancelled_chain_skips_processing(self, db):
        chain, (step0,) = self._chain_with_steps(1)
        chain.status = WorkflowStatus.CANCELLED
        chain.save(update_fields=["status"])
        attempt = self._attempt_for_step(
            step0, success=True, task_status=TaskStatus.SUCCEEDED
        )

        ChainDispatcher(chain, step0, attempt).handle()

        chain.refresh_from_db()
        assert chain.status == WorkflowStatus.CANCELLED


class TestChainAdvanceEnqueueAtomicity:
    """Regression: a broker failure while queueing step N+1 must roll back the
    index advance, or the chain wedges at RUNNING with no task for N+1.
    There is no redelivery actor (ack_failure=True); recovery is manual."""

    def test_enqueue_failure_rolls_back_advance_leaving_chain_at_step(self, db):
        chain, (step0, step1) = _chain_with_steps(2)
        attempt = _chain_attempt(step0, success=True, task_status=TaskStatus.SUCCEEDED)

        with patch(
            "qraft.tasks.q2_async_task", side_effect=RuntimeError("broker down")
        ):
            with pytest.raises(RuntimeError):
                ChainDispatcher(chain, step0, attempt).handle()

        chain.refresh_from_db()
        step1.refresh_from_db()
        assert chain.status == WorkflowStatus.RUNNING
        assert chain.current_step_index == 0
        assert step1.qraft_task is None
        # step0's task survives; the rolled-back step1 task does not
        assert QraftTask.objects.count() == 1

        # Manual re-handle (not broker redelivery) can still advance.
        with patch("qraft.tasks.q2_async_task") as mock_async:
            mock_async.return_value = "q2-next"
            ChainDispatcher(chain, step0, attempt).handle()

        chain.refresh_from_db()
        step1.refresh_from_db()
        assert chain.current_step_index == 1
        assert step1.qraft_task is not None


class TestChainCancelNotClobbered:
    """Mirror of the parallel cancel-race guard: a cancel committing between
    handle()'s unlocked check and the locked write must survive, not be
    overwritten to SUCCEEDED/FAILED or advanced over."""

    def test_cancel_racing_final_completion_is_not_overwritten(self, db):
        chain, (step0,) = _chain_with_steps(1, success_hook="showcase.tasks.on_success")
        attempt = _chain_attempt(step0, success=True, task_status=TaskStatus.SUCCEEDED)
        dispatcher = ChainDispatcher(chain, step0, attempt)

        # handle()'s outer check saw RUNNING; the cancel commits before the
        # lock is taken (update() keeps the dispatcher's instance stale).
        QraftChainModel.objects.filter(id=chain.id).update(
            status=WorkflowStatus.CANCELLED
        )

        with patch("qraft.dispatchers.q2_async_task") as mock_async:
            dispatcher._handle_step_success()
            mock_async.assert_not_called()

        chain.refresh_from_db()
        assert chain.status == WorkflowStatus.CANCELLED

    def test_cancel_racing_step_failure_is_not_overwritten(self, db):
        chain, (step0, step1) = _chain_with_steps(
            2, failure_hook="showcase.tasks.on_failure"
        )
        attempt = _chain_attempt(step0, success=False, task_status=TaskStatus.EXHAUSTED)
        dispatcher = ChainDispatcher(chain, step0, attempt)

        QraftChainModel.objects.filter(id=chain.id).update(
            status=WorkflowStatus.CANCELLED
        )

        with patch("qraft.dispatchers.q2_async_task") as mock_async:
            dispatcher._handle_step_failure()
            mock_async.assert_not_called()

        chain.refresh_from_db()
        assert chain.status == WorkflowStatus.CANCELLED

    def test_cancel_racing_intermediate_success_queues_nothing(self, db):
        chain, (step0, step1) = _chain_with_steps(2)
        attempt = _chain_attempt(step0, success=True, task_status=TaskStatus.SUCCEEDED)
        dispatcher = ChainDispatcher(chain, step0, attempt)

        QraftChainModel.objects.filter(id=chain.id).update(
            status=WorkflowStatus.CANCELLED
        )

        with patch("qraft.tasks.q2_async_task") as mock_async:
            dispatcher._handle_step_success()
            mock_async.assert_not_called()

        chain.refresh_from_db()
        step1.refresh_from_db()
        assert chain.status == WorkflowStatus.CANCELLED
        assert chain.current_step_index == 0
        assert step1.qraft_task is None


class TestProgressHookDispatch:
    """Progress hooks fire only on genuine partial completions - not for
    duplicate deliveries (counters already final) or cancelled workflows."""

    @pytest.fixture
    def progress_iter(self, db):
        return QraftIterModel.objects.create(
            func="demo.showcase.tasks.noop_task",
            total_count=2,
            status=WorkflowStatus.RUNNING,
            success_hook="showcase.tasks.on_success",
            progress_hook="showcase.tasks.on_progress",
        )

    def test_partial_completion_fires_progress_hook(self, db, progress_iter):
        attempt = _make_attempt("qraft_iter", progress_iter, success=True)

        with patch("qraft.dispatchers.q2_async_task") as mock_async:
            ParallelDispatcher(progress_iter, attempt).handle()

        mock_async.assert_called_once()
        assert mock_async.call_args[0][0] == "showcase.tasks.on_progress"

    def test_duplicate_delivery_fires_no_progress_hook(self, db, progress_iter):
        attempt = _make_attempt("qraft_iter", progress_iter, success=True)

        with patch("qraft.dispatchers.q2_async_task") as mock_async:
            ParallelDispatcher(progress_iter, attempt).handle()
            ParallelDispatcher(progress_iter, attempt).handle()

        # First delivery counted and fired the progress hook; the duplicate
        # is a no-op and must not fire it again.
        assert mock_async.call_count == 1

    def test_cancelled_workflow_fires_no_progress_hook(self, db, progress_iter):
        attempt = _make_attempt("qraft_iter", progress_iter, success=True)
        dispatcher = ParallelDispatcher(progress_iter, attempt)

        QraftIterModel.objects.filter(id=progress_iter.id).update(
            status=WorkflowStatus.CANCELLED
        )

        with patch("qraft.dispatchers.q2_async_task") as mock_async:
            dispatcher.handle()

        mock_async.assert_not_called()

    def test_completion_fires_workflow_hook_not_progress(self, db, progress_iter):
        attempt1 = _make_attempt("qraft_iter", progress_iter, success=True)
        attempt2 = _make_attempt("qraft_iter", progress_iter, success=True)

        with patch("qraft.dispatchers.q2_async_task") as mock_async:
            mock_async.return_value = "q2-hook"
            ParallelDispatcher(progress_iter, attempt1).handle()
            ParallelDispatcher(progress_iter, attempt2).handle()

        hooks_fired = [c[0][0] for c in mock_async.call_args_list]
        assert hooks_fired == [
            "showcase.tasks.on_progress",
            "showcase.tasks.on_success",
        ]

    def test_increment_outcomes_are_distinguished(self, db, progress_iter):
        attempt1 = _make_attempt("qraft_iter", progress_iter, success=True)
        attempt2 = _make_attempt("qraft_iter", progress_iter, success=True)

        assert (
            ParallelDispatcher(progress_iter, attempt1)._atomic_increment()
            is INCREMENT_PARTIAL
        )
        # Redelivery of an already-counted attempt
        assert (
            ParallelDispatcher(progress_iter, attempt1)._atomic_increment()
            is INCREMENT_NOOP
        )
        assert (
            ParallelDispatcher(progress_iter, attempt2)._atomic_increment()
            is INCREMENT_COMPLETE
        )


class TestWorkflowHookDispatchFailureCleanup:
    """If queueing the hook task raises, the placeholder dispatch row must be removed
    so a later retry of the same hook is not permanently blocked."""

    def test_placeholder_row_deleted_on_dispatch_failure(self, db):
        with patch(
            "qraft.dispatchers.q2_async_task", side_effect=RuntimeError("broker down")
        ):
            _dispatch_workflow_hook(
                workflow_type="batch",
                workflow_id="11111111-1111-1111-1111-111111111111",
                hook_type="failure",
                hook_path="showcase.tasks.on_failure",
                hook_args=[],
                hook_kwargs={},
            )

        assert not WorkflowHookDispatch.objects.filter(
            workflow_type="batch",
            workflow_id="11111111-1111-1111-1111-111111111111",
            hook_type="failure",
        ).exists()


@pytest.mark.django_db
class TestChainStepLinkOrdering:
    """
    A chain step must never be visible as queued-but-unlinked.

    _create_workflow_task() enqueues to Django-Q2 as it goes. If the link to
    QraftChainStep commits separately, a fast worker can finish the task
    first; route_workflow_completion() then finds no chain_step, and since
    workflow tasks carry no task-level hooks the chain stalls at RUNNING.
    """

    def _chain_with_step(self):
        chain = QraftChainModel.objects.create(status=WorkflowStatus.RUNNING)
        step = QraftChainStep.objects.create(
            chain=chain, step_index=0, func="demo.showcase.tasks.noop_task"
        )
        return chain, step

    def test_link_and_enqueue_share_one_transaction(self, db):
        from qraft.dispatchers import _queue_chain_step

        chain, step = self._chain_with_step()

        with patch("qraft.tasks.q2_async_task") as mock_async:
            mock_async.return_value = "q2-step-0"
            with patch.object(
                QraftChainStep, "save", side_effect=RuntimeError("link failed")
            ):
                with pytest.raises(RuntimeError):
                    _queue_chain_step(chain, step)

        # The QraftTask is rolled back with the failed link, rather than
        # surviving as an enqueued task no dispatcher can route.
        assert QraftTask.objects.count() == 0

    def test_step_is_linked_once_queued(self, db):
        from qraft.dispatchers import _queue_chain_step

        chain, step = self._chain_with_step()

        with patch("qraft.tasks.q2_async_task") as mock_async:
            mock_async.return_value = "q2-step-0"
            _queue_chain_step(chain, step)

        step.refresh_from_db()
        assert step.qraft_task is not None

    def test_parallel_workflows_link_before_enqueueing(self, db, iter_model):
        """Iter/batch set the FK inside _create_workflow_task, so they are safe."""
        from qraft.tasks import _create_workflow_task

        linked_at_enqueue = []

        def _record(*args, **kwargs):
            linked_at_enqueue.append(
                QraftTask.objects.filter(qraft_iter=iter_model).count()
            )
            return "q2-iter-task"

        with patch("qraft.tasks.q2_async_task", side_effect=_record):
            _create_workflow_task(
                func="demo.showcase.tasks.noop_task",
                args=[],
                kwargs={},
                qraft_options={},
                qraft_iter_id=iter_model.id,
            )

        assert linked_at_enqueue == [1]


@pytest.mark.django_db
class TestTerminalWithoutRetryPolicy:
    """
    A task with no retry policy settles at FAILED, never at EXHAUSTED.

    handle_task_retry() returns early when there is no policy, leaving the
    status the hook handler wrote. Counting only EXHAUSTED dropped every such
    task and hung its workflow at RUNNING forever.
    """

    def test_failed_task_counts_towards_a_parallel_workflow(self, db, iter_model):
        task = QraftTask.objects.create(
            func="demo.showcase.tasks.noop_task",
            status=TaskStatus.FAILED,
            retry_policy={},
            qraft_iter=iter_model,
        )
        attempt = QraftTaskAttempt.objects.create(
            qraft_task=task, attempt_number=1, q2_task_id="q2-nopolicy", success=False
        )

        with patch("qraft.dispatchers.q2_async_task"):
            ParallelDispatcher(iter_model, attempt).handle()

        iter_model.refresh_from_db()
        assert iter_model.completed_count == 1
        assert iter_model.failure_count == 1

    def test_failed_step_fails_the_chain(self, db):
        chain = QraftChainModel.objects.create(status=WorkflowStatus.RUNNING)
        step = QraftChainStep.objects.create(
            chain=chain, step_index=0, func="demo.showcase.tasks.noop_task"
        )
        task = QraftTask.objects.create(func=step.func, status=TaskStatus.FAILED)
        step.qraft_task = task
        step.save(update_fields=["qraft_task"])
        attempt = QraftTaskAttempt.objects.create(
            qraft_task=task,
            attempt_number=1,
            q2_task_id="q2-step-nopolicy",
            success=False,
        )

        with patch("qraft.dispatchers.q2_async_task"):
            ChainDispatcher(chain, step, attempt).handle()

        chain.refresh_from_db()
        assert chain.status == WorkflowStatus.FAILED


@pytest.mark.django_db
class TestCompletionIsDefensive:
    """An equality test the counter steps over wedges the workflow forever."""

    def test_overshooting_the_total_still_completes(self, db, iter_model):
        iter_model.completed_count = iter_model.total_count
        iter_model.save(update_fields=["completed_count"])
        attempt = _make_attempt("qraft_iter", iter_model, success=True)

        with patch("qraft.dispatchers.q2_async_task") as mock_async:
            mock_async.return_value = "q2-hook-overshoot"
            ParallelDispatcher(iter_model, attempt).handle()

        iter_model.refresh_from_db()
        assert iter_model.status == WorkflowStatus.SUCCEEDED
        mock_async.assert_called_once()


@pytest.mark.django_db
class TestReaperRoutesWorkflowCompletion:
    """A reaped workflow member must still reach its dispatcher."""

    def test_reaped_iter_task_advances_the_workflow(self, db, iter_model, monkeypatch):
        from datetime import timedelta

        from django.utils import timezone

        from qraft.reaper import reap_orphans

        iter_model.total_count = 1
        iter_model.save(update_fields=["total_count"])
        task = QraftTask.objects.create(
            func="demo.showcase.tasks.noop_task",
            status=TaskStatus.RUNNING,
            qraft_iter=iter_model,
        )
        QraftTaskAttempt.objects.create(
            qraft_task=task,
            attempt_number=1,
            q2_task_id="q2-orphan-iter",
            heartbeat_at=timezone.now() - timedelta(hours=1),
        )

        with patch("qraft.dispatchers.q2_async_task") as mock_async:
            mock_async.return_value = "q2-hook-reaped"
            assert reap_orphans() == 1

        iter_model.refresh_from_db()
        assert iter_model.completed_count == 1
        assert iter_model.status == WorkflowStatus.FAILED
        mock_async.assert_called_once()
