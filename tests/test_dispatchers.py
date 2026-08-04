"""Regression tests for workflow dispatchers (v1.1.1 race/idempotency fixes)."""

from unittest.mock import patch

import pytest

# Disable hook path import validation for tests using fake module paths
pytestmark = pytest.mark.usefixtures("_disable_hook_validation")

from qraft.dispatchers import (  # noqa: E402
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
        """A failed attempt whose task will still retry (not EXHAUSTED) is ignored."""
        task = QraftTask.objects.create(
            func="demo.showcase.tasks.noop_task",
            status=TaskStatus.FAILED,  # not yet EXHAUSTED - retry pending
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
        """Failure while task is still FAILED (not EXHAUSTED) means a retry is
        pending - the chain must not be failed or advanced."""
        chain, (step0, step1) = self._chain_with_steps(2)
        attempt = self._attempt_for_step(
            step0, success=False, task_status=TaskStatus.FAILED
        )

        with patch("qraft.tasks.q2_async_task") as mock_next_step:
            ChainDispatcher(chain, step0, attempt).handle()
            mock_next_step.assert_not_called()

        chain.refresh_from_db()
        step1.refresh_from_db()
        assert chain.status == WorkflowStatus.RUNNING
        assert chain.current_step_index == 0
        assert step1.qraft_task is None

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
