"""Tests for chain human-in-the-loop approval steps."""

from unittest.mock import patch

import pytest

# Disable hook path validation for tests with fake module paths
pytestmark = pytest.mark.usefixtures("_disable_hook_validation")

from qraft.chain import QraftChain  # noqa: E402
from qraft.dispatchers import ChainDispatcher  # noqa: E402
from qraft.models import (  # noqa: E402
    InvalidStatusTransition,
    QraftTask,
    QraftTaskAttempt,
    TaskStatus,
    WorkflowHookDispatch,
    WorkflowStatus,
)


class TestRunWithApprovalGatedFirstStep:
    """run() parks immediately when step 0 requires approval."""

    def test_run_parks_at_waiting_approval_without_queueing(self, db):
        chain = QraftChain()
        chain.append("demo.showcase.tasks.noop_task", requires_approval=True)
        chain.append("demo.showcase.tasks.noop_task")

        with patch("qraft.tasks.q2_async_task") as mock_async:
            chain_id = chain.run()
            mock_async.assert_not_called()

        assert chain_id == chain.id
        assert chain.status == WorkflowStatus.WAITING_APPROVAL

        first_step = chain._model.steps.get(step_index=0)
        assert first_step.qraft_task is None


class TestMidChainApproval:
    """A gated step mid-chain parks the dispatcher; approve() resumes it."""

    def _run_two_step_chain(self):
        chain = QraftChain()
        chain.append("demo.showcase.tasks.noop_task", 1)
        chain.append("demo.showcase.tasks.noop_task", 2, requires_approval=True)
        with patch("qraft.tasks.q2_async_task") as mock_async:
            mock_async.return_value = "q2-step-0"
            chain.run()
        return chain

    def test_dispatcher_parks_chain_before_gated_step(self, db):
        chain = self._run_two_step_chain()
        step0 = chain._model.steps.get(step_index=0)
        step1 = chain._model.steps.get(step_index=1)

        task = QraftTask.objects.create(func=step0.func, status=TaskStatus.SUCCEEDED)
        step0.qraft_task = task
        step0.save(update_fields=["qraft_task"])
        attempt = QraftTaskAttempt.objects.create(
            qraft_task=task,
            attempt_number=1,
            q2_task_id=f"q2-{task.id}",
            success=True,
        )

        with patch("qraft.tasks.q2_async_task") as mock_async:
            ChainDispatcher(chain._model, step0, attempt).handle()
            mock_async.assert_not_called()

        chain._model.refresh_from_db()
        step1.refresh_from_db()
        assert chain._model.status == WorkflowStatus.WAITING_APPROVAL
        assert chain._model.current_step_index == 1
        assert step1.qraft_task is None

    def test_approve_queues_pending_step_and_resumes_chain(self, db):
        chain = self._run_two_step_chain()
        step0 = chain._model.steps.get(step_index=0)
        step1 = chain._model.steps.get(step_index=1)
        task = QraftTask.objects.create(func=step0.func, status=TaskStatus.SUCCEEDED)
        step0.qraft_task = task
        step0.save(update_fields=["qraft_task"])
        attempt = QraftTaskAttempt.objects.create(
            qraft_task=task,
            attempt_number=1,
            q2_task_id=f"q2-{task.id}",
            success=True,
        )
        with patch("qraft.tasks.q2_async_task"):
            ChainDispatcher(chain._model, step0, attempt).handle()

        with patch("qraft.tasks.q2_async_task") as mock_async:
            mock_async.return_value = "q2-step-1"
            chain_id = chain.approve()

        assert chain_id == chain.id
        assert chain.status == WorkflowStatus.RUNNING
        step1.refresh_from_db()
        assert step1.qraft_task is not None
        mock_async.assert_called_once()

    def test_full_completion_after_approval(self, db):
        """Chain finishes normally once the approved step's completion is
        reported back through the dispatcher, mirroring the hook-handler flow."""
        chain = self._run_two_step_chain()
        step0 = chain._model.steps.get(step_index=0)
        step1 = chain._model.steps.get(step_index=1)
        task0 = QraftTask.objects.create(func=step0.func, status=TaskStatus.SUCCEEDED)
        step0.qraft_task = task0
        step0.save(update_fields=["qraft_task"])
        attempt0 = QraftTaskAttempt.objects.create(
            qraft_task=task0,
            attempt_number=1,
            q2_task_id=f"q2-{task0.id}",
            success=True,
        )
        with patch("qraft.tasks.q2_async_task"):
            ChainDispatcher(chain._model, step0, attempt0).handle()

        with patch("qraft.tasks.q2_async_task") as mock_async:
            mock_async.return_value = "q2-step-1"
            chain.approve()

        step1.refresh_from_db()
        task1 = step1.qraft_task
        task1.status = TaskStatus.SUCCEEDED
        task1.save(update_fields=["status"])
        attempt1 = QraftTaskAttempt.objects.get(qraft_task=task1, attempt_number=1)
        attempt1.success = True
        attempt1.save(update_fields=["success"])

        ChainDispatcher(chain._model, step1, attempt1).handle()

        chain._model.refresh_from_db()
        assert chain._model.status == WorkflowStatus.SUCCEEDED


class TestReject:
    """reject() cancels a parked chain and fires the on_cancelled hook."""

    def test_reject_cancels_chain_and_dispatches_hook(self, db):
        chain = QraftChain(on_cancelled="showcase.tasks.on_cancelled")
        chain.append("demo.showcase.tasks.noop_task", requires_approval=True)
        with patch("qraft.tasks.q2_async_task"):
            chain.run()

        with patch("qraft.dispatchers.q2_async_task") as mock_async:
            mock_async.return_value = "q2-cancel-hook"
            chain_id = chain.reject(reason="not ready")

        assert chain_id == chain.id
        assert chain.status == WorkflowStatus.CANCELLED
        mock_async.assert_called_once()
        assert mock_async.call_args[0][0] == "showcase.tasks.on_cancelled"
        assert (
            WorkflowHookDispatch.objects.filter(
                workflow_type="chain",
                workflow_id=chain.id,
                hook_type="cancelled",
            ).count()
            == 1
        )

    def test_reject_without_hook_configured_does_not_dispatch(self, db):
        chain = QraftChain()
        chain.append("demo.showcase.tasks.noop_task", requires_approval=True)
        with patch("qraft.tasks.q2_async_task"):
            chain.run()

        with patch("qraft.dispatchers.q2_async_task") as mock_async:
            chain.reject()
            mock_async.assert_not_called()

        assert chain.status == WorkflowStatus.CANCELLED


class TestApproveInvalidState:
    """approve() only valid from WAITING_APPROVAL (RUNNING->CANCELLED via
    reject() is a legitimate transition, same as plain cancel())."""

    def test_approve_on_running_chain_raises(self, db):
        chain = QraftChain()
        chain.append("demo.showcase.tasks.noop_task")
        with patch("qraft.tasks.q2_async_task") as mock_async:
            mock_async.return_value = "q2-task-0"
            chain.run()

        assert chain.status == WorkflowStatus.RUNNING
        with pytest.raises(InvalidStatusTransition):
            chain.approve()
