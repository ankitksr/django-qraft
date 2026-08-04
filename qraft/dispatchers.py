"""Workflow dispatchers for chain continuation and parallel completion tracking."""

import logging

from django.db import transaction
from django_q.tasks import async_task as q2_async_task

from qraft.models import (
    QraftBatchModel,
    QraftChainModel,
    QraftChainStep,
    QraftIterModel,
    QraftTaskAttempt,
    TaskStatus,
    WorkflowHookDispatch,
    WorkflowStatus,
)

_logger = logging.getLogger("qraft.dispatchers")


class ChainDispatcher:
    """Handles chain step completion and sequential continuation."""

    def __init__(
        self,
        chain: QraftChainModel,
        step: QraftChainStep,
        attempt: QraftTaskAttempt,
    ):
        """
        Initialize chain dispatcher.

        Args:
            chain: The QraftChainModel instance
            step: The QraftChainStep that just completed
            attempt: The QraftTaskAttempt record for this execution
        """
        self.chain = chain
        self.step = step
        self.attempt = attempt

    def handle(self):
        """
        Handle chain step completion.

        If step succeeded: queue next step or complete chain
        If step failed and retries exhausted: mark chain as failed
        If step failed but will retry: do nothing (let retry happen)
        """
        # Skip processing if chain was cancelled
        self.chain.refresh_from_db()
        if self.chain.status == WorkflowStatus.CANCELLED:
            _logger.debug("Chain %s cancelled, skipping step processing", self.chain.id)
            return

        if self.attempt.success:
            self._handle_step_success()
        else:
            # Check if this was final attempt (retries exhausted)
            if self._is_task_exhausted():
                self._handle_step_failure()
            # Else: retry is being scheduled, do nothing

    def _handle_step_success(self):
        """Handle successful step completion."""
        # Check if there's a next step
        next_step = self.chain.steps.filter(step_index=self.step.step_index + 1).first()

        if not next_step:
            # Chain complete
            self._complete_chain(success=True)
            _logger.info(
                "Chain %s: final step %d succeeded, chain complete",
                self.chain.id,
                self.step.step_index,
            )
            return

        if next_step.requires_approval:
            with transaction.atomic():
                chain = QraftChainModel.objects.select_for_update().get(
                    id=self.chain.id
                )
                chain.current_step_index = next_step.step_index
                chain.transition_to(WorkflowStatus.WAITING_APPROVAL)
                chain.save(
                    update_fields=["current_step_index", "status", "date_updated"]
                )
            self.chain = chain
            _logger.info(
                "Chain %s: step %d succeeded, step %d requires approval, parked",
                self.chain.id,
                self.step.step_index,
                next_step.step_index,
            )
            return

        # Queue next step
        with transaction.atomic():
            self.chain.current_step_index = next_step.step_index
            self.chain.save(update_fields=["current_step_index", "date_updated"])

        _queue_chain_step(self.chain, next_step)
        _logger.info(
            "Chain %s: step %d succeeded, queued step %d",
            self.chain.id,
            self.step.step_index,
            next_step.step_index,
        )

    def _handle_step_failure(self):
        """Handle step failure after retries exhausted."""
        self._complete_chain(success=False)
        _logger.warning(
            "Chain %s: step %d failed (exhausted retries), chain failed",
            self.chain.id,
            self.step.step_index,
        )

    def _complete_chain(self, success: bool):
        """
        Mark chain as complete and dispatch workflow hook.

        Args:
            success: True if chain succeeded, False if failed
        """
        with transaction.atomic():
            self.chain.status = (
                WorkflowStatus.SUCCEEDED if success else WorkflowStatus.FAILED
            )
            self.chain.save(update_fields=["status", "date_updated"])

        # Dispatch chain-level hook
        hook = self.chain.success_hook if success else self.chain.failure_hook
        if hook:
            _dispatch_workflow_hook(
                workflow_type="chain",
                workflow_id=self.chain.id,
                hook_type="success" if success else "failure",
                hook_path=hook,
                hook_args=(
                    self.chain.success_args if success else self.chain.failure_args
                ),
                hook_kwargs=(
                    self.chain.success_kwargs if success else self.chain.failure_kwargs
                ),
            )

    def _is_task_exhausted(self) -> bool:
        """Check if the task has exhausted all retries."""
        return self.attempt.qraft_task.status == TaskStatus.EXHAUSTED


class ParallelDispatcher:
    """Handles iter/batch task completion with atomic counter updates."""

    def __init__(
        self,
        workflow: QraftIterModel | QraftBatchModel,
        attempt: QraftTaskAttempt,
    ):
        """
        Initialize parallel dispatcher.

        Args:
            workflow: QraftIterModel or QraftBatchModel instance
            attempt: The QraftTaskAttempt record for this execution
        """
        self.workflow = workflow
        self.attempt = attempt
        self.workflow_type = "iter" if isinstance(workflow, QraftIterModel) else "batch"

    def handle(self):
        """
        Handle task completion in parallel workflow.

        Only processes terminal states (success or exhausted).
        Uses atomic counter updates to prevent race conditions.
        """
        # Skip processing if workflow was cancelled
        self.workflow.refresh_from_db()
        if self.workflow.status == WorkflowStatus.CANCELLED:
            _logger.debug(
                "%s %s cancelled, skipping task processing",
                self.workflow_type,
                self.workflow.id,
            )
            return

        # Only process if task is truly done (not being retried)
        if not self._is_task_terminal():
            return

        is_complete = self._atomic_increment()

        if is_complete:
            self._dispatch_workflow_hook()
        elif self.workflow.progress_hook:
            self._dispatch_progress_hook()

    def _is_task_terminal(self) -> bool:
        """
        Check if task is in terminal state (succeeded or exhausted).

        Returns:
            bool: True if task won't be retried
        """
        if self.attempt.success:
            return True
        # Check if task status is EXHAUSTED (all retries done)
        self.attempt.qraft_task.refresh_from_db()
        return self.attempt.qraft_task.status == TaskStatus.EXHAUSTED

    def _atomic_increment(self) -> bool:
        """
        Atomically update counters using F() expressions.

        Uses the `counted` flag on QraftTaskAttempt for idempotency -
        if this attempt was already counted, skip the increment.

        Returns:
            bool: True if workflow is now complete
        """
        from qraft.models import QraftTaskAttempt

        with transaction.atomic():
            # Lock the attempt to check/set counted flag atomically
            attempt = QraftTaskAttempt.objects.select_for_update().get(
                id=self.attempt.id
            )
            if attempt.counted:
                _logger.debug(
                    "%s %s: attempt %s already counted, skipping",
                    self.workflow_type,
                    self.workflow.id,
                    attempt.id,
                )
                return False

            attempt.counted = True
            attempt.save(update_fields=["counted"])

            workflow = (
                type(self.workflow).objects.select_for_update().get(id=self.workflow.id)
            )

            workflow.completed_count += 1
            if self.attempt.success:
                workflow.success_count += 1
            else:
                workflow.failure_count += 1

            is_complete = workflow.completed_count == workflow.total_count
            update_fields = [
                "completed_count",
                "success_count",
                "failure_count",
                "date_updated",
            ]

            if is_complete:
                workflow.status = (
                    WorkflowStatus.SUCCEEDED
                    if workflow.failure_count == 0
                    else WorkflowStatus.FAILED
                )
                update_fields.append("status")

            workflow.save(update_fields=update_fields)
            self.workflow = workflow

            _logger.debug(
                "%s %s: task completed (%s), counters: %d/%d (success=%d, failure=%d)",
                self.workflow_type.capitalize(),
                self.workflow.id,
                "success" if self.attempt.success else "failure",
                workflow.completed_count,
                workflow.total_count,
                workflow.success_count,
                workflow.failure_count,
            )

            if is_complete:
                _logger.info(
                    "%s %s: all tasks complete, status=%s",
                    self.workflow_type.capitalize(),
                    workflow.id,
                    workflow.status,
                )
                return True

        return False

    def _dispatch_progress_hook(self):
        """Dispatch progress hook after each task completion (non-terminal)."""
        try:
            q2_async_task(
                self.workflow.progress_hook,
                workflow_id=str(self.workflow.id),
                workflow_type=self.workflow_type,
                completed_count=self.workflow.completed_count,
                total_count=self.workflow.total_count,
                success_count=self.workflow.success_count,
                failure_count=self.workflow.failure_count,
                hook=None,
            )
        except Exception as e:
            _logger.warning(
                "Failed to dispatch progress hook for %s %s: %s",
                self.workflow_type,
                self.workflow.id,
                e,
            )

    def _dispatch_workflow_hook(self):
        """Dispatch workflow-level hook based on completion outcome."""
        success = self.workflow.failure_count == 0
        hook = self.workflow.success_hook if success else self.workflow.failure_hook

        if hook:
            _dispatch_workflow_hook(
                workflow_type=self.workflow_type,
                workflow_id=self.workflow.id,
                hook_type="success" if success else "failure",
                hook_path=hook,
                hook_args=(
                    self.workflow.success_args
                    if success
                    else self.workflow.failure_args
                ),
                hook_kwargs=(
                    self.workflow.success_kwargs
                    if success
                    else self.workflow.failure_kwargs
                ),
            )


def _queue_chain_step(chain: QraftChainModel, step: QraftChainStep):
    """
    Internal helper to queue a chain step as a QraftTask.

    Creates a QraftTask and links it to the step.
    """
    from qraft.tasks import _create_workflow_task

    # Create QraftTask for this step (doesn't queue to Q2 yet)
    qraft_task = _create_workflow_task(
        func=step.func,
        args=step.task_args,
        kwargs=step.task_kwargs,
        qraft_options=step.qraft_options,
    )

    # Link step to task
    step.qraft_task = qraft_task
    step.save(update_fields=["qraft_task"])

    _logger.debug(
        "Queued chain step %d (chain=%s, task=%s)",
        step.step_index,
        chain.id,
        qraft_task.id,
    )


def _dispatch_workflow_hook(
    workflow_type: str,
    workflow_id: str,
    hook_type: str,
    hook_path: str,
    hook_args: list,
    hook_kwargs: dict,
):
    """
    Dispatch workflow-level hook with idempotency.

    Args:
        workflow_type: 'chain', 'iter', or 'batch'
        workflow_id: UUID of the workflow
        hook_type: 'success' or 'failure'
        hook_path: Dotted path to hook function
        hook_args: Positional args for hook
        hook_kwargs: Keyword args for hook
    """
    import uuid as _uuid

    # Use get_or_create with placeholder to avoid TOCTOU race
    placeholder_id = f"p-{_uuid.uuid4().hex[:30]}"
    try:
        with transaction.atomic():
            dispatch, created = WorkflowHookDispatch.objects.get_or_create(
                workflow_type=workflow_type,
                workflow_id=workflow_id,
                hook_type=hook_type,
                defaults={
                    "hook_path": hook_path,
                    "q2_task_id": placeholder_id,
                },
            )

        if not created:
            _logger.debug(
                "Workflow hook already dispatched: %s %s %s (q2_task_id=%s)",
                workflow_type,
                workflow_id,
                hook_type,
                dispatch.q2_task_id,
            )
            return

        # Queue hook as async task
        q2_task_id = q2_async_task(
            hook_path,
            *hook_args,
            hook=None,  # Prevent recursion
            **hook_kwargs,
        )

        # Update with actual Q2 task ID
        dispatch.q2_task_id = q2_task_id
        dispatch.save(update_fields=["q2_task_id"])

        _logger.info(
            "Dispatched %s hook for %s %s (q2_task_id=%s)",
            hook_type,
            workflow_type,
            workflow_id,
            q2_task_id,
        )
    except Exception as e:
        # Clean up dispatch record on failure
        WorkflowHookDispatch.objects.filter(
            workflow_type=workflow_type,
            workflow_id=workflow_id,
            hook_type=hook_type,
            q2_task_id=placeholder_id,
        ).delete()
        _logger.exception(
            "Failed to dispatch %s hook for %s %s: %s",
            hook_type,
            workflow_type,
            workflow_id,
            e,
        )
