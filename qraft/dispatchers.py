"""Workflow dispatchers for chain continuation and parallel completion tracking."""

import logging

from django.db import transaction
from django_q.tasks import async_task as q2_async_task

from qraft.conf import executing_cluster
from qraft.hooks import dispatch_hook_once
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

# A failed task settles at EXHAUSTED once a retry policy runs out, but at
# FAILED when it has no policy at all - handle_task_retry() leaves the status
# the hook handler wrote. Treating only EXHAUSTED as terminal silently drops
# every policy-less workflow task, hanging the workflow forever.
TERMINAL_FAILURE_STATUSES = (TaskStatus.EXHAUSTED, TaskStatus.FAILED)

# _atomic_increment outcomes. A duplicate delivery and a cancel-under-lock
# both leave the counters untouched, but they must stay distinguishable from
# a genuine partial completion, or handle() fires progress hooks for no-ops.
INCREMENT_NOOP = "noop"
INCREMENT_PARTIAL = "partial"
INCREMENT_COMPLETE = "complete"


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
        """Handle successful step completion.

        A duplicate hook delivery for the same step must not advance (and
        thus queue) the chain twice. The lock + current_step_index check
        below is the guard: once this step's completion has advanced the
        chain, current_step_index no longer matches the step index, so a
        second delivery bails before touching anything.

        The advance and the next step's creation/enqueue share the locked
        transaction: if the enqueue fails, the advance rolls back with it,
        so a redelivery can retry instead of finding the chain stranded at
        an index whose task was never queued. With the ORM broker the queue
        row is in the same database, so the commit publishes the advance and
        the enqueue together (the argument _queue_chain_step makes for the
        task/link pairing).
        """
        next_step = self.chain.steps.filter(step_index=self.step.step_index + 1).first()
        completed = False

        with transaction.atomic():
            chain = QraftChainModel.objects.select_for_update().get(id=self.chain.id)
            # A cancel can land between handle()'s unlocked check and this
            # lock; re-checking here keeps it from being advanced over or
            # overwritten to SUCCEEDED (mirrors _atomic_increment).
            if chain.status == WorkflowStatus.CANCELLED:
                self.chain = chain
                _logger.debug(
                    "Chain %s: cancelled under lock, skipping step processing",
                    chain.id,
                )
                return
            if chain.current_step_index != self.step.step_index:
                _logger.debug(
                    "Chain %s: step %d already advanced past (current=%d), "
                    "skipping duplicate completion",
                    chain.id,
                    self.step.step_index,
                    chain.current_step_index,
                )
                return
            self.chain = chain

            if not next_step:
                chain.status = WorkflowStatus.SUCCEEDED
                chain.save(update_fields=["status", "date_updated"])
                completed = True
            elif next_step.requires_approval:
                chain.current_step_index = next_step.step_index
                chain.transition_to(WorkflowStatus.WAITING_APPROVAL)
                chain.save(
                    update_fields=["current_step_index", "status", "date_updated"]
                )
                _logger.info(
                    "Chain %s: step %d succeeded, step %d requires approval, parked",
                    chain.id,
                    self.step.step_index,
                    next_step.step_index,
                )
                return
            else:
                chain.current_step_index = next_step.step_index
                chain.save(update_fields=["current_step_index", "date_updated"])
                _queue_chain_step(chain, next_step)

        if completed:
            self._dispatch_chain_hook(success=True)
            _logger.info(
                "Chain %s: final step %d succeeded, chain complete",
                self.chain.id,
                self.step.step_index,
            )
            return

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

        Re-checks cancellation under the row lock: a cancel that committed
        after handle()'s unlocked check must be left alone, not overwritten
        to SUCCEEDED/FAILED (mirrors _atomic_increment).

        Args:
            success: True if chain succeeded, False if failed
        """
        with transaction.atomic():
            chain = QraftChainModel.objects.select_for_update().get(id=self.chain.id)
            if chain.status == WorkflowStatus.CANCELLED:
                self.chain = chain
                _logger.debug(
                    "Chain %s: cancelled under lock, skipping completion", chain.id
                )
                return
            chain.status = (
                WorkflowStatus.SUCCEEDED if success else WorkflowStatus.FAILED
            )
            chain.save(update_fields=["status", "date_updated"])
            self.chain = chain

        self._dispatch_chain_hook(success)

    def _dispatch_chain_hook(self, success: bool):
        """Dispatch the chain-level success/failure hook, if configured."""
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
        """Check whether the task is done failing (no further retry pending)."""
        self.attempt.qraft_task.refresh_from_db(fields=["status"])
        return self.attempt.qraft_task.status in TERMINAL_FAILURE_STATUSES


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
        Cancellation is checked inside _atomic_increment, under the row
        lock - an unlocked pre-check here could never be authoritative.
        """
        # Only process if task is truly done (not being retried)
        if not self._is_task_terminal():
            return

        outcome = self._atomic_increment()

        if outcome is INCREMENT_COMPLETE:
            self._dispatch_workflow_hook()
        elif outcome is INCREMENT_PARTIAL and self.workflow.progress_hook:
            self._dispatch_progress_hook()

    def _is_task_terminal(self) -> bool:
        """
        Check if task is in terminal state (succeeded or exhausted).

        Returns:
            bool: True if task won't be retried
        """
        if self.attempt.success:
            return True
        self.attempt.qraft_task.refresh_from_db()
        return self.attempt.qraft_task.status in TERMINAL_FAILURE_STATUSES

    def _atomic_increment(self) -> str:
        """
        Update the workflow counters under a row lock.

        `select_for_update()` on the workflow serialises concurrent
        completions, and the `counted` flag on QraftTaskAttempt makes a
        redelivered completion a no-op. F() expressions were considered and
        rejected: they would make the increment atomic but could not dedupe,
        and the completion test needs to read the resulting value back, so
        the lock is doing work an F() update cannot replace.

        Cancellation is checked here, under `select_for_update()`: a cancel
        can commit at any point before the lock is taken, so a workflow
        found cancelled is left alone, uncounted and uncompleted, instead
        of being flipped back to SUCCEEDED/FAILED.

        Returns:
            One of INCREMENT_NOOP (duplicate delivery or cancelled - nothing
            counted), INCREMENT_PARTIAL (counted, workflow still running),
            INCREMENT_COMPLETE (counted, workflow now complete).
        """
        with transaction.atomic():
            workflow = (
                type(self.workflow).objects.select_for_update().get(id=self.workflow.id)
            )
            if workflow.status == WorkflowStatus.CANCELLED:
                _logger.debug(
                    "%s %s: cancelled under lock, skipping increment",
                    self.workflow_type,
                    workflow.id,
                )
                self.workflow = workflow
                return INCREMENT_NOOP

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
                return INCREMENT_NOOP

            attempt.counted = True
            attempt.save(update_fields=["counted"])

            workflow.completed_count += 1
            if self.attempt.success:
                workflow.success_count += 1
            else:
                workflow.failure_count += 1

            # `>=`, not `==`: an equality test that the counter steps over
            # once leaves the workflow wedged at RUNNING with its hook never
            # firing, which is a far worse failure than completing twice
            # (WorkflowHookDispatch already makes the hook idempotent).
            is_complete = workflow.completed_count >= workflow.total_count
            if workflow.completed_count > workflow.total_count:
                _logger.warning(
                    "%s %s: completed_count %d exceeds total_count %d",
                    self.workflow_type.capitalize(),
                    workflow.id,
                    workflow.completed_count,
                    workflow.total_count,
                )
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

        return INCREMENT_COMPLETE if is_complete else INCREMENT_PARTIAL

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
                cluster=executing_cluster(),
                ack_failure=True,
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

    Creates a QraftTask, queues it, and links it to the step.

    All three share one transaction because `_create_workflow_task` enqueues
    to Django-Q2 as it goes. A worker that picks the task up before the link
    commits finds no `chain_step` in `route_workflow_completion`, and since
    workflow tasks carry no task-level hooks the chain then stalls at RUNNING
    forever. With the ORM broker the queue row is in the same database, so
    the commit makes the enqueue and the link visible together.
    """
    from qraft.tasks import _create_workflow_task

    with transaction.atomic():
        qraft_task = _create_workflow_task(
            func=step.func,
            args=step.task_args,
            kwargs=step.task_kwargs,
            qraft_options=step.qraft_options,
        )
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
    dispatch_hook_once(
        WorkflowHookDispatch,
        {
            "workflow_type": workflow_type,
            "workflow_id": workflow_id,
            "hook_type": hook_type,
        },
        hook_path,
        lambda: q2_async_task(
            hook_path,
            *hook_args,
            hook=None,  # Prevent recursion
            cluster=executing_cluster(),
            # Nothing above this layer retries a workflow hook, so an
            # unacknowledged failure would be redelivered indefinitely.
            ack_failure=True,
            **hook_kwargs,
        ),
    )


def route_workflow_completion(qraft_task, attempt) -> bool:
    """
    Hand a completed task to its workflow dispatcher.

    Args:
        qraft_task: QraftTask instance
        attempt: QraftTaskAttempt instance

    Returns:
        bool: False if the task belongs to no workflow, so the caller falls
        back to task-level hooks.
    """
    try:
        step = qraft_task.chain_step
    except QraftChainStep.DoesNotExist:
        step = None

    if step is not None:
        ChainDispatcher(step.chain, step, attempt).handle()
        return True

    workflow = qraft_task.qraft_iter or qraft_task.qraft_batch
    if workflow is not None:
        ParallelDispatcher(workflow, attempt).handle()
        return True

    return False
