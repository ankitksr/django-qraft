"""Workflow dispatchers for chain continuation and parallel completion tracking."""

import logging

from django.db import transaction
from django.utils import timezone
from django_q.tasks import async_task as q2_async_task

from qraft import metrics, signals
from qraft.brokers import broker_for_cluster
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

TERMINAL_WORKFLOW_STATUSES = (
    WorkflowStatus.SUCCEEDED,
    WorkflowStatus.FAILED,
    WorkflowStatus.CANCELLED,
)


def settle_workflow(model, workflow_id, status: str, workflow_type: str):
    """
    Move a workflow to a terminal status exactly once.

    One conditional update on `settled_at`: a final-step redelivery, an
    already-terminal `_complete_chain`, a cancel racing a completion and a
    replay against a row from before the column existed (terminal status, null
    `settled_at`) all
    match zero rows. `workflow_settled` and the `workflow.settled` counter
    fire only when the update matched; the caller dispatches the workflow
    hook on the same condition, with `WorkflowHookDispatch` as the second
    line of defence.

    Returns the refreshed workflow when this call settled it, else None.
    """
    now = timezone.now()
    updated = (
        model.objects.filter(pk=workflow_id, settled_at__isnull=True)
        .exclude(status__in=TERMINAL_WORKFLOW_STATUSES)
        .update(status=status, settled_at=now, date_updated=now)
    )
    if not updated:
        return None
    workflow = model.objects.get(pk=workflow_id)
    signals.send(
        signals.workflow_settled,
        model,
        signals.workflow_payload(workflow, workflow_type, status),
    )
    metrics.counter_on_commit(
        "qraft.workflow.settled", workflow_type=workflow_type, outcome=status
    )
    return workflow


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
        so the chain stays clean at step i instead of stranded at an index
        whose task was never queued. With the ORM broker the queue row is
        in the same database, so the commit publishes the advance and the
        enqueue together (the argument _queue_chain_step makes for the
        task/link pairing). On a non-ORM broker a broker-down enqueue still
        rolls the ORM advance back, but there is no redelivery actor
        (ack_failure=True; the monitor delivers once) - the chain stays
        RUNNING at step i until manual intervention.
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
                # The final step never advances current_step_index, so the
                # index check above passes on a replayed completion; the
                # settlement compare-and-swap is what makes this fire once.
                settled = settle_workflow(
                    QraftChainModel, chain.id, WorkflowStatus.SUCCEEDED, "chain"
                )
                completed = settled is not None
                self.chain = settled or chain
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

        if not next_step:
            if completed:
                self._dispatch_chain_hook(success=True)
                _logger.info(
                    "Chain %s: final step %d succeeded, chain complete",
                    self.chain.id,
                    self.step.step_index,
                )
            else:
                _logger.debug(
                    "Chain %s: already settled, skipping replayed final completion",
                    self.chain.id,
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
            settled = settle_workflow(
                QraftChainModel,
                chain.id,
                WorkflowStatus.SUCCEEDED if success else WorkflowStatus.FAILED,
                "chain",
            )
            self.chain = settled or chain

        if settled is None:
            _logger.debug("Chain %s: already settled, skipping completion", chain.id)
            return
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
                context=(
                    workflow_hook_context(self.chain, "chain")
                    if self.chain.hook_context
                    else None
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

            workflow.save(update_fields=update_fields)
            self.workflow = workflow

            if is_complete:
                settled = settle_workflow(
                    type(workflow),
                    workflow.id,
                    (
                        WorkflowStatus.SUCCEEDED
                        if workflow.failure_count == 0
                        else WorkflowStatus.FAILED
                    ),
                    self.workflow_type,
                )
                if settled is None:
                    _logger.debug(
                        "%s %s: already settled, not completing again",
                        self.workflow_type,
                        workflow.id,
                    )
                    return INCREMENT_NOOP
                self.workflow = settled

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
                    self.workflow.status,
                )

        return INCREMENT_COMPLETE if is_complete else INCREMENT_PARTIAL

    def _dispatch_progress_hook(self):
        """Dispatch progress hook after each task completion (non-terminal)."""
        from qraft.runner import dispatch_spec

        func, hook_args, hook_kwargs = dispatch_spec(
            self.workflow.progress_hook,
            (),
            {
                "workflow_id": str(self.workflow.id),
                "workflow_type": self.workflow_type,
                "completed_count": self.workflow.completed_count,
                "total_count": self.workflow.total_count,
                "success_count": self.workflow.success_count,
                "failure_count": self.workflow.failure_count,
            },
        )
        try:
            q2_async_task(
                func,
                *hook_args,
                **hook_kwargs,
                hook=None,
                cluster=executing_cluster(),
                broker=broker_for_cluster(executing_cluster()),
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
                context=(
                    workflow_hook_context(self.workflow, self.workflow_type)
                    if self.workflow.hook_context
                    else None
                ),
            )


def workflow_hook_context(workflow, workflow_type: str) -> dict:
    """
    The `context` a workflow hook receives under `hook_context=True`.

    The same shape whatever settled the workflow. A cancelled fan-out is
    exactly where the counters are worth reading - they say how much of the
    work had already finished when the cancel landed - so they are not
    dropped for it.
    """
    context = {
        "workflow_id": str(workflow.id),
        "workflow_type": workflow_type,
        "outcome": workflow.status,
    }
    if workflow_type == "chain":
        context["current_step_index"] = workflow.current_step_index
    else:
        context.update(
            completed_count=workflow.completed_count,
            total_count=workflow.total_count,
            success_count=workflow.success_count,
            failure_count=workflow.failure_count,
        )
    return context


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
    from qraft.tasks import _create_workflow_task, workflow_member_labels

    with transaction.atomic():
        qraft_task = _create_workflow_task(
            func=step.func,
            args=step.task_args,
            kwargs=step.task_kwargs,
            qraft_options=step.qraft_options,
            labels=workflow_member_labels(chain),
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
    context: dict | None = None,
):
    """
    Dispatch workflow-level hook with idempotency.

    Args:
        workflow_type: 'chain', 'iter', 'batch' or 'graph'
        workflow_id: UUID of the workflow
        hook_type: 'success', 'failure', 'cancelled' or 'settled'
        hook_path: Dotted path to hook function
        hook_args: Positional args for hook
        hook_kwargs: Keyword args for hook
        context: Extra `context` keyword argument, when the workflow opted in
    """
    from qraft.runner import dispatch_spec

    if context is not None:
        hook_kwargs = {**(hook_kwargs or {}), "context": context}
    func, hook_args, hook_kwargs = dispatch_spec(hook_path, hook_args, hook_kwargs)
    dispatch_hook_once(
        WorkflowHookDispatch,
        {
            "workflow_type": workflow_type,
            "workflow_id": workflow_id,
            "hook_type": hook_type,
        },
        hook_path,
        lambda: q2_async_task(
            func,
            *hook_args,
            hook=None,  # Prevent recursion
            cluster=executing_cluster(),
            # Named explicitly: `cluster=` alone would resolve the broker
            # against this process's Conf, not the target cluster's.
            broker=broker_for_cluster(executing_cluster()),
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
        # The task a step is bound to is the chain's generation marker.
        # Callers route with a task they joined the step onto earlier (the
        # hook handler's select_related, the reaper's replay batch) and
        # resume() rebinds the step to a fresh task in between, so the
        # binding is re-read rather than trusted: a completion from the
        # generation before a resume must not settle the one after it.
        bound = (
            QraftChainStep.objects.filter(pk=step.pk)
            .values_list("qraft_task_id", flat=True)
            .first()
        )
        if bound != qraft_task.id:
            _logger.info(
                "Chain %s step %d is bound to task %s, not %s; dropping the "
                "completion of a superseded generation",
                step.chain_id,
                step.step_index,
                bound,
                qraft_task.id,
            )
            return True
        ChainDispatcher(step.chain, step, attempt).handle()
        return True

    workflow = qraft_task.qraft_iter or qraft_task.qraft_batch
    if workflow is not None:
        ParallelDispatcher(workflow, attempt).handle()
        return True

    return False
