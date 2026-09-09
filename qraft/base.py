"""Base workflow class with shared patterns for chain/iter/batch."""

import logging
import time
from uuid import UUID

from django.db import transaction
from django.utils import timezone
from django.utils.module_loading import import_string

from qraft import metrics, signals
from qraft.models import InvalidStatusTransition, WorkflowStatus
from qraft.results import TaskResult, WorkflowResult

_logger = logging.getLogger("qraft.workflow")

# Polling constants for result() with exponential backoff
_POLL_INITIAL_INTERVAL = 0.05  # 50ms
_POLL_MAX_INTERVAL = 2.0  # 2 seconds
_POLL_BACKOFF_FACTOR = 1.5

# Terminal statuses (no more processing expected)
_TERMINAL_STATUSES = frozenset(
    {
        WorkflowStatus.SUCCEEDED,
        WorkflowStatus.FAILED,
        WorkflowStatus.CANCELLED,
    }
)

# Statuses cancel() may still move to CANCELLED. Matches VALID_TRANSITIONS
# that list CANCELLED as a target; a completion that already committed past
# these must not be overwritten by a late cancel.
_CANCELLABLE_STATUSES = frozenset(
    {
        WorkflowStatus.PENDING,
        WorkflowStatus.RUNNING,
        WorkflowStatus.WAITING_APPROVAL,
    }
)

# Statuses that stop result() polling: terminal states plus WAITING_APPROVAL,
# since a chain parked on approval won't progress without external action.
_POLL_STOP_STATUSES = _TERMINAL_STATUSES | {WorkflowStatus.WAITING_APPROVAL}


def _validate_hook_path(hook_path: str | None) -> None:
    """Validate a hook dotted path is importable.

    Args:
        hook_path: Dotted import path to validate

    Raises:
        ValueError: If the path cannot be imported
    """
    if not hook_path:
        return
    try:
        import_string(hook_path)
    except ImportError as e:
        raise ValueError(f"Hook path '{hook_path}' cannot be imported: {e}") from e


class BaseWorkflow:
    """
    Base class providing shared patterns for workflow primitives.

    Subclasses must set `_model` to the Django model instance.
    """

    _model = None
    _workflow_type: str = ""  # "chain", "iter", or "batch"

    @property
    def id(self) -> UUID:
        """Get the workflow UUID."""
        return self._model.id

    @property
    def status(self) -> str:
        """Get current workflow status (refreshed from DB)."""
        self._model.refresh_from_db()
        return self._model.status

    def cancel(self) -> bool:
        """
        Cancel the workflow.

        Sets status to CANCELLED via a conditional UPDATE so a completion
        that already committed to a terminal state cannot be overwritten.
        This stops future orchestration - no new steps queued, no member
        retries scheduled, no success/failure hooks fired, no counters
        updated - and dispatches `on_cancelled` (idempotently, shared with
        reject()). It does not revoke member tasks already queued or running;
        they complete and their outcome is ignored.

        Returns:
            True if the cancel took effect.

        Raises:
            InvalidStatusTransition: If the workflow can't be cancelled from
                its current state (including when a race already committed a
                terminal status).
        """
        # The same settlement compare-and-swap the dispatchers use, so a cancel
        # racing a completion produces exactly one settlement. The announcement
        # shares the transaction: nothing here replays a workflow cancel, so a
        # crash between the two would leave the run's stage row believing this
        # unit is still running.
        now = timezone.now()
        with transaction.atomic():
            updated = self._model.__class__.objects.filter(
                pk=self._model.pk,
                status__in=_CANCELLABLE_STATUSES,
                settled_at__isnull=True,
            ).update(
                status=WorkflowStatus.CANCELLED,
                settled_at=now,
                date_updated=now,
            )
            if updated:
                self._model.status = WorkflowStatus.CANCELLED
                self._model.settled_at = now
                _logger.info(
                    "%s %s cancelled",
                    self._workflow_type.capitalize(),
                    self._model.id,
                )
                self._announce_cancelled()
        if updated:
            return True

        self._model.refresh_from_db()
        raise InvalidStatusTransition(
            f"Cannot transition from {self._model.status} to {WorkflowStatus.CANCELLED}"
        )

    def _announce_cancelled(self) -> None:
        """Signal, count and hook a cancellation that just took effect."""
        signals.send(
            signals.workflow_settled,
            self._model.__class__,
            signals.workflow_payload(
                self._model, self._workflow_type, WorkflowStatus.CANCELLED
            ),
        )
        metrics.counter_on_commit(
            "qraft.workflow.settled",
            workflow_type=self._workflow_type,
            outcome=WorkflowStatus.CANCELLED,
        )
        if self._model.on_cancelled:
            from qraft.dispatchers import _dispatch_workflow_hook, workflow_hook_context

            context = None
            if self._model.hook_context:
                # Re-read first: the counters this instance holds were read
                # before the members ran, and how much had finished when the
                # cancel landed is the whole point of the context.
                self._model.refresh_from_db()
                context = workflow_hook_context(self._model, self._workflow_type)
            _dispatch_workflow_hook(
                workflow_type=self._workflow_type,
                workflow_id=self._model.id,
                hook_type="cancelled",
                hook_path=self._model.on_cancelled,
                hook_args=[],
                hook_kwargs={},
                context=context,
            )

    def _poll_until_terminal(self, timeout_ms: int | None) -> None:
        """
        Poll until workflow reaches a terminal state with exponential backoff.

        Also returns on WAITING_APPROVAL: that status won't progress without
        an external approve()/reject() call, so a caller blocked in result()
        would otherwise hang until timeout. Control is handed back instead.

        Args:
            timeout_ms: Timeout in milliseconds. None means no waiting.

        Raises:
            TimeoutError: If workflow doesn't complete within timeout
        """
        if not timeout_ms:
            return

        start = time.time()
        interval = _POLL_INITIAL_INTERVAL

        while self.status not in _POLL_STOP_STATUSES:
            elapsed_ms = (time.time() - start) * 1000
            if elapsed_ms > timeout_ms:
                raise TimeoutError(
                    f"{self._workflow_type.capitalize()} did not complete "
                    f"within {timeout_ms}ms"
                )
            time.sleep(interval)
            interval = min(interval * _POLL_BACKOFF_FACTOR, _POLL_MAX_INTERVAL)

    def _build_workflow_result(self, tasks_qs) -> WorkflowResult:
        """Build a WorkflowResult from a queryset of QraftTask objects."""
        return WorkflowResult(
            task_results=[TaskResult.from_qraft_task(task) for task in tasks_qs]
        )

    def __repr__(self):
        return (
            f"<{self.__class__.__name__} id={self._model.id}"
            f" status={self._model.status}>"
        )


class ParallelWorkflow(BaseWorkflow):
    """
    Shared surface for the fan-out primitives (iter/batch).

    Both track the same completion counters and expose their tasks through
    the same `tasks` reverse FK.
    """

    @property
    def total_count(self) -> int:
        self._model.refresh_from_db()
        return self._model.total_count

    @property
    def completed_count(self) -> int:
        self._model.refresh_from_db()
        return self._model.completed_count

    @property
    def success_count(self) -> int:
        self._model.refresh_from_db()
        return self._model.success_count

    @property
    def failure_count(self) -> int:
        self._model.refresh_from_db()
        return self._model.failure_count

    def result(self, wait: int | None = None) -> WorkflowResult:
        """
        Get results from all tasks (unordered).

        Args:
            wait: Timeout in milliseconds to wait for completion

        Raises:
            TimeoutError: If wait is provided and the workflow doesn't
                complete in time
        """
        self._poll_until_terminal(wait)
        return self._build_workflow_result(self._model.tasks.all())

    def __repr__(self):
        return (
            f"<{self.__class__.__name__} id={self._model.id}"
            f" status={self._model.status}"
            f" completed={self.completed_count}/{self.total_count}>"
        )
