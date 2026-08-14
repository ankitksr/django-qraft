"""Base workflow class with shared patterns for chain/iter/batch."""

import logging
import time
from uuid import UUID

from django.utils.module_loading import import_string

from qraft.models import WorkflowStatus
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

    def cancel(self) -> None:
        """
        Cancel the workflow.

        Sets status to CANCELLED. This stops future orchestration only - no
        new steps queued, no hooks fired, no counters updated - but does not
        revoke member tasks already queued or running; they complete and
        their outcome is ignored.

        Raises:
            InvalidStatusTransition: If workflow can't be cancelled from current state
        """
        self._model.refresh_from_db()
        self._model.transition_to(WorkflowStatus.CANCELLED)
        self._model.save(update_fields=["status", "date_updated"])
        _logger.info(
            "%s %s cancelled",
            self._workflow_type.capitalize(),
            self._model.id,
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
