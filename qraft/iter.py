"""QraftIter: Parallel execution of the same function with different inputs."""

import logging
from uuid import UUID

from django.db import transaction

from qraft.base import ParallelWorkflow, _validate_hook_path
from qraft.models import QraftIterModel, WorkflowStatus

_logger = logging.getLogger("qraft.iter")


class QraftIter(ParallelWorkflow):
    """
    Parallel workflow primitive for same function with many inputs.

    Features:
    - All tasks execute in parallel
    - Single function applied to different inputs
    - Atomic counter tracking for completion
    - Workflow-level success/failure hooks
    - Cancellation support
    - Progress hook called on each task completion

    Example:
        iter_task = QraftIter(
            'myapp.tasks.download_report',
            qraft_options={'max_attempts': 3},
            cluster='io-workers',
            on_success='myapp.hooks.all_reports_ready',
        )
        for report_id in [1, 2, 3, 4, 5]:
            iter_task.append(report_id)
        iter_id = iter_task.run()
    """

    _workflow_type = "iter"

    def __init__(
        self,
        func: str,
        qraft_options: dict | None = None,
        cluster: str | None = None,
        on_success: str | None = None,
        on_failure: str | None = None,
        on_cancelled: str | None = None,
        progress_hook: str | None = None,
        success_args: tuple = (),
        success_kwargs: dict | None = None,
        failure_args: tuple = (),
        failure_kwargs: dict | None = None,
        iter_id: UUID | str | None = None,
    ):
        self._items = []

        # Validate hooks at creation time
        for hook in (on_success, on_failure, on_cancelled, progress_hook):
            _validate_hook_path(hook)

        if iter_id:
            self._model = QraftIterModel.objects.get(id=iter_id)
        else:
            merged_options = qraft_options or {}
            if cluster is not None:
                merged_options = {**merged_options, "cluster": cluster}

            self._model = QraftIterModel.objects.create(
                func=func,
                default_qraft_options=merged_options,
                success_hook=on_success,
                success_args=list(success_args),
                success_kwargs=success_kwargs or {},
                failure_hook=on_failure,
                failure_args=list(failure_args),
                failure_kwargs=failure_kwargs or {},
                on_cancelled=on_cancelled,
                progress_hook=progress_hook,
            )

        _logger.debug("Initialized QraftIter %s for func %s", self._model.id, func)

    def append(self, *args, **kwargs):
        """
        Add an item to the iter with the given arguments.

        Raises:
            ValueError: If iter has already been run
        """
        if self._model.status != WorkflowStatus.PENDING:
            raise ValueError("Cannot append to an iter that has already been run")

        from qraft.tasks import _reject_workflow_member_opt_keys

        _reject_workflow_member_opt_keys(kwargs)

        self._items.append({"args": args, "kwargs": kwargs})
        _logger.debug("Appended item %d to iter %s", len(self._items), self._model.id)

    def run(self) -> UUID:
        """
        Start executing all items in parallel.

        Returns:
            UUID: Iter ID

        Raises:
            ValueError: If iter is empty or has already been run
        """
        if not self._items:
            raise ValueError("Cannot run empty iter")

        if self._model.status != WorkflowStatus.PENDING:
            raise ValueError(f"Iter already run (status: {self._model.status})")

        from qraft.tasks import _create_workflow_task

        # One transaction for the publish and the whole fan-out: a failure
        # creating any member rolls back total_count/RUNNING too, instead of
        # leaving an unfinishable workflow with a partial member set. The
        # publish must stay first - committing members before a final
        # total_count would let the >= completion test finish the workflow
        # on the first early completion.
        try:
            with transaction.atomic():
                self._model.total_count = len(self._items)
                self._model.transition_to(WorkflowStatus.RUNNING)
                self._model.save(
                    update_fields=["total_count", "status", "date_updated"]
                )

                for idx, item in enumerate(self._items):
                    _create_workflow_task(
                        func=self._model.func,
                        args=item["args"],
                        kwargs=item["kwargs"],
                        qraft_options=self._model.default_qraft_options,
                        qraft_iter_id=self._model.id,
                    )
                    _logger.debug(
                        "Queued iter task %d/%d (iter=%s)",
                        idx + 1,
                        len(self._items),
                        self._model.id,
                    )
        except Exception:
            # The rollback reverted the DB but not this instance; refresh so
            # a retry of run() isn't refused as "already run".
            self._model.refresh_from_db()
            raise

        _logger.info(
            "Started QraftIter %s with %d items",
            self._model.id,
            len(self._items),
        )
        return self._model.id

    def length(self) -> int:
        """Get the total number of items."""
        return self._model.total_count
