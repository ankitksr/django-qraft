"""QraftBatch: Parallel execution of different functions (fork-join pattern)."""

import logging
import warnings
from uuid import UUID

from django.db import transaction

from qraft.base import ParallelWorkflow, _validate_hook_path
from qraft.models import QraftBatchModel, WorkflowStatus

_logger = logging.getLogger("qraft.batch")


class QraftBatch(ParallelWorkflow):
    """
    Parallel workflow primitive for different functions (fork-join).

    Features:
    - All tasks execute in parallel
    - Different functions with different arguments
    - Each task can have its own retry policy and cluster routing
    - Workflow-level success/failure hooks
    - Cancellation support
    - Progress hook called on each task completion

    Example:
        batch = QraftBatch(
            on_success='myapp.hooks.generate_report',
            on_failure='myapp.hooks.partial_failure',
        )
        batch.append('myapp.tasks.fetch_sales', region='NA',
                  qraft_options={'max_attempts': 3, 'cluster': 'io-workers'})
        batch.append('myapp.tasks.fetch_inventory', warehouse='main',
                  qraft_options={'cluster': 'io-workers'})
        batch.append('myapp.tasks.fetch_shipping', carrier='fedex',
                  qraft_options={'cluster': 'default'})
        batch_id = batch.run()
    """

    _workflow_type = "batch"

    def __init__(
        self,
        on_success: str | None = None,
        on_failure: str | None = None,
        on_cancelled: str | None = None,
        progress_hook: str | None = None,
        success_args: tuple = (),
        success_kwargs: dict | None = None,
        failure_args: tuple = (),
        failure_kwargs: dict | None = None,
        batch_id: UUID | str | None = None,
        subject: tuple | None = None,
        run=None,
        stage: str | None = None,
        hook_context: bool = False,
    ):
        self._tasks = []

        # Validate hooks at creation time
        for hook in (on_success, on_failure, on_cancelled, progress_hook):
            _validate_hook_path(hook)

        if batch_id:
            self._model = QraftBatchModel.objects.get(id=batch_id)
        else:
            from qraft import runs
            from qraft.tasks import parse_subject

            subject_type, subject_id = parse_subject(subject)
            # The run's row lock is held across the insert, so a
            # concurrent runs.bind_subject() cannot leave this workflow with
            # a null subject it never backfills.
            with runs.correlating(run, stage, subject_type, subject_id) as correlation:
                self._model = QraftBatchModel.objects.create(
                    success_hook=on_success,
                    success_args=list(success_args),
                    success_kwargs=success_kwargs or {},
                    failure_hook=on_failure,
                    failure_args=list(failure_args),
                    failure_kwargs=failure_kwargs or {},
                    on_cancelled=on_cancelled,
                    progress_hook=progress_hook,
                    subject_type=correlation["subject_type"],
                    subject_id=correlation["subject_id"],
                    run_id=correlation["run_id"],
                    stage=correlation["stage"],
                    hook_context=hook_context,
                )

        _logger.debug("Initialized QraftBatch %s", self._model.id)

    def append(
        self,
        func: str,
        *args,
        qraft_options: dict | None = None,
        **kwargs,
    ):
        """
        Append a task to the batch.

        Args:
            func: Dotted path to the task function
            *args: Positional arguments for the task
            qraft_options: Qraft-specific options (retry policy, cluster routing, etc.)
            **kwargs: Keyword arguments for the task

        Raises:
            ValueError: If batch has already been run
        """
        if self._model.status != WorkflowStatus.PENDING:
            raise ValueError("Cannot add to a batch that has already been run")

        from qraft.tasks import _reject_workflow_member_opt_keys

        _reject_workflow_member_opt_keys(kwargs)

        task_data = {
            "func": func,
            "args": args,
            "kwargs": kwargs,
            "qraft_options": qraft_options or {},
        }
        self._tasks.append(task_data)
        _logger.debug(
            "Added task %d to batch %s: %s",
            len(self._tasks),
            self._model.id,
            func,
        )

    def add(
        self,
        func: str,
        *args,
        qraft_options: dict | None = None,
        **kwargs,
    ):
        """Deprecated: use append() instead."""
        warnings.warn(
            "QraftBatch.add() is deprecated, use append() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.append(func, *args, qraft_options=qraft_options, **kwargs)

    def run(self) -> UUID:
        """
        Start executing all tasks in parallel.

        Returns:
            UUID: Batch ID

        Raises:
            ValueError: If batch is empty or has already been run
        """
        if not self._tasks:
            raise ValueError("Cannot run empty batch")

        if self._model.status != WorkflowStatus.PENDING:
            raise ValueError(f"Batch already run (status: {self._model.status})")

        from qraft.tasks import _create_workflow_task, workflow_member_labels

        labels = workflow_member_labels(self._model)
        # One transaction for the publish and the whole fan-out: a failure
        # creating any member rolls back total_count/RUNNING too, instead of
        # leaving an unfinishable workflow with a partial member set. The
        # publish must stay first - committing members before a final
        # total_count would let the >= completion test finish the workflow
        # on the first early completion.
        try:
            with transaction.atomic():
                self._model.total_count = len(self._tasks)
                self._model.transition_to(WorkflowStatus.RUNNING)
                self._model.save(
                    update_fields=["total_count", "status", "date_updated"]
                )
                self._bind_to_run()

                for idx, task_data in enumerate(self._tasks):
                    _create_workflow_task(
                        func=task_data["func"],
                        args=task_data["args"],
                        kwargs=task_data["kwargs"],
                        qraft_options=task_data["qraft_options"],
                        qraft_batch_id=self._model.id,
                        labels=labels,
                    )
                    _logger.debug(
                        "Queued batch task %d/%d (batch=%s, func=%s)",
                        idx + 1,
                        len(self._tasks),
                        self._model.id,
                        task_data["func"],
                    )
        except Exception:
            # The rollback reverted the DB but not this instance; refresh so
            # a retry of run() isn't refused as "already run".
            self._model.refresh_from_db()
            raise

        _logger.info(
            "Started QraftBatch %s with %d tasks",
            self._model.id,
            len(self._tasks),
        )
        return self._model.id
