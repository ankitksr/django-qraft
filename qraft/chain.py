"""QraftChain: Sequential task execution with resume capability."""

import logging
from uuid import UUID

from django.db import transaction

from qraft.base import BaseWorkflow, _validate_hook_path
from qraft.models import QraftChainModel, QraftChainStep, WorkflowStatus
from qraft.results import WorkflowResult

_logger = logging.getLogger("qraft.chain")


def _queue_chain_step(chain, step):
    """Queue a chain step as a QraftTask. Delegates to dispatchers."""
    from qraft.dispatchers import _queue_chain_step as _dispatch

    _dispatch(chain, step)


class QraftChain(BaseWorkflow):
    """
    Sequential workflow primitive that executes tasks one after another.

    Features:
    - Each step can have its own retry policy and cluster routing
    - Chain-level success/failure hooks
    - Resume from failed step after fixing issues
    - Automatic continuation after each step succeeds
    - Cancellation support

    Example:
        chain = QraftChain(
            on_success='myapp.hooks.pipeline_complete',
            on_failure='myapp.hooks.pipeline_failed',
        )
        chain.append('myapp.tasks.extract', source_id,
                     qraft_options={'max_attempts': 3, 'cluster': 'io-workers'})
        chain.append('myapp.tasks.transform', format='json',
                     qraft_options={'cluster': 'cpu-workers'})
        chain.append('myapp.tasks.load', dest_id,
                     qraft_options={'cluster': 'io-workers'})
        chain_id = chain.run()
    """

    _workflow_type = "chain"

    def __init__(
        self,
        on_success: str | None = None,
        on_failure: str | None = None,
        on_cancelled: str | None = None,
        success_args: tuple = (),
        success_kwargs: dict | None = None,
        failure_args: tuple = (),
        failure_kwargs: dict | None = None,
        chain_id: UUID | str | None = None,
    ):
        self._steps = []

        # Validate hooks at creation time
        for hook in (on_success, on_failure, on_cancelled):
            _validate_hook_path(hook)

        if chain_id:
            self._model = QraftChainModel.objects.get(id=chain_id)
        else:
            self._model = QraftChainModel.objects.create(
                success_hook=on_success,
                success_args=list(success_args),
                success_kwargs=success_kwargs or {},
                failure_hook=on_failure,
                failure_args=list(failure_args),
                failure_kwargs=failure_kwargs or {},
                on_cancelled=on_cancelled,
            )

        _logger.debug("Initialized QraftChain %s", self._model.id)

    def append(
        self,
        func: str,
        *args,
        qraft_options: dict | None = None,
        **kwargs,
    ):
        """
        Append a step to the chain.

        Args:
            func: Dotted path to the task function
            *args: Positional arguments for the task
            qraft_options: Qraft-specific options (retry policy, cluster routing, etc.)
            **kwargs: Keyword arguments for the task

        Raises:
            ValueError: If chain has already been run
        """
        if self._model.status != WorkflowStatus.PENDING:
            raise ValueError("Cannot append to a chain that has already been run")

        step_data = {
            "func": func,
            "task_args": list(args),
            "task_kwargs": kwargs,
            "qraft_options": qraft_options or {},
        }
        self._steps.append(step_data)

        _logger.debug(
            "Appended step %d to chain %s: %s",
            len(self._steps) - 1, self._model.id, func,
        )

    def run(self) -> UUID:
        """
        Start executing the chain from step 0.

        Returns:
            UUID: Chain ID

        Raises:
            ValueError: If chain is empty or has already been run
        """
        if not self._steps:
            raise ValueError("Cannot run empty chain")

        if self._model.status != WorkflowStatus.PENDING:
            raise ValueError(f"Chain already run (status: {self._model.status})")

        with transaction.atomic():
            for idx, step_data in enumerate(self._steps):
                QraftChainStep.objects.create(
                    chain=self._model,
                    step_index=idx,
                    **step_data,
                )

            self._model.status = WorkflowStatus.RUNNING
            self._model.save(update_fields=["status", "date_updated"])

        first_step = self._model.steps.get(step_index=0)
        _queue_chain_step(self._model, first_step)

        _logger.info(
            "Started QraftChain %s with %d steps",
            self._model.id, len(self._steps),
        )
        return self._model.id

    def resume(self) -> UUID:
        """
        Resume a failed chain from the failed step.

        Returns:
            UUID: Chain ID

        Raises:
            ValueError: If chain is not in FAILED status
        """
        if self._model.status != WorkflowStatus.FAILED:
            raise ValueError("Can only resume failed chains")

        current_step = self._model.steps.get(step_index=self._model.current_step_index)

        with transaction.atomic():
            self._model.status = WorkflowStatus.RUNNING
            self._model.save(update_fields=["status", "date_updated"])

            if current_step.qraft_task:
                current_step.qraft_task = None
                current_step.save(update_fields=["qraft_task"])

        _queue_chain_step(self._model, current_step)

        _logger.info(
            "Resumed QraftChain %s from step %d",
            self._model.id, current_step.step_index,
        )
        return self._model.id

    def current(self) -> int:
        """Get the current step index."""
        self._model.refresh_from_db()
        return self._model.current_step_index

    def result(self, wait: int | None = None) -> WorkflowResult:
        """
        Get results from all completed steps.

        Args:
            wait: Timeout in milliseconds to wait for completion

        Returns:
            WorkflowResult with step results in order

        Raises:
            TimeoutError: If wait is provided and chain doesn't complete in time
        """
        self._poll_until_terminal(wait)

        from qraft.results import TaskResult

        task_results = []
        for step in self._model.steps.order_by("step_index"):
            if step.qraft_task:
                task_results.append(TaskResult.from_qraft_task(step.qraft_task))
            else:
                break

        return WorkflowResult(task_results=task_results)

    def __repr__(self):
        return f"<QraftChain id={self._model.id} status={self._model.status}>"
