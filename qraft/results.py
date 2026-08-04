"""Rich result objects for workflow primitives."""

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID


@dataclass
class TaskResult:
    """Result from a single task within a workflow."""

    task_id: UUID
    func: str
    success: bool
    result: Any = None
    error: str | None = None
    attempt_count: int = 1

    @classmethod
    def from_qraft_task(cls, qraft_task):
        """Build a TaskResult from a QraftTask instance."""
        attempt = qraft_task.latest_attempt
        q2_result = None
        error = None

        if attempt:
            q2_task = attempt.get_q2_task()
            if q2_task:
                q2_result = q2_task.result if attempt.success else None
                error = q2_task.result if not attempt.success else None

        return cls(
            task_id=qraft_task.id,
            func=qraft_task.func,
            success=attempt.success if attempt else False,
            result=q2_result,
            error=str(error) if error else None,
            attempt_count=qraft_task.attempts.count(),
        )


@dataclass
class WorkflowResult:
    """Aggregated result from a workflow (chain/iter/batch).

    Supports iteration to maintain backward compatibility with list results:
        list(workflow_result)  # yields result values
    """

    task_results: list[TaskResult] = field(default_factory=list)

    @property
    def succeeded(self) -> list[TaskResult]:
        """Return only successful task results."""
        return [r for r in self.task_results if r.success]

    @property
    def failed_results(self) -> list[TaskResult]:
        """Return only failed task results."""
        return [r for r in self.task_results if not r.success]

    @property
    def values(self) -> list[Any]:
        """Return result values from all tasks (None for failed)."""
        return [r.result for r in self.task_results]

    def errors(self) -> list[dict]:
        """Return structured error info for failed tasks."""
        return [
            {
                "task_id": r.task_id,
                "func": r.func,
                "error": r.error,
                "attempts": r.attempt_count,
            }
            for r in self.task_results
            if not r.success
        ]

    def __iter__(self):
        """Iterate over result values for backward compatibility."""
        return iter(self.values)

    def __len__(self):
        return len(self.task_results)
