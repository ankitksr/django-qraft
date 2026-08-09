"""
Read-only lookups the scenarios use to see what qraft did.

Everything here works from the Django-Q2 task id that `async_task()` returns,
which stays valid across retries: attempt 1 keeps it even after attempts 2 and
3 get ids of their own.
"""

from qraft.models import QraftTask, QraftTaskAttempt, TaskStatus

TERMINAL = (TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.EXHAUSTED)


def attempt_of(q2_task_id: str):
    """The QraftTaskAttempt for a Django-Q2 task id, or None."""
    return (
        QraftTaskAttempt.objects.select_related("qraft_task")
        .filter(q2_task_id=q2_task_id)
        .first()
    )


def task_of(q2_task_id: str):
    """The QraftTask behind a Django-Q2 task id, or None."""
    attempt = attempt_of(q2_task_id)
    return attempt.qraft_task if attempt else None


def fresh(qraft_task):
    """Re-read a QraftTask from the database."""
    return QraftTask.objects.get(id=qraft_task.id)


def settled(q2_task_id: str):
    """The QraftTask if it has reached a terminal status, else None."""
    task = task_of(q2_task_id)
    if task is not None and task.status in TERMINAL:
        return task
    return None


def in_status(q2_task_id: str, *statuses):
    """The QraftTask if its status is one of `statuses`, else None."""
    task = task_of(q2_task_id)
    if task is not None and task.status in statuses:
        return task
    return None


def attempts(qraft_task) -> list:
    """Every attempt of a task, oldest first."""
    return list(qraft_task.attempts.order_by("attempt_number"))


def attempt_count(q2_task_id: str) -> int:
    task = task_of(q2_task_id)
    return task.attempts.count() if task else 0


def gaps(qraft_task) -> list[float]:
    """
    Seconds between each attempt finishing and the next one starting.

    This is the retry delay as it was actually served, measured from the
    lease timestamps that the worker wrote, not from anything the scenario
    computed for itself.
    """
    rows = attempts(qraft_task)
    measured = []
    for previous, current in zip(rows, rows[1:]):
        if previous.date_completed and current.date_started:
            measured.append(
                (current.date_started - previous.date_completed).total_seconds()
            )
    return measured


def chosen_delays(qraft_task) -> list[float]:
    """
    Seconds between each attempt finishing and the next one becoming due.

    This is the delay qraft chose, read from the `not_before` it wrote on the
    scheduled attempt. The exact counterpart of `gaps()`, which is what was
    actually served - comparing the two is what shows delivery honouring the
    decision rather than rounding it up to a poll.
    """
    rows = attempts(qraft_task)
    chosen = []
    for previous, current in zip(rows, rows[1:]):
        if previous.date_completed and current.not_before:
            chosen.append(
                (current.not_before - previous.date_completed).total_seconds()
            )
    return chosen


def nth_attempt(qraft_task_id, attempt_number: int):
    """One attempt of a task by its number, or None."""
    return QraftTaskAttempt.objects.filter(
        qraft_task_id=qraft_task_id, attempt_number=attempt_number
    ).first()


def hook_dispatches(qraft_task) -> list[str]:
    """Hook types qraft recorded a dispatch row for."""
    return sorted(qraft_task.hook_dispatches.values_list("hook_type", flat=True))


def q2_task_named(name: str) -> bool:
    """Whether Django-Q2 stored a finished task under this name."""
    from django_q.models import Task as Q2Task

    return Q2Task.objects.filter(name=name).exists()
