# Dead Letter Queue

Tasks that exhaust their retries (`EXHAUSTED`) or fail with no retry policy (`FAILED`) sit
in that state indefinitely. `qraft/dlq.py` is a thin service over `QraftTask` for finding
and requeueing them — there's no separate table; "dead" is just those two statuses.

## Inspecting dead tasks

```python
from qraft.dlq import dead_letters

for task in dead_letters():
    print(task.id, task.status, task.func, task.latest_attempt.exception_class)
```

`dead_letters()` returns a `QuerySet[QraftTask]` filtered to `FAILED`/`EXHAUSTED`, newest
first, with `attempts` prefetched.

## Requeueing

```python
from qraft.dlq import requeue

requeue(task)  # -> QraftTaskAttempt id (str)
```

`requeue()` re-enqueues the task's stored `func`/`task_args`/`task_kwargs` for immediate
execution. It goes through `qraft.scheduler.schedule_attempt()`, the same path a
scheduled retry takes: a SCHEDULED `QraftTaskAttempt` row with an immediate ETA, which
the dispatcher then claims and enqueues. No Django-Q2 `Schedule` row and no marker name
are involved. This means:

- It runs as the **next attempt on the same `QraftTask`**, not a new one — attempt history,
  hooks, and idempotency key are all preserved.
- The task's status returns to `PENDING` immediately, then follows the normal lifecycle.
- Raises `ValueError` if the task isn't currently `FAILED` or `EXHAUSTED`.

Because it reuses the existing row rather than creating one, a task with an
`idempotency_key` requeues cleanly — there's no second row to collide with the unique
constraint.

## Admin action

The `QraftTask` admin has a **"Requeue selected dead tasks"** bulk action. Select any mix
of rows and run it; tasks not currently dead are skipped, and the result message reports
how many were requeued vs. skipped.
