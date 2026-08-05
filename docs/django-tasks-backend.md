# django.tasks Backend

Django 6.0 ships `django.tasks` (DEP 14), an official interface for background tasks — but no retries, hooks, chaining, or workers. `qraft.backend.QraftTaskBackend` supplies those: your code uses the standard `@task` API, Qraft executes it.

Requires Django 6.0 or later. On older Django the module raises `ImproperlyConfigured` when Django tries to instantiate the backend; the rest of Qraft is unaffected.

## Configuration

```python
TASKS = {
    "default": {"BACKEND": "qraft.backend.QraftTaskBackend"},
}
```

Declaring `TASKS` is safe on Django < 6.0 — the setting is simply ignored.

```python
from django.tasks import task

@task(priority=10)
def summarize(doc_id):
    ...

result = summarize.enqueue(doc_id)
result.id       # the QraftTask UUID
result.status   # TaskResultStatus
```

## Supported features

| Flag | Value | Notes |
|------|-------|-------|
| `supports_get_result` | yes | Reads `QraftTask`/`QraftTaskAttempt` |
| `supports_priority` | yes | Maps to Qraft's three lanes |
| `supports_defer` | yes | `run_after` schedules through Django-Q2's `Schedule` model |
| `supports_async_task` | no | Coroutine tasks are not executed by Qraft workers |

### Deferred execution (`run_after`)

```python
result = summarize.using(run_after=timezone.now() + timedelta(hours=1)).enqueue(doc_id)
result.status  # READY - nothing has run yet
```

A deferred task creates its `QraftTask` immediately (`PENDING`, no attempts yet) and a Django-Q2 `Schedule` (`ONCE`, firing at `run_after`) carrying a Qraft marker, the same linkage mechanism used for scheduled retries. The attempt row - and the usual `RUNNING`/`SUCCEEDED`/`FAILED` transitions - only appear once the schedule actually fires.

### `TaskContext` (`takes_context`)

```python
@task(takes_context=True)
def summarize(context, doc_id):
    context.attempt  # 1, 2, ... across retries
```

Qraft dispatches the raw target function straight to Django-Q2, so the context can't be injected by the caller - `QraftTaskBackend` queues a small worker-side wrapper (`qraft.backend.run_task_with_context`) instead, which builds a `TaskContext` from `get_result()` and calls the real function with it. `QraftTask.func`/args/kwargs still record the real target, so `get_result()` is unaffected.

For a deferred (`run_after`) task, `context.attempt` reads `0` during the first run rather than `1`: the attempt row isn't created until the hook handler runs, after that first run has already completed.

## Priority mapping

`django.tasks` priority is an integer in `[-100, 100]`; Qraft has three lanes. Sign decides the lane, so the exact integer does not survive a round trip through `get_result()`.

| Task priority | Qraft lane | Priority reported back |
|---------------|------------|------------------------|
| `> 0` | `high` | `10` |
| `0` | `default` | `0` |
| `< 0` | `low` | `-10` |

Lanes are only drained by a cluster running `qraft.brokers.QraftOrmBroker` — see [AI Workloads](ai-workloads.md#priority-lanes).

## Status mapping

| `QraftTask.status` | `TaskResultStatus` |
|--------------------|--------------------|
| `PENDING` | `READY` |
| `RUNNING` | `RUNNING` |
| `SUCCEEDED` | `SUCCESSFUL` |
| `FAILED` | `FAILED` |
| `EXHAUSTED` | `FAILED` |

A fresh `enqueue()` reports `RUNNING`, not `READY`: Qraft marks a task `RUNNING` as soon as it is handed to the broker.

`TaskResult.errors` carries the last attempt's exception. Qraft stores the bare class name, so `TaskError.exception_class_path` is not always importable.

## Qraft options

Retry policies, dual-phase hooks, workflows, idempotency keys, and throttling stay on the native API — `django.tasks` has no place to express them. Use `qraft.tasks.async_task()` where you need them and the standard `@task` API everywhere else; both write the same `QraftTask` records.

For a live end-to-end run, see `demo tasks-api` in the demo application.
