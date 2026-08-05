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
| `supports_defer` | no | `run_after` is not wired into Qraft's pipeline |
| `supports_async_task` | no | Coroutine tasks are not executed by Qraft workers |

`validate_task()` also rejects `takes_context=True`: Qraft calls the target function with the stored args and kwargs, and has no layer to inject a leading context argument.

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
