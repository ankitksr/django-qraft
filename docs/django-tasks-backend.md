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
| `supports_defer` | yes | `run_after` creates a `SCHEDULED` attempt row, dispatched at its due time |
| `supports_async_task` | yes | An `async def` task runs to completion with `asyncio.run()` in its worker slot |

### Deferred execution (`run_after`)

```python
result = summarize.using(run_after=timezone.now() + timedelta(hours=1)).enqueue(doc_id)
result.status  # READY - nothing has run yet
```

A deferred task creates its `QraftTask` immediately (`PENDING`) with attempt 1 as a `SCHEDULED` row due at `run_after` - the same owned-scheduling path retries use. Qraft's dispatcher enqueues it at that time; the usual `RUNNING`/`SUCCEEDED`/`FAILED` transitions only start then.

### Coroutine tasks

`@task`-decorated `async def` functions enqueue and run like sync ones. The worker runs the coroutine to completion with `asyncio.run()` in the slot it already occupies, so lease, timeout, and retry semantics are identical - one coroutine per slot, not an event-loop worker. Concurrency still comes from workers and threads, not from the coroutine.

Django's async rules apply inside the coroutine: the sync ORM raises `SynchronousOnlyOperation` under a running event loop, so use the async ORM (`aget`, `acreate`, `aupdate`) or wrap sync calls in `sync_to_async`. The same holds for `async def` hooks. `qraft.tasks.async_task()` and workflow members accept `async def` targets the same way.

### `TaskContext` (`takes_context`)

```python
@task(takes_context=True)
def summarize(context, doc_id):
    context.attempt  # 1, 2, ... across retries
```

Qraft dispatches the raw target function straight to Django-Q2, so the context can't be injected by the caller - `QraftTaskBackend` queues a small worker-side wrapper (`qraft.backend.run_task_with_context`) instead, which builds a `TaskContext` from `get_result()` and calls the real function with it. `QraftTask.func`/args/kwargs still record the real target, so `get_result()` is unaffected.

For a deferred (`run_after`) task, `context.attempt` reads `0` during the first run rather than `1`: the attempt row isn't created until the hook handler runs, after that first run has already completed.

A `takes_context=True` task that gets retried or requeued (via `qraft.dlq.requeue()`) does not get `TaskContext` re-injected on that run - not supported yet.

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
