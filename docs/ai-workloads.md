# AI Workloads

Primitives for jobs that call LLM and other metered providers: rate-limit-aware retries, cross-worker backpressure, token/cost accounting, idempotency, and crash recovery.

## Rate-limit-aware retries

Provider throttling (429/503/529) is not an ordinary hard error. `RetryPolicy` treats a known set of exception class names as rate limits:

```python
from qraft.retry import RATE_LIMIT_EXCEPTIONS
# RateLimited, RateLimitError, TooManyRequests, TooManyRequestsError,
# ThrottlingException, ResourceExhausted, ServiceUnavailableError,
# OverloadedError, APIStatusError429
```

Matching is by bare class name, so most provider SDKs work without configuration. Two things change when a failure matches:

- It retries even under a restrictive `retry_exceptions` allowlist. Only `skip_exceptions` can override that.
- Backoff is exponential regardless of the configured `backoff_strategy`, capped at `rate_limit_max_delay` (default 300s). A `fixed` policy must not hammer a 429.

If the error text carries a hint — `Retry-After: 12`, `retry_after=12`, `try again in 12 seconds` — that value is honored as a floor, capped at `rate_limit_max_delay`, with a small positive jitter added on top. Qraft never waits less than the provider asked for.

```python
async_task('myapp.tasks.summarize', doc_id,
    qraft_options={
        'max_attempts': 5,
        'base_delay': 2.0,
        'retry_exceptions': ['ConnectionError'],   # rate limits retry anyway
        'rate_limit_exceptions': ['MyProviderThrottle'],  # replaces the default set
        'rate_limit_max_delay': 120.0,
    }
)
```

## Throttling with a shared token bucket

Retries react to a rate limit after you hit it. `throttled()` avoids hitting it. The bucket lives in a `RateBucket` row, so N workers across processes and machines share one limit — a per-process semaphore cannot.

```python
from qraft.throttle import throttled

@throttled(key='openai:acme-tenant', rate=10, capacity=20)
def summarize(doc_id):
    ...
```

`rate` is tokens per second, `capacity` the burst ceiling (defaults to `max(rate, cost)`), `cost` the tokens one call needs. A new bucket starts full. When the bucket is empty the call raises `qraft.throttle.RateLimited`, which is a rate-limit exception, so the task reschedules itself with backoff instead of blocking a worker.

`acquire(key, rate, capacity, cost)` is available directly if you need to gate part of a function rather than all of it.

Postgres only for concurrent correctness: the refill-and-drain runs under `select_for_update()`, which SQLite does not implement.

## Usage and progress

Task code records what it spent, without any argument threading — the executing attempt is found through a context variable set by a `pre_execute` receiver.

```python
from qraft.context import record_usage, report_progress

def agent_loop(thread_id):
    for step, prompt in enumerate(prompts):
        report_progress(current=step + 1, total=len(prompts), message='calling model')
        response = client.messages.create(...)
        record_usage(
            model='claude-opus-4',
            input_tokens=response.usage.input_tokens,
            output_tokens=response.usage.output_tokens,
            cost=price(response.usage),
        )
```

`record_usage()` merges into `QraftTaskAttempt.usage`: numeric fields accumulate across calls in the same attempt, non-numeric fields (model name) take the latest value. `report_progress()` merges into `QraftTask.progress`, readable at any time without touching Django-Q2 internals. Both are no-ops outside a task.

Aggregate across retries and across a workflow:

```python
from qraft.context import aggregate_usage, aggregate_workflow_usage

aggregate_usage(qraft_task)          # summed over every attempt
aggregate_workflow_usage(chain_model)  # summed over a chain/iter/batch
```

For parallel workflows, `progress_hook` fires on each task completion — see [Workflow Primitives](workflows.md).

## Idempotency keys

Pass an `idempotency_key` and a second enqueue under the same key is a no-op that returns the first call's task id.

```python
async_task('billing.charge', invoice_id,
    qraft_options={'idempotency_key': f'charge:{invoice_id}'})
```

The key is a permanent dedupe, not a "retry if it failed" signal: a `FAILED` or `EXHAUSTED` task still blocks re-enqueue. To run again, use a new key. Uniqueness is enforced by a DB constraint, so concurrent callers racing on one key still produce a single task.

## Orphan reaper

If a worker dies mid-task — OOM kill, `kill -9`, hardware loss — the monitor never sees a result. The attempt stays unresolved and the task stays `RUNNING` forever. The reaper finds those and resolves them through the normal retry path.

An attempt is orphaned when its task is `RUNNING`, the attempt has no outcome, it is older than `reap_stale_after`, and no Django-Q2 `Task` row exists for it. Reaped attempts are marked failed with `exception_class="OrphanedTask"` and handed to the retry policy; with no policy the task goes to `FAILED`.

The reaper runs automatically as a daemon thread beside the monitor process in `QraftCluster`. Call it directly for tests or a one-off sweep:

```python
from qraft.reaper import reap_orphans
reap_orphans(stale_after=300)   # returns the number of attempts reaped
```

| Setting | Default | Meaning |
|---------|---------|---------|
| `reap_interval` | `60.0` | Seconds between sweeps |
| `reap_stale_after` | `3600.0` | Seconds an attempt may go unresolved before it counts as orphaned |

Set `reap_stale_after` above your longest task timeout. Too low and the reaper requeues work that is still running.

## Broker choice

Use the ORM broker. Django-Q2's Redis broker has no delivery receipts: tasks in flight when a worker crashes are gone, and no reaper can recover what left no trace. `QraftCluster` logs a warning at startup when the configured broker implements neither `acknowledge()` nor `fail()`.

```python
Q_CLUSTER = {"name": "default", "workers": 4, "orm": "default"}
```

Postgres is the supported database. The throttle needs real row locks, and the reaper and workflow dispatchers rely on `select_for_update()`.

## Priority lanes

`QraftOrmBroker` splits one ORM queue into three lanes — `{list_key}--high`, `{list_key}`, `{list_key}--low` — and drains them in that order on every dequeue, so interactive or paying-tenant jobs preempt batch work without a second deployment.

```python
Q_CLUSTER = {
    "name": "default",
    "orm": "default",
    "broker_class": "qraft.brokers.QraftOrmBroker",
}
```

```python
async_task('myapp.tasks.answer', query, qraft_options={'priority': 'high'})
async_task('myapp.tasks.reindex', qraft_options={'priority': 'low'})
```

Without `broker_class`, high and low tasks are still enqueued into their lanes but a stock ORM broker never drains them. Set it on every cluster that consumes the queue.

Current limitation: scheduled retries go through the default path and are not re-routed by priority.
