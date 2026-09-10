# AI Workloads

Primitives for jobs that call LLM and other metered providers: rate-limit-aware retries, cross-worker backpressure, token/cost accounting, idempotency, and crash recovery.

## Subjects

A task records everything about its execution and nothing about the thing it is for.
A subject closes that gap: a `(type, id)` string pair on the task, indexed, so "every
task for worksheet 4117" is one query instead of a JSON scan.

```python
async_task('myapp.tasks.ingest', worksheet_id,
           qraft_options={'subject': ('worksheet', worksheet_id)})

QraftTask.objects.for_subject('worksheet', 4117)   # newest first
```

The id is coerced with `str()`, so integer primary keys work unchanged. Strings rather
than a generic foreign key: the subject may live in another database or another service,
and a string pair is what every consumer can produce.

Retries inherit it — the subject is on the task, not the attempt. Workflow constructors
take `subject=` directly and copy it onto every member they create:

```python
batch = QraftBatch(subject=('worksheet', 4117))
batch.append('myapp.tasks.revenue_pass', worksheet_id)
batch.append('myapp.tasks.entity_pass', worksheet_id)
batch.run()
```

The dashboard filter box and the admin's `subject_type` filter read the same pair.
Tasks without a subject show a dash and are excluded from subject filters.

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

## Request budgets

A token bucket answers "how fast", not "how much". It refills, so a pipeline that keeps retrying keeps being allowed to spend. A budget is the other half: a fixed allowance one graph may ask of a provider, across every node and every retry of every node.

Declare it on the graph and spend it one request at a time:

```python
from qraft import graphs

graph = graphs.Graph(
    subject=("worksheet", 4117),
    kind="scoring",
    budgets={"openai_requests": 40},
)
graph.node("ingest", "myapp.tasks.ingest", 4117, recovery="transactional")
graph.node("rules", "myapp.tasks.rules", 4117, after=["ingest"], recovery="transactional")
graph.node("ai", "myapp.tasks.score", 4117, after=["ingest"], recovery="idempotent")
graph_id = graph.start()
```

```python
from qraft.context import BudgetExhausted, consume_budget, record_usage

@throttled(key="openai:acme-tenant", rate=10, capacity=20)   # how fast
def score_chunk(chunk_id):
    consume_budget("openai_requests")                        # how much, before the call
    response = client.responses.create(...)
    record_usage(                                            # what it cost, after it
        model="gpt-5.4-nano",
        input_tokens=response.usage.input_tokens,
        output_tokens=response.usage.output_tokens,
    )
```

**One request is one `consume_budget()` call before the call and one `record_usage()` call after it.** Spending before means a crash between the two costs the budget one request rather than losing the accounting for a request the provider already billed.

`consume_budget(key, n=1, graph_id=None)` decrements atomically — one guarded `UPDATE ... RETURNING` on Postgres, a locked read-modify-write elsewhere — and returns what is left. A decrement that would take the key below zero matches no row, so two workers cannot both spend the last request; the loser gets `BudgetExhausted`. `remaining_budget(key, graph_id=None)` reads without spending.

The budget lives on the graph because that is the scope worth bounding. Combine the two: `throttled()` keeps the fleet inside the provider's rate limit, `consume_budget()` keeps one worksheet inside its own cost ceiling.

A key the graph does not declare is unmetered and `consume_budget()` returns `None` — Qraft does not invent a limit the application never asked for. Outside a graph it is a no-op, the same as `record_usage()`. Budget values are whole counts of requests; `Graph.start()` raises `GraphError` on a fractional one rather than rounding it.

`BudgetExhausted` is an ordinary task failure: the attempt resolves failed and the retry policy applies, so list it under `skip_exceptions` if a retry should not burn attempts on a ceiling that will not move.

### A retry re-runs the whole node

Qraft retries an attempt, not the part of it that had not finished. A metered node that fails on its last chunk re-executes from the top on attempt 2 and asks the provider for every chunk again, unless the task itself skips work it has already persisted. The pattern is to make the unit of work durable and to check for it first:

```python
def score_chunk(worksheet_id, chunk_id):
    if Score.objects.filter(worksheet_id=worksheet_id, chunk_id=chunk_id).exists():
        return                       # already paid for on an earlier attempt
    consume_budget("openai_requests")
    ...
```

The graph's budget is what bounds the cost when that pattern is not in place: the retry spends from the same allowance, so a node that keeps failing runs out of budget instead of out of money.

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

`record_usage()` merges into `QraftTaskAttempt.usage`: numeric fields accumulate across calls in the same attempt, non-numeric fields (model name) take the latest value. Money keys are kept as decimal strings and summed as `Decimal`, never as float. A call that names a `model` also appends one priced entry — see [Cost from usage](#cost-from-usage).

`report_progress()` merges into `QraftTaskAttempt.progress` and returns whether a write happened. Both are no-ops outside a task.

Coroutine tasks use `areport_progress()` and `arecord_usage()`, which run the same writes off the event loop.

### Progress belongs to the attempt

A retry is a new execution. Attempt 2 starting at zero must not display attempt 1's 90%,
and an attempt 1 that is still running must not overwrite attempt 2's numbers. So the
payload lives on `QraftTaskAttempt`, and `QraftTask.progress` is a denormalised snapshot
for the dashboard's task rows, stamped with the `attempt_id` that wrote it. The snapshot
update carries its own `WHERE` — no attempt of this task has a higher `attempt_number` —
so a late write from a superseded attempt updates zero rows.

Two timestamps ride with it:

| Column | Moves |
|---|---|
| `progress_reported_at` | on every `report_progress()` call |
| `progress_advanced_at` | only when `current` or `total` differ from the stored values |

A task that reports "waiting for provider" every ten seconds has a fresh `reported_at`
and a stale `advanced_at`. Without the distinction a task could look busy while hung.

Writes are atomic. On Postgres the merge is one statement (`progress || %s::jsonb`, with
the advanced timestamp set by a `CASE` comparing the incoming `current`/`total` against
the stored ones); on other databases the helper takes `select_for_update()` on the
attempt row and does the read-modify-write inside that lock. Two reporters in one attempt
— a threaded task, a coroutine fan-out — never lose an increment.

A chunk-heavy task can throttle its writes with `progress_min_interval` (seconds,
`QRAFT_CLUSTER`, default 0): a call inside the interval is skipped unless `current` or
`total` changed, or the caller passes `force=True`. The default keeps the pre-1.4
behaviour of writing on every call.

Aggregate across retries, a workflow, a graph, or a subject:

```python
from qraft.context import (
    aggregate_usage, aggregate_workflow_usage,
    aggregate_graph_usage, aggregate_subject_usage,
)

aggregate_usage(qraft_task)              # summed over every attempt
aggregate_workflow_usage(chain_model)    # summed over a chain/iter/batch
aggregate_graph_usage(graph_id)          # summed over every task in a graph
aggregate_subject_usage('worksheet', 4117)
```

For parallel workflows, `progress_hook` fires on each task completion — see [Workflow Primitives](workflows.md).

## Cost from usage

`record_usage()` counts tokens. Turning them into money is optional, off by default, and
lives in one place when you want it.

```python
QRAFT_PRICING = {
    "resolver": "qraft.pricing.StaticTablePricing",   # the default when the dict exists
    "currency": "USD",
    "revision": "2026-09",
    "models": {
        "gpt-5.4-nano": {"input": 0.05, "output": 0.40, "cached_input": 0.005},
    },
}
```

Rates are per million tokens. No `QRAFT_PRICING` means no resolver, and every cost path
answers "unknown" rather than guessing.

### Cost is recorded per increment

Every `record_usage()` call that names a `model` appends one entry to `usage["entries"]`
carrying the model, provider, the three token counts, `estimated_cost`, `currency`,
`pricing_revision` and `cost_source` (`caller`, `resolver` or `none`).

Entries exist because the totals cannot be priced. The merge keeps the latest model name
while summing all tokens, so an attempt that calls two models would have one model's
tokens billed at the other's rate. And because each increment is priced when it is
written, a corrected table later never re-prices what a provider already billed.

Caller-supplied `cost` always wins for its entry:

```python
record_usage(model='gpt-5.4-nano', input_tokens=1200, output_tokens=300,
             cached_input_tokens=800, cost=response.billed_amount)
```

### Cached tokens are a subset of input tokens

That is how OpenAI reports `cached_tokens`, inside `input_tokens_details`, and the formula
follows it:

```
(input - cached) × input_rate + cached × cached_rate + output × output_rate
```

A provider that reports cached tokens *separately* from input sets
`cached_is_subset: False` on its model entry, and they are priced on top instead. A model
with no `cached_input` rate bills cached tokens at the input rate. Every resolver should
document which convention it follows.

### Reading cost

```python
from qraft.pricing import cost

summary = cost(attempt.usage)
summary.amount      # Decimal
summary.currency
summary.coverage    # 'complete' | 'partial' | 'none'
summary.estimated   # False only when every costed entry came from the caller
```

`coverage` says whether every entry that has tokens also has a cost; `estimated` says
whether any of it came from your table rather than from the provider. **A configured price
is an estimate of what the provider will bill, never proof of it** — which is why the
answer is a summary with those two flags rather than a bare number.

Money never passes through a float. Whatever you pass — `Decimal`, `float`, or a numeric
string — is stored as its decimal string form and summed as `Decimal`, so two calls
recording `0.1` and `0.2` total exactly `0.3`.

Entries written before a resolver existed, or under a model the table did not know, are
priced at read time when the current table can, and marked estimated.

Every aggregate carries the same summary under `cost_summary`, beside the token totals:

```python
aggregate_usage(qraft_task)["cost_summary"]
aggregate_graph_usage(graph_id)["cost_summary"]
aggregate_subject_usage('worksheet', 4117)["cost_summary"]
```

It is a separate key from `usage["cost"]`, which stays the caller's own running total.
A graph's `summary` snapshot carries it too, so a pruned graph still knows what it cost. The
dashboard's usage panel shows it with its coverage word and an `est` marker, and honours
the same subject and graph filters as the rest of the page.

### Writing your own resolver

`qraft.pricing.PricingResolver` is a protocol with one method:

```python
class PricingResolver(Protocol):
    def price(self, model: str, provider: str | None = None) -> Price | None: ...
```

Point `QRAFT_PRICING["resolver"]` at any class implementing it — one that reads a
database table, or a vendor price feed. Returning `None` for a model you do not know is
correct: the entry stays unpriced and the coverage says so.

The resolver is built once per process. If your resolver reads a table you edit at
runtime, either make `price()` do the reading, or call `qraft.pricing.reset_resolver()`
when the table changes.

## Idempotency keys

Pass an `idempotency_key` and a second enqueue under the same key is a no-op that returns the first call's task id.

```python
async_task('billing.charge', invoice_id,
    qraft_options={'idempotency_key': f'charge:{invoice_id}'})
```

The key is a permanent dedupe by default, not a "retry if it failed" signal: a `FAILED` or `EXHAUSTED` task still blocks re-enqueue. To run again, use a new key. Uniqueness is enforced by a DB constraint, so concurrent callers racing on one key still produce a single task.

Pass `idempotency_retry_dead=True` to change that: a `FAILED`/`EXHAUSTED` task under the key no longer blocks re-enqueue. The old task's key is cleared with a single race-tolerant `UPDATE` and the call proceeds as a fresh enqueue holding the key. A live (not yet terminal) task under the key is still deduped either way.

```python
async_task('billing.charge', invoice_id,
    qraft_options={
        'idempotency_key': f'charge:{invoice_id}',
        'idempotency_retry_dead': True,
    })
```

## Orphan reaper

If a worker dies mid-task — OOM kill, `kill -9`, hardware loss — the monitor never sees a result. The attempt stays unresolved and the task stays `RUNNING` forever. The reaper finds those and resolves them through the normal retry path.

Liveness comes from a Qraft-owned execution lease, not from Django-Q2. Django-Q2 writes its `Task` row only at completion, so "no `Task` row" says nothing about whether the worker is alive. Instead, the worker stamps `date_started` and `heartbeat_at` on the attempt at `pre_execute` and refreshes `heartbeat_at` from a daemon thread every `heartbeat_interval` seconds until the task ends. A stale heartbeat is positive evidence that the worker died.

An attempt is orphaned when its task is `RUNNING`, the attempt has no outcome, no Django-Q2 `Task` row exists for it, and either:

- its heartbeat is older than `max(3 * heartbeat_interval, min_heartbeat_grace)` — the worker started the task and died, or
- it never heartbeat, is older than `reap_stale_after`, and its pack is no longer queued in the ORM broker — it was delivered to a worker that died before starting it.

A long-running task is never reaped while it heartbeats, however far past `reap_stale_after` it runs — up to one bound. The heartbeat thread stops itself at a hard deadline: `Q_CLUSTER["timeout"] + 60` seconds when a timeout is configured, and 24 hours when none is. A task that outlives its deadline stops heartbeating while it is still running, and the next sweep reaps it as an orphan and retries it, so the work runs twice.

That deadline is above the task timeout by design — with a timeout set, the sentinel kills the process first and the deadline is never reached. It only bites when no `timeout` is configured and a single task runs longer than 24 hours. Set a `timeout` if you have tasks that long.

Reaped attempts are marked failed and handed to the retry policy; with no policy the task goes to `FAILED`. The exception class says which of two things happened:

| `exception_class` | `returned_at` | What it means |
|---|---|---|
| `OrphanedTask` | unset | The worker died somewhere inside the function. How far it got is unknown. |
| `ResultLost` | set | The function ran to the end and its result never reached the monitor. The work is done; a retry does it again. |

`qraft.runner.run_task` stamps `returned_at` and stops the heartbeat the moment the target returns or raises, before the result is handed back to Django-Q2. That closes a gap the lease could not otherwise see: kill the monitor and it dies holding the result queue's write lock, the worker blocks in `result_queue.put()` with the task already finished, and the heartbeat thread keeps the lease fresh for work that is over. The reaper then reads the attempt as alive — correctly, and uselessly. Ending the lease where the function ends makes the heartbeat mean "the attempt is progressing" rather than "the process exists".

Put `ResultLost` in `retry_exceptions` only for a node that is safe to run twice. The retry is usually cheap for an idempotent one — it re-does work whose writes are already there — and it is what lets the graph settle instead of staying open.

The trade this makes: a monitor that is merely slow, not dead, now has the grace period to record a result rather than forever. Past it the attempt is reaped and retried, and when the real completion finally arrives the hook handler's `success IS NULL` compare-and-swap drops it as a late duplicate. That is the same window a dead worker already had, and the grace is `max(3 * heartbeat_interval, min_heartbeat_grace)` — 90s by default, against a monitor that records results in milliseconds when it is healthy. Lower `min_heartbeat_grace` deliberately, not casually: it is the whole margin between "the result is still coming" and "the result is never coming".

The reaper runs automatically as a daemon thread beside the monitor process in `QraftCluster`. Call it directly for tests or a one-off sweep:

```python
from qraft.reaper import reap_orphans
reap_orphans(stale_after=300)   # returns the number of attempts reaped
```

| Setting | Default | Meaning |
|---------|---------|---------|
| `reap_interval` | `60.0` | Seconds between sweeps |
| `reap_stale_after` | `3600.0` | Seconds an attempt that never started may sit unresolved before it counts as orphaned |
| `heartbeat_interval` | `30.0` | Seconds between lease heartbeats from a running worker |
| `min_heartbeat_grace` | `90.0` | Floor on the grace period, so a short `heartbeat_interval` cannot make the reaper trigger-happy |
| `max_executions_per_attempt` | `1` | How many deliveries of one attempt a worker may begin |

`stall_after` is per task, not a cluster setting — see [Stall observation](#stall-observation).

`reap_stale_after` is now only the fallback for attempts that never reached `pre_execute`; running tasks are governed by the heartbeat. Lower `heartbeat_interval` to detect dead workers sooner, at the cost of one small `UPDATE` per task per interval.

Detection latency is `max(3 * heartbeat_interval, min_heartbeat_grace)`, and the floor is what usually decides it: at the default `heartbeat_interval` of 30s the formula gives 90s either way. Lowering `heartbeat_interval` alone changes nothing below 30s. For a pipeline of sub-second nodes, 90s of lost work per crash can cost more than a rare false reap of a briefly-paused worker, and `min_heartbeat_grace` is what to lower — deliberately, together with `heartbeat_interval`:

```python
QRAFT_CLUSTER = {
    "heartbeat_interval": 2.0,
    "min_heartbeat_grace": 6.0,   # detection in ~6s instead of ~90s
}
```

### One attempt, one execution

Django-Q2 redelivers any message it never got an acknowledgement for. After a monitor crash that means re-running a task whose attempt row is still unresolved — and because every re-run refreshed the lease heartbeat, the reaper's liveness test answered "alive" for a task stuck in a redelivery loop. Nothing above resolved it; graphs stayed running until an operator cancelled them.

Every attempt Qraft enqueues now runs through `qraft.runner.run_task`, which claims the delivery before calling anything. The claim is a compare-and-swap on `QraftTaskAttempt.execution_count`: at most `max_executions_per_attempt` deliveries (default 1) are admitted, and an attempt that already has an outcome admits none. A refused delivery does not call the function, does not refresh the lease, counts `qraft.attempt.redelivered`, and raises `qraft.runner.RedeliveredAttempt` — so the attempt resolves through the normal failure path with `exception_class="RedeliveredAttempt"`, and the retry policy, not the broker's delivery loop, decides whether there is an attempt N+1.

`execution_count` and the `redelivered` flag ride on every attempt signal payload.

Raise `max_executions_per_attempt` only if a broker's at-least-once delivery is genuinely the retry mechanism you want; Qraft's own retry policy is the supported one.

The guard covers every attempt Qraft enqueues, because every one of them is queued as `run_task`. One delivery shape predates that and is not covered: a pre-1.3 Django-Q2 `Schedule` row still in flight after an upgrade queues the target function directly, with only a marker in its `task_name`. Such a delivery is refused at the lease — it opens no heartbeat, so it cannot hide from the reaper — but there is no wrapper on that path to stop the function running, and a `pre_execute` receiver cannot raise (django_q's worker wraps no try/except around the send). Those rows drain within one release.

## Stall observation

The heartbeat proves the worker process is alive. It says nothing about whether the task
is getting anywhere. An AI task blocked on a provider call that never returns heartbeats
exactly like one that is scoring rows.

Set `stall_after` and the reaper answers the second question too:

```python
async_task('myapp.tasks.score', worksheet_id,
           qraft_options={'stall_after': 300})   # seconds
```

Every sweep, an attempt is flagged as a suspected stall when all of this holds: its task
declares `stall_after`, the attempt has no outcome yet, its heartbeat is fresh (the worker
is up), it has not already been flagged, and `progress_advanced_at` — falling back to
`date_started` — is older than `stall_after`. `stall_suspected_at` is set by
compare-and-swap, `attempt_stall_suspected` is sent, `qraft.attempt.stall_suspected` is
counted, and the dashboard marks the row.

It reads `progress_advanced_at`, not `progress_reported_at`. A task that reports "waiting
for provider" every ten seconds cannot narrate its way out of detection.

The orphan sweep runs first, so an attempt is never both orphaned and stalled. A
`stall_after` below the heartbeat grace is not a mistake and draws no warning: a task can
heartbeat every thirty seconds while advancing nothing for sixty, and the two thresholds
measure different conditions.

A flagged attempt that advances afterwards keeps the flag as history; the dashboard shows
it as recovered rather than dropping it.

### Nothing is retried

Qraft flags and stops. The attempt is not resolved, no retry is scheduled, and the worker
is not stopped.

That is deliberate, not an omission. The original attempt keeps running. The hook
handler's compare-and-swap would drop its *reported result*, but nothing drops its
database writes or its provider charges. A retry that starts while attempt 1 is still
writing suggestions produces two attempts writing the same rows. Resolving automatically
needs one of three things the library cannot supply: a stopped execution, cooperative
cancellation, or an application contract that tolerates overlapping attempts. Until a
cancellation design exists, the decision stays with the application.

### The ownership pattern

An application that wants to act on a suspected stall itself — or that wants its writes
safe against *any* overlapping attempt, flagged or not — puts the attempt id in the same
statement as the write:

```python
from qraft.context import current_attempt_id, report_progress

def score(worksheet_id):
    attempt_id = current_attempt_id()
    for chunk in chunks(worksheet_id):
        suggestions = model.score(chunk)
        # The attempt id is part of the write, not a check before it.
        updated = Worksheet.objects.filter(
            id=worksheet_id, owner_attempt_id=attempt_id
        ).update(suggestions=suggestions)
        if not updated:
            return          # a newer attempt owns this worksheet now
        report_progress(current=chunk.index, total=chunk.count)
```

A standalone "am I still the current attempt?" check followed by a write is not enough,
because the answer can change between the two. The same shape works as an insert whose
unique key includes the attempt id.

`current_attempt_id()` returns `None` outside a task.

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
