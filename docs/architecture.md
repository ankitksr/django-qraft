# Architecture

This document describes Django-Qraft's internal architecture, design patterns, and extension strategy.

## Design Philosophy

**Core principle**: Drop-in enhancement of Django-Q2 via selective inheritance, not a fork.

Django-Qraft extends Django-Q2 at specific points to add features while preserving compatibility:
- Uses Django-Q2's broker system unchanged
- Reuses Django-Q2's monitor and pusher unchanged
- Selectively overrides cluster and worker spawning
- Enhances hook handling via interception

## System Overview

```
┌─────────────────────────────────────────────────────────┐
│                    QraftCluster                         │
│  (extends django_q.cluster.Cluster)                     │
└──────────────────────┬──────────────────────────────────┘
                       │
                       │ spawns
                       ▼
┌─────────────────────────────────────────────────────────┐
│                   QraftSentinel                         │
│  (extends django_q.cluster.Sentinel)                    │
│                                                         │
│  ┌──────────┐  ┌─────────┐  ┌───────────────────────┐ │
│  │  Pusher  │  │ Monitor │  │  Worker Processes     │ │
│  │ (Django- │  │(Django- │  │  ┌─────────────────┐  │ │
│  │  Q2)     │  │  Q2)    │  │  │  Standard       │  │ │
│  └──────────┘  └─────────┘  │  │  (threads=1)    │  │ │
│                              │  └─────────────────┘  │ │
│                              │  ┌─────────────────┐  │ │
│                              │  │  Threaded       │  │ │
│                              │  │  (threads>1)    │  │ │
│                              │  │  ThreadPool     │  │ │
│                              │  └─────────────────┘  │ │
│                              └───────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

## Extension Pattern: Selective Override

### Inheritance Hierarchy

```python
# Django-Q2 base classes
django_q.cluster.Cluster
django_q.cluster.Sentinel

# Qraft extensions
qraft.cluster.QraftCluster(Cluster)
qraft.cluster.QraftSentinel(Sentinel)
```

### Key Override Points

**1. QraftCluster.start()**
```python
def start(self):
    # Override to spawn QraftSentinel instead of Sentinel
    self.sentinel = QraftSentinel(
        stop_event=self.stop_event,
        start_event=self.start_event,
        # ... other params
    )
    return self.sentinel.start()
```

**2. QraftSentinel.spawn_worker()**
```python
def spawn_worker(self):
    # Conditionally spawn threaded or standard worker
    if self.conf.threads > 1:
        worker = threaded_worker  # Qraft's threaded worker
    else:
        worker = django_q_worker  # Standard Django-Q2 worker

    return self.spawn_process(worker, ...)
```

**3. Hook Handler Interception**
```python
# Register global hook handler
QRAFT_CLUSTER = {
    "hook": "qraft.hooks.qraft_hook_handler",
}

# Handler intercepts all task completions
def qraft_hook_handler(task):
    # Update QraftTask status
    # Dispatch success/failure hooks
    # Handle retries
```

All other Django-Q2 components (pusher, monitor, broker) remain unchanged.

## Database Schema

### Model Relationships

```
┌──────────────────────────────────────────────────────────┐
│                      QraftTask                           │
│  ┌────────────────────────────────────────────────────┐  │
│  │ id (UUID, PK)                                      │  │
│  │ func (str)                                         │  │
│  │ task_args (JSON)                                   │  │
│  │ task_kwargs (JSON)                                 │  │
│  │ status (enum: PENDING/RUNNING/SUCCEEDED/...)       │  │
│  │ success_hook, failure_hook (str, nullable)         │  │
│  │ retry_policy (JSON, nullable)                      │  │
│  │ subject_type, subject_id (str, nullable, indexed)  │  │
│  │ progress (JSON, nullable) — latest attempt snapshot│  │
│  │ hook_context (bool)                                │  │
│  │ created_at, updated_at                             │  │
│  └────────────────────────────────────────────────────┘  │
└─────────────────────┬────────────────────────────────────┘
                      │
                      │ 1:N
                      ▼
┌──────────────────────────────────────────────────────────┐
│                  QraftTaskAttempt                        │
│  ┌────────────────────────────────────────────────────┐  │
│  │ id (AutoField, PK)                                 │  │
│  │ qraft_task (FK → QraftTask)                        │  │
│  │ attempt_number (int)                               │  │
│  │ q2_task_id (str, unique, indexed)                  │  │
│  │ success (bool)                                     │  │
│  │ exception_class (str, nullable)                    │  │
│  │ usage (JSON, nullable)                             │  │
│  │ progress (JSON, nullable)                          │  │
│  │ progress_reported_at, progress_advanced_at         │  │
│  │ enqueued_at, date_started, date_completed          │  │
│  │ heartbeat_at (indexed)                             │  │
│  │ trace_context (str, nullable) — W3C traceparent    │  │
│  │ created_at                                         │  │
│  │                                                    │  │
│  │ unique_together: (qraft_task, attempt_number)      │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
                      ▲
                      │ links to
                      │
┌──────────────────────────────────────────────────────────┐
│              Django-Q2 Task (django_q.Task)              │
│  ┌────────────────────────────────────────────────────┐  │
│  │ id (str, PK)                                       │  │
│  │ name (str)                                         │  │
│  │ func (str)                                         │  │
│  │ args, kwargs (binary)                              │  │
│  │ success (bool)                                     │  │
│  │ result (binary)                                    │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────┐
│                    HookDispatch                          │
│  ┌────────────────────────────────────────────────────┐  │
│  │ id (AutoField, PK)                                 │  │
│  │ qraft_task (FK → QraftTask, CASCADE)               │  │
│  │ hook_type (enum: SUCCESS/FAILURE)                  │  │
│  │ hook_path (str)                                    │  │
│  │ q2_task_id (str, unique)                           │  │
│  │ dispatched_at                                      │  │
│  │                                                    │  │
│  │ unique_together: (qraft_task, hook_type)           │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

### Key Design Decisions

**1. QraftTask as Logical Task**
- Stores function metadata and configuration
- Survives across retries (same UUID for all attempts)
- Tracks overall status (PENDING → RUNNING → SUCCEEDED/FAILED/EXHAUSTED)

**2. QraftTaskAttempt as Execution Record**
- One record per execution (initial + retries)
- Links to Django-Q2 Task via `q2_task_id` (unique, indexed)
- Unique constraint: `(qraft_task, attempt_number)` prevents duplicates

**3. No Foreign Key to Django-Q2**
- `q2_task_id` is a string field, not FK
- Allows Django-Q2 Task to be deleted without breaking Qraft records
- Supports Django-Q2's ORM broker task cleanup

**4. HookDispatch for Idempotency**
- Unique constraint: `(qraft_task, hook_type)` ensures one hook per type
- Prevents duplicate hook execution on monitor restarts
- CASCADE deletion cleans up when QraftTask is deleted

**5. Subject as a String Pair**
- `(subject_type, subject_id)` on the task and on all three workflow models, through the
  abstract `SubjectMixin`, with a composite index `qraft_task_subject_idx`
- Strings rather than a generic foreign key: the subject may live in another database or
  another service
- Members inherit the workflow's subject; retries inherit the task's

**6. Progress on the Attempt**
- The attempt owns `progress` with `progress_reported_at` and `progress_advanced_at`
- `QraftTask.progress` is a denormalised snapshot stamped with `attempt_id`, written only
  when the writer is the task's latest attempt (enforced by the update's own `WHERE`)
- `advanced_at` moves only when `current`/`total` change, so a task narrating a hang
  cannot look busy

**7. A Run Spans Stages That Enqueue Each Other**
- `QraftRun` declares the stages a pipeline expects; `QraftRunStage` binds each to
  exactly one completion unit (a task or a workflow) and records how it ended
- `QraftTask` and the three workflow models gain `run` (`SET_NULL`) and `stage` through
  `RunMemberMixin` — correlation only. Deleting a run must never delete work, and a
  member of a workflow carries the pair without ever settling a stage
- The run settles by derivation over its declared stages, not by an explicit close call:
  the code that knows a pipeline is done is the last stage's success path, the one place
  a crash loses the call
- `runs.note_unit_settled()` is called from inside the window the attempt's `routed` flag
  protects, so `reaper.replay_unrouted()` recovers a crash between a unit settling and
  its stage being recorded

**8. Stall Observation Is a Column, Not a Retry Setting**
- `QraftTask.stall_after` and `QraftTaskAttempt.stall_suspected_at`. `stall_after` is a
  column rather than a `retry_policy` key because `RetryPolicy.from_options` copies a
  whitelist and would drop an unknown key silently — and stalling is not retry semantics
- The reaper flags and stops. The attempt keeps running, so resolving it would let a
  retry write the rows the original is still writing

**9. `settled_at` as Transition Identity**
- The workflow models' settlement compare-and-swap column
- Status alone is not a settlement identity: a chain's final step never advances
  `current_step_index`, a cancel can race a completion, and `resume()` settles the same
  chain twice legitimately
- `resume()` clears it in the same transaction that returns the chain to RUNNING

**10. The Broker Follows the Target Cluster, Not the Producer**
- Django-Q2 resolves an omitted broker against the *enqueuing* process's `Conf`, so a
  Redis-configured web process hands every task to Redis whatever the target cluster runs
- `qraft.brokers.broker_for_cluster()` builds the broker from the target cluster's own
  merged `Q_CLUSTER` entry, and every Qraft enqueue passes it explicitly
- `RoutingBroker` extends the same routing to enqueues Qraft does not make itself
- Degradation (receipts, queue-readable gauges, atomic bind-and-enqueue) is therefore per
  cluster, and capability checks unwrap the router to ask its delegate

**11. `execution_count` as the Delivery Claim**
- Django-Q2 redelivers an unacknowledged message; each re-run refreshed the lease
  heartbeat, so the reaper's liveness test said "alive" for a task in a redelivery loop
- The compare-and-swap on `execution_count` admits at most `max_executions_per_attempt`
  deliveries, and none for an attempt that already has an outcome
- The refusal is raised from `qraft.runner.run_task`, not from the `pre_execute` receiver:
  django_q's worker wraps no try/except around that send, so a raise there would be lost

**12. The Subject May Be Named After the Run Starts**
- The stage that creates a run's domain object is often the run's own first stage
- `runs.bind_subject()` is allowed once, under the run's row lock, and backfills every
  member already bound; a member insert takes the same lock, so neither side can leave a
  member unlabelled

## Task Lifecycle

### Linking a Django-Q2 task to its Qraft attempt

Every attempt — the first and every retry — has a `QraftTaskAttempt` row before it is
enqueued, and the row carries the `q2_task_id`. The hook handler looks it up by that id.
There is no second path.

**Initial task** (`async_task`)
```
1. Create QraftTask (UUID generated)
2. Create QraftTaskAttempt with attempt_number=1
3. Enqueue qraft.runner.run_task through Django-Q2, get q2_task_id
4. Stamp q2_task_id on the QraftTaskAttempt
5. Hook handler: lookup by q2_task_id
```

**Retry** (`schedule_retry` → `scheduler.schedule_attempt`)
```
1. Write a SCHEDULED QraftTaskAttempt carrying the not_before ETA
2. scheduler.dispatch_due() claims it by compare-and-swap and enqueues
   qraft.runner.run_task, stamping q2_task_id on the same row
3. Hook handler: lookup by q2_task_id
```

The user's `task_name` is preserved throughout. Dispatched attempts still carry the
`qraft:{id}:{attempt}` marker name, but only a Django-Q2 `Schedule` row written by a
pre-1.3 release and fired after an upgrade resolves through parsing it.

### State Transitions

```
PENDING ──────┐
              │
              │ task starts
              ▼
          RUNNING ──────┐
              │         │
    success   │         │ failure
              │         │
              ▼         ▼
        SUCCEEDED   FAILED ─────┐
                        │       │
                        │       │ should retry?
                        │       │
                        │    yes│
                        │       ▼
                        │   (schedule retry,
                        │    stays FAILED)
                        │
                    no  │
                        ▼
                   EXHAUSTED
```

### Lifecycle Example

**Scenario**: Task fails twice, succeeds on attempt 3

```python
# User creates task
task_id = async_task('myapp.tasks.flaky', qraft_options={'max_attempts': 3})

# QraftTask: id=UUID-1, status=PENDING, retry_policy={max_attempts:3}
# QraftTaskAttempt: qraft_task=UUID-1, attempt_number=1, q2_task_id="abc123"
```

**Attempt 1**: Task fails
```
1. Worker executes, raises exception
2. Hook handler called by Django-Q2 monitor
3. Lookup QraftTaskAttempt by q2_task_id="abc123" → found
4. Update: QraftTaskAttempt.success=False, exception_class="ValueError"
5. Update: QraftTask.status=FAILED
6. Check retry policy: should_retry=True (1 < 3)
7. Schedule retry: create Schedule with task_name="qraft:UUID-1:2", next_run=+30s
8. Call failure_hook? No (still retrying)
```

**Attempt 2**: Task fails again
```
1. Scheduler queues task from Schedule
2. Worker executes, raises exception
3. Hook handler: q2_task_id lookup fails (task wasn't created via async_task)
4. Fallback: parse task_name="qraft:UUID-1:2" → extract UUID-1, attempt=2
5. get_or_create QraftTaskAttempt(qraft_task=UUID-1, attempt_number=2)
6. Update: success=False, exception_class="ConnectionError"
7. Check retry policy: should_retry=True (2 < 3)
8. Schedule retry: create Schedule for attempt 3, next_run=+60s
```

**Attempt 3**: Task succeeds
```
1. Worker executes, returns result
2. Hook handler: parse task_name="qraft:UUID-1:3"
3. get_or_create QraftTaskAttempt(qraft_task=UUID-1, attempt_number=3)
4. Update: success=True
5. Update: QraftTask.status=SUCCEEDED
6. Check retry policy: N/A (succeeded)
7. Call success_hook('myapp.hooks.on_success')
```

## Hook Dispatching Flow

### Async Hook Execution (Default)

```
Task completes
  │
  ▼
qraft_hook_handler() called by Django-Q2 monitor
  │
  ├─► Lookup QraftTaskAttempt (query or parse task_name)
  │
  ├─► Update attempt outcome (success/exception)
  │
  ├─► Update QraftTask status
  │
  ├─► HookDispatcher.dispatch()
  │     │
  │     ├─► If failed: _handle_retry()
  │     │     │
  │     │     ├─► should_retry? Yes → schedule_retry()
  │     │     │                 No  → set EXHAUSTED, call failure_hook
  │     │     │
  │     │     └─► _call_hook_async('failure_hook')
  │     │           │
  │     │           ├─► Create HookDispatch record (idempotency)
  │     │           │
  │     │           └─► Queue hook as task with hook=None
  │     │
  │     └─► If succeeded: _call_hook_async('success_hook')
  │           │
  │           ├─► Create HookDispatch record
  │           │
  │           └─► Queue hook as task with hook=None
  │
  └─► Return (monitor continues)
```

### Synchronous Hook Execution (sync_hooks=True)

```
Task completes
  │
  ▼
qraft_hook_handler()
  │
  ├─► Lookup attempt, update status (same as async)
  │
  ├─► HookDispatcher.dispatch()
  │     │
  │     └─► _call_hook_sync()
  │           │
  │           ├─► Resolve hook function
  │           │
  │           ├─► Call hook(*args, **kwargs) directly
  │           │
  │           └─► No HookDispatch record created
  │
  └─► Return
```

**Critical**: Retries use `get_or_create()` for QraftTaskAttempt, allowing hook handler to create records for retry executions.

## Observability

### Signal send sites

| Signal | Sent from | Guarded by |
|---|---|---|
| `task_started` | `lease._on_pre_execute_lease` → `stamp_start` (worker) | the `date_started__isnull=True` update matching |
| `attempt_finished`, `task_settled` | `hooks.qraft_hook_handler`, `reaper._reap_one` | the `success__isnull=True` resolution update matching |
| `workflow_settled` | `dispatchers.settle_workflow`, `base.cancel` | the `settled_at__isnull=True` update matching |
| `attempt_stall_suspected` | `reaper.flag_stalls`, after the orphan sweep | the `stall_suspected_at` compare-and-swap matching |
| `run_settled` | `runs._settle`, from the hook handler, the dispatchers, the reaper and the explicit transitions | the run's `settled_at` compare-and-swap matching |
| `run_overdue` | `runs.flag_overdue` in the reaper thread | the `overdue_flagged_at` compare-and-swap matching |

Every send is registered with `transaction.on_commit` from inside the transaction that
performs the transition, and every send is `send_robust()` — a receiver that raises is
logged, not propagated into the monitor's completion routing. Payloads are immutable
mappings of ids and ISO timestamps, never model instances: an instance captured before
commit is a snapshot that may already be stale by the time a receiver reads it.

The window this does not cover is a crash between commit and the `on_commit` callback.
The row still records the transition; anything an application must not miss goes through
a hook, whose `HookDispatch` row survives the crash and whose replay the reaper owns.

### Run settlement

`qraft/runs.py` holds the whole surface: `start`, `bind`, `skip`, `cancel`, `abandon`,
`note_unit_settled` and `flag_overdue`. Two compare-and-swaps carry it. The stage's
`settled_at` decides whether a unit's outcome is recorded at all — which is also what
keeps a workflow *member* out, since the update matches on the stage's `unit_id`. The
run's own `settled_at` decides whether the derived transition takes effect. Both run
under `select_for_update()` on the run row, so a bind racing a cancel and two stages
finishing at once each produce one decision rather than two.

Retention protects run membership in every pass. The workflow pass matters as much as
the task pass: deleting an iter or batch cascades to its member tasks, so a completed
batch under an open run would otherwise lose its rows through the workflow pass alone.
Terminal runs are pruned after their members, and the `summary` written at settlement is
what makes that safe.

### Metrics

`qraft/metrics/__init__.py` resolves `QRAFT_CLUSTER["metrics_sink"]` once per process and
wraps every emission: labels are checked against a forbidden set (`subject_id`, `run_id`,
`task_id`, `attempt_id`, `revision`, `metadata`) and a sink that raises is caught,
counted and throttled to one log line a minute. `qraft/metrics/gauges.py` holds the
backlog gauges, emitted from the dispatcher loop only on the one cluster flagged
`metrics_gauges=True`.

`qraft/dashboard/metrics.py` is unrelated: it computes percentiles from the tables on
request, for the dashboard page, and works with the sink set to null.

### Cost

`qraft/pricing.py` is schemaless: entries live in the existing `usage` JSON. A
`PricingResolver` answers `price(model, provider)`; `StaticTablePricing` reads
`QRAFT_PRICING`, and no setting means no resolver. `record_usage()` prices each increment
as it is written, so a corrected table never re-prices what a provider already billed,
and an attempt that calls two models is not billed at one rate. `cost()` sums the entries
into a `CostSummary` with `coverage` and `estimated` flags, re-pricing at read time what
an older table left unpriced.

### Trace and log context

`qraft/tracing.py` injects a W3C `traceparent` at enqueue when `opentelemetry-api` is
importable and a span is current, and starts a child span at `pre_execute` that ends when
the lease's heartbeat loop does. `qraft/context.py` binds the attempt's ids into a
`ContextVar` at the same point; `qraft/logging.QraftContextFilter` reads them onto every
log record, and `current_attempt_id()` exposes the attempt id to task code.

## Configuration System

### Settings Hierarchy

```
1. Django settings.QRAFT_CLUSTER (primary)
2. Django settings.Q_CLUSTER (fallback, with warning)
3. Environment variable Q_CLUSTER_NAME (selects ALT_CLUSTERS entry)
4. Pydantic defaults
```

### Custom Django Settings Source

```python
class DjangoSettingsSource:
    """Load settings from Django's settings.QRAFT_CLUSTER or Q_CLUSTER"""

    def __call__(self):
        # 1. Try QRAFT_CLUSTER
        if hasattr(django_settings, "QRAFT_CLUSTER"):
            return django_settings.QRAFT_CLUSTER

        # 2. Fallback to Q_CLUSTER
        if hasattr(django_settings, "Q_CLUSTER"):
            logger.warning("Using Q_CLUSTER (deprecated). Use QRAFT_CLUSTER.")
            return django_settings.Q_CLUSTER

        return {}
```

### ALT_CLUSTERS Pattern

For mixed workloads (CPU + I/O), run multiple clusters with different configs:

```python
QRAFT_CLUSTER = {
    "name": "default",
    "workers": 4,
    "threads": 1,  # Standard workers

    "ALT_CLUSTERS": {
        "io-workers": {
            "workers": 2,
            "threads": 8,  # Threaded workers
            "max_inflight": 16,
        },
        "cpu-workers": {
            "workers": 8,
            "threads": 1,  # More processes for CPU
        }
    }
}
```

Start clusters:
```bash
python manage.py qraftcluster                          # default
Q_CLUSTER_NAME=io-workers python manage.py qraftcluster  # io-workers
Q_CLUSTER_NAME=cpu-workers python manage.py qraftcluster # cpu-workers
```

Route tasks:
```python
async_task('cpu.task')                     # → default
async_task('io.task', cluster='io-workers')   # → io-workers
async_task('cpu.task', cluster='cpu-workers') # → cpu-workers
```

Each cluster may also run its own broker — one on the ORM broker while the rest stay on
Redis. `qraft.brokers.broker_for_cluster()` resolves it from that cluster's merged
`Q_CLUSTER` entry, and every Qraft enqueue names the result, so routing does not depend
on what the enqueuing process happens to run. See
[configuration.md](configuration.md#mixed-brokers-across-clusters).

## Worker Architecture

### Standard Worker (threads=1)

```
Worker Process
  │
  ├─► Listen on task_queue (blocking)
  │
  ├─► Receive task dict
  │
  ├─► Resolve function from dotted path
  │
  ├─► Execute: result = func(*args, **kwargs)
  │
  ├─► Publish result to result_queue
  │
  └─► Loop
```

**Characteristics:**
- One task at a time per process
- Process-level timeout via sentinel timer
- Signal-based termination (SIGTERM)
- Best for CPU-bound tasks

### Threaded Worker (threads>1)

```
Worker Process
  │
  ├─► Create ThreadPoolExecutor(max_workers=threads)
  │
  ├─► Create Semaphore(max_inflight)
  │
  ├─► Listen on task_queue (blocking, in main thread)
  │
  ├─► Receive task dict
  │
  ├─► Acquire semaphore (backpressure control)
  │
  ├─► Submit to thread pool:
  │     └─► Thread executes task
  │           │
  │           ├─► close_old_connections() before
  │           ├─► result = func(*args, **kwargs)
  │           ├─► close_old_connections() after
  │           │
  │           ├─► Publish result to result_queue
  │           │
  │           └─► Release semaphore
  │
  └─► Loop
```

**Characteristics:**
- Multiple tasks concurrently (up to `threads`)
- Backpressure via semaphore (`max_inflight`)
- Database connection per thread
- Process-level timeout (kills all threads)
- Best for I/O-bound tasks

## Performance Characteristics

### Threading Overhead

- ThreadPoolExecutor creation: one-time per worker process
- Semaphore acquire/release: ~microseconds per task
- Extra DB query for attempt lookup: ~1ms per task completion
- Connection management: ~1ms per task (open/close)

### Expected Speedup (I/O-bound)

See [Threading](threading.md#performance-characteristics) for the throughput table and the caveat that
those numbers are modelled ceilings, not measurements.

### No Speedup For:

- CPU-bound tasks (Python GIL prevents true parallelism)
- Tasks with many DB writes (DB becomes bottleneck)
- Tasks that block on locks or shared resources

## Key Extension Points

For developers extending Qraft:

1. **Custom worker types**: Override `QraftSentinel.spawn_worker()`
2. **Custom hook dispatching**: Override `HookDispatcher._call_hook_async()`
3. **Custom retry logic**: Override `HookDispatcher._handle_retry()`
4. **Custom status tracking**: Extend `QraftTask` model
5. **Custom brokers**: Use Django-Q2's broker system (works unchanged)

## Security Model

### Queue payloads are signed pickles

Qraft enqueues through Django-Q2, so a queued message is a pickled task dict signed with
`settings.SECRET_KEY` (`django_q.signing.SignedPackage`). Signing proves the message came
from something holding the key. It does not make unpickling safe: a worker that unpickles
an attacker-chosen payload executes attacker-chosen code.

Two things therefore have to hold:

1. **`SECRET_KEY` is a code-execution credential.** Anyone who has it can enqueue a message
   that runs arbitrary code on every worker. Treat a leak as worker compromise, not as a
   session-cookie problem, and rotate deliberately — in-flight messages signed with the old
   key stop validating.
2. **Write access to the queue table is equivalent to write access to the workers.** The
   ORM broker keeps messages in `django_q.OrmQ` in your own database. A crafted row there
   is unpickled by a worker exactly like a legitimate one.

This is inherited from Django-Q2 rather than introduced by Qraft, and it is unchanged by
running the ORM broker. Qraft's own columns are safer: `QraftTask.func` is a dotted path
and `task_args`/`task_kwargs` are JSON, so a retry re-enqueues from JSON rather than from
the original pickle. The pickle stays on the initial enqueue path.

Phase 3 of the [absorption plan](future/q2-absorption.md) — a Qraft-owned queue table —
would let the initial enqueue drop pickle too, which is a security benefit and not only an
architectural one.

## Common Pitfalls

1. **A pre-1.3 retry has no attempt row**
   - Owned scheduling creates the `SCHEDULED` attempt row before the retry is enqueued,
     so the hook handler resolves it by `q2_task_id` like any other attempt
   - Solution: the marker-parsing fallback (`qraft:{id}:{attempt}`) remains for one
     release, to resolve pre-1.3 `Schedule` deliveries still in flight across an upgrade

2. **Threading != parallelism for CPU-bound**
   - Python GIL prevents true parallelism
   - Solution: Use standard workers (threads=1) for CPU tasks

3. **Process-level timeouts with threading**
   - One stuck thread terminates entire worker process
   - Solution: Keep tasks under timeout, or use standard workers

4. **Database connections per thread**
   - Each thread needs a connection
   - Solution: Size connection pool ≥ workers × threads

5. **Hook recursion**
   - Hooks must use `hook=None` to prevent infinite loops
   - Solution: Framework enforces this automatically

6. **task_name preservation**
   - User's task_name is preserved, not overwritten
   - Solution: Use `q2_task_id` for linkage, not task_name parsing

## Related Documentation

- [Configuration](configuration.md) - Settings reference
- [Threading](threading.md) - Multithreaded worker details
- [Hooks](hooks.md) - Hook system details
- [Retry](retry.md) - Retry policy details
- [Development](development.md) - Contributing guidelines
