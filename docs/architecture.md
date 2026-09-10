# Architecture

This document describes Django-Qraft's internal architecture, design patterns, and extension strategy.

## Design Philosophy

**Core principle**: drop-in enhancement of Django-Q2 by selective inheritance, not a fork.
Qraft overrides cluster and worker spawning and intercepts hook handling; the broker,
monitor and pusher run unchanged.

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

**7. A Graph Owns the Plan and the Dispatch**
- `QraftGraph` holds one execution of a plan; `QraftGraphNode` holds each node, its
  `after` edges and the one task bound to it. The whole plan is written at `start()` and
  the topology is sealed there — no node adds nodes
- Qraft dispatches every node whose dependencies are met, through a scheduled attempt
  (`graphs._dispatch_node`). The application never enqueues the next step, so a crash
  between two nodes cannot strand a pipeline. `resume()` is the one exception: it commits
  the reset before dispatching the frontier, so a crash in that window leaves pending
  nodes with nothing queued
- `QraftTask` and the three workflow models gain `graph` (`SET_NULL`) and `node` through
  `GraphMemberMixin` — correlation only, so deleting a graph never deletes correlated
  work. Membership is the other relation: `QraftTask.graph_node` is `CASCADE`, so a
  node's own execution tasks go with the graph. A workflow's members carry the
  correlation pair without ever settling a node
- `graphs.handle_node_completion()` runs from the monitor's completion routing, inside
  the window the attempt's `routed` flag protects, so `reaper.replay_unrouted()` recovers
  a crash between a task settling and its node being recorded. Receipt-based recovery
  (`reaper._resolve_committed`) is outside that window — it sets `routed=True` in the same
  breath as scheduling the completion, so a crash before the `on_commit` callback is not
  replayed

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

**12. The Subject May Be Named After the Graph Starts**
- The node that creates a graph's domain object is often the graph's own first node
- `graphs.bind_subject()` is allowed once, under the graph's row lock, and backfills every
  member already bound; a member insert takes the same lock, so neither side can leave a
  member unlabelled

## Task Lifecycle

### Linking a Django-Q2 task to its Qraft attempt

Every attempt — the first and every retry — has a `QraftTaskAttempt` row carrying its
`q2_task_id`, and the hook handler looks it up by that id. There is no second path. A
retry's row exists before the message is enqueued; the first attempt's row is written
just after `q2_async_task()` returns the id, inside the same transaction.

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
`qraft:{id}:{attempt}` marker name, and `lease` parses it whenever the `q2_task_id`
lookup finds no attempt — the pre-1.3 `Schedule` row fired after an upgrade, and the
delivery that beats the dispatcher's own commit of the id on an external broker.

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

**Scenario**: a task fails twice and succeeds on attempt 3.

```python
task_id = async_task(
    'myapp.tasks.flaky',
    qraft_options={
        'max_attempts': 3,
        'jitter': False,                       # so the ETAs below are exact
        'success_hook': 'myapp.hooks.on_success',
    },
)

# QraftTask: id=UUID-1, status=RUNNING, retry_policy={max_attempts: 3, ...}
# QraftTaskAttempt: qraft_task=UUID-1, attempt_number=1, q2_task_id="abc123"
```

A task is created `RUNNING`, not `PENDING`: the message is on the broker and the row
describes work in flight. `PENDING` is where a task waits for a *scheduled* attempt.

**Attempt 1 fails**
```
1. The worker executes run_task, the target raises ValueError
2. The monitor's hook handler resolves the attempt by q2_task_id="abc123"
3. Resolution update: success=False, exception_class="ValueError"
4. QraftTask.status=FAILED for the moment the resolution holds it there
5. The retry policy allows attempt 2, so scheduler.schedule_attempt writes a
   SCHEDULED QraftTaskAttempt with not_before=+30s and puts the task back to
   PENDING — an attempt failed, the task has not
6. No failure hook: the task is still retrying
```

**Attempt 2 fails**
```
1. scheduler.dispatch_due() claims the SCHEDULED row by compare-and-swap at its
   exact due time, enqueues run_task and stamps q2_task_id on that same row
2. The worker executes, the target raises ConnectionError
3. The hook handler resolves the attempt by q2_task_id — the same one path
4. The retry policy allows attempt 3: a SCHEDULED row with not_before=+60s
```

**Attempt 3 succeeds**
```
1. dispatch_due() claims and enqueues the row as before
2. The worker executes, the target returns
3. The hook handler resolves the attempt: success=True
4. QraftTask.status=SUCCEEDED
5. The configured success hook is dispatched through its HookDispatch row
```

With the default `jitter=True`, 30s and 60s are the base delays and the real ETAs are
randomised below them.

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
| `task_started` | `lease._on_pre_execute_lease` → `claim_delivery` → `_announce_start` (worker) | the `date_started__isnull=True` update matching |
| `attempt_finished`, `task_settled` | `hooks.qraft_hook_handler`, `reaper._reap_one` | the `success__isnull=True` resolution update matching |
| `workflow_settled` | `dispatchers.settle_workflow`, `base.cancel` | the `settled_at__isnull=True` update matching |
| `attempt_stall_suspected` | `reaper.flag_stalls`, after the orphan sweep | the `stall_suspected_at` compare-and-swap matching |
| `node_settled` | `graphs.handle_node_completion`, and `graphs.skip` for a pending node | the node's `settled_at` compare-and-swap matching |
| `graph_settled` | `graphs._settle`, from the completion routing and the explicit transitions | the graph's `settled_at` compare-and-swap matching |
| `graph_overdue` | `graphs.flag_overdue` in the reaper thread | the `overdue_flagged_at` compare-and-swap matching |

Every send is registered with `transaction.on_commit` and is `send_robust()`. Most
transitions run inside an explicit transaction, so the callback fires at its commit;
`flag_overdue` and the lease's claim run in autocommit, where the callback fires at once — a receiver that raises is
logged, not propagated into the monitor's completion routing. Payloads are immutable
mappings of ids and ISO timestamps, never model instances: an instance captured before
commit is a snapshot that may already be stale by the time a receiver reads it.

The window this does not cover is a crash between commit and the `on_commit` callback.
The row still records the transition; anything an application must not miss goes through
a hook, whose `HookDispatch` row survives the crash and whose replay the reaper owns.

### Graph settlement

`qraft/graphs.py` holds the whole surface: `Graph.start`, `get`, `skip`, `cancel`,
`resume`, `preview_resume`, `approve`, `reject`, `publish`, `bind_subject`, `snapshot`,
`flag_overdue` and `replay_settled_hooks`. Two compare-and-swaps carry settlement. The
node's `settled_at` decides whether a task's outcome is recorded at all. A workflow
*member* never reaches it, because completion routing reads `graph_node` and a member
carries only the correlation pair; a task from a superseded generation is dropped by the
separate check that the node is still bound to it. The graph's own `settled_at` decides
whether the transition takes effect. Completion routing takes `select_for_update()` on
the graph row, so a bind racing a completion and two nodes finishing at once each produce
one decision rather than two; `cancel()` settles through the same compare-and-swap
without taking that lock, and reaches a graph parked at a gate as well as a running one.

A graph settles on quiescence, not on first failure: while any node is running or ready
to dispatch, the graph stays running, and only when nothing can advance does the outcome
derive from the node statuses — failed, then cancelled, then succeeded when every node
succeeded or was skipped. A node parked at an approval gate moves the graph to
`WAITING_APPROVAL`, which is not terminal.

Retention protects the membership of a graph that has not settled, in every pass. The workflow
pass matters as much as the task pass: deleting an iter or batch cascades to its member
tasks, so a completed batch under a running graph would otherwise lose its rows through
the workflow pass alone. A graph parked at `WAITING_APPROVAL` is live for the same reason.
Terminal graphs are pruned after their members, and the `summary` written at settlement is
what makes that safe.

### Metrics

`qraft/metrics/__init__.py` resolves `QRAFT_CLUSTER["metrics_sink"]` once per process and
wraps every emission: labels are checked against a forbidden set (`subject_id`, `graph_id`,
`graph`, `task_id`, `attempt_id`, `revision`, `metadata`, `generation`) and a sink that raises is caught,
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
1. Django settings.QRAFT_CLUSTER
2. Environment variable Q_CLUSTER_NAME (selects the ALT_CLUSTERS entry to merge)
3. Pydantic defaults
```

`Q_CLUSTER` is not a fallback source. The one value Qraft reads from it is an explicit
`save_limit`, which becomes `retention_max_tasks` when no retention key is set in
`QRAFT_CLUSTER` — a user who bounded Django-Q2's history has already said Qraft's tables
should be bounded too. Django-Q2's own default of 250 is deliberately not mirrored, since
that would prune the history of a user who never asked for pruning.

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

See [Threading](threading.md#performance-characteristics) for the throughput table and the
caveat that those numbers are modelled ceilings, not measurements. Threading buys nothing
for CPU-bound work (the GIL), for write-heavy tasks (the database is the bottleneck), or
for tasks that block on a shared lock.

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

## Related Documentation

- [Configuration](configuration.md) - Settings reference
- [Threading](threading.md) - Multithreaded worker details
- [Hooks](hooks.md) - Hook system details
- [Retry](retry.md) - Retry policy details
- [Development](development.md) - Contributing guidelines
