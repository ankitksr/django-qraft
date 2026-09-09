# Observability: subjects, runs, signals, stalls, metrics, cost

> This design has shipped. For the user-facing reference on runs, see
> [`docs/runs.md`](../runs.md). This document is kept for the reasoning behind the
> shipped design.

Qraft records everything about a task and nothing about the thing the task is
for. This document proposed the additions that close that gap: a subject on
every task, attempt-owned progress, signals Qraft emits itself, a metrics sink
and logging context, a run that groups a subject's stages, stall observation,
and cost derived from usage. They shipped in four steps across 1.4.0 and 1.5.0.

## 1. Why

The consumer that motivates this is a Django app running a per-document
pipeline: a report arrives, an ingest task loads it, a rules task classifies
its rows, an AI task scores what the rules left undecided, and only then is
the document ready for a person. Each stage is a Qraft task. Each stage
enqueues the next from its own success path. The app's operators ask six
questions about that pipeline, and today Qraft answers none of them without
the app building its own tables beside Qraft's.

*Which tasks belong to worksheet 4117?* `QraftTask` has `func`, `task_args`,
`task_kwargs`, `idempotency_key`, `priority`, and workflow foreign keys
(`qraft/models/tasks.py`). The worksheet id is inside `task_kwargs` JSON, which
nothing indexes. The admin searches `id` and `func` only
(`qraft/admin.py`, `QraftTaskAdmin.search_fields`); the dashboard lists the
newest sixty tasks with no filter (`qraft/dashboard/views.py`, `TASK_LIMIT`).

*Tell me when a stage finishes, so I can write my own domain event.* Qraft
consumes Django-Q2's `pre_execute` and `post_execute` (`qraft/apps.py`) and
emits nothing of its own. The only way to react to a completion is a hook
string per task, which runs as a separate queued task and receives only the
arguments the caller froze at enqueue: it does not know which attempt
produced it or what the outcome was (`qraft/hooks.py`,
`HookDispatcher.dispatch_success_hook`). Parallel workflows have a
`progress_hook` (`qraft/dispatchers.py`, `_dispatch_progress_hook`); chains
and plain tasks do not.

*How long from report arrival to ready?* Nothing spans stages. A chain would,
but the stages here enqueue each other from application code, not from a
chain definition, and the roadmap lists nested workflows as unshipped.

*Is this AI task hung on a provider call?* The lease heartbeat
(`qraft/lease.py`) proves the worker process is alive. It says nothing about
whether the task is making progress. `report_progress()` writes one JSON blob
to `QraftTask.progress` and overwrites it on every call with no timestamp and
no record of which attempt wrote it (`qraft/context.py`), so a task whose
last progress write was forty minutes ago looks identical to one that wrote a
second ago, and a retry inherits its predecessor's 90%.

*Give me the numbers in Grafana, and let me follow one slow worksheet.*
`qraft/dashboard/metrics.py` computes percentiles on request from the tables,
capped at 5,000 samples, for the dashboard page only. Nothing is exported,
log lines carry no task or attempt id, and no trace context crosses the
enqueue boundary.

*What did this worksheet cost?* `record_usage()` sums tokens per attempt. The
`usage` field's help text names `cost` as a key the app may write, and
`docs/ai-workloads.md` shows the app computing it, but Qraft holds no prices
and cannot turn tokens into money on its own.

Every one of these gaps has the same shape: the execution record is complete,
the domain correlation is absent. An app can bridge it with its own event
table, its own progress keys, and its own token ledger, which is exactly what
the motivating consumer has built and wants to delete.

## 2. Principles

- **The row is the truth; everything else is derived from it.** Subject, run,
  stage, progress, timestamps and cost all live on Qraft's own tables.
  Signals and metrics are emitted from those rows after commit and are never
  the only record of anything. Anything an application must not miss goes
  through a hook, whose dispatch row survives a crash. This is what makes
  best-effort signals acceptable (section 4.3) and what requires a durable
  hook beside the run signal (section 4.6).
- **Qraft owns execution evidence; the application owns meaning.** Qraft says
  which attempt ran, when, and how it ended. Whether a worksheet is "ready",
  whether a stalled call may be retried, and what a token is worth are
  application decisions Qraft records but does not make. This is why stall
  detection observes and does not act (section 4.7) and why pricing is
  optional (section 4.8).
- **A pipeline whose stages enqueue each other is a first-class shape.** The
  motivating consumer does not use a chain and should not have to. This is
  what makes the run a lighter primitive than nested workflows (section 4.6)
  and what rules out deriving run completion from "no live members".
- **Every guarantee names its guard.** A claim of at-most-once points at the
  compare-and-swap that enforces it. Where the code has no such guard today,
  the spec adds one rather than asserting the property.
- **Nothing per-subject reaches a metrics label.** Subject ids are unbounded;
  metric cardinality is not. Subject type, function, cluster, outcome and
  exception class are labels; subject id, run id and task id are never
  (section 4.4).
- **Additive and nullable.** Every new column is nullable or defaulted, every
  new kwarg optional, and a caller that passes none of them gets exactly the
  1.3.0 behaviour.

## 3. Design

A task gains three optional labels: a subject (`subject_type`, `subject_id`),
a run it belongs to, and the stage it plays in that run. Progress moves from
the task to the attempt that produced it, with two timestamps: when it was
last reported and when it last advanced. A run is a small row the app creates
when a pipeline starts, declaring the stages it expects. Each stage is bound
to exactly one completion unit, a task or a workflow, and settles when that
unit does; the run settles when every stage has. Workflows carry the same
labels and copy them to their members for correlation, but a member never
settles a stage.

Around those rows Qraft adds three read paths and one detector. Signals fire
after each transition commits, carrying ids and outcomes rather than model
instances, so the app writes cheap observers from one receiver. A metrics
sink receives counters, histograms and gauges at the same points, labelled
without ids, through the OpenTelemetry API; a logging filter and trace
propagation let an operator follow one attempt across processes. The
dashboard, admin and a queryset helper filter by subject and run. The reaper
gains a second test beside the heartbeat: an attempt whose progress last
advanced longer ago than its declared `stall_after`, while its heartbeat is
fresh, is flagged as a suspected stall. It is not retried; the application
decides. Cost is an optional resolver over the usage rows, recorded per
increment so a corrected table never re-prices what a provider already
billed.

```
app: runs.start(subject=("worksheet","4117"), stages=["ingest","rules","ai"])
        │
        ▼
 QraftRun ──1:N── QraftRunStage(unit) ──▶ QraftTask | Chain | Iter | Batch
    │                                          │
    │                                          ▼ members inherit run+stage
    │                                     QraftTask(subject, run, stage)
    │                                          │
    │                                          ▼
    │                                     QraftTaskAttempt(progress, advanced_at,
    │                                                      enqueued_at, stall_suspected_at)
    │  unit settles ⇒ stage settles         │
    │  all stages ⇒ run settles              │ commit
    ▼                                        ▼
 on_settled hook (durable)          signals (best effort) ─▶ app receivers
 summary snapshot                   metrics (labels, no ids) ─▶ sink
                                    reaper: heartbeat fresh, advanced_at stale
                                            ⇒ stall_suspected (flag, signal, metric)
```

The execution path (pusher, worker, monitor, hook handler compare-and-swap)
is unchanged; every addition hangs off a transition that already commits, and
every new column is nullable.

## 4. Detail

The sections below follow the order they shipped in: what shipped first is
argued first.

### 4.1 Subject

Exists so "every task for X" is one indexed query (section 1, first
question).

**Data model.** `QraftTask` gains `subject_type` (`CharField(100)`, null) and
`subject_id` (`CharField(255)`, null), with a composite index
`qraft_task_subject_idx` on `(subject_type, subject_id)`. `QraftChainModel`,
`QraftIterModel` and `QraftBatchModel` gain the same pair through a shared
abstract `SubjectMixin` in `qraft/models/mixins.py`. Strings, not a generic
foreign key: the subject may live in another database or another service, and
a string pair is what every consumer can produce.

The alternative was an indexed JSON `tags` field. JSON containment queries
need a GIN index that only Postgres provides, filtering in admin and dashboard
becomes a free-text expression rather than two equality tests, and the
motivating queries are all "one entity, one id". Tags can be added later
without disturbing the pair; the pair cannot be recovered from tags without a
convention.

**API.** `async_task(..., qraft_options={"subject": ("worksheet", "4117")})`.
The tuple's second element is coerced with `str()`, so integer ids work. The
option lives in `qraft_options` rather than as a named parameter: named
parameters on `async_task` shadow function kwargs of the same name, and
`subject` is a plausible function kwarg. Workflow constructors take
`subject=` directly since they have no kwargs passthrough:
`QraftChain(subject=("worksheet", "4117"))`, likewise `QraftIter` and
`QraftBatch`. `_create_workflow_task` (`qraft/tasks.py`) and
`_queue_chain_step` (`qraft/dispatchers.py`) copy the workflow's subject onto
each member. Retries inherit automatically: the subject is on the task, not
the attempt.

A manager method `QraftTask.objects.for_subject(subject_type, subject_id)`
returns the queryset, newest first, and is the one call the app's detail view
needs.

**Dashboard and admin.** `api/state/` accepts `subject_type`, `subject_id`
and `run` query parameters and filters the task and workflow rows when given.
The page gets one text input that sets them. `QraftTaskAdmin` adds
`subject_type` to `list_filter` and `subject_type`, `subject_id` to
`search_fields` and `list_display`. Workflow admins do the same.

**Compatibility.** Rows without a subject show a dash and are excluded from
subject filters. Nothing else changes.

### 4.2 Attempt-owned progress

Exists so progress identifies the attempt that produced it and separates
"said something" from "moved" (section 1, fourth question), which is what
stall observation (section 4.7) measures.

**Data model.** `QraftTaskAttempt` gains `progress` (JSON, null),
`progress_reported_at` (DateTime, null) and `progress_advanced_at` (DateTime,
null). `QraftTask.progress` stays as a denormalised snapshot for the
dashboard's task rows, and its payload gains an `attempt_id` key naming the
attempt that wrote it. The attempt owns progress because a retry is a new
execution: attempt 2 starting at zero must not display attempt 1's 90%, and
an attempt 1 that is still running (a stall, section 4.7) must not overwrite
attempt 2's numbers. With the payload on the attempt neither can happen; the
snapshot on the task is written only when the writer is the task's latest
attempt, enforced by the update's own `WHERE` (no attempt of this task has a
higher `attempt_number`), so a late write from a superseded attempt updates
zero rows.

**Reported versus advanced.** `progress_reported_at` moves on every call.
`progress_advanced_at` moves only when `current` or `total` differ from the
stored values. A task that reports "waiting for provider" every ten seconds
has a fresh `reported_at` and a stale `advanced_at`, and it is `advanced_at`
the stall detector reads. Without the distinction, a task could defeat
detection by narrating its own hang.

**Atomic updates.** The current helpers read the JSON, merge in Python and
save (`qraft/context.py`, `report_progress` and `record_usage`), so two
reporters in one attempt, which a threaded task or a coroutine fan-out can
produce, lose increments. On Postgres the merge becomes one statement:
`progress = COALESCE(progress, '{}'::jsonb) || %s::jsonb`, with `advanced_at`
set through a `CASE` that compares the incoming `current`/`total` against
`progress->>'current'` and `progress->>'total'`. On other databases the helper
takes `select_for_update()` on the attempt row and does the read-modify-write
inside that lock. The same treatment applies to `record_usage` (section 4.8).
`areport_progress` and `arecord_usage` are added for coroutine tasks and use
the async ORM; the sync helpers keep working under `sync_to_async`.

**Coalescing.** `progress_min_interval` (seconds, `QRAFT_CLUSTER`, default 0)
lets a chunk-heavy task throttle its writes: a call within the interval since
the attempt's last write is skipped unless `current`/`total` changed or the
caller passes `force=True`. The default keeps 1.3.0 behaviour. "One extra
write is invisible" was an assumption in the first draft; this setting is
what makes it a choice the operator can measure and adjust.

**Dashboard.** Task rows show the latest attempt's progress and the age of
`advanced_at`. The attempt inline in the admin shows all three fields.

### 4.3 Signals

Exists so the app reacts to transitions from one receiver instead of one hook
per task (section 1, second question), while the row stays the truth
(principle 1).

**Module.** `qraft/signals.py`, which does not exist today, defines these
`django.dispatch.Signal` instances:

| Signal | Fires when | Process |
|---|---|---|
| `task_started` | the lease opens on an attempt for the first time | worker |
| `attempt_finished` | an attempt resolves, whatever the outcome | monitor, or the process that reaped |
| `task_settled` | a task reaches SUCCEEDED, FAILED or EXHAUSTED | monitor |
| `workflow_settled` | a chain, iter or batch settles | monitor, or the web process that cancelled |
| `attempt_stall_suspected` | the reaper flags an attempt (section 4.7) | monitor |
| `run_settled`, `run_overdue` | a run settles or is flagged overdue (section 4.6) | monitor |

There is no separate `attempt_failed`: a receiver that only wants failures
reads `success` on `attempt_finished`, and one signal with a flag is easier
to keep consistent than two that must never both fire.

**Payloads are ids, not instances.** Every signal sends `sender=` the model
class and one `payload` kwarg: an immutable mapping (`types.MappingProxyType`
over a plain dict) holding `task_id`, `attempt_id`, `attempt_number`,
`outcome`, `exception_class`, `run_id`, `stage`, `subject_type`,
`subject_id`, and the relevant timestamps as ISO strings. A model instance
captured before commit and handed to a receiver after it is a snapshot that
may already be stale; ids are what a receiver should look up if it needs
more. The mapping is also what lets the same payload feed the metrics sink
and the logging filter.

**`send_robust`, always.** Django's `Signal.send()` propagates the first
receiver exception into the caller, which here is the hook handler in the
monitor. A broken observer must not break completion routing, so every send
uses `send_robust()`, and a returned exception is logged at warning with the
receiver's name. Signals are best-effort observers for logs, caches and
in-process reactions; anything the application must not miss goes through a
hook. That sentence goes in `docs/hooks.md` verbatim.

**Send sites.** `task_started` is sent from `_on_pre_execute_lease`
(`qraft/lease.py`). `stamp_start` today updates by `q2_task_id`
unconditionally, so a redelivered message would stamp and notify twice; the
update becomes conditional on `date_started__isnull=True`, and the signal and
the `attempt.started` metric fire only when that update matched a row. A
second delivery still refreshes `heartbeat_at` through the existing
`touch()` path. This runs in the worker process, which is why it is listed
separately: receivers for it execute in the worker, beside the task.

`attempt_finished` and `task_settled` are sent from `qraft_hook_handler`
(`qraft/hooks.py`) inside the `transaction.atomic()` block that resolves the
attempt, and from `_reap_one` (`qraft/reaper.py`) inside its own.
`workflow_settled` is sent from the settlement compare-and-swap described
next. `run_settled` and `run_overdue` are sent from run settlement and the
overdue sweep (section 4.6).

**After commit, not inside.** Every send is registered with
`transaction.on_commit` from inside the atomic block that performs the
transition. A receiver will read rows, and it must see the committed
resolution rather than the pre-transaction snapshot its own connection would
otherwise serve; and a transaction that rolls back after the send point must
not have announced a transition that never happened. Outside any
transaction, `on_commit` runs the callback immediately, so autocommit
behaviour is unchanged.

**Transition identity for workflows.** The existing guards make the
*transitions* idempotent but do not identify a *settlement*. On a chain's
final step, `_handle_step_success` (`qraft/dispatchers.py`) sets SUCCEEDED
without advancing `current_step_index`, so a replayed completion passes the
index check and enters the success path again. `_complete_chain` sets FAILED
under the lock with no check that the chain was already terminal. `cancel()`
runs in whatever process calls it (`qraft/base.py`). And `resume()` moves a
FAILED chain back to RUNNING (`qraft/chain.py`), so one chain can legitimately
settle twice. Workflow id alone cannot distinguish these.

Each workflow model gains `settled_at` (DateTime, null). Settlement becomes
one conditional update, `filter(pk=..., settled_at__isnull=True)
.update(status=..., settled_at=now)`, and `workflow_settled` plus the
workflow hook dispatch run only when that update matched. `resume()` clears
`settled_at` in the same transaction that moves the chain to RUNNING, so the
next settlement is a genuinely new one and fires again. `cancel()` uses the
same conditional update, so a cancel racing a completion produces exactly one
settlement. This replaces the `WorkflowHookDispatch` unique row as the first
line of defence; that row stays as the second. The final-step replay, the
already-terminal `_complete_chain`, the web-process cancel and the resume
are all covered by the one column.

**At most once, and what that excludes.** With the guards above, every
signal fires at most once per transition. What the guards do not cover is a
crash between commit and the `on_commit` callback. That window loses the
signal, and `on_commit` callbacks are not durable. This is the trade
principle 1 makes: the row already records the transition, and an
application that needs a durable per-transition effect has hooks, whose
`HookDispatch` row survives the crash and whose replay the reaper already
owns. The run gets its own durable hook for the same reason (section 4.6).

**Receiver placement.** All signals except `task_started` fire on the thread
that runs the hook handler, in the monitor process, or in the reaper thread
beside it. That thread is the one Qraft moved hooks off so the monitor would
not bottleneck. Receivers must be cheap: a row insert, a cache write, a log
line. A receiver that calls a provider or runs a report belongs in a hook.

### 4.4 Metrics sink, logging context, trace propagation

Exists so the numbers the dashboard computes on request also reach the
operator's own system, and so one slow attempt can be followed across
processes (section 1, fifth question), without a second collector
(principle 1) and without unbounded labels (principle 5).

**Module.** `qraft/metrics/__init__.py` defines a `Sink` protocol with
`counter(name, value=1, **labels)`, `histogram(name, value, **labels)` and
`gauge(name, value, **labels)`, a `NullSink`, and `get_sink()` that resolves
`QRAFT_CLUSTER["metrics_sink"]` (a dotted path, default
`qraft.metrics.NullSink`) once per process. `qraft/metrics/otel.py` provides
`OpenTelemetrySink`, which instruments through `opentelemetry-api` only,
installed by the extra `django-qraft[otel]`. Qraft creates instruments on a
meter named `qraft` and records to them; the host application configures the
SDK, the exporter, resources and process lifecycle. That is the boundary
OpenTelemetry draws between a library and an application, and it means a
host that has not configured an SDK gets a no-op meter, not an error.
`dashboard/metrics.py` is untouched: it answers a different question (what
happened in the last hour, from the rows) and keeps working with the sink
set to null.

OpenTelemetry first, Prometheus later. A Qraft cluster is several processes,
and Prometheus's pull model needs a multiprocess registry or a push gateway
to see them all; OTLP pushes from each process and the collector merges.

**Enqueue time.** `QraftTaskAttempt` gains `enqueued_at` (DateTime, null),
stamped in `_create_and_enqueue` for attempt 1 and in `_claim_and_enqueue`
for dispatched attempts. Pickup is measured from it. Today the nearest proxy
is `claimed_at` for scheduled attempts and `date_created` for the first,
which conflates an intentional backoff with queue wait; an explicit column
separates the four intervals an operator wants apart: intentional delay
(`not_before - date_created`), scheduler lateness (`claimed_at -
not_before`), queue wait (`date_started - enqueued_at`) and execution
(`date_completed - date_started`).

**Emission points and labels.**

| Metric | Type | Emitted from | Labels |
|---|---|---|---|
| `qraft.attempt.started` | counter | lease open, first time (worker) | `func`, `cluster` |
| `qraft.attempt.pickup` | histogram (s) | lease open (worker) | `func`, `cluster` |
| `qraft.attempt.finished` | counter | every resolution path | `func`, `cluster`, `outcome`, `exception_class` |
| `qraft.attempt.duration` | histogram (s) | every resolution path | `func`, `cluster`, `outcome` |
| `qraft.attempt.stall_suspected` | counter | reaper | `func`, `cluster` |
| `qraft.scheduler.lag` | histogram (s) | dispatcher claim | `cluster` |
| `qraft.retry.scheduled` | counter | `handle_task_retry` | `func`, `exception_class` |
| `qraft.reaper.action` | counter | reaper | `action` (`orphaned`, `stalled`, `reconciled`, `rearmed`, `replayed`) |
| `qraft.workflow.settled` | counter | settlement CAS | `workflow_type`, `outcome` |
| `qraft.run.settled` | counter | run settlement | `subject_type`, `kind`, `outcome` |
| `qraft.run.duration` | histogram (s) | run settlement | `subject_type`, `kind`, `outcome` |
| `qraft.run.report_to_ready` | histogram (s) | run settlement, success only | `subject_type`, `kind` |
| `qraft.queue.depth` | gauge | gauge owner | `cluster` |
| `qraft.queue.oldest_ready_age` | gauge (s) | gauge owner | `cluster` |
| `qraft.attempt.active` | gauge | gauge owner | `cluster` |
| `qraft.scheduler.overdue` | gauge | gauge owner | |
| `qraft.attempt.unrouted_age_max` | gauge (s) | gauge owner | |
| `qraft.run.open_age_max` | gauge (s) | gauge owner | `subject_type` |

Pickup is emitted at start, not at resolution as the first draft had it: an
attempt that is hung right now must already be in the pickup histogram, and
its absence from the duration histogram is itself the signal. Duration is
emitted on every resolution path, including `_reap_one` and stall
resolution, with `outcome` in `succeeded`, `failed`, `orphaned`, `stalled`,
so a fleet whose failures are detected by the reaper does not look faster
than one whose failures return. `run.duration` carries the outcome;
`report_to_ready` is the same reading restricted to success, kept as its own
name because it is the number the consumer's dashboard is built on.

**One gauge owner.** Every cluster runs a dispatcher and a reaper, and a
gauge emitted by all of them would let a consumer sum the same backlog once
per replica. Gauges are emitted only by clusters started with
`metrics_gauges=True` in their `QRAFT_CLUSTER` entry (default False), from
the dispatcher loop once per pass. The docs say to set it on exactly one
cluster.

`subject_id`, run id, task id, `revision` and `metadata` never appear as
labels. `subject_type` and `kind` are bounded by the application's own
vocabulary and are the finest grain a dashboard needs. An operator who wants
one worksheet's timeline has the subject filter, not a metric.

**Failure isolation.** A sink that raises is caught; the exception is logged
at most once per minute per process and counted in an in-process health
counter that `api/state/` exposes as `metrics_health`. The sink is never
disabled: a transient exporter failure must not silence a process until
restart, which is what the first draft would have done.

**Logging context.** `qraft.logging.QraftContextFilter` is a
`logging.Filter` that reads the contextvars set at `pre_execute` (task id,
attempt id, attempt number, run id, stage, subject) and attaches them to
every record as attributes, so a host's formatter can print or ship them.
Outside a task the attributes are present and empty. The docs show the two
lines of `LOGGING` configuration.

**Trace propagation.** When `opentelemetry-api` is importable and a span is
current at enqueue, `async_task` injects W3C `traceparent` into a new
`trace_context` column (`CharField(128)`, null) on the attempt row; the
dispatcher copies it onto retry attempts. At `pre_execute` the lease reads it
and starts a child span named after `func` for the attempt's duration, so a
trace begun in the web request that enqueued the ingest continues into the
worker and across retries. Hooks receive the same `traceparent` only through
the opt-in hook context (section 4.5), since a hook's `**kwargs` are the
caller's and cannot safely gain a key. Without the package, the column stays
null and nothing else changes.

### 4.5 Durable hook context

Exists because the motivating complaint about hooks is that they do not know
which execution they follow (section 1, second question), and the fix is
small.

`async_task(..., qraft_options={"hook_context": True})` makes success and
failure hooks receive one extra keyword argument, `context`, a plain dict:
`task_id`, `attempt_id`, `attempt_number`, `outcome`, `exception_class`,
`run_id`, `stage`, `subject_type`, `subject_id`, `result_ref` (the Django-Q2
task id whose `result` holds the return value), `traceparent`, and the
attempt's `date_started` and `date_completed`. Opt-in, so no existing hook
signature breaks. Workflow-level hooks gain the same flag on their
constructors and receive the workflow's id, type, outcome and counters. The
context is assembled in the hook handler from the rows it already holds and
frozen into the hook task's arguments, so it survives the same crashes the
`HookDispatch` row does.

### 4.6 Runs and stages

Exists so a pipeline whose stages enqueue each other has one row that spans
them (principle 3), one number for report-to-ready, and a durable
completion event (principle 1).

**Data model.** Two new models.

`QraftRun`:

| Field | Type | Purpose |
|---|---|---|
| `id` | UUID | primary key |
| `subject_type`, `subject_id` | from `SubjectMixin` | what the run is for |
| `kind` | `CharField(50)`, null | execution fingerprint: `shadow`, `parity`, `simulation`; a metric label |
| `revision` | `CharField(100)`, null | pipeline or input revision; never a label |
| `metadata` | JSON, null | application data; never a label |
| `previous_run` | FK self, null, `SET_NULL` | the run this one reruns |
| `status` | `OPEN`, `SUCCEEDED`, `FAILED`, `CANCELLED`, `ABANDONED` | indexed |
| `date_started` | DateTime | defaults to now; the app may backdate it |
| `settled_at` | DateTime, null | the settlement compare-and-swap column |
| `overdue_flagged_at` | DateTime, null | set once by the overdue sweep |
| `on_settled`, `on_settled_kwargs` | hook path and JSON | the durable completion hook |
| `summary` | JSON, null | snapshot written at settlement (below) |
| `date_created`, `date_updated` | DateTime | bookkeeping |

`QraftRunStage`, one row per declared stage, created by `runs.start`:

| Field | Type | Purpose |
|---|---|---|
| `run` | FK `QraftRun`, `CASCADE` | owner |
| `name` | `CharField(100)` | unique with `run` |
| `position` | small int | declaration order, for display only |
| `unit_type` | `task`, `chain`, `iter`, `batch`, null | what is bound |
| `unit_id` | UUID, null | the bound task or workflow |
| `status` | `PENDING`, `BOUND`, `SUCCEEDED`, `FAILED`, `CANCELLED`, `SKIPPED` | |
| `skip_reason` | text, null | |
| `bound_at`, `settled_at` | DateTime, null | |

`QraftTask` and the three workflow models gain `run` (FK `QraftRun`, null,
`SET_NULL`) and `stage` (`CharField(100)`, null) through a `RunMemberMixin`,
for correlation. `SET_NULL` rather than `CASCADE`: deleting a run must never
delete work.

**One stage, one completion unit.** The first draft let a stage count as
complete when any one of its tasks succeeded. That is wrong the moment a
stage has more than one task: a rules stage with three passes, one finished
and two running, would settle, and a later failure could not reopen it. The
rule now is that a stage is bound to exactly one unit, a `QraftTask` or one
workflow, and settles when that unit does. A stage that needs several tasks
uses an `Iter` or a `Batch`, whose membership closes at `run()` (`append`
raises afterwards, `qraft/iter.py`) and whose completion the dispatchers
already track. Members inherit `run` and `stage` so they filter and log
correctly, but a member's outcome never touches the stage row; only the
unit's does.

**API.**

```python
from qraft import runs

run_id = runs.start(
    subject=("worksheet", "4117"),
    stages=["ingest", "rules", "ai"],
    kind="shadow",
    revision="rules@v7",
    started_at=report.received_at,           # optional; defaults to now
    on_settled="revenue.hooks.run_settled",  # optional durable hook
)
async_task("revenue.tasks.ingest", worksheet_id,
           qraft_options={"run": run_id, "stage": "ingest"})

batch = QraftBatch(run=run_id, stage="rules")   # the batch is the unit
batch.append("revenue.tasks.revenue_pass", worksheet_id)
batch.append("revenue.tasks.entity_pass", worksheet_id)
batch.run()
```

A plain `async_task` with `run` and `stage` binds the task as the stage's
unit at enqueue. A workflow constructed with `run` and `stage` binds itself
at `run()`, the moment work is committed; its members are created with the
same `run` and `stage` but do not bind. Binding is a compare-and-swap on the
stage row, `filter(run=..., name=..., unit_id__isnull=True).update(...)`,
under `select_for_update()` on the run row so the terminal check and the
bind are one decision. `run` implies the subject: a task that names a run
inherits the run's subject unless it names a different one, which raises.

**Edge rules.** Each is enforced at the bind, where a mistake is cheap.

1. Enqueueing into a run whose status is terminal raises.
2. Binding a stage that already has a unit raises. A stage runs once per run.
3. Binding a stage the run did not declare raises.
4. `runs.skip(run_id, stage, reason)` marks an unbound stage `SKIPPED`;
   skipping a stage that has a unit raises.
5. `runs.cancel(run_id)` is the settlement compare-and-swap to `CANCELLED`.
   Further binds raise. Units already in flight finish and their outcomes are
   recorded on their stage rows, which do not change the run.
6. `runs.abandon(run_id, reason)` is the same transition to `ABANDONED`, for
   an `OPEN` run an operator has decided will never complete (a stage that
   was never enqueued). Exposed as a dashboard action and an admin action.
7. `QraftChain.resume()` on a chain bound to a run checks the run first: it
   proceeds only while the run is `OPEN`, and raises otherwise with the
   message to start a new run. Under the settlement rule below, a chain that
   is a stage's unit fails its run the moment it fails, so in practice a
   bound chain is resumable only if the run has not yet observed its failure
   (a crash between the two, which `replay_unrouted` closes). The rule is
   stated for completeness; the expected path is a new run.
8. Stage order is display order. Qraft does not enforce that `rules` waits
   for `ingest`; the application enqueues in the order it wants.
9. A settled run is never mutated. A rerun is a new run with `previous_run`
   pointing at the old one; the dashboard shows the link.

**Settlement.** When a unit settles, the code that settled it calls
`runs.note_unit_settled(unit)` inside the window the `routed` flag protects
(for a task unit, the hook handler after resolution when no retry was
scheduled; for a workflow unit, immediately after the workflow's own
settlement compare-and-swap in the dispatchers), so a crash between the two
is replayed by `replay_unrouted`. It locks the run row and, if the run is
still `OPEN`, moves the stage to the unit's outcome and applies two rules.
If the stage is `FAILED` or `CANCELLED`, the run becomes `FAILED`. Otherwise,
if every stage is `SUCCEEDED` or `SKIPPED`, the run becomes `SUCCEEDED`. Both
are the `settled_at` compare-and-swap, so a run settles exactly once. A run
whose remaining stages have no unit stays `OPEN`; that is correct and
visible (below), not something Qraft times out on its own.

Explicit close was considered as the only mechanism. It is simpler in Qraft
and worse for the consumer: the code that knows the pipeline is done is the
last stage's success path, which is the one place a crash loses the call.
Deriving from "no live members" was rejected because between stage N
committing and stage N+1's row existing the run has zero live members.
Declared stages with bound units close both holes: the rules stage is
expected whether or not its task exists yet, and it is done only when its
one unit is.

**Summary snapshot.** In the settlement transaction the run's `summary` is
written: per stage the unit type and id, outcome, `bound_at`, `settled_at`
and duration; the run's duration; the aggregated usage over all member
attempts; and the cost summary (section 4.8) when a resolver is configured.
Retention may later remove the member rows (below); the run keeps the
answer.

**Durable completion hook.** `on_settled` is dispatched through
`dispatch_hook_once` with `WorkflowHookDispatch` rows keyed
`("run", run_id, "settled")`, the same idempotency the workflow hooks use,
and receives one `context` dict: `run_id`, `subject_type`, `subject_id`,
`kind`, `revision`, `metadata`, `outcome`, `stages` (name to outcome),
`started_at`, `settled_at`, `duration_s`, `previous_run_id`. Because a run
settles once and is never reopened, `(run_id, "settled")` is a stable event
identity the application can dedupe on. `run_settled` the signal fires
beside it for observers; the hook is what the consumer's "worksheet ready"
transition hangs on.

**Overdue.** `QRAFT_RUN_OVERDUE_AFTER` (seconds, default None) turns on a
sweep in the reaper thread: an `OPEN` run whose `date_started` is older than
the threshold and whose `overdue_flagged_at` is null gets the flag set (a
compare-and-swap, so once), `run_overdue` is sent, and `qraft.run.settled` is
not touched. The dashboard shows a badge; `qraft.run.open_age_max` reports
the oldest open run. Nothing fails automatically: an unenqueued stage is an
application defect, and the operator's tools are `skip`, `cancel` and
`abandon`.

**Report-to-ready.** `settled_at - date_started`, emitted as `run.duration`
with the outcome and as `report_to_ready` on success. The app backdates
`date_started` to the moment the report arrived, so the number includes the
time the first task spent queued.

**Why not nested workflows.** A chain of chains would model this pipeline
too, and it is the roadmap's listed gap. It is also the heavier fit. A chain
owns its steps' definitions up front (`QraftChainStep` rows exist before
execution) and enqueues each step itself from `ChainDispatcher`. The
motivating pipeline decides what to enqueue next inside application code:
whether to score at all, which worksheets a promotion re-runs, whether a
version change needs a rerun. Forcing those decisions into a static step
list means a chain per branch or approval steps standing in for
conditionals. A run declares the names of the stages and lets the app bind
what it likes under them. Nested workflows stay on the roadmap for pipelines
that are static; a run whose stage is a chain already works under this
design.

**Dashboard and admin.** `api/state/` gains `runs`: the newest twelve, each
with subject, kind, status, elapsed, an overdue badge, `previous_run`, and
one chip per stage coloured by the stage row's status. Filterable by the
same query parameters as tasks. Actions: `abandon`, `cancel`. `QraftRunAdmin`
is read-only with inlines for stages and member tasks.

**Retention.** Three changes to `qraft/retention.py`. The task pass excludes
tasks whose `run` is not terminal, as it excludes members of live workflows.
The workflow pass excludes workflows whose `run` is not terminal, because
deleting an iter or batch cascades to its member tasks (`QraftTask.qraft_iter`
and `qraft_batch` are `CASCADE`), and the first draft protected membership
only in the task pass; a completed batch under an open run would have lost
its rows through the workflow pass. (Deleting a chain sets its tasks'
`chain_step` link null rather than deleting them, but the same exclusion
applies for consistency.) Terminal runs older than the cutoff are pruned
after the task pass, and `QraftRunStage` rows cascade with them; the
`summary` written at settlement is what makes that safe.

### 4.7 Stall observation

Exists so a task that is alive but not moving is distinguishable from one
that is working (section 1, fourth question), while the decision to act
stays with the application (principle 2).

**Data model.** `QraftTask` gains `stall_after` (`PositiveIntegerField`,
seconds, null). `QraftTaskAttempt` gains `stall_suspected_at` (DateTime,
null). `stall_after` is a column rather than a `retry_policy` key:
`RetryPolicy.from_options` (`qraft/retry.py`) copies a whitelist of keys and
would drop an unknown one silently, and stalling is not retry semantics.

**Detection.** The reaper gains `flag_stalls()`, run from `reap_orphans`
after the orphan sweep. It selects unresolved attempts whose task has
`stall_after` set, whose `heartbeat_at` is inside the heartbeat grace (the
worker is alive), whose `stall_suspected_at` is null, and whose
`coalesce(progress_advanced_at, date_started)` is older than `now -
stall_after`. Each gets `stall_suspected_at` set by compare-and-swap,
`attempt_stall_suspected` is sent, `qraft.attempt.stall_suspected` is
counted, and the dashboard marks the row. A task that later advances keeps
the flag as history; the row shows "recovered" when `progress_advanced_at`
is newer than `stall_suspected_at`.

**No automatic retry.** The first draft resolved a stalled attempt and
scheduled a retry. The review showed why that is unsafe as a default: the
original attempt keeps running, and while the hook handler's
compare-and-swap drops its *reported result*, nothing drops its database
writes or its provider charges. A retry that starts while attempt 1 is still
writing suggestions produces two attempts writing the same rows. An
automatic resolve therefore needs one of three things Qraft cannot supply in
1.4.0: a stopped execution, cooperative cancellation, or an application
contract that tolerates overlapping attempts. Stall observation ships
without it. A `stall_action="resolve"` mode is deferred until a cancellation
design exists (section 6).

**The ownership pattern.** An application that wants to act on a suspected
stall itself, or that wants its writes safe against any overlapping attempt,
uses `qraft.context.current_attempt_id()` and puts the attempt id in the same
statement as the write: `UPDATE ... SET ... WHERE id = %s AND owner_attempt_id
= %s`, or an insert whose unique key includes the attempt id. A standalone
"am I still current?" check followed by a write is not enough, because the
answer can change between the two. `docs/ai-workloads.md` carries this
pattern with the revenue-style example of persisting per-chunk results.

**Two thresholds, two conditions.** The first draft called a `stall_after`
below the heartbeat grace pointless. It is not: a task can heartbeat every
thirty seconds while advancing nothing for sixty, and a sixty-second
`stall_after` beside a ninety-second orphan grace measures a different
condition. They coexist; the orphan sweep runs first so an attempt is never
both, and no warning is emitted for small values.

### 4.8 Cost from usage

Exists so tokens become money in one place when the operator wants that
(section 1, sixth question), without making pricing a prerequisite for
adopting the library (principle 2). Optional, off by default, last in
delivery.

**Resolver.** `qraft.pricing.PricingResolver` is a protocol with one method,
`price(model, provider=None) -> Price | None`, where `Price` carries
`input`, `output`, `cached_input` per million tokens, `currency` and
`revision`. `qraft.pricing.StaticTablePricing` implements it from
`QRAFT_PRICING`:

```python
QRAFT_PRICING = {
    "resolver": "qraft.pricing.StaticTablePricing",   # default when the dict exists
    "currency": "USD",
    "revision": "2026-09",
    "models": {
        "gpt-5.4-nano": {"input": 0.05, "output": 0.40, "cached_input": 0.005},
    },
}
```

No `QRAFT_PRICING` means no resolver, and every cost path answers "unknown".

**Per-increment entries.** `record_usage(model=..., input_tokens=...,
output_tokens=..., cached_input_tokens=..., provider=..., cost=...)` keeps
summing the numeric totals as today and additionally appends one entry to
`usage["entries"]`: `model`, `provider`, the three token counts,
`estimated_cost` (as a decimal string), `currency`, `pricing_revision`, and
`cost_source` (`caller` when the caller passed `cost`, `resolver` when the
table priced it, `none`). Entries are what make an attempt that calls two
models priceable: the current merge keeps the latest model name while
summing all tokens, so pricing the totals would bill one model's tokens at
another's rate. Caller-supplied `cost` always wins for its entry.

**Cached tokens are a subset of input tokens.** That is how OpenAI reports
`cached_tokens` inside `input_tokens_details`, and the formula prices
accordingly: `(input - cached) × input_rate + cached × cached_rate + output ×
output_rate`. A provider that reports cached tokens separately from input
sets `cached_is_subset=False` on its `Price`, and the resolver documents
which convention it follows.

**Decimal.** `_is_numeric` in `qraft/context.py` accepts `int` and `float`
only; it gains `Decimal`, and cost fields are stored as strings by
`DjangoJSONEncoder` and parsed back with `Decimal()` in aggregation, so
money is never summed as a float.

**Reading cost.** `qraft.pricing.cost(usage) -> CostSummary`, a dataclass
with `amount: Decimal`, `currency`, `coverage` in `complete`, `partial`,
`none` (whether every entry with tokens has a cost), and `estimated: bool`
(true unless every costed entry came from the caller). Entries recorded
before a resolver existed, or under a model the table does not know, are
priced at read time when the current table can, and marked `estimated`.
Aggregates (`aggregate_usage`, `aggregate_workflow_usage`, the new
`aggregate_run_usage` and `aggregate_subject_usage`) return the same
dataclass beside the token totals, so a dashboard can show "$1.42, partial,
estimated" rather than a bare number. A configured price is an estimate of
what the provider will bill, never proof of it; the field names say so.

### 4.9 Migrations

One migration per delivery step, all operations additive and nullable, no
backfill. A rolling deploy in which the previous release still inserts rows
keeps working (the same reasoning as the `db_default` on
`QraftTaskAttempt.state`).

- `0011_subjects_progress_metrics`: `QraftTask.subject_type`, `subject_id`,
  index `qraft_task_subject_idx`; `QraftTaskAttempt.progress`,
  `progress_reported_at`, `progress_advanced_at`, `enqueued_at`,
  `trace_context`; `settled_at` on the three workflow models; `subject_type`,
  `subject_id` on the three workflow models.
- `0012_runs`: `CreateModel QraftRun` and `QraftRunStage` with their indexes;
  `run`, `stage` on `QraftTask` and the three workflow models.
- `0013_stall_observation`: `QraftTask.stall_after`,
  `QraftTaskAttempt.stall_suspected_at`.
- Pricing adds no schema; entries live in the existing `usage` JSON.

Pre-existing rows have no subject, run, stage or progress timestamps; they
appear in unfiltered views as before and in no filtered view. `settled_at` is
null on workflows that settled before the upgrade; the settlement
compare-and-swap treats a terminal `status` with null `settled_at` as already
settled, so a replay against an old row sends nothing.

## 5. Decisions

| ID | Decision |
|---|---|
| D1 | Subject is a `(subject_type, subject_id)` string pair with a composite index, not JSON tags |
| D2 | Subject, run, stage, stall_after and hook_context are `qraft_options` keys on `async_task`, constructor kwargs on workflows |
| D3 | Progress is attempt-owned with `reported_at` and `advanced_at`; the task holds a snapshot stamped with `attempt_id`, written only by the latest attempt |
| D4 | Progress and usage merges are atomic (`jsonb \|\|` on Postgres, locked RMW elsewhere); coalescing is a setting defaulting to off |
| D5 | Signals use `send_robust` and carry immutable id payloads; they are best-effort observers |
| D6 | Signals send via `on_commit`, at most once, guarded by named compare-and-swaps; workflows gain `settled_at` as transition identity, cleared by `resume()` |
| D7 | `task_started` and pickup fire only when the conditional first-start update matched |
| D8 | `enqueued_at` is recorded; pickup is emitted at start; duration is emitted on every resolution path with an outcome label |
| D9 | Gauges are emitted by the one cluster flagged `metrics_gauges=True` |
| D10 | Qraft instruments via `opentelemetry-api`; the host configures the SDK; sink errors are logged and counted, never disable the sink |
| D11 | No subject id, run id, task id, revision or metadata ever becomes a metric label |
| D12 | Trace context is stored on the attempt row; hooks receive it only via `hook_context` |
| D13 | A stage is bound to exactly one completion unit; members inherit correlation and never settle a stage |
| D14 | Runs settle by derivation over declared stages with bound units; `skip`, `cancel`, `abandon` are the explicit transitions |
| D15 | A settled run is immutable; reruns are new runs linked by `previous_run` |
| D16 | `run` on members is `SET_NULL`; run membership is protected in every retention pass; a summary snapshot survives member pruning |
| D17 | `on_settled` is the durable run completion event, keyed `(run_id, "settled")` |
| D18 | Overdue runs are flagged, signalled and gauged, never failed automatically |
| D19 | Nested workflows stay on the roadmap; a run is the primitive for stages that enqueue each other |
| D20 | Stall detection observes only; automatic resolve is deferred behind a cancellation design |
| D21 | `stall_after` is a column; small values coexist with the heartbeat grace without warning |
| D22 | Cost is optional, resolver-based, recorded per increment with source and revision; caller cost wins; cached tokens are a subset of input by default |
| D23 | `cost()` returns a summary with coverage and estimated flags, never a bare number |
| D24 | One additive migration per delivery step, no backfill |

## 6. Scope edges

Out of scope: a per-task cancel API and cooperative cancellation of a running
attempt (the dashboard note that none exists still holds, and this is the
gate on `stall_action="resolve"`); a Prometheus sink; JSON tags on tasks;
run timeouts that fail a run automatically; admission control (a shared
concurrency cap beside the token bucket), which the review raised and which
is a real gap but a separate design; and changing how hooks receive
arguments today, though the review confirmed that `docs/hooks.md` shows
success hooks receiving `result` while `dispatch_success_hook` passes only
`success_args` and `success_kwargs`, which is documentation drift to fix
separately.

Open, each with a recommendation:

1. Should a run with a skipped stage settle `SUCCEEDED`, or a distinct
   `PARTIAL` status? Recommend `SUCCEEDED` with the skip recorded on the
   stage row and in `summary`; a skip is a decision the app made, not a
   failure Qraft observed.
2. Should a stage's unit failing fail the run immediately, or should the run
   wait for every bound unit to settle before deciding? Recommend
   immediately; the consumer's readiness depends on every stage, and waiting
   only delays the same answer while hiding it from the dashboard.
3. Should `hook_context` become the default in 2.0? Recommend yes, with the
   opt-in as the 1.x bridge.
4. Should the Postgres CI job gate merges or run as advisory? Recommend
   gating; the concurrency guarantees are the point of the release.
5. Should `progress_min_interval` be settable per task as well as per
   cluster? Recommend cluster-only until a task asks for it.
