# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Approval gates on graph nodes.** `requires_approval=True` parks a node at
  `WAITING_APPROVAL` once its dependencies are met, and `graphs.approve()` /
  `graphs.reject()` release or refuse it. The gate stops its own node, never its siblings,
  and a graph parked at one is neither settled nor swept as overdue. Migration `0018`
- **Completion receipts for graph nodes.** `qraft.graphs.publish()` is a transaction in
  which the application's writes and the node's receipt commit together, so a crash
  between the two is impossible and a resume's kept set is a fact rather than a guess. The
  block locks the node and refuses an attempt it is no longer bound to, so a straggler from
  before a resume cannot publish behind it, and a node publishes at most once per
  generation. The reaper resolves an unresolved attempt carrying a commit stamp as
  succeeded rather than reaping it, because the receipt outranks a lost Django-Q2 result.
  New `QraftGraphNode.receipt`, `QraftGraphNode.receipt_attempt_id` and
  `QraftTaskAttempt.output_committed_at`; migration `0017`
- **Selective resume for graphs.** `graphs.resume(graph_id, nodes=None)` re-runs a set of
  nodes and everything downstream that ever ran, keeping every other node's result, and
  `graphs.preview_resume()` answers what it would do without writing. Rerun nodes advance
  a generation and drop their task link, so a completion from before the resume settles
  nothing; the graph's durable-hook dispatch rows are cleared so the next settlement fires
  `on_settled` again. Refused while the graph is running -- the reason settlement waits for
  quiescence -- and on a cancelled graph. A succeeded graph must name its nodes explicitly.
  The dashboard carries the preview on the resume button
- **Execution graphs.** A graph is one execution of a plan over a subject:
  `graphs.Graph(...)` declares nodes with explicit `after` edges, `start()` writes the
  whole plan in one transaction, and Qraft dispatches every node whose dependencies are
  met through a scheduled attempt. The graph settles once, on quiescence, under a
  `settled_at` compare-and-swap, with a durable `on_settled` hook, a `summary` snapshot
  and `previous_graph` lineage. Per-node cluster, retry policy, `stall_after` and
  success/failure hooks; `qraft.context.current_node()` for correlation from inside a
  node; `graphs.snapshot()` as the visibility contract. `graphs.skip()` and
  `graphs.cancel()` are the explicit transitions, and every topology rule — an unknown
  dependency, a self-edge, a cycle, a duplicate key — raises at `start()`, while
  correlating work onto a terminal graph raises at the enqueue. Members inherit
  `graph` and `node` for correlation and never settle a node.
  New `graph_settled`, `node_settled` and `graph_overdue` signals, `qraft.graph.*` /
  `qraft.node.*` metrics, an overdue sweep behind `QRAFT_GRAPH_OVERDUE_AFTER`, a
  dashboard graph panel, and a read-only `QraftGraphAdmin`. Migrations `0016`-`0019`

  A graph may `start()` without a subject and name it later with
  `graphs.bind_subject()`, which updates the graph and every member already bound to it
  under the graph's row lock — for a pipeline whose first node creates the domain object
  the graph is about. `graphs.Graph(..., budgets={"openai_requests": 40})` declares a
  provider allowance that `qraft.context.consume_budget()` spends atomically across every
  node and every retry, which is the question a refilling token bucket cannot answer

- **A finished task stops heartbeating, and says when it finished.**
  `qraft.runner.run_task` stamps the new `QraftTaskAttempt.returned_at` and stops the
  lease heartbeat as soon as the target returns or raises, rather than at the monitor's
  `post_execute`. A killed monitor holds the result queue's write lock, so the worker
  blocks in `result_queue.put()` with its task already done while the heartbeat keeps
  refreshing the lease — the reaper then reads the attempt as alive and never resolves
  it. Such an attempt is now reaped as `ResultLost` rather than `OrphanedTask`: the work
  ran and a retry re-does it, which a retry policy may treat differently. New migration
  `0015`
- **Redelivery guard: an attempt executes at most once.** Django-Q2 redelivers a message
  it never got an acknowledgement for, so a monitor crash re-ran a task whose attempt row
  was still unresolved, and each re-run refreshed the heartbeat so the reaper read it as
  alive. Every attempt Qraft enqueues now runs through `qraft.runner.run_task`, which
  claims the delivery by compare-and-swap on the new `QraftTaskAttempt.execution_count`
  before calling anything. A refused delivery neither calls the function nor refreshes the
  lease; it counts `qraft.attempt.redelivered` and raises
  `qraft.runner.RedeliveredAttempt`, so the retry policy decides attempt N+1. New setting
  `max_executions_per_attempt` (default 1). Attempt signal payloads gain `execution_count`
  and `redelivered`
- **`min_heartbeat_grace` setting** (default 90.0). The reaper's grace period is
  `max(3 * heartbeat_interval, min_heartbeat_grace)`, and the floor was a module constant,
  so `heartbeat_interval` read like a detection-latency knob and was not one below 30s. For
  sub-second stages, 90s of lost work per crash can cost more than a rare false reap; the
  floor is now lowerable per cluster
- **Per-cluster brokers.** `qraft.brokers.broker_for_cluster(cluster, priority)` builds the
  broker a *named* cluster runs, from that cluster's own `Q_CLUSTER` entry merged over the
  base, and every Qraft enqueue now passes it as django_q's `broker=` kwarg — `async_task`,
  the scheduler's dispatch of SCHEDULED attempts (and so DLQ requeues), workflow members and
  chain steps, task hooks, workflow hooks, the progress hook and the django.tasks backend.
  Django-Q2 resolves an omitted broker against the *enqueuing* process's config, so before
  this a web process on Redis handed every task to Redis whatever the target cluster ran.
  A project may now run `revenue` on the ORM broker while `default` and `reports` stay on
  Redis. `async_task(broker=...)` now requires `cluster=` beside it, since the attempt row
  records the cluster and every retry and DLQ requeue inherits it — without a name the row
  would claim the enqueuing process's cluster rather than the one the broker feeds.
  `qraft.brokers.RoutingBroker`, declared as the base `broker_class`, extends the
  same routing to enqueues Qraft does not make itself (`django_q.tasks.async_task(...,
  cluster="revenue")` from application code). The ORM broker's database alias is pinned onto
  the instance, since `ORM.get_connection()` re-reads `Conf.ORM` on every call. Brokers are
  cached per process and cluster; `reset_broker_cache()` clears them and a `Q_CLUSTER`
  change under `override_settings` does it automatically
- **Cost from usage** (`qraft.pricing`, optional, schemaless). `QRAFT_PRICING` names a
  resolver — `StaticTablePricing` reads a per-million-token table out of the setting — and
  without the setting there is no resolver and every cost path answers "unknown". Each
  `record_usage()` call that names a `model` now appends one entry to `usage["entries"]`
  with the model, provider, token counts, `estimated_cost`, `currency`,
  `pricing_revision` and `cost_source`. Entries exist because the totals cannot be priced:
  the merge keeps the latest model name while summing all tokens, so pricing the totals
  would bill one model's tokens at another's rate. Caller-supplied `cost` always wins for
  its entry, and pricing an increment as it is written means a corrected table never
  re-prices a call the provider already billed. Cached tokens are a subset of input tokens
  by default (the OpenAI convention), with `cached_is_subset: False` for providers that
  report them separately. `cost(usage)` returns a `CostSummary(amount, currency, coverage,
  estimated)` rather than a bare number — a configured price is an estimate of what the
  provider will bill, never proof of it — and every aggregate carries the same summary
  under `cost_summary` beside the token totals. Money is summed as `Decimal` throughout
- **Stall observation.** `qraft_options={"stall_after": 300}` puts a second question to
  the reaper beside "is the worker alive": every sweep, an unresolved attempt whose
  heartbeat is fresh but whose `progress_advanced_at` (falling back to `date_started`) is
  older than `stall_after` gets `stall_suspected_at` set by compare-and-swap, sends
  `attempt_stall_suspected`, counts `qraft.attempt.stall_suspected`, and is marked on the
  dashboard. It reads `advanced_at`, not `reported_at`, so a task narrating its own hang
  cannot defeat it. Nothing is resolved and nothing is retried: the attempt keeps running,
  and while the hook handler would drop its reported result, nothing drops its database
  writes or its provider charges. `qraft.context.current_attempt_id()` is the supported
  way to act on the flag — put the attempt id in the same statement as the write
- **Subjects.** `async_task(..., qraft_options={"subject": ("worksheet", 4117)})` records
  the domain entity a task is for as an indexed `(subject_type, subject_id)` string pair;
  workflow constructors take `subject=` and copy it onto every member. Retries inherit it.
  `QraftTask.objects.for_subject(type, id)` is the one query an application detail view
  needs, and the dashboard filter box and admin filters read the same pair
- **Attempt-owned progress.** `report_progress()` now writes to `QraftTaskAttempt`, with
  `progress_reported_at` moving on every call and `progress_advanced_at` only when
  `current`/`total` change — so a task narrating its own hang is distinguishable from one
  that is moving. `QraftTask.progress` stays as the dashboard's snapshot, stamped with the
  writing `attempt_id` and updated only by the task's latest attempt, so a superseded
  attempt can no longer overwrite its successor's numbers. Progress and usage merges are
  atomic (one `jsonb ||` statement on Postgres, a locked read-modify-write elsewhere),
  and `progress_min_interval` coalesces writes from chunk-heavy tasks. New
  `areport_progress()` and `arecord_usage()` for coroutine tasks
- **Signals** (`qraft/signals.py`): `task_started`, `attempt_finished`, `task_settled`,
  `workflow_settled`, `attempt_stall_suspected`, `node_settled`, `graph_settled`
  and `graph_overdue`. Every
  send is `send_robust()` from `transaction.on_commit` inside the transaction that
  performs the transition, and every payload is an immutable mapping of ids, outcomes and
  ISO timestamps rather than a model instance. Signals are best-effort observers; anything
  the application must not miss still goes through a hook
- **Metrics sink** (`qraft/metrics/`): counters, histograms and gauges at Qraft's own
  transition points, through a `Sink` protocol resolved from `QRAFT_CLUSTER["metrics_sink"]`
  (default `NullSink`). `qraft.metrics.otel.OpenTelemetrySink` records through
  `opentelemetry-api` only, installed by the new `django-qraft[otel]` extra; the host owns
  the SDK. Backlog gauges are emitted by the one cluster flagged `metrics_gauges=True`, so
  a consumer cannot sum the same backlog once per replica. No subject id, graph id, task id,
  revision or metadata is ever a label. A sink that raises is logged once a minute and
  counted in `api/state/`'s `metrics_health`, never disabled
- **Enqueue time on the attempt** (`QraftTaskAttempt.enqueued_at`), so queue wait is
  measured from the moment the broker received the attempt rather than from row creation,
  which conflated an intentional backoff with a queue backlog
- **Log context and trace propagation.** `qraft.logging.QraftContextFilter` stamps every
  log record with the executing attempt's ids; with `opentelemetry-api` importable,
  `async_task()` injects a W3C `traceparent` onto the attempt row and the lease starts a
  child span for the attempt, so a trace begun in the web request continues into the
  worker and across retries
- **Durable hook context.** `qraft_options={"hook_context": True}` (and `hook_context=` on
  workflow constructors) gives hooks one extra `context` keyword argument naming the
  attempt that produced them: ids, outcome, exception class, subject, `result_ref`,
  `traceparent` and timestamps. Opt-in, so no existing hook signature breaks
- **Coroutine tasks.** An `async def` target now runs everywhere Qraft executes a
  callable: `qraft.tasks.async_task()`, workflow members, task and workflow hooks, and
  the `django.tasks` backend (`supports_async_task` is now true). The enqueue reroutes
  coroutine targets through `qraft.runner.run_task`, which runs them to completion with
  `asyncio.run()` in the worker slot they already occupy — one coroutine per slot, so
  lease, timeout, and retry semantics are unchanged. Previously a coroutine target was
  called but never awaited. Django's async rules apply inside the coroutine: use the
  async ORM or `sync_to_async`, since the sync ORM raises `SynchronousOnlyOperation`
  under a running event loop
- **Postgres concurrency tests** (`tests/test_postgres_concurrency.py`, marker
  `postgres`) and a Postgres CI job. Every compare-and-swap in Qraft is a no-op under
  SQLite, which ignores `select_for_update()`, so the settlement, delivery-claim,
  progress-attribution, budget and subject-bind races are only really tested here
- **A security model** in `docs/architecture.md`: the queue carries signed pickles, so
  `SECRET_KEY` is a code-execution credential, not just a session secret
- **End-to-end tests** (`tests/test_e2e.py`): the enqueue seam is no longer mocked
  everywhere. These drive the real path — `async_task()` writes an `OrmQ` row, Django-Q2's
  own `pusher`/`worker`/`monitor` loops run in-process, and the saved `Task` row fires
  `qraft_hook_handler` through its normal `post_save` receiver — across success, retry,
  exhaustion, DLQ requeue, chain ordering, iter counting, and `threaded_worker`

### Fixed
- **A graph waiting on a person could be pruned, and could not be cancelled.** Retention
  protected only `RUNNING` graphs, so a graph parked at `WAITING_APPROVAL` — live, not
  settled — was swept with its members, and `_settle` accepted only `RUNNING`, so
  `graphs.cancel()` refused a parked graph and a sibling's failure could not settle one
  either. Both now treat a parked graph as live. A graph whose roots are all gated also
  parks at `start()` rather than sitting `RUNNING`, which is what kept the overdue sweep
  flagging it for waiting
- **The dashboard's graph buttons posted `graphs/undefined/`.** `wire()` read four named
  dataset keys, so cancel and skip sent no id; the view tests call the endpoints directly,
  which is why nothing caught it. It now reads the button's own attribute, and the graph
  panel gained the resume button and revision it already had the data for
- **A reaped task never fired its failure hook.** `reaper._reap_one` called
  `route_workflow_completion()` and discarded the answer, so a task with a `failure_hook`
  and no workflow — a task whose worker was OOM-killed, or whose result the monitor lost —
  settled FAILED or EXHAUSTED with the hook never dispatched. The reaper then set
  `routed=True`, which took the attempt out of `replay_unrouted()`'s query, so nothing
  recovered it. It now falls back to `HookDispatcher.dispatch()` the way the replay path
  always did
- **Receiver order decided whether any task could run.** `context._on_pre_execute` cleared
  the memoised delivery claim, so connecting it after `lease._on_pre_execute_lease` would
  have made `runner.guard_delivery` re-claim an attempt that had already spent its
  execution budget and refuse every task as `RedeliveredAttempt`. The claim is keyed by q2
  task id and dropped by `clear_context()`, so the receiver no longer clears it
- **A stale completion could settle a resumed chain.** Routing was handed a task with its
  chain step already joined on (the hook handler's `select_related`, the reaper's replay
  batch), and `resume()` rebinds that step to a new task in between, so a completion from
  before the resume settled the generation after it FAILED. The step's bound task is now
  re-read as the chain's generation marker and a superseded completion is dropped
- **A chain that failed, resumed and failed again dispatched no second hook.** The
  `WorkflowHookDispatch` row is keyed on `(workflow, hook_type)` and knows nothing of
  generations, so the first failure's row suppressed the second's hook. `resume()` now
  clears the chain's dispatch rows with `settled_at`
- **A rolled-back transition still emitted its counter.** Counters at the settlement,
  resolution, retry and reap points fired inside `transaction.atomic()`, so a savepoint
  that rolled back left a count with no row change behind it. They now ride
  `transaction.on_commit`, as the signals already did
- **`QraftTask.progress` could be attributed to a superseded attempt.** The snapshot's
  "no newer attempt" guard and the next attempt's insert were not serialised. Both now
  take the task's row lock, so a report racing its own retry finds the newer attempt and
  is refused
- **`report_progress()` raised on a UUID against Postgres.** The Postgres merge builds its
  payload with raw SQL and used a plain `json.dumps`, so a call that worked on SQLite
  raised `TypeError` there. Both paths now use the encoder the JSON columns declare
- **A worker thread carried the finished attempt's context into the next task.** The
  logging context vars and the OpenTelemetry span attached on the worker thread were never
  cleared — `end_span()` runs on the heartbeat thread, which cannot reach the worker
  thread's token — so a pool thread logged the previous attempt's ids and an untraced
  attempt enqueued its work inside a finished span. Both are cleared in the worker's
  `finally`
- **`metrics_gauges` was inherited by every `ALT_CLUSTERS` entry.** Setting it on the base
  entry made every alt cluster a second reporter of the same fleet-wide backlog. An alt
  cluster now gets the flag only by declaring it, as the documentation already said
- **Queue gauges counted leased messages as ready.** `qraft.queue.depth` and
  `qraft.queue.oldest_ready_age` read every `OrmQ` row, including the ones a worker had
  already leased, so depth repeated the in-flight work the active gauge reports and the
  oldest reading was dragged below the true age. Both now read the ready set, the same one
  django_q's own `queue_size()` counts. A cluster that owns the gauges but cannot read its
  queue (a non-ORM broker) now says so once at startup instead of publishing nothing
- **A whole dispatch batch shared one enqueue timestamp.** `enqueued_at` came from the
  pass's due cutoff, so every attempt after the first in a slow batch was backdated and
  its measured queue wait inflated. Each attempt is stamped when it is claimed. An attempt
  with no cluster of its own also now records the cluster that actually took it, so its
  start and pickup metrics carry a cluster label and the active gauge counts it
- **A cancelled fan-out's hook lost the counters.** An opted-in `on_cancelled` hook
  received only the workflow id, type and outcome, while a completion's hook received the
  counts. How much had finished when the cancel landed is what the hook is for, so cancel
  and completion now build the same context
- **Retention could delete a live graph's evidence through the workflow pass.** Membership
  of a graph that has not settled was protected only in the task pass, but deleting an iter
  or batch cascades to its member tasks, so a completed batch under a running graph lost its
  rows. Both passes now exclude running graphs, terminal graphs are pruned after their members
  with their nodes cascading, and the `summary` written at settlement is what survives
- **Workflow settlement could fire hooks on an already terminal workflow.** A chain's
  final step never advances `current_step_index`, so a replayed completion passed the
  index check and re-entered the success path, and `_complete_chain` had no
  already-terminal check at all. Each workflow model now carries `settled_at` and
  settlement is one conditional update on it: the final-step replay, the already-terminal
  completion, a `cancel()` racing a completion and a replay against a row from before this release all
  match zero rows. `resume()` clears the column, so a resumed chain settles again as it
  should
- **A redelivered message re-stamped the attempt start.** `stamp_start()` updated by
  `q2_task_id` unconditionally; it is now conditional on `date_started` being null, so
  `task_started` and the pickup histogram fire once per attempt while a second delivery
  still refreshes the heartbeat
- Documentation drift: `docs/retry.md` described retries as Django-Q2 `Schedule` rows,
  which owned scheduling replaced in 1.3.0; coverage figures disagreed between README and
  `docs/development.md`; README claimed an unsourced "8-10x" throughput number that its own
  table contradicted; `docs/roadmap.md` motivated the absorption plan with upstream
  stagnation rather than with the capability gaps it actually targets

### Changed
- **`usage["cost"]` is stored as a decimal string, not a float.** Previously only a
  `Decimal` took the exact path and a caller passing floats kept float arithmetic, so two
  calls recording `0.1` and `0.2` totalled `0.30000000000000004`. Money now takes the
  `Decimal` path whatever the caller passes. Read it with `Decimal(usage["cost"])`, or use
  `qraft.pricing.cost()`, which returns a `Decimal` either way

### Planned
- Chain and Batch as facades over the execution graph

## [1.3.0] - 2026-08-18

Phase 2 of the Django-Q2 absorption plan
([q2-absorption.md](docs/future/q2-absorption.md)): Qraft owns scheduling.

### Added
- **Qraft-owned scheduling** (`qraft/scheduler.py`): a delayed attempt is a `SCHEDULED`
  `QraftTaskAttempt` row — created up front with its final attempt number, target
  cluster, and `not_before` due time — not a Django-Q2 `Schedule`. A dispatcher loop
  (daemon thread beside the monitor) claims due rows by compare-and-swap
  (`SELECT ... FOR UPDATE SKIP LOCKED` where the database offers it) and hands them to
  the broker, stamping the q2 task id at enqueue. Delay is exact instead of quantized
  to Django-Q2's 30-second scheduler cycle, and priority lanes survive the delay. All
  three delay paths use it: retries (`RetryPolicy.schedule_retry()`), DLQ requeues
  (`dlq.requeue()`), and deferred `django.tasks` (`run_after`). The marker fallback in
  the hook handler stays for one release as the bridge for pre-1.3 schedules still in
  flight. New settings `dispatch_interval`, `dispatch_batch`
- **Bundled monitoring dashboard** (`qraft.dashboard`): staff-only Django app with
  task and workflow views, approve/reject/cancel/requeue actions, and JSON state and
  latency-percentile metrics endpoints. Add `qraft.dashboard` to `INSTALLED_APPS` and
  mount `qraft.dashboard.urls` — see [docs/dashboard.md](docs/dashboard.md)
- **Retention sweep** (`qraft/retention.py`): bounded, batched pruning of settled
  `QraftTask`/`QraftTaskAttempt`/`HookDispatch`/workflow rows by age
  (`retention_days`), count (`retention_max_tasks`), or the stricter of the two. Live
  rows never go; a workflow is pruned as a unit. An explicit `Q_CLUSTER["save_limit"]`
  is inherited as the count bound unless overridden
  (`retention_inherited_from_save_limit`). Settings `retention_interval`,
  `retention_batch_size`
- **Worker identity on the attempt row**: the lease stamps worker pid and thread name
  at `pre_execute`, so a stuck or dead attempt names its worker
- **`cluster` is a real `async_task()` parameter**: routes the task to a named
  `ALT_CLUSTERS` pool, replacing the `q_options`-only path. Workflow members validate
  their cluster at `append()`. Retries and DLQ requeues inherit the owning cluster
  instead of landing on whichever cluster dispatches them

### Fixed
- **Hook-handler retry-vs-completion race**: the retry decision commits in the same
  transaction as the completion, and a result arriving for an attempt already resolved
  (reaped, superseded) is dropped instead of double-dispatching
- **Reaper replays saved completions the hook handler missed**: a completion Django-Q2
  saved but never delivered (monitor died mid-dispatch) is resolved from the saved
  result instead of being retried as a false orphan
- **Chain advance queues the next step under the row lock**, and parallel workflow
  fan-out enqueues members atomically, closing double-queue windows under duplicate
  hook delivery
- **Workflow cancel is a guarded update**: cancel only lands from a cancellable state,
  and a committed completion racing the cancel wins instead of being overwritten.
  Cancel now also stops member retries (a failed member of a cancelled workflow
  schedules no new attempts) and dispatches the workflow's `on_cancelled` hook
- **Completion routing survives a monitor crash**: resolution commits with a
  `routed=False` flag; the reaper replays workflow routing and hook dispatch (both
  idempotent) for attempts whose post-commit dispatch died, instead of wedging the
  workflow at RUNNING forever
- **Retention cannot delete revived or live work**: the batched delete re-applies
  the sweep's filter, so a task a DLQ requeue flipped back to PENDING between
  select and delete survives; a terminal workflow with still-running members is
  kept until they settle
- **Chain `run()`/`resume()`/`approve()` transition and enqueue in one locked
  transaction**: a crash between them no longer strands the chain RUNNING with
  nothing queued, and a concurrent second resume or racing cancel fails the
  transition instead of double-queueing the step
- **`django.tasks` `get_result().started_at` reads the lease's `date_started`**:
  a SCHEDULED attempt's row exists before any worker touches it, so
  `date_created` no longer means "started"

### Changed
- **`async_task()` validates harder at enqueue**: callables must be importable (bound
  methods and `functools.partial` rejected), reserved `q_options` keys are rejected,
  django-q option names hiding in function kwargs are rejected, and `save=True` is
  forced so the hook handler always sees a result row
- **Threaded workers enforce per-task deadlines**: a task overrunning its timeout gets
  a grace period, then the worker process exits forcibly so the sentinel can recycle
  it — a stuck thread no longer wedges the pool silently
- **Grey-area semantics pinned**: `max_attempts` counts total executions (documented
  on the field), priority lanes are strict (no cross-lane stealing), and workflow
  cancel scope covers members not yet enqueued
- Dropped the dead scheduler notify seam and the redundant `q2_task_id` index

### Database
- Migration `0006_scheduler_owned`: `QraftTaskAttempt.state`, `not_before`, `cluster`,
  `dispatch_func`, `dispatch_args`; nullable with DB-level defaults for a safe live
  rollout
- Migration `0007_worker_identity`: `QraftTaskAttempt.worker_pid`, `worker_thread`
- Migration `0008_drop_redundant_db_index`
- Migration `0009_attempt_routed`: `QraftTaskAttempt.routed`, backfilled True for
  already-resolved attempts
- Migration `0010_alter_qrafttaskattempt_cluster`: help-text catch-up, no DB change

### Demo
- Demo rebuilt as a self-verifying scenario suite: 32 scenarios in 6 groups
  (`core`, `workflows`, `durability`, `ai`, `django-tasks`, `bench`), each proving its
  claims through recorded checks. `manage.py demo {all|run|list|serve}`; `serve` boots
  a scenario-runner dashboard with soak load and per-cluster controls

## [1.2.1] - 2026-08-05

### Added
- **Execution lease with heartbeat** (`qraft/lease.py`): a worker stamps `date_started`
  and `heartbeat_at` on the attempt at `pre_execute` and refreshes `heartbeat_at` from a
  daemon thread every `heartbeat_interval` seconds (new setting, 30s) until the task ends.
  New `QraftTaskAttempt.date_started` / `heartbeat_at` fields (migration
  `0005_execution_lease`)

### Fixed
- **Reaper no longer reaps live long-running tasks.** The old predicate treated "no
  Django-Q2 `Task` row" as worker death, but Django-Q2 writes that row only at completion,
  so any task running past `reap_stale_after` was killed and requeued. `reap_orphans()`
  now reaps on a stale lease heartbeat (older than `max(3 * heartbeat_interval, 90s)`), or
  on a never-started attempt older than `reap_stale_after` whose pack is no longer in the
  ORM broker queue. `reap_stale_after` stays as the never-started fallback knob

- **Dead letter queue** (`qraft/dlq.py`): `dead_letters()` finds `FAILED`/`EXHAUSTED`
  tasks; `requeue()` re-enqueues one as the next attempt on the same `QraftTask`,
  preserving history and idempotency key. Admin gains a "Requeue selected dead tasks"
  bulk action
- **`django.tasks` deferred execution and `TaskContext`** (`qraft/backend.py`):
  `supports_defer` schedules `run_after` tasks through a marker-carrying Django-Q2
  `Schedule`, the same linkage used for retries. `takes_context=True` tasks get a real
  `TaskContext` injected worker-side via `run_task_with_context`, without disturbing the
  `QraftTask` record of the real target function/args/kwargs
- **Status-aware idempotency keys** (`qraft/tasks.py`): `idempotency_retry_dead=True`
  lets a `FAILED`/`EXHAUSTED` task's key be reclaimed by a new enqueue instead of
  permanently blocking it. Default `False` keeps the existing permanent-dedupe behavior

### Fixed
- **Parallel workflow cancel race**: `ParallelDispatcher` re-checks workflow status under
  the row lock inside `_atomic_increment()`, so a cancel racing the final task completion
  is no longer overwritten back to `SUCCEEDED`/`FAILED`
- **Chain double-queue on duplicate hook delivery**: `ChainDispatcher` now advances
  `current_step_index` only while it still matches the completing step's index, checked
  under a row lock, so a duplicate `qraft_hook_handler` delivery for the same step no
  longer queues the next step twice
- **Retries/DLQ requeue of `@task`-decorated functions**: `RetryPolicy.schedule_retry()`
  and `dlq.requeue()` now schedule the new `qraft.runner.run_task` instead of the stored
  dotted path directly, so a retried or requeued `django.tasks`-decorated function is
  unwrapped and called correctly instead of resolving to the non-callable `@task` wrapper.
  Requeued `takes_context=True` tasks still don't get `TaskContext` re-injected - out of
  scope, see `docs/django-tasks-backend.md`
- **Rate-limit delay cap now applied after jitter**: a `retry_after` hint near
  `rate_limit_max_delay` could slip past the cap once jitter was added (e.g. 300s cap ->
  330s). `parse_retry_after()` also floors sub-second hints to 1.0s, so a provider hint of
  `0` can no longer produce a zero/negative retry delay

### Changed
- **`async_task()` rejects `sync=True`, `save=False`, and `cached=...`**: completion
  tracking depends on the hook handler seeing a saved Django-Q2 result after the
  `QraftTaskAttempt` row exists, and these modes all break that: `sync=True` finishes (and
  fires the hook) before the row is committed, and `save=False`/`cached=...` never produce
  the result row at all. All three now raise `ValueError` instead of silently corrupting
  tracking

## [1.2.0] - 2026-08-05

Positioning shift: durable background jobs and workflows for Django — Postgres only, no
extra infra, built for AI workloads.

### Added

#### Durability
- **Orphan reaper** (`qraft/reaper.py`): `reap_orphans()` finds attempts whose worker died
  mid-run (task still `RUNNING`, attempt unresolved, no Django-Q2 `Task` row) and resolves
  them through the normal retry path, marking them `exception_class="OrphanedTask"`. Runs
  as a daemon thread beside the monitor process; settings `reap_interval` (60s) and
  `reap_stale_after` (3600s)
- **Broker receipt warning**: `QraftCluster.start()` warns when the configured broker
  overrides neither `acknowledge()` nor `fail()` — in-flight tasks are lost on a worker
  crash

#### AI-workload primitives
- **Rate-limit-aware retries**: `RATE_LIMIT_EXCEPTIONS` (RateLimited, RateLimitError,
  TooManyRequests, ThrottlingException, ResourceExhausted, OverloadedError, …) retry even
  under a restrictive `retry_exceptions` allowlist, back off exponentially regardless of
  the configured strategy, and cap at `rate_limit_max_delay` (300s). `parse_retry_after()`
  honors provider `Retry-After` hints as a floor
- **Cross-worker throttle** (`qraft/throttle.py`): `throttled()` decorator and `acquire()`
  over a DB token bucket (`RateBucket`), so N workers share one provider rate limit.
  Raises `RateLimited` when empty, which reschedules with rate-limit backoff
- **Idempotency keys**: unique `idempotency_key` on `QraftTask`; a repeat `async_task()`
  returns the original call's Q2 task id instead of enqueueing again. Permanent dedupe —
  a `FAILED`/`EXHAUSTED` task still blocks re-enqueue
- **Usage and progress** (`qraft/context.py`): `record_usage()` accumulates numeric fields
  into `QraftTaskAttempt.usage`, `report_progress()` merges into `QraftTask.progress`.
  Both find the executing attempt through a `pre_execute` receiver, no argument threading.
  `aggregate_usage()` and `aggregate_workflow_usage()` roll up across attempts and
  workflows
- **Priority lanes** (`qraft/brokers.py`): `QraftOrmBroker` drains `{list_key}--high`,
  `{list_key}`, `{list_key}--low` in order on one ORM queue. Enqueue with
  `qraft_options={'priority': 'high'|'low'}`; consume by setting
  `Q_CLUSTER["broker_class"] = "qraft.brokers.QraftOrmBroker"`

#### Workflows
- **Approval-gated chain steps**: `chain.append(..., requires_approval=True)` parks the
  chain in `WAITING_APPROVAL` at zero compute before that step. `QraftChain.approve()` and
  `reject(reason)` resume or cancel it under a row lock with transition validation
- **`result()` returns on parked chains**: `WAITING_APPROVAL` is a polling stop state, so
  `result(wait=...)` hands back completed steps instead of blocking to the timeout

#### django.tasks (DEP 14)
- **`qraft.backend.QraftTaskBackend`**: engines Django 6.0's official Tasks API on Qraft's
  pipeline. `supports_get_result` and `supports_priority`; deferred, coroutine, and
  context-taking tasks are rejected by `validate_task()`. Status mapping
  PENDING/RUNNING/SUCCEEDED/FAILED-EXHAUSTED → READY/RUNNING/SUCCESSFUL/FAILED. Import is
  guarded, so the package stays import-safe on Django < 6.0

### Changed
- `qraft.models` now re-exports `RateBucket` and `TaskPriority` alongside the other models
- `RetryPolicy` accepts `rate_limit_exceptions` and `rate_limit_max_delay`;
  `calculate_delay()`/`next_eta()`/`schedule_retry()` take `retry_after` and
  `is_rate_limit`
- `qraft_hook_handler()` passes the raw task result to `handle_task_retry()` so
  `Retry-After` hints can be parsed
- `QraftSentinel.spawn_monitor()` spawns the monitor with the reaper thread attached

### Database
- Migration `0004_ai_workload_fields`: `QraftTask.idempotency_key` (unique),
  `QraftTask.priority`, `QraftTask.progress`, `QraftTaskAttempt.usage`,
  `QraftChainStep.requires_approval`, `WAITING_APPROVAL` workflow status choice, and the
  new `RateBucket` table

### Demo
- New scenarios: `demo approval`, `demo ratelimit`, `demo usage`, `demo idempotent`,
  `demo reaper`
- New tasks: `throttled_task`, `llm_task`, `charge_task`, `review_task`, `publish_task`

### Documentation
- New `docs/ai-workloads.md` and `docs/django-tasks-backend.md`
- `docs/workflows.md`: approval steps and the updated status lifecycle
- `docs/roadmap.md`: shipped items collected at the top
- README: new feature sections, repositioned tagline, Postgres requirement

### Testing
- New test modules: `test_approval.py`, `test_backend.py`, `test_brokers.py`,
  `test_context.py`, `test_reaper.py`, `test_throttle.py`. Suite is 275 passing;
  `test_backend.py` skips unless Django 6.0+ is installed

## [1.1.1] - 2026-02-17

### Fixed

#### Concurrency Bugs
- **Fix `attempt_count` race in retry logic**: Use concrete `attempt.attempt_number` instead of COUNT query to prevent race conditions in concurrent retry scheduling
- **Add `select_for_update()` in hook handler**: Prevent concurrent status updates on the same QraftTask
- **Fix TOCTOU race in `_dispatch_workflow_hook()`**: Replace filter-then-create pattern with atomic `get_or_create()` for workflow hook dispatch
- **Fix HookDispatch placeholder cleanup**: Delete placeholder dispatch record on `q2_async_task()` failure instead of silently swallowing the error
- **Wrap `async_task()` in transaction**: QraftTask creation + Q2 queueing + QraftTaskAttempt creation are now atomic
- **Add ParallelDispatcher idempotency**: New `counted` field on QraftTaskAttempt prevents double-counting in parallel workflows

#### Configuration & Error Handling
- **LRU-cached `get_conf()`**: Settings instances are now cached per cluster name, avoiding re-creation on every call
- **Use `get_conf()` in RetryPolicy**: Retry defaults now respect ALT_CLUSTERS at runtime instead of using module-level singleton
- **Unified retry marker constants**: Centralized marker format strings in `qraft/retry.py` for consistency
- **UUID validation in marker parsing**: `_parse_qraft_marker()` now validates UUID format before returning

### Added

#### Workflow Improvements
- **Cancellation support**: All workflow types (Chain, Iter, Batch) now support `cancel()` method with `CANCELLED` status
- **Progress hooks**: Parallel workflows (Iter, Batch) support `progress_hook` called on each task completion
- **Rich result objects**: `WorkflowResult` and `TaskResult` dataclasses with `succeeded`, `failed_results`, `errors()`, and backward-compatible iteration
- **BaseWorkflow class**: Shared base class for Chain/Iter/Batch with common patterns (polling, cancellation, result building)
- **Hook path validation**: Hook dotted paths are validated at workflow creation time using `import_string()`
- **Exponential backoff polling**: `result(wait=...)` now uses exponential backoff (50ms → 2s) instead of fixed 100ms sleep
- **`on_cancelled` hook field**: Workflows can specify a hook to call on cancellation
- **`QraftBatch.append()`**: Consistent API across all workflow types; `add()` is deprecated

#### Admin & Models
- **Workflow admin registration**: `QraftChainModel`, `QraftIterModel`, `QraftBatchModel`, and `WorkflowHookDispatch` are now registered in Django admin with colored status, counters, and hook indicators
- **Fix N+1 in QraftTaskAdmin**: `get_queryset()` now annotates `attempt_count` to avoid per-row queries
- **State machine validation**: `WorkflowStatusMixin.transition_to()` validates status transitions with clear error messages
- **`counted` field on QraftTaskAttempt**: Supports dispatcher idempotency

#### Packaging & CI
- **Fixed `requires-python`**: Changed from `>=3.9` to `>=3.10` (match syntax used in codebase)
- **Fixed CI workflow**: Updated Python matrix, fixed `uv sync --group test`, added Codecov token, added `--cov-fail-under=80`
- **Added packaging metadata**: classifiers, project URLs, keywords
- **Version metadata**: package version is exposed via `pyproject.toml` / `importlib.metadata.version("django-qraft")`
- **Fixed README/CHANGELOG placeholders**: Replaced `yourusername` with actual GitHub username

### Changed
- `QraftTask.qraft_iter` and `qraft_batch` FK cascade changed from `SET_NULL` to `CASCADE`
- `QraftBatch.add()` deprecated in favor of `append()` for API consistency
- `RetryPolicy.__init__()` now uses `get_conf()` instead of module-level `conf`
- `schedule_retry()` now takes explicit `current_attempt` parameter
- Ruff target version changed from `py312` to `py310`

### Database
- `QraftTaskAttempt.counted`, `WorkflowHookMixin.on_cancelled`, `WorkflowHookMixin.progress_hook`, `CANCELLED` status choice, and FK cascade changes folded into migration `0003_workflow_primitives` (squashed pre-release)

## [1.1.0] - 2026-01-24

### Added

#### Workflow Primitives
- **QraftChain**: Sequential task execution with resume capability
  - Each step can have its own retry policy
  - Chain-level success/failure hooks
  - Resume from failed step after fixing issues
  - Automatic continuation after each step succeeds
  - API: `QraftChain.append()`, `run()`, `resume()`, `result()`, `current()`

- **QraftIter**: Parallel execution of same function with different inputs
  - Atomic counter tracking for completion
  - Default retry policy applies to all items
  - Workflow-level hooks fire when all tasks complete
  - API: `QraftIter.append()`, `run()`, `result()`, `length()`

- **QraftBatch**: Parallel execution of different functions (fork-join)
  - Each task can have individual retry policy
  - Heterogeneous task support
  - Workflow-level hooks fire when all tasks complete
  - API: `QraftBatch.add()`, `run()`, `result()`

#### New Models
- `QraftChainModel`: Chain workflow state tracking
- `QraftChainStep`: Individual chain step with OneToOne link to QraftTask
- `QraftIterModel`: Iter workflow with atomic counters
- `QraftBatchModel`: Batch workflow with atomic counters
- `WorkflowHookDispatch`: Idempotent workflow hook tracking

#### Module Reorganization
- Refactored `qraft/models.py` into modular structure:
  - `qraft/models/tasks.py`: QraftTask and QraftTaskAttempt
  - `qraft/models/workflows.py`: Workflow models
  - `qraft/models/hooks.py`: Hook dispatch models
  - `qraft/models/mixins.py`: Shared enums and mixins (WorkflowStatus, WorkflowHookMixin)

#### Workflow Infrastructure
- Workflow detection in hook handler with automatic routing
- `ChainDispatcher` for sequential workflow continuation
- `ParallelDispatcher` for atomic completion tracking with F() expressions
- `_create_workflow_task()` internal helper for workflow task creation
- Workflow-level dual-phase hooks (separate from task-level hooks)

### Changed
- Hook handler now detects workflow membership and routes to appropriate dispatcher
- Workflow tasks skip task-level hooks (workflow-level hooks only)
- Models module is now a package with organized submodules

### Database
- Migration `0003_workflow_primitives`: Adds all workflow models and QraftTask FKs
  - New tables: `qraft_qraftchainmodel`, `qraft_qraftchainstep`, `qraft_qraftitermodel`, `qraft_qraftbatchmodel`, `qraft_workflowhookdispatch`
  - Added fields: `qraft_task.qraft_iter`, `qraft_task.qraft_batch`

### Demo Updates
- Added `demo chain` command for sequential workflow demonstration
- Added `demo iter` command for parallel same-function demonstration
- Added `demo batch` command for parallel multi-function demonstration

### Testing
- Added comprehensive unit tests for Chain, Iter, and Batch primitives
- Added integration tests marked with `@pytest.mark.integration`
- Test files: `test_chain.py`, `test_iter.py`, `test_batch.py`, `test_workflow_integration.py`

### Documentation
- Updated README.md with workflow primitives documentation and examples
- Updated CLAUDE.md with workflow architecture details
- Added workflow primitives to feature list and use cases

## [1.0.0] - 2025-01-24

### Added
- Dual-phase hooks with separate success and failure handlers
- Rich retry policies with exponential, linear, and fixed backoff strategies
- Jitter support for retry delays to prevent thundering herd
- Optional multithreaded workers for I/O-bound workloads
- Async hook execution to prevent monitor bottlenecks
- `QraftTask` model for logical task tracking
- `QraftTaskAttempt` model for execution history
- `HookDispatch` model for idempotent hook execution
- `async_task()` function with Qraft-specific options
- `qraftcluster` management command
- Pydantic-based settings with Django integration
- ALT_CLUSTERS support for mixed workload pools
- Comprehensive unit test suite (108 tests, >90% coverage)
- Demo application with hooks, retry, and performance benchmarks

### Features

#### Dual-Phase Hooks
- Separate `success_hook` and `failure_hook` configuration
- Custom arguments and kwargs for each hook
- Async execution via worker pool (default) or sync in monitor
- Idempotent dispatch with `HookDispatch` tracking

#### Retry Policies
- Maximum attempts with exhaustion handling
- Backoff strategies: exponential (default), linear, fixed
- Jitter randomization (0-30% by default)
- Exception filtering with `retry_exceptions` and `skip_exceptions`
- Automatic scheduling via Django-Q2 Schedule

#### Multithreaded Workers
- Configurable threads per worker process
- Semaphore-based backpressure control (`max_inflight`)
- Graceful shutdown with configurable grace period
- Database connection management per thread
- Process-level timeout enforcement

#### Configuration
- `QRAFT_CLUSTER` settings (fallback to `Q_CLUSTER`)
- Pydantic validation with sensible defaults
- Environment-based cluster selection (`Q_CLUSTER_NAME`)
- Backward compatibility with Django-Q2

### Compatibility
- Python 3.10+
- Django 4.2+
- Django-Q2 1.8+
- Drop-in enhancement of Django-Q2

## [0.1.0] - 2025-01-10 (Initial Development)

### Added
- Initial project structure
- Basic task queue functionality
- Integration with Django-Q2

---

## Release Notes Guidelines

### Version Format
- **Major (X.0.0)**: Breaking changes
- **Minor (0.X.0)**: New features, backward compatible
- **Patch (0.0.X)**: Bug fixes, backward compatible

### Categories
- **Added**: New features
- **Changed**: Changes to existing functionality
- **Deprecated**: Soon-to-be removed features
- **Removed**: Removed features
- **Fixed**: Bug fixes
- **Security**: Security fixes

[Unreleased]: https://github.com/ankitksr/django-qraft/compare/v1.3.0...HEAD
[1.3.0]: https://github.com/ankitksr/django-qraft/compare/v1.2.1...v1.3.0
[1.2.1]: https://github.com/ankitksr/django-qraft/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/ankitksr/django-qraft/compare/v1.1.1...v1.2.0
[1.1.1]: https://github.com/ankitksr/django-qraft/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/ankitksr/django-qraft/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/ankitksr/django-qraft/releases/tag/v1.0.0
[0.1.0]: https://github.com/ankitksr/django-qraft/releases/tag/v0.1.0
