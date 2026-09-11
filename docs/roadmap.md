# Roadmap

Positioning: **durable background jobs and workflows for Django — Postgres only, no extra
infra, with primitives for jobs that call metered AI providers.** Django-Q2 provides the
cluster runtime; Qraft owns task state, retries, hooks, and orchestration. Features compete
with Procrastinate/Chancy on Django-nativeness and with Temporal/Hatchet-class systems on
"no new infra to operate", not with Celery/Dramatiq on raw broker throughput.

Why these bets: Django-Q2 is actively maintained (1.11.0, August 2026), so the bets are
about its task model, not its health. That model leaves five gaps, and Qraft's features are
exactly those five:

| Django-Q2 | Qraft |
|---|---|
| One `Task` row per execution, overwritten on retry — no attempt history | `QraftTaskAttempt` per execution, with outcome and exception |
| Retry is a cluster-wide redelivery setting | Per-task policy: backoff curve, jitter, exception filtering |
| Delay quantized to the scheduler's 30-second cycle | Exact ETA on an owned row |
| A hook is one callback for both outcomes | Separate success and failure hooks, dispatched asynchronously |
| A worker that dies mid-task leaves nothing to detect it by | Lease, heartbeat, and a reaper that resolves orphans |

Requests for several of these are open upstream rather than shipped (django-q2#202, #203,
#327). `django.tasks` (Django 6.0, DEP 14) standardizes the *interface* but explicitly
excludes retries, hooks, chaining, and workers from its first pass. No Django-native library
today offers Canvas-depth workflows or AI-workload primitives.

## Shipped in 1.4.0

- **The lease ends where the function ends.** `run_task` stamps `returned_at` and stops
  the heartbeat when the target returns, so a task whose result is stranded by a dead
  monitor stops looking alive. Reaped as `ResultLost`, which a retry policy can treat
  differently from `OrphanedTask`.
- **Observability.** Subjects on
  every task and workflow; progress owned by the attempt, with reported and advanced
  timestamps; signals Qraft emits about its own transitions; a metrics sink with an
  OpenTelemetry implementation, a logging filter and trace propagation; durable hook
  context.
- **Execution graphs** ([workflows.md](workflows.md#graphs)). `graphs.Graph(...)` declares
  nodes with explicit `after` edges and Qraft dispatches every node whose dependencies are
  met, rather than the application enqueueing the next stage. The graph settles once, on
  quiescence, with a durable `on_settled` hook and a `summary` snapshot. Selective resume
  reruns named nodes and their downstream closure under a new generation; a completion
  receipt makes the "kept" claim exact; approval gates park a node until a person decides.
  A graph may start without a subject and bind it once the node that creates the subject
  has run, and `budgets=` with `context.consume_budget()` bounds what one graph may ask of
  a provider across every node and every retry — the question a refilling token bucket
  cannot answer.
- **Cost from usage.** An optional `QRAFT_PRICING` resolver turns recorded tokens into
  money, priced per increment so a corrected table never re-prices a billed call.
  `cost()` answers with coverage and estimated flags, never a bare number.
- **Stall observation.** `stall_after` on a task; the reaper flags an attempt whose
  heartbeat is fresh but whose progress has not advanced. Observation only — Qraft never
  retries on this signal, and the application acts through the ownership pattern.
- **Coroutine tasks.** An `async def` target runs via `asyncio.run()` in its worker slot.
- **Per-cluster brokers.** `qraft.brokers.broker_for_cluster()` builds the broker a named
  cluster runs and every Qraft enqueue names it, so one cluster can run the ORM broker
  while the rest stay on Redis. `RoutingBroker` covers enqueues Qraft does not make.
- **One attempt, one execution.** A compare-and-swap on `execution_count` refuses a broker
  redelivery of an attempt that already ran, so the retry policy — not the delivery loop —
  decides what happens next.

## Shipped in 1.3.0

- **Qraft-owned scheduling** (phase 2 of the absorption plan). A delayed attempt is a
  `SCHEDULED` row dispatched at its exact due time; retries, DLQ requeues, and deferred
  `django.tasks` all go through it. Priority and target cluster survive the delay.
- **Bundled monitoring dashboard** (`qraft.dashboard`) — see [dashboard.md](dashboard.md).
- **Retention sweep** — bounded pruning of settled rows by age and/or count.
- **Cluster routing** — `cluster` is a real `async_task()` parameter; retries and
  requeues inherit the owning cluster.
- **Cancel/completion hardening** — cancel is a guarded update, chain advance and
  fan-out are atomic, the reaper replays completions the hook handler missed.

## Shipped in 1.2.0

- **ORM broker as the blessed default.** `QraftCluster` warns at startup when the broker
  implements no delivery receipts. Docs and demo run on Postgres + ORM.
- **Orphan detection and auto-requeue.** `qraft.reaper.reap_orphans()`, run as a daemon
  thread beside the monitor, resolves attempts whose worker died mid-run through the
  normal retry path (`reap_interval`, `reap_stale_after`).
- **django.tasks backend.** `qraft.backend.QraftTaskBackend` engines the DEP 14 API on
  Qraft's pipeline — see [django-tasks-backend.md](django-tasks-backend.md).
- **Rate-limit-aware retries.** `RATE_LIMIT_EXCEPTIONS`, `Retry-After` parsing, forced
  exponential backoff capped by `rate_limit_max_delay`.
- **Idempotency keys.** Unique `idempotency_key` on `QraftTask`; a repeat enqueue returns
  the original task id.
- **Token/cost accounting.** `record_usage()` per attempt, `aggregate_usage()` and
  `aggregate_workflow_usage()` for rollups.
- **Human-in-the-loop chain step.** `requires_approval=True` parks a chain in
  `WAITING_APPROVAL`; `approve()`/`reject()` resume or cancel it.
- **Cross-worker backpressure.** `qraft.throttle.throttled()` over a shared `RateBucket`
  row.
- **Priority lanes.** `qraft.brokers.QraftOrmBroker` drains high/default/low lanes on one
  ORM queue.
- **Progress reporting.** `report_progress()` writes to `QraftTask.progress`.

Nothing remains from the original plan; what landed in 1.4.0 is listed above.
(`TaskContext` and deferred tasks shipped in 1.2.1; priority routing for scheduled
retries shipped in 1.3.0 with owned scheduling.)

## Open questions on graphs

- **How many nodes is too many?** No ceiling exists today. One would keep the frontier
  scan and the graph lock honest, but the number should come from a wide fan-in
  measurement rather than intuition.
- **Should a node carry a timeout?** A scheduled attempt runs under its cluster's timeout,
  so today the answer is a cluster per timeout class. Reconsider only for a consumer whose
  node durations differ by an order of magnitude within one cluster.
- **Does a graph need a concurrency cap?** Backpressure today is worker count, throttles
  and budgets. Dispatch through scheduled rows makes a cap a small later change with no
  new state, so deferring costs nothing.
- **Should `QraftChain` and `QraftBatch` become facades over the graph?** A chain is nodes
  with one `after` edge each and a batch is nodes with none, so both would gain resume by
  construction. Breaking, and gated on somebody needing it. `QraftIter` is not re-based:
  ten thousand rows of one function with a counter is the right shape for an iter.

## Architecture direction

Incremental absorption of Django-Q2, one owned subsystem per phase: execution
state (shipped in 1.2.1, the lease), scheduling (shipped in 1.3.0, the owned
scheduler), the worker loop (phase 3 — gated, not started).

## Deliberately not planned

- **Semantic/result caching** — app-layer concern; scope creep for a queue.
- **LangGraph/Pydantic-AI adapters** — only worth building on top of §3/§4 once they
  exist; DBOS already owns the generic version of this play.
- **asyncio worker** — threading already covers I/O-bound concurrency; revisit only on
  demonstrated demand.
- **Graph timeouts that fail a graph automatically** — a graph running too long is
  flagged, never failed. Whether to skip a node or cancel the graph is an application
  decision.
- **Dynamic node expansion, result passing between nodes, a node that is itself a
  workflow, cross-graph edges** — the topology is sealed at `start()` and Qraft does not
  own application outputs.
- **Automatic resume** — the retry policy owns attempts, and a resume is a decision made
  with a preview in front of it.
- **A graph renderer beyond a layered list** — a picture is not what an incident needs
  first.
- **Automatically resolving a suspected stall** — gated on cooperative cancellation. The
  original attempt keeps running and keeps writing, so a retry would double-write.
- **Broker work beyond Postgres** — throughput races with Redis/RabbitMQ queues are not
  the niche. Per-cluster broker *routing* shipped; per-cluster broker *features* did not.
- **At-least-once delivery as the retry mechanism** — `max_executions_per_attempt`
  defaults to 1 and the supported retry path is the policy. A broker redelivering until
  something succeeds hides the failure from the attempt record.


## Next release: stabilize and measure

Before expanding orchestration, verify graph transitions on PostgreSQL, preserve
settlement hooks by generation across resume and retention, and exercise the installed
wheel as well as the source checkout. The bundled dashboard should expose approval,
rejection and cancellation consistently with the API.

Measure wide fan-out/fan-in, graph lock duration, dashboard query counts and retention
volume before choosing node ceilings or per-graph concurrency caps. Native-worker
absorption, nested workflows and a shared asyncio worker remain gated on actual demand.
