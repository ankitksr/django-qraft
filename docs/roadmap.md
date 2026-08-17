# Roadmap

Positioning: **durable background jobs and workflows for Django — Postgres only, no extra
infra, with primitives for jobs that call metered AI providers.** Django-Q2 provides the
cluster runtime; Qraft owns task state, retries, hooks, and orchestration. Features compete
with Procrastinate/Chancy on Django-nativeness and with Temporal/Hatchet-class systems on
"no new infra to operate", not with Celery/Dramatiq on raw broker throughput.

Why these bets: Django-Q2 upstream is in maintenance mode and has declined or stalled on
dual-phase hooks, retry policies, and workflow enrichment (django-q2#202, #203, #327).
`django.tasks` (Django 6.0, DEP 14) standardizes the *interface* but explicitly excludes
retries, hooks, chaining, and workers from its first pass. No Django-native library today
offers Canvas-depth workflows or AI-workload primitives.

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

Remaining from the original plan: coroutine tasks on the django.tasks backend.
(`TaskContext` and deferred tasks shipped in 1.2.1; priority routing for scheduled
retries shipped in 1.3.0 with owned scheduling.)

## Architecture direction

Incremental absorption of Django-Q2, one owned subsystem per phase: execution
state (shipped in 1.2.1, the lease), scheduling (shipped in 1.3.0, the owned
scheduler), the worker loop (phase 3 — gated, not started).
Design and decision gates: [future/q2-absorption.md](future/q2-absorption.md).

## Deliberately not planned

- **Semantic/result caching** — app-layer concern; scope creep for a queue.
- **LangGraph/Pydantic-AI adapters** — only worth building on top of §3/§4 once they
  exist; DBOS already owns the generic version of this play.
- **asyncio worker** — threading already covers I/O-bound concurrency; revisit only on
  demonstrated demand (`docs/future/asyncio-worker.md` holds the design).
- **Broker work beyond Postgres** — throughput races with Redis/RabbitMQ queues are not
  the niche.
