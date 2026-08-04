# Roadmap

Positioning: **durable background jobs and workflows for Django — Postgres only, no extra
infra, built for AI workloads.** Django-Q2 provides the cluster runtime; Qraft owns task
state, retries, hooks, and orchestration. Features compete with Procrastinate/Chancy on
Django-nativeness and with Temporal/Hatchet-class systems on "no new infra to operate",
not with Celery/Dramatiq on raw broker throughput.

Why these bets: Django-Q2 upstream is in maintenance mode and has declined or stalled on
dual-phase hooks, retry policies, and workflow enrichment (django-q2#202, #203, #327).
`django.tasks` (Django 6.0, DEP 14) standardizes the *interface* but explicitly excludes
retries, hooks, chaining, and workers from its first pass. No Django-native library today
offers Canvas-depth workflows or AI-workload primitives.

## 1. Foundation — earn the "durable" claim

- **Postgres/ORM broker as the blessed default.** The Redis broker loses in-flight tasks
  on worker crash (no message receipts — documented Django-Q2 limitation). Docs and demo
  default to the ORM broker; Redis documented as at-your-own-risk.
- **Orphan detection and auto-requeue.** Worker heartbeat on `QraftTaskAttempt`; a reaper
  requeues attempts whose worker died mid-run instead of leaving tasks stuck in RUNNING.
  Closes the crash-path gap upstream has in `MAX_ATTEMPTS`/`ack_failure` (django-q2#328).

## 2. django.tasks first-class support

Implement the DEP 14 backend interface so Qraft is an *engine* for Django's official API:

- `qraft.backend.QraftTaskBackend(BaseTaskBackend)` — `enqueue()`/`aenqueue()` map to
  `qraft.tasks.async_task()`; `priority` and `queue_name` map to cluster routing;
  `get_result()` reads `QraftTask`/`QraftTaskAttempt`.
- `TaskContext.attempt` backed by our attempt tracking; result statuses mapped
  (READY/RUNNING/SUCCESSFUL/FAILED ↔ PENDING/RUNNING/SUCCEEDED/FAILED-EXHAUSTED).
- Qraft-specific options (retry policy, success/failure hooks, workflows) remain available
  via our native API; the backend covers the standard surface so `@task` code stays
  portable.

This is a positioning play: teams adopt the official interface, Qraft supplies the
missing execution, retries, and orchestration.

## 3. AI-workload table stakes

- **Rate-limit-aware retries.** Extend `RetryPolicy` with a distinct retryable class for
  provider throttling (429/503/529): honor `Retry-After` when present, cap with jitter,
  never count against the hard-error budget the same way.
- **Idempotency keys.** Optional unique `idempotency_key` on `QraftTask`; `async_task()`
  returns the existing task instead of double-enqueueing. Prevents duplicate side effects
  (double charge, double email) on retries and redelivery.
- **Token/cost accounting.** Optional per-attempt usage recording
  (`model, input_tokens, output_tokens, cost`) on `QraftTaskAttempt`, aggregated at task
  and workflow level, surfaced in admin.

## 4. Differentiators

- **Human-in-the-loop workflow step.** A chain step that parks in `WAITING_FOR_APPROVAL`
  at zero compute and resumes on an external signal (model method + view helper). No
  Django-native tool offers distributed pause/resume today.
- **Cross-worker backpressure.** Shared token bucket (DB row per provider/tenant key) so
  N workers don't retry a rate-limited provider in lockstep — per-process semaphores can't
  coordinate this.
- **Priority lanes.** Priority-within-cluster scheduling on top of existing
  `ALT_CLUSTERS`/`cluster=` routing, so paying-tenant or interactive jobs preempt batch
  work without a second deployment.
- **Progress reporting.** Task-updatable progress state between RUNNING and terminal
  (counter + free-form payload) so apps can show live agent-loop progress without polling
  Django-Q2 internals; pairs with `progress_hook` on parallel workflows.

## Deliberately not planned

- **Semantic/result caching** — app-layer concern; scope creep for a queue.
- **LangGraph/Pydantic-AI adapters** — only worth building on top of §3/§4 once they
  exist; DBOS already owns the generic version of this play.
- **asyncio worker** — threading already covers I/O-bound concurrency; revisit only on
  demonstrated demand (`docs/future/asyncio-worker.md` holds the design).
- **Broker work beyond Postgres** — throughput races with Redis/RabbitMQ queues are not
  the niche.
