# Monitoring Dashboard

Qraft ships a single-page monitoring dashboard as a bundled Django app. It
reads qraft's own tables on every poll — there is no collector process, no
extra storage, and no static assets or CDN dependencies.

## Install

```python
# settings.py
INSTALLED_APPS += ["qraft.dashboard"]

# urls.py
urlpatterns += [path("qraft/", include("qraft.dashboard.urls"))]
```

Open `/qraft/`. The URL namespace is `qraft_dashboard`; mount it at any prefix.

## Auth

Every view requires `request.user.is_active and request.user.is_staff`. The
page redirects to the admin login (or `LOGIN_URL` if the admin is not
mounted); the JSON endpoints answer 403. One escape hatch for demos and local
development:

```python
QRAFT_DASHBOARD = {"public": True}
```

## What it shows

- **Queue pills** — broker depth (`OrmQ` rows), qraft SCHEDULED attempts with
  seconds until the next one is due, and a legacy `Schedule` count if any
  pre-1.3 rows remain.
- **Status tiles** — task counts by status.
- **Tasks** — recent tasks: attempts, elapsed time, heartbeat age, progress,
  seconds since progress last advanced, a suspected-stall marker (⚠, or ✓ once
  the attempt has advanced since it was flagged), subject, exception class. Task ids
  deep-link to the Django admin when it is mounted and `QraftTask` is
  registered; plain text otherwise.
- **Graphs** — the newest twelve graphs: subject, kind, revision, status,
  generation, elapsed, an overdue badge, a link back to `previous_graph`, and
  one chip per node in dependency order, coloured by node status (pending,
  running, waiting_approval, succeeded, failed, cancelled, skipped). A node's
  chip deep-links to the task bound to it and a failed node carries its
  exception class, so a node that failed before the subject existed — an
  ingest that never made the worksheet — is legible on the graph row rather
  than nowhere. A failed graph carries a resume button whose tooltip names the
  nodes a resume would re-run and the ones it would keep, so the click is a
  decision rather than a guess. Like the task ids above, a link degrades to
  plain text when the admin is not mounted.
- **Workflows** — chain/iter/batch cards with member chips, counters, subject,
  and a gated indicator for approval steps.
- **Dead letters** — FAILED/EXHAUSTED tasks with a requeue button.
- **Rate buckets** and the usage rollup summed from attempt `usage`. With
  `QRAFT_PRICING` configured the panel adds a cost row carrying its coverage
  (`complete`, `partial`, `none`) and an `est` marker — a configured price is
  an estimate of what the provider will bill, never proof of it. The rollup
  honours the subject and graph filters, so it answers "what did this worksheet
  cost" as readily as "what did today cost".
- **Metrics sink health** — a pill, hidden while the configured sink is
  healthy, showing the failure count and the last error when it is not. The
  sink is never disabled; the pill is how you learn it is failing.
- **Metrics** — computed over a selectable window (15m / 1h / 24h): pickup
  latency p50/p95, dispatch lag p50/p95 for delayed attempts, retry rate, an
  exception-class leaderboard, a throughput sparkline, and a per-function
  table (runs, success %, p95 duration). Sample queries are capped at 5000
  rows per poll.

## Filtering by subject

The header's filter box takes `type:id` and narrows the task table, the graph
cards and the workflow cards; `type` alone filters by subject type. It sets the
`subject_type` and `subject_id` query parameters on `api/state/`, which also
accepts `graph` — that one narrows tasks and workflows to a graph's members and
the graph panel to the graph itself. Rows without a subject are excluded from a
subject filter.

## Actions

All actions are POST, CSRF-protected, and staff-gated like everything else.
An unknown graph or task answers 404; every other refusal — an unknown node
key, requeueing a live task, approving a chain that is not parked — answers
409 without side effects.

- Requeue a dead letter (`qraft.dlq.requeue`)
- Approve / reject a chain parked at `WAITING_APPROVAL`
- Cancel a chain, iter, or batch (`BaseWorkflow.cancel`); repeating a cancel
  or reject on an already-cancelled workflow answers 200 with `"already": true`
- Cancel a live graph (`qraft.graphs.cancel`). It does not revoke work
  already in flight: tasks finish and their outcomes are recorded on their node
  rows, which no longer move the settled graph. A graph parked at a gate can be
  cancelled too; cancelling a terminal graph answers 409
- Resume a failed graph (`qraft.graphs.resume`), which re-runs its failed nodes
  and everything downstream of them under a new generation
- Skip a pending node of a running graph (`qraft.graphs.skip`)

There is no single-task cancel because the library has no such API.

## Dashboard metrics versus the metrics sink

The metrics panel computes percentiles from Qraft's own tables on request, for
this page only. It answers "what happened in the last hour" and keeps working
with no sink configured. `metrics_sink` is the separate, push-based path that
feeds your own monitoring system — see [Configuration](configuration.md#metrics-sink).
Neither depends on the other.

## JSON endpoints

The page polls two endpoints you can also build on:

- `GET api/state/` (polled every 2s) — counts, queue depths, recent tasks,
  graphs, workflows, DLQ, rate buckets, usage rollup, `metrics_health`. Accepts
  `subject_type`, `subject_id` and `graph` as filters, echoed back in `filters`.
- `GET api/metrics/?window=15m|1h|24h` (polled every 10s) — the metrics panel
  payload; an unknown window falls back to `1h`.

Both return a flat JSON object per response; field names match the panels
above and are intended to stay stable.
