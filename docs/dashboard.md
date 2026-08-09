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
  pre-2.0 rows remain.
- **Status tiles** — task counts by status.
- **Tasks** — recent tasks: attempts, elapsed time, heartbeat age, progress,
  exception class. Task ids deep-link to the Django admin when it is mounted
  and `QraftTask` is registered; plain text otherwise.
- **Workflows** — chain/iter/batch cards with member chips, counters, and a
  gated indicator for approval steps.
- **Dead letters** — FAILED/EXHAUSTED tasks with a requeue button.
- **Rate buckets** and the usage/cost rollup summed from attempt `usage`.
- **Metrics** — computed over a selectable window (15m / 1h / 24h): pickup
  latency p50/p95, dispatch lag p50/p95 for delayed attempts, retry rate, an
  exception-class leaderboard, a throughput sparkline, and a per-function
  table (runs, success %, p95 duration). Sample queries are capped at 5000
  rows per poll.

## Actions

All actions are POST, CSRF-protected, and staff-gated like everything else.
Unknown ids answer 404; invalid states (e.g. requeueing a live task,
approving a chain that is not parked) answer 409 without side effects.

- Requeue a dead letter (`qraft.dlq.requeue`)
- Approve / reject a chain parked at `WAITING_APPROVAL`
- Cancel a chain, iter, or batch (`BaseWorkflow.cancel`); repeating a cancel
  or reject on an already-cancelled workflow answers 200 with `"already": true`

There is no single-task cancel because the library has no such API.

## JSON endpoints

The page polls two endpoints you can also build on:

- `GET api/state/` (polled every 2s) — counts, queue depths, recent tasks,
  workflows, DLQ, rate buckets, usage rollup.
- `GET api/metrics/?window=15m|1h|24h` (polled every 10s) — the metrics panel
  payload; an unknown window falls back to `1h`.

Both return a flat JSON object per response; field names match the panels
above and are intended to stay stable.
