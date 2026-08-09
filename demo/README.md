# Qraft demo and verification suite

A self-verifying proof of concept for django-qraft. Every scenario declares
what it expects, drives the real queue, and asserts the end state. The suite
boots the worker clusters it needs, runs, tears them down, prints a PASS/FAIL
matrix, and exits non-zero on failure.

Runs on Django 6.0, which is also the first version where
`qraft.backend.QraftTaskBackend` (django.tasks, DEP 14) can be exercised at all.

## Setup

```bash
cd demo
make setup            # uv sync + migrate
```

PostgreSQL is the default and is what the suite is tuned for: a dozen worker
processes share one database, and the throttle and parallel-workflow paths
depend on real row locks. Create the database once:

```bash
createdb qraft_demo
```

Set `DEMO_DB=sqlite` for a quick look without PostgreSQL. SQLite works, but
`ai.throttle` is only correct there because Django opens SQLite transactions
in `IMMEDIATE` mode; heavier parallel runs can still contend.

## Run everything

```bash
make demo             # or: uv run python manage.py demo all
```

One command. It resets the database, starts the clusters, runs all 32
scenarios, stops the clusters, and prints the matrix:

```
SCENARIO                RESULT  CHECKS   TIME
----------------------  ------  ------  -----
[core]
core.backoff            PASS     24/24   87.8s
...
[durability]
dur.reaper-kill         PASS     11/11   23.9s
...
32/32 scenarios passed, 260/260 checks passed
```

A full run takes several minutes. The long poles are deliberate: the
durability scenarios idle through reaper grace periods, and `bench.delay`
waits for django-q2's 30-second scheduler tick on purpose, to measure it.

Useful variants:

```bash
uv run python manage.py demo list                 # every scenario and what it proves
uv run python manage.py demo all --group durability
uv run python manage.py demo run core.hooks wf.chain
uv run python manage.py demo all --keep-clusters  # leave the workers up afterwards
```

## Watch it happen

```bash
make ui               # boots the clusters, serves http://127.0.0.1:8000/
```

The dashboard polls once a second and shows tasks with their status, attempt
count, heartbeat age and progress; workflows with their members and counters;
the dead-letter queue with a requeue button; rate buckets; the token and cost
rollup; and a live feed of what tasks and hooks recorded. Any scenario can be
started from the browser and watched as it runs. Object detail links go to the
Django admin, which `qraft.admin` already provides.

Two panels drive load by hand. **Clusters** starts and stops any worker
profile from the page. **Soak** fans out long mock API calls (default 8 tasks
of 3–10 minutes each, 20% transient failure) onto the `soak` cluster, which it
boots on demand — the task table then shows pickup order, heartbeat age,
progress and the retries as they happen.

The page is self-contained: inline CSS and JavaScript, no CDN, no build step.

## Scenarios

**core**

| Key | Proves |
| --- | --- |
| `core.hooks` | Success fires only the success hook, failure only the failure hook, each with its configured arguments. |
| `core.backoff` | Exponential, linear and fixed backoff each honour `max_attempts` and serve delays whose measured shape matches the strategy. |
| `core.jitter` | Jitter spreads the delay inside the configured fraction; turning it off makes the delay constant. |
| `core.exception-filter` | `retry_exceptions` and `skip_exceptions` each stop a retry series that an otherwise identical policy runs. |
| `core.rate-limit-retry` | A provider `Retry-After` hint overrides the configured backoff, and `rate_limit_max_delay` caps a hint that is too large. |
| `core.threading` | On an I/O-bound load the threaded cluster clears the same queue measurably faster than an equal process cluster. |
| `core.hook-modes` | Async hooks run as their own queued task with a dispatch row; `sync_hooks=True` runs the hook in the monitor with neither. |

**workflows**

| Key | Proves |
| --- | --- |
| `wf.chain` | Steps run in order, a step's own retry policy applies, the chain hook fires once. |
| `wf.chain-failure` | An exhausted step fails the chain, fires the failure hook, and later steps never run. |
| `wf.chain-resume` | `resume()` restarts the failed step only. |
| `wf.iter` | Every member runs, counters land on the total, the workflow hook fires once. |
| `wf.batch` | Different functions fan out together, each with its own retry policy. |
| `wf.progress` | The progress hook fires on every partial completion and stops before the last. |
| `wf.approval` | A gated chain parks at `WAITING_APPROVAL` and consumes nothing; `approve()` resumes, `reject()` cancels. |
| `wf.cancel` | Cancelling stops further work, and a cancel racing the last completion is not overwritten. |

**durability**

| Key | Proves |
| --- | --- |
| `dur.lease` | A running task stamps `date_started` and keeps `heartbeat_at` moving. |
| `dur.reaper-kill` | A worker killed with a real SIGKILL leaves no result, and the reaper reclaims and retries the task. |
| `dur.reaper-retry-crash` | A retry attempt (created SCHEDULED by the dispatcher, not by `async_task()`) is leased too, so killing its worker is also reclaimable. |
| `dur.dlq` | An exhausted task lands in the DLQ; `requeue()` continues the same attempt series with history and idempotency key intact. |
| `dur.retention` | The sweep prunes settled rows past the window and leaves live rows and members of unfinished workflows alone. |

**ai**

| Key | Proves |
| --- | --- |
| `ai.idempotency` | A repeat enqueue under a live key returns the original id; `idempotency_retry_dead` releases a dead key. |
| `ai.usage` | `record_usage` accumulates per attempt; the aggregates roll up per task and per workflow. |
| `ai.progress` | `report_progress()` moves while the task runs and settles on the final step. |
| `ai.throttle` | Two clusters on one `RateBucket` cannot together exceed capacity plus refill over the observed window. |
| `ai.priority` | High drains before default before low; a cluster whose broker cannot drain the lanes warns and still runs the task. |

**django-tasks**

| Key | Proves |
| --- | --- |
| `dt.enqueue` | A `@task` enqueued by Django's Tasks API runs on a qraft cluster; the backend reports status, errors and return value. |
| `dt.defer` | `run_after` stays unrun until due, then executes. |
| `dt.context` | `takes_context=True` receives a `TaskContext` whose `TaskResult` is its own. |
| `dt.priority` | `supports_priority` follows the deployed broker, and Django rejects a priority enqueue when it is false. |

**bench**

| Key | Proves |
| --- | --- |
| `bench.delay` | A 2-second delay through qraft's dispatcher is served in about 2 seconds; the same ask through a django-q2 `Schedule` waits for the ~30-second scheduler tick. |
| `bench.throughput` | Qraft's bookkeeping (task + attempt rows, lease, status updates) costs a bounded factor over raw django-q2 on the same workers; the measured numbers land in the notes. |
| `bench.pickup` | The p95 gap between enqueue and a worker starting the task stays in low seconds. |

## How a scenario proves anything

Task functions and hooks run in worker processes, so they write an `Event` row
for everything observable they do. A scenario reads those rows back, together
with qraft's own tables, and scores each expectation through `ctx.check`. A
scenario that records no checks is reported as an error, not a pass.

Waiting is always `ctx.poll` with a deadline. `ctx.settle` appears only where
the claim is a negative one — "nothing else happened" — which no poll can
establish.

## Clusters

One environment variable, `Q_CLUSTER_NAME`, picks a profile. django-q2 and
qraft both read it and apply their own `ALT_CLUSTERS` entry, so the name
selects the broker lane and the worker settings together.

| Profile | What it is for |
| --- | --- |
| `default` | 3 process workers, async hooks. Most scenarios. |
| `baseline` / `threaded` | 2 workers each; 1 thread versus 8, for the throughput comparison. |
| `synchooks` | `sync_hooks=True`. |
| `throttle-a` / `throttle-b` | Two clusters sharing one rate bucket. |
| `lanes` | 1 worker draining high, then default, then low. |
| `soak` | 4 workers, 30-minute timeout, for the dashboard's long fake-API tasks. |

The suite starts and stops these itself. Cluster stdout goes to
`.demo-logs/<profile>.log`.

To run one by hand:

```bash
Q_CLUSTER_NAME=threaded uv run python manage.py qraftcluster
```

## Known limits

- The two reaper scenarios kill a real worker process, then push its last
  heartbeat 300 seconds into the past. qraft floors the heartbeat grace period
  at 90 seconds (`qraft.reaper.MIN_HEARTBEAT_GRACE`) and does not expose it as
  a setting, so the alternative is 90 seconds of idling per scenario. The kill
  and the reclaim are real; only the clock is moved.
- `core.jitter` and `dt.priority` are in-process checks against the library,
  not end-to-end runs. Each says so in its own output.
- `wf.approval` counts `on_cancelled` dispatches globally rather than per
  run: qraft calls that hook with no arguments, so it cannot be told which
  workflow was cancelled. `cancel()` does not dispatch it at all — only
  `QraftChain.reject()` does.
