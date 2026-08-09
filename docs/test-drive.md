# Test drive

This document tells you how to run the demo application and its verification
suite. All prose in this document follows ASD-STE100 Simplified Technical
English. Each command is safe to run more than one time.

The demo is a verifier, not a brochure. Each scenario declares what it
expects, drives the queue, and then examines the result. The suite prints
PASS or FAIL for each scenario.

## 1. Set up the environment

The demo application is in `demo/` and has its own virtual environment. It
needs Python 3.12 or later and Django 6.0.

```bash
git clone https://github.com/ankitksr/django-qraft.git
cd django-qraft/demo
make setup
```

`make setup` installs the dependencies and applies the migrations.

## 2. Select the database

PostgreSQL is the default. The suite starts more than ten worker processes
against one database, and the throttle and workflow paths need true row
locks. Create the database one time:

```bash
createdb qraft_demo
```

To use SQLite instead, set `DEMO_DB=sqlite`. SQLite is sufficient for a first
look, but it can lock under a heavy parallel load.

## 3. Run the suite

```bash
make demo
```

This one command does all of the work:

1. It resets the database.
2. It starts the worker clusters that the scenarios need.
3. It runs all 32 scenarios.
4. It stops the clusters.
5. It prints a PASS/FAIL matrix and sets the exit code.

You do not have to open more than one terminal. The cluster output goes to
`demo/.demo-logs/`.

## 4. Run part of the suite

```bash
uv run python manage.py demo list
uv run python manage.py demo all --group durability
uv run python manage.py demo run core.hooks wf.chain
```

`demo list` shows each scenario and what it proves. `--group` limits the run
to one group: `core`, `workflows`, `durability`, `ai`, `django-tasks`, or
`bench`.

To keep the clusters after the run, add `--keep-clusters`.

## 5. Watch the queue

```bash
make ui
```

This starts the clusters and serves the monitor on http://127.0.0.1:8000/.

The page shows:

- each task with its status, attempt count, heartbeat age, and progress
- each workflow with its members and counters
- the dead-letter queue, with a button to requeue a task
- the rate buckets, and the token and cost totals
- a live feed of what the tasks and the hooks recorded

You can start any scenario from the page and watch it run. The page polls the
server; it does not load anything from the network.

Two more panels create load by hand:

- **Clusters** — start or stop any worker profile with one button.
- **Soak** — put long mock API calls on the `soak` cluster. Set the task
  count, the duration range, and the failure rate, then click start. The
  panel boots the `soak` cluster when necessary. Watch the task table for
  the pickup order, the heartbeat age, the progress, and the retries.

For per-object detail, use the Django admin at
http://127.0.0.1:8000/admin/qraft/.

## 6. What the scenarios cover

- **core** — dual-phase hooks; exponential, linear and fixed backoff; jitter;
  `retry_exceptions` and `skip_exceptions`; `Retry-After` handling; threaded
  workers against process workers; async hooks against `sync_hooks`.
- **workflows** — chain order and per-step retry; chain failure; chain
  resume; iter and batch fan-out; the progress hook; approval and rejection;
  cancellation, including a cancel that races a completion.
- **durability** — the execution lease and its heartbeat; orphan recovery
  after a true SIGKILL; orphan recovery during a retry attempt; the
  dead-letter queue and requeue; the retention sweep.
- **ai** — idempotency keys; token and cost accounting; a rate bucket shared
  by two clusters; priority lanes and the misconfigured fallback; task
  progress.
- **django-tasks** — enqueue and result retrieval through
  `QraftTaskBackend`; `run_after`; `takes_context`; honest reporting of
  `supports_priority`.
- **bench** — qraft against plain django-q2 on the same workers: delay
  precision (a 2-second ask against the ~30-second scheduler tick),
  throughput overhead, and pickup latency. The measured numbers are in the
  notes of each run.

`demo/README.md` lists each scenario and its claim.

## 7. Run the unit tests

The unit tests do not need a cluster or PostgreSQL:

```bash
cd ..
uv run pytest
```

The root environment uses Django 5.2, so `tests/test_backend.py` does not
run there. The demo environment is where the `django.tasks` backend is
exercised, through the `django-tasks` group of scenarios.

## Troubleshooting

- **A scenario fails with a timeout.** Read
  `demo/.demo-logs/<profile>.log`. A cluster that cannot reach the database
  fails there first.
- **The database has old rows.** `make demo` resets the database at the
  start. Add `--keep-data` only if you want the results to accumulate.
- **Code changes have no effect.** Worker processes load the code one time
  at start. The suite starts the clusters for each run, so this affects only
  `make ui` or a cluster you started by hand.
