# Test drive

This document tells you how to set up the demo application and run each demo
scenario. All prose in this document follows ASD-STE100 Simplified Technical
English. Each scenario is safe to run more than one time.

## 1. Set up the environment

The demo application lives in `demo/` and has its own virtual environment.
It requires Python 3.12 or later and Django 6.0 or later.

1. Clone the repository and go into the demo directory:

```bash
git clone https://github.com/ankitksr/django-qraft.git
cd django-qraft/demo
```

2. Create the environment and install the package:

```bash
uv venv
uv sync
uv pip install -e ..
```

## 2. Select the database

PostgreSQL is the recommended database. SQLite works for a first look, but it
can lock under concurrent load. The `ratelimit`, `priority`, and `perf`
scenarios need PostgreSQL.

For PostgreSQL, create the database one time, then set the environment
variable for every command that follows:

```bash
createdb qraft_demo
export POSTGRES_DB=qraft_demo
```

If you do not set `POSTGRES_DB`, the demo uses SQLite.

3. Apply the migrations:

```bash
uv run python manage.py migrate
```

## 3. Start the cluster

Open a second terminal in `demo/`. Start the cluster there and keep the
terminal open. The cluster terminal shows the task logs, the hook messages,
and the reaper activity.

```bash
export POSTGRES_DB=qraft_demo   # if you use PostgreSQL
uv run python manage.py qraftcluster
```

To stop the cluster, press Ctrl+C.

## 4. Run the scenarios

Run each command from the first terminal. Each subsection tells you what the
scenario shows and what output to expect.

### Core features

**Hooks** — dual-phase success and failure handlers.

```bash
uv run python manage.py demo hooks -n 5
```

The command queues five tasks with a 50% failure rate. Watch the cluster
terminal: successful tasks fire `SUCCESS HOOK` messages, failed tasks fire
`FAILURE HOOK` messages.

**Retry** — exponential backoff with jitter.

```bash
uv run python manage.py demo retry --fail-times 2 --max-attempts 4
```

The task fails two times, then succeeds on the third attempt. The cluster log
shows each retry with an increasing delay.

**Perf** — threaded workers against standard workers. This scenario needs a
second cluster; follow the three-terminal instructions in `demo/README.md`.

### Workflows

**Chain** — sequential steps.

```bash
uv run python manage.py demo chain -n 3 --wait 30000
```

Three steps run one after another. The output lists each step result in
order.

**Iter** — one function over many inputs, in parallel.

```bash
uv run python manage.py demo iter -n 5 --wait 30000
```

**Batch** — different functions in parallel (fork-join).

```bash
uv run python manage.py demo batch -n 3 --wait 30000
```

**Cancel** — workflow cancellation and the `on_cancelled` hook.

```bash
uv run python manage.py demo cancel
```

**Approval** — a human-in-the-loop chain step.

```bash
uv run python manage.py demo approval
```

The chain runs step one, then parks in `waiting_approval` at zero compute.
The command calls `approve()` and the chain completes. Expect a final status
of `succeeded` with two step results.

### AI-workload primitives

**Ratelimit** — a shared token bucket and rate-limit-aware retries.

```bash
uv run python manage.py demo ratelimit
```

Task one takes the only token. Task two raises `RateLimited` and is
rescheduled with backoff. The output shows the new schedule and the task in
the `pending` state.

**Usage** — token and cost accounting.

```bash
uv run python manage.py demo usage
```

The task makes three mock LLM calls and records usage after each one. The
output shows the accumulated tokens and cost on the attempt, and the same
values from `aggregate_usage()`.

**Idempotent** — deduplicated enqueue.

```bash
uv run python manage.py demo idempotent
```

The command enqueues one task two times under one key. Both calls return the
same task id, and only one `QraftTask` row exists.

**Reaper** — crash recovery.

```bash
uv run python manage.py demo reaper
```

The command fabricates a crashed worker, then calls `reap_orphans()`. The
orphan is marked `OrphanedTask` and retried. Expect a final status of
`succeeded` with two attempts in the history.

**Progress** — live progress reporting.

```bash
uv run python manage.py demo progress --steps 5 --delay 0.5
```

The command polls `QraftTask.progress` while the task runs and prints each
step as it lands.

### Integration

**Tasks-API** — the official `django.tasks` API (Django 6.0, DEP 14).

```bash
uv run python manage.py demo tasks-api
```

The command enqueues a `@task`-decorated function through `enqueue()` and
polls `get_result()`. Expect the status transition `RUNNING -> SUCCESSFUL`
and the return value.

**Priority** — a high-priority task passes a backlog.

```bash
uv run python manage.py demo priority
```

The command queues five slow low-priority tasks, then one high-priority
task. The completion order shows the high-priority task in first place.

## 5. Explore the admin

```bash
uv run python manage.py createsuperuser
uv run python manage.py runserver
```

Open http://127.0.0.1:8000/admin/. The Qraft section shows tasks with
attempt history and usage, workflows with status and counters, and the
"Requeue selected dead tasks" action on the task list.

## 6. Run the test suite

The unit tests do not need the cluster or PostgreSQL:

```bash
cd ..
uv run pytest
```

The suite completes in under two seconds. To run the `django.tasks` backend
tests on Django 6:

```bash
uv run --with "django>=6.0,<6.1" --with django-q2 --with pydantic-settings \
  --with pytest-django pytest tests/test_backend.py
```

## Troubleshooting

- A scenario hangs in `pending`: the cluster is not running, or it runs
  against a different database. Make sure `POSTGRES_DB` is set in both
  terminals.
- Stale results from earlier runs make output confusing: stop the cluster,
  delete the rows (`Schedule`, `OrmQ`, `Task`, `QraftTask`), and start again.
- Code changes do not take effect: restart the cluster. Worker processes
  load the code one time at start.
