# Configuration

This guide covers all Django-Qraft configuration options, Django integration, environment variables, and the ALT_CLUSTERS pattern for mixed workloads.

## Table of Contents

- [Basic Setup](#basic-setup)
- [Core Settings](#core-settings)
- [Threading Settings](#threading-settings)
- [Retry Defaults](#retry-defaults)
- [Hook Settings](#hook-settings)
- [Observability Settings](#observability-settings)
- [Durability Settings](#durability-settings)
- [Retention Settings](#retention-settings)
- [ALT_CLUSTERS Pattern](#alt_clusters-pattern)
- [Environment Variables](#environment-variables)
- [Django-Q2 Compatibility](#django-q2-compatibility)
- [Settings Hierarchy](#settings-hierarchy)
- [Configuration Examples](#configuration-examples)

## Basic Setup

Django-Qraft reads `QRAFT_CLUSTER` in your Django settings. It never falls back to
`Q_CLUSTER` for its own settings — see [Django-Q2 Compatibility](#django-q2-compatibility)
for the two places Qraft does read `Q_CLUSTER`.

```python
# settings.py
QRAFT_CLUSTER = {
    # Standard Django-Q2 settings
    "name": "default",
    "workers": 4,
    "timeout": 60,
    "retry": 90,  # Broker-level retry timeout
    "orm": "default",  # Use Django ORM as broker

    # Qraft-specific settings
    "threads": 1,
    "sync_hooks": False,
    "retry_defaults": {
        "max_attempts": 3,
        "delay": 30.0,
        "backoff": "exponential",
        "jitter": True,
    },
}
```

**Minimal configuration:**

```python
QRAFT_CLUSTER = {
    "workers": 4,
    "timeout": 60,
    "orm": "default",
}
```

All Qraft-specific settings have sensible defaults and are optional.

## Core Settings

Settings inherited from Django-Q2 (full compatibility maintained):

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `name` | `str` | `"default"` | Cluster name for identification |
| `workers` | `int` | `4` | Number of worker processes to spawn |
| `timeout` | `int` | `60` | Task timeout in seconds (process-level) |
| `retry` | `int` | `90` | Broker retry timeout in seconds |
| `orm` | `str` | `"default"` | Database alias for ORM broker |
| `redis` | `dict` | `None` | Redis connection settings |
| `sqs` | `dict` | `None` | AWS SQS settings |
| `recycle` | `int` | `500` | Recycle worker after N tasks |
| `compress` | `bool` | `False` | Compress task payloads |
| `save_limit` | `int` | `250` | Limit saved task results |
| `sync` | `bool` | `False` | Run synchronously (for testing) |
| `ack_failures` | `bool` | `True` | Acknowledge failed tasks |
| `max_attempts` | `int` | `0` | Django-Q2 retry attempts (deprecated, use Qraft retry) |
| `poll` | `int` | `200` | Broker polling interval (ms) |

**Note**: All Django-Q2 settings are fully supported. See [Django-Q2 documentation](https://django-q2.readthedocs.io/) for complete reference.

## Threading Settings

Control multithreaded worker behavior:

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `threads` | `int` | `1` | Threads per worker process. Set to `1` to disable threading (uses standard Django-Q2 workers). Set to `>1` to enable threaded workers. |
| `max_inflight` | `int` | `threads * 2` | Maximum concurrent tasks per worker process. Controls backpressure when thread pool is saturated. |
| `grace_period` | `float` | `30.0` | Seconds to wait for in-flight threads to complete during graceful shutdown. |

**Example configurations:**

```python
# Standard workers (no threading)
QRAFT_CLUSTER = {
    "workers": 4,
    "threads": 1,  # Default
}

# Threaded workers for I/O-bound tasks
QRAFT_CLUSTER = {
    "workers": 2,
    "threads": 8,  # 2 * 8 = 16 concurrent tasks
    "max_inflight": 16,  # Limit concurrent tasks per worker
    "grace_period": 45.0,  # Wait longer for cleanup
}
```

**Guidelines:**

- **CPU-bound tasks**: Use `threads=1` (standard workers)
- **I/O-bound tasks**: Set `threads` to 4-16 based on workload
- **max_inflight**: Start with `threads * 2`, adjust based on monitoring
- **grace_period**: Increase if tasks need longer cleanup time

See [Threading Guide](threading.md) for detailed information.

## Retry Defaults

Default retry policy for all tasks (can be overridden per-task):

```python
QRAFT_CLUSTER = {
    "retry_defaults": {
        "max_attempts": 3,      # Maximum retry attempts (1-10)
        "delay": 30.0,          # Initial retry delay in seconds
        "backoff": "exponential",  # Backoff strategy
        "jitter": True,         # Add randomization to delays
        "jitter_max": 0.2,      # Max jitter as fraction (0.0-1.0)
    },
}
```

### Retry Settings Reference

| Setting | Type | Default | Range | Description |
|---------|------|---------|-------|-------------|
| `max_attempts` | `int` | `3` | `1-10` | Maximum retry attempts including initial attempt |
| `delay` | `float` | `30.0` | `≥0` | Base delay in seconds between retries |
| `backoff` | `str` | `"exponential"` | See below | Backoff strategy |
| `jitter` | `bool` | `True` | - | Enable random jitter to prevent thundering herd |
| `jitter_max` | `float` | `0.2` | `0.0-1.0` | Maximum jitter as fraction of delay |

### Backoff Strategies

| Strategy | Formula | Example (delay=30, attempts 1-4) |
|----------|---------|----------------------------------|
| `"fixed"` | `delay` | 30s, 30s, 30s, 30s |
| `"linear"` | `delay * attempt` | 30s, 60s, 90s, 120s |
| `"exponential"` | `delay * 2^(attempt-1)` | 30s, 60s, 120s, 240s |

**With jitter** (jitter_max=0.2): Each delay is randomized by ±20%

**Examples:**

```python
# Conservative retry (long delays)
"retry_defaults": {
    "max_attempts": 5,
    "delay": 60.0,
    "backoff": "exponential",
}

# Aggressive retry (quick retries)
"retry_defaults": {
    "max_attempts": 3,
    "delay": 5.0,
    "backoff": "linear",
}

# Fixed interval retry
"retry_defaults": {
    "max_attempts": 3,
    "delay": 30.0,
    "backoff": "fixed",
    "jitter": False,  # No randomization
}
```

See [Retry Policies Guide](retry.md) for detailed information.

## Hook Settings

Control hook execution behavior:

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `sync_hooks` | `bool` | `False` | Run hooks synchronously in monitor (legacy behavior) vs asynchronously over workers (default) |

**Async hooks (default - recommended):**

```python
QRAFT_CLUSTER = {
    "sync_hooks": False,  # Default
}
```

Hooks are queued as async tasks over workers, preventing the monitor from becoming a bottleneck. Creates `HookDispatch` records for idempotency tracking.

**Synchronous hooks (legacy):**

```python
QRAFT_CLUSTER = {
    "sync_hooks": True,
}
```

Hooks execute synchronously in the monitor process after task completion. Use for:
- Debugging hook execution
- Hooks that must complete before monitor acknowledges task
- Maintaining exact Django-Q2 behavior

See [Hooks Guide](hooks.md) for detailed information.

## Observability Settings

Metrics, progress coalescing and log context. All three are off or inert by default.

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `metrics_sink` | `str` | `"qraft.metrics.NullSink"` | Dotted path to the sink class; resolved once per process |
| `metrics_gauges` | `bool` | `False` | Whether this cluster emits the backlog gauges |
| `progress_min_interval` | `float` | `0.0` | Seconds between progress writes from one attempt; `0` writes on every call |

### Metrics sink

Qraft emits counters, histograms and gauges at its own transition points. The sink is a
class with three methods:

```python
class Sink:
    def counter(self, name, value=1, **labels): ...
    def histogram(self, name, value, **labels): ...
    def gauge(self, name, value, **labels): ...
```

The bundled OpenTelemetry sink instruments through `opentelemetry-api` only:

```bash
pip install 'django-qraft[otel]'
```

```python
QRAFT_CLUSTER = {
    "metrics_sink": "qraft.metrics.otel.OpenTelemetrySink",
}
```

Qraft creates instruments on a meter named `qraft` and records to them. The host
application configures the SDK, the exporter, the resource and the process lifecycle —
that is the boundary OpenTelemetry draws between a library and an application, and it
means a host that has not configured an SDK gets a no-op meter rather than an error.

What is emitted:

| Metric | Type | Labels |
|---|---|---|
| `qraft.attempt.started` | counter | `func`, `cluster` |
| `qraft.attempt.pickup` | histogram (s) | `func`, `cluster` |
| `qraft.attempt.finished` | counter | `func`, `cluster`, `outcome`, `exception_class` |
| `qraft.attempt.duration` | histogram (s) | `func`, `cluster`, `outcome` |
| `qraft.attempt.stall_suspected` | counter | `func`, `cluster` |
| `qraft.attempt.redelivered` | counter | `func`, `cluster` |
| `qraft.scheduler.lag` | histogram (s) | `cluster` |
| `qraft.retry.scheduled` | counter | `func`, `exception_class` |
| `qraft.reaper.action` | counter | `action` |
| `qraft.workflow.settled` | counter | `workflow_type`, `outcome` |
| `qraft.graph.settled` | counter | `subject_type`, `kind`, `outcome` |
| `qraft.graph.duration` | histogram (s) | `subject_type`, `kind`, `outcome` |
| `qraft.graph.report_to_ready` | histogram (s) | `subject_type`, `kind` |
| `qraft.node.settled` | counter | `kind`, `key`, `outcome` |
| `qraft.node.duration` | histogram (s) | `kind`, `key`, `outcome` |
| `qraft.queue.depth` | gauge | `cluster` |
| `qraft.queue.oldest_ready_age` | gauge (s) | `cluster` |
| `qraft.attempt.active` | gauge | `cluster` |
| `qraft.scheduler.overdue` | gauge | |
| `qraft.attempt.unrouted_age_max` | gauge (s) | |
| `qraft.graph.open_age_max` | gauge (s) | `subject_type` |

`outcome` is `succeeded`, `failed` or `orphaned`, so a fleet whose failures are detected
by the reaper does not look faster than one whose failures return. Pickup is recorded
when the attempt starts, not when it finishes: an attempt that is hung right now is
already in the pickup histogram, and its absence from the duration histogram is itself
the signal.

No subject id, graph id, task id, generation, revision or metadata is ever a label. Subject ids are
unbounded; metric cardinality is not. An operator who wants one worksheet's timeline uses
the dashboard's subject filter, not a metric.

A sink that raises is caught: the exception is logged at most once per minute per process
and counted in a health counter that `api/state/` exposes as `metrics_health` and the
dashboard shows as a pill. The sink is never disabled — a transient exporter failure must
not silence a process until restart.

### One gauge owner

Every cluster runs a dispatcher and a reaper. A gauge emitted by all of them lets a
consumer sum the same backlog once per replica, so gauges are emitted only by clusters
started with `metrics_gauges=True`, from the dispatcher loop once per pass. **Set it on
exactly one cluster.**

```python
QRAFT_CLUSTER = {
    "metrics_sink": "qraft.metrics.otel.OpenTelemetrySink",
    "metrics_gauges": True,
    "ALT_CLUSTERS": {
        "io-workers": {"threads": 8},   # inherits the sink, not the gauge flag
    },
}
```

`metrics_gauges` is the one key an `ALT_CLUSTERS` entry does not inherit: an entry that
does not name it is not a gauge owner, whatever the base entry says. Ownership names one
process, and inheriting it would make every alt cluster a second reporter of the same
numbers.

The two queue gauges read `OrmQ` rows, so they need the ORM broker. A gauge owner running
any other broker logs one warning at startup and emits the remaining gauges, which read
Qraft's own tables.

### Overdue graphs

`QRAFT_GRAPH_OVERDUE_AFTER` is a plain Django setting, not a `QRAFT_CLUSTER` key, because
it describes application work rather than cluster behaviour:

```python
QRAFT_GRAPH_OVERDUE_AFTER = 3600   # seconds; None (the default) turns the sweep off
```

With it set, the reaper thread flags every `RUNNING` graph whose `date_started` is older
than the threshold: `overdue_flagged_at` is set once by compare-and-swap, the
`graph_overdue` signal fires, and the dashboard shows a badge. Nothing is failed
automatically — `graphs.skip` and `graphs.cancel` are the tools for deciding what a
stuck graph deserves. `qraft.graph.open_age_max` reports the oldest running graph per
subject type. A graph parked at an approval gate is not swept: it is waiting for a
person, not overdue.

### Pricing

`QRAFT_PRICING` is a plain Django setting, like `QRAFT_GRAPH_OVERDUE_AFTER`, and turning
tokens into money is entirely optional:

```python
QRAFT_PRICING = {
    "resolver": "qraft.pricing.StaticTablePricing",   # the default when the dict exists
    "currency": "USD",
    "revision": "2026-09",
    "models": {
        "gpt-5.4-nano": {"input": 0.05, "output": 0.40, "cached_input": 0.005},
    },
}
```

Rates are per million tokens. Without the setting there is no resolver and every cost
path answers "unknown" rather than guessing. The resolver is resolved once per process;
`qraft.pricing.reset_resolver()` re-reads the setting. See
[AI Workloads](ai-workloads.md#cost-from-usage) for the per-increment entries, the
cached-token convention, and what `coverage` and `estimated` mean.

### Log context

`qraft.logging.QraftContextFilter` attaches the executing attempt's ids to every log
record, so a host's formatter can print or ship them:

```python
LOGGING = {
    "version": 1,
    "filters": {"qraft": {"()": "qraft.logging.QraftContextFilter"}},
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "filters": ["qraft"],
            "formatter": "qraft",
        },
    },
    "formatters": {
        "qraft": {
            "format": "%(asctime)s %(levelname)s %(qraft_task_id)s "
                      "%(qraft_attempt_number)s %(qraft_subject_type)s "
                      "%(qraft_subject_id)s %(message)s",
        },
    },
}
```

The attributes are `qraft_task_id`, `qraft_attempt_id`, `qraft_attempt_number`,
`qraft_graph_id`, `qraft_node_key`, `qraft_generation`, `qraft_subject_type` and
`qraft_subject_id`. Outside a task they are present and empty, so a format string that
names them never raises.

### Trace propagation

With `opentelemetry-api` importable and a span current at enqueue, `async_task()` injects
a W3C `traceparent` onto the attempt row; the dispatcher copies it onto retry attempts.
At `pre_execute` the lease starts a child span named after `func` for the attempt's
duration, so a trace begun in the web request that enqueued the first stage continues
into the worker and across retries. Hooks receive the same `traceparent`, but only
through `hook_context` — a hook's keyword arguments are the caller's and cannot safely
gain a key. Without the package the column stays null and nothing else changes.

## Durability Settings

These govern the execution lease, the orphan reaper and the redelivery guard — how Qraft
decides that a running attempt is dead, and what it does about a delivery it has already
seen.

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `heartbeat_interval` | `float` | `30.0` | Seconds between lease heartbeats from a running worker |
| `min_heartbeat_grace` | `float` | `90.0` | Floor on the grace period before a stale heartbeat is reaped |
| `reap_interval` | `float` | `60.0` | Seconds between reaper sweeps |
| `reap_stale_after` | `float` | `3600.0` | Seconds an attempt that never started may sit unresolved before it counts as orphaned |
| `max_executions_per_attempt` | `int` | `1` | How many times one attempt may be handed to a worker |

### The heartbeat grace period

The reaper reclaims an attempt whose heartbeat is older than
`max(3 * heartbeat_interval, min_heartbeat_grace)`. The floor exists so a short
`heartbeat_interval` cannot make the reaper trigger-happy on a briefly paused worker.
Lower it when your stages are short enough that 90 seconds of lost work per crash costs
more than a rare false reap:

```python
QRAFT_CLUSTER = {
    "heartbeat_interval": 5.0,
    "min_heartbeat_grace": 20.0,
}
```

### The redelivery guard

Django-Q2 redelivers a message it never got an acknowledgement for, so a monitor crash
used to re-run a task whose attempt row was still unresolved — and each re-run refreshed
the heartbeat, so the reaper read it as alive and the graph stayed running until somebody
cancelled it by hand.

Every attempt Qraft enqueues runs through `qraft.runner.run_task`, which claims the
delivery by compare-and-swap on `QraftTaskAttempt.execution_count` before calling
anything. With the default `max_executions_per_attempt` of 1, a second delivery of an
attempt that already started neither calls the function nor refreshes the lease. It
counts `qraft.attempt.redelivered` and raises `qraft.runner.RedeliveredAttempt`, so the
attempt resolves through the normal failure path and the retry policy decides attempt
N+1 — the retry policy, not the broker's delivery loop.

Raise it only for a target that is genuinely idempotent and cheap to repeat.

## Retention Settings

Django-Q2 caps its own `Task` table with `save_limit`. Qraft's tables have no
such cap, so `QraftTask`, `QraftTaskAttempt`, `HookDispatch` and the workflow
tables grow without bound until a retention window is set.

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `retention_days` | `float \| None` | `None` | Age bound: days to keep settled rows |
| `retention_max_tasks` | `int \| None` | `None` | Count bound: keep only the newest N settled `QraftTask` rows |
| `retention_interval` | `float` | `3600.0` | Seconds between sweeps |
| `retention_batch_size` | `int` | `500` | Rows deleted per transaction |

```python
QRAFT_CLUSTER = {
    "retention_days": 30,
}
```

Both bounds are ceilings on how much history is kept, so a row goes as soon
as either says so. When both are set, the stricter one decides.

The sweep runs as a daemon thread beside the orphan reaper, in the monitor
process, and only starts when a bound is in force. It deletes only rows
that have settled: a task that is still `PENDING` or `RUNNING` is kept
however old it is, and a task belonging to a workflow that has not finished
is kept until that workflow does. Deletions are batched so the first sweep
over a large table cannot hold one long transaction.

### Inheriting `save_limit`

Django-Q2's `save_limit` is count-based; Qraft's `retention_days` is
age-based. Rather than convert one into the other, Qraft mirrors the intent
directly onto its own count bound, `retention_max_tasks`.

The resolution rule, in order:

1. `retention_days` or `retention_max_tasks` in `QRAFT_CLUSTER` — used as
   written. Nothing is inherited.
2. Otherwise, if `Q_CLUSTER` **contains the key** `save_limit` with a value
   above `0`, that value becomes `retention_max_tasks`.
3. Otherwise retention stays off and history is kept forever.

Step 2 reads the raw `Q_CLUSTER` dict, not `Conf.SAVE_LIMIT`. `Conf.SAVE_LIMIT`
is `250` even for users who never mentioned it, and mirroring that default
would delete almost all task history without anyone asking for it. Only a key
that is actually present counts as intent.

Two `save_limit` values carry no count to mirror and so inherit nothing:
`0`, which is Django-Q2's "keep everything", and any negative value, which
tells Django-Q2 to save no successful results at all.

Qraft forces `save=True` on every task it enqueues, so a successful run
always writes a Django-Q2 `Task` row for the hook handler. Combined with
`SAVE_LIMIT < 0`, that means django_q's own trimming of successful results
is effectively disabled — size retention with `retention_days` /
`retention_max_tasks` (or a positive `save_limit`) accordingly.

`save_limit_per` (`group`/`name`/`func`) is not mirrored. Qraft rows have no
equivalent grouping, so the inherited bound is always global.

The resolved policy is logged once at cluster start, under the `qraft` logger:

```
Qraft retention: pruning settled rows beyond the newest 1000 task(s)
[inherited from Q_CLUSTER['save_limit']], every 3600.0s
```

To sweep on demand instead — from a shell, or from your own scheduled job:

```python
from qraft.retention import sweep_retention

sweep_retention(retention_days=30)   # -> {"QraftTask": 1420, ...}
sweep_retention(max_tasks=1000)
```

## ALT_CLUSTERS Pattern

Run multiple clusters with different configurations for mixed workloads (CPU-bound + I/O-bound).

### Configuration

```python
QRAFT_CLUSTER = {
    # Default cluster configuration
    "name": "default",
    "workers": 4,
    "threads": 1,  # Standard workers for CPU-bound tasks
    "timeout": 300,

    # Alternative cluster configurations
    "ALT_CLUSTERS": {
        "io-workers": {
            "workers": 2,
            "threads": 8,           # Threaded workers
            "max_inflight": 16,
            "timeout": 60,
        },
        "cpu-intensive": {
            "workers": 8,
            "threads": 1,           # More processes for CPU
            "timeout": 600,         # Longer timeout
        },
        "quick-tasks": {
            "workers": 2,
            "threads": 4,
            "timeout": 30,
        },
    },
}
```

### Starting Alternative Clusters

**Option 1: Command-line flag**

```bash
# Default cluster
python manage.py qraftcluster

# Alternative cluster
python manage.py qraftcluster --name io-workers
python manage.py qraftcluster --name cpu-intensive
```

**Option 2: Environment variable**

```bash
# Default cluster
python manage.py qraftcluster

# Alternative cluster
Q_CLUSTER_NAME=io-workers python manage.py qraftcluster
Q_CLUSTER_NAME=cpu-intensive python manage.py qraftcluster
```

The two forms are equivalent. `--name` re-executes the process with
`Q_CLUSTER_NAME` set, because Django-Q2 builds its own `Conf` at import time —
before any management command runs — and a variable set later would give the
cluster Qraft's threading config while it still drained the *default* queue.

Declare each alternative cluster in **`Q_CLUSTER`** as well, even if the entry
is empty. Django-Q2 reads `ALT_CLUSTERS` from `Q_CLUSTER` to switch queues, and
raises `KeyError` on import when the key is absent:

```python
Q_CLUSTER = {
    "name": "default",
    "orm": "default",
    "ALT_CLUSTERS": {"io-workers": {}, "cpu-intensive": {}},
}
```

`--name` validates the name against both dicts and fails with a clear message
before re-executing, so a typo cannot start a cluster on the wrong queue.

### Routing Tasks to Clusters

```python
from qraft.tasks import async_task

# Route to default cluster (implicit)
async_task('myapp.tasks.cpu_bound_task', data)

# Route to specific cluster
async_task('myapp.tasks.io_api_call', url, cluster='io-workers')
async_task('myapp.tasks.heavy_compute', matrix, cluster='cpu-intensive')
async_task('myapp.tasks.quick_update', id, cluster='quick-tasks')
```

### Use Cases

**1. CPU vs I/O workloads:**

```python
"ALT_CLUSTERS": {
    "io-workers": {"threads": 8},      # High concurrency for I/O
    "cpu-workers": {"workers": 16},    # Many processes for CPU
}
```

**2. Different timeout requirements:**

```python
"ALT_CLUSTERS": {
    "long-running": {"timeout": 3600},   # 1 hour
    "quick-tasks": {"timeout": 30},      # 30 seconds
}
```

**3. Different brokers:**

```python
QRAFT_CLUSTER = {
    "orm": "default",  # Default uses ORM
    "ALT_CLUSTERS": {
        "redis-queue": {
            "redis": {"host": "localhost", "port": 6379},
        },
    },
}
```

### Configuration Merging

Alternative cluster configs are merged with the base config:

```python
QRAFT_CLUSTER = {
    "workers": 4,
    "timeout": 60,
    "orm": "default",

    "ALT_CLUSTERS": {
        "custom": {
            "workers": 8,  # Overrides base
            # timeout=60 inherited from base
            # orm="default" inherited from base
        }
    }
}
```

## Environment Variables

| Variable | Type | Description | Example |
|----------|------|-------------|---------|
| `Q_CLUSTER_NAME` | `str` | Select alternative cluster configuration | `Q_CLUSTER_NAME=io-workers` |

The `Q_CLUSTER_NAME` variable is checked at cluster startup to select the appropriate configuration from `ALT_CLUSTERS`.

**Usage in deployment:**

```bash
# Supervisor/systemd service for different clusters
[program:qraft-default]
command=python manage.py qraftcluster

[program:qraft-io]
command=python manage.py qraftcluster
environment=Q_CLUSTER_NAME=io-workers

[program:qraft-cpu]
command=python manage.py qraftcluster
environment=Q_CLUSTER_NAME=cpu-intensive
```

## Django-Q2 Compatibility

Django-Qraft maintains full backward compatibility with Django-Q2:

### Q_CLUSTER still belongs to Django-Q2

`Q_CLUSTER` keeps configuring Django-Q2 itself. Qraft does not copy it and emits no
deprecation warning; a setting you want Qraft to honour goes in `QRAFT_CLUSTER`. Qraft
reads `Q_CLUSTER` for exactly two things:

- **Broker resolution.** `qraft.brokers.broker_for_cluster()` builds a named cluster's
  broker from that cluster's own `Q_CLUSTER` entry merged over the base.
- **Retention inheritance.** An explicit `Q_CLUSTER["save_limit"]` says task history
  should be bounded, so Qraft mirrors it as `retention_max_tasks` when neither
  `retention_days` nor `retention_max_tasks` is set in `QRAFT_CLUSTER`. The read-only
  `retention_inherited_from_save_limit` flag records that this happened. Django-Q2's own
  default of 250 is not inherited — only a value you wrote yourself.

### Using qcluster Command

The standard Django-Q2 command still works:

```bash
# Django-Q2 behavior (no Qraft features)
python manage.py qcluster

# Qraft behavior (enhanced features)
python manage.py qraftcluster
```

### Broker Support

Every Django-Q2 broker will run Qraft tasks, but only one configuration
carries Qraft's full guarantees: **the ORM broker on PostgreSQL**. The
durable truth is the database row — `QraftTask` and `QraftTaskAttempt` — and
the broker is only a delivery hint. Features that need the broker and the
task rows to be in the same database degrade on every other broker.

**Supported configuration:**

```python
QRAFT_CLUSTER = {
    "orm": "default",
    "broker_class": "qraft.brokers.QraftOrmBroker",  # required for priority lanes
}
```

PostgreSQL specifically, because the throttle, the reaper and the workflow
dispatchers all coordinate through `SELECT ... FOR UPDATE`.

**Degradation is per cluster.** Qraft resolves a broker from the *target*
cluster's own merged `Q_CLUSTER` entry (`qraft.brokers.broker_for_cluster()`),
so a project may run one cluster on the ORM broker while the rest stay on
Redis. Read the table below against the cluster the task runs on, not against
the process that enqueued it: a task enqueued from a Redis-configured web
process onto an ORM-broker cluster gets the ORM broker's guarantees, and the
cluster's startup warnings report that cluster's own broker.

**What degrades on a broker other than the ORM broker** (Redis, SQS, IronMQ,
MongoDB):

| Feature | Behaviour |
|---------|-----------|
| Delivery receipts | Lost. `Broker.acknowledge`/`fail` are no-ops, so a task in flight when a worker dies is never redelivered. The cluster warns about this at startup. |
| Priority lanes | Lost. Lanes are extra `OrmQ` keys; `priority_list_key()` declines to route and the task runs in the default lane with a warning. |
| Reaper | Half. Attempts with a stale heartbeat are still reclaimed, but attempts that never started cannot be checked against the queue, so the reaper leaves them alone rather than risk duplicating a task that is merely waiting. |
| Enqueue visibility | Racy. The ORM broker enqueues inside the caller's transaction, so a task and the rows describing it commit together. Any other broker makes the task visible to a worker before the transaction commits. |
| Start observability | Lost for the tasks that lose the enqueue race above. A worker that picks a task up before its attempt row commits finds nothing to open a lease on, and nothing retries the lookup: that attempt gets no `date_started`, no heartbeat, no `task_started`, no pickup measurement and no span. It still runs, and its completion still resolves the attempt normally. The marker fallback cannot cover it — the marker resolves through the same uncommitted rows. |
| Queue gauges | Lost. `qraft.queue.depth` and `qraft.queue.oldest_ready_age` read `OrmQ` rows. The gauge owner warns once at startup and emits the rest, which read Qraft's own tables. |

Retries, hooks, workflows and the DLQ work on any broker: they run off the
task rows, not the queue.

### Mixed brokers across clusters

Every Qraft enqueue names its broker explicitly, built from the target
cluster's merged `Q_CLUSTER` entry. Django-Q2 on its own would resolve the
broker from the *enqueuing* process's config
(`broker = task.pop("broker") or get_broker(task["cluster"])`), which is what
sends a task meant for an ORM-broker cluster into whatever the web process
happens to run.

```python
Q_CLUSTER = {
    "name": "default",
    # Only needed for enqueues Qraft does not make itself - a plain
    # django_q.tasks.async_task(..., cluster="revenue") from application code.
    "broker_class": "qraft.brokers.RoutingBroker",
    "redis": {"host": "127.0.0.1", "port": 6379},
    "timeout": 300,
    "retry": 360,
    "ALT_CLUSTERS": {
        # Stays on Redis, inherited from the base entry.
        "reports": {"workers": 4},
        # Runs on the ORM broker, with priority lanes.
        "revenue": {
            "orm": "default",
            "broker_class": "qraft.brokers.QraftOrmBroker",
            "workers": 2,
        },
    },
}
```

With that config:

- `qraft.async_task(..., qraft_options={"cluster": "revenue"})` from the web
  process enqueues an `OrmQ` row under the key `revenue`, inside the caller's
  transaction, whatever the web process's own broker is.
- `qraft.async_task(...)` with no cluster keeps using Redis.
- `django_q.tasks.async_task(..., cluster="revenue")` — code Qraft does not
  own — reaches the same ORM queue, because `RoutingBroker` forwards every
  call to `broker_for_cluster("revenue")`.
- `python manage.py qraftcluster --name revenue` re-execs with
  `Q_CLUSTER_NAME=revenue`, so Django-Q2's own `Conf` carries the merged entry
  (`Conf.ORM == "default"`) and the cluster drains the ORM queue it was
  configured for.

Two things stay fleet-wide rather than per cluster, and a mixed deployment has
to keep them that way:

- **Hook tasks follow the cluster that dispatches them, not the one that ran
  the task.** A hook is queued with `cluster=executing_cluster()` so it lands
  where a worker is known to be running (`cluster=None` would send it to the
  default cluster, which may not be up). Normally that is the same cluster,
  because the monitor that resolves a completion belongs to it — but every
  cluster runs a reaper, and `reaper.replay_unrouted()` sweeps the whole
  table, so a resolution whose routing was lost can be replayed, and its hook
  queued, by a different cluster. Hook functions must therefore be importable
  by every cluster's workers.
- **Retries and DLQ requeues do not.** They inherit `QraftTaskAttempt.cluster`,
  so an attempt always retries on the cluster that ran it. That is why
  `async_task(broker=...)` requires `cluster=` as well: the broker routes
  attempt 1, the recorded cluster routes everything after it.

Per-cluster connection settings are read when the broker is first built and
cached per process; `qraft.brokers.reset_broker_cache()` drops the cache, and
a `Q_CLUSTER`/`QRAFT_CLUSTER` change under `override_settings` does it
automatically. The ORM broker's database alias is pinned onto the instance,
because `ORM.get_connection()` re-reads `Conf.ORM` on every call.

**Public API:**

| Name | Meaning |
|------|---------|
| `broker_for_cluster(cluster=None, priority=None)` | The broker instance that enqueues onto that cluster's queue, in that priority lane. `cluster=None` means this process's own. |
| `cluster_broker_config(cluster=None)` | The merged `Q_CLUSTER` dict that cluster runs under. |
| `RoutingBroker` | A `broker_class` that delegates every call to `broker_for_cluster(self.list_key)`. |
| `delivering_broker(broker=None)` | Unwraps a `RoutingBroker` to the broker that actually moves messages, for capability checks. |
| `reset_broker_cache()` | Drops every resolved broker. |

## Settings Hierarchy

Configuration is loaded in the following order (later sources override earlier):

1. **Pydantic defaults** - Built-in defaults from `qraft/conf.py`
2. **Django settings** - `QRAFT_CLUSTER` (or `Q_CLUSTER` as fallback)
3. **Environment variables** - `Q_CLUSTER_NAME` selects ALT_CLUSTERS entry
4. **Runtime overrides** - Via `get_conf()` function

### Implementation

Django-Qraft uses Pydantic Settings with a custom Django settings source:

```python
from qraft.conf import conf, get_conf

# Global instance (loaded at import)
print(conf.threads)  # Access settings

# Runtime instance (respects current Q_CLUSTER_NAME)
current_conf = get_conf()
print(current_conf.threads)
```

### Validation

All settings are validated by Pydantic:

```python
QRAFT_CLUSTER = {
    "threads": -1,  # Error: must be >= 1
    "max_attempts": 20,  # Error: must be <= 10
    "backoff": "invalid",  # Error: not a valid backoff strategy
}
```

Validation errors are raised at Django startup with clear messages.

## Configuration Examples

### Minimal Setup (ORM Broker)

```python
QRAFT_CLUSTER = {
    "workers": 4,
    "timeout": 60,
    "orm": "default",
}
```

### Production Setup (ORM Broker on PostgreSQL)

```python
QRAFT_CLUSTER = {
    "name": "production",
    "workers": 8,
    "timeout": 120,
    "recycle": 1000,
    "retry": 90,
    "save_limit": 500,

    "orm": "default",
    "broker_class": "qraft.brokers.QraftOrmBroker",

    "threads": 1,
    "retention_days": 30,
    "retry_defaults": {
        "max_attempts": 5,
        "delay": 60.0,
        "backoff": "exponential",
    },
}
```

### I/O-Optimized Setup

```python
QRAFT_CLUSTER = {
    "workers": 4,
    "threads": 8,  # High concurrency
    "max_inflight": 20,
    "grace_period": 60.0,
    "timeout": 60,
    "orm": "default",
}
```

### Mixed Workload Setup

```python
QRAFT_CLUSTER = {
    # Default: balanced
    "name": "default",
    "workers": 4,
    "threads": 2,
    "timeout": 120,
    "orm": "default",

    "ALT_CLUSTERS": {
        # I/O-heavy workloads
        "io-bound": {
            "workers": 2,
            "threads": 16,
            "max_inflight": 32,
            "timeout": 60,
        },

        # CPU-heavy workloads
        "cpu-bound": {
            "workers": 16,
            "threads": 1,
            "timeout": 600,
        },

        # Quick background tasks
        "background": {
            "workers": 2,
            "threads": 4,
            "timeout": 30,
        },
    },

    "retry_defaults": {
        "max_attempts": 3,
        "delay": 30.0,
        "backoff": "exponential",
        "jitter": True,
    },
}
```

### Development/Testing Setup

```python
QRAFT_CLUSTER = {
    "workers": 1,
    "threads": 1,
    "timeout": 30,
    "orm": "default",
    "save_limit": 50,
    "sync": True,  # Synchronous execution for testing
    "sync_hooks": True,  # Synchronous hooks for debugging
}
```

## Database Connection Sizing

For threaded workers, ensure your database connection pool is sized appropriately:

```python
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'mydb',
        'CONN_MAX_AGE': 60,  # Keep connections alive
        'OPTIONS': {
            # workers * threads + buffer for web requests
            'MAX_CONNS': 100,  # e.g., 4 workers * 8 threads = 32 + 68 buffer
        }
    }
}
```

**Formula**: `MAX_CONNS >= (workers * threads) + web_workers + buffer`

## Best Practices

1. **Start conservative**: Begin with `threads=1` and measure performance before increasing
2. **Monitor resource usage**: Watch CPU, memory, and database connections
3. **Use ALT_CLUSTERS**: Separate workloads by characteristics (I/O vs CPU)
4. **Set appropriate timeouts**: Match timeout to longest expected task duration
5. **Configure retry wisely**: Balance retry attempts with delay to avoid overwhelming systems
6. **Enable async hooks**: Use default `sync_hooks=False` for better performance
7. **Size connection pool**: For threaded workers, ensure adequate database connections
8. **Test with run-once**: Use `--run-once` flag for testing configuration changes

## Troubleshooting

### Configuration not loading

**Check Django settings:**

```python
# Ensure INSTALLED_APPS includes both:
INSTALLED_APPS = [
    'django_q',
    'qraft',
]
```

**Check setting name:**

```python
# Correct
QRAFT_CLUSTER = {...}

# Wrong (but works with warning)
Q_CLUSTER = {...}
```

### Alternative cluster not found

**Check Q_CLUSTER_NAME:**

```bash
# Must match key in ALT_CLUSTERS
Q_CLUSTER_NAME=io-workers  # Must exist in ALT_CLUSTERS
```

**Check ALT_CLUSTERS structure:**

```python
QRAFT_CLUSTER = {
    "ALT_CLUSTERS": {
        "io-workers": {...},  # Must be a dict
    }
}
```

### Validation errors

**Check value ranges:**

- `threads`: Must be >= 1
- `max_attempts`: Must be 1-10
- `jitter_max`: Must be 0.0-1.0
- `backoff`: Must be "fixed", "linear", or "exponential"

**Check types:**

```python
QRAFT_CLUSTER = {
    "workers": 4,       # int, not "4"
    "timeout": 60,      # int, not "60"
    "threads": 1,       # int, not "1"
    "jitter": True,     # bool, not "true"
}
```

## Related Documentation

- [Getting Started](getting-started.md) - Installation and basic usage
- [Threading Guide](threading.md) - Multithreaded workers in detail
- [Hooks Guide](hooks.md) - Hook system configuration
- [Retry Policies Guide](retry.md) - Retry configuration
- [Architecture Guide](architecture.md) - How configuration is loaded
