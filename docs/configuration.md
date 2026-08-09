# Configuration

This guide covers all Django-Qraft configuration options, Django integration, environment variables, and the ALT_CLUSTERS pattern for mixed workloads.

## Table of Contents

- [Basic Setup](#basic-setup)
- [Core Settings](#core-settings)
- [Threading Settings](#threading-settings)
- [Retry Defaults](#retry-defaults)
- [Hook Settings](#hook-settings)
- [Retention Settings](#retention-settings)
- [ALT_CLUSTERS Pattern](#alt_clusters-pattern)
- [Environment Variables](#environment-variables)
- [Django-Q2 Compatibility](#django-q2-compatibility)
- [Settings Hierarchy](#settings-hierarchy)
- [Configuration Examples](#configuration-examples)

## Basic Setup

Django-Qraft uses `QRAFT_CLUSTER` in your Django settings (with fallback to `Q_CLUSTER` for compatibility):

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

### Using Q_CLUSTER (Deprecated)

```python
# Still works, but shows deprecation warning
Q_CLUSTER = {
    "workers": 4,
    "timeout": 60,
}
```

**Migration:**

```python
# Recommended: Rename to QRAFT_CLUSTER
QRAFT_CLUSTER = {
    "workers": 4,
    "timeout": 60,
}
```

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

**What degrades on a broker other than the ORM broker** (Redis, SQS, IronMQ,
MongoDB):

| Feature | Behaviour |
|---------|-----------|
| Delivery receipts | Lost. `Broker.acknowledge`/`fail` are no-ops, so a task in flight when a worker dies is never redelivered. The cluster warns about this at startup. |
| Priority lanes | Lost. Lanes are extra `OrmQ` keys; `priority_list_key()` declines to route and the task runs in the default lane with a warning. |
| Reaper | Half. Attempts with a stale heartbeat are still reclaimed, but attempts that never started cannot be checked against the queue, so the reaper leaves them alone rather than risk duplicating a task that is merely waiting. |
| Enqueue visibility | Racy. The ORM broker enqueues inside the caller's transaction, so a task and the rows describing it commit together. Any other broker makes the task visible to a worker before the transaction commits. |

Retries, hooks, workflows and the DLQ work on any broker: they run off the
task rows, not the queue.

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
