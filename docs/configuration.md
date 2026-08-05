# Configuration

This guide covers all Django-Qraft configuration options, Django integration, environment variables, and the ALT_CLUSTERS pattern for mixed workloads.

## Table of Contents

- [Basic Setup](#basic-setup)
- [Core Settings](#core-settings)
- [Threading Settings](#threading-settings)
- [Retry Defaults](#retry-defaults)
- [Hook Settings](#hook-settings)
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

All Django-Q2 brokers are supported:

**ORM (Django database):**

```python
QRAFT_CLUSTER = {
    "orm": "default",  # Use Django ORM
}
```

**Redis:**

```python
QRAFT_CLUSTER = {
    "redis": {
        "host": "localhost",
        "port": 6379,
        "db": 0,
    }
}
```

**AWS SQS:**

```python
QRAFT_CLUSTER = {
    "sqs": {
        "aws_region": "us-east-1",
        "queue_name": "my-queue",
    }
}
```

**IronMQ, MongoDB, etc.:** All Django-Q2 brokers work unchanged.

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

### Production Setup (Redis Broker)

```python
QRAFT_CLUSTER = {
    "name": "production",
    "workers": 8,
    "timeout": 120,
    "recycle": 1000,
    "retry": 90,
    "save_limit": 500,

    "redis": {
        "host": "redis.example.com",
        "port": 6379,
        "db": 0,
        "password": "secret",
    },

    "threads": 1,
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
