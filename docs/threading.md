# Multithreaded Workers

Django-Qraft can optionally run multiple threads within each worker process, enabling higher concurrency for I/O-bound workloads without spawning additional processes.

## Table of Contents

- [Overview](#overview)
- [When to Use Threading](#when-to-use-threading)
- [Configuration](#configuration)
- [How It Works](#how-it-works)
- [Performance Characteristics](#performance-characteristics)
- [Database Connections](#database-connections)
- [Timeout Behavior](#timeout-behavior)
- [Mixed Worker Pools](#mixed-worker-pools)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

## Overview

By default, Django-Qraft uses standard process-based workers (one task at a time per process), identical to Django-Q2. When you enable threading (`threads > 1`), each worker process maintains a thread pool that can execute multiple tasks concurrently.

**Architecture comparison:**

```
Standard Workers (threads=1):
├── Worker Process 1 → Task A
├── Worker Process 2 → Task B
└── Worker Process 3 → Task C
Total concurrency: 3 tasks

Threaded Workers (threads=8):
├── Worker Process 1
│   ├── Thread 1 → Task A
│   ├── Thread 2 → Task B
│   ├── ...
│   └── Thread 8 → Task H
├── Worker Process 2
│   ├── Thread 1 → Task I
│   ├── ...
│   └── Thread 8 → Task P
Total concurrency: 16 tasks
```

## When to Use Threading

### Threading is Beneficial For:

- **I/O-bound tasks**: API calls, file operations, database queries
- **High-latency operations**: External service integrations, web scraping
- **Network operations**: HTTP requests, email sending, S3 uploads
- **Memory-constrained environments**: More concurrency with fewer processes

**Example I/O-bound tasks:**

```python
def fetch_api_data(url):
    """Network I/O - benefits from threading"""
    response = requests.get(url)  # Waits for network
    return response.json()

def process_image(path):
    """File I/O - benefits from threading"""
    with open(path, 'rb') as f:  # Waits for disk
        image = Image.open(f)
        return image.resize((800, 600))

def send_email(to, subject, body):
    """Network I/O - benefits from threading"""
    smtp.sendmail(to, subject, body)  # Waits for SMTP
```

### Threading is NOT Recommended For:

- **CPU-bound tasks**: Image processing, data analysis, encryption
- **Pure computation**: Mathematical operations, parsing large datasets
- **Tasks requiring hard timeouts**: Thread-level timeouts are not enforced

**Example CPU-bound tasks:**

```python
def calculate_fibonacci(n):
    """CPU-bound - use standard workers"""
    if n <= 1:
        return n
    return calculate_fibonacci(n-1) + calculate_fibonacci(n-2)

def process_video(path):
    """CPU-bound - use standard workers"""
    video = cv2.VideoCapture(path)
    # Heavy computation, not I/O wait
    return process_frames(video)
```

**Why not CPU tasks?** Python's Global Interpreter Lock (GIL) prevents true parallelism for CPU-bound work. Use standard workers (processes) instead.

## Configuration

Enable threading by setting `threads > 1` in your cluster configuration:

```python
# settings.py
Q_CLUSTER = {
    "workers": 4,           # 4 worker processes
    "timeout": 60,
}

QRAFT_CLUSTER = {
    "threads": 8,           # 8 threads per worker = 32 concurrent tasks
    "max_inflight": 16,     # Limit concurrent tasks per worker (backpressure)
}
```

### Settings Reference

| Setting | Default | Description |
|---------|---------|-------------|
| `threads` | `1` | Number of threads per worker process. Set to `1` to disable threading and use standard Django-Q2 workers. |
| `max_inflight` | `threads * 2` | Maximum concurrent tasks per worker process. Controls backpressure when thread pool is saturated. |
| `grace_period` | `30.0` | Seconds to wait for in-flight threads to complete during shutdown. |

### Configuration Examples

**Light threading (2-4 threads):**

```python
Q_CLUSTER = {
    "workers": 4,
}

QRAFT_CLUSTER = {
    "threads": 2,  # Conservative, good starting point
    "max_inflight": 4,
}
# Total concurrency: 4 workers (Q_CLUSTER) * 2 threads (QRAFT_CLUSTER) = 8 tasks
```

**Medium threading (4-8 threads):**

```python
Q_CLUSTER = {
    "workers": 4,
}

QRAFT_CLUSTER = {
    "threads": 8,  # Good balance for I/O tasks
    "max_inflight": 16,
}
# Total concurrency: 4 workers (Q_CLUSTER) * 8 threads (QRAFT_CLUSTER) = 32 tasks
```

**Heavy threading (16+ threads):**

```python
Q_CLUSTER = {
    "workers": 2,
}

QRAFT_CLUSTER = {
    "threads": 16,  # Maximum concurrency for very I/O-heavy workloads
    "max_inflight": 32,
    "grace_period": 60.0,  # Longer grace period for cleanup
}
# Total concurrency: 2 workers (Q_CLUSTER) * 16 threads (QRAFT_CLUSTER) = 32 tasks
```

**No threading (standard workers):**

```python
Q_CLUSTER = {
    "workers": 8,
}

QRAFT_CLUSTER = {
    "threads": 1,  # Default: standard Django-Q2 behavior
}
# Total concurrency: 8 workers (Q_CLUSTER) * 1 thread (QRAFT_CLUSTER) = 8 tasks
```

## How It Works

### Architecture

```
Worker Process
  │
  ├─► Create ThreadPoolExecutor(max_workers=threads)
  │
  ├─► Create Semaphore(max_inflight)
  │
  ├─► Listen on task_queue (blocking, main thread)
  │
  ├─► Receive task dict
  │
  ├─► Acquire semaphore (backpressure control)
  │     └─► If max_inflight tasks running, block here
  │
  ├─► Submit to thread pool:
  │     └─► Thread executes task
  │           │
  │           ├─► close_old_connections() before
  │           ├─► qraft.runner.run_task(...) -> call_target()
  │           ├─► close_old_connections() after
  │           │
  │           ├─► Publish result to result_queue
  │           │
  │           └─► Release semaphore
  │
  └─► Loop (receive next task)
```

### Key Components

**ThreadPoolExecutor:**
- Created once per worker process at startup
- Reuses threads across tasks for efficiency
- Configured with `max_workers=threads`

**Semaphore (Backpressure):**
- Limits concurrent tasks to `max_inflight`
- Prevents overwhelming the worker when tasks complete slowly
- Main thread blocks on semaphore if limit reached

**Connection Management:**
- Each thread gets its own database connection
- `close_old_connections()` called before and after each task
- Prevents connection accumulation and stale connections

### Execution Flow Example

```python
# Configuration
Q_CLUSTER = {
    "workers": 2,
}

QRAFT_CLUSTER = {
    "threads": 4,
    "max_inflight": 8,
}

# Task submission
for i in range(20):
    async_task('myapp.tasks.fetch_api', f'https://api.example.com/{i}')

# Execution:
# Worker 1:
#   - Threads 1-4: Processing tasks 1-4 (in-flight: 4)
#   - Semaphore allows 4 more tasks
#   - Receives tasks 5-8, submits to thread pool (in-flight: 8)
#   - Semaphore full, main thread blocks on next receive
#   - Thread 1 completes task 1, releases semaphore
#   - Main thread unblocks, receives task 9 (in-flight: 8)
#   - ...continues

# Worker 2:
#   - Similar pattern, processes tasks 11-20
```

## Performance Characteristics

### Threading Overhead

**One-time costs (per worker process):**
- ThreadPoolExecutor creation: ~1ms
- Semaphore creation: negligible

**Per-task costs:**
- Semaphore acquire/release: ~1-10 microseconds
- Extra DB query for attempt lookup: ~1-5ms
- Connection management: ~1ms (open/close)

**Total overhead:** ~2-6ms per task (negligible for I/O-bound tasks that take 100ms+)

### Expected Speedup (I/O-bound Tasks)

| Configuration | Concurrent Tasks | Expected Throughput | Use Case |
|---------------|------------------|---------------------|----------|
| 2 workers, threads=1 | 2 | 1x (baseline) | CPU-bound |
| 2 workers, threads=4 | 8 | 3-4x | Light I/O |
| 2 workers, threads=8 | 16 | 6-8x | Medium I/O |
| 4 workers, threads=8 | 32 | 10-15x | Heavy I/O |
| 4 workers, threads=16 | 64 | 15-25x | Extreme I/O |

These are modelled ceilings for tasks that are almost entirely I/O wait, not measured
results. `python manage.py demo perf` in the demo app measures your own workload against a
threaded and a non-threaded cluster.

**Note:** Actual speedup depends on:
- I/O wait time (higher = better speedup)
- Number of I/O operations per task
- External service rate limits
- Database connection pool size

### Diminishing Returns

**Beyond threads=16:**
- Context switching overhead increases
- CPU time spent managing threads grows
- Memory usage increases (stack per thread)
- Marginal throughput gains

**Recommendation:** Start with `threads=8`, measure throughput, adjust if needed.

### No Speedup For

**CPU-bound tasks:**

```python
def process_data(data):
    # Pure CPU computation - GIL prevents parallelism
    result = [complex_calculation(x) for x in data]
    return result
```

**Speedup:** None (GIL limitation). Use `threads=1` with more worker processes.

**Tasks with many DB writes:**

```python
def bulk_update(records):
    # DB becomes bottleneck, not I/O wait
    for record in records:
        record.status = 'processed'
        record.save()  # DB serialization point
```

**Speedup:** Limited. Database becomes bottleneck due to transaction serialization.

**Tasks blocking on shared resources:**

```python
import threading
lock = threading.Lock()

def synchronized_task(data):
    with lock:  # All threads wait here
        return process(data)
```

**Speedup:** None. Lock forces serialization.

## Database Connections

### Connection Per Thread

Each thread maintains its own database connection from Django's connection pool:

```
Worker Process (threads=8)
  ├─► Thread 1 → DB Connection 1
  ├─► Thread 2 → DB Connection 2
  ├─► ...
  └─► Thread 8 → DB Connection 8

Total connections: workers * threads
```

### Connection Management

Django-Qraft automatically calls `close_old_connections()` before and after each task:

```python
# qraft/worker.py
def _execute_task_in_thread(task_dict):
    close_old_connections()
    try:
        result = qraft.runner.call_target(...)
    finally:
        close_old_connections()
```

This prevents:
- Connection accumulation (memory leak)
- Stale connection errors
- Connection pool exhaustion

### Sizing the Connection Pool

**Formula:**

```
MIN_CONNS = workers * threads + web_workers + buffer

Example:
- 4 Qraft workers * 8 threads = 32
- 4 web workers (gunicorn/uwsgi) = 4
- Buffer for spikes = 20
Total: 56 connections minimum
```

**PostgreSQL example:**

```python
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'mydb',
        'USER': 'myuser',
        'PASSWORD': 'secret',
        'HOST': 'db.example.com',
        'PORT': 5432,
        'CONN_MAX_AGE': 60,  # Keep connections alive (seconds)
        'OPTIONS': {
            'MAX_CONNS': 100,  # Total connection pool size
        }
    }
}
```

**MySQL example:**

```python
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        'OPTIONS': {
            'init_command': "SET sql_mode='STRICT_TRANS_TABLES'",
            'max_connections': 100,  # Total connection pool size
        },
        'CONN_MAX_AGE': 60,
    }
}
```

### Connection Pool Exhaustion

**Symptoms:**
- `OperationalError: too many connections`
- Tasks hanging waiting for connections
- Database refusing connections

**Solutions:**

1. **Increase pool size:**
   ```python
   'OPTIONS': {'MAX_CONNS': 200}
   ```

2. **Reduce threading:**
   ```python
   QRAFT_CLUSTER = {
       "threads": 4,  # Down from 8
   }
   ```

3. **Add more database servers:**
   - Use read replicas for read-heavy tasks
   - Shard database across multiple servers

4. **Enable connection pooling:**
   - Use PgBouncer (PostgreSQL)
   - Use ProxySQL (MySQL)

## Timeout Behavior

### Process-Level Timeouts

With threading enabled, timeouts continue to be enforced at the **process level** via the sentinel's timer mechanism:

```
Sentinel
  │
  ├─► Monitor worker process timer
  │
  ├─► Worker timeout? (any thread taking too long)
  │     │
  │     └─► Terminate entire worker process (SIGTERM)
  │           │
  │           └─► All threads in that worker are killed
  │
  └─► Reincarnate worker process
```

**Critical behavior:**
- One stuck thread terminates the entire worker process
- All in-flight tasks in that worker are terminated
- Tasks are requeued by the broker (if configured)

### Example Scenario

```python
Q_CLUSTER = {
    "workers": 2,
    "timeout": 60,  # 60 second process timeout
}

QRAFT_CLUSTER = {
    "threads": 8,
}

# Worker 1 has 8 threads running:
# - Thread 1: 10 seconds elapsed
# - Thread 2: 20 seconds elapsed
# - Thread 3: 65 seconds elapsed (STUCK!)
# - Threads 4-8: various times

# Result: Entire Worker 1 process is terminated
# All 8 tasks are killed and potentially requeued
```

### Implications

**Cannot enforce per-task timeouts:**

Python threads cannot be safely terminated individually. The timeout applies to the worker process, not individual threads.

**For tasks requiring strict timeouts:**

Use standard workers (`threads=1`):

```python
Q_CLUSTER = {
    "workers": 8,
    "timeout": 60,
}

QRAFT_CLUSTER = {
    "threads": 1,  # Process-level timeout = task-level timeout
}
```

**For threaded workers with varying task durations:**

Set timeout to the longest expected task:

```python
QRAFT_CLUSTER = {
    "threads": 8,
}

Q_CLUSTER = {
    "timeout": 300,  # Set to longest task duration
}
```

And implement application-level timeouts in tasks:

```python
import requests

def fetch_api(url):
    # Application-level timeout
    response = requests.get(url, timeout=30)
    return response.json()
```

## Mixed Worker Pools

Run both standard (process-based) and threaded workers together using ALT_CLUSTERS for different workload types.

### Configuration

A named cluster that needs settings from both systems needs an entry under `ALT_CLUSTERS`
in both `Q_CLUSTER` and `QRAFT_CLUSTER`, keyed by the same cluster name:

```python
# Default: Standard workers for CPU-bound tasks
Q_CLUSTER = {
    "name": "default",
    "workers": 8,
    "timeout": 300,

    "ALT_CLUSTERS": {
        # Threaded workers for I/O-bound tasks
        "io-workers": {
            "workers": 4,
            "timeout": 60,
        },

        # Many processes for heavy CPU tasks
        "cpu-intensive": {
            "workers": 16,
            "timeout": 600,
        },
    },
}

QRAFT_CLUSTER = {
    "threads": 1,

    "ALT_CLUSTERS": {
        "io-workers": {
            "threads": 8,
            "max_inflight": 16,
        },

        "cpu-intensive": {
            "threads": 1,
        },
    },
}
```

### Starting Multiple Clusters

Run separate processes for each cluster:

```bash
# Terminal 1: Default cluster (CPU-bound)
python manage.py qraftcluster

# Terminal 2: I/O-bound cluster
Q_CLUSTER_NAME=io-workers python manage.py qraftcluster

# Terminal 3: CPU-intensive cluster
Q_CLUSTER_NAME=cpu-intensive python manage.py qraftcluster
```

### Routing Tasks

Route tasks to the appropriate cluster based on workload type:

```python
from qraft.tasks import async_task

# CPU-bound → default cluster (standard workers)
async_task('myapp.tasks.process_video', video_path)
async_task('myapp.tasks.analyze_data', dataset)

# I/O-bound → io-workers cluster (threaded workers)
async_task('myapp.tasks.fetch_api_data', url, cluster='io-workers')
async_task('myapp.tasks.send_email', to, subject, cluster='io-workers')

# Heavy CPU → cpu-intensive cluster (many processes)
async_task('myapp.tasks.render_3d', scene, cluster='cpu-intensive')
```

### Deployment Example (Supervisor)

```ini
[program:qraft-default]
command=python manage.py qraftcluster
directory=/app
user=www-data
autostart=true
autorestart=true
stdout_logfile=/var/log/qraft/default.log

[program:qraft-io]
command=python manage.py qraftcluster
directory=/app
user=www-data
environment=Q_CLUSTER_NAME=io-workers
autostart=true
autorestart=true
stdout_logfile=/var/log/qraft/io.log

[program:qraft-cpu]
command=python manage.py qraftcluster
directory=/app
user=www-data
environment=Q_CLUSTER_NAME=cpu-intensive
autostart=true
autorestart=true
stdout_logfile=/var/log/qraft/cpu.log
```

## Reporting from several threads

`report_progress()` and `record_usage()` are safe to call from more than one thread inside
the same attempt — a fan-out inside a threaded task, or a coroutine gathering several
provider calls. The merge is one statement on Postgres (`jsonb ||`, with the advanced
timestamp set by a `CASE`) and a read-modify-write inside `select_for_update()` elsewhere,
so no reporter overwrites another's keys and no increment is lost.

What they do not do is coordinate *meaning*. Two threads reporting `current=` for
different sub-tasks will overwrite each other's number, because "current" is one value.
Report one dimension per attempt, or give each thread its own key.

See [AI Workloads](ai-workloads.md#usage-and-progress).

## Best Practices

### 1. Start Conservative

Begin with low thread counts and measure:

```python
# Phase 1: Baseline
Q_CLUSTER = {"workers": 4}
QRAFT_CLUSTER = {"threads": 1}

# Phase 2: Light threading
Q_CLUSTER = {"workers": 4}
QRAFT_CLUSTER = {"threads": 2}

# Phase 3: Medium threading (monitor metrics)
Q_CLUSTER = {"workers": 4}
QRAFT_CLUSTER = {"threads": 8}
```

### 2. Monitor Key Metrics

Track:
- **Throughput**: Tasks/second completed
- **Latency**: Task completion time
- **CPU usage**: Should stay below 80% with I/O tasks
- **Memory usage**: Each thread adds ~8MB stack
- **DB connections**: Active connections from workers

### 3. Set Appropriate max_inflight

**Too low:** Threads sit idle waiting for semaphore

```python
QRAFT_CLUSTER = {
    "threads": 8,
    "max_inflight": 4,  # Only 4 of 8 threads can run!
}
```

**Too high:** Worker overwhelmed with tasks

```python
QRAFT_CLUSTER = {
    "threads": 8,
    "max_inflight": 100,  # Way too many!
}
```

**Recommended:** Start with `threads * 2`:

```python
QRAFT_CLUSTER = {
    "threads": 8,
    "max_inflight": 16,  # threads * 2
}
```

### 4. Match Workload to Worker Type

| Workload Type | workers | threads | Example |
|---------------|---------|---------|---------|
| CPU-bound | 8 | 1 | Video processing |
| Light I/O | 4 | 2-4 | Database queries |
| Medium I/O | 4 | 4-8 | API calls |
| Heavy I/O | 2-4 | 8-16 | Web scraping |
| Mixed | Use ALT_CLUSTERS | - | CPU + I/O tasks |

### 5. Set Realistic Timeouts

For threaded workers, set timeout to longest task:

```python
QRAFT_CLUSTER = {
    "threads": 8,
}

Q_CLUSTER = {
    "timeout": 120,  # Longest task takes 90s
}
```

Not the average:

```python
# Wrong: average task takes 30s, but some take 90s
QRAFT_CLUSTER = {
    "threads": 8,
}

Q_CLUSTER = {
    "timeout": 30,  # Will kill long tasks!
}
```

### 6. Size Database Connections

Formula: `(workers * threads) + web_workers + buffer`

```python
# Qraft: 4 workers * 8 threads = 32
# Web: 8 gunicorn workers = 8
# Buffer: 20
# Total: 60 minimum

DATABASES = {
    'default': {
        'OPTIONS': {'MAX_CONNS': 80},  # 60 + margin
    }
}
```

### 7. Use Graceful Shutdown

Allow time for in-flight tasks to complete:

```python
QRAFT_CLUSTER = {
    "grace_period": 60.0,  # Wait up to 60s for threads
}
```

Handle signals properly in tasks:

```python
import signal
import sys

def long_running_task(data):
    # Check for shutdown signal
    if signal.getsignal(signal.SIGTERM) != signal.SIG_DFL:
        print("Shutting down gracefully...")
        sys.exit(0)

    # ... task work ...
```

### 8. Test Under Load

Benchmark with realistic workloads:

```bash
# Generate load
for i in {1..1000}; do
    python manage.py shell -c "from qraft.tasks import async_task; async_task('myapp.tasks.fetch_api', 'https://api.example.com/$i', cluster='io-workers')"
done

# Monitor metrics
watch -n 1 'ps aux | grep qraftcluster'
```

## Troubleshooting

### High CPU Usage (I/O Tasks)

**Symptom:** CPU at 100% despite tasks being I/O-bound

**Causes:**
1. Too many threads (context switching overhead)
2. Tasks are actually CPU-bound
3. Python GIL contention

**Solutions:**
- Reduce thread count: `"threads": 4`
- Profile tasks to identify CPU work
- Move CPU work to standard workers

### Database Connection Errors

**Symptom:** `OperationalError: too many connections`

**Solution:**

Increase connection pool:

```python
DATABASES = {
    'default': {
        'OPTIONS': {'MAX_CONNS': 150},  # Increase
    }
}
```

Or reduce concurrency:

```python
Q_CLUSTER = {
    "workers": 4,
}

QRAFT_CLUSTER = {
    "threads": 4,  # Down from 8
}
```

### Tasks Hanging

**Symptom:** Tasks stuck in RUNNING state

**Causes:**
1. Deadlock in task code
2. Blocking operation without timeout
3. Worker process killed

**Solutions:**

Add timeouts to blocking operations:

```python
import requests

def fetch_api(url):
    response = requests.get(url, timeout=30)  # Add timeout
    return response.json()
```

Check worker logs:

```bash
python manage.py qraftcluster  # Watch for errors
```

### Memory Leaks

**Symptom:** Worker memory grows over time

**Causes:**
1. Database connections not closing
2. Objects accumulating in threads
3. Circular references

**Solutions:**

Enable worker recycling:

```python
Q_CLUSTER = {
    "recycle": 500,  # Recycle after 500 tasks
}
```

Force connection cleanup:

```python
from django.db import close_old_connections

def my_task(data):
    try:
        # ... task work ...
        return result
    finally:
        close_old_connections()  # Ensure cleanup
```

### Inconsistent Performance

**Symptom:** Throughput varies widely

**Causes:**
1. External service rate limits
2. Task duration variation
3. Uneven task distribution

**Solutions:**

Implement backoff in tasks:

```python
import time
import random

def fetch_api(url):
    time.sleep(random.uniform(0.1, 0.5))  # Jitter
    response = requests.get(url)
    return response.json()
```

Monitor queue depth:

```python
from django_q.models import OrmQ
print(OrmQ.objects.count())  # Pending tasks
```

## Related Documentation

- [Configuration Guide](configuration.md) - Threading settings reference
- [Architecture Guide](architecture.md) - Worker architecture details
- [Getting Started](getting-started.md) - Basic setup
- [Best Practices](development.md) - Development guidelines
