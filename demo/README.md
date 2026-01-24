# Qraft Demo

Minimal demonstration of Qraft's features: dual-phase hooks, retry policies, and multithreaded workers.

## Quick Start

```bash
# From the project root
cd demo

# Install dependencies
uv sync

# Run migrations
python manage.py migrate

# Start the cluster (in one terminal)
python manage.py qraftcluster

# Run demos (in another terminal)
python manage.py demo hooks
python manage.py demo retry

# For performance comparison, see "Perf Demo" section below
```

## Demo Scenarios

### Hooks: Dual-Phase Success/Failure

```bash
python manage.py demo hooks -n 5
```

Queues tasks with 50% random failure rate. Watch the cluster logs for:
- `SUCCESS HOOK: task_id=...` when tasks succeed
- `FAILURE HOOK: task_id=...` when tasks fail

### Retry: Exponential Backoff

```bash
python manage.py demo retry --fail-times 2 --max-attempts 4
```

Task fails exactly N times before succeeding. With `--fail-times 2` and `--max-attempts 4`:
- Attempt 1: fails, retry in 2s
- Attempt 2: fails, retry in 4s
- Attempt 3: succeeds

Try `--fail-times 5 --max-attempts 3` to see retry exhaustion.

### Perf: Threading Performance Comparison

This demo compares **baseline Django-Q2** (no threading) vs **Qraft's multithreaded workers** side-by-side.

**Setup (requires two terminal windows):**

```bash
# Terminal 1: Start baseline cluster (standard Django-Q2)
python manage.py qraftcluster

# Terminal 2: Start Qraft cluster (with threading)
Q_CLUSTER_NAME=qraft python manage.py qraftcluster

# Terminal 3: Run the benchmark
python manage.py demo perf -n 20 --duration 1.0
```

**What it does:**
- Queues 20 identical tasks to BOTH clusters
- Each task sleeps for 1.0 second (simulates I/O-bound work)
- Measures and compares completion times

**Expected results:**
- **Baseline** (2 workers, no threading): ~10 seconds (2 tasks at a time)
- **Qraft** (2 workers x 4 threads): ~2.5 seconds (8 tasks at a time)
- **Speedup**: ~4x improvement

The baseline cluster processes tasks sequentially (2 at a time with 2 workers), while Qraft's multithreaded workers can handle up to 8 concurrent I/O-bound tasks.

## Using PostgreSQL

SQLite works for demos but has concurrency limitations. For production-like testing:

```bash
# Option 1: Environment variables
export POSTGRES_DB=qraft_demo
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres
export POSTGRES_HOST=localhost

# Option 2: .env file (recommended)
cat > .env <<EOF
DEMO_USE_POSTGRES=1
POSTGRES_DB=qraft_demo
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
POSTGRES_HOST=localhost
EOF

python manage.py migrate
python manage.py qraftcluster
```

## Cluster Configuration

The demo uses a **dual-cluster** setup for performance comparisons:

```python
# Baseline: Standard Django-Q2 (no threading)
Q_CLUSTER = {
    "name": "baseline",
    "workers": 2,
    "timeout": 60,
    "orm": "default",
}

# Alternative cluster: Qraft with threading
ALT_CLUSTERS = {
    "qraft": {
        "name": "qraft",
        "workers": 2,
        "timeout": 60,
        "orm": "default",
    }
}

# Qraft-specific settings (applies to qraft cluster)
QRAFT_CLUSTER = {
    "threads": 4,        # Threads per worker
    "max_inflight": 8,   # Max concurrent tasks per worker
    "retry_defaults": {  # Default retry policy for tasks
        "max_attempts": 3,
        "delay": 2.0,
        "backoff": "exponential",
        "jitter": True,
    },
}
```

**Running specific clusters:**
```bash
# Start baseline cluster
python manage.py qraftcluster

# Start Qraft cluster (in separate terminal)
Q_CLUSTER_NAME=qraft python manage.py qraftcluster
```

**Tuning guidelines:**
- **I/O-bound tasks** (API calls, DB queries, file I/O): Increase `threads` (e.g., 8-16)
- **CPU-bound tasks** (data processing, computations): Increase `workers`, keep `threads=1`
- **max_inflight**: Should be ≥ `workers * threads` for full utilization
