# Qraft Demo

Minimal demonstration of Qraft's features: dual-phase hooks, retry policies, multithreaded workers, and workflow primitives.

## Quick Start

```bash
# From the project root
cd demo

# Install dependencies
uv sync

# Run migrations
uv run python manage.py migrate

# Create an admin superuser (optional, for browsing results)
uv run python manage.py createsuperuser

# Start the cluster (in one terminal)
uv run python manage.py qraftcluster

# Run demos (in another terminal)
uv run python manage.py demo hooks
uv run python manage.py demo retry
uv run python manage.py demo chain
uv run python manage.py demo iter
uv run python manage.py demo batch
uv run python manage.py demo cancel

# For performance comparison, see "Perf Demo" section below
```

## Admin UI

The demo includes Django's admin interface for browsing Qraft models. After running `createsuperuser`, visit [http://localhost:8000/admin/](http://localhost:8000/admin/) (start the dev server with `uv run python manage.py runserver`).

You can inspect:
- **QraftTasks** — task status, arguments, retry policy, and attempt history
- **QraftChainModels** — chain workflows with inline step details
- **QraftIterModels** — parallel iter workflows with progress counters
- **QraftBatchModels** — parallel batch workflows with progress counters
- **HookDispatches** — async hook execution records

## Demo Scenarios

### Hooks: Dual-Phase Success/Failure

```bash
uv run python manage.py demo hooks -n 5
```

Queues tasks with 50% random failure rate. Watch the cluster logs for:
- `SUCCESS HOOK: task_id=...` when tasks succeed
- `FAILURE HOOK: task_id=...` when tasks fail

### Retry: Exponential Backoff

```bash
uv run python manage.py demo retry --fail-times 2 --max-attempts 4
```

Task fails exactly N times before succeeding. With `--fail-times 2` and `--max-attempts 4`:
- Attempt 1: fails, retry in 2s
- Attempt 2: fails, retry in 4s
- Attempt 3: succeeds

Try `uv run python manage.py demo retry --fail-times 5 --max-attempts 3` to see retry exhaustion.

### Chain: Sequential Workflow

```bash
uv run python manage.py demo chain -n 3
uv run python manage.py demo chain -n 3 --wait 10000
```

Creates a sequential chain of N steps. Each step executes only after the previous one succeeds. Use `--wait` to block until completion and see the `WorkflowResult`.

Features demonstrated: step-by-step execution, chain-level hooks, `on_cancelled` hook.

### Iter: Parallel Homogeneous Workflow

```bash
uv run python manage.py demo iter -n 5
uv run python manage.py demo iter -n 5 --wait 10000
```

Applies the same function (`noop_task`) to N different inputs in parallel. Includes a `progress_hook` that logs after each task completes. Use `--wait` to see final results.

Features demonstrated: parallel fan-out, progress callbacks, workflow-level hooks.

### Batch: Parallel Heterogeneous Workflow

```bash
uv run python manage.py demo batch -n 3
uv run python manage.py demo batch -n 3 --wait 30000
```

Runs N different tasks in parallel (fork-join). Each task can be a different function with its own retry policy. Includes a `progress_hook`. Use `--wait` to see final results with error details.

Features demonstrated: heterogeneous fan-out, per-task retry policies, error aggregation.

### Cancel: Workflow Cancellation

```bash
uv run python manage.py demo cancel
```

Creates a chain of slow tasks, then immediately cancels it. Demonstrates:
- `chain.cancel()` sets status to CANCELLED
- In-flight tasks complete but no further steps are queued
- The `on_cancelled` hook fires

### Perf: Threading Performance Comparison

This demo compares **baseline Django-Q2** (no threading) vs **Qraft's multithreaded workers** side-by-side.

**Setup (requires three terminal windows):**

```bash
# Terminal 1: Start baseline cluster (standard Django-Q2)
uv run python manage.py qraftcluster

# Terminal 2: Start Qraft cluster (with threading)
Q_CLUSTER_NAME=qraft uv run python manage.py qraftcluster

# Terminal 3: Run the benchmark
uv run python manage.py demo perf -n 20 --duration 1.0
```

**What it does:**
- Queues 20 identical tasks to BOTH clusters
- Each task sleeps for 1.0 second (simulates I/O-bound work)
- Measures and compares completion times

**Expected results:**
- **Baseline** (2 workers, no threading): ~10 seconds (2 tasks at a time)
- **Qraft** (2 workers x 4 threads): ~2.5 seconds (8 tasks at a time)
- **Speedup**: ~4x improvement

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

uv run python manage.py migrate
uv run python manage.py qraftcluster
```

## Cluster Configuration

The demo uses a **dual-cluster** setup for performance comparisons:

```python
# Django-Q2 cluster config
Q_CLUSTER = {
    "name": "baseline",
    "workers": 2,
    "timeout": 300,
    "retry": 600,
    "orm": "default",
    "ALT_CLUSTERS": {
        "qraft": {
            "name": "qraft",
            "workers": 2,
            "timeout": 300,
            "retry": 600,
            "orm": "default",
        }
    },
}

# Qraft-specific settings
QRAFT_CLUSTER = {
    "threads": 1,           # Baseline: no threading
    "max_inflight": 2,
    "retry_defaults": {
        "max_attempts": 3,
        "delay": 2.0,
        "backoff": "exponential",
        "jitter": True,
    },
    "ALT_CLUSTERS": {
        "qraft": {
            "threads": 4,       # 4 threads per worker = 8 concurrent
            "max_inflight": 8,
        }
    },
}
```

**Running specific clusters:**
```bash
# Start baseline cluster
uv run python manage.py qraftcluster

# Start Qraft cluster (in separate terminal)
Q_CLUSTER_NAME=qraft uv run python manage.py qraftcluster
```

**Tuning guidelines:**
- **I/O-bound tasks** (API calls, DB queries, file I/O): Increase `threads` (e.g., 8-16)
- **CPU-bound tasks** (data processing, computations): Increase `workers`, keep `threads=1`
- **max_inflight**: Should be >= `workers * threads` for full utilization
