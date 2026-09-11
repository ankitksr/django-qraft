# Django-Qraft

[![Python Version](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![CI](https://github.com/ankitksr/django-qraft/actions/workflows/test.yml/badge.svg)](https://github.com/ankitksr/django-qraft/actions/workflows/test.yml)
[![Django Version](https://img.shields.io/badge/django-5.0+-green.svg)](https://www.djangoproject.com/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Durable background jobs and workflows for Django — Postgres only, no extra infra, with primitives for jobs that call metered AI providers.

Django-Qraft is a drop-in enhancement of [Django-Q2](https://django-q2.readthedocs.io/): Django-Q2 supplies the cluster runtime, Qraft owns task state, retries, hooks, and orchestration. Existing Q2 task functions and cluster settings can be reused; Qraft-owned tasks enforce stricter execution options.

## Features

### 🎯 Dual-Phase Hooks
Separate success and failure hooks with custom arguments and kwargs. Hooks run asynchronously by default to prevent monitor bottlenecks.

```python
async_task('myapp.tasks.process_data', data_id,
    qraft_options={
        'success_hook': 'myapp.hooks.on_success',
        'success_kwargs': {'notify': True},
        'failure_hook': 'myapp.hooks.on_failure',
    }
)
```

[Learn more →](docs/hooks.md)

### 🔄 Rich Retry Policies
Multiple backoff strategies (exponential, linear, fixed) with jitter and exception filtering.

```python
async_task('myapp.tasks.api_call', url,
    qraft_options={
        'max_attempts': 5,
        'base_delay': 10,
        'backoff_strategy': 'exponential',
        'jitter': True,
        'retry_exceptions': ['ConnectionError', 'Timeout'],
    }
)
```

[Learn more →](docs/retry.md)

### ⚡ Multithreaded Workers
Optional thread pool execution for higher concurrency on I/O-bound workloads.

```python
# settings.py
QRAFT_CLUSTER = {
    "workers": 4,
    "threads": 8,        # 8 threads per worker = 32 concurrent tasks
    "max_inflight": 16,  # Backpressure control
}
```

[Learn more →](docs/threading.md)

### 🔗 Workflow Primitives
Orchestrate complex task workflows with chain (sequential), iter (parallel homogeneous), and batch (parallel heterogeneous) patterns.

```python
from qraft.chain import QraftChain

# Sequential pipeline
chain = QraftChain(on_success='myapp.hooks.pipeline_complete')
chain.append('myapp.tasks.extract', source_id)
chain.append('myapp.tasks.transform', format='json')
chain.append('myapp.tasks.load', dest_id)
chain.run()
```

```python
from qraft.iter import QraftIter

# Parallel processing of multiple items
iter_task = QraftIter('myapp.tasks.process_report',
    qraft_options={'max_attempts': 3},
    on_success='myapp.hooks.all_reports_ready'
)
for report_id in report_ids:
    iter_task.append(report_id)
iter_task.run()
```

```python
from qraft.batch import QraftBatch

# Fork-join pattern for heterogeneous tasks
batch = QraftBatch(on_success='myapp.hooks.generate_report')
batch.append('myapp.tasks.fetch_sales', region='NA')
batch.append('myapp.tasks.fetch_inventory', warehouse='main')
batch.append('myapp.tasks.fetch_shipping', carrier='fedex')
batch.run()
```

Chain steps can park for human approval at zero compute:

```python
chain.append('myapp.tasks.publish', release_id, requires_approval=True)
chain.run()
# ... later, from a view or the admin
QraftChain(chain_id=chain_id).approve()
```

[Learn more →](docs/workflows.md)

### 🤖 AI-Workload Primitives
Rate-limit-aware retries, shared token buckets, token/cost accounting, and idempotency keys for jobs that call metered providers.

```python
from qraft.throttle import throttled
from qraft.context import record_usage

@throttled(key='openai:acme-tenant', rate=10, capacity=20)
def summarize(doc_id):
    response = client.messages.create(...)
    record_usage(model='claude-opus-4', input_tokens=..., cost=...)

async_task('myapp.tasks.summarize', doc_id,
    qraft_options={'idempotency_key': f'summary:{doc_id}', 'max_attempts': 5})
```

Provider throttling (429/503/529) retries with forced exponential backoff and honors `Retry-After`, separate from your hard-error budget.

Set `stall_after` and the reaper distinguishes a task that is alive from one that is
moving: an attempt whose heartbeat is fresh but whose progress has not advanced is flagged,
signalled and marked on the dashboard. It is never retried automatically — the original
attempt is still running and still writing.

With an optional `QRAFT_PRICING` table, recorded tokens become money, priced per increment
so an attempt that calls two models is not billed at one rate.

[Learn more →](docs/ai-workloads.md)

### 🔭 Subjects, Graphs and Signals
Every task can name the domain entity it is for, and a graph declares a whole pipeline up
front — nodes with explicit `after` edges, dispatched by Qraft as their dependencies are
met, settling once when nothing can advance, with a durable completion hook and one number
for end-to-end latency.

```python
from qraft import graphs

builder = graphs.Graph(subject=('worksheet', 4117),
                       on_settled='revenue.hooks.worksheet_ready')
builder.node('ingest', 'revenue.tasks.ingest', 4117, recovery='transactional')
builder.node('rules', 'revenue.tasks.rules_pass', 4117,
             after=('ingest',), recovery='idempotent')
graph_id = builder.start()

QraftTask.objects.for_subject('worksheet', 4117)   # every task for one entity
```

A failed graph resumes selectively: `graphs.resume(graph_id)` re-runs the failed nodes and
everything downstream, and `preview_resume()` says what that would be before you do it.

Qraft also emits signals about its own transitions (`task_settled`, `workflow_settled`,
`graph_settled`, …), all `send_robust` and all carrying ids rather than model instances,
plus an optional OpenTelemetry metrics sink, a logging filter and W3C trace propagation
across the enqueue boundary.

[Learn more →](docs/workflows.md#graphs)

### ⏱️ Exact-Delay Scheduling
Delayed work (retries, requeues, `run_after` tasks) is a Qraft-owned row dispatched at its due time — a 2-second backoff fires in about 2 seconds, not on Django-Q2's 30-second scheduler cycle. Priority and target cluster survive the delay.

[Learn more →](docs/retry.md)

### 📊 Monitoring Dashboard
A bundled staff-only dashboard: live task and workflow state, approve/reject/cancel/requeue actions, latency percentiles, and JSON endpoints for your own tooling.

```python
INSTALLED_APPS += ["qraft.dashboard"]
# urls.py: path("qraft/", include("qraft.dashboard.urls"))
```

[Learn more →](docs/dashboard.md)

### 🛟 Crash Recovery
A reaper resolves attempts whose worker died mid-run instead of leaving tasks stuck in RUNNING. It runs alongside the monitor; the cluster warns at startup if the broker has no delivery receipts.

```python
QRAFT_CLUSTER = {"reap_interval": 60, "reap_stale_after": 3600}
```

[Learn more →](docs/ai-workloads.md#orphan-reaper)

### 💀 Dead Letter Queue
`dead_letters()` finds tasks that failed permanently; `requeue()` re-enqueues one as the next attempt on the same task, preserving history. Also available as an admin bulk action.

```python
from qraft.dlq import dead_letters, requeue

for task in dead_letters():
    requeue(task)
```

[Learn more →](docs/dlq.md)

### 🎚️ Priority Lanes
High, default, and low lanes on a single ORM queue — interactive jobs preempt batch work without a second deployment.

```python
Q_CLUSTER = {"orm": "default", "broker_class": "qraft.brokers.QraftOrmBroker"}

async_task('myapp.tasks.answer', query, qraft_options={'priority': 'high'})
```

[Learn more →](docs/ai-workloads.md#priority-lanes)

### 🧩 django.tasks Engine
Qraft implements the Django 6.0 Tasks API (DEP 14), so `@task` code stays portable while Qraft supplies the execution, retries, and orchestration DEP 14 leaves out.

```python
TASKS = {"default": {"BACKEND": "qraft.backend.QraftTaskBackend"}}
```

[Learn more →](docs/django-tasks-backend.md)

### 🔌 Drop-in Compatible
Reuses Django-Q2 configuration and task functions. Use existing `Q_CLUSTER` settings or migrate to `QRAFT_CLUSTER`.

## Quick Start

### Installation

```bash
pip install django-qraft
```

### Configuration

Add to your Django settings:

```python
# settings.py
INSTALLED_APPS = [
    # ...
    'django_q',
    'qraft',
]

QRAFT_CLUSTER = {
    "workers": 4,
    "timeout": 60,
    "orm": "default",
}
```

Run migrations:

```bash
python manage.py migrate
```

### Start the Cluster

```bash
python manage.py qraftcluster
```

### Create Tasks

```python
from qraft.tasks import async_task

# Basic task
task_id = async_task('myapp.tasks.send_email', recipient, subject, body)

# With hooks and retries
task_id = async_task(
    'myapp.tasks.process_order',
    order_id,
    qraft_options={
        'max_attempts': 3,
        'success_hook': 'myapp.hooks.on_order_processed',
        'failure_hook': 'myapp.hooks.on_order_failed',
    }
)
```

[Full Getting Started Guide →](docs/getting-started.md)

## Documentation

### Core Guides
- [Getting Started](docs/getting-started.md) - Installation, setup, and basic usage
- [Configuration Reference](docs/configuration.md) - Complete settings documentation
- [Dual-Phase Hooks](docs/hooks.md) - Success and failure hook system
- [Retry Policies](docs/retry.md) - Backoff strategies and retry configuration
- [Multithreaded Workers](docs/threading.md) - Concurrency for I/O-bound tasks
- [Workflow Primitives](docs/workflows.md) - Chain, Iter, Batch, approval steps, and graphs
- [AI Workloads](docs/ai-workloads.md) - Subjects, rate limits, throttling, usage and cost, idempotency, reaper, stall observation, priority lanes
- [Monitoring Dashboard](docs/dashboard.md) - Bundled staff dashboard with live metrics and JSON endpoints
- [django.tasks Backend](docs/django-tasks-backend.md) - Qraft as an engine for Django 6.0's Tasks API

### Advanced Topics
- [Architecture](docs/architecture.md) - System design and extension patterns
- [Development Guide](docs/development.md) - Contributing and local development
- [Testing Guide](tests/README.md) - Running and writing tests
- [Demo Application](demo/README.md) - Interactive feature demonstrations
- [Roadmap](docs/roadmap.md) - Positioning and planned features
- [Test Drive](docs/test-drive.md) - Guided walkthrough of every demo scenario
- [Lifecycle Map](docs/lifecycle-map.html) - One-page visual reference for the task and worker lifecycle

## Use Cases

### High-Concurrency I/O Operations

Perfect for workloads with many API calls, database queries, or file operations:

```python
QRAFT_CLUSTER = {
    "workers": 2,
    "threads": 16,  # Handle 32 concurrent I/O operations
}
```

**Result**: in-flight concurrency scales with `workers × threads` rather than `workers`, so
an I/O-bound workload spends its wait time on other tasks. Measure your own workload with
`python manage.py demo perf` in the demo app — it runs the same tasks against a threaded
and a non-threaded cluster side by side.

[Threading Guide →](docs/threading.md)

### Reliable External API Calls

Automatic retries with exponential backoff prevent transient failures:

```python
async_task('myapp.tasks.stripe_charge', amount,
    qraft_options={
        'max_attempts': 5,
        'base_delay': 10,
        'backoff_strategy': 'exponential',
        'retry_exceptions': ['RequestException'],
        'skip_exceptions': ['AuthenticationError'],
    }
)
```

[Retry Guide →](docs/retry.md)

### Data Processing Pipelines

Chain tasks using success hooks for sequential processing:

```python
async_task('etl.extract', source_id,
    qraft_options={
        'success_hook': 'etl.transform',
        'success_kwargs': {'pipeline_id': 'pipe-123'},
    }
)
```

[Hooks Guide →](docs/hooks.md)

### Mixed Workload Clusters

Run separate clusters for CPU-bound and I/O-bound tasks:

```python
QRAFT_CLUSTER = {
    "workers": 4,
    "threads": 1,  # Standard workers for CPU tasks

    "ALT_CLUSTERS": {
        "io-workers": {
            "workers": 2,
            "threads": 8,  # Threaded workers for I/O tasks
        }
    }
}
```

Route tasks to appropriate clusters:

```python
async_task('cpu.task')                        # → default
async_task('io.task', cluster='io-workers')  # → io-workers
```

[Configuration Guide →](docs/configuration.md)

## Architecture

Django-Qraft extends Django-Q2 through selective inheritance:

```
QraftCluster (extends Cluster)
  └── QraftSentinel (extends Sentinel)
        ├── Pusher (unchanged)
        ├── Monitor (unchanged)
        └── Worker Processes
              ├── Standard worker (threads=1)
              └── Threaded worker (threads>1)
```

**Key enhancement points:**
- `QraftCluster.start()` → spawns `QraftSentinel`
- `QraftSentinel.spawn_worker()` → conditionally spawns threaded workers
- Hook handler intercepts Django-Q2 task completion
- Qraft owns scheduling, lease tracking, reaping, retention, and completion routing; Django-Q2 supplies the pusher, monitor, and standard worker loop.

[Architecture Guide →](docs/architecture.md)

## Requirements

- Python 3.10+
- Django 5.0+ (6.0+ for the `django.tasks` backend)
- Django-Q2 1.8+
- PostgreSQL — the throttle, reaper, and workflow dispatchers need real row locks
- The ORM broker (`"orm"`, plus `"broker_class": "qraft.brokers.QraftOrmBroker"` for priority lanes) — see [broker support](docs/configuration.md#broker-support) for what other brokers give up

## Demo Application

Django-Qraft includes interactive demos:

```bash
cd demo

# Dual-phase hooks demo
python manage.py demo hooks -n 5

# Retry policies demo
python manage.py demo retry --fail-times 2 --max-attempts 4

# Performance comparison (requires 2 clusters)
# Terminal 1: python manage.py qraftcluster
# Terminal 2: Q_CLUSTER_NAME=qraft python manage.py qraftcluster
# Terminal 3: python manage.py demo perf -n 20

# Workflow primitives demos
python manage.py demo chain -n 3
python manage.py demo iter -n 5
python manage.py demo batch -n 3

# AI-workload demos
python manage.py demo approval
python manage.py demo ratelimit
python manage.py demo usage
python manage.py demo idempotent
python manage.py demo reaper
```

[Demo Guide →](demo/README.md)

## Performance

**Threading speedup** (I/O-bound tasks):

| Configuration | Concurrent Tasks | Expected Throughput |
|---------------|------------------|---------------------|
| 2 workers, threads=1 | 2 | 1x (baseline) |
| 2 workers, threads=4 | 8 | 3-4x |
| 2 workers, threads=8 | 16 | 6-8x |

**Note**: These are modelled ceilings for tasks that are almost entirely I/O wait, not
measured results — real speedup depends on how much of the task is wait. Threading provides
no speedup at all for CPU-bound tasks, because of Python's GIL.

[Threading Guide →](docs/threading.md)

## Testing

Django-Qraft's test suite covers 85% of lines, with CI gated at 72%. `qraft/admin.py` is excluded and verified manually via the demo app, and `qraft/backend.py` only reports coverage on Django 6.0+, where its tests run. `tests/test_e2e.py` runs the whole path — real broker, real worker, real hook handler — with nothing stubbed.

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=qraft --cov-report=html

# Run in parallel
pytest -n auto
```

[Testing Guide →](tests/README.md)

## Contributing

Contributions are welcome! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

**Quick development setup:**

```bash
# Clone repository
git clone https://github.com/ankitksr/django-qraft.git
cd django-qraft

# Install dependencies
uv sync --group test

# Run tests
pytest

# Run linter
ruff check qraft/
```

[Development Guide →](docs/development.md)

## Compatibility

Django-Qraft reuses Django-Q2 with these compatibility boundaries:

- ✅ All Django-Q2 broker types run Qraft tasks; the ORM broker on PostgreSQL is the only one with full guarantees — every other broker loses delivery receipts and priority lanes, halves the reaper, and makes an enqueue visible before its transaction commits ([what degrades](docs/configuration.md#broker-support))
- ✅ Existing `Q_CLUSTER` settings keep configuring Django-Q2; Qraft's own settings live in `QRAFT_CLUSTER` ([what Qraft reads from `Q_CLUSTER`](docs/configuration.md#q_cluster-still-belongs-to-django-q2))
- ✅ Standard `qcluster` command continues to work
- ✅ Tasks queued via Django-Q2's `async_task` work seamlessly
- Qraft tasks reject `sync=True`, `save=False`, cached results, and `ack_failure=False`: durable completion tracking and retry ownership require these restrictions.

## Roadmap

- [x] **v1.1.0**: Workflow primitives (Chain, Iter, Batch)
- [x] **v1.2.0**: Orphan reaper, rate-limit-aware retries, idempotency keys, usage accounting, approval steps, cross-worker throttling, priority lanes, `django.tasks` backend
- [x] **v1.2.1**: Execution lease with heartbeat, dead letter queue, `TaskContext` and deferred tasks on the `django.tasks` backend
- [x] **v1.3.0**: Qraft-owned scheduling (exact delays, priority-preserving retries), monitoring dashboard, retention sweep, cluster routing
- [ ] **Unreleased** (on `main`, not yet tagged): observability — subjects, signals, a
  metrics sink, log context and trace propagation; execution graphs with selective
  resume, completion receipts and approval gates; cost from usage; stall observation;
  per-cluster brokers; the redelivery guard; coroutine tasks
  ([details](docs/roadmap.md#shipped-after-130-unreleased))
- [ ] Nested workflow support

[Future Plans →](docs/future/)

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Acknowledgments

Built on top of the excellent [Django-Q2](https://django-q2.readthedocs.io/) project. Special thanks to the Django-Q2 maintainers and contributors.

## Links

- **Documentation**: [docs/](docs/)
- **Source Code**: [GitHub Repository](https://github.com/ankitksr/django-qraft)
- **Issue Tracker**: [GitHub Issues](https://github.com/ankitksr/django-qraft/issues)
- **Changelog**: [CHANGELOG.md](CHANGELOG.md)
- **Contributing**: [CONTRIBUTING.md](CONTRIBUTING.md)
