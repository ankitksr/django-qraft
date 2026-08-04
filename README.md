# Django-Qraft

[![Python Version](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![CI](https://github.com/ankitksr/django-qraft/actions/workflows/test.yml/badge.svg)](https://github.com/ankitksr/django-qraft/actions/workflows/test.yml)
[![Django Version](https://img.shields.io/badge/django-4.2+-green.svg)](https://www.djangoproject.com/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Django-Qraft is a next-generation distributed task queue for Django, built as a drop-in enhancement of [Django-Q2](https://django-q2.readthedocs.io/). It extends Django-Q2 with advanced features while maintaining full backward compatibility.

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

[Learn more →](docs/workflows.md)

### 🔌 Drop-in Compatible
Fully compatible with Django-Q2 configuration and behavior. Use existing `Q_CLUSTER` settings or migrate to `QRAFT_CLUSTER`.

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
- [Workflow Primitives](docs/workflows.md) - Chain, Iter, and Batch orchestration

### Advanced Topics
- [Architecture](docs/architecture.md) - System design and extension patterns
- [Development Guide](docs/development.md) - Contributing and local development
- [Testing Guide](tests/README.md) - Running and writing tests
- [Demo Application](demo/README.md) - Interactive feature demonstrations

## Use Cases

### High-Concurrency I/O Operations

Perfect for workloads with many API calls, database queries, or file operations:

```python
QRAFT_CLUSTER = {
    "workers": 2,
    "threads": 16,  # Handle 32 concurrent I/O operations
}
```

**Result**: 8-10x throughput improvement over standard workers for I/O-bound tasks.

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
- All other components unchanged from Django-Q2

[Architecture Guide →](docs/architecture.md)

## Requirements

- Python 3.10+
- Django 4.2+
- Django-Q2 1.8+

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
```

[Demo Guide →](demo/README.md)

## Performance

**Threading speedup** (I/O-bound tasks):

| Configuration | Concurrent Tasks | Speedup |
|---------------|------------------|---------|
| 2 workers, threads=1 | 2 | 1x (baseline) |
| 2 workers, threads=4 | 8 | 3-4x |
| 2 workers, threads=8 | 16 | 6-8x |

**Note**: Threading provides no speedup for CPU-bound tasks due to Python's GIL.

[Threading Guide →](docs/threading.md)

## Testing

Django-Qraft has a comprehensive test suite (75% coverage; the admin UI is excluded and verified manually via the demo app):

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
uv pip install -e ".[dev,test]"

# Run tests
pytest

# Run linter
ruff check qraft/
```

[Development Guide →](docs/development.md)

## Compatibility

Django-Qraft maintains full backward compatibility with Django-Q2:

- ✅ All Django-Q2 broker types supported (Redis, ORM, SQS, etc.)
- ✅ Existing `Q_CLUSTER` settings work (with deprecation warning)
- ✅ Standard `qcluster` command continues to work
- ✅ Tasks queued via Django-Q2's `async_task` work seamlessly
- ✅ Drop-in replacement, no breaking changes

## Roadmap

- [x] **v1.1.0**: Workflow primitives (Chain, Iter, Batch)
- [ ] Async/await worker support for native asyncio tasks
- [ ] Enhanced monitoring and metrics
- [ ] Task prioritization
- [ ] Dead letter queue for failed tasks
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
