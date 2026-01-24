# Django-Qraft Documentation

Welcome to the Django-Qraft documentation! This guide will help you understand, configure, and use Django-Qraft effectively.

## Quick Navigation

### Getting Started
- [Getting Started Guide](getting-started.md) - Installation, setup, and basic usage
- [Configuration Reference](configuration.md) - Complete settings documentation

### Core Features
- [Dual-Phase Hooks](hooks.md) - Success and failure hook system
- [Retry Policies](retry.md) - Backoff strategies and retry configuration
- [Multithreaded Workers](threading.md) - Concurrency for I/O-bound tasks

### Advanced Topics
- [Architecture](architecture.md) - System design and extension patterns
- [Development Guide](development.md) - Contributing and local development

### Additional Resources
- [Testing Guide](../tests/README.md) - Running and writing tests
- [Demo Application](../demo/README.md) - Interactive feature demonstrations
- [Future Features](future/) - Planned enhancements

## Documentation Overview

### What is Django-Qraft?

Django-Qraft is a next-generation distributed task queue for Django, built as a drop-in enhancement of [Django-Q2](https://django-q2.readthedocs.io/). It extends Django-Q2 with advanced features while maintaining full compatibility.

**Key enhancements:**
- Dual-phase hooks with separate success and failure handlers
- Rich retry policies with multiple backoff strategies
- Optional multithreaded workers for I/O-bound workloads
- Async hook execution to prevent monitor bottlenecks

### Who Should Use Django-Qraft?

Django-Qraft is ideal for projects that:

- **Need reliable task retries** with sophisticated backoff strategies
- **Have mixed workloads** (CPU-bound and I/O-bound tasks)
- **Want better hook control** than Django-Q2's single hook system
- **Require high concurrency** for I/O operations without spawning many processes
- **Already use Django-Q2** and want enhanced features without breaking changes

### Documentation Structure

#### 1. [Getting Started](getting-started.md)
Your first stop for installation and basic usage. Covers:
- Installation via pip/uv
- Django configuration
- Starting the cluster
- Creating your first task

#### 2. [Configuration](configuration.md)
Complete reference for all settings. Covers:
- `QRAFT_CLUSTER` settings
- Threading configuration
- Retry defaults
- ALT_CLUSTERS for mixed workloads
- Environment-based selection

#### 3. [Hooks](hooks.md)
Deep dive into the dual-phase hook system. Covers:
- Success and failure hooks
- Hook arguments and kwargs
- Async vs sync hook execution
- Hook tracking with `HookDispatch`
- Idempotency guarantees

#### 4. [Retry Policies](retry.md)
Comprehensive guide to retry configuration. Covers:
- Retry policy options
- Backoff strategies (exponential, linear, fixed)
- Jitter configuration
- Exception filtering
- Retry exhaustion handling
- Manual retry scheduling

#### 5. [Multithreaded Workers](threading.md)
Complete guide to thread-based concurrency. Covers:
- When to use threading
- Thread pool configuration
- Backpressure control
- Database connection management
- Timeout behavior
- Mixed worker pools
- Performance tuning

#### 6. [Architecture](architecture.md)
System design and internal structure. Covers:
- Extension pattern (selective override)
- Database models and relationships
- Task lifecycle and dual lookup
- Hook dispatching flow
- Configuration system
- Worker spawning

#### 7. [Development Guide](development.md)
For contributors and advanced users. Covers:
- Local development setup
- Running tests
- Code style guidelines
- Common development tasks
- Architecture guidelines
- Testing strategy

## Common Use Cases

### Use Case 1: API Calls with Retries

```python
from qraft.tasks import async_task

# Queue API call with exponential backoff
async_task(
    'myapp.tasks.call_external_api',
    api_url='https://api.example.com/data',
    qraft_options={
        'max_attempts': 5,
        'base_delay': 10,
        'backoff_strategy': 'exponential',
        'retry_exceptions': ['RequestException', 'Timeout'],
        'success_hook': 'myapp.hooks.on_api_success',
        'failure_hook': 'myapp.hooks.on_api_failure',
    }
)
```

See: [Retry Policies](retry.md), [Hooks](hooks.md)

### Use Case 2: Mixed CPU and I/O Workloads

```python
# settings.py
QRAFT_CLUSTER = {
    "workers": 4,
    "threads": 1,  # Standard workers for CPU-bound

    "ALT_CLUSTERS": {
        "io-workers": {
            "workers": 2,
            "threads": 8,  # Threaded workers for I/O-bound
            "max_inflight": 16,
        }
    }
}

# Route tasks appropriately
async_task('myapp.tasks.process_video')  # -> default (CPU)
async_task('myapp.tasks.fetch_data', cluster='io-workers')  # -> I/O
```

See: [Threading](threading.md), [Configuration](configuration.md)

### Use Case 3: Data Pipeline with Hooks

```python
# Chain tasks using success hooks
async_task(
    'myapp.tasks.extract_data',
    source_id=123,
    qraft_options={
        'success_hook': 'myapp.tasks.transform_data',
        'success_kwargs': {'pipeline_id': 'pipe-123'},
        'failure_hook': 'myapp.hooks.notify_failure',
        'max_attempts': 3,
    }
)
```

See: [Hooks](hooks.md)

## Quick Reference

### Essential Commands

```bash
# Start the cluster
python manage.py qraftcluster

# Start with alternative configuration
python manage.py qraftcluster --name io-workers
# Or: Q_CLUSTER_NAME=io-workers python manage.py qraftcluster

# Run tests
pytest

# Run demo scenarios
cd demo
python manage.py demo hooks
python manage.py demo retry
python manage.py demo perf
```

### Key Settings

```python
QRAFT_CLUSTER = {
    # Worker configuration
    "workers": 4,              # Number of worker processes
    "threads": 1,              # Threads per worker (1=disabled)
    "max_inflight": None,      # Max concurrent tasks (default: threads*2)
    "timeout": 60,             # Task timeout in seconds

    # Retry defaults
    "retry_defaults": {
        "max_attempts": 3,
        "delay": 30.0,
        "backoff": "exponential",
        "jitter": True,
    },

    # Hook behavior
    "sync_hooks": False,       # False=async (default), True=sync
    "grace_period": 30.0,      # Shutdown grace period
}
```

### Key Models

```python
from qraft.models import QraftTask, QraftTaskAttempt, HookDispatch

# Check task status
task = QraftTask.objects.get(id=task_id)
print(task.status)  # PENDING, RUNNING, SUCCEEDED, FAILED, EXHAUSTED

# View attempt history
attempts = task.attempts.all()
for attempt in attempts:
    print(f"Attempt {attempt.attempt_number}: {attempt.success}")

# Check hook execution
hooks = HookDispatch.objects.filter(qraft_task=task)
```

## Getting Help

### Resources
- [Main README](../README.md) - Project overview
- [CONTRIBUTING](../CONTRIBUTING.md) - How to contribute
- [CHANGELOG](../CHANGELOG.md) - Version history

### Support
- **Issues**: [GitHub Issues](https://github.com/yourusername/django-qraft/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/django-qraft/discussions)

## Next Steps

1. **New to Django-Qraft?** → Start with [Getting Started](getting-started.md)
2. **Migrating from Django-Q2?** → Check [Configuration](configuration.md) for compatibility
3. **Need retries?** → Read [Retry Policies](retry.md)
4. **Want better performance?** → See [Multithreaded Workers](threading.md)
5. **Contributing?** → Review [Development Guide](development.md)
