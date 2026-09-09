# Development Guide

This guide covers local setup, testing, code style, and common development tasks for contributing to Django-Qraft.

## Table of Contents

- [Local Setup](#local-setup)
- [Project Structure](#project-structure)
- [Running Tests](#running-tests)
- [Code Style](#code-style)
- [Testing Strategy](#testing-strategy)
- [Demo Application](#demo-application)
- [Common Development Tasks](#common-development-tasks)
- [Database Migrations](#database-migrations)
- [Performance Testing](#performance-testing)
- [Debugging](#debugging)
- [Contributing](#contributing)

## Local Setup

### Prerequisites

- Python 3.10 or higher
- uv (recommended) or pip
- Git

### Clone Repository

```bash
git clone https://github.com/ankitksr/django-qraft.git
cd django-qraft
```

### Install with uv (Recommended)

```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment
uv venv

# Activate virtual environment
source .venv/bin/activate  # On Linux/Mac
# or
.venv\Scripts\activate  # On Windows

# Install package in editable mode with dev dependencies
uv sync --group dev
```

### Install with pip

```bash
# Create virtual environment
python -m venv .venv

# Activate virtual environment
source .venv/bin/activate  # On Linux/Mac

# Install package in editable mode
pip install -e .
```

### Verify Installation

```bash
# Check imports work
python -c "from importlib.metadata import version; print(version('django-qraft'))"

# Run quick test
pytest tests/ -k test_basic --no-cov
```

## Project Structure

```
django-qraft/
├── qraft/                       # Main package
│   ├── __init__.py
│   ├── admin.py                # Django admin configuration for Qraft models
│   ├── apps.py                 # QraftConfig (AppConfig)
│   ├── backend.py              # QraftTaskBackend, the django.tasks (DEP 14) backend for Django 6.0+
│   ├── base.py                 # Shared workflow patterns for chain/iter/batch
│   ├── batch.py                # QraftBatch wrapper for parallel heterogeneous workflows
│   ├── brokers.py              # broker_for_cluster(), RoutingBroker, QraftOrmBroker (priority lanes)
│   ├── chain.py                 # QraftChain wrapper for sequential workflows
│   ├── cluster.py               # QraftCluster and QraftSentinel (Django-Q2 extensions)
│   ├── conf.py                  # Pydantic settings with DjangoSettingsSource + ALT_CLUSTERS support
│   ├── context.py                # Per-attempt progress/cost reporting, run request budgets
│   ├── dashboard/                 # Staff-only monitoring UI and JSON metrics endpoints
│   │   ├── apps.py
│   │   ├── metrics.py            # Bounded metric queries for the dashboard
│   │   ├── templates/qraft_dashboard/dashboard.html
│   │   ├── urls.py
│   │   └── views.py
│   ├── dispatchers.py             # ChainDispatcher and ParallelDispatcher
│   ├── dlq.py                      # Dead-letter listing and requeue onto the same attempt series
│   ├── hooks.py                     # Global hook handler with workflow detection and routing
│   ├── iter.py                      # QraftIter wrapper for parallel homogeneous workflows
│   ├── lease.py                     # Execution lease; creates the attempt row for retries/requeues/deferred tasks
│   ├── logging.py                   # QraftContextFilter, stamps log records with the executing attempt's ids
│   ├── management/commands/qraftcluster.py   # `qraftcluster` management command
│   ├── metrics/                     # Sink protocol + emission points
│   │   ├── __init__.py              # Sink protocol, NullSink, label guard and health counter
│   │   ├── gauges.py                 # Backlog gauges
│   │   └── otel.py                   # OpenTelemetrySink
│   ├── migrations/                   # 15 migrations — see Database Migrations below
│   ├── models/                       # Database schema (modularized in v1.1.0)
│   │   ├── __init__.py               # Re-exports all
│   │   ├── hooks.py                   # HookDispatch, WorkflowHookDispatch
│   │   ├── mixins.py                   # WorkflowStatus enum, SubjectMixin, RunMemberMixin, WorkflowStatusMixin, WorkflowHookMixin
│   │   ├── runs.py                      # QraftRun, QraftRunStage, RunStatus/StageStatus/UnitType
│   │   ├── tasks.py                     # QraftTask, QraftTaskAttempt, RateBucket
│   │   └── workflows.py                  # Chain/Iter/Batch models
│   ├── pricing.py                        # Optional cost resolver over usage entries (schemaless; no migration)
│   ├── reaper.py                         # Orphan detection, routing replay, stall flagging, run overdue sweep
│   ├── results.py                        # Rich result objects for workflow primitives
│   ├── retention.py                      # Bounded pruning of settled rows (opt-in via retention_days)
│   ├── retry.py                          # RetryPolicy with backoff calculations + schedule_retry()
│   ├── runner.py                         # Worker-side entry point for every attempt
│   ├── runs.py                           # Run and stage lifecycle: start, bind, skip, cancel, abandon, settlement
│   ├── scheduler.py                      # Owned scheduling: SCHEDULED attempt rows + the claiming dispatcher
│   ├── signals.py                        # Signals Qraft emits about its own transitions
│   ├── tasks.py                          # Enhanced async_task() + _create_workflow_task() helper
│   ├── throttle.py                       # Shared Postgres token bucket for cross-worker rate limiting
│   ├── tracing.py                        # W3C trace propagation across the enqueue boundary
│   └── worker.py                         # threaded_worker() using ThreadPoolExecutor
│
├── tests/                       # Unit tests (see tests/README.md for details)
│   ├── conftest.py              # Shared fixtures and pytest configuration
│   ├── settings.py              # Django settings for tests
│   ├── urls.py                  # URLconf for the dashboard tests
│   ├── e2e_tasks.py             # Real task/hook functions the e2e tests execute
│   ├── test_models.py           # QraftTask, QraftTaskAttempt, HookDispatch
│   ├── test_tasks.py            # async_task(): validation, options, enqueue
│   ├── test_retry.py            # RetryPolicy: backoff, jitter, exception filtering
│   ├── test_hooks.py            # Hook handler, dual-phase dispatch, idempotency
│   ├── test_scheduler.py        # The dispatcher that owns delayed execution
│   ├── test_lease.py            # Execution lease and heartbeat
│   ├── test_reaper.py           # Orphan detection, replay, stall flagging
│   ├── test_dlq.py              # Dead-letter listing and requeue
│   ├── test_retention.py        # Bounded pruning of settled rows
│   ├── test_chain.py            # QraftChain sequential workflow
│   ├── test_iter.py             # QraftIter parallel fan-out
│   ├── test_batch.py            # QraftBatch fork-join
│   ├── test_approval.py         # Human-in-the-loop chain steps
│   ├── test_dispatchers.py      # Chain/parallel dispatcher races and idempotency
│   ├── test_worker.py           # threaded_worker loop and thread execution
│   ├── test_cluster.py          # QraftSentinel/QraftCluster worker selection
│   ├── test_commands.py         # qraftcluster management command
│   ├── test_brokers.py          # Priority lanes, per-cluster brokers, RoutingBroker
│   ├── test_throttle.py         # Shared token bucket
│   ├── test_context.py          # Progress and usage reporting
│   ├── test_backend.py          # django.tasks backend (skips below Django 6.0)
│   ├── test_conf.py             # Settings and ALT_CLUSTERS
│   ├── test_dashboard.py        # Bundled monitoring dashboard
│   ├── test_runs.py             # Binding rules, derived settlement, replay paths
│   ├── test_signals.py          # Send sites, id payloads, send_robust isolation
│   ├── test_metrics.py          # Emission points, label sets, gauge ownership
│   ├── test_logging.py          # QraftContextFilter inside and outside a task
│   ├── test_pricing.py          # Cost resolver, subset formula, coverage flags
│   ├── test_e2e.py              # Real broker + worker + monitor, nothing stubbed
│   ├── test_integration.py      # Cross-cutting flows with the enqueue seam mocked
│   ├── test_workflow_integration.py  # Workflow flows (integration marker)
│   └── test_postgres_concurrency.py  # Row-lock races; skipped unless run on Postgres
│
├── demo/                  # Integration tests
│   ├── manage.py
│   ├── showcase/
│   │   ├── tasks.py
│   │   ├── hooks.py
│   │   ├── scenarios/            # core, ai, bench, djangotasks, durability, workflows
│   │   └── management/
│   │       └── commands/
│   │           └── demo.py
│
├── docs/                  # Documentation
│   ├── getting-started.md
│   ├── architecture.md
│   ├── configuration.md
│   ├── threading.md
│   ├── hooks.md
│   ├── retry.md
│   └── development.md
│
├── pyproject.toml        # Package metadata
├── README.md
├── CLAUDE.md             # AI assistant guidelines
└── Makefile              # Common commands
```

## Running Tests

### Quick Test Run

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_retry.py

# Run specific test
pytest tests/test_retry.py::test_exponential_backoff

# Run tests matching pattern
pytest -k "retry"
```

### With Coverage

```bash
# Run with coverage report
pytest --cov=qraft --cov-report=html

# View HTML report
open htmlcov/index.html  # On Mac
# or
xdg-open htmlcov/index.html  # On Linux
```

### Parallel Execution

```bash
# Run tests in parallel (faster)
pytest -n auto

# Run with 4 workers
pytest -n 4
```

### Watch Mode (Development)

```bash
# Install pytest-watch
uv pip install pytest-watch

# Run tests on file changes
ptw
```

### Using Make Commands

```bash
# Run all tests
make test

# Run with coverage
make test-cov

# Run in parallel
make test-parallel

# Lint code
make lint

# Format code
make format

# Run all checks (lint + test)
make check
```

## Code Style

### Python Version

Use Python 3.12 syntax and features:

```python
# Modern type hints (no imports needed)
def process_data(items: list[dict[str, int]]) -> str | None:
    pass

# Pipe operator for unions
value: str | int | None = get_value()

# Match statements
match status:
    case "pending":
        return "Waiting"
    case "running":
        return "Processing"
    case _:
        return "Unknown"
```

### Type Hints

Use minimal, modern type hints:

```python
# Good: Inbuilt types
def my_function(items: list[str], count: int | None = None) -> dict[str, Any]:
    pass

# Avoid: Importing from typing
from typing import List, Dict, Optional  # Not needed in 3.12+
```

### Logging

Use lazy % formatting in logging:

```python
import logging

logger = logging.getLogger(__name__)

# Good: Lazy formatting
logger.debug("Task %s failed with %s", task_id, exception)
logger.info("Processing %d items", len(items))

# Avoid: f-strings or .format()
logger.debug(f"Task {task_id} failed")  # Evaluated even if not logged
```

### Docstrings

Use concise docstrings:

```python
def calculate_delay(self, attempt_number: int) -> float:
    """
    Calculate delay for the next retry attempt.

    Args:
        attempt_number: Current attempt number (1-based)

    Returns:
        Delay in seconds
    """
    pass
```

### Code Organization

**Keep functions focused:**

```python
# Good: Single responsibility
def should_retry(self, attempt, exception):
    if attempt >= self.max_attempts:
        return False
    return self._check_exception(exception)

def _check_exception(self, exception):
    if exception in self.skip_exceptions:
        return False
    if self.retry_exceptions:
        return exception in self.retry_exceptions
    return True

# Avoid: Too many responsibilities
def should_retry(self, attempt, exception):
    # 50 lines of logic mixing concerns
```

**Use meaningful names:**

```python
# Good
def calculate_exponential_backoff(attempt_number, base_delay):
    return base_delay * (2 ** (attempt_number - 1))

# Avoid
def calc(n, d):
    return d * (2 ** (n - 1))
```

### Django Patterns

**Use atomic transactions:**

```python
from django.db import transaction

with transaction.atomic():
    task.status = TaskStatus.FAILED
    task.save(update_fields=['status', 'updated_at'])
    schedule_retry(task)
```

**Use select_related/prefetch_related:**

```python
# Good: Avoid N+1 queries
attempt = QraftTaskAttempt.objects.select_related('qraft_task').get(id=id)

# Avoid: N+1 query
attempt = QraftTaskAttempt.objects.get(id=id)
task = attempt.qraft_task  # Extra query
```

**Close database connections in threads:**

```python
from django.db import close_old_connections

def execute_in_thread():
    close_old_connections()  # Before
    try:
        # ... work ...
        return result
    finally:
        close_old_connections()  # After
```

### Linting

```bash
# Run ruff linter
uv run ruff check qraft/

# Auto-fix issues
uv run ruff check --fix qraft/

# Check specific file
uv run ruff check qraft/retry.py
```

**Ruff configuration** (in `pyproject.toml`):

```toml
[tool.ruff]
target-version = "py310"
exclude = ["*/migrations/*", "tests/*"]

[tool.ruff.lint]
select = [
    "E",   # pycodestyle errors
    "F",   # pyflakes
    "I",   # isort
]
```

## Testing Strategy

### Unit Tests

Test individual components in isolation:

**Example: Testing retry policy**

```python
# tests/test_retry.py
import pytest
from qraft.retry import RetryPolicy

def test_exponential_backoff():
    policy = RetryPolicy(
        max_attempts=3,
        base_delay=10,
        backoff_strategy='exponential',
    )

    assert policy.calculate_delay(1) == 10   # 10 * 2^0
    assert policy.calculate_delay(2) == 20   # 10 * 2^1
    assert policy.calculate_delay(3) == 40   # 10 * 2^2

def test_should_retry_with_exception_filter():
    policy = RetryPolicy(
        max_attempts=3,
        retry_exceptions=['ConnectionError'],
    )

    assert policy.should_retry(1, 'ConnectionError') is True
    assert policy.should_retry(1, 'ValueError') is False
```

### End-to-End Tests

`tests/test_e2e.py` runs the whole path with nothing stubbed: `async_task()` writes a real
`OrmQ` row, Django-Q2's own `pusher`/`worker`/`monitor` loops run in-process, and the
`Task` row they save fires `qraft_hook_handler` through its normal `post_save` receiver.
Task functions live in `tests/e2e_tasks.py` because a retry re-imports them by dotted path.

Two things stand in for parts that cannot run inside a test process: the sentinel (process
spawning, recycling, timeout kills) is absent, and `dispatch_due()` is called directly
instead of polled by the dispatcher thread — which is also what makes backoff instant here.

**Example: retry really re-runs the task**

```python
def test_retry_reruns_the_task(broker):
    e2e_tasks.FAIL_BUDGET['flaky'] = 1
    async_task(
        'tests.e2e_tasks.flaky', 'beta',
        qraft_options={'max_attempts': 3, 'base_delay': 0,
                       'backoff_strategy': 'fixed', 'jitter': False},
    )

    run_once()                        # attempt 1 executes and fails
    assert QraftTask.objects.get().status == TaskStatus.PENDING
    retry = QraftTaskAttempt.objects.get(attempt_number=2)
    assert retry.state == AttemptState.SCHEDULED  # a row, not a queued message

    assert dispatch_due() == 1        # the dispatcher hands it to the broker
    run_once()                        # attempt 2 executes and succeeds
    assert QraftTask.objects.get().status == TaskStatus.SUCCEEDED
```

### Integration Tests

`tests/test_integration.py` and `tests/test_workflow_integration.py` cover component
interactions with the enqueue seam mocked, which keeps them fast and lets them assert on
call arguments. Reach for an end-to-end test instead whenever the thing under test is
whether the seam itself behaves.

### Test Database

Tests run against in-memory SQLite, declared in `tests/settings.py` (selected by
`--ds=tests.settings` in `pyproject.toml`):

```python
# tests/settings.py
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }
}
```

SQLite is fast but cannot express `SELECT ... FOR UPDATE SKIP LOCKED`, which several
concurrency paths use where the database offers it. Those paths fall back to their
compare-and-swap branch here, so the fallback is what the suite verifies; run the suite
against Postgres before trusting the locking behaviour of a change to `scheduler.py`,
`reaper.py`, or `dispatchers.py`.

### Fixtures

Use pytest fixtures for common setups:

```python
# tests/conftest.py
import pytest
from qraft.models import QraftTask

@pytest.fixture
def sample_task():
    """Create a sample QraftTask for testing."""
    task = QraftTask.objects.create(
        func='myapp.tasks.sample_task',
        task_args=[1, 2, 3],
        task_kwargs={'key': 'value'},
    )
    return task

@pytest.fixture
def retry_policy():
    """Create a sample retry policy."""
    return {
        'max_attempts': 3,
        'base_delay': 10,
        'backoff_strategy': 'exponential',
    }

# Use in tests
def test_something(sample_task, retry_policy):
    sample_task.retry_policy = retry_policy
    sample_task.save()
    # ... test logic
```

### Coverage Goals

- **Overall**: 85% today; CI fails the build below 72%
- **Critical paths**: 100% coverage (retry logic, hook dispatching)
- **Fast execution**: All tests should complete in <30 seconds

`qraft/admin.py` is not imported by the test settings (no `django.contrib.admin`), so it
never appears in the report; it is verified manually via the demo app. `qraft/backend.py`
reads 0% on Django 5.x, where its tests skip.

**Check coverage:**

```bash
# Generate coverage report
pytest --cov=qraft --cov-report=term-missing

# Show lines not covered
pytest --cov=qraft --cov-report=html
open htmlcov/index.html
```

## Demo Application

The demo app provides end-to-end testing and feature demonstrations.

### Setup

```bash
cd demo

# Create virtual environment
uv venv

# Install django-qraft in editable mode
uv pip install -e ..

# Run migrations
uv run python manage.py migrate
```

### Running Demo Scenarios

**Terminal 1: Start cluster**

```bash
cd demo
uv run python manage.py qraftcluster
```

**Terminal 2: Run demos**

```bash
# Test dual-phase hooks
uv run python manage.py demo hooks -n 5

# Test retry policies
uv run python manage.py demo retry --fail-times 2 --max-attempts 4

# Performance benchmark (requires 2 clusters)
uv run python manage.py demo perf -n 20 --duration 5.0
```

### Demo Tasks

Located in `demo/showcase/tasks.py`:

```python
def flaky_task(fail_rate=0.5):
    """Randomly fails based on fail_rate."""
    if random.random() < fail_rate:
        raise Exception("Random failure")
    return "Success"

def countdown_task(remaining_failures=2):
    """Fails N times then succeeds."""
    if remaining_failures > 0:
        raise Exception(f"{remaining_failures} failures remaining")
    return "Finally succeeded"

def slow_io_task(duration=1.0):
    """Simulates I/O-bound work."""
    time.sleep(duration)
    return f"Completed after {duration}s"
```

### Creating New Demo Scenarios

```python
# demo/showcase/management/commands/demo.py

class Command(BaseCommand):
    def add_arguments(self, parser):
        subparsers = parser.add_subparsers(dest='scenario')

        # Add new scenario
        my_scenario = subparsers.add_parser('my_scenario')
        my_scenario.add_argument('--count', type=int, default=10)

    def handle(self, *args, **options):
        scenario = options['scenario']

        if scenario == 'my_scenario':
            self.run_my_scenario(options['count'])

    def run_my_scenario(self, count):
        """My custom scenario."""
        for i in range(count):
            async_task('showcase.tasks.my_task', i)
```

## Common Development Tasks

### Adding a New Feature

1. **Create feature branch:**

   ```bash
   git checkout -b feature/my-feature
   ```

2. **Write tests first (TDD):**

   ```python
   # tests/test_my_feature.py
   def test_my_feature():
       # Test behavior
       assert expected == actual
   ```

3. **Implement feature:**

   ```python
   # qraft/my_feature.py
   def my_feature():
       # Implementation
       pass
   ```

4. **Run tests:**

   ```bash
   pytest tests/test_my_feature.py
   ```

5. **Update documentation:**

   ```markdown
   # docs/my-feature.md
   ```

6. **Lint and format:**

   ```bash
   make lint
   make format
   ```

7. **Commit and push:**

   ```bash
   git add .
   git commit -m "Add my feature"
   git push origin feature/my-feature
   ```

### Modifying Core Logic

See "When Editing Core Logic" in `CLAUDE.md` for what to test when changing
`cluster.py`, `hooks.py`, `retry.py`, or `worker.py`.

### Debugging Tests

**Run single test with output:**

```bash
pytest tests/test_retry.py::test_exponential_backoff -v -s
```

**Drop into debugger on failure:**

```bash
pytest --pdb tests/test_retry.py
```

**Set breakpoint in code:**

```python
def my_function():
    import pdb; pdb.set_trace()  # Debugger breakpoint
    # ... code
```

**Use pytest fixtures for debugging:**

```python
@pytest.fixture
def debug_mode():
    import logging
    logging.basicConfig(level=logging.DEBUG)
    yield
    logging.basicConfig(level=logging.WARNING)

def test_with_debug(debug_mode):
    # Test runs with DEBUG logging
    pass
```

## Database Migrations

### Creating Migrations

```bash
# Navigate to demo project
cd demo

# Create migration
uv run python manage.py makemigrations qraft

# Review migration file
cat qraft/migrations/0003_new_migration.py
```

### Migration Guidelines

1. **Test migrations both ways:**

   ```bash
   # Forward
   python manage.py migrate qraft

   # Backward
   python manage.py migrate qraft 0002_previous_migration
   ```

2. **Ensure backward compatibility:**

   - Don't remove fields that existing tasks reference
   - Use default values for new required fields
   - Add migration for data transformation if needed

3. **Document breaking changes:**

   ```python
   # migrations/0003_breaking_change.py
   """
   BREAKING CHANGE: Removes deprecated field `old_field`.

   Before upgrading:
   1. Ensure all tasks using `old_field` are completed
   2. Run: python manage.py migrate_old_field_data
   """
   ```

### Migration History

See "Database Migrations" in `CLAUDE.md` for the annotated list of migrations.

Generate a migration with `makemigrations`; never hand-author one. Every column
added since 1.4.0 is nullable or defaulted (`db_default` where a Python default
would not survive it), so a rolling deploy — where the previous release is
still inserting rows on the old schema — keeps working.

## Performance Testing

### Benchmark Script

```python
# scripts/benchmark.py
import time
from qraft.tasks import async_task

def benchmark_throughput(n_tasks=100):
    """Measure task throughput."""
    start = time.time()

    # Queue tasks
    task_ids = []
    for i in range(n_tasks):
        task_id = async_task('showcase.tasks.fast_task', i)
        task_ids.append(task_id)

    # Wait for completion
    # ... (polling logic)

    elapsed = time.time() - start
    throughput = n_tasks / elapsed

    print(f"Throughput: {throughput:.2f} tasks/sec")
    return throughput

if __name__ == '__main__':
    benchmark_throughput(100)
```

### Load Testing

```bash
# Generate high load
for i in {1..1000}; do
    python manage.py shell -c "
from qraft.tasks import async_task
async_task('showcase.tasks.io_task', $i)
"
done

# Monitor cluster
watch -n 1 'ps aux | grep qraftcluster'
```

### Profiling

**Profile a specific function:**

```python
import cProfile
import pstats

def profile_function():
    profiler = cProfile.Profile()
    profiler.enable()

    # Code to profile
    my_function()

    profiler.disable()
    stats = pstats.Stats(profiler)
    stats.sort_stats('cumulative')
    stats.print_stats(20)  # Top 20 slowest

profile_function()
```

**Profile with py-spy:**

```bash
# Install py-spy
pip install py-spy

# Profile running cluster
py-spy top --pid $(pgrep -f qraftcluster)

# Generate flamegraph
py-spy record -o profile.svg -- python manage.py qraftcluster --run-once
```

## Debugging

### Enable Debug Logging

```python
# settings.py
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
        },
    },
    'loggers': {
        'qraft': {
            'handlers': ['console'],
            'level': 'DEBUG',
        },
        'django-q': {
            'handlers': ['console'],
            'level': 'DEBUG',
        },
    },
}
```

### Debugging Workers

**Run cluster in foreground:**

```bash
python manage.py qraftcluster
# Ctrl+C to stop, see all output
```

**Run once for testing:**

```bash
python manage.py qraftcluster --run-once
# Processes one task and exits
```

**Attach debugger to worker:**

```python
# In task code
def my_task():
    import debugpy
    debugpy.listen(5678)
    debugpy.wait_for_client()  # Pause here
    # ... task code
```

### Common Issues

**Tasks stuck in RUNNING:**

Check worker logs for timeout errors:

```bash
python manage.py qraftcluster | grep -i timeout
```

**Database connection errors:**

Increase connection pool:

```python
DATABASES = {
    'default': {
        'OPTIONS': {'MAX_CONNS': 200},
    }
}
```

**Import errors in tasks:**

Test import manually:

```bash
python manage.py shell
>>> from myapp.tasks import my_task  # Should not raise ImportError
```

## Contributing

### Pull Request Checklist

- [ ] Tests added for new features
- [ ] Existing tests pass (`make test`)
- [ ] Code linted (`make lint`)
- [ ] Documentation updated
- [ ] CHANGELOG.md updated
- [ ] Commit messages are clear

### Commit Message Format

Not Conventional Commits — no `feat:`/`fix:` prefixes. Prefix with the
app or area instead (`Scheduler: ...`, `Dashboard: ...`).

- Default to a title only.
- Upgrade to title + one short paragraph only when the behaviour change
  is not obvious from the title.
- Upgrade to 2-3 bullets only when the change has genuinely separable
  axes. Never more than 3, and never bullet diff-readable mechanics.
- No AI attribution or `Co-Authored-By` lines.

**Examples:**

```
Scheduler: Add linear backoff strategy

Retry: Handle a missing exception class without crashing the hook handler
```

### Release Process

1. **Update version** in `pyproject.toml`
2. **Update CHANGELOG.md** with changes
3. **Run full test suite:**

   ```bash
   make test-cov
   make lint
   ```

4. **Build package:**

   ```bash
   python -m build
   ```

5. **Test package:**

   ```bash
   pip install dist/django_qraft-*.whl
   ```

6. **Create git tag:**

   ```bash
   git tag v1.2.3
   git push origin v1.2.3
   ```

7. **Publish to PyPI:**

   ```bash
   python -m twine upload dist/*
   ```

## Related Documentation

- [Getting Started](getting-started.md) - Installation and basic usage
- [Architecture](architecture.md) - System design and internals
- [Configuration](configuration.md) - Settings reference
- [Testing README](../tests/README.md) - Detailed testing documentation
