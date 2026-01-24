# Django-Qraft Tests

Comprehensive unit test suite for django-qraft using modern Python testing practices.

## Quick Start

### Install Test Dependencies

```bash
# Using uv (recommended)
uv pip install --group test

# Or using pip
pip install -e ".[test]"
```

### Run All Tests

```bash
# Using uv
uv run pytest

# Or directly with pytest
pytest
```

## Test Structure

```
tests/
├── __init__.py           # Package marker
├── conftest.py          # Shared fixtures and pytest configuration
├── test_settings.py     # Django settings for tests
├── test_models.py       # Tests for QraftTask, QraftTaskAttempt, HookDispatch
├── test_retry.py        # Tests for RetryPolicy and retry logic
├── test_tasks.py        # Tests for async_task function
├── test_hooks.py        # Tests for hook dispatching
└── test_conf.py         # Tests for configuration system
```

## Running Tests

### Basic Usage

```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/test_models.py

# Run specific test class
uv run pytest tests/test_models.py::TestQraftTask

# Run specific test function
uv run pytest tests/test_models.py::TestQraftTask::test_create_minimal_task

# Run tests matching a pattern
uv run pytest -k "retry"
```

### Parallel Execution

```bash
# Run tests in parallel using all CPU cores
uv run pytest -n auto

# Run tests using 4 workers
uv run pytest -n 4
```

### Coverage Reports

```bash
# Run tests with coverage
uv run pytest --cov=qraft

# Generate HTML coverage report
uv run pytest --cov=qraft --cov-report=html

# View coverage report
open htmlcov/index.html
```

### Verbose Output

```bash
# Show detailed test output
uv run pytest -v

# Show even more detail (print statements, etc.)
uv run pytest -vv -s
```

### Debugging

```bash
# Stop on first failure
uv run pytest -x

# Drop into debugger on failure
uv run pytest --pdb

# Show local variables in tracebacks
uv run pytest -l
```

## Test Organization

### Models Tests (`test_models.py`)

Tests for the database models:
- **TestQraftTask**: Task creation, status transitions, properties
- **TestQraftTaskAttempt**: Attempt tracking, uniqueness constraints
- **TestHookDispatch**: Hook idempotency, cascade deletion

### Retry Tests (`test_retry.py`)

Tests for retry logic:
- **TestRetryPolicy**: Policy initialization, validation, backoff calculations
- Delay calculations for fixed/linear/exponential strategies
- Exception filtering (retry_exceptions, skip_exceptions)
- Retry scheduling with Django-Q2 Schedule

### Tasks Tests (`test_tasks.py`)

Tests for task creation:
- **TestAsyncTask**: Task queueing, hook configuration, retry setup
- Django-Q2 parameter passing
- Legacy hook migration
- Task name preservation

### Hooks Tests (`test_hooks.py`)

Tests for hook dispatching:
- **TestExtractExceptionClass**: Exception parsing from tracebacks
- **TestParseQraftMarker**: Task name parsing for retries
- **TestQraftHookHandler**: Hook handler logic, dual lookup
- **TestHookDispatcher**: Hook dispatching, retry handling, idempotency

### Configuration Tests (`test_conf.py`)

Tests for settings management:
- **TestRetrySettings**: Retry configuration validation
- **TestQraftSettings**: Main settings, threading configuration
- **TestDjangoSettingsIntegration**: Django settings loading, ALT_CLUSTERS
- **TestGetConf**: Dynamic configuration

## Fixtures

Common fixtures available in all tests (defined in `conftest.py`):

- `qraft_task`: A test QraftTask instance
- `qraft_task_attempt`: A test QraftTaskAttempt instance
- `mock_q2_task_success`: Mock successful Django-Q2 task
- `mock_q2_task_failure`: Mock failed Django-Q2 task
- `retry_policy`: Test RetryPolicy instance

## Testing Philosophy

### Modern Practices

1. **pytest over unittest**: We use pytest for better readability and powerful fixtures
2. **Fixtures over setUp/tearDown**: Composable dependency injection
3. **Minimal mocking**: Test real behavior; mock only external dependencies
4. **Fast tests**: In-memory SQLite database, optimized for speed
5. **Isolated tests**: Each test is independent and can run in any order

### Database Testing

All database tests use `@pytest.mark.django_db`:

```python
@pytest.mark.django_db
class TestQraftTask:
    def test_create_task(self):
        task = QraftTask.objects.create(func="test.function")
        assert task.id is not None
```

The `--reuse-db` flag (enabled by default) reuses the test database between runs for speed.

### Mocking

We mock external dependencies but test real code paths:

```python
@patch("qraft.tasks.q2_async_task")
def test_async_task(mock_q2_async):
    mock_q2_async.return_value = "task-id"
    # Test real async_task logic
    result = async_task("test.function")
    assert result == "task-id"
```

## Coverage Goals

We aim for:
- **>90% code coverage** overall
- **100% coverage** for critical paths (retry logic, hook dispatching)
- **Clear test names** that document behavior
- **Minimal but effective** test cases

## Continuous Integration

These tests are designed to run in CI environments:

```bash
# CI-friendly command
uv run pytest --cov=qraft --cov-report=xml --cov-report=term -n auto
```

## Contributing

When adding new features:

1. Write tests first (TDD approach recommended)
2. Ensure all tests pass: `uv run pytest`
3. Check coverage: `uv run pytest --cov=qraft`
4. Run linter: `uv run ruff check qraft/`

## Troubleshooting

### Import Errors

If you get import errors, ensure the package is installed:

```bash
uv pip install -e .
```

### Database Errors

If you get database errors, try recreating the test database:

```bash
uv run pytest --create-db
```

### Fixture Not Found

Ensure `conftest.py` is in the tests directory and pytest can discover it.

## Resources

- [pytest documentation](https://docs.pytest.org/)
- [pytest-django documentation](https://pytest-django.readthedocs.io/)
- [pytest-cov documentation](https://pytest-cov.readthedocs.io/)
