# Contributing to Django-Qraft

Thank you for your interest in contributing to Django-Qraft! This document provides guidelines and instructions for contributing to the project.

## Code of Conduct

By participating in this project, you agree to maintain a respectful and collaborative environment for all contributors.

## Getting Started

### Development Setup

1. **Clone the repository**

```bash
git clone https://github.com/ankitksr/django-qraft.git
cd django-qraft
```

2. **Install uv (recommended package manager)**

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

3. **Create a virtual environment and install dependencies**

```bash
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv sync --group test
```

4. **Run migrations (for demo app)**

```bash
cd demo
uv run python manage.py migrate
```

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=qraft --cov-report=html

# Run in parallel
uv run pytest -n auto

# Run specific test file
uv run pytest tests/test_models.py
```

See [tests/README.md](tests/README.md) for detailed testing documentation.

### Running the Demo

```bash
cd demo

# Start the cluster (Terminal 1)
uv run python manage.py qraftcluster

# Run demos (Terminal 2)
uv run python manage.py demo hooks
uv run python manage.py demo retry
```

See [demo/README.md](demo/README.md) for more demo scenarios.

## Development Workflow

### 1. Create a Branch

```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/your-bug-fix
```

### 2. Make Your Changes

- Follow the code style guidelines (see below)
- Add tests for new functionality
- Update documentation as needed
- Ensure all tests pass

### 3. Run Code Quality Checks

```bash
# Lint your code
uv run ruff check qraft/

# Auto-fix linting issues
uv run ruff check --fix qraft/

# Run tests
uv run pytest
```

### 4. Commit Your Changes

Follow conventional commit format:

```bash
git commit -m "feat: add new retry backoff strategy"
git commit -m "fix: resolve hook dispatch race condition"
git commit -m "docs: update threading configuration guide"
git commit -m "test: add tests for retry exhaustion"
```

**Commit message prefixes:**
- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation changes
- `test:` - Test additions or modifications
- `refactor:` - Code refactoring
- `perf:` - Performance improvements
- `chore:` - Maintenance tasks

### 5. Push and Create a Pull Request

```bash
git push origin feature/your-feature-name
```

Then create a pull request on GitHub with a clear description of your changes.

## Code Style Guidelines

### Python Code

Django-Qraft follows modern Python conventions:

- **Python version**: 3.9+ syntax
- **Style**: PEP 8 compliant (enforced by ruff)
- **Type hints**: Minimal, using modern syntax (built-in types, pipe operators)
- **Docstrings**: For public APIs and complex functions
- **Line length**: 120 characters (ruff default)

### Code Patterns

**Logging:**
```python
# Use lazy % formatting
logger.debug("Processing task %s with %d attempts", task_id, attempt)
```

**Database Operations:**
```python
# Use atomic transactions for critical updates
from django.db import transaction

with transaction.atomic():
    task.status = TaskStatus.RUNNING
    task.save()
```

**JSON Fields:**
```python
# Use DjangoJSONEncoder for all JSON fields
from django.core.serializers.json import DjangoJSONEncoder

data = json.dumps(obj, cls=DjangoJSONEncoder)
```

## Testing Guidelines

### Writing Tests

1. **Use pytest, not unittest**

```python
# Good
def test_retry_policy_calculates_delay():
    policy = RetryPolicy(base_delay=10, backoff="exponential")
    delay = policy.calculate_delay(attempt=2)
    assert delay == 40  # 10 * 2^2

# Avoid
class TestRetryPolicy(unittest.TestCase):
    def test_retry_policy_calculates_delay(self):
        ...
```

2. **Use descriptive test names**

```python
# Good
def test_task_transitions_to_exhausted_after_max_attempts()

# Avoid
def test_task_status()
```

3. **Use fixtures for setup**

```python
@pytest.fixture
def qraft_task(db):
    return QraftTask.objects.create(
        func="test.function",
        task_args=[],
        task_kwargs={}
    )

def test_task_creation(qraft_task):
    assert qraft_task.status == TaskStatus.PENDING
```

4. **Test one thing per test**

```python
# Good - separate tests
def test_success_hook_is_called_on_success():
    ...

def test_failure_hook_is_called_on_failure():
    ...

# Avoid - testing multiple things
def test_hooks():
    # tests both success and failure hooks
    ...
```

### Test Coverage

- Aim for >90% coverage on new code
- 100% coverage for critical paths (retry logic, hook dispatching)
- Integration tests for worker/cluster interactions
- Unit tests for business logic

## Documentation Guidelines

### Where to Document

- **README.md**: Project overview and quick start
- **docs/**: Detailed guides and reference documentation
- **Code comments**: For complex logic or non-obvious decisions
- **Docstrings**: For public APIs

### Documentation Style

- Use clear, concise language
- Provide code examples for features
- Include both simple and advanced usage patterns
- Link to related documentation

### Updating Documentation

When adding features, update:

1. Relevant guide in `docs/`
2. README.md if it affects the quick start
3. CHANGELOG.md with the change
4. Code examples in `demo/` if applicable

## Pull Request Guidelines

### Before Submitting

- [ ] All tests pass (`uv run pytest`)
- [ ] Code is linted (`uv run ruff check qraft/`)
- [ ] New features have tests
- [ ] Documentation is updated
- [ ] CHANGELOG.md is updated (for notable changes)
- [ ] Commit messages follow conventional format

### PR Description Template

```markdown
## Description
Brief description of what this PR does.

## Motivation
Why is this change needed? What problem does it solve?

## Changes
- List of key changes
- Another change

## Testing
How was this tested? Include test output if relevant.

## Documentation
- [ ] Updated relevant documentation
- [ ] Added/updated code examples
- [ ] Updated CHANGELOG.md

## Checklist
- [ ] Tests pass
- [ ] Code is linted
- [ ] Documentation updated
```

## Reporting Issues

### Bug Reports

When reporting bugs, include:

1. **Description**: Clear description of the bug
2. **Steps to reproduce**: Minimal steps to reproduce the issue
3. **Expected behavior**: What you expected to happen
4. **Actual behavior**: What actually happened
5. **Environment**: Python version, Django version, Django-Q2 version
6. **Logs**: Relevant error messages or stack traces

### Feature Requests

When requesting features, include:

1. **Use case**: What problem does this solve?
2. **Proposed solution**: How would you like it to work?
3. **Alternatives**: What alternatives have you considered?
4. **Examples**: Code examples of how it would be used

## Architecture Guidelines

### Design Principles

1. **Drop-in compatibility**: Maintain compatibility with Django-Q2
2. **Selective inheritance**: Override only what's necessary
3. **Minimal type hints**: Use modern Python syntax, avoid over-typing
4. **Performance-conscious**: Consider thread safety and database connections
5. **Idempotent operations**: Use unique constraints and atomic transactions

### Key Extension Points

- `QraftCluster` → extends `Cluster`
- `QraftSentinel` → extends `Sentinel`
- `threaded_worker()` → alternative to standard worker
- Hook dispatcher → intercepts Django-Q2 hook execution

See [docs/architecture.md](docs/architecture.md) for detailed architecture documentation.

## Release Process

(For maintainers)

1. Update the version in `pyproject.toml`.
2. Move the `[Unreleased]` section of `CHANGELOG.md` under the new version and date it.
3. Push a tag: `git tag v1.4.0 && git push origin v1.4.0`.

The tag triggers `.github/workflows/release.yml`, which builds the distributions, installs
the wheel into a clean environment and runs `tests/wheel_smoke.py` against it, then
publishes to PyPI through Trusted Publishing. There is no API token to hold.

To rehearse first, run the same workflow from the Actions tab with the target `testpypi`.

## Getting Help

- **Documentation**: [docs/](docs/)
- **Issues**: [GitHub Issues](https://github.com/ankitksr/django-qraft/issues)
- **Discussions**: [GitHub Discussions](https://github.com/ankitksr/django-qraft/discussions)

## License

By contributing to Django-Qraft, you agree that your contributions will be licensed under the MIT License.
