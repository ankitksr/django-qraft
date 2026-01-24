# Retry Policies

Django-Qraft provides rich retry policies with multiple backoff strategies, exception filtering, jitter, and seamless integration with the hook system.

## Table of Contents

- [Overview](#overview)
- [Basic Usage](#basic-usage)
- [Backoff Strategies](#backoff-strategies)
- [Exception Filtering](#exception-filtering)
- [Jitter](#jitter)
- [Default Retry Policy](#default-retry-policy)
- [Retry Scheduling](#retry-scheduling)
- [Exhaustion Handling](#exhaustion-handling)
- [Monitoring Retries](#monitoring-retries)
- [Use Cases and Patterns](#use-cases-and-patterns)
- [Advanced Topics](#advanced-topics)
- [Troubleshooting](#troubleshooting)

## Overview

Django-Qraft's retry system provides:

- **Multiple backoff strategies**: Exponential, linear, or fixed delays
- **Jitter support**: Random variation to prevent thundering herd
- **Exception filtering**: Retry only specific exceptions
- **Hook integration**: Failure hooks called only after exhaustion
- **Transparent scheduling**: Retries use Django-Q2's Schedule model

**Key concepts:**

- **Attempt**: Each execution of a task (initial + retries)
- **max_attempts**: Total attempts including the initial one
- **Backoff**: Strategy for increasing delay between retries
- **Exhaustion**: When all retry attempts are consumed
- **Jitter**: Random variation added to delays

## Basic Usage

### Simple Retry

```python
from qraft.tasks import async_task

# Retry up to 3 times with exponential backoff
task_id = async_task(
    'myapp.tasks.flaky_api_call',
    url='https://api.example.com/data',
    qraft_options={
        'max_attempts': 3,
    }
)
```

**Execution timeline:**
- Attempt 1 (immediate): Fails
- Wait 30s (default delay)
- Attempt 2: Fails
- Wait 60s (exponential backoff)
- Attempt 3: Succeeds ✓

### Custom Backoff

```python
task_id = async_task(
    'myapp.tasks.api_call',
    url,
    qraft_options={
        'max_attempts': 5,
        'base_delay': 10,  # Start with 10 second delay
        'backoff_strategy': 'linear',
    }
)
```

**Execution timeline:**
- Attempt 1: Fails
- Wait 10s
- Attempt 2: Fails
- Wait 20s
- Attempt 3: Fails
- Wait 30s
- Attempt 4: Fails
- Wait 40s
- Attempt 5: Succeeds ✓

### Complete Configuration

```python
task_id = async_task(
    'myapp.tasks.external_service',
    data,
    qraft_options={
        'max_attempts': 5,
        'base_delay': 30,
        'backoff_strategy': 'exponential',
        'jitter': True,
        'jitter_max': 0.2,
        'retry_exceptions': ['ConnectionError', 'Timeout'],
        'skip_exceptions': ['AuthenticationError'],
    }
)
```

## Backoff Strategies

Django-Qraft supports three backoff strategies for calculating retry delays.

### Exponential Backoff (Default)

Delay doubles with each attempt: `delay = base_delay * 2^(attempt-1)`

```python
qraft_options={
    'max_attempts': 5,
    'base_delay': 30,
    'backoff_strategy': 'exponential',
}
```

**Delay sequence (base_delay=30):**

| Attempt | Formula | Delay |
|---------|---------|-------|
| 1 | Initial | 0s |
| 2 | 30 * 2^0 | 30s |
| 3 | 30 * 2^1 | 60s |
| 4 | 30 * 2^2 | 120s |
| 5 | 30 * 2^3 | 240s |

**Total time:** 450 seconds (7.5 minutes)

**Best for:**
- Transient failures (network issues, rate limits)
- Services that need time to recover
- Avoiding overwhelming failing services

**Example:**

```python
task_id = async_task(
    'myapp.tasks.api_call_with_rate_limit',
    url,
    qraft_options={
        'max_attempts': 4,
        'base_delay': 60,  # Start with 1 minute
        'backoff_strategy': 'exponential',
    }
)
# Retries after: 1min, 2min, 4min
```

### Linear Backoff

Delay increases linearly: `delay = base_delay * attempt`

```python
qraft_options={
    'max_attempts': 5,
    'base_delay': 30,
    'backoff_strategy': 'linear',
}
```

**Delay sequence (base_delay=30):**

| Attempt | Formula | Delay |
|---------|---------|-------|
| 1 | Initial | 0s |
| 2 | 30 * 1 | 30s |
| 3 | 30 * 2 | 60s |
| 4 | 30 * 3 | 90s |
| 5 | 30 * 4 | 120s |

**Total time:** 300 seconds (5 minutes)

**Best for:**
- Predictable retry intervals
- Services with consistent recovery time
- Moderate backpressure

**Example:**

```python
task_id = async_task(
    'myapp.tasks.database_sync',
    records,
    qraft_options={
        'max_attempts': 3,
        'base_delay': 45,
        'backoff_strategy': 'linear',
    }
)
# Retries after: 45s, 90s
```

### Fixed Delay

Constant delay between retries: `delay = base_delay`

```python
qraft_options={
    'max_attempts': 5,
    'base_delay': 30,
    'backoff_strategy': 'fixed',
}
```

**Delay sequence (base_delay=30):**

| Attempt | Formula | Delay |
|---------|---------|-------|
| 1 | Initial | 0s |
| 2 | 30 | 30s |
| 3 | 30 | 30s |
| 4 | 30 | 30s |
| 5 | 30 | 30s |

**Total time:** 120 seconds (2 minutes)

**Best for:**
- Quick retries for intermittent failures
- Services with fast recovery
- High-frequency polling

**Example:**

```python
task_id = async_task(
    'myapp.tasks.check_status',
    job_id,
    qraft_options={
        'max_attempts': 6,
        'base_delay': 10,  # Check every 10 seconds
        'backoff_strategy': 'fixed',
    }
)
# Retries after: 10s, 10s, 10s, 10s, 10s
```

### Strategy Comparison

| Strategy | Growth Rate | Total Time (5 attempts, base=30) | Best For |
|----------|-------------|----------------------------------|----------|
| Exponential | 2^n | 450s (7.5m) | Transient failures, rate limits |
| Linear | n | 300s (5m) | Predictable intervals |
| Fixed | 1 | 120s (2m) | Quick retries, status polling |

## Exception Filtering

Control which exceptions trigger retries using `retry_exceptions` and `skip_exceptions`.

### Retry Specific Exceptions

Only retry for specified exceptions:

```python
task_id = async_task(
    'myapp.tasks.external_api',
    url,
    qraft_options={
        'max_attempts': 3,
        'retry_exceptions': ['ConnectionError', 'Timeout', 'RequestException'],
    }
)
```

**Behavior:**
- `ConnectionError` → Retry
- `Timeout` → Retry
- `RequestException` → Retry
- `ValueError` → Fail immediately (no retry)
- `AuthenticationError` → Fail immediately (no retry)

### Skip Specific Exceptions

Retry all exceptions except specified:

```python
task_id = async_task(
    'myapp.tasks.process_data',
    data,
    qraft_options={
        'max_attempts': 3,
        'skip_exceptions': ['ValidationError', 'PermissionDenied'],
    }
)
```

**Behavior:**
- `ValidationError` → Fail immediately (no retry)
- `PermissionDenied` → Fail immediately (no retry)
- Any other exception → Retry

### Combined Filtering

```python
task_id = async_task(
    'myapp.tasks.complex_operation',
    data,
    qraft_options={
        'max_attempts': 5,
        # Only retry these...
        'retry_exceptions': ['ConnectionError', 'Timeout', 'TemporaryError'],
        # ...except these (takes precedence)
        'skip_exceptions': ['FatalError'],
    }
)
```

**Behavior:**
- `ConnectionError` → Retry (in retry_exceptions, not in skip_exceptions)
- `FatalError` → Fail immediately (in skip_exceptions)
- `ValueError` → Fail immediately (not in retry_exceptions)

**Precedence:** `skip_exceptions` takes precedence over `retry_exceptions`

### Exception Name Matching

Exception names are matched as strings (class name only, not full path):

```python
# In your task
class CustomError(Exception):
    pass

def my_task():
    raise CustomError("Something went wrong")

# In retry config
qraft_options={
    'retry_exceptions': ['CustomError'],  # Match by class name
}
```

**Note:** Only the exception class name is used, not the full module path.

### Real-World Examples

**API calls (retry network errors only):**

```python
task_id = async_task(
    'myapp.tasks.fetch_api',
    url,
    qraft_options={
        'max_attempts': 5,
        'base_delay': 30,
        'backoff_strategy': 'exponential',
        'retry_exceptions': [
            'ConnectionError',
            'Timeout',
            'HTTPError',  # 5xx errors
        ],
        'skip_exceptions': [
            'AuthenticationError',  # Don't retry auth failures
            'NotFoundError',  # Don't retry 404s
        ],
    }
)
```

**File processing (retry transient I/O errors):**

```python
task_id = async_task(
    'myapp.tasks.process_file',
    file_path,
    qraft_options={
        'max_attempts': 3,
        'base_delay': 10,
        'retry_exceptions': [
            'IOError',
            'OSError',
            'PermissionError',
        ],
        'skip_exceptions': [
            'FileNotFoundError',  # Don't retry missing files
            'ValidationError',  # Don't retry invalid files
        ],
    }
)
```

## Jitter

Jitter adds random variation to retry delays to prevent thundering herd problems.

### What is Jitter?

Without jitter, all failed tasks retry at the same time:

```
100 tasks fail at T=0
  ↓
All retry at T=30s (thundering herd!)
  ↓
All retry at T=60s (thundering herd!)
```

With jitter, retries are spread out:

```
100 tasks fail at T=0
  ↓
Retry between T=24s-36s (spread out)
  ↓
Retry between T=48s-72s (spread out)
```

### Configuration

```python
qraft_options={
    'base_delay': 30,
    'jitter': True,  # Enable jitter (default)
    'jitter_max': 0.2,  # ±20% variation (default)
}
```

**Delay calculation:**

```python
# Base delay
delay = 30

# Jitter amount (20% of delay)
jitter_amount = 30 * 0.2 = 6

# Random jitter between -6 and +6
jitter = random.uniform(-6, 6)

# Final delay (min 1 second)
final_delay = max(1, 30 + jitter)
# Range: 24-36 seconds
```

### Jitter Examples

**Low jitter (10%):**

```python
qraft_options={
    'base_delay': 60,
    'jitter': True,
    'jitter_max': 0.1,  # ±10%
}
# Delay range: 54-66 seconds
```

**High jitter (50%):**

```python
qraft_options={
    'base_delay': 60,
    'jitter': True,
    'jitter_max': 0.5,  # ±50%
}
# Delay range: 30-90 seconds
```

**No jitter:**

```python
qraft_options={
    'base_delay': 60,
    'jitter': False,  # Exact delay
}
# Delay: exactly 60 seconds
```

### When to Use Jitter

**Enable jitter (default) for:**
- High-volume task queues
- Shared external services
- Rate-limited APIs
- Database-heavy operations

**Disable jitter for:**
- Predictable retry intervals required
- Single-task scenarios
- Testing/debugging

## Default Retry Policy

Set default retry behavior for all tasks in cluster configuration.

### Configuration

```python
# settings.py
QRAFT_CLUSTER = {
    "retry_defaults": {
        "max_attempts": 3,
        "delay": 30.0,
        "backoff": "exponential",
        "jitter": True,
        "jitter_max": 0.2,
    },
}
```

### Per-Task Override

Tasks can override defaults:

```python
# Uses defaults (3 attempts, exponential, 30s delay)
task_id = async_task('myapp.tasks.task1')

# Override max_attempts only
task_id = async_task(
    'myapp.tasks.task2',
    qraft_options={'max_attempts': 5}  # Other settings from defaults
)

# Override all retry settings
task_id = async_task(
    'myapp.tasks.task3',
    qraft_options={
        'max_attempts': 10,
        'base_delay': 10,
        'backoff_strategy': 'linear',
    }
)

# No retry policy (task without retry)
task_id = async_task('myapp.tasks.task4')  # No qraft_options
```

### Validation

Retry settings are validated:

```python
# Valid ranges
max_attempts: 1-10
base_delay: ≥ 0
jitter_max: 0.0-1.0
backoff_strategy: 'exponential', 'linear', 'fixed'
```

**Examples:**

```python
# Error: max_attempts out of range
qraft_options={'max_attempts': 20}  # Max is 10

# Error: invalid backoff
qraft_options={'backoff_strategy': 'quadratic'}  # Not supported

# Error: invalid jitter
qraft_options={'jitter_max': 1.5}  # Max is 1.0
```

## Retry Scheduling

Retries are scheduled using Django-Q2's Schedule model.

### How It Works

```
Task fails (Attempt 1)
  │
  ├─► Hook handler checks retry policy
  │     │
  │     └─► should_retry(attempt=1) → True
  │
  ├─► Calculate next delay: 30s (exponential, attempt 1)
  │
  ├─► Create Schedule record:
  │     - name: qraft_retry:{task_id}:2
  │     - func: original task function
  │     - next_run: now + 30s
  │
  └─► Set QraftTask status to PENDING

30 seconds later...
  │
  ├─► Django-Q2 scheduler picks up Schedule
  │
  ├─► Queues task for execution (Attempt 2)
  │
  └─► Worker executes → Success or retry again
```

### Schedule Records

Query scheduled retries:

```python
from django_q.models import Schedule

# Find retries for a specific task
retries = Schedule.objects.filter(
    name__startswith=f'qraft_retry:{task_id}'
)

for retry in retries:
    print(f"Next run: {retry.next_run}")
    print(f"Function: {retry.func}")
    print(f"Attempt: {retry.name.split(':')[-1]}")
```

### Task Naming

Retry tasks use a special naming convention:

```
Initial task: user-provided task_name or auto-generated
Retry task: qraft:{task_id}:{attempt_number}

Examples:
- qraft:123e4567-e89b-12d3-a456-426614174000:2
- qraft:123e4567-e89b-12d3-a456-426614174000:3
```

This allows the hook handler to identify retry tasks and create attempt records.

## Exhaustion Handling

When all retry attempts are consumed, the task reaches EXHAUSTED status.

### Status Lifecycle

```
PENDING → RUNNING → FAILED → PENDING (retry) → RUNNING → FAILED → ... → EXHAUSTED
```

**State transitions:**

1. **Initial attempt fails** → Status: FAILED, retry scheduled
2. **Retry attempts (N-1) fail** → Status: FAILED, retry scheduled
3. **Final attempt fails** → Status: EXHAUSTED, failure hook called

### Checking Exhaustion

```python
from qraft.models import QraftTask, TaskStatus

task = QraftTask.objects.get(id=task_id)

if task.status == TaskStatus.EXHAUSTED:
    print(f"Task exhausted after {task.attempt_count} attempts")

    # Get all attempts
    for attempt in task.attempts.all():
        print(f"Attempt {attempt.attempt_number}: {attempt.exception_class}")
```

### Failure Hook Integration

Failure hooks are called only when exhausted:

```python
def on_failure(task_id, exception_class, **kwargs):
    from qraft.models import QraftTask

    task = QraftTask.objects.get(id=task_id)

    # This is guaranteed to be EXHAUSTED
    assert task.status == TaskStatus.EXHAUSTED

    print(f"Task failed permanently after {task.attempt_count} attempts")
    print(f"Final exception: {exception_class}")

    # Alert, log, cleanup, etc.

task_id = async_task(
    'myapp.tasks.critical_operation',
    data,
    qraft_options={
        'max_attempts': 3,
        'failure_hook': 'myapp.hooks.on_failure',
    }
)
```

**Timeline:**

```
Attempt 1: Fails → No hook (retrying)
Attempt 2: Fails → No hook (retrying)
Attempt 3: Fails → Exhausted → Failure hook called
```

### Manual Retry

You can manually retry an exhausted task:

```python
from qraft.models import QraftTask, TaskStatus

task = QraftTask.objects.get(id=task_id)

if task.status == TaskStatus.EXHAUSTED:
    # Queue new task with same function/args
    new_task_id = async_task(
        task.func,
        *task.task_args,
        **task.task_kwargs,
        qraft_options={
            'max_attempts': 3,  # Fresh retry budget
        }
    )
```

## Monitoring Retries

Track retry behavior using Django ORM.

### Query Retry Status

```python
from qraft.models import QraftTask, QraftTaskAttempt, TaskStatus

# Find tasks currently retrying
retrying = QraftTask.objects.filter(status=TaskStatus.PENDING, attempt_count__gt=1)

for task in retrying:
    print(f"Task {task.id}: {task.attempt_count} attempts")

# Find exhausted tasks
exhausted = QraftTask.objects.filter(status=TaskStatus.EXHAUSTED)

# Find tasks with multiple failures
multi_fail = QraftTask.objects.annotate(
    fail_count=Count('attempts', filter=Q(attempts__success=False))
).filter(fail_count__gte=2)
```

### Analyze Attempt History

```python
task = QraftTask.objects.get(id=task_id)

# Get all attempts
attempts = task.attempts.order_by('attempt_number')

for attempt in attempts:
    print(f"Attempt {attempt.attempt_number}:")
    print(f"  Success: {attempt.success}")
    print(f"  Exception: {attempt.exception_class}")
    print(f"  Completed: {attempt.date_completed}")

# Calculate retry delay (approximate)
if attempts.count() > 1:
    first = attempts[0]
    second = attempts[1]
    delay = (second.created_at - first.date_completed).total_seconds()
    print(f"Retry delay: {delay}s")
```

### Dashboard Metrics

```python
from django.db.models import Count, Q
from qraft.models import QraftTask, TaskStatus

# Retry statistics
stats = {
    'total_tasks': QraftTask.objects.count(),
    'exhausted': QraftTask.objects.filter(status=TaskStatus.EXHAUSTED).count(),
    'succeeded_first_try': QraftTask.objects.filter(
        status=TaskStatus.SUCCEEDED,
        attempt_count=1
    ).count(),
    'succeeded_after_retry': QraftTask.objects.filter(
        status=TaskStatus.SUCCEEDED,
        attempt_count__gt=1
    ).count(),
}

# Average attempts for successful tasks
from django.db.models import Avg
avg_attempts = QraftTask.objects.filter(
    status=TaskStatus.SUCCEEDED
).aggregate(avg=Avg('attempt_count'))

print(f"Average attempts: {avg_attempts['avg']:.2f}")
```

## Use Cases and Patterns

### Pattern 1: API Calls with Rate Limiting

```python
task_id = async_task(
    'myapp.tasks.fetch_external_api',
    url='https://api.example.com/data',
    qraft_options={
        'max_attempts': 5,
        'base_delay': 60,  # 1 minute
        'backoff_strategy': 'exponential',
        'jitter': True,
        'retry_exceptions': ['RateLimitError', 'ConnectionError'],
        'skip_exceptions': ['AuthenticationError', 'NotFoundError'],
    }
)
```

**Timeline:** 1m, 2m, 4m, 8m (with jitter)

### Pattern 2: Database Deadlock Retry

```python
task_id = async_task(
    'myapp.tasks.bulk_update',
    records,
    qraft_options={
        'max_attempts': 3,
        'base_delay': 5,  # Quick retry
        'backoff_strategy': 'linear',
        'jitter': True,  # Spread out concurrent retries
        'retry_exceptions': ['OperationalError'],  # DB deadlock
    }
)
```

**Timeline:** 5s, 10s (with jitter)

### Pattern 3: File Processing with Cleanup

```python
def on_failure(task_id, exception_class, temp_path=None, **kwargs):
    # Cleanup after exhaustion
    if temp_path and os.path.exists(temp_path):
        os.remove(temp_path)

temp_path = '/tmp/upload-123.tmp'

task_id = async_task(
    'myapp.tasks.process_upload',
    temp_path,
    qraft_options={
        'max_attempts': 3,
        'base_delay': 10,
        'backoff_strategy': 'fixed',
        'failure_hook': 'myapp.hooks.on_failure',
        'failure_kwargs': {'temp_path': temp_path},
    }
)
```

### Pattern 4: Status Polling

```python
task_id = async_task(
    'myapp.tasks.check_job_status',
    job_id='job-123',
    qraft_options={
        'max_attempts': 10,  # Poll up to 10 times
        'base_delay': 5,  # Check every 5 seconds
        'backoff_strategy': 'fixed',
        'jitter': False,  # Exact intervals
        'retry_exceptions': ['JobNotReadyError'],
    }
)
```

**Timeline:** Check every 5s for up to 50s

### Pattern 5: Circuit Breaker

```python
from datetime import datetime, timedelta

def should_retry_with_circuit_breaker(exception_class):
    """
    Custom retry logic with circuit breaker pattern.
    """
    from django.core.cache import cache

    # Check if service is in circuit breaker state
    if cache.get('service_down'):
        return False  # Don't retry

    # Track failures
    failures = cache.get('service_failures', 0)
    if failures >= 5:
        # Open circuit for 5 minutes
        cache.set('service_down', True, 300)
        return False

    cache.set('service_failures', failures + 1, 60)
    return True

# In task
def external_service_call(data):
    try:
        result = call_service(data)
        # Success: reset circuit breaker
        cache.delete('service_failures')
        return result
    except ServiceError as e:
        if not should_retry_with_circuit_breaker(e.__class__.__name__):
            raise  # Fail immediately
        raise  # Normal retry

task_id = async_task(
    'myapp.tasks.external_service_call',
    data,
    qraft_options={
        'max_attempts': 3,
        'base_delay': 30,
        'backoff_strategy': 'exponential',
    }
)
```

## Advanced Topics

### Retry Policy Serialization

Retry policies are stored as JSON in the QraftTask model:

```python
task = QraftTask.objects.get(id=task_id)
print(task.retry_policy)
# {
#     'max_attempts': 3,
#     'base_delay': 30.0,
#     'backoff_strategy': 'exponential',
#     'jitter': True,
#     'jitter_max': 0.2,
#     'retry_exceptions': ['ConnectionError'],
#     'skip_exceptions': ['AuthenticationError'],
# }
```

### Programmatic Retry Policy

Create retry policies programmatically:

```python
from qraft.retry import RetryPolicy

# Create policy
policy = RetryPolicy(
    max_attempts=5,
    base_delay=30,
    backoff_strategy='exponential',
    jitter=True,
)

# Check if should retry
should_retry = policy.should_retry(
    current_attempt=2,
    exception='ConnectionError',
)

# Calculate delay
delay = policy.calculate_delay(attempt_number=2)
print(f"Next retry in {delay} seconds")

# Get next ETA
eta = policy.next_eta(attempt_number=2)
print(f"Retry at {eta}")

# Convert to dict for storage
policy_dict = policy.to_dict()
```

### Custom Retry Logic

Implement custom retry logic by extending RetryPolicy:

```python
from qraft.retry import RetryPolicy

class CustomRetryPolicy(RetryPolicy):
    def should_retry(self, current_attempt, exception=None):
        # Custom logic: Don't retry on weekends
        from datetime import datetime
        if datetime.now().weekday() >= 5:  # Saturday or Sunday
            return False

        # Call parent implementation
        return super().should_retry(current_attempt, exception)

# Use custom policy (requires custom task creation)
```

## Troubleshooting

### Retries Not Happening

**Check retry policy:**

```python
task = QraftTask.objects.get(id=task_id)
print(f"Retry policy: {task.retry_policy}")
```

If `None`, no retry policy configured.

**Check attempt count:**

```python
print(f"Attempts: {task.attempt_count}")
print(f"Max attempts: {task.retry_policy['max_attempts']}")
```

If `attempt_count >= max_attempts`, retries exhausted.

**Check exception filtering:**

```python
last_attempt = task.attempts.latest('attempt_number')
print(f"Exception: {last_attempt.exception_class}")
print(f"Skip exceptions: {task.retry_policy['skip_exceptions']}")
```

**Check scheduled retries:**

```python
from django_q.models import Schedule

retries = Schedule.objects.filter(name__startswith=f'qraft_retry:{task_id}')
for retry in retries:
    print(f"Scheduled for {retry.next_run}")
```

### Retries Happening Too Quickly

**Increase base delay:**

```python
qraft_options={
    'base_delay': 60,  # Increase from 30
}
```

**Use exponential backoff:**

```python
qraft_options={
    'backoff_strategy': 'exponential',  # Delays increase faster
}
```

### Retries Taking Too Long

**Decrease base delay:**

```python
qraft_options={
    'base_delay': 10,  # Decrease from 30
}
```

**Use fixed backoff:**

```python
qraft_options={
    'backoff_strategy': 'fixed',  # Constant delay
}
```

### Wrong Exception Being Retried

**Check exception class name:**

```python
# In task
try:
    # ...
except CustomError as e:
    print(f"Exception class: {e.__class__.__name__}")  # Use this name
    raise
```

**Update retry_exceptions:**

```python
qraft_options={
    'retry_exceptions': ['CustomError'],  # Class name, not full path
}
```

### Exhaustion Not Triggering Failure Hook

**Check failure hook configuration:**

```python
task = QraftTask.objects.get(id=task_id)
print(f"Failure hook: {task.failure_hook}")
```

**Check task status:**

```python
print(f"Status: {task.status}")  # Should be EXHAUSTED
```

**Check HookDispatch:**

```python
from qraft.models import HookDispatch

hook = HookDispatch.objects.filter(
    qraft_task=task,
    hook_type='failure',
).first()

if hook:
    print("Failure hook was dispatched")
else:
    print("Failure hook not dispatched")
```

## Related Documentation

- [Getting Started](getting-started.md) - Basic retry usage
- [Configuration](configuration.md) - Retry defaults configuration
- [Hooks Guide](hooks.md) - Retry integration with hooks
- [Architecture](architecture.md) - Retry scheduling internals
