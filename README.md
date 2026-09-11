# django-qraft

Durable background jobs, execution graphs, and AI-workload primitives for Django on PostgreSQL,
with no infrastructure beyond the database.

django-qraft extends [Django-Q2](https://django-q2.readthedocs.io/). Django-Q2 provides the
cluster runtime (pusher, monitor, worker loops). Qraft owns task state, attempt history,
execution leases, orphan reaping, retries, dual-phase hooks, scheduled delays, retention,
and workflow orchestration.

---

## Why Qraft

Standard background task queues treat jobs as transient messages. In production, this causes
recurring failure modes:

| Failure Mode | Standard Queue Behavior | Qraft Mechanism |
|---|---|---|
| **Retried failures** | Overwrites the task row; previous exceptions and attempt counts are lost | Each execution creates a `QraftTaskAttempt` recording timestamp, duration, usage, and exception class |
| **Worker crash / OOM** | Task stays stuck in `RUNNING` indefinitely | Workers maintain an execution lease with heartbeats; the reaper resolves abandoned attempts |
| **Multi-step workflows** | Manually chained across success callbacks; failures leave orphaned state | `QraftChain`, `QraftIter`, `QraftBatch`, and `graphs.Graph` manage topology and quiescent settlement in Postgres |
| **Metered API / LLM limits** | 429 throttles burn retries immediately; unmetered loops exceed budgets | Shared database token buckets (`@throttled`), backoff that reads the provider's retry hint, and atomic request budgets |
| **Delayed execution** | Quantized to scheduler poll intervals (often 30–60s) | Qraft owns scheduling; delayed attempts execute at their exact target timestamp |

---

## Quick Start

### 1. Install

```bash
pip install django-qraft
```

### 2. Configure Django Settings

Add `django_q` and `qraft` to `INSTALLED_APPS`. Point Django-Q2 to the ORM broker and configure Qraft:

```python
# settings.py
INSTALLED_APPS = [
    # ...
    "django_q",
    "qraft",
    "qraft.dashboard",  # Optional: staff-only monitoring UI
]

Q_CLUSTER = {
    "name": "default",
    "workers": 4,
    "timeout": 60,
    "retry": 90,
    "orm": "default",
    "broker_class": "qraft.brokers.QraftOrmBroker",  # Enables priority lanes
}

QRAFT_CLUSTER = {
    "threads": 1,
    "sync_hooks": False,
    "retry_defaults": {
        "max_attempts": 3,
        "delay": 30.0,
        "backoff": "exponential",
        "jitter": True,
    },
}
```

`Q_CLUSTER` keeps configuring Django-Q2: workers, timeout, and the broker. `QRAFT_CLUSTER`
holds only Qraft's own settings. Qraft never reads worker or broker keys from
`QRAFT_CLUSTER`, and silently ignores any it finds there.

### 3. Migrate and Start the Cluster

```bash
python manage.py migrate
python manage.py qraftcluster
```

`qraftcluster` is required. The standard `qcluster` command still runs plain Django-Q2,
without Qraft's dispatcher, reaper, or retention loops.

### 4. Enqueue Tasks

```python
from qraft.tasks import async_task

# Basic enqueue
task_id = async_task("myapp.tasks.send_welcome_email", user_id=42)

# Enqueue with retry policy, dual-phase hooks, and domain subject
task_id = async_task(
    "myapp.tasks.sync_customer_data",
    customer_id,
    qraft_options={
        "max_attempts": 4,
        "base_delay": 10.0,
        "backoff_strategy": "exponential",
        "jitter": True,
        "retry_exceptions": ["ConnectionError", "Timeout"],
        "skip_exceptions": ["InvalidAccountError"],
        "success_hook": "myapp.hooks.on_sync_success",
        "failure_hook": "myapp.hooks.on_sync_failure",
        "hook_context": True,
        "subject": ("customer", customer_id),
    },
)
```

---

## Core Capabilities

### Retries and Dual-Phase Hooks

Qraft separates success and failure handling. Hooks run asynchronously by default across worker
processes to avoid blocking the monitor loop.

- **Retry policies**: Support `exponential`, `linear`, or `fixed` backoff with randomized jitter.
- **Exception filters**: Allow specific exception classes with `retry_exceptions` or fail fast with `skip_exceptions`.
- **Rate-limit awareness**: Throttling exceptions matched by class name (`RateLimitError`, `TooManyRequests`, `ThrottlingException`, `OverloadedError`, and others) enforce exponential backoff, read a retry-after hint from the error text when one is present, and bypass restrictive `retry_exceptions` filters. The delay is capped by `rate_limit_max_delay`.
- **Dual-phase hooks**: `success_hook` executes on task completion; `failure_hook` executes only when all retries are exhausted.
- **Hook context**: With `hook_context=True`, hooks receive execution metadata (`task_id`, `attempt_number`, `outcome`, `exception_class`, `subject_type`, `result_ref`).

```python
# myapp/hooks.py
def on_sync_failure(alert_channel="ops", context=None, **kwargs):
    if context:
        logger.error(
            "Task %s failed on attempt %d: %s",
            context["task_id"],
            context["attempt_number"],
            context["exception_class"],
        )
```

```python
# Enqueue task with hook arguments
async_task(
    "myapp.tasks.process_payment",
    order_id,
    qraft_options={
        "failure_hook": "myapp.hooks.on_sync_failure",
        "failure_kwargs": {"alert_channel": "billing"},
        "hook_context": True,
    },
)
```

---

### Workflows and Execution Graphs

Qraft provides four orchestration primitives. All primitives record member state in the database,
support cancellation, accept domain subjects, and emit settlement signals.

#### 1. Sequential Pipelines (`QraftChain`)

Executes steps sequentially. A failed step stops the chain. Chains can be resumed from the failed
step or gated for human approval at zero compute cost:

```python
from qraft.chain import QraftChain

chain = QraftChain(
    subject=("order", order_id),
    on_success="myapp.hooks.order_completed",
    on_failure="myapp.hooks.order_failed",
)
chain.append("myapp.tasks.validate_order", order_id)
chain.append("myapp.tasks.capture_funds", order_id, requires_approval=True)
chain.append("myapp.tasks.dispatch_shipment", order_id)
chain_id = chain.run()

# Later, resume from an approval gate or view:
chain = QraftChain(chain_id=chain_id)
if chain.status == "waiting_approval":
    chain.approve()  # Dispatches next step; or chain.reject("fraud_check_failed")
```

#### 2. Parallel Homogeneous Map (`QraftIter`)

Applies a single function across multiple inputs in parallel with progress tracking:

```python
from qraft.iter import QraftIter

iter_job = QraftIter(
    "myapp.tasks.render_pdf_page",
    qraft_options={"max_attempts": 3},
    on_success="myapp.hooks.all_pages_rendered",
    progress_hook="myapp.hooks.on_render_progress",
)
for page_num in range(1, total_pages + 1):
    iter_job.append(doc_id, page_num)
iter_id = iter_job.run()
```

#### 3. Parallel Heterogeneous Fork-Join (`QraftBatch`)

Runs distinct functions in parallel and triggers completion hooks when all members finish:

```python
from qraft.batch import QraftBatch

batch = QraftBatch(on_success="myapp.hooks.metrics_aggregated")
batch.append("myapp.tasks.fetch_stripe_payouts", date=target_date)
batch.append("myapp.tasks.fetch_shopify_orders", date=target_date)
batch.append("myapp.tasks.fetch_warehouse_stock", warehouse_id="wh-1")
batch_id = batch.run()
```

#### 4. Execution Graphs (`graphs.Graph`)

Declares an explicit directed acyclic graph (DAG) of task nodes with named dependencies. Qraft
dispatches frontier nodes as prerequisites succeed and settles the graph on quiescence:

```python
from qraft import graphs

graph = graphs.Graph(
    subject=("worksheet", worksheet_id),
    kind="underwriting",
    budgets={"openai_requests": 25},
    on_settled="myapp.hooks.worksheet_settled",
)
graph.node("extract", "myapp.tasks.extract_tables", worksheet_id, recovery="transactional")
graph.node("rules", "myapp.tasks.run_rules", worksheet_id, after=("extract",), recovery="transactional")
graph.node("ai_summary", "myapp.tasks.generate_summary", worksheet_id, after=("extract",), recovery="idempotent")
graph.node("publish", "myapp.tasks.publish_docket", worksheet_id, after=("rules", "ai_summary"), recovery="manual")
graph_id = graph.start()
```

- **Selective Resume**: If a node fails, `graphs.preview_resume(graph_id)` inspects what will run, and `graphs.resume(graph_id)` re-executes only the failed node and its downstream closure.
- **Completion Receipts**: Nodes running under `with graphs.publish() as completion:` commit their domain writes and task receipts in a single transaction.

---

### Metered AI and API Primitives

Qraft includes primitives designed for tasks that call rate-limited, metered external APIs:

```python
from qraft.throttle import throttled
from qraft.context import consume_budget, record_usage, report_progress

# 1. Database-backed token bucket shared across all workers and processes
@throttled(key="openai:enterprise-tier", rate=10.0, capacity=20.0, cost=1.0)
def analyze_contract(contract_id):
    # 2. Atomic graph request budget decrement (prevents runaway spend across retries)
    consume_budget("openai_requests")

    # 3. Step-level progress reporting with stall detection
    report_progress(current=1, total=3, message="requesting model completion")

    response = client.chat.completions.create(...)

    # 4. Attempt-level token and cost accounting
    record_usage(
        model="gpt-4o",
        input_tokens=response.usage.prompt_tokens,
        output_tokens=response.usage.completion_tokens,
    )
```

- **Idempotency Keys**: Set `qraft_options={"idempotency_key": "contract:1042"}` to prevent duplicate task creation on network retries.
- **Usage Aggregation**: Roll up tokens and costs via `aggregate_usage(task)`, `aggregate_workflow_usage(workflow)`, or `aggregate_subject_usage("contract", 1042)`.

---

### Leases, Reaper, and Crash Recovery

Workers acquire an execution lease with periodic heartbeats when executing an attempt. If a worker
dies mid-task (OOM, power failure, SIGKILL):

1. **Heartbeat expiration**: The execution lease becomes stale after `max(3 * heartbeat_interval, min_heartbeat_grace)`.
2. **Orphan reaper**: The sentinel daemon identifies abandoned attempts and resolves them through the normal retry path (`OrphanedTask`).
3. **Dead Letter Queue**: Permanently failed or exhausted tasks are queried with `dead_letters()` and requeued via `requeue(task)` as the next attempt on the original task record.

```python
from qraft.dlq import dead_letters, requeue

# Inspect and requeue dead letters without creating duplicate task rows
for task in dead_letters():
    if task.latest_attempt.exception_class == "TemporaryProviderOutage":
        requeue(task)
```

---

### Priority Lanes and Multithreading

- **Priority Lanes**: When using `qraft.brokers.QraftOrmBroker`, tasks enqueued with `priority="high"`, `"default"`, or `"low"` land in dedicated lanes on the same database table and are drained in priority order without separate worker deployments.
- **Multithreaded Workers**: For I/O-bound workloads (HTTP requests, database exports), set `threads > 1` in `QRAFT_CLUSTER` to run a thread pool inside each worker process:

```python
Q_CLUSTER = {"workers": 2, ...}   # process count stays here

QRAFT_CLUSTER = {
    "threads": 8,        # 2 worker processes * 8 threads = 16 concurrent tasks
    "max_inflight": 16,  # Backpressure ceiling per process
}
```

---

### Staff Dashboard

Qraft includes a self-contained monitoring dashboard mounted as a Django app. It queries Qraft's
database tables directly with zero external storage and no frontend build step:

```python
# urls.py
from django.urls import include, path

urlpatterns = [
    # ...
    path("qraft/", include("qraft.dashboard.urls")),
]
```

- **Live views**: Task queues, execution graphs, workflow trees, rate buckets, usage summaries, and dead letters.
- **Interactive actions**: Staff users (`is_staff=True`) can approve/reject workflow gates, cancel workflows, resume failed graphs, and requeue dead letters.
- **Subject search**: Filter tasks, graphs, and usage by domain subject (`type:id`).

---

### `django.tasks` Backend (Django 6.0 / DEP 14)

Qraft implements the standard Django 6.0 Tasks API backend:

```python
# settings.py
TASKS = {
    "default": {"BACKEND": "qraft.backend.QraftTaskBackend"},
}
```

```python
from django.tasks import task

@task(priority=10)
def process_upload(upload_id):
    ...

result = process_upload.enqueue(upload_id)
```

---

## Compatibility and Broker Guarantees

### Invariant Restrictions

Qraft tasks reject the following legacy Django-Q2 options:
- `sync=True`
- `save=False`
- `cached=True`
- `ack_failure=False`

*Reason*: Durable attempt logging, dual-phase hooks, leases, and retry tracking require database
persistence and asynchronous worker execution.

### Broker Matrix

| Feature | PostgreSQL + ORM Broker (`QraftOrmBroker`) | Non-ORM Brokers (Redis, SQS) |
|---|---|---|
| **Transaction safety** | Enqueue commits atomically with business data | Enqueue occurs before transaction commit |
| **Delivery receipts** | Supported via database row locks | Unavailable |
| **Priority lanes** | Supported (`high`, `default`, `low`) | Single queue polling |
| **Orphan reaper** | Detects both unstarted and running abandoned attempts | Reaps running attempts only after lease timeout |
| **Token buckets & budgets** | Serialized by PostgreSQL row locks | Unaffected; they read the database, not the queue |

---

## Configuration Reference

Key settings in `QRAFT_CLUSTER`:

Worker count, task timeout, and broker selection are Django-Q2 settings and belong in
`Q_CLUSTER`. Qraft's own settings:

| Setting | Type | Default | Description |
|---|---|---|---|
| `threads` | `int` | `1` | Threads per worker process (`>1` enables thread pool) |
| `max_inflight` | `int` | `threads * 2` | Maximum concurrent tasks per worker process |
| `sync_hooks` | `bool` | `False` | Run hooks synchronously in monitor (`True`) or over workers (`False`) |
| `reap_interval` | `float` | `60.0` | Seconds between orphan reaper sweeps |
| `heartbeat_interval` | `float` | `30.0` | Seconds between worker lease heartbeats |
| `min_heartbeat_grace` | `float` | `90.0` | Floor grace period before reaping an expired lease |
| `retention_days` | `float` | `None` | Prune settled rows older than N days (`None` keeps all) |
| `retention_max_tasks` | `int` | `None` | Keep newest N settled tasks |
| `retry_defaults` | `dict` | *See below* | Global retry configuration dict |

`retry_defaults` structure:
- `max_attempts` (`int`, default: `3`): Total executions including initial run.
- `delay` (`float`, default: `30.0`): Base delay in seconds.
- `backoff` (`str`, default: `"exponential"`): `"exponential"`, `"linear"`, or `"fixed"`.
- `jitter` (`bool`, default: `True`): Add random jitter (±20% by default).

---

## Documentation

- [Getting Started](https://github.com/ankitksr/django-qraft/blob/main/docs/getting-started.md)
- [Configuration Reference](https://github.com/ankitksr/django-qraft/blob/main/docs/configuration.md)
- [Retry Policies](https://github.com/ankitksr/django-qraft/blob/main/docs/retry.md)
- [Dual-Phase Hooks](https://github.com/ankitksr/django-qraft/blob/main/docs/hooks.md)
- [Workflows and Graphs](https://github.com/ankitksr/django-qraft/blob/main/docs/workflows.md)
- [AI Workloads & Metering](https://github.com/ankitksr/django-qraft/blob/main/docs/ai-workloads.md)
- [Multithreading Guide](https://github.com/ankitksr/django-qraft/blob/main/docs/threading.md)
- [Dead Letter Queue](https://github.com/ankitksr/django-qraft/blob/main/docs/dlq.md)
- [Monitoring Dashboard](https://github.com/ankitksr/django-qraft/blob/main/docs/dashboard.md)
- [django.tasks Backend](https://github.com/ankitksr/django-qraft/blob/main/docs/django-tasks-backend.md)
- [Architecture & Internal Design](https://github.com/ankitksr/django-qraft/blob/main/docs/architecture.md)

---

## Project Status

Version 1.4.0, first public release. MIT licensed, one maintainer. 715 tests, including an
end-to-end suite that runs a real broker, a real worker, and the real hook handler with
nothing stubbed; CI gates line coverage at 72%. Nested workflows are not implemented.

## License

MIT License. See [LICENSE](https://github.com/ankitksr/django-qraft/blob/main/LICENSE) for details.
