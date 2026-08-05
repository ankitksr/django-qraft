# Workflow Primitives

Django-Qraft provides three workflow primitives for orchestrating complex task patterns:

- **QraftChain** — Sequential execution (pipeline)
- **QraftIter** — Parallel same-function execution (map)
- **QraftBatch** — Parallel different-function execution (fork-join)

All workflows support:
- Workflow-level success/failure hooks
- Cancellation
- Progress tracking (parallel workflows)
- Rich result objects

## QraftChain

Executes tasks one after another. Each step can have its own retry policy. If any step fails and exhausts retries, the chain fails. Failed chains can be resumed from the failed step.

```python
from qraft.chain import QraftChain

chain = QraftChain(
    on_success='myapp.hooks.pipeline_complete',
    on_failure='myapp.hooks.pipeline_failed',
)
chain.append('myapp.tasks.extract', source_id,
             qraft_options={'max_attempts': 3, 'cluster': 'io-workers'})
chain.append('myapp.tasks.transform', format='json')
chain.append('myapp.tasks.load', dest_id)
chain_id = chain.run()
```

### Resume from failure

```python
# Load existing chain and resume from failed step
chain = QraftChain(chain_id=chain_id)
if chain.status == 'failed':
    chain.resume()
```

### Approval steps

Mark a step `requires_approval=True` and the chain parks in `WAITING_APPROVAL` immediately before that step runs. Nothing is queued and no worker is held while it waits.

```python
chain = QraftChain(on_cancelled='myapp.hooks.publish_rejected')
chain.append('myapp.tasks.draft_release_notes', release_id)
chain.append('myapp.tasks.publish', release_id, requires_approval=True)
chain.run()
```

Resume or abandon it from anywhere — a view, the admin, a shell — by loading the chain by id:

```python
chain = QraftChain(chain_id=chain_id)
if chain.status == 'waiting_approval':
    chain.approve()              # queues the parked step, status → running
    # or
    chain.reject('not ready')    # status → cancelled, fires on_cancelled
```

`approve()` and `reject()` take a row lock and validate the transition, so two reviewers racing produce one decision and an `InvalidStatusTransition` for the loser.

`result(wait=...)` returns as soon as the chain parks instead of blocking to the timeout: `WAITING_APPROVAL` cannot clear without an external decision. The returned `WorkflowResult` holds the steps completed so far.

```python
partial = chain.result(wait=30000)   # returns at once if parked
print(len(partial))                  # steps finished before the gate
```

### Getting results

```python
result = chain.result(wait=30000)  # wait up to 30 seconds
for value in result:
    print(value)

# Or inspect detailed results
print(result.succeeded)
print(result.failed_results)
print(result.errors())
```

## QraftIter

Applies the same function to different inputs in parallel. All tasks share the same retry policy.

```python
from qraft.iter import QraftIter

iter_task = QraftIter(
    'myapp.tasks.process_report',
    qraft_options={'max_attempts': 3},
    cluster='io-workers',
    on_success='myapp.hooks.all_reports_ready',
    progress_hook='myapp.hooks.on_progress',
)
for report_id in report_ids:
    iter_task.append(report_id)
iter_id = iter_task.run()
```

### Progress tracking

The `progress_hook` is called after each task completes (before the final hook). It receives:

```python
def on_progress(workflow_id, workflow_type, completed_count,
                total_count, success_count, failure_count):
    pct = (completed_count / total_count) * 100
    print(f"Progress: {pct:.0f}%")
```

## QraftBatch

Executes different functions in parallel (fork-join pattern). Each task can have its own retry policy and cluster routing.

```python
from qraft.batch import QraftBatch

batch = QraftBatch(
    on_success='myapp.hooks.generate_report',
    on_failure='myapp.hooks.partial_failure',
)
batch.append('myapp.tasks.fetch_sales', region='NA',
          qraft_options={'max_attempts': 3, 'cluster': 'io-workers'})
batch.append('myapp.tasks.fetch_inventory', warehouse='main')
batch.append('myapp.tasks.fetch_shipping', carrier='fedex')
batch_id = batch.run()
```

> **Note**: `QraftBatch.add()` is deprecated in favor of `append()` for API consistency.

## Cancellation

All workflow types support cancellation:

```python
chain.cancel()  # or iter_task.cancel() or batch.cancel()
```

Cancellation is cooperative: in-flight tasks will complete, but no further steps are queued and no completion hooks fire. The `on_cancelled` hook (if configured) is called instead.

```python
chain = QraftChain(
    on_cancelled='myapp.hooks.pipeline_cancelled',
)
```

## Result Objects

All `result()` methods return a `WorkflowResult` object:

```python
result = batch.result(wait=60000)

# Iterate over values (backward compatible with list)
values = list(result)

# Inspect details
result.succeeded       # list[TaskResult] — successful tasks
result.failed_results  # list[TaskResult] — failed tasks
result.values          # list[Any] — raw values
result.errors()        # list[dict] — structured error info

# Each TaskResult has:
for tr in result.task_results:
    print(tr.task_id, tr.func, tr.success, tr.result, tr.error, tr.attempt_count)
```

## Models

Workflow state is stored in the database:

| Model | Description |
|-------|-------------|
| `QraftChainModel` | Chain metadata, status, current step |
| `QraftChainStep` | Individual step definition + link to QraftTask |
| `QraftIterModel` | Iter metadata, atomic counters |
| `QraftBatchModel` | Batch metadata, atomic counters |
| `WorkflowHookDispatch` | Idempotent workflow hook tracking |

## Status Lifecycle

```
PENDING → RUNNING → SUCCEEDED
                  → FAILED → RUNNING (resume, chain only)
                  → WAITING_APPROVAL → RUNNING   (approve, chain only)
                                     → CANCELLED (reject)
         → CANCELLED (from PENDING or RUNNING)
```
