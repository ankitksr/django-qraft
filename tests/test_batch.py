"""Tests for QraftBatch parallel workflow primitive."""

from unittest.mock import Mock, patch

import pytest

# Disable hook path validation for tests with fake module paths
pytestmark = pytest.mark.usefixtures("_disable_hook_validation")

from qraft.batch import QraftBatch
from qraft.dispatchers import ParallelDispatcher
from qraft.hooks import qraft_hook_handler
from qraft.models import (
    QraftBatchModel,
    QraftTask,
    QraftTaskAttempt,
    TaskStatus,
    WorkflowHookDispatch,
    WorkflowStatus,
)


@pytest.fixture
def simple_batch():
    """Create a simple batch with 3 different tasks."""
    batch = QraftBatch()
    batch.append("demo.showcase.tasks.noop_task", 1)
    batch.append("demo.showcase.tasks.noop_task", 2)
    batch.append("demo.showcase.tasks.noop_task", 3)
    return batch


class TestBatchCreation:
    """Test batch creation and configuration."""

    def test_create_batch(self, db):
        """Test creating a batch."""
        batch = QraftBatch()

        assert batch.id is not None
        assert batch.status == WorkflowStatus.PENDING
        assert isinstance(batch._model, QraftBatchModel)

    def test_create_batch_with_hooks(self, db):
        """Test creating a batch with success/failure hooks."""
        batch = QraftBatch(
            on_success="myapp.hooks.all_complete",
            on_failure="myapp.hooks.partial_failure",
            success_args=(1, 2),
            success_kwargs={"result": "ok"},
            failure_args=(3,),
            failure_kwargs={"result": "error"},
        )

        model = batch._model
        assert model.success_hook == "myapp.hooks.all_complete"
        assert model.failure_hook == "myapp.hooks.partial_failure"
        assert model.success_args == [1, 2]
        assert model.success_kwargs == {"result": "ok"}
        assert model.failure_args == [3]
        assert model.failure_kwargs == {"result": "error"}

    def test_load_existing_batch(self, db):
        """Test loading an existing batch by ID."""
        batch1 = QraftBatch()
        batch_id = batch1.id

        batch2 = QraftBatch(batch_id=batch_id)
        assert batch2.id == batch_id
        assert batch2._model.id == batch1._model.id


class TestBatchTaskManagement:
    """Test adding tasks to batch."""

    def test_add_task(self, db):
        """Test adding a task."""
        batch = QraftBatch()
        batch.append("myapp.tasks.task1", 1, 2, key="value")

        assert len(batch._tasks) == 1
        task = batch._tasks[0]
        assert task["func"] == "myapp.tasks.task1"
        assert task["args"] == (1, 2)
        assert task["kwargs"] == {"key": "value"}
        assert task["qraft_options"] == {}

    def test_add_task_with_qraft_options(self, db):
        """Test adding a task with Qraft options."""
        batch = QraftBatch()
        batch.append(
            "myapp.tasks.task1",
            qraft_options={"max_attempts": 5, "backoff_strategy": "linear"},
        )

        task = batch._tasks[0]
        assert task["qraft_options"] == {
            "max_attempts": 5,
            "backoff_strategy": "linear",
        }

    def test_add_multiple_tasks(self, db, simple_batch):
        """Test adding multiple tasks."""
        assert len(simple_batch._tasks) == 3

    def test_add_different_functions(self, db):
        """Test adding tasks with different functions."""
        batch = QraftBatch()
        batch.append("myapp.tasks.fetch_sales")
        batch.append("myapp.tasks.fetch_inventory")
        batch.append("myapp.tasks.fetch_shipping")

        assert batch._tasks[0]["func"] == "myapp.tasks.fetch_sales"
        assert batch._tasks[1]["func"] == "myapp.tasks.fetch_inventory"
        assert batch._tasks[2]["func"] == "myapp.tasks.fetch_shipping"

    def test_cannot_add_after_run(self, db, simple_batch):
        """Test that adding after run raises error."""
        simple_batch.run()

        with pytest.raises(ValueError, match="already been run"):
            simple_batch.append("myapp.tasks.task4")


class TestBatchExecution:
    """Test batch execution and lifecycle."""

    def test_run_empty_batch_fails(self, db):
        """Test that running an empty batch raises error."""
        batch = QraftBatch()

        with pytest.raises(ValueError, match="Cannot run empty batch"):
            batch.run()

    def test_run_updates_status_and_count(self, db, simple_batch):
        """Test that running updates status and total_count."""
        batch_id = simple_batch.run()

        assert batch_id == simple_batch.id
        assert simple_batch.status == WorkflowStatus.RUNNING
        assert simple_batch.total_count == 3

    def test_run_creates_tasks(self, db, simple_batch):
        """Test that running creates QraftTask instances."""
        simple_batch.run()

        # Check that tasks were created with qraft_batch FK
        tasks = QraftTask.objects.filter(qraft_batch=simple_batch._model)
        assert tasks.count() == 3

        # Verify tasks are linked to batch
        for task in tasks:
            assert task.qraft_batch_id == simple_batch.id
            assert task.status == TaskStatus.RUNNING

    def test_cannot_run_twice(self, db, simple_batch):
        """Test that running a batch twice raises error."""
        simple_batch.run()

        with pytest.raises(ValueError, match="already run"):
            simple_batch.run()

    def test_tasks_have_no_task_level_hooks(self, db, simple_batch):
        """Test that workflow tasks don't have task-level hooks."""
        simple_batch.run()

        for task in QraftTask.objects.filter(qraft_batch=simple_batch._model):
            assert task.success_hook is None
            assert task.failure_hook is None


class TestBatchCounters:
    """Test batch counter properties."""

    def test_initial_counters(self, db, simple_batch):
        """Test initial counter values."""
        assert simple_batch.total_count == 0
        assert simple_batch.completed_count == 0
        assert simple_batch.success_count == 0
        assert simple_batch.failure_count == 0

    def test_counters_after_run(self, db, simple_batch):
        """Test counter values after run."""
        simple_batch.run()

        assert simple_batch.total_count == 3
        assert simple_batch.completed_count == 0  # No tasks completed yet
        assert simple_batch.success_count == 0
        assert simple_batch.failure_count == 0

    def test_counters_update_on_completion(self, db, simple_batch):
        """Test that counters update when tasks complete."""
        simple_batch.run()

        # Manually increment counters (normally done by ParallelDispatcher)
        model = simple_batch._model
        model.completed_count = 2
        model.success_count = 1
        model.failure_count = 1
        model.save()

        simple_batch._model.refresh_from_db()
        assert simple_batch.completed_count == 2
        assert simple_batch.success_count == 1
        assert simple_batch.failure_count == 1


class TestBatchQueries:
    """Test batch status queries and result retrieval."""

    def test_result_empty_batch(self, db, simple_batch):
        """Test getting results from unrun batch."""
        results = simple_batch.result()
        assert len(results) == 0

    def test_result_timeout(self, db, simple_batch):
        """Test result with timeout on incomplete batch."""
        simple_batch.run()

        with pytest.raises(TimeoutError, match="did not complete within"):
            simple_batch.result(wait=100)  # 100ms timeout


class TestBatchRepr:
    """Test batch string representation."""

    def test_batch_repr(self, db, simple_batch):
        """Test batch repr."""
        simple_batch.run()
        repr_str = repr(simple_batch)

        assert "QraftBatch" in repr_str
        assert str(simple_batch.id) in repr_str
        assert "running" in repr_str
        assert "0/3" in repr_str  # completed/total


class TestBatchIntegration:
    """Integration tests with task execution."""

    @pytest.mark.django_db(transaction=True)
    def test_batch_with_different_retry_policies(self, db):
        """Test that each task can have different retry policies."""
        batch = QraftBatch()
        batch.append("task1", qraft_options={"max_attempts": 1})
        batch.append("task2", qraft_options={"max_attempts": 5})
        batch.append("task3", qraft_options={"max_attempts": 3})

        batch.run()

        tasks = list(QraftTask.objects.filter(qraft_batch=batch._model).order_by("date_created"))
        assert tasks[0].retry_policy["max_attempts"] == 1
        assert tasks[1].retry_policy["max_attempts"] == 5
        assert tasks[2].retry_policy["max_attempts"] == 3

    @pytest.mark.django_db(transaction=True)
    def test_batch_with_heterogeneous_tasks(self, db):
        """Test batch with different functions and argument patterns."""
        batch = QraftBatch()

        # Different functions with different args
        batch.append("myapp.tasks.fetch_sales", region="NA")
        batch.append("myapp.tasks.fetch_inventory", warehouse="main", count=100)
        batch.append("myapp.tasks.fetch_shipping", carrier="fedex")

        batch.run()

        tasks = list(QraftTask.objects.filter(qraft_batch=batch._model).order_by("date_created"))

        # Verify heterogeneous nature
        assert tasks[0].func == "myapp.tasks.fetch_sales"
        assert tasks[0].task_kwargs == {"region": "NA"}

        assert tasks[1].func == "myapp.tasks.fetch_inventory"
        assert tasks[1].task_kwargs == {"warehouse": "main", "count": 100}

        assert tasks[2].func == "myapp.tasks.fetch_shipping"
        assert tasks[2].task_kwargs == {"carrier": "fedex"}

    @pytest.mark.django_db(transaction=True)
    def test_batch_fork_join_pattern(self, db):
        """Test that batch implements fork-join pattern."""
        batch = QraftBatch(on_success="myapp.hooks.aggregate_results")

        # Fork: Launch multiple different tasks
        batch.append("myapp.tasks.task_a", 1)
        batch.append("myapp.tasks.task_b", 2)
        batch.append("myapp.tasks.task_c", 3)

        batch.run()

        # All tasks should be queued in parallel
        tasks = QraftTask.objects.filter(qraft_batch=batch._model)
        assert tasks.count() == 3

        # All should be RUNNING (not waiting for each other)
        for task in tasks:
            assert task.status == TaskStatus.RUNNING

    @patch("qraft.tasks.q2_async_task")
    def test_batch_with_cluster_routing(self, mock_q2_async, db):
        """Test that each task can route to different clusters."""
        # Return different IDs for each call to avoid unique constraint violation
        mock_q2_async.side_effect = ["q2-task-1", "q2-task-2", "q2-task-3"]

        batch = QraftBatch()
        batch.append("myapp.tasks.fetch_sales", qraft_options={"cluster": "io-workers"})
        batch.append("myapp.tasks.compute_stats", qraft_options={"cluster": "cpu-workers"})
        batch.append("myapp.tasks.send_email", qraft_options={"cluster": "default"})

        batch.run()

        # Verify each task was queued with correct cluster parameter
        assert mock_q2_async.call_count == 3

        # Extract cluster from each call
        clusters = [call_obj[1]["cluster"] for call_obj in mock_q2_async.call_args_list]
        assert "io-workers" in clusters
        assert "cpu-workers" in clusters
        assert "default" in clusters


@pytest.mark.django_db(transaction=True)
class TestBatchHookDispatch:
    """Test batch hook dispatching via workflow completion."""

    def test_failure_hook_fires_when_task_exhausted(self):
        """Test that batch failure hook fires when a task exhausts retries."""
        batch = QraftBatch(
            on_failure="showcase.tasks.on_failure",
            failure_args=["batch-failed"],
        )
        batch.append(
            "showcase.tasks.noop_task",
            qraft_options={"max_attempts": 1},
        )
        batch_id = batch.run()

        qraft_task = QraftTask.objects.get(qraft_batch=batch._model)
        attempt = QraftTaskAttempt.objects.get(
            qraft_task=qraft_task, attempt_number=1,
        )

        # Mark task as EXHAUSTED (all retries done)
        qraft_task.status = TaskStatus.EXHAUSTED
        qraft_task.save()

        mock_q2_task = Mock()
        mock_q2_task.id = attempt.q2_task_id
        mock_q2_task.success = False
        mock_q2_task.stopped = "2024-01-24T00:00:00Z"
        mock_q2_task.result = "ValueError: test error"

        qraft_hook_handler(mock_q2_task)

        batch._model.refresh_from_db()
        assert batch._model.completed_count == 1
        assert batch._model.failure_count == 1
        assert batch._model.status == WorkflowStatus.FAILED

        hook_dispatch = WorkflowHookDispatch.objects.filter(
            workflow_type="batch",
            workflow_id=batch_id,
            hook_type="failure",
        ).first()
        assert hook_dispatch is not None
        assert hook_dispatch.hook_path == "showcase.tasks.on_failure"

    def test_success_hook_fires_when_all_tasks_succeed(self):
        """Test that batch success hook fires when all tasks succeed."""
        batch = QraftBatch(
            on_success="showcase.tasks.on_success",
            success_args=["batch-success"],
        )
        batch.append("showcase.tasks.noop_task", 1)
        batch.append("showcase.tasks.noop_task", 2)
        batch_id = batch.run()

        tasks = list(
            QraftTask.objects.filter(qraft_batch=batch._model),
        )
        assert len(tasks) == 2

        for qraft_task in tasks:
            attempt = QraftTaskAttempt.objects.get(
                qraft_task=qraft_task, attempt_number=1,
            )
            mock_q2_task = Mock()
            mock_q2_task.id = attempt.q2_task_id
            mock_q2_task.success = True
            mock_q2_task.stopped = "2024-01-24T00:00:00Z"
            mock_q2_task.result = {"value": 1}
            qraft_hook_handler(mock_q2_task)

        batch._model.refresh_from_db()
        assert batch._model.completed_count == 2
        assert batch._model.success_count == 2
        assert batch._model.failure_count == 0
        assert batch._model.status == WorkflowStatus.SUCCEEDED

        hook_dispatch = WorkflowHookDispatch.objects.filter(
            workflow_type="batch",
            workflow_id=batch_id,
            hook_type="success",
        ).first()
        assert hook_dispatch is not None
        assert hook_dispatch.hook_path == "showcase.tasks.on_success"

    def test_parallel_dispatcher_with_exhausted_task(self):
        """Test ParallelDispatcher directly with exhausted task."""
        batch_model = QraftBatchModel.objects.create(
            total_count=1,
            status=WorkflowStatus.RUNNING,
            failure_hook="showcase.tasks.on_failure",
            failure_args=["test"],
            failure_kwargs={},
        )

        qraft_task = QraftTask.objects.create(
            func="showcase.tasks.noop_task",
            task_args=[],
            task_kwargs={},
            status=TaskStatus.EXHAUSTED,
            qraft_batch=batch_model,
        )

        attempt = QraftTaskAttempt.objects.create(
            qraft_task=qraft_task,
            attempt_number=1,
            q2_task_id="test-123",
            success=False,
            exception_class="ValueError",
        )

        dispatcher = ParallelDispatcher(batch_model, attempt)
        dispatcher.handle()

        batch_model.refresh_from_db()
        assert batch_model.status == WorkflowStatus.FAILED
        assert batch_model.completed_count == 1
        assert batch_model.failure_count == 1

        hook_dispatch = WorkflowHookDispatch.objects.filter(
            workflow_type="batch",
            workflow_id=batch_model.id,
            hook_type="failure",
        ).first()
        assert hook_dispatch is not None
