"""Tests for QraftIter parallel workflow primitive."""

from unittest.mock import call, patch

import pytest

# Disable hook path validation for tests with fake module paths
pytestmark = pytest.mark.usefixtures("_disable_hook_validation")

from qraft.iter import QraftIter  # noqa: E402
from qraft.models import (  # noqa: E402
    QraftIterModel,
    QraftTask,
    TaskStatus,
    WorkflowStatus,
)


@pytest.fixture
def simple_iter():
    """Create a simple iter with 5 items."""
    iter_task = QraftIter("demo.showcase.tasks.noop_task")
    for i in range(1, 6):
        iter_task.append(i)
    return iter_task


class TestIterCreation:
    """Test iter creation and configuration."""

    def test_create_iter(self, db):
        """Test creating an iter."""
        iter_task = QraftIter("myapp.tasks.process_item")

        assert iter_task.id is not None
        assert iter_task.status == WorkflowStatus.PENDING
        assert isinstance(iter_task._model, QraftIterModel)
        assert iter_task._model.func == "myapp.tasks.process_item"

    def test_create_iter_with_options(self, db):
        """Test creating an iter with qraft_options."""
        iter_task = QraftIter(
            "myapp.tasks.process_item",
            qraft_options={"max_attempts": 3, "backoff_strategy": "exponential"},
        )

        assert iter_task._model.default_qraft_options == {
            "max_attempts": 3,
            "backoff_strategy": "exponential",
        }

    def test_create_iter_with_hooks(self, db):
        """Test creating an iter with success/failure hooks."""
        iter_task = QraftIter(
            "myapp.tasks.process_item",
            on_success="myapp.hooks.all_done",
            on_failure="myapp.hooks.some_failed",
            success_args=(1,),
            success_kwargs={"status": "complete"},
            failure_args=(2,),
            failure_kwargs={"status": "partial"},
        )

        model = iter_task._model
        assert model.success_hook == "myapp.hooks.all_done"
        assert model.failure_hook == "myapp.hooks.some_failed"
        assert model.success_args == [1]
        assert model.success_kwargs == {"status": "complete"}
        assert model.failure_args == [2]
        assert model.failure_kwargs == {"status": "partial"}

    def test_load_existing_iter(self, db):
        """Test loading an existing iter by ID."""
        iter1 = QraftIter("myapp.tasks.process_item")
        iter_id = iter1.id

        iter2 = QraftIter("myapp.tasks.process_item", iter_id=iter_id)
        assert iter2.id == iter_id
        assert iter2._model.id == iter1._model.id


class TestIterItemManagement:
    """Test appending items to iter."""

    def test_append_item(self, db):
        """Test appending an item."""
        iter_task = QraftIter("myapp.tasks.process_item")
        iter_task.append(1, 2, 3, key="value")

        assert len(iter_task._items) == 1
        item = iter_task._items[0]
        assert item["args"] == (1, 2, 3)
        assert item["kwargs"] == {"key": "value"}

    def test_append_multiple_items(self, db, simple_iter):
        """Test appending multiple items."""
        assert len(simple_iter._items) == 5
        assert simple_iter._items[0]["args"] == (1,)
        assert simple_iter._items[4]["args"] == (5,)

    def test_cannot_append_after_run(self, db, simple_iter):
        """Test that appending after run raises error."""
        simple_iter.run()

        with pytest.raises(ValueError, match="already been run"):
            simple_iter.append(6)

    def test_append_rejects_opt_key_kwargs_immediately(self, db):
        """Opt-key kwargs must fail at append, before run fans out."""
        iter_task = QraftIter("myapp.tasks.process_item")
        with pytest.raises(ValueError, match="save"):
            iter_task.append(1, save=False)

        assert iter_task._items == []
        assert iter_task._model.status == WorkflowStatus.PENDING


class TestIterExecution:
    """Test iter execution and lifecycle."""

    def test_run_empty_iter_fails(self, db):
        """Test that running an empty iter raises error."""
        iter_task = QraftIter("myapp.tasks.process_item")

        with pytest.raises(ValueError, match="Cannot run empty iter"):
            iter_task.run()

    def test_run_updates_status_and_count(self, db, simple_iter):
        """Test that running updates status and total_count."""
        iter_id = simple_iter.run()

        assert iter_id == simple_iter.id
        assert simple_iter.status == WorkflowStatus.RUNNING
        assert simple_iter.total_count == 5

    def test_run_creates_tasks(self, db, simple_iter):
        """Test that running creates QraftTask instances."""
        simple_iter.run()

        # Check that tasks were created with qraft_iter FK
        tasks = QraftTask.objects.filter(qraft_iter=simple_iter._model)
        assert tasks.count() == 5

        # Verify tasks are linked to iter
        for task in tasks:
            assert task.qraft_iter_id == simple_iter.id
            assert task.status == TaskStatus.RUNNING
            assert task.func == "demo.showcase.tasks.noop_task"

    def test_cannot_run_twice(self, db, simple_iter):
        """Test that running an iter twice raises error."""
        simple_iter.run()

        with pytest.raises(ValueError, match="already run"):
            simple_iter.run()

    def test_tasks_have_no_task_level_hooks(self, db):
        """Test that workflow tasks don't have task-level hooks."""
        iter_task = QraftIter("myapp.tasks.process_item")
        iter_task.append(1)
        iter_task.run()

        task = QraftTask.objects.get(qraft_iter=iter_task._model)
        assert task.success_hook is None
        assert task.failure_hook is None


class TestIterRunAtomicity:
    """run() publishes total_count/RUNNING and creates all members in one
    transaction - a failure mid-fan-out must not leave a durably RUNNING
    workflow with a partial member set."""

    def test_member_failure_rolls_back_publish(self, db, simple_iter):
        with patch(
            "qraft.tasks.q2_async_task",
            side_effect=["q2-1", "q2-2", RuntimeError("broker down")],
        ):
            with pytest.raises(RuntimeError):
                simple_iter.run()

        model = QraftIterModel.objects.get(id=simple_iter.id)
        assert model.status == WorkflowStatus.PENDING
        assert model.total_count == 0
        assert QraftTask.objects.filter(qraft_iter=model).count() == 0

    def test_run_can_retry_after_failure(self, db, simple_iter):
        with patch(
            "qraft.tasks.q2_async_task", side_effect=RuntimeError("broker down")
        ):
            with pytest.raises(RuntimeError):
                simple_iter.run()

        with patch(
            "qraft.tasks.q2_async_task",
            side_effect=[f"q2-{i}" for i in range(5)],
        ):
            simple_iter.run()

        assert simple_iter.status == WorkflowStatus.RUNNING
        assert simple_iter.total_count == 5
        assert QraftTask.objects.filter(qraft_iter=simple_iter._model).count() == 5


class TestIterCounters:
    """Test iter counter properties."""

    def test_initial_counters(self, db, simple_iter):
        """Test initial counter values."""
        assert simple_iter.total_count == 0
        assert simple_iter.completed_count == 0
        assert simple_iter.success_count == 0
        assert simple_iter.failure_count == 0

    def test_counters_after_run(self, db, simple_iter):
        """Test counter values after run."""
        simple_iter.run()

        assert simple_iter.total_count == 5
        assert simple_iter.completed_count == 0  # No tasks completed yet
        assert simple_iter.success_count == 0
        assert simple_iter.failure_count == 0

    def test_counters_update_on_completion(self, db, simple_iter):
        """Test that counters update when tasks complete."""
        simple_iter.run()

        # Manually increment counters (normally done by ParallelDispatcher)
        model = simple_iter._model
        model.completed_count = 3
        model.success_count = 2
        model.failure_count = 1
        model.save()

        simple_iter._model.refresh_from_db()
        assert simple_iter.completed_count == 3
        assert simple_iter.success_count == 2
        assert simple_iter.failure_count == 1


class TestIterQueries:
    """Test iter status queries and result retrieval."""

    def test_length(self, db, simple_iter):
        """Test getting total count via length()."""
        assert simple_iter.length() == 0

        simple_iter.run()
        assert simple_iter.length() == 5

    def test_result_empty_iter(self, db, simple_iter):
        """Test getting results from unrun iter."""
        results = simple_iter.result()
        assert len(results) == 0

    def test_result_timeout(self, db, simple_iter):
        """Test result with timeout on incomplete iter."""
        simple_iter.run()

        with pytest.raises(TimeoutError, match="did not complete within"):
            simple_iter.result(wait=100)  # 100ms timeout


class TestIterRepr:
    """Test iter string representation."""

    def test_iter_repr(self, db, simple_iter):
        """Test iter repr."""
        simple_iter.run()
        repr_str = repr(simple_iter)

        assert "QraftIter" in repr_str
        assert str(simple_iter.id) in repr_str
        assert "running" in repr_str
        assert "0/5" in repr_str  # completed/total


class TestIterIntegration:
    """Integration tests with task execution."""

    @pytest.mark.django_db(transaction=True)
    def test_iter_with_different_args(self, db):
        """Test iter with different argument patterns."""
        iter_task = QraftIter("myapp.tasks.process")

        # Positional only
        iter_task.append(1)

        # Keyword only
        iter_task.append(user_id=2)

        # Mixed
        iter_task.append(3, status="active")

        iter_task.run()

        tasks = list(
            QraftTask.objects.filter(qraft_iter=iter_task._model).order_by(
                "date_created"
            )
        )
        assert tasks[0].task_args == [1]
        assert tasks[0].task_kwargs == {}

        assert tasks[1].task_args == []
        assert tasks[1].task_kwargs == {"user_id": 2}

        assert tasks[2].task_args == [3]
        assert tasks[2].task_kwargs == {"status": "active"}

    @pytest.mark.django_db(transaction=True)
    def test_iter_retry_policy_applies_to_all(self, db):
        """Test that default qraft_options apply to all tasks."""
        iter_task = QraftIter(
            "myapp.tasks.process",
            qraft_options={"max_attempts": 7, "base_delay": 2.0},
        )

        for i in range(3):
            iter_task.append(i)

        iter_task.run()

        tasks = QraftTask.objects.filter(qraft_iter=iter_task._model)
        for task in tasks:
            assert task.retry_policy["max_attempts"] == 7
            assert task.retry_policy["base_delay"] == 2.0

    @patch("qraft.tasks.q2_async_task")
    def test_iter_with_cluster_routing(self, mock_q2_async, db):
        """Test that cluster parameter routes all tasks to specified cluster."""
        # Return different IDs for each call to avoid unique constraint violation
        mock_q2_async.side_effect = ["q2-task-1", "q2-task-2", "q2-task-3"]

        iter_task = QraftIter(
            "myapp.tasks.process",
            cluster="io-workers",
            qraft_options={"max_attempts": 3},
        )

        for i in range(3):
            iter_task.append(i)

        iter_task.run()

        # Verify cluster was stored in default_qraft_options
        assert iter_task._model.default_qraft_options["cluster"] == "io-workers"
        assert iter_task._model.default_qraft_options["max_attempts"] == 3

        # Verify all tasks were queued with cluster parameter
        assert mock_q2_async.call_count == 3
        for call_obj in mock_q2_async.call_args_list:
            call_kwargs = call_obj[1]
            assert call_kwargs["cluster"] == "io-workers"


class TestCancelGuardedUpdate:
    """cancel() must not overwrite a completion that already committed."""

    def test_cancel_racing_committed_succeeded_is_noop(self, db):
        from qraft.models import InvalidStatusTransition

        iter_task = QraftIter("myapp.tasks.process")
        iter_task._model.status = WorkflowStatus.SUCCEEDED
        iter_task._model.save(update_fields=["status", "date_updated"])

        with pytest.raises(InvalidStatusTransition):
            iter_task.cancel()

        iter_task._model.refresh_from_db()
        assert iter_task._model.status == WorkflowStatus.SUCCEEDED

    def test_cancel_from_running_takes_effect(self, db):
        iter_task = QraftIter("myapp.tasks.process")
        iter_task._model.status = WorkflowStatus.RUNNING
        iter_task._model.save(update_fields=["status", "date_updated"])

        assert iter_task.cancel() is True
        iter_task._model.refresh_from_db()
        assert iter_task._model.status == WorkflowStatus.CANCELLED
