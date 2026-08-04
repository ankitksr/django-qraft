"""Tests for QraftChain sequential workflow primitive."""

from unittest.mock import patch

import pytest
from django.db import IntegrityError

# Disable hook path validation for tests with fake module paths
pytestmark = pytest.mark.usefixtures("_disable_hook_validation")

from qraft.chain import QraftChain
from qraft.models import (
    QraftChainModel,
    QraftChainStep,
    QraftTask,
    TaskStatus,
    WorkflowStatus,
)


@pytest.fixture
def simple_chain():
    """Create a simple 3-step chain."""
    chain = QraftChain()
    chain.append("demo.showcase.tasks.noop_task", 1)
    chain.append("demo.showcase.tasks.noop_task", 2)
    chain.append("demo.showcase.tasks.noop_task", 3)
    return chain


class TestChainCreation:
    """Test chain creation and configuration."""

    def test_create_empty_chain(self, db):
        """Test creating an empty chain."""
        chain = QraftChain()
        assert chain.id is not None
        assert chain.status == WorkflowStatus.PENDING
        assert isinstance(chain._model, QraftChainModel)

    def test_create_chain_with_hooks(self, db):
        """Test creating a chain with success/failure hooks."""
        chain = QraftChain(
            on_success="myapp.hooks.success",
            on_failure="myapp.hooks.failure",
            success_args=(1, 2),
            success_kwargs={"key": "value"},
            failure_args=(3,),
            failure_kwargs={"error": True},
        )

        model = chain._model
        assert model.success_hook == "myapp.hooks.success"
        assert model.failure_hook == "myapp.hooks.failure"
        assert model.success_args == [1, 2]
        assert model.success_kwargs == {"key": "value"}
        assert model.failure_args == [3]
        assert model.failure_kwargs == {"error": True}

    def test_load_existing_chain(self, db):
        """Test loading an existing chain by ID."""
        chain1 = QraftChain()
        chain_id = chain1.id

        chain2 = QraftChain(chain_id=chain_id)
        assert chain2.id == chain_id
        assert chain2._model.id == chain1._model.id


class TestChainStepManagement:
    """Test appending and managing chain steps."""

    def test_append_step(self, db):
        """Test appending a step to the chain."""
        chain = QraftChain()
        chain.append("myapp.tasks.task1", 1, 2, key="value")

        assert len(chain._steps) == 1
        step = chain._steps[0]
        assert step["func"] == "myapp.tasks.task1"
        assert step["task_args"] == [1, 2]
        assert step["task_kwargs"] == {"key": "value"}
        assert step["qraft_options"] == {}

    def test_append_step_with_qraft_options(self, db):
        """Test appending a step with Qraft options."""
        chain = QraftChain()
        chain.append(
            "myapp.tasks.task1",
            qraft_options={"max_attempts": 3, "backoff_strategy": "exponential"},
        )

        step = chain._steps[0]
        assert step["qraft_options"] == {
            "max_attempts": 3,
            "backoff_strategy": "exponential",
        }

    def test_append_multiple_steps(self, db, simple_chain):
        """Test appending multiple steps."""
        assert len(simple_chain._steps) == 3
        assert simple_chain._steps[0]["task_args"] == [1]
        assert simple_chain._steps[1]["task_args"] == [2]
        assert simple_chain._steps[2]["task_args"] == [3]

    def test_cannot_append_after_run(self, db, simple_chain):
        """Test that appending after run raises error."""
        simple_chain.run()

        with pytest.raises(ValueError, match="already been run"):
            simple_chain.append("myapp.tasks.task4")


class TestChainExecution:
    """Test chain execution and lifecycle."""

    def test_run_empty_chain_fails(self, db):
        """Test that running an empty chain raises error."""
        chain = QraftChain()

        with pytest.raises(ValueError, match="Cannot run empty chain"):
            chain.run()

    def test_run_creates_step_records(self, db, simple_chain):
        """Test that running a chain creates step records."""
        chain_id = simple_chain.run()

        # Check chain status
        assert simple_chain.status == WorkflowStatus.RUNNING
        assert chain_id == simple_chain.id

        # Check step records created
        steps = QraftChainStep.objects.filter(chain_id=chain_id).order_by("step_index")
        assert steps.count() == 3

        # Verify step data
        assert steps[0].step_index == 0
        assert steps[0].func == "demo.showcase.tasks.noop_task"
        assert steps[0].task_args == [1]

        assert steps[1].step_index == 1
        assert steps[2].step_index == 2

    def test_run_queues_first_step(self, db, simple_chain):
        """Test that running a chain queues the first step."""
        simple_chain.run()

        # Check that first step has a linked QraftTask
        first_step = QraftChainStep.objects.get(chain=simple_chain._model, step_index=0)
        assert first_step.qraft_task is not None
        assert isinstance(first_step.qraft_task, QraftTask)
        assert first_step.qraft_task.status == TaskStatus.RUNNING

    def test_cannot_run_twice(self, db, simple_chain):
        """Test that running a chain twice raises error."""
        simple_chain.run()

        with pytest.raises(ValueError, match="already run"):
            simple_chain.run()

    def test_step_unique_constraint(self, db):
        """Test that duplicate step_index for same chain is prevented."""
        chain = QraftChain()
        chain.append("myapp.tasks.task1")
        chain.run()

        # Try to create duplicate step manually
        with pytest.raises(IntegrityError):
            QraftChainStep.objects.create(
                chain=chain._model,
                step_index=0,
                func="myapp.tasks.task2",
            )


class TestChainResume:
    """Test chain resume capability."""

    def test_resume_failed_chain(self, db, simple_chain):
        """Test resuming a failed chain."""
        simple_chain.run()

        # Manually mark chain as failed
        simple_chain._model.status = WorkflowStatus.FAILED
        simple_chain._model.current_step_index = 1
        simple_chain._model.save()

        # Get the current step and mark its task as exhausted
        current_step = simple_chain._model.steps.get(step_index=1)
        old_task = QraftTask.objects.create(
            func="demo.showcase.tasks.noop_task",
            status=TaskStatus.EXHAUSTED,
        )
        current_step.qraft_task = old_task
        current_step.save()

        # Resume should work
        chain_id = simple_chain.resume()
        assert chain_id == simple_chain.id
        assert simple_chain.status == WorkflowStatus.RUNNING

        # Check that step's task was replaced with a new one
        current_step.refresh_from_db()
        assert current_step.qraft_task is not None
        assert current_step.qraft_task != old_task
        assert current_step.qraft_task.status == TaskStatus.RUNNING

    def test_resume_non_failed_chain_fails(self, db, simple_chain):
        """Test that resuming a non-failed chain raises error."""
        with pytest.raises(ValueError, match="Can only resume failed chains"):
            simple_chain.resume()

        simple_chain.run()
        with pytest.raises(ValueError, match="Can only resume failed chains"):
            simple_chain.resume()


class TestChainQueries:
    """Test chain status queries and result retrieval."""

    def test_current_step_index(self, db, simple_chain):
        """Test getting current step index."""
        assert simple_chain.current() == 0

        simple_chain.run()
        assert simple_chain.current() == 0

        # Manually advance
        simple_chain._model.current_step_index = 2
        simple_chain._model.save()
        assert simple_chain.current() == 2

    def test_result_empty_chain(self, db, simple_chain):
        """Test getting results from unrun chain."""
        results = simple_chain.result()
        assert len(results) == 0

    def test_result_partial_chain(self, db, simple_chain):
        """Test getting results from partially executed chain."""
        simple_chain.run()

        # Only first step queued, no completions yet
        results = simple_chain.result()
        assert len(results) == 1  # Only first step

    def test_result_timeout(self, db, simple_chain):
        """Test result with timeout on incomplete chain."""
        simple_chain.run()

        with pytest.raises(TimeoutError, match="did not complete within"):
            simple_chain.result(wait=100)  # 100ms timeout


class TestChainRepr:
    """Test chain string representation."""

    def test_chain_repr(self, db):
        """Test chain repr."""
        chain = QraftChain()
        repr_str = repr(chain)

        assert "QraftChain" in repr_str
        assert str(chain.id) in repr_str
        assert "pending" in repr_str


class TestChainIntegration:
    """Integration tests with actual task execution."""

    @pytest.mark.django_db(transaction=True)
    def test_chain_with_different_retry_policies(self, db):
        """Test that each step can have different retry policies."""
        chain = QraftChain()
        chain.append("task1", qraft_options={"max_attempts": 1})
        chain.append("task2", qraft_options={"max_attempts": 5})
        chain.append("task3", qraft_options={"max_attempts": 3})

        chain.run()

        steps = chain._model.steps.all()
        assert steps[0].qraft_options["max_attempts"] == 1
        assert steps[1].qraft_options["max_attempts"] == 5
        assert steps[2].qraft_options["max_attempts"] == 3

    @patch("qraft.tasks.q2_async_task")
    def test_chain_with_cluster_routing(self, mock_q2_async, db):
        """Test that each step can route to different clusters."""
        mock_q2_async.return_value = "q2-task-123"

        chain = QraftChain()
        chain.append("task1", qraft_options={"cluster": "io-workers"})
        chain.append("task2", qraft_options={"cluster": "cpu-workers"})
        chain.append("task3", qraft_options={"cluster": "default"})

        chain.run()

        # Verify cluster routing was stored in step options
        steps = chain._model.steps.all()
        assert steps[0].qraft_options["cluster"] == "io-workers"
        assert steps[1].qraft_options["cluster"] == "cpu-workers"
        assert steps[2].qraft_options["cluster"] == "default"

        # Verify first step was queued with cluster parameter
        mock_q2_async.assert_called_once()
        call_kwargs = mock_q2_async.call_args[1]
        assert call_kwargs["cluster"] == "io-workers"
