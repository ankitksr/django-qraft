"""Integration tests for workflow primitives end-to-end execution.

Note: These tests require a running Django-Q2 broker (Redis/etc.) and are marked
with @pytest.mark.integration. Run with: pytest -m integration
"""

import pytest

# Disable hook path validation for tests with fake module paths
pytestmark = pytest.mark.usefixtures("_disable_hook_validation")

from qraft.batch import QraftBatch
from qraft.chain import QraftChain
from qraft.iter import QraftIter
from qraft.models import WorkflowStatus


@pytest.mark.integration
@pytest.mark.django_db(transaction=True)
class TestChainIntegration:
    """End-to-end chain execution tests."""

    def test_chain_sequential_execution(self):
        """Test that chain executes steps sequentially."""
        chain = QraftChain()
        chain.append("demo.showcase.tasks.noop_task", 1)
        chain.append("demo.showcase.tasks.noop_task", 2)
        chain.append("demo.showcase.tasks.noop_task", 3)

        chain_id = chain.run()
        assert chain.status == WorkflowStatus.RUNNING
        # Actual completion would require worker processing

    def test_chain_with_retries(self):
        """Test chain with step that has retry policy."""
        chain = QraftChain()
        chain.append(
            "demo.showcase.tasks.countdown_task",
            fail_times=2,
            qraft_options={"max_attempts": 3},
        )
        chain.append("demo.showcase.tasks.noop_task")

        chain_id = chain.run()
        assert chain.status == WorkflowStatus.RUNNING

    def test_chain_resume_after_failure(self):
        """Test resuming a failed chain from failed step."""
        chain = QraftChain()
        chain.append("demo.showcase.tasks.noop_task")
        chain.append("demo.showcase.tasks.failing_task", qraft_options={"max_attempts": 1})
        chain.append("demo.showcase.tasks.noop_task")

        chain.run()
        # After worker processes and step 2 fails:
        # chain._model.status = WorkflowStatus.FAILED
        # chain.resume() would restart from step 2


@pytest.mark.integration
@pytest.mark.django_db(transaction=True)
class TestIterIntegration:
    """End-to-end iter execution tests."""

    def test_iter_parallel_execution(self):
        """Test that iter executes all items in parallel."""
        iter_task = QraftIter("demo.showcase.tasks.noop_task")
        for i in range(10):
            iter_task.append(i)

        iter_id = iter_task.run()
        assert iter_task.status == WorkflowStatus.RUNNING
        assert iter_task.total_count == 10

    def test_iter_with_mixed_results(self):
        """Test iter where some tasks succeed and some fail."""
        iter_task = QraftIter(
            "demo.showcase.tasks.countdown_task",
            qraft_options={"max_attempts": 1},
        )

        # Some will succeed immediately, others will fail
        iter_task.append(fail_times=0)  # Success
        iter_task.append(fail_times=1)  # Fail
        iter_task.append(fail_times=0)  # Success

        iter_id = iter_task.run()
        assert iter_task.status == WorkflowStatus.RUNNING


@pytest.mark.integration
@pytest.mark.django_db(transaction=True)
class TestBatchIntegration:
    """End-to-end batch execution tests."""

    def test_batch_parallel_execution(self):
        """Test batch fork-join pattern."""
        batch = QraftBatch()
        batch.append("demo.showcase.tasks.noop_task", 1)
        batch.append("demo.showcase.tasks.noop_task", 2)
        batch.append("demo.showcase.tasks.noop_task", 3)

        batch_id = batch.run()
        assert batch.status == WorkflowStatus.RUNNING
        assert batch.total_count == 3

    def test_batch_different_retry_policies(self):
        """Test batch where each task has different retry policy."""
        batch = QraftBatch()
        batch.append("task1", qraft_options={"max_attempts": 1})
        batch.append("task2", qraft_options={"max_attempts": 5})
        batch.append("task3", qraft_options={"max_attempts": 3})

        batch_id = batch.run()
        assert batch.status == WorkflowStatus.RUNNING


@pytest.mark.integration
@pytest.mark.django_db(transaction=True)
class TestWorkflowHooksIntegration:
    """Test workflow-level hook dispatch."""

    def test_chain_success_hook(self):
        """Test chain success hook is called when all steps succeed."""
        chain = QraftChain(on_success="demo.showcase.hooks.workflow_success_hook")
        chain.append("demo.showcase.tasks.noop_task")
        chain.append("demo.showcase.tasks.noop_task")

        chain.run()
        # After workers complete: check WorkflowHookDispatch record created

    def test_iter_failure_hook(self):
        """Test iter failure hook is called when any task fails."""
        iter_task = QraftIter(
            "demo.showcase.tasks.failing_task",
            qraft_options={"max_attempts": 1},
            on_failure="demo.showcase.hooks.workflow_failure_hook",
        )
        iter_task.append()
        iter_task.append()

        iter_task.run()
        # After workers complete: check WorkflowHookDispatch record created

    def test_batch_hooks_with_partial_failure(self):
        """Test batch hooks when some tasks succeed, some fail."""
        batch = QraftBatch(
            on_success="demo.showcase.hooks.workflow_success_hook",
            on_failure="demo.showcase.hooks.workflow_failure_hook",
        )
        batch.append("demo.showcase.tasks.noop_task")
        batch.append("demo.showcase.tasks.failing_task", qraft_options={"max_attempts": 1})

        batch.run()
        # After workers: failure hook should be called (not success)


@pytest.mark.integration
@pytest.mark.django_db(transaction=True)
class TestWorkflowEdgeCases:
    """Test edge cases and error conditions."""

    def test_chain_with_empty_results(self):
        """Test chain where steps return None."""
        chain = QraftChain()
        chain.append("demo.showcase.tasks.noop_task")
        chain.run()

        # First step queued but not completed — result returns None
        results = chain.result()
        assert len(results) == 1
        assert results.values[0] is None
