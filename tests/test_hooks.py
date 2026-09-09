"""Tests for qraft.hooks module."""

from unittest.mock import Mock, patch

import pytest

from qraft.hooks import (
    HookDispatcher,
    _extract_exception_class,
    _parse_qraft_marker,
    dispatch_hook_once,
    qraft_hook_handler,
)
from qraft.models import HookDispatch, QraftTask, QraftTaskAttempt, TaskStatus


class TestExtractExceptionClass:
    """Tests for _extract_exception_class function."""

    def test_extract_simple_exception(self):
        """Test extracting a simple exception class name."""
        result = "Error message : Traceback\nValueError: test error"
        exc_class = _extract_exception_class(result)
        assert exc_class == "ValueError"

    def test_extract_qualified_exception(self):
        """Test extracting a fully qualified exception class."""
        result = "Error : Traceback\nmy.module.CustomError: something went wrong"
        exc_class = _extract_exception_class(result)
        assert exc_class == "CustomError"

    def test_extract_chained_exception(self):
        """Test extracting from chained exceptions (takes last one)."""
        result = (
            "Error : Traceback\n"
            "KeyError: 'missing'\n"
            "During handling...\n"
            "ValueError: final error"
        )
        exc_class = _extract_exception_class(result)
        assert exc_class == "ValueError"

    def test_extract_with_none_result(self):
        """Test handling None result."""
        assert _extract_exception_class(None) is None

    def test_extract_with_non_string_result(self):
        """Test handling non-string result."""
        assert _extract_exception_class(123) is None

    def test_extract_with_no_exception(self):
        """Test handling result with no exception pattern."""
        result = "Just a regular message"
        assert _extract_exception_class(result) is None


class TestParseQraftMarker:
    """Tests for _parse_qraft_marker function."""

    def test_parse_valid_marker(self):
        """Test parsing a valid qraft marker."""
        marker = "qraft:123e4567-e89b-12d3-a456-426614174000:2"
        task_id, attempt = _parse_qraft_marker(marker)
        assert task_id == "123e4567-e89b-12d3-a456-426614174000"
        assert attempt == 2

    def test_parse_invalid_format(self):
        """Test parsing marker with invalid format."""
        assert _parse_qraft_marker("invalid:marker") is None
        assert _parse_qraft_marker("qraft:id") is None
        assert _parse_qraft_marker("other:id:1") is None

    def test_parse_invalid_attempt_number(self):
        """Test parsing marker with non-numeric attempt."""
        marker = "qraft:some-id:not-a-number"
        assert _parse_qraft_marker(marker) is None


@pytest.mark.django_db
class TestQraftHookHandler:
    """Tests for qraft_hook_handler function."""

    def test_handler_with_query_lookup(
        self, qraft_task, qraft_task_attempt, mock_q2_task_success
    ):
        """Test handler using fast query-based lookup."""
        mock_q2_task_success.id = qraft_task_attempt.q2_task_id

        with patch("qraft.hooks.HookDispatcher") as mock_dispatcher:
            qraft_hook_handler(mock_q2_task_success)

            # Verify attempt was updated
            qraft_task_attempt.refresh_from_db()
            assert qraft_task_attempt.success is True

            # Verify task status updated
            qraft_task.refresh_from_db()
            assert qraft_task.status == TaskStatus.SUCCEEDED

            # Verify dispatcher was called
            mock_dispatcher.assert_called_once()

    def test_handler_query_diet_on_plain_task(
        self, db, mock_q2_task_success, django_assert_max_num_queries
    ):
        """
        The monitor serializes on this handler, so the plain-task hot path is
        query-budgeted: resolve (workflow membership joined in), the locked
        status update, and nothing else. A membership lookup creeping back in
        busts the ceiling.
        """
        task = QraftTask.objects.create(
            func="test.module.plain", status=TaskStatus.RUNNING
        )
        QraftTaskAttempt.objects.create(
            qraft_task=task, attempt_number=1, q2_task_id="q2-diet-1"
        )
        mock_q2_task_success.id = "q2-diet-1"

        # 7 = resolve + savepoint/release pair + locked select + 2 updates
        # + the post-dispatch routed flip
        with django_assert_max_num_queries(7):
            qraft_hook_handler(mock_q2_task_success)

        task.refresh_from_db()
        assert task.status == TaskStatus.SUCCEEDED

    def test_handler_with_task_name_parsing(self, qraft_task, mock_q2_task_success):
        """Test handler with task_name parsing for retry tasks."""
        mock_q2_task_success.name = f"qraft:{qraft_task.id}:2"
        mock_q2_task_success.id = "new-task-id"

        with patch("qraft.hooks.HookDispatcher") as mock_dispatcher:
            qraft_hook_handler(mock_q2_task_success)

            # Verify attempt was created
            attempt = QraftTaskAttempt.objects.get(
                qraft_task=qraft_task,
                attempt_number=2,
            )
            assert attempt.q2_task_id == "new-task-id"

            # Verify dispatcher was called
            mock_dispatcher.assert_called_once()

    def test_handler_skips_non_qraft_task(self, mock_q2_task_success):
        """Test handler skips tasks without qraft marker."""
        mock_q2_task_success.name = "regular-task"

        with patch("qraft.hooks.HookDispatcher") as mock_dispatcher:
            qraft_hook_handler(mock_q2_task_success)

            # Dispatcher should not be called
            mock_dispatcher.assert_not_called()

    def test_handler_with_failed_task(
        self, qraft_task, qraft_task_attempt, mock_q2_task_failure
    ):
        """Test handler with failed task (no retry policy)."""
        mock_q2_task_failure.id = qraft_task_attempt.q2_task_id
        qraft_task.retry_policy = {}  # No retry policy
        qraft_task.save()

        with patch("qraft.hooks.HookDispatcher") as mock_dispatcher:
            qraft_hook_handler(mock_q2_task_failure)

            # Verify attempt was updated
            qraft_task_attempt.refresh_from_db()
            assert qraft_task_attempt.success is False
            assert qraft_task_attempt.exception_class == "ValueError"

            # No retry policy, so FAILED rather than EXHAUSTED
            qraft_task.refresh_from_db()
            assert qraft_task.status == TaskStatus.FAILED

            # Verify dispatcher was called (for standalone task hook dispatch)
            mock_dispatcher.assert_called_once()

    def test_handler_with_invalid_marker(self, mock_q2_task_success):
        """Test handler with invalid qraft marker."""
        mock_q2_task_success.name = "qraft:invalid-format"

        with patch("qraft.hooks.HookDispatcher") as mock_dispatcher:
            qraft_hook_handler(mock_q2_task_success)

            # Should skip processing
            mock_dispatcher.assert_not_called()


@pytest.mark.django_db
class TestHookDispatcher:
    """Tests for HookDispatcher class."""

    @pytest.mark.parametrize(
        "success, hook_field, hook_type",
        [
            (True, "success_hook", "success"),
            (False, "failure_hook", "failure"),
        ],
    )
    @patch("qraft.hooks.q2_async_task")
    def test_dispatch_records_a_hook_dispatch_for_the_outcome(
        self,
        mock_q2_async,
        success,
        hook_field,
        hook_type,
        qraft_task,
        qraft_task_attempt,
    ):
        """dispatch() queues the hook matching the outcome and records it,
        regardless of retry policy - dispatch() assumes retry was already
        handled by the caller."""
        mock_q2_async.return_value = "hook-task-123"
        setattr(qraft_task, hook_field, f"test.hooks.on_{hook_type}")
        qraft_task.save()

        qraft_task_attempt.success = success
        if not success:
            qraft_task_attempt.exception_class = "ValueError"
        qraft_task_attempt.save()

        HookDispatcher(qraft_task, qraft_task_attempt).dispatch(success=success)

        dispatch = HookDispatch.objects.get(qraft_task=qraft_task)
        assert dispatch.hook_type == hook_type
        assert dispatch.hook_path == f"test.hooks.on_{hook_type}"

    def test_dispatch_no_hook_configured(
        self, qraft_task, qraft_task_attempt, mock_q2_task_success
    ):
        """Test dispatch when no hooks are configured."""
        qraft_task.success_hook = None
        qraft_task.failure_hook = None
        qraft_task.save()

        dispatcher = HookDispatcher(qraft_task, qraft_task_attempt)
        dispatcher.dispatch(success=True)

        assert not HookDispatch.objects.filter(qraft_task=qraft_task).exists()

    @patch("qraft.hooks.q2_async_task")
    def test_call_hook_async(
        self, mock_q2_async, qraft_task, qraft_task_attempt, mock_q2_task_success
    ):
        """Test async hook dispatching."""
        mock_q2_async.return_value = "hook-task-123"

        dispatcher = HookDispatcher(qraft_task, qraft_task_attempt)

        with patch("qraft.hooks.get_conf") as mock_conf:
            mock_conf.return_value.sync_hooks = False

            dispatcher._call_hook(
                hook_path="test.hooks.success",
                args=[1, 2],
                kwargs={"key": "value"},
                hook_type="success",
            )

        # Verify hook was queued
        mock_q2_async.assert_called_once()
        call_args = mock_q2_async.call_args
        assert call_args[0][0] == "test.hooks.success"
        assert call_args[0][1:] == (1, 2)
        assert call_args[1]["key"] == "value"
        assert call_args[1]["hook"] is None  # Prevent recursion

        # Verify HookDispatch record was created
        hook_dispatch = HookDispatch.objects.get()
        assert hook_dispatch.qraft_task == qraft_task
        assert hook_dispatch.hook_type == "success"
        assert hook_dispatch.q2_task_id == "hook-task-123"

    @patch("qraft.hooks.q2_async_task")
    def test_call_hook_async_idempotency(
        self, mock_q2_async, qraft_task, qraft_task_attempt, mock_q2_task_success
    ):
        """Test that hooks are only dispatched once (idempotency)."""
        mock_q2_async.return_value = "hook-task-123"

        # Create existing hook dispatch
        HookDispatch.objects.create(
            qraft_task=qraft_task,
            hook_type="success",
            hook_path="test.hooks.success",
            q2_task_id="existing-task",
        )

        dispatcher = HookDispatcher(qraft_task, qraft_task_attempt)

        with patch("qraft.hooks.get_conf") as mock_conf:
            mock_conf.return_value.sync_hooks = False

            dispatcher._call_hook(
                hook_path="test.hooks.success",
                args=[],
                kwargs={},
                hook_type="success",
            )

        # Hook should NOT be queued again
        mock_q2_async.assert_not_called()

    @patch("qraft.hooks.import_string")
    def test_call_hook_sync(
        self, mock_import, qraft_task, qraft_task_attempt, mock_q2_task_success
    ):
        """Test synchronous hook calling."""
        mock_hook = Mock()
        mock_import.return_value = mock_hook

        dispatcher = HookDispatcher(qraft_task, qraft_task_attempt)

        with patch("qraft.hooks.get_conf") as mock_conf:
            mock_conf.return_value.sync_hooks = True

            dispatcher._call_hook(
                hook_path="test.hooks.success",
                args=[1, 2],
                kwargs={"key": "value"},
                hook_type="success",
            )

        # Verify hook was called synchronously
        mock_hook.assert_called_once_with(1, 2, key="value")


@pytest.mark.django_db
class TestCompletionRetryAtomicity:
    """The failed completion and its retry commit in one transaction (B1)."""

    def test_failed_completion_rolls_back_with_its_retry(
        self, qraft_task, qraft_task_attempt, mock_q2_task_failure
    ):
        """
        A crash between recording the failure and scheduling the retry must
        not commit the failure alone - a durably FAILED task with no retry
        row is indistinguishable from a legitimate terminal failure.
        """
        qraft_task.status = TaskStatus.RUNNING
        qraft_task.save(update_fields=["status"])
        mock_q2_task_failure.id = qraft_task_attempt.q2_task_id

        with patch("qraft.hooks.handle_task_retry", side_effect=RuntimeError("crash")):
            with pytest.raises(RuntimeError):
                qraft_hook_handler(mock_q2_task_failure)

        # Nothing committed: the attempt is still unresolved, the task still
        # RUNNING, and a redelivery can process the completion cleanly.
        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.success is None
        qraft_task.refresh_from_db()
        assert qraft_task.status == TaskStatus.RUNNING

    def test_retry_is_scheduled_when_the_completion_commits(
        self, qraft_task, qraft_task_attempt, mock_q2_task_failure
    ):
        """After the handler returns, the SCHEDULED retry already exists."""
        from qraft.models.tasks import AttemptState

        mock_q2_task_failure.id = qraft_task_attempt.q2_task_id

        with patch("qraft.hooks.HookDispatcher") as mock_dispatcher:
            qraft_hook_handler(mock_q2_task_failure)

        # Retry scheduled, so no task-level hook fires
        mock_dispatcher.assert_not_called()

        retry = qraft_task.attempts.get(attempt_number=2)
        assert retry.state == AttemptState.SCHEDULED
        assert retry.not_before is not None
        qraft_task.refresh_from_db()
        assert qraft_task.status == TaskStatus.PENDING


@pytest.mark.django_db
class TestLateResultDropped:
    """A completion for an already-resolved attempt is dropped (B2)."""

    def test_late_result_leaves_attempt_task_and_workflows_untouched(
        self, qraft_task, qraft_task_attempt, mock_q2_task_failure
    ):
        # The attempt was already settled (e.g. reaped as an orphan) and the
        # slow original worker's result arrives late.
        qraft_task_attempt.success = True
        qraft_task_attempt.save(update_fields=["success"])
        qraft_task.status = TaskStatus.SUCCEEDED
        qraft_task.save(update_fields=["status"])
        mock_q2_task_failure.id = qraft_task_attempt.q2_task_id

        with (
            patch("qraft.dispatchers.route_workflow_completion") as mock_route,
            patch("qraft.hooks.HookDispatcher") as mock_dispatcher,
            patch("qraft.hooks._logger.warning") as mock_warn,
        ):
            qraft_hook_handler(mock_q2_task_failure)

        mock_route.assert_not_called()
        mock_dispatcher.assert_not_called()
        mock_warn.assert_called_once()
        assert mock_warn.call_args[0][1:] == (
            qraft_task_attempt.attempt_number,
            qraft_task.id,
            mock_q2_task_failure.id,
        )

        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.success is True
        assert qraft_task_attempt.exception_class is None
        qraft_task.refresh_from_db()
        assert qraft_task.status == TaskStatus.SUCCEEDED
        # No retry was scheduled off the dropped failure
        assert qraft_task.attempts.count() == 1


@pytest.mark.django_db
class TestDispatchHookOnce:
    """Claim and enqueue share one transaction (B3)."""

    def test_enqueue_failure_removes_placeholder_and_logs(self, qraft_task):
        """The hook is lost on enqueue failure; nothing retries it."""

        def boom():
            raise ConnectionError("broker down")

        with patch("qraft.hooks._logger.exception") as mock_log:
            dispatch_hook_once(
                HookDispatch,
                {"qraft_task": qraft_task, "hook_type": "success"},
                "test.hooks.success",
                boom,
            )

        assert not HookDispatch.objects.exists()
        mock_log.assert_called_once()
        assert mock_log.call_args[0][1:] == ("test.hooks.success", "HookDispatch")

    def test_claim_and_enqueue_roll_back_together(self, qraft_task):
        """
        With the ORM broker the queue write lands in the same database, so a
        failure after it must take the queue row down with the placeholder -
        neither a claimed-but-unqueued hook nor a queued-but-unclaimed one.
        """

        def enqueue_then_crash():
            # Stands in for the OrmQ row the ORM broker writes
            QraftTask.objects.create(func="test.module.queue_row")
            raise RuntimeError("crash after broker write")

        dispatch_hook_once(
            HookDispatch,
            {"qraft_task": qraft_task, "hook_type": "success"},
            "test.hooks.success",
            enqueue_then_crash,
        )

        assert not HookDispatch.objects.exists()
        # Only the fixture task survives; the in-transaction write rolled back
        assert QraftTask.objects.count() == 1


@pytest.mark.django_db
class TestHookRouting:
    """A hook must land on the cluster whose worker ran the task."""

    def _dispatch(self, qraft_task, qraft_task_attempt):
        from qraft.hooks import HookDispatcher

        dispatcher = HookDispatcher(qraft_task, qraft_task_attempt)
        with patch("qraft.hooks.q2_async_task") as mock_async:
            mock_async.return_value = "q2-hook-routed"
            dispatcher._call_hook_async("test.hooks.success", [], {}, "success")
        return mock_async.call_args[1]

    def test_hook_is_pinned_to_the_executing_cluster(
        self, qraft_task, qraft_task_attempt
    ):
        from django_q.conf import Conf

        # The hook handler runs in the monitor of the cluster that executed
        # the task, so that cluster's name is the correct destination.
        assert self._dispatch(qraft_task, qraft_task_attempt)["cluster"] == (
            Conf.CLUSTER_NAME
        )

    def test_hook_failures_are_acknowledged(self, qraft_task, qraft_task_attempt):
        # Nothing retries a hook, so an unacknowledged failure would be
        # redelivered by the broker forever.
        assert self._dispatch(qraft_task, qraft_task_attempt)["ack_failure"] is True


class TestCancelledWorkflowRetrySuppression:
    """A failed member of a cancelled workflow must not schedule new retries."""

    def test_failed_member_of_cancelled_workflow_is_not_retried(
        self, db, mock_q2_task_failure
    ):
        from qraft.models import QraftIterModel, WorkflowStatus

        iter_model = QraftIterModel.objects.create(
            func="demo.showcase.tasks.noop_task",
            total_count=2,
            status=WorkflowStatus.CANCELLED,
        )
        task = QraftTask.objects.create(
            func="demo.showcase.tasks.noop_task",
            status=TaskStatus.RUNNING,
            qraft_iter=iter_model,
            retry_policy={
                "max_attempts": 3,
                "base_delay": 10.0,
                "backoff_strategy": "exponential",
                "jitter": False,
            },
        )
        attempt = QraftTaskAttempt.objects.create(
            qraft_task=task, attempt_number=1, q2_task_id=mock_q2_task_failure.id
        )

        qraft_hook_handler(mock_q2_task_failure)

        task.refresh_from_db()
        assert task.status == TaskStatus.FAILED
        # No SCHEDULED retry row was created for the cancelled workflow's member
        assert task.attempts.count() == 1
        attempt.refresh_from_db()
        assert attempt.success is False
        assert attempt.routed is True


class TestRoutedFlag:
    """Resolution commits routed=False; the flag flips after post-commit work."""

    def test_success_path_marks_attempt_routed(
        self, qraft_task, qraft_task_attempt, mock_q2_task_success
    ):
        mock_q2_task_success.id = qraft_task_attempt.q2_task_id

        with patch("qraft.hooks.HookDispatcher"):
            qraft_hook_handler(mock_q2_task_success)

        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.routed is True

    def test_scheduled_retry_marks_attempt_routed_at_commit(
        self, qraft_task, qraft_task_attempt, mock_q2_task_failure
    ):
        """A retry leaves no post-commit work, so routed commits with it."""
        mock_q2_task_failure.id = qraft_task_attempt.q2_task_id

        qraft_hook_handler(mock_q2_task_failure)

        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.routed is True
        # The retry row itself exists (and is not the replay sweep's business)
        assert qraft_task.attempts.count() == 2
