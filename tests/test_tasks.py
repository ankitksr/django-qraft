"""Tests for qraft.tasks module."""

import functools
from datetime import timedelta
from unittest.mock import patch

import pytest
from django.utils import timezone

from qraft.models import QraftTask, QraftTaskAttempt, TaskStatus
from qraft.tasks import async_task


def _module_level_task():
    """Importable at `tests.test_tasks._module_level_task` - a valid func."""


class _CallableHolder:
    """Only used to produce a bound method that is not importable."""

    def bound_method(self):
        pass


@pytest.mark.django_db
class TestAsyncTask:
    """Tests for async_task function."""

    @patch("qraft.tasks.q2_async_task")
    def test_minimal_task_creation(self, mock_q2_async):
        """Test creating a task with minimal parameters."""
        mock_q2_async.return_value = "q2-task-123"

        result = async_task("test.module.function", 1, 2, 3)

        assert result == "q2-task-123"

        # Verify QraftTask was created
        qraft_task = QraftTask.objects.get()
        assert qraft_task.func == "test.module.function"
        assert qraft_task.task_args == [1, 2, 3]
        assert qraft_task.task_kwargs == {}
        assert qraft_task.status == TaskStatus.RUNNING

        # Verify attempt was created
        attempt = QraftTaskAttempt.objects.get()
        assert attempt.qraft_task == qraft_task
        assert attempt.attempt_number == 1
        assert attempt.q2_task_id == "q2-task-123"

    @patch("qraft.tasks.q2_async_task")
    def test_task_with_kwargs(self, mock_q2_async):
        """Test creating a task with keyword arguments."""
        mock_q2_async.return_value = "q2-task-456"

        async_task("test.function", 1, 2, key1="value1", key2="value2")

        qraft_task = QraftTask.objects.get()
        assert qraft_task.task_kwargs == {"key1": "value1", "key2": "value2"}

    @patch("qraft.tasks.q2_async_task")
    def test_task_with_success_hook(self, mock_q2_async):
        """Test creating a task with success hook."""
        mock_q2_async.return_value = "q2-task-789"

        async_task(
            "test.function",
            qraft_options={
                "success_hook": "test.hooks.on_success",
                "success_args": [1, 2],
                "success_kwargs": {"key": "value"},
            },
        )

        qraft_task = QraftTask.objects.get()
        assert qraft_task.success_hook == "test.hooks.on_success"
        assert qraft_task.success_args == [1, 2]
        assert qraft_task.success_kwargs == {"key": "value"}

    @patch("qraft.tasks.q2_async_task")
    def test_task_with_failure_hook(self, mock_q2_async):
        """Test creating a task with failure hook."""
        mock_q2_async.return_value = "q2-task-999"

        async_task(
            "test.function",
            qraft_options={
                "failure_hook": "test.hooks.on_failure",
                "failure_args": [3, 4],
                "failure_kwargs": {"error": "msg"},
            },
        )

        qraft_task = QraftTask.objects.get()
        assert qraft_task.failure_hook == "test.hooks.on_failure"
        assert qraft_task.failure_args == [3, 4]
        assert qraft_task.failure_kwargs == {"error": "msg"}

    @patch("qraft.tasks.q2_async_task")
    def test_task_with_retry_policy(self, mock_q2_async):
        """Test creating a task with retry policy."""
        mock_q2_async.return_value = "q2-task-retry"

        async_task(
            "test.function",
            qraft_options={
                "max_attempts": 5,
                "base_delay": 60.0,
                "backoff_strategy": "linear",
                "jitter": False,
            },
        )

        qraft_task = QraftTask.objects.get()
        assert qraft_task.retry_policy["max_attempts"] == 5
        assert qraft_task.retry_policy["base_delay"] == 60.0
        assert qraft_task.retry_policy["backoff_strategy"] == "linear"
        assert qraft_task.retry_policy["jitter"] is False

    @patch("qraft.tasks.q2_async_task")
    def test_task_with_callable_func(self, mock_q2_async):
        """Test creating a task with a module-level callable function."""
        mock_q2_async.return_value = "q2-task-callable"

        async_task(_module_level_task)

        qraft_task = QraftTask.objects.get()
        # Should extract module.name from callable, and it must actually
        # import back to the same function (see TestCallableValidation).
        assert qraft_task.func == f"{__name__}._module_level_task"

    @patch("qraft.tasks.q2_async_task")
    def test_legacy_hook_deprecation_warning(self, mock_q2_async):
        """Test deprecation warning for legacy hook parameter."""
        mock_q2_async.return_value = "q2-task-legacy"

        with pytest.warns(DeprecationWarning, match="hook.*deprecated"):
            async_task("test.function", hook="test.legacy_hook")

        qraft_task = QraftTask.objects.get()
        # Legacy hook should be converted to success_hook
        assert qraft_task.success_hook == "test.legacy_hook"

    @patch("qraft.tasks.q2_async_task")
    def test_legacy_hook_ignored_with_qraft_hooks(self, mock_q2_async):
        """Test legacy hook is ignored when qraft hooks are provided."""
        mock_q2_async.return_value = "q2-task-override"

        with pytest.warns(DeprecationWarning, match="Ignoring legacy"):
            async_task(
                "test.function",
                hook="test.legacy_hook",
                qraft_options={"success_hook": "test.new_hook"},
            )

        qraft_task = QraftTask.objects.get()
        assert qraft_task.success_hook == "test.new_hook"

    @patch("qraft.tasks.q2_async_task")
    def test_preserves_task_name(self, mock_q2_async):
        """Test that user's task_name is preserved."""
        mock_q2_async.return_value = "q2-task-name"

        async_task("test.function", task_name="My Custom Task")

        # Verify task_name was passed to q2_async_task
        call_kwargs = mock_q2_async.call_args[1]
        assert call_kwargs["task_name"] == "My Custom Task"

    @patch("qraft.tasks.q2_async_task")
    def test_passes_django_q2_parameters(self, mock_q2_async):
        """Test that Django-Q2 parameters are passed through."""
        mock_q2_async.return_value = "q2-task-params"

        async_task(
            "test.function",
            group="my-group",
            timeout=300,
            save=True,
            ack_failure=True,
        )

        call_kwargs = mock_q2_async.call_args[1]
        assert call_kwargs["group"] == "my-group"
        assert call_kwargs["timeout"] == 300
        assert call_kwargs["save"] is True
        assert call_kwargs["ack_failure"] is True

    @patch("qraft.tasks.q2_async_task")
    def test_sets_qraft_hook_handler(self, mock_q2_async):
        """Test that qraft_hook_handler is set as the hook."""
        mock_q2_async.return_value = "q2-task-hook"

        async_task("test.function")

        call_kwargs = mock_q2_async.call_args[1]
        assert call_kwargs["hook"] == "qraft.hooks.qraft_hook_handler"

    @patch("qraft.tasks.q2_async_task")
    def test_no_retry_policy_when_not_specified(self, mock_q2_async):
        """Test that retry_policy is empty when not specified."""
        mock_q2_async.return_value = "q2-task-no-retry"

        async_task("test.function", qraft_options={})

        qraft_task = QraftTask.objects.get()
        assert qraft_task.retry_policy == {}

    @patch("qraft.tasks.q2_async_task")
    def test_mixed_retry_and_hook_options(self, mock_q2_async):
        """Test task with both retry and hook options."""
        mock_q2_async.return_value = "q2-task-mixed"

        async_task(
            "test.function",
            1,
            2,
            key="value",
            qraft_options={
                "success_hook": "test.on_success",
                "failure_hook": "test.on_failure",
                "max_attempts": 4,
                "base_delay": 45.0,
                "backoff_strategy": "exponential",
            },
        )

        qraft_task = QraftTask.objects.get()

        # Verify hooks
        assert qraft_task.success_hook == "test.on_success"
        assert qraft_task.failure_hook == "test.on_failure"

        # Verify retry policy
        assert qraft_task.retry_policy["max_attempts"] == 4
        assert qraft_task.retry_policy["base_delay"] == 45.0
        assert qraft_task.retry_policy["backoff_strategy"] == "exponential"

        # Verify task args
        assert qraft_task.task_args == [1, 2]
        assert qraft_task.task_kwargs == {"key": "value"}


@pytest.mark.django_db
class TestAsyncTaskIdempotency:
    """Tests for idempotency_key handling in async_task."""

    @patch("qraft.tasks.q2_async_task")
    def test_second_call_same_key_returns_first_q2_task_id(self, mock_q2_async):
        """A repeated key is a no-op: it returns the original q2_task_id."""
        mock_q2_async.return_value = "q2-task-idem-1"
        first = async_task(
            "test.function", qraft_options={"idempotency_key": "same-key"}
        )

        mock_q2_async.return_value = "q2-task-idem-2"
        second = async_task(
            "test.function", qraft_options={"idempotency_key": "same-key"}
        )

        assert first == "q2-task-idem-1"
        assert second == "q2-task-idem-1"
        assert mock_q2_async.call_count == 1
        assert QraftTask.objects.count() == 1

    @patch("qraft.tasks.q2_async_task")
    def test_different_keys_create_separate_tasks(self, mock_q2_async):
        """Distinct idempotency keys are independent tasks."""
        mock_q2_async.side_effect = ["q2-task-a", "q2-task-b"]

        async_task("test.function", qraft_options={"idempotency_key": "key-a"})
        async_task("test.function", qraft_options={"idempotency_key": "key-b"})

        assert QraftTask.objects.count() == 2
        keys = set(QraftTask.objects.values_list("idempotency_key", flat=True))
        assert keys == {"key-a", "key-b"}

    @patch("qraft.tasks.q2_async_task")
    def test_key_stored_on_task(self, mock_q2_async):
        """The idempotency key is persisted on QraftTask."""
        mock_q2_async.return_value = "q2-task-store"

        async_task("test.function", qraft_options={"idempotency_key": "my-key"})

        qraft_task = QraftTask.objects.get()
        assert qraft_task.idempotency_key == "my-key"

    @patch("qraft.tasks.q2_async_task")
    def test_no_key_does_not_dedupe(self, mock_q2_async):
        """Calls without an idempotency_key are never deduped against each other."""
        mock_q2_async.side_effect = ["q2-task-x", "q2-task-y"]

        async_task("test.function")
        async_task("test.function")

        assert QraftTask.objects.count() == 2

    @patch("qraft.tasks.q2_async_task")
    def test_retry_dead_flag_reclaims_key_from_exhausted_task(self, mock_q2_async):
        """A dead task under the key no longer blocks re-enqueue with the flag set."""
        mock_q2_async.return_value = "q2-task-dead-1"
        async_task("test.function", qraft_options={"idempotency_key": "dead-key"})
        dead_task = QraftTask.objects.get()
        dead_task.status = TaskStatus.EXHAUSTED
        dead_task.save(update_fields=["status"])

        mock_q2_async.return_value = "q2-task-dead-2"
        second = async_task(
            "test.function",
            qraft_options={
                "idempotency_key": "dead-key",
                "idempotency_retry_dead": True,
            },
        )

        assert second == "q2-task-dead-2"
        assert QraftTask.objects.count() == 2
        new_task = QraftTask.objects.exclude(id=dead_task.id).get()
        assert new_task.idempotency_key == "dead-key"
        dead_task.refresh_from_db()
        assert dead_task.idempotency_key is None

    @patch("qraft.tasks.q2_async_task")
    def test_retry_dead_flag_still_dedupes_live_task(self, mock_q2_async):
        """A live (non-terminal) task under the key is still deduped, flag or not."""
        mock_q2_async.return_value = "q2-task-live-1"
        first = async_task(
            "test.function", qraft_options={"idempotency_key": "live-key"}
        )

        mock_q2_async.return_value = "q2-task-live-2"
        second = async_task(
            "test.function",
            qraft_options={
                "idempotency_key": "live-key",
                "idempotency_retry_dead": True,
            },
        )

        assert second == first
        assert QraftTask.objects.count() == 1

    @patch("qraft.tasks.q2_async_task")
    def test_retry_dead_flag_default_unchanged(self, mock_q2_async):
        """Without the flag, a dead task under the key still blocks re-enqueue."""
        mock_q2_async.return_value = "q2-task-default-1"
        first = async_task(
            "test.function", qraft_options={"idempotency_key": "default-key"}
        )
        dead_task = QraftTask.objects.get()
        dead_task.status = TaskStatus.FAILED
        dead_task.save(update_fields=["status"])

        mock_q2_async.return_value = "q2-task-default-2"
        second = async_task(
            "test.function", qraft_options={"idempotency_key": "default-key"}
        )

        assert second == first
        assert QraftTask.objects.count() == 1


@pytest.mark.django_db
class TestAckFailure:
    """
    Qraft schedules its own retries, so Django-Q2 must never also redeliver.

    Django-Q2 acknowledges a broker message only on success or when
    ack_failure is set, so leaving it unset means a failed task's message is
    redelivered while Qraft has already queued the next attempt.
    """

    @patch("qraft.tasks.q2_async_task")
    def test_defaults_to_true(self, mock_q2_async):
        mock_q2_async.return_value = "q2-ack"

        async_task("test.function")

        assert mock_q2_async.call_args[1]["ack_failure"] is True

    @patch("qraft.tasks.q2_async_task")
    def test_explicit_true_is_accepted(self, mock_q2_async):
        mock_q2_async.return_value = "q2-ack"

        async_task("test.function", ack_failure=True)

        assert mock_q2_async.call_args[1]["ack_failure"] is True

    def test_explicit_false_is_rejected(self, db):
        with pytest.raises(ValueError, match="ack_failure=False"):
            async_task("test.function", ack_failure=False)

    @patch("qraft.tasks.q2_async_task")
    def test_workflow_tasks_also_acknowledge_failures(self, mock_q2_async, db):
        from qraft.models import QraftIterModel
        from qraft.tasks import _create_workflow_task

        mock_q2_async.return_value = "q2-workflow-ack"
        iter_model = QraftIterModel.objects.create(func="test.function", total_count=1)

        _create_workflow_task(
            func="test.function",
            args=[],
            kwargs={},
            qraft_options={},
            qraft_iter_id=iter_model.id,
        )

        assert mock_q2_async.call_args[1]["ack_failure"] is True


@pytest.mark.django_db
class TestReservedQOptions:
    """
    A1: q_options is merged by django_q ahead of the plain keyword
    arguments, so a reserved key slipped in there would silently override
    an invariant the top-level guards above exist to protect.
    """

    @pytest.mark.parametrize(
        "key,value",
        [
            ("hook", "some.other.hook"),
            ("save", False),
            ("sync", True),
            ("cached", 60),
            ("ack_failure", False),
            ("broker", object()),
            ("cluster", "other-cluster"),
        ],
    )
    def test_reserved_key_is_rejected(self, key, value):
        with pytest.raises(ValueError, match=key):
            async_task("test.function", q_options={key: value})

    @patch("qraft.tasks.q2_async_task")
    def test_harmless_key_passes_through(self, mock_q2_async):
        """`timeout` is not Qraft-managed, so q_options is free to carry it."""
        mock_q2_async.return_value = "q2-task-qopt"

        async_task("test.function", q_options={"timeout": 120})

        call_kwargs = mock_q2_async.call_args[1]
        assert call_kwargs["q_options"] == {"timeout": 120}


@pytest.mark.django_db
class TestForcedSavePersistence:
    """
    A3: save=True must always reach django_q. With Conf.SAVE_LIMIT < 0,
    Django-Q2's monitor only saves a successful Task row when the task asks
    for save=True - without a row, the hook (a post_save receiver) never
    fires, and the lease reaper later re-executes the task as a false
    orphan.
    """

    @patch("qraft.tasks.q2_async_task")
    def test_save_true_when_caller_passes_nothing(self, mock_q2_async):
        mock_q2_async.return_value = "q2-save-default"

        async_task("test.function")

        assert mock_q2_async.call_args[1]["save"] is True

    @patch("qraft.tasks.q2_async_task")
    def test_save_true_alongside_other_kwargs(self, mock_q2_async):
        mock_q2_async.return_value = "q2-save-other"

        async_task("test.function", group="g", timeout=5)

        assert mock_q2_async.call_args[1]["save"] is True

    def test_save_false_still_rejected(self):
        with pytest.raises(ValueError, match="save=False"):
            async_task("test.function", save=False)

    @patch("qraft.tasks.q2_async_task")
    def test_workflow_tasks_also_force_save(self, mock_q2_async):
        """_create_workflow_task builds its own q2_kwargs; an unsaved
        successful member never counts and hangs the workflow."""
        from qraft.models import QraftIterModel
        from qraft.tasks import _create_workflow_task

        mock_q2_async.return_value = "q2-workflow-save"
        iter_model = QraftIterModel.objects.create(func="test.function", total_count=1)

        _create_workflow_task(
            func="test.function",
            args=[],
            kwargs={},
            qraft_options={},
            qraft_iter_id=iter_model.id,
        )

        assert mock_q2_async.call_args[1]["save"] is True


@pytest.mark.django_db
class TestWorkflowMemberOptKeyCollision:
    """
    Member kwargs that share a name with django_q opt_keys would override
    Qraft's forced hook/save/ack_failure/group and never reach the function.
    """

    def test_save_false_rejected(self, db):
        from qraft.models import QraftIterModel
        from qraft.tasks import _create_workflow_task

        iter_model = QraftIterModel.objects.create(func="test.function", total_count=1)
        with pytest.raises(ValueError, match="save"):
            _create_workflow_task(
                func="test.function",
                args=[],
                kwargs={"save": False},
                qraft_options={},
                qraft_iter_id=iter_model.id,
            )

    def test_hook_rejected(self, db):
        from qraft.models import QraftIterModel
        from qraft.tasks import _create_workflow_task

        iter_model = QraftIterModel.objects.create(func="test.function", total_count=1)
        with pytest.raises(ValueError, match="hook"):
            _create_workflow_task(
                func="test.function",
                args=[],
                kwargs={"hook": "evil.hook"},
                qraft_options={},
                qraft_iter_id=iter_model.id,
            )

    def test_q_options_rejected(self, db):
        from qraft.models import QraftIterModel
        from qraft.tasks import _create_workflow_task

        iter_model = QraftIterModel.objects.create(func="test.function", total_count=1)
        with pytest.raises(ValueError, match="q_options"):
            _create_workflow_task(
                func="test.function",
                args=[],
                kwargs={"q_options": {"hook": "evil", "save": False}},
                qraft_options={},
                qraft_iter_id=iter_model.id,
            )

    def test_task_name_rejected(self, db):
        from qraft.models import QraftIterModel
        from qraft.tasks import _create_workflow_task

        iter_model = QraftIterModel.objects.create(func="test.function", total_count=1)
        with pytest.raises(ValueError, match="task_name"):
            _create_workflow_task(
                func="test.function",
                args=[],
                kwargs={"task_name": "x"},
                qraft_options={},
                qraft_iter_id=iter_model.id,
            )

    @patch("qraft.tasks.q2_async_task")
    def test_harmless_kwarg_passes(self, mock_q2_async, db):
        from qraft.models import QraftIterModel
        from qraft.tasks import _create_workflow_task

        mock_q2_async.return_value = "q2-workflow-ok"
        iter_model = QraftIterModel.objects.create(func="test.function", total_count=1)

        _create_workflow_task(
            func="test.function",
            args=[],
            kwargs={"payload": {"n": 1}},
            qraft_options={},
            qraft_iter_id=iter_model.id,
        )

        call_kwargs = mock_q2_async.call_args[1]
        assert call_kwargs["payload"] == {"n": 1}
        assert call_kwargs["save"] is True
        assert call_kwargs["hook"] == "qraft.hooks.qraft_hook_handler"


@pytest.mark.django_db
class TestAsyncTaskKwargsOptKeyCollision:
    """
    Opt-key names in **kwargs split attempt 1 (consumed as options) from
    retry (replayed into the function). Real async_task() parameters
    (including cluster) never land in **kwargs; chain/iter_count still can.
    """

    def test_chain_kwarg_rejected(self):
        with pytest.raises(ValueError, match="chain"):
            async_task("test.function", chain=["a.b", "c.d"])

    @patch("qraft.tasks.q2_async_task")
    def test_cluster_via_qraft_options_still_works(self, mock_q2_async):
        mock_q2_async.return_value = "q2-cluster-ok"

        async_task("test.function", qraft_options={"cluster": "io-workers"})

        assert mock_q2_async.call_args[1]["cluster"] == "io-workers"


@pytest.mark.django_db
class TestAsyncTaskClusterParam:
    """Named cluster= routes like qraft_options['cluster'] and stays out of task_kwargs."""

    @patch("qraft.tasks.q2_async_task")
    def test_cluster_param_routes(self, mock_q2_async):
        mock_q2_async.return_value = "q2-cluster-param"

        async_task("test.function", cluster="io-workers")

        assert mock_q2_async.call_args[1]["cluster"] == "io-workers"
        attempt = QraftTaskAttempt.objects.get()
        assert attempt.cluster == "io-workers"

    @patch("qraft.tasks.q2_async_task")
    def test_cluster_param_not_in_task_kwargs(self, mock_q2_async):
        mock_q2_async.return_value = "q2-cluster-no-kwargs"

        async_task("test.function", cluster="io-workers", payload=1)

        qraft_task = QraftTask.objects.get()
        assert "cluster" not in qraft_task.task_kwargs
        assert qraft_task.task_kwargs == {"payload": 1}

    @patch("qraft.tasks.q2_async_task")
    def test_cluster_param_matches_qraft_options(self, mock_q2_async):
        mock_q2_async.return_value = "q2-cluster-agree"

        async_task(
            "test.function",
            cluster="io-workers",
            qraft_options={"cluster": "io-workers"},
        )

        assert mock_q2_async.call_args[1]["cluster"] == "io-workers"

    def test_cluster_param_conflicts_with_qraft_options(self):
        with pytest.raises(ValueError, match="conflicts"):
            async_task(
                "test.function",
                cluster="io-workers",
                qraft_options={"cluster": "cpu-workers"},
            )


@pytest.mark.django_db
class TestIdempotencyBackoffDedupe:
    """
    A4: a SCHEDULED backoff attempt has a null q2_task_id until a dispatcher
    claims it, so the dedupe lookup must not mistake that for "no task holds
    this key" - it has to walk attempts for the last one that actually
    reached the broker.
    """

    @patch("qraft.tasks.q2_async_task")
    def test_second_call_during_backoff_returns_original_id_no_integrity_error(
        self, mock_q2_async
    ):
        from qraft.scheduler import schedule_attempt

        mock_q2_async.return_value = "q2-original"
        first = async_task(
            "test.function", qraft_options={"idempotency_key": "backoff-key"}
        )
        qraft_task = QraftTask.objects.get()

        # Simulate a retry sitting in backoff: attempt 2 is SCHEDULED with
        # q2_task_id still null until a dispatcher claims and enqueues it.
        schedule_attempt(qraft_task, 2, timezone.now() + timedelta(seconds=30))

        mock_q2_async.reset_mock()
        second = async_task(
            "test.function", qraft_options={"idempotency_key": "backoff-key"}
        )

        assert first == "q2-original"
        assert second == "q2-original"
        assert mock_q2_async.call_count == 0
        assert QraftTask.objects.count() == 1
        assert QraftTaskAttempt.objects.filter(qraft_task=qraft_task).count() == 2


@pytest.mark.django_db
class TestCallableValidation:
    """
    A5: async_task() re-runs a retry by importing the stored dotted path,
    not the live object, so a callable's derived path has to round-trip
    back to that exact object at enqueue time - not fail later, mid-retry.
    """

    @patch("qraft.tasks.q2_async_task")
    def test_module_level_function_is_accepted(self, mock_q2_async):
        mock_q2_async.return_value = "q2-callable-ok"

        result = async_task(_module_level_task)

        assert result == "q2-callable-ok"
        assert QraftTask.objects.get().func == f"{__name__}._module_level_task"

    def test_bound_method_is_rejected(self):
        holder = _CallableHolder()
        with pytest.raises(ValueError, match="cannot import"):
            async_task(holder.bound_method)

    def test_functools_partial_is_rejected(self):
        partial_func = functools.partial(_module_level_task)
        with pytest.raises(ValueError, match="__module__/__name__"):
            async_task(partial_func)
