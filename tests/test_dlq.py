"""Tests for qraft.dlq module."""

from unittest.mock import Mock

import pytest
from django_q.models import Schedule

from qraft.dlq import dead_letters, requeue
from qraft.hooks import qraft_hook_handler
from qraft.models import QraftTask, QraftTaskAttempt, TaskStatus
from qraft.models.tasks import AttemptState


@pytest.mark.django_db
class TestDeadLetters:
    """Tests for dead_letters()."""

    def test_returns_only_failed_and_exhausted(self):
        succeeded = QraftTask.objects.create(func="f", status=TaskStatus.SUCCEEDED)
        pending = QraftTask.objects.create(func="f", status=TaskStatus.PENDING)
        running = QraftTask.objects.create(func="f", status=TaskStatus.RUNNING)
        failed = QraftTask.objects.create(func="f", status=TaskStatus.FAILED)
        exhausted = QraftTask.objects.create(func="f", status=TaskStatus.EXHAUSTED)

        ids = set(dead_letters().values_list("id", flat=True))

        assert ids == {failed.id, exhausted.id}
        assert succeeded.id not in ids
        assert pending.id not in ids
        assert running.id not in ids

    def test_orders_newest_first(self):
        older = QraftTask.objects.create(func="f", status=TaskStatus.FAILED)
        newer = QraftTask.objects.create(func="f", status=TaskStatus.EXHAUSTED)

        assert list(dead_letters()) == [newer, older]


@pytest.mark.django_db
class TestRequeue:
    """Tests for requeue()."""

    def test_raises_on_non_dead_task(self):
        task = QraftTask.objects.create(func="f", status=TaskStatus.RUNNING)

        with pytest.raises(ValueError, match="not dead"):
            requeue(task)

    def test_creates_next_attempt_and_resets_status(self):
        task = QraftTask.objects.create(
            func="test.module.function",
            task_args=[1, 2],
            task_kwargs={"key": "value"},
            status=TaskStatus.EXHAUSTED,
        )
        QraftTaskAttempt.objects.create(
            qraft_task=task, attempt_number=1, q2_task_id="t-1", success=False
        )
        QraftTaskAttempt.objects.create(
            qraft_task=task,
            attempt_number=2,
            q2_task_id="t-2",
            success=False,
            cluster="io-workers",
        )

        attempt_id = requeue(task)

        attempt = QraftTaskAttempt.objects.get(id=attempt_id)
        assert attempt.attempt_number == 3
        assert attempt.state == AttemptState.SCHEDULED
        assert attempt.not_before is not None
        # The operator's shell cannot know where this ran; the last attempt can.
        assert attempt.cluster == "io-workers"
        assert not Schedule.objects.exists()

        task.refresh_from_db()
        assert task.status == TaskStatus.PENDING

    def test_requeue_with_no_prior_attempts_starts_at_attempt_one(self):
        task = QraftTask.objects.create(
            func="test.module.function", status=TaskStatus.FAILED
        )

        attempt = QraftTaskAttempt.objects.get(id=requeue(task))

        assert attempt.attempt_number == 1
        # Nothing recorded a cluster, so any dispatcher may claim it.
        assert attempt.cluster is None

    def test_requeue_with_idempotency_key_does_not_violate_unique_constraint(self):
        """Requeue reuses the same row, so its idempotency_key is untouched."""
        task = QraftTask.objects.create(
            func="test.module.function",
            status=TaskStatus.EXHAUSTED,
            idempotency_key="dedupe-me",
        )
        QraftTaskAttempt.objects.create(
            qraft_task=task, attempt_number=1, q2_task_id="t-1", success=False
        )

        requeue(task)  # must not raise IntegrityError

        task.refresh_from_db()
        assert task.idempotency_key == "dedupe-me"
        assert task.status == TaskStatus.PENDING

    def test_requeued_attempt_lands_on_same_qraft_task_via_marker_fallback(self):
        """Simulates the scheduled task firing and the hook handler processing it."""
        task = QraftTask.objects.create(
            func="test.module.function",
            status=TaskStatus.EXHAUSTED,
            retry_policy={},
        )
        QraftTaskAttempt.objects.create(
            qraft_task=task, attempt_number=1, q2_task_id="t-1", success=False
        )

        requeue(task)

        # The scheduler would enqueue task_name=marker; simulate the q2 task
        # completing and the hook handler resolving it via the marker fallback.
        mock_q2_task = Mock()
        mock_q2_task.id = "t-2"
        mock_q2_task.name = f"qraft:{task.id}:2"
        mock_q2_task.success = True
        mock_q2_task.stopped = None
        mock_q2_task.result = "ok"

        qraft_hook_handler(mock_q2_task)

        attempt = QraftTaskAttempt.objects.get(qraft_task=task, attempt_number=2)
        assert attempt.q2_task_id == "t-2"
        assert attempt.success is True

        task.refresh_from_db()
        assert task.status == TaskStatus.SUCCEEDED
        assert task.attempts.count() == 2


@pytest.mark.django_db
def test_requeue_acknowledges_failures(qraft_task):
    """The requeued run must not be redelivered by the broker on failure."""

    from django_q.models import OrmQ
    from django_q.signing import SignedPackage

    from qraft.dlq import requeue
    from qraft.models import TaskStatus
    from qraft.scheduler import dispatch_due

    qraft_task.status = TaskStatus.EXHAUSTED
    qraft_task.save(update_fields=["status"])

    requeue(qraft_task)
    assert dispatch_due() == 1

    pack = SignedPackage.loads(OrmQ.objects.get().payload)
    assert pack["ack_failure"] is True
