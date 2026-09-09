"""Tests for qraft.models."""

import pytest
from django.db import IntegrityError

from qraft.models import HookDispatch, QraftTask, QraftTaskAttempt, TaskStatus


@pytest.mark.django_db
class TestQraftTask:
    """Tests for QraftTask model."""

    def test_attempt_count_property(self, qraft_task):
        """Test attempt_count property."""
        assert qraft_task.attempt_count == 0

        # Add attempts
        QraftTaskAttempt.objects.create(
            qraft_task=qraft_task,
            attempt_number=1,
            q2_task_id="task-1",
        )
        assert qraft_task.attempt_count == 1

        QraftTaskAttempt.objects.create(
            qraft_task=qraft_task,
            attempt_number=2,
            q2_task_id="task-2",
        )
        assert qraft_task.attempt_count == 2

    def test_latest_attempt_property(self, qraft_task):
        """Test latest_attempt property."""
        assert qraft_task.latest_attempt is None

        attempt1 = QraftTaskAttempt.objects.create(
            qraft_task=qraft_task,
            attempt_number=1,
            q2_task_id="task-1",
        )
        assert qraft_task.latest_attempt == attempt1

        attempt2 = QraftTaskAttempt.objects.create(
            qraft_task=qraft_task,
            attempt_number=2,
            q2_task_id="task-2",
        )
        assert qraft_task.latest_attempt == attempt2

    def test_string_representation(self, qraft_task):
        """Test __str__ method."""
        assert str(qraft_task) == f"QraftTask {qraft_task.id} (pending)"

        qraft_task.status = TaskStatus.SUCCEEDED
        qraft_task.save()
        assert str(qraft_task) == f"QraftTask {qraft_task.id} (succeeded)"


@pytest.mark.django_db
class TestQraftTaskAttempt:
    """Tests for QraftTaskAttempt model."""

    def test_unique_q2_task_id(self, qraft_task):
        """Test that q2_task_id must be unique."""
        QraftTaskAttempt.objects.create(
            qraft_task=qraft_task,
            attempt_number=1,
            q2_task_id="unique-id",
        )

        task2 = QraftTask.objects.create(func="test.function2")

        # Should raise IntegrityError for duplicate q2_task_id
        with pytest.raises(IntegrityError):
            QraftTaskAttempt.objects.create(
                qraft_task=task2,
                attempt_number=1,
                q2_task_id="unique-id",
            )

    def test_unique_attempt_per_task(self, qraft_task):
        """Test unique constraint on (qraft_task, attempt_number)."""
        QraftTaskAttempt.objects.create(
            qraft_task=qraft_task,
            attempt_number=1,
            q2_task_id="task-1",
        )

        # Should raise IntegrityError for duplicate attempt number
        with pytest.raises(IntegrityError):
            QraftTaskAttempt.objects.create(
                qraft_task=qraft_task,
                attempt_number=1,
                q2_task_id="task-2",
            )

    def test_string_representation(self, qraft_task_attempt):
        """Test __str__ method."""
        # Pending state
        assert "pending" in str(qraft_task_attempt)

        # Success state
        qraft_task_attempt.success = True
        qraft_task_attempt.save()
        assert "ok" in str(qraft_task_attempt)

        # Failed state
        qraft_task_attempt.success = False
        qraft_task_attempt.save()
        assert "failed" in str(qraft_task_attempt)


@pytest.mark.django_db
class TestHookDispatch:
    """Tests for HookDispatch model."""

    def test_unique_hook_dispatch_per_task_type(self, qraft_task):
        """Test unique constraint on (qraft_task, hook_type)."""
        HookDispatch.objects.create(
            qraft_task=qraft_task,
            hook_type="success",
            hook_path="test.hooks.success",
            q2_task_id="task-1",
        )

        # Should raise IntegrityError for duplicate hook type
        with pytest.raises(IntegrityError):
            HookDispatch.objects.create(
                qraft_task=qraft_task,
                hook_type="success",
                hook_path="test.hooks.success",
                q2_task_id="task-2",
            )

    def test_string_representation(self, qraft_task):
        """Test __str__ method."""
        dispatch = HookDispatch.objects.create(
            qraft_task=qraft_task,
            hook_type="success",
            hook_path="test.hooks.success",
            q2_task_id="task-1",
        )

        assert "success" in str(dispatch)
        assert str(qraft_task.id) in str(dispatch)

    def test_cascade_delete(self, qraft_task):
        """Test that hook dispatches are deleted when task is deleted."""
        HookDispatch.objects.create(
            qraft_task=qraft_task,
            hook_type="success",
            hook_path="test.hooks.success",
            q2_task_id="task-1",
        )

        assert HookDispatch.objects.count() == 1

        qraft_task.delete()

        assert HookDispatch.objects.count() == 0


@pytest.mark.django_db
class TestSubjectAndObservabilityFields:
    def test_for_subject_and_new_columns(self, qraft_task):
        from qraft.models import (
            QraftBatchModel,
            QraftChainModel,
            QraftIterModel,
            QraftTaskAttempt,
        )

        older = QraftTask.objects.create(
            func="t.f", subject_type="worksheet", subject_id="4117"
        )
        newer = QraftTask.objects.create(
            func="t.g", subject_type="worksheet", subject_id="4117"
        )
        QraftTask.objects.create(func="t.h", subject_type="worksheet", subject_id="1")

        # Newest first, int ids coerced, rows without a subject excluded.
        assert list(QraftTask.objects.for_subject("worksheet", 4117)) == [newer, older]
        assert qraft_task.subject_type is None
        assert qraft_task not in QraftTask.objects.for_subject("worksheet", "4117")
        assert "qraft_task_subject_idx" in {
            index.name for index in QraftTask._meta.indexes
        }
        assert qraft_task.hook_context is False

        attempt = QraftTaskAttempt.objects.create(
            qraft_task=qraft_task, attempt_number=1, q2_task_id="q2-new-cols"
        )
        assert attempt.progress is None
        assert attempt.progress_reported_at is None
        assert attempt.progress_advanced_at is None
        assert attempt.enqueued_at is None
        assert attempt.trace_context is None

        for model in (QraftChainModel, QraftIterModel, QraftBatchModel):
            names = {field.name for field in model._meta.get_fields()}
            assert {"subject_type", "subject_id", "settled_at", "hook_context"} <= names
