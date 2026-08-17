"""Tests for qraft.retention (bounded pruning of settled rows)."""

from datetime import timedelta
from unittest.mock import patch

import pytest
from django.utils import timezone

from qraft.models import (
    HookDispatch,
    QraftBatchModel,
    QraftChainModel,
    QraftChainStep,
    QraftIterModel,
    QraftTask,
    QraftTaskAttempt,
    TaskStatus,
    WorkflowHookDispatch,
    WorkflowStatus,
)
from qraft.retention import log_retention_policy, sweep_retention

pytestmark = pytest.mark.django_db


def _age(instance, days):
    """Backdate a row past the retention window (auto_now blocks a plain save)."""
    when = timezone.now() - timedelta(days=days)
    type(instance).objects.filter(pk=instance.pk).update(date_updated=when)
    return instance


def _task(status=TaskStatus.SUCCEEDED, days_old=90, **extra):
    task = QraftTask.objects.create(
        func="demo.showcase.tasks.noop_task", status=status, **extra
    )
    QraftTaskAttempt.objects.create(
        qraft_task=task, attempt_number=1, q2_task_id=f"q2-{task.id}", success=True
    )
    return _age(task, days_old)


class TestDisabledByDefault:
    def test_no_op_without_a_retention_window(self):
        _task()

        assert sweep_retention() == {}
        assert QraftTask.objects.count() == 1


class TestTerminalOnly:
    def test_prunes_settled_tasks_and_their_children(self):
        task = _task()
        HookDispatch.objects.create(
            qraft_task=task,
            hook_type="success",
            hook_path="test.hooks.success",
            q2_task_id="q2-hook-old",
        )

        assert sweep_retention(retention_days=30) == {"QraftTask": 1}
        assert QraftTaskAttempt.objects.count() == 0
        assert HookDispatch.objects.count() == 0

    @pytest.mark.parametrize("status", [TaskStatus.PENDING, TaskStatus.RUNNING])
    def test_keeps_live_tasks_however_old(self, status):
        _task(status=status, days_old=900)

        sweep_retention(retention_days=30)

        assert QraftTask.objects.count() == 1

    def test_keeps_recent_terminal_tasks(self):
        _task(days_old=1)

        sweep_retention(retention_days=30)

        assert QraftTask.objects.count() == 1


class TestWorkflowSafety:
    def test_keeps_a_task_whose_iter_is_still_running(self):
        workflow = QraftIterModel.objects.create(
            func="demo.showcase.tasks.noop_task",
            total_count=2,
            status=WorkflowStatus.RUNNING,
        )
        _task(qraft_iter=workflow)

        sweep_retention(retention_days=30)

        assert QraftTask.objects.count() == 1

    def test_prunes_a_settled_iter_with_its_tasks(self):
        workflow = QraftIterModel.objects.create(
            func="demo.showcase.tasks.noop_task",
            total_count=1,
            status=WorkflowStatus.SUCCEEDED,
        )
        _age(workflow, 90)
        _task(qraft_iter=workflow)

        sweep_retention(retention_days=30)

        assert QraftIterModel.objects.count() == 0
        assert QraftTask.objects.count() == 0

    def test_keeps_a_task_whose_chain_is_still_running(self):
        chain = QraftChainModel.objects.create(status=WorkflowStatus.RUNNING)
        task = _task()
        QraftChainStep.objects.create(
            chain=chain,
            step_index=0,
            func="demo.showcase.tasks.noop_task",
            qraft_task=task,
        )

        sweep_retention(retention_days=30)

        assert QraftTask.objects.count() == 1

    def test_prunes_a_settled_batch(self):
        batch = QraftBatchModel.objects.create(
            total_count=1, status=WorkflowStatus.FAILED
        )
        _age(batch, 90)

        sweep_retention(retention_days=30)

        assert QraftBatchModel.objects.count() == 0

    def test_prunes_aged_workflow_hook_dispatches(self):
        dispatch = WorkflowHookDispatch.objects.create(
            workflow_type="iter",
            workflow_id=QraftIterModel.objects.create(
                func="demo.showcase.tasks.noop_task"
            ).id,
            hook_type="success",
            hook_path="test.hooks.success",
            q2_task_id="q2-wf-hook-old",
        )
        WorkflowHookDispatch.objects.filter(pk=dispatch.pk).update(
            date_created=timezone.now() - timedelta(days=90)
        )

        sweep_retention(retention_days=30)

        assert WorkflowHookDispatch.objects.count() == 0


class TestBatching:
    def test_deletes_in_bounded_batches(self):
        for _ in range(5):
            _task()

        assert sweep_retention(retention_days=30, batch_size=2) == {"QraftTask": 5}
        assert QraftTask.objects.count() == 0

    def test_stops_at_the_batch_ceiling(self, monkeypatch, caplog):
        import qraft.retention

        monkeypatch.setattr(qraft.retention, "MAX_BATCHES", 1)
        for _ in range(3):
            _task()

        sweep_retention(retention_days=30, batch_size=1)

        assert QraftTask.objects.count() == 2
        assert "batch ceiling" in caplog.text


class TestCountBased:
    """The count-based bound Qraft inherits from Q_CLUSTER['save_limit']."""

    def _ladder(self):
        """Five settled tasks, oldest first, one clear age apart."""
        return [_task(days_old=days) for days in (50, 40, 30, 20, 10)]

    def test_keeps_only_the_newest_n_settled_tasks(self):
        self._ladder()

        assert sweep_retention(max_tasks=2) == {"QraftTask": 3}
        assert QraftTask.objects.count() == 2

    def test_no_op_while_under_the_cap(self):
        self._ladder()

        assert sweep_retention(max_tasks=10) == {}
        assert QraftTask.objects.count() == 5

    def test_live_tasks_do_not_count_against_the_cap(self):
        """Q2's save_limit bounds saved results, so only settled rows count."""
        for days in (50, 40, 30):
            _task(status=TaskStatus.RUNNING, days_old=days)
        self._ladder()

        sweep_retention(max_tasks=2)

        assert QraftTask.objects.filter(status=TaskStatus.RUNNING).count() == 3
        assert QraftTask.objects.filter(status=TaskStatus.SUCCEEDED).count() == 2

    def test_the_stricter_of_the_two_bounds_wins(self):
        self._ladder()

        # Age alone would drop 2 rows (50d, 40d); the count bound reaches
        # further back, so it decides.
        assert sweep_retention(retention_days=35, max_tasks=2) == {"QraftTask": 3}

    def test_age_wins_when_it_is_the_stricter_bound(self):
        self._ladder()

        assert sweep_retention(retention_days=15, max_tasks=4) == {"QraftTask": 4}

    def test_a_running_workflow_still_protects_its_tasks(self):
        workflow = QraftIterModel.objects.create(
            func="demo.showcase.tasks.noop_task",
            total_count=1,
            status=WorkflowStatus.RUNNING,
        )
        _task(days_old=50, qraft_iter=workflow)
        self._ladder()

        sweep_retention(max_tasks=1)

        assert QraftTask.objects.filter(qraft_iter=workflow).count() == 1


class TestPolicyLogging:
    @pytest.fixture(autouse=True)
    def _fresh_conf(self, settings, caplog):
        import logging

        from qraft.conf import _cached_conf

        caplog.set_level(logging.INFO, logger="qraft")
        settings.QRAFT_CLUSTER = {}
        settings.Q_CLUSTER = {"name": "test", "orm": "default"}
        _cached_conf.cache_clear()
        yield
        _cached_conf.cache_clear()

    def test_reports_that_nothing_is_pruned(self, caplog):
        log_retention_policy()

        assert "disabled" in caplog.text

    def test_names_the_inherited_source(self, settings, caplog):
        from qraft.conf import _cached_conf

        settings.Q_CLUSTER = {**settings.Q_CLUSTER, "save_limit": 1000}
        _cached_conf.cache_clear()

        log_retention_policy()

        assert "newest 1000 task(s)" in caplog.text
        assert "save_limit" in caplog.text

    def test_names_an_explicit_age_window(self, settings, caplog):
        from qraft.conf import _cached_conf

        settings.QRAFT_CLUSTER = {"retention_days": 30}
        _cached_conf.cache_clear()

        log_retention_policy()

        assert "older than 30.0 day(s)" in caplog.text
        assert "save_limit" not in caplog.text


class TestRevivedRows:
    """The delete re-applies the sweep's filter, not just the selected pks."""

    def test_delete_refilters_rows_revived_after_select(self):
        """Regression: a DLQ requeue flipping a task back to PENDING between
        the sweep's select and its delete must not have the row (and its
        fresh retry attempt) cascaded away."""
        from django.db.models import QuerySet

        task = _task()
        original_delete = QuerySet.delete

        def revive_then_delete(qs):
            QraftTask.objects.filter(pk=task.pk).update(status=TaskStatus.PENDING)
            return original_delete(qs)

        with patch.object(QuerySet, "delete", revive_then_delete):
            deleted = sweep_retention(retention_days=30)

        assert deleted == {}
        assert QraftTask.objects.filter(pk=task.pk).exists()


class TestLiveMemberWorkflows:
    """A terminal workflow waits for its live members before pruning."""

    def test_terminal_workflow_with_live_members_is_kept(self):
        iter_model = QraftIterModel.objects.create(
            func="demo.showcase.tasks.noop_task",
            total_count=2,
            status=WorkflowStatus.CANCELLED,
        )
        _age(iter_model, 90)
        live = QraftTask.objects.create(
            func="demo.showcase.tasks.noop_task",
            status=TaskStatus.RUNNING,
            qraft_iter=iter_model,
        )

        assert sweep_retention(retention_days=30) == {}
        assert QraftIterModel.objects.filter(pk=iter_model.pk).exists()
        assert QraftTask.objects.filter(pk=live.pk).exists()

    def test_workflow_prunes_once_members_settle(self):
        iter_model = QraftIterModel.objects.create(
            func="demo.showcase.tasks.noop_task",
            total_count=1,
            status=WorkflowStatus.CANCELLED,
        )
        member = QraftTask.objects.create(
            func="demo.showcase.tasks.noop_task",
            status=TaskStatus.FAILED,
            qraft_iter=iter_model,
        )
        _age(member, 90)
        _age(iter_model, 90)

        deleted = sweep_retention(retention_days=30)

        assert deleted.get("QraftIterModel") == 1
        # Member cascades with its workflow
        assert not QraftTask.objects.filter(pk=member.pk).exists()
