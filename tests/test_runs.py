"""Tests for qraft.runs: binding rules, derived settlement, explicit transitions."""

from unittest.mock import patch

import pytest
from django.utils import timezone

from qraft import runs
from qraft.models import (
    QraftBatchModel,
    QraftChainModel,
    QraftTask,
    TaskStatus,
    WorkflowHookDispatch,
    WorkflowStatus,
)
from qraft.models.runs import QraftRun, QraftRunStage, RunStatus, StageStatus, UnitType

pytestmark = pytest.mark.django_db


def make_run(stages=("ingest", "rules"), **kwargs):
    return runs.start(subject=("worksheet", 4117), stages=list(stages), **kwargs)


def make_task(run_id=None, stage=None, status=TaskStatus.RUNNING):
    return QraftTask.objects.create(
        func="app.tasks.ingest",
        status=status,
        run_id=run_id,
        stage=stage,
        subject_type="worksheet",
        subject_id="4117",
    )


def settle(unit, status):
    unit.status = status
    unit.save(update_fields=["status"])
    runs.note_unit_settled(unit)


def make_batch(run_id=None, stage=None, status=WorkflowStatus.RUNNING):
    return QraftBatchModel.objects.create(
        status=status, total_count=1, run_id=run_id, stage=stage
    )


class TestStart:
    def test_declares_stages_and_records_the_subject_and_fingerprint(self):
        run_id = make_run(
            kind="shadow", revision="rules@v7", metadata={"trigger": "upload"}
        )
        run = QraftRun.objects.get(id=run_id)

        assert (run.subject_type, run.subject_id) == ("worksheet", "4117")
        assert (run.status, run.kind, run.revision) == (
            RunStatus.OPEN,
            "shadow",
            "rules@v7",
        )
        assert run.metadata == {"trigger": "upload"}
        assert list(run.stages.values_list("name", "position", "status")) == [
            ("ingest", 0, StageStatus.PENDING),
            ("rules", 1, StageStatus.PENDING),
        ]

    def test_backdated_start_and_previous_run_link(self):
        earlier = timezone.now() - timezone.timedelta(hours=2)
        first = make_run()
        second = make_run(started_at=earlier, previous_run=first)

        run = QraftRun.objects.get(id=second)
        assert run.date_started == earlier
        assert str(run.previous_run_id) == first

    def test_empty_or_duplicate_stage_names_are_refused(self):
        with pytest.raises(runs.RunError):
            runs.start(subject=("worksheet", 1), stages=[])
        with pytest.raises(runs.RunError):
            runs.start(subject=("worksheet", 1), stages=["a", "a"])


class TestBinding:
    def test_binds_once_and_refuses_a_second_unit_or_an_undeclared_stage(self):
        run_id = make_run()
        task = make_task(run_id, "ingest")
        runs.bind(run_id, "ingest", task)

        stage = QraftRunStage.objects.get(run_id=run_id, name="ingest")
        assert (stage.status, stage.unit_type) == (StageStatus.BOUND, UnitType.TASK)
        assert stage.unit_id == task.id and stage.bound_at is not None

        with pytest.raises(runs.RunError, match="already"):
            runs.bind(run_id, "ingest", make_task(run_id, "ingest"))
        with pytest.raises(runs.RunError, match="no stage named"):
            runs.bind(run_id, "nope", make_task(run_id, "nope"))

    def test_terminal_run_refuses_further_binds(self):
        run_id = make_run()
        runs.cancel(run_id)

        with pytest.raises(runs.RunError, match="cancelled"):
            runs.bind(run_id, "ingest", make_task(run_id, "ingest"))
        with pytest.raises(runs.RunError, match="cancelled"):
            runs.member_labels(run_id, "ingest", None, None)

    def test_workflow_binds_as_its_own_unit_type(self):
        run_id = make_run()
        batch = make_batch(run_id, "rules")
        runs.bind(run_id, "rules", batch)

        stage = QraftRunStage.objects.get(run_id=run_id, name="rules")
        assert (stage.unit_type, stage.unit_id) == (UnitType.BATCH, batch.id)


class TestSettlement:
    def test_every_stage_succeeding_settles_the_run_once(
        self, signal_log, django_capture_on_commit_callbacks
    ):
        run_id = make_run()
        ingest = make_task(run_id, "ingest")
        rules = make_task(run_id, "rules")
        runs.bind(run_id, "ingest", ingest)
        runs.bind(run_id, "rules", rules)

        settle(ingest, TaskStatus.SUCCEEDED)
        assert QraftRun.objects.get(id=run_id).status == RunStatus.OPEN

        with django_capture_on_commit_callbacks(execute=True):
            settle(rules, TaskStatus.SUCCEEDED)
        run = QraftRun.objects.get(id=run_id)
        assert run.status == RunStatus.SUCCEEDED
        assert run.settled_at is not None
        assert len(signal_log["run_settled"]) == 1

        # A replayed completion of the same unit settles nothing again.
        with django_capture_on_commit_callbacks(execute=True):
            runs.note_unit_settled(rules)
        assert len(signal_log["run_settled"]) == 1

    def test_a_failed_unit_fails_the_run_immediately(
        self, signal_log, django_capture_on_commit_callbacks
    ):
        run_id = make_run()
        ingest = make_task(run_id, "ingest")
        runs.bind(run_id, "ingest", ingest)
        runs.bind(run_id, "rules", make_task(run_id, "rules"))

        with django_capture_on_commit_callbacks(execute=True):
            settle(ingest, TaskStatus.EXHAUSTED)
        run = QraftRun.objects.get(id=run_id)
        assert run.status == RunStatus.FAILED
        assert (
            QraftRunStage.objects.get(run_id=run_id, name="ingest").status
            == StageStatus.FAILED
        )
        # The still-open stage is left alone; the run is what settled.
        assert (
            QraftRunStage.objects.get(run_id=run_id, name="rules").status
            == StageStatus.BOUND
        )
        assert len(signal_log["run_settled"]) == 1

    def test_a_skipped_stage_still_lets_the_run_succeed(self):
        run_id = make_run()
        ingest = make_task(run_id, "ingest")
        runs.bind(run_id, "ingest", ingest)
        runs.skip(run_id, "rules", reason="nothing undecided")

        settle(ingest, TaskStatus.SUCCEEDED)
        assert QraftRun.objects.get(id=run_id).status == RunStatus.SUCCEEDED

    def test_skip_is_refused_on_a_bound_stage_and_bind_on_a_skipped_one(self):
        run_id = make_run()
        runs.bind(run_id, "ingest", make_task(run_id, "ingest"))
        with pytest.raises(runs.RunError, match="only a stage with no unit"):
            runs.skip(run_id, "ingest")

        runs.skip(run_id, "rules")
        with pytest.raises(runs.RunError, match="already skipped"):
            runs.bind(run_id, "rules", make_task(run_id, "rules"))

    def test_a_workflow_unit_settles_its_stage_but_a_member_never_does(self):
        run_id = make_run(stages=["rules"])
        batch = make_batch(run_id, "rules")
        runs.bind(run_id, "rules", batch)
        member = make_task(run_id, "rules", status=TaskStatus.SUCCEEDED)
        member.qraft_batch = batch
        member.save(update_fields=["qraft_batch"])

        runs.note_unit_settled(member)
        assert QraftRun.objects.get(id=run_id).status == RunStatus.OPEN
        assert (
            QraftRunStage.objects.get(run_id=run_id, name="rules").status
            == StageStatus.BOUND
        )

        settle(batch, WorkflowStatus.SUCCEEDED)
        assert QraftRun.objects.get(id=run_id).status == RunStatus.SUCCEEDED

    def test_a_live_unit_records_nothing(self):
        run_id = make_run(stages=["ingest"])
        ingest = make_task(run_id, "ingest")
        runs.bind(run_id, "ingest", ingest)

        runs.note_unit_settled(ingest)  # still RUNNING
        assert (
            QraftRunStage.objects.get(run_id=run_id, name="ingest").status
            == StageStatus.BOUND
        )


class TestExplicitTransitions:
    def test_cancel_and_abandon_settle_once_and_refuse_a_second_call(
        self, signal_log, django_capture_on_commit_callbacks
    ):
        cancelled = make_run()
        abandoned = make_run()

        with django_capture_on_commit_callbacks(execute=True):
            assert runs.cancel(cancelled) is True
            assert runs.abandon(abandoned, reason="never enqueued") is True
        assert QraftRun.objects.get(id=cancelled).status == RunStatus.CANCELLED
        assert QraftRun.objects.get(id=abandoned).status == RunStatus.ABANDONED
        assert QraftRun.objects.get(id=abandoned).summary["reason"] == "never enqueued"
        assert len(signal_log["run_settled"]) == 2

        with (
            django_capture_on_commit_callbacks(execute=True),
            pytest.raises(runs.RunError, match="already"),
        ):
            runs.cancel(cancelled)
        assert len(signal_log["run_settled"]) == 2

    def test_a_unit_settling_after_a_cancel_records_its_stage_not_the_run(self):
        """
        Edge rule 5: a cancel does not revoke work already in flight, and what
        that work did is recorded on its stage row. Only the run's own status
        is closed to it.
        """
        run_id = make_run()
        ingest = make_task(run_id, "ingest")
        runs.bind(run_id, "ingest", ingest)
        runs.cancel(run_id)

        settle(ingest, TaskStatus.SUCCEEDED)
        run = QraftRun.objects.get(id=run_id)
        assert run.status == RunStatus.CANCELLED
        stage = QraftRunStage.objects.get(run_id=run_id, name="ingest")
        assert (stage.status, stage.settled_at is not None) == (
            StageStatus.SUCCEEDED,
            True,
        )

    def test_unknown_run_raises_rather_than_returning_none(self):
        with pytest.raises(runs.RunError):
            runs.cancel("00000000-0000-0000-0000-000000000000")


class TestSummaryAndHook:
    def test_summary_snapshots_every_stage_and_the_run_duration(self):
        run_id = make_run()
        ingest = make_task(run_id, "ingest")
        runs.bind(run_id, "ingest", ingest)
        runs.skip(run_id, "rules", reason="nothing undecided")
        settle(ingest, TaskStatus.SUCCEEDED)

        summary = QraftRun.objects.get(id=run_id).summary
        assert summary["outcome"] == RunStatus.SUCCEEDED
        assert summary["duration_s"] >= 0
        stages = {stage["name"]: stage for stage in summary["stages"]}
        assert stages["ingest"]["unit_type"] == UnitType.TASK
        assert stages["ingest"]["unit_id"] == str(ingest.id)
        assert stages["ingest"]["outcome"] == StageStatus.SUCCEEDED
        assert stages["rules"]["outcome"] == StageStatus.SKIPPED
        assert stages["rules"]["skip_reason"] == "nothing undecided"

    def test_on_settled_is_dispatched_once_with_a_context(self):
        run_id = make_run(stages=["ingest"], on_settled="app.hooks.ready", kind="shadow")
        ingest = make_task(run_id, "ingest")
        runs.bind(run_id, "ingest", ingest)

        with patch("qraft.dispatchers.q2_async_task", return_value="q2-hook") as queued:
            settle(ingest, TaskStatus.SUCCEEDED)
            runs.note_unit_settled(ingest)  # replay: dispatches nothing more

        dispatches = WorkflowHookDispatch.objects.filter(
            workflow_type="run", workflow_id=run_id, hook_type="settled"
        )
        assert dispatches.count() == 1
        context = queued.call_args.kwargs["context"]
        assert context["run_id"] == run_id
        assert context["outcome"] == RunStatus.SUCCEEDED
        assert context["kind"] == "shadow"
        assert context["stages"] == {"ingest": StageStatus.SUCCEEDED}
        assert context["duration_s"] >= 0


class TestOverdue:
    def test_flags_once_and_only_past_the_threshold(
        self, settings, signal_log, django_capture_on_commit_callbacks
    ):
        settings.QRAFT_RUN_OVERDUE_AFTER = 60
        fresh = make_run()
        stale = make_run(started_at=timezone.now() - timezone.timedelta(minutes=5))

        with django_capture_on_commit_callbacks(execute=True):
            assert runs.flag_overdue() == 1
            assert runs.flag_overdue() == 0
        assert QraftRun.objects.get(id=stale).overdue_flagged_at is not None
        assert QraftRun.objects.get(id=fresh).overdue_flagged_at is None
        assert QraftRun.objects.get(id=stale).status == RunStatus.OPEN
        assert len(signal_log["run_overdue"]) == 1

    def test_unset_threshold_sweeps_nothing(self, settings):
        settings.QRAFT_RUN_OVERDUE_AFTER = None
        make_run(started_at=timezone.now() - timezone.timedelta(days=1))
        assert runs.flag_overdue() == 0


class TestChainResume:
    def test_resume_is_refused_once_the_run_is_terminal(self, _disable_hook_validation):
        from qraft.chain import QraftChain

        run_id = make_run(stages=["ingest"])
        chain = QraftChain()
        chain.append("app.tasks.step_one")
        with patch("qraft.tasks.q2_async_task", return_value="q2-1"):
            chain.run()
        QraftChainModel.objects.filter(id=chain.id).update(
            run_id=run_id, stage="ingest", status=WorkflowStatus.FAILED
        )
        runs.bind(run_id, "ingest", QraftChainModel.objects.get(id=chain.id))
        runs.cancel(run_id)

        chain._model.refresh_from_db()
        with pytest.raises(ValueError, match="run"):
            chain.resume()



class TestReplayPaths:
    """The three ways a settlement can reach a stage more than once."""

    def test_replay_unrouted_settles_the_stage_a_crashed_handler_missed(self):
        from qraft.models import QraftTaskAttempt
        from qraft.reaper import replay_unrouted

        run_id = make_run(stages=["ingest"])
        task = make_task(run_id, "ingest", status=TaskStatus.SUCCEEDED)
        runs.bind(run_id, "ingest", task)
        QraftTaskAttempt.objects.create(
            qraft_task=task,
            attempt_number=1,
            q2_task_id="q2-unrouted",
            success=True,
            routed=False,
            date_completed=timezone.now() - timezone.timedelta(hours=1),
        )

        assert replay_unrouted() == 1
        assert QraftRun.objects.get(id=run_id).status == RunStatus.SUCCEEDED
        # A second sweep finds nothing: `routed` is set and the stage settled.
        assert replay_unrouted() == 0

    def test_duplicate_completion_delivery_settles_the_run_once(self, signal_log):
        from unittest.mock import Mock

        from qraft.hooks import qraft_hook_handler
        from qraft.models import QraftTaskAttempt

        run_id = make_run(stages=["ingest"])
        task = make_task(run_id, "ingest")
        runs.bind(run_id, "ingest", task)
        attempt = QraftTaskAttempt.objects.create(
            qraft_task=task, attempt_number=1, q2_task_id="q2-dupe"
        )
        q2_task = Mock(
            id=attempt.q2_task_id, success=True, result="ok", stopped=timezone.now()
        )

        with patch("qraft.hooks.q2_async_task", return_value="hook-1"):
            qraft_hook_handler(q2_task)
            qraft_hook_handler(q2_task)

        assert QraftRun.objects.get(id=run_id).status == RunStatus.SUCCEEDED
        assert (
            QraftRunStage.objects.get(run_id=run_id, name="ingest").status
            == StageStatus.SUCCEEDED
        )

    def test_a_replayed_unit_re_offers_the_on_settled_hook(self):
        """
        The durable hook is dispatched after the run's settlement commits. A
        crash in that window loses it, so a replayed unit settlement offers it
        again; WorkflowHookDispatch makes the offer a no-op if the first got
        through.
        """
        run_id = make_run(stages=["ingest"], on_settled="app.hooks.ready")
        ingest = make_task(run_id, "ingest")
        runs.bind(run_id, "ingest", ingest)

        # Settle the run without letting the hook dispatch run, which is what
        # a process dying between the commit and the dispatch leaves behind.
        with patch("qraft.runs._dispatch_settled_hook"):
            settle(ingest, TaskStatus.SUCCEEDED)
        assert QraftRun.objects.get(id=run_id).status == RunStatus.SUCCEEDED
        assert not WorkflowHookDispatch.objects.filter(workflow_id=run_id).exists()

        with patch("qraft.dispatchers.q2_async_task", return_value="q2-hook"):
            runs.note_unit_settled(ingest)
            runs.note_unit_settled(ingest)

        assert (
            WorkflowHookDispatch.objects.filter(
                workflow_type="run", workflow_id=run_id, hook_type="settled"
            ).count()
            == 1
        )

    def test_a_replayed_workflow_settlement_records_the_stage_once(self, signal_log):
        from qraft.dispatchers import settle_workflow

        run_id = make_run(stages=["rules"])
        batch = make_batch(run_id, "rules")
        runs.bind(run_id, "rules", batch)

        assert settle_workflow(
            QraftBatchModel, batch.id, WorkflowStatus.SUCCEEDED, "batch"
        )
        # The settlement compare-and-swap refuses the replay, so the stage and
        # the run are never touched a second time.
        assert (
            settle_workflow(QraftBatchModel, batch.id, WorkflowStatus.FAILED, "batch")
            is None
        )
        run = QraftRun.objects.get(id=run_id)
        assert run.status == RunStatus.SUCCEEDED
        assert (
            QraftRunStage.objects.get(run_id=run_id, name="rules").status
            == StageStatus.SUCCEEDED
        )


class TestSettledHookRecovery:
    """
    `skip`, `cancel` and `abandon` settle a run with no attempt behind them, so
    nothing replays their hook the way a unit settlement replays its own. The
    reaper's sweep is what makes the durability D17 promises unconditional.
    """

    def _settled_without_its_hook(self, action):
        run_id = make_run(stages=["ingest"], on_settled="app.hooks.ready")
        with patch("qraft.runs._dispatch_settled_hook"):
            action(run_id)
        QraftRun.objects.filter(id=run_id).update(
            settled_at=timezone.now() - timezone.timedelta(minutes=10)
        )
        assert not WorkflowHookDispatch.objects.filter(workflow_id=run_id).exists()
        return run_id

    def test_a_cancel_whose_hook_never_dispatched_is_replayed_once(self):
        run_id = self._settled_without_its_hook(runs.cancel)

        with patch("qraft.dispatchers.q2_async_task", return_value="q2-hook"):
            assert runs.replay_settled_hooks(grace=60) == 1
            assert runs.replay_settled_hooks(grace=60) == 0

        assert (
            WorkflowHookDispatch.objects.filter(
                workflow_type="run", workflow_id=run_id, hook_type="settled"
            ).count()
            == 1
        )

    def test_it_leaves_open_runs_fresh_settlements_and_hookless_runs_alone(self):
        make_run(stages=["ingest"], on_settled="app.hooks.ready")  # still OPEN
        fresh = make_run(stages=["ingest"], on_settled="app.hooks.ready")
        with patch("qraft.runs._dispatch_settled_hook"):
            runs.cancel(fresh)  # settled just now, inside the grace
        hookless = make_run(stages=["ingest"])
        runs.cancel(hookless)
        QraftRun.objects.filter(id=hookless).update(
            settled_at=timezone.now() - timezone.timedelta(minutes=10)
        )

        with patch("qraft.dispatchers.q2_async_task", return_value="q2-hook"):
            assert runs.replay_settled_hooks(grace=60) == 0

    def test_the_reaper_sweep_runs_it(self):
        from qraft.reaper import reap_orphans

        run_id = self._settled_without_its_hook(
            lambda rid: runs.abandon(rid, reason="never enqueued")
        )
        with patch("qraft.dispatchers.q2_async_task", return_value="q2-hook"):
            reap_orphans(stale_after=60)

        assert WorkflowHookDispatch.objects.filter(workflow_id=run_id).count() == 1


@pytest.mark.django_db(transaction=True)
class TestRejectAtomicity:
    """
    `reject()` settles a chain with no attempt behind it, so nothing replays it.
    Its stage record therefore has to commit with the cancel, not after it —
    which is only observable by asking, at the moment of the record, whether a
    transaction is open. `transaction=True` removes pytest-django's own wrapper
    so the answer means what it says.
    """

    def test_the_stage_record_happens_inside_the_cancel_transaction(
        self, _disable_hook_validation
    ):
        from django.db import connection, transaction

        from qraft.chain import QraftChain
        from qraft.models import WorkflowStatus

        run_id = runs.start(subject=("worksheet", 1), stages=["ingest"])
        chain = QraftChain(run=run_id, stage="ingest")
        chain.append("app.tasks.step_one", requires_approval=True)
        with patch("qraft.tasks.q2_async_task", return_value="q2-1"):
            chain.run()
        assert chain.status == WorkflowStatus.WAITING_APPROVAL

        in_atomic = []
        original = runs.note_unit_settled

        def spy(unit):
            in_atomic.append(connection.in_atomic_block)
            return original(unit)

        runs.note_unit_settled = spy
        try:
            chain.reject(reason="not this time")
        finally:
            runs.note_unit_settled = original

        assert in_atomic == [True]
        assert not connection.in_atomic_block
        assert transaction.get_autocommit()

        stage = QraftRunStage.objects.get(run_id=run_id, name="ingest")
        assert stage.status == StageStatus.CANCELLED
        assert QraftRun.objects.get(id=run_id).status == RunStatus.FAILED


class TestLateSubjectBind:
    """A run may name its subject after the stage that creates it has run."""

    def test_a_run_may_start_without_a_subject(self):
        run_id = runs.start(None, ["ingest", "score"])

        run = QraftRun.objects.get(id=run_id)
        assert run.subject_type is None
        assert run.subject_id is None
        assert run.status == RunStatus.OPEN

    def test_the_bind_updates_the_run_and_every_bound_member(self):
        from qraft.models import QraftIterModel

        run_id = runs.start(None, ["ingest", "score"])
        task = QraftTask.objects.create(
            func="tests.e2e_tasks.succeed",
            task_args=[],
            task_kwargs={},
            status=TaskStatus.RUNNING,
            run_id=run_id,
            stage="ingest",
        )
        chain = QraftChainModel.objects.create(run_id=run_id, stage="score")
        batch = QraftBatchModel.objects.create(run_id=run_id)
        iter_model = QraftIterModel.objects.create(
            func="tests.e2e_tasks.succeed", total_count=1, run_id=run_id
        )

        runs.bind_subject(run_id, ("worksheet", 4117))

        run = QraftRun.objects.get(id=run_id)
        assert (run.subject_type, run.subject_id) == ("worksheet", "4117")
        for row in (task, chain, batch, iter_model):
            row.refresh_from_db()
            assert (row.subject_type, row.subject_id) == ("worksheet", "4117")

    def test_a_second_bind_and_a_settled_run_are_both_refused(self):
        run_id = runs.start(None, ["ingest"])
        runs.bind_subject(run_id, ("worksheet", 1))

        with pytest.raises(runs.RunError, match="declared once"):
            runs.bind_subject(run_id, ("worksheet", 2))

        # A run that named its subject at start() is equally closed to it.
        other = runs.start(("worksheet", 9), ["ingest"])
        with pytest.raises(runs.RunError, match="declared once"):
            runs.bind_subject(other, ("worksheet", 10))

        settled = runs.start(None, ["ingest"])
        runs.cancel(settled)
        with pytest.raises(runs.RunError, match="subject is final"):
            runs.bind_subject(settled, ("worksheet", 3))

    def test_a_member_enqueued_after_the_bind_inherits_the_subject(self):
        run_id = runs.start(None, ["ingest", "score"])
        runs.bind_subject(run_id, ("worksheet", 4117))

        with patch("qraft.tasks.q2_async_task", return_value="q2-after-bind"):
            from qraft.tasks import async_task

            async_task(
                "tests.e2e_tasks.succeed",
                "x",
                qraft_options={"run": run_id, "stage": "score"},
            )

        task = QraftTask.objects.get(stage="score")
        assert (task.subject_type, task.subject_id) == ("worksheet", "4117")

    def test_the_model_method_delegates_and_refreshes(self):
        run = QraftRun.objects.get(id=runs.start(None, ["ingest"]))

        run.bind_subject("worksheet", 4117)

        assert (run.subject_type, run.subject_id) == ("worksheet", "4117")

    def test_an_unbound_run_still_rejects_a_member_naming_its_own_subject(self):
        """
        A subject on a member of a subject-less run is the case bind_subject
        exists for; taking it silently would leave the run unlabelled.
        """
        run_id = runs.start(None, ["ingest"])

        with pytest.raises(runs.RunError, match="a run implies its subject"):
            runs.member_labels(run_id, "ingest", "worksheet", "4117")
