"""Demo controls and polling regressions without booting worker processes."""

import json
from unittest.mock import patch

from django.test import TestCase
from django.utils import timezone

from qraft.models import QraftTask, QraftTaskAttempt
from showcase import runner, views
from showcase.models import ScenarioRun


class DemoControlsTests(TestCase):
    def tearDown(self):
        views._ACTIVE.clear()

    def test_scenarios_are_serialized(self):
        views._ACTIVE.add("wf.graph-resume")
        response = self.client.post("/scenarios/wf.graph-approval/run/")
        self.assertEqual(response.status_code, 409)
        self.assertEqual(views._ACTIVE, {"wf.graph-resume"})

    def test_soak_rejects_non_finite_or_non_object_input(self):
        with patch("showcase.views._clusters") as clusters:
            for body in ("[]", "null", '{"count": Infinity}', '{"min_seconds": NaN}'):
                response = self.client.post(
                    "/soak/start/", body, content_type="application/json"
                )
                self.assertEqual(response.status_code, 400, body)
            clusters.assert_not_called()

    def test_running_worker_stays_visible_after_recent_window(self):
        task = QraftTask.objects.create(func="showcase.tasks.ping")
        QraftTaskAttempt.objects.create(
            qraft_task=task,
            attempt_number=1,
            worker_pid=12345,
            cluster="soak",
            date_started=timezone.now() - timezone.timedelta(minutes=20),
            heartbeat_at=timezone.now(),
        )
        panel = views._workers_panel(timezone.now())
        self.assertTrue(panel)
        self.assertIn("12345", json.dumps(panel))

    def test_skipped_scenario_is_not_reported_as_passed(self):
        result = ScenarioRun(key="dt.context", status=ScenarioRun.SKIPPED)
        self.assertIn("0/1 scenarios passed (1 skipped)", runner.matrix([result]))

    def test_cluster_controls_cannot_interrupt_a_scenario(self):
        views._ACTIVE.add("wf.graph-resume")
        with patch("showcase.views._clusters") as clusters:
            self.assertEqual(
                self.client.post("/clusters/default/stop/").status_code, 409
            )
            self.assertEqual(self.client.post("/reset/").status_code, 409)
            clusters.assert_not_called()
