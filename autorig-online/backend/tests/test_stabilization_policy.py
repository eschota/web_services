import unittest
import sys
from datetime import datetime, timedelta
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from animal_submission_policy import (
    animal_detection_accepted,
    animal_preset_topology_compatible,
    animal_rejection_code,
    detected_animal_type,
)
from rig_v2_vision_policy import extract_vision_assessment
from youtube_policy import rolling_budget_available, task_is_in_upload_window


class AnimalSubmissionPolicyTests(unittest.TestCase):
    def test_unsupported_is_parsed_as_successful_rejection(self):
        result = extract_vision_assessment(
            '{"animal_type":"unsupported","confidence_float":0.91,"riggable_bool":false,'
            '"body_topology":"vehicle","rejection_code":"vehicle_or_prop"}',
            ["dog", "cat", "humanoid"],
        )
        self.assertTrue(result["success_bool"])
        self.assertFalse(result["riggable_bool"])
        self.assertEqual(result["body_topology"], "vehicle")
        self.assertEqual(result["rejection_code"], "vehicle_or_prop")

    def test_manual_selection_does_not_replace_ai_rejection(self):
        detection = {
            "manual_selection": True,
            "user_selected_bool": True,
            "accepted": True,
            "animal_decision_accepted_bool": False,
            "rejection_code": "preset_mismatch",
        }
        self.assertFalse(animal_detection_accepted(detection))
        self.assertEqual(animal_rejection_code(detection), "preset_mismatch")

    def test_admin_override_is_explicit(self):
        self.assertTrue(animal_detection_accepted({
            "riggable_bool": False,
            "experimental_admin_override_bool": True,
        }))

    def test_original_ai_type_is_available_before_manual_preset_mutation(self):
        self.assertEqual(
            detected_animal_type({"animal_type": "cat", "user_selected_bool": True}),
            "cat",
        )

    def test_larva_cannot_use_a_quadruped_preset(self):
        self.assertFalse(animal_preset_topology_compatible("larva", "cat"))
        self.assertFalse(animal_preset_topology_compatible("multipart robot", "dog"))
        self.assertTrue(animal_preset_topology_compatible("compact quadruped", "dog"))
        self.assertTrue(animal_preset_topology_compatible("", "dog"))


class YoutubeWindowSourceContractTests(unittest.TestCase):
    def test_rolling_budget_is_nine_in_any_24_hours(self):
        now = datetime(2026, 8, 11, 12, 0, 0)
        self.assertTrue(task_is_in_upload_window(now - timedelta(hours=23, minutes=59), now))
        self.assertFalse(task_is_in_upload_window(now - timedelta(hours=24, seconds=1), now))
        self.assertTrue(rolling_budget_available(8, limit=9))
        self.assertFalse(rolling_budget_available(9, limit=9))

    def test_youtube_source_contains_rolling_budget_and_no_quota_retry(self):
        source = (Path(__file__).resolve().parents[1] / "youtube_upload.py").read_text(encoding="utf-8")
        self.assertIn('YOUTUBE_ROLLING_UPLOAD_LIMIT', source)
        self.assertIn('YOUTUBE_UPLOAD_WINDOW_HOURS', source)
        self.assertIn('Task.youtube_upload_error == "video_source_pending"', source)
        self.assertIn('youtube_upload_error="quota_window_expired"', source)
        self.assertNotIn('Task.youtube_upload_error != "video_source_pending"', source)

    def test_slow_disk_maintenance_does_not_block_http_startup(self):
        source = (Path(__file__).resolve().parents[1] / "main.py").read_text(encoding="utf-8")
        self.assertIn(
            "app.state.startup_disk_maintenance = asyncio.create_task(_run_startup_disk_maintenance())",
            source,
        )
        lifespan_source = source[source.index("async def lifespan"):source.index("limiter = Limiter")]
        self.assertNotIn("await _run_startup_disk_maintenance()", lifespan_source)

    def test_restart_route_does_not_force_accept_animal_selection(self):
        source = (Path(__file__).resolve().parents[1] / "main.py").read_text(encoding="utf-8")
        restart_start = source.index("async def api_restart_task")
        restart_end = source.index("@app.get(\"/api/task/{task_id}/purchases\"", restart_start)
        restart_source = source[restart_start:restart_end]
        self.assertIn("animal_preset_override_rejected", restart_source)
        self.assertIn("if not animal_detection_accepted(updated_detection)", restart_source)
        animal_selection_start = restart_source.index("updated_detection = {")
        animal_selection_end = restart_source.index(
            'settings["rig_v2_animal_detection"] = updated_detection',
            animal_selection_start,
        )
        self.assertNotIn('"accepted": True', restart_source[animal_selection_start:animal_selection_end])

    def test_six_hour_healthcheck_tracks_release_acceptance_metrics(self):
        source = (
            Path(__file__).resolve().parents[2]
            / "deploy"
            / "healthcheck"
            / "renderfin_healthcheck.py"
        ).read_text(encoding="utf-8")
        self.assertIn('AUTORIG_HEALTHCHECK_MIN_FREE_GB", "5.49"', source)
        self.assertIn('AUTORIG_HEALTHCHECK_VIDEO_CACHE_WARN_GB", "1.5"', source)
        self.assertIn("AUTORIG_HEALTHCHECK_SERVICES", source)
        self.assertIn("AUTORIG_HEALTHCHECK_RENDERFIN_URL", source)
        self.assertIn("AUTORIG_HEALTHCHECK_FAILED_JOB_ALERT_SECONDS", source)
        self.assertIn("YOUTUBE_ROLLING_LIMIT = 9", source)
        self.assertIn('STABILIZATION_RELEASE_UTC = "2026-08-11 11:18:47"', source)
        self.assertIn("terminal Unity missing-video errors since release (max 24h)", source)

    def test_storage_host_has_intensive_self_expiring_monitor(self):
        storage_host = Path(__file__).resolve().parents[2] / "deploy" / "storage-host"
        service = (storage_host / "autorig-storage-postmigration-monitor.service").read_text(
            encoding="utf-8"
        )
        timer = (storage_host / "autorig-storage-postmigration-monitor.timer").read_text(
            encoding="utf-8"
        )
        self.assertIn("AUTORIG_POSTMIGRATION_DURATION_HOURS=72", service)
        self.assertIn("AUTORIG_POSTMIGRATION_EMAIL_PROBE_HOURS=12", service)
        self.assertIn("SupplementaryGroups=systemd-journal", service)
        self.assertIn("OnUnitActiveSec=10min", timer)


if __name__ == "__main__":
    unittest.main()
