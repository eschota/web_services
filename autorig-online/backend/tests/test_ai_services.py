"""The typed service catalogue and the handoff it drives."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import ai_services  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(ai_services.router)
    return TestClient(app)


class CatalogueShapeTests(unittest.TestCase):
    def test_every_service_the_owner_asked_for_is_declared(self):
        ids = {entry["id"] for entry in ai_services.SERVICES}
        self.assertEqual(ids, {"vision", "text", "image", "video", "3dmodel"})

    def test_every_declared_type_is_a_known_entity_type(self):
        known = {entry["id"] for entry in ai_services.ENTITY_TYPES}
        for entry in ai_services.SERVICES:
            for item in entry["inputs"] + entry["outputs"]:
                self.assertIn(item["type"], known, entry["id"])

    def test_every_service_takes_something_and_produces_something(self):
        for entry in ai_services.SERVICES:
            self.assertTrue(entry["inputs"], entry["id"])
            self.assertTrue(entry["outputs"], entry["id"])

    def test_a_service_is_either_live_or_honestly_planned(self):
        for entry in ai_services.SERVICES:
            self.assertIn(entry["status"], ("live", "planned"), entry["id"])

    def test_each_input_names_the_field_a_caller_sends(self):
        for entry in ai_services.SERVICES:
            for item in entry["inputs"]:
                self.assertTrue(str(item.get("field") or "").strip(), entry["id"])


class HandoffTests(unittest.TestCase):
    def test_text_can_be_handed_to_the_services_that_take_text(self):
        targets = {t["service_id"] for t in ai_services.targets_for(ai_services.TEXT)}
        # A description can become a prompt for a picture, a clip, or more text.
        self.assertIn("image", targets)
        self.assertIn("text", targets)
        self.assertIn("vision", targets)

    def test_a_picture_can_be_handed_to_vision_video_and_3d(self):
        targets = {t["service_id"] for t in ai_services.targets_for(ai_services.IMAGE)}
        self.assertTrue({"vision", "video", "3dmodel"}.issubset(targets))

    def test_a_handoff_target_says_which_field_receives_it(self):
        for target in ai_services.targets_for(ai_services.IMAGE):
            self.assertTrue(target["field"])
            self.assertTrue(target["input_title"])

    def test_a_planned_destination_is_offered_but_flagged(self):
        video = [t for t in ai_services.targets_for(ai_services.IMAGE)
                 if t["service_id"] == "video"]
        self.assertEqual(video[0]["status"], "planned")

    def test_the_chain_the_owner_described_is_possible(self):
        """image → vision → text → image → video, each step by declared types."""
        vision = ai_services.service("vision")
        self.assertIn(ai_services.IMAGE, ai_services.accepted_types(vision))
        self.assertIn(ai_services.TEXT, ai_services.produced_types(vision))

        image = ai_services.service("image")
        self.assertIn(ai_services.TEXT, ai_services.accepted_types(image))
        self.assertIn(ai_services.IMAGE, ai_services.produced_types(image))

        video = ai_services.service("video")
        self.assertIn(ai_services.IMAGE, ai_services.accepted_types(video))
        self.assertIn(ai_services.VIDEO, ai_services.produced_types(video))

    def test_no_type_is_produced_that_nothing_can_receive_or_show(self):
        produced = set()
        for entry in ai_services.SERVICES:
            produced.update(ai_services.produced_types(entry))
        known = {entry["id"] for entry in ai_services.ENTITY_TYPES}
        self.assertTrue(produced.issubset(known))


class CatalogueEndpointTests(unittest.TestCase):
    def test_the_endpoint_serves_types_services_and_handoff(self):
        body = _client().get("/api/ai/services").json()
        self.assertTrue(body["success_bool"])
        self.assertTrue(body["entity_types_array"])
        self.assertTrue(body["services_array"])
        self.assertIn("image", body["handoff_object"])

    def test_each_service_reports_what_it_accepts_and_produces(self):
        body = _client().get("/api/ai/services").json()
        by_id = {entry["id"]: entry for entry in body["services_array"]}
        self.assertEqual(by_id["vision"]["produces_array"], ["text"])
        self.assertIn("image", by_id["vision"]["accepts_array"])
        self.assertEqual(by_id["image"]["produces_array"], ["image"])

    def test_handoff_entries_point_at_real_pages(self):
        body = _client().get("/api/ai/services").json()
        paths = {entry["path"] for entry in body["services_array"]}
        for targets in body["handoff_object"].values():
            for target in targets:
                self.assertIn(target["path"], paths)


if __name__ == "__main__":
    unittest.main()
