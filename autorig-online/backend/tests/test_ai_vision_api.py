"""Public /api/vision and /api/text2text dispatch to the converter farm."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import ai_vision_api  # noqa: E402
from fastapi import FastAPI, HTTPException  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

WORKER = {
    "name": "F1",
    "url": "http://127.0.0.1:15132",
    "token": "test-token",
    "physical_node": "f1-pc",
}


def _app() -> TestClient:
    app = FastAPI()
    app.include_router(ai_vision_api.router)
    return TestClient(app)


class ModelCatalogueTests(unittest.TestCase):
    def test_polled_status_reports_the_worker_model_not_default_bonsai(self):
        worker = {"name": "f13", "physical_node": "f13", "url": "http://example.test", "token": "test"}
        async def fake_status(client, selected, task_id):
            return {"status": "Completed", "mode": "text", "model": "qwen35-9b-uncensored", "answer": "neutral"}
        with mock.patch.object(ai_vision_api, "_worker_by_key", return_value=worker), \
             mock.patch.object(ai_vision_api, "_fetch_status", fake_status):
            response = _app().get("/api/ai/status/f13.test-task")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["model_string"], "qwen35-9b-uncensored")

    def test_an_uncensored_model_is_offered_and_flagged(self):
        body = _app().get("/api/ai/models").json()
        wild = [m for m in body["models_array"] if m.get("uncensored")]
        self.assertTrue(wild, "no uncensored model in the catalogue")
        self.assertNotEqual(wild[0]["id"], body["default_model_string"])

    def test_models_endpoint_lists_a_default(self):
        response = _app().get("/api/ai/models")
        self.assertEqual(response.status_code, 200)
        body = response.json()
        ids = [m["id"] for m in body["models_array"]]
        self.assertIn(body["default_model_string"], ids)

    def test_unknown_model_is_rejected_with_the_available_list(self):
        with self.assertRaises(HTTPException) as caught:
            ai_vision_api._model_entry("gpt-nonexistent")
        detail = caught.exception.detail
        self.assertEqual(detail["error_string"], "unknown_model")
        self.assertIn(ai_vision_api.DEFAULT_MODEL_ID, detail["available_models_array"])

    def test_blank_model_falls_back_to_the_default(self):
        self.assertEqual(
            ai_vision_api._model_entry(None)["id"], ai_vision_api.DEFAULT_MODEL_ID
        )

    def test_get_mirrors_document_both_endpoints(self):
        client = _app()
        for path in ("/api/vision", "/api/text2text"):
            body = client.get(path).json()
            self.assertEqual(body["method_string"], "POST")
            self.assertEqual(body["url_string"], path)


class EffectiveRenderModelTests(unittest.TestCase):
    def test_legacy_motion_video_url_reaches_validation_without_404(self):
        response = _app().post('/api/ai/video', json={})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()['detail']['error_string'], 'image_required')

    def _entries(self, include_family_default=True):
        entries = [
            {"kind": "checkpoint", "family": "flux2",
             "file": "flux-2-klein-4b.safetensors", "usable": True,
             "services": ["image"], "default_for_services": ["image"],
             "workflow": "gen_image_flux2_klein.json",
             "sampling_policy": {"cfg": "workflow_native"}},
            {"kind": "checkpoint", "family": "pony",
             "file": "pony.safetensors", "usable": True,
             "services": ["image"], "default_for_families": ["pony", "sdxl"],
             "control_channels": ["pose", "depth", "canny"],
             "workflow": "gen_image_sdxl.json",
             "sampling_policy": {"cfg": "ksampler"}},
            {"kind": "lora", "family": "zimage",
             "file": "zit-style.safetensors", "usable": True,
             "services": ["image"], "workflow": "gen_image.json"},
        ]
        if include_family_default:
            entries.append(
                {"kind": "checkpoint", "family": "zimage",
                 "file": "z_image_turbo_fp8_e4m3fn.safetensors", "usable": True,
                 "services": ["image"], "default_for_families": ["zimage"],
                 "workflow": "gen_image.json", "recommended": {"steps": 8},
                 "recommended_from": "author reference"})
        return entries

    def _known(self, entries, name, kind):
        return next((entry for entry in entries
                     if entry.get("file") == name and entry.get("kind") == kind), None)

    def test_blank_checkpoint_with_zimage_lora_resolves_concrete_base(self):
        entries = self._entries()
        with mock.patch("ai_model_catalogue.entries", return_value=entries), \
             mock.patch("ai_model_catalogue.known_file",
                        side_effect=lambda name, kind: self._known(entries, name, kind)):
            effective, _ = ai_vision_api._effective_model_settings(
                "image", None, "zit-style.safetensors", {})
        self.assertEqual(effective["checkpoint"], "z_image_turbo_fp8_e4m3fn.safetensors")
        self.assertEqual(effective["lora"], "zit-style.safetensors")
        self.assertEqual(effective["work_flow"], "gen_image.json")
        self.assertEqual(effective["steps"], 8)

    def test_control_without_model_materializes_pony_and_reports_policy(self):
        entries = self._entries()
        with mock.patch("ai_model_catalogue.entries", return_value=entries), \
             mock.patch("ai_model_catalogue.known_file",
                        side_effect=lambda name, kind: self._known(entries, name, kind)):
            response = _app().get("/api/ai/model-settings?service=image&control_channel=pose")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["checkpoint_string"], "pony.safetensors")
        self.assertEqual(response.json()["sampling_policy_object"], {"cfg": "ksampler"})

    def test_legacy_mode_uses_zimage_without_flattening_template_auto_sampling(self):
        entries = self._entries()
        with mock.patch("ai_model_catalogue.entries", return_value=entries), \
             mock.patch("ai_model_catalogue.known_file",
                        side_effect=lambda name, kind: self._known(entries, name, kind)):
            effective, _ = ai_vision_api._effective_model_settings(
                "image", None, None, {}, mode="t_pose", use_default=False)
        self.assertEqual(effective["checkpoint"], "z_image_turbo_fp8_e4m3fn.safetensors")
        self.assertNotIn("steps", effective)
        self.assertNotIn("sampler", effective)
        self.assertNotIn("scheduler", effective)

    def test_legacy_mode_keeps_explicit_sampling_knobs(self):
        entries = self._entries()
        with mock.patch("ai_model_catalogue.entries", return_value=entries), \
             mock.patch("ai_model_catalogue.known_file",
                        side_effect=lambda name, kind: self._known(entries, name, kind)):
            effective, _ = ai_vision_api._effective_model_settings(
                "image", None, None, {"steps": 7, "sampler": "euler"},
                mode="t_pose", use_default=False)
        self.assertEqual((effective["steps"], effective["sampler"]), (7, "euler"))

    def test_legacy_mode_rejects_explicit_flux2(self):
        entries = self._entries()
        with mock.patch("ai_model_catalogue.entries", return_value=entries), \
             mock.patch("ai_model_catalogue.known_file",
                        side_effect=lambda name, kind: self._known(entries, name, kind)):
            with self.assertRaises(HTTPException) as caught:
                ai_vision_api._effective_model_settings(
                    "image", "flux-2-klein-4b.safetensors", None, {}, mode="open_pose")
        self.assertEqual(caught.exception.detail["error_string"], "mode_model_incompatible")

    def test_inpaint_runs_on_the_zimage_family_default(self):
        # FLUX.1 Fill Dev was never installed; the Z-Image Fun ControlNet
        # Union patch carries inpaint now.
        entries = self._entries()
        with mock.patch("ai_model_catalogue.entries", return_value=entries),              mock.patch("ai_model_catalogue.known_file",
                        side_effect=lambda name, kind: self._known(entries, name, kind)):
            effective, _ = ai_vision_api._effective_model_settings(
                "image", None, None, {}, mode="inpaint", use_default=False)
        self.assertEqual(effective["checkpoint"], "z_image_turbo_fp8_e4m3fn.safetensors")
        self.assertEqual(effective["work_flow"], "gen_image.json")

    def test_inpaint_rejects_a_non_zimage_checkpoint(self):
        entries = self._entries()
        with mock.patch("ai_model_catalogue.entries", return_value=entries),              mock.patch("ai_model_catalogue.known_file",
                        side_effect=lambda name, kind: self._known(entries, name, kind)):
            with self.assertRaises(HTTPException) as caught:
                ai_vision_api._effective_model_settings(
                    "image", "pony.safetensors", None, {}, mode="inpaint")
        self.assertEqual(caught.exception.detail["error_string"], "mode_model_incompatible")

    def test_compatible_pair_sampling_error_is_not_misreported_as_family_error(self):
        entries = self._entries()
        with mock.patch("ai_model_catalogue.entries", return_value=entries), \
             mock.patch("ai_model_catalogue.known_file",
                        side_effect=lambda name, kind: self._known(entries, name, kind)), \
             mock.patch("ai_model_defaults.resolve", side_effect=ValueError("unsupported scheduler")):
            with self.assertRaises(HTTPException) as caught:
                ai_vision_api._effective_model_settings(
                    "image", "flux-2-klein-4b.safetensors", None, {})
        self.assertEqual(caught.exception.detail["error_string"], "invalid_sampling_settings")

    def test_modern_ltx_hq_is_rejected_and_standard_preserves_model_workflow(self):
        with self.assertRaises(HTTPException) as caught:
            ai_vision_api._video_quality_workflow("hq", "ltx23")
        self.assertEqual(caught.exception.detail["error_string"], "unsupported_video_quality")
        self.assertEqual(ai_vision_api._video_quality_workflow("standard", "ltx23"), "")
        self.assertEqual(
            ai_vision_api._video_quality_workflow("hq", "ltx"),
            "gen_animation_hq_by_url.json",
        )

    def test_mode_cache_profile_includes_legacy_family_sampling_policy(self):
        entries = self._entries()
        base = next(entry for entry in entries if entry.get("file") == "z_image_turbo_fp8_e4m3fn.safetensors")
        base["sampling_policy"] = {"scheduler": "template_stages"}
        with mock.patch("ai_model_catalogue.entries", return_value=entries), \
             mock.patch("ai_model_catalogue.known_file",
                        side_effect=lambda name, kind: self._known(entries, name, kind)):
            profile = ai_vision_api._render_model_profile(
                "image", None, None, mode="t_pose")
        files = {item["file"]: item for item in profile}
        self.assertIn("z_image_turbo_fp8_e4m3fn.safetensors", files)
        self.assertEqual(
            files["z_image_turbo_fp8_e4m3fn.safetensors"]["sampling_policy"],
            {"scheduler": "template_stages"},
        )

    def test_blank_checkpoint_and_lora_still_uses_modern_service_default(self):
        entries = self._entries()
        with mock.patch("ai_model_catalogue.entries", return_value=entries), \
             mock.patch("ai_model_catalogue.known_file",
                        side_effect=lambda name, kind: self._known(entries, name, kind)):
            effective, _ = ai_vision_api._effective_model_settings(
                "image", None, None, {})
        self.assertEqual(effective["checkpoint"], "flux-2-klein-4b.safetensors")
        self.assertEqual(effective["work_flow"], "gen_image_flux2_klein.json")

    def test_lora_without_declared_family_base_is_actionable_error(self):
        entries = self._entries(include_family_default=False)
        with mock.patch("ai_model_catalogue.entries", return_value=entries), \
             mock.patch("ai_model_catalogue.known_file",
                        side_effect=lambda name, kind: self._known(entries, name, kind)):
            with self.assertRaises(HTTPException) as caught:
                ai_vision_api._effective_model_settings(
                    "image", None, "zit-style.safetensors", {})
        self.assertEqual(
            caught.exception.detail["error_string"],
            "checkpoint_required_for_lora",
        )

    def test_explicit_checkpoint_is_never_replaced_by_family_default(self):
        entries = self._entries()
        entries.append({"kind": "checkpoint", "family": "zimage",
                        "file": "explicit-zit.safetensors", "usable": True,
                        "services": ["image"], "workflow": "gen_image.json"})
        with mock.patch("ai_model_catalogue.entries", return_value=entries), \
             mock.patch("ai_model_catalogue.known_file",
                        side_effect=lambda name, kind: self._known(entries, name, kind)):
            effective, _ = ai_vision_api._effective_model_settings(
                "image", "explicit-zit.safetensors", "zit-style.safetensors", {})
        self.assertEqual(effective["checkpoint"], "explicit-zit.safetensors")


class WorkerFileTests(unittest.TestCase):
    """The node list is shared with Hunyuan; its parking reasons are not."""

    def _write(self, tmp, payload):
        path = Path(tmp) / "workers.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return str(path)

    def test_a_node_parked_for_hunyuan_still_serves_ai(self):
        # f11 is quarantined for a Hunyuan crash; it reads images perfectly well.
        payload = {"workers": [
            {"name": "f11", "physical_node": "f11", "url": "http://127.0.0.1:15533",
             "token": "t", "enabled": False, "canary_approved": False,
             "disabled_reason": "Hunyuan-only quarantine"},
        ]}
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.object(ai_vision_api, "WORKERS_FILE", self._write(tmp, payload)):
                workers = ai_vision_api._load_ai_workers()
        self.assertEqual([w["physical_node"] for w in workers], ["f11"])

    def test_an_ai_specific_opt_out_is_honoured(self):
        payload = {"workers": [
            {"name": "f7", "physical_node": "f7", "url": "http://127.0.0.1:15131",
             "token": "t", "ai_vision_enabled": False},
            {"name": "f1", "physical_node": "f1", "url": "http://127.0.0.1:15132",
             "token": "t"},
        ]}
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.object(ai_vision_api, "WORKERS_FILE", self._write(tmp, payload)):
                workers = ai_vision_api._load_ai_workers()
        self.assertEqual([w["physical_node"] for w in workers], ["f1"])

    def test_entries_without_a_token_or_url_are_skipped(self):
        payload = {"workers": [
            {"name": "no-token", "physical_node": "a", "url": "http://127.0.0.1:1"},
            {"name": "no-url", "physical_node": "b", "token": "t"},
            {"name": "ok", "physical_node": "c", "url": "http://127.0.0.1:2", "token": "t"},
        ]}
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.object(ai_vision_api, "WORKERS_FILE", self._write(tmp, payload)):
                workers = ai_vision_api._load_ai_workers()
        self.assertEqual([w["physical_node"] for w in workers], ["c"])

    def test_one_physical_node_is_listed_once(self):
        payload = {"workers": [
            {"name": "raptor", "physical_node": "ryzen-server", "url": "http://127.0.0.1:15188", "token": "t"},
            {"name": "raptor-alias", "physical_node": "ryzen-server", "url": "http://127.0.0.1:15189", "token": "t"},
        ]}
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.object(ai_vision_api, "WORKERS_FILE", self._write(tmp, payload)):
                workers = ai_vision_api._load_ai_workers()
        self.assertEqual(len(workers), 1)

    def test_a_missing_file_is_not_a_crash(self):
        with mock.patch.object(ai_vision_api, "WORKERS_FILE", "/nope/missing.json"):
            self.assertEqual(ai_vision_api._load_ai_workers(), [])

    def test_the_converter_api_path_is_appended_to_the_origin(self):
        self.assertEqual(
            ai_vision_api._ai_base(WORKER), "http://127.0.0.1:15132/api-converter-glb"
        )


class HunyuanWorkerTests(unittest.TestCase):
    """3D and AI read the same file but must not read the same flag."""

    def _write(self, tmp, payload):
        path = Path(tmp) / "workers.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return str(path)

    def test_a_node_parked_for_hunyuan_is_kept_out_of_3d(self):
        payload = {"workers": [
            {"name": "f11", "physical_node": "f11", "url": "http://127.0.0.1:15533",
             "token": "t", "enabled": False,
             "disabled_reason": "Hunyuan-only quarantine"},
            {"name": "f13", "physical_node": "f13", "url": "http://127.0.0.1:15267",
             "token": "t", "enabled": True},
        ]}
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.object(ai_vision_api, "WORKERS_FILE", self._write(tmp, payload)):
                three_d = [w["physical_node"] for w in ai_vision_api._load_hunyuan_workers()]
                ai = [w["physical_node"] for w in ai_vision_api._load_ai_workers()]
        # The quarantine is about 3D bakes, so AI keeps the node and 3D does not.
        self.assertEqual(three_d, ["f13"])
        self.assertEqual(sorted(ai), ["f11", "f13"])

    def test_an_unapproved_canary_is_kept_out_of_3d(self):
        payload = {"workers": [
            {"name": "f2", "physical_node": "f2", "url": "http://127.0.0.1:15279",
             "token": "t", "enabled": True, "canary_approved": False},
        ]}
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.object(ai_vision_api, "WORKERS_FILE", self._write(tmp, payload)):
                self.assertEqual(ai_vision_api._load_hunyuan_workers(), [])

    def test_no_cleared_node_is_a_service_error_not_a_crash(self):
        with mock.patch.object(ai_vision_api, "_load_hunyuan_workers", return_value=[]):
            response = _app().post("/api/3dmodel", json={"image_url": "https://e.test/a.png"})
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["detail"]["error_string"], "no_3d_node_available")

    def test_3d_requires_a_picture(self):
        response = _app().post("/api/3dmodel", json={})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"]["error_string"], "image_required")

    def test_a_3d_task_id_carries_its_node(self):
        response = _app().get("/api/3dmodel/status/no-node")
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"]["error_string"], "malformed_task_id")


class TaskIdRoutingTests(unittest.TestCase):
    def test_node_key_is_url_safe_and_stable(self):
        self.assertEqual(ai_vision_api._node_key(WORKER), "f1-pc")
        self.assertEqual(
            ai_vision_api._node_key({"physical_node": "Raptor / RYZEN 01"}),
            "raptor---ryzen-01",
        )

    def test_status_rejects_a_task_id_without_a_node(self):
        response = _app().get("/api/ai/status/no-node-here")
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"]["error_string"], "malformed_task_id")

    def test_status_reports_an_unconfigured_node(self):
        with mock.patch.object(ai_vision_api, "_load_ai_workers", return_value=[WORKER]):
            response = _app().get("/api/ai/status/ghost-node.abc")
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["detail"]["error_string"], "unknown_node")


class DispatchTests(unittest.TestCase):
    def setUp(self):
        # These cases exercise fresh dispatch under different worker mocks.
        # Persistent cache behavior is covered by test_ai_request_cache.py.
        import ai_request_cache
        root = BACKEND.parents[1] / ".codex_tmp"
        root.mkdir(parents=True, exist_ok=True)
        folder = tempfile.TemporaryDirectory(prefix="vision-dispatch-", dir=root)
        self.addCleanup(folder.cleanup)
        patcher = mock.patch.object(ai_request_cache, "_default_cache",
            ai_request_cache.AIRequestCache(Path(folder.name) / "cache.sqlite3"))
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_vision_requires_an_image(self):
        response = _app().post("/api/vision", json={"prompt": "what is this"})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"]["error_string"], "image_required")

    def test_empty_prompt_is_rejected(self):
        response = _app().post("/api/text2text", json={"prompt": "   "})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"]["error_string"], "prompt_required")

    def test_no_configured_worker_is_a_service_error(self):
        with mock.patch.object(ai_vision_api, "_load_ai_workers", return_value=[]):
            response = _app().post("/api/text2text", json={"prompt": "hi"})
        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            response.json()["detail"]["error_string"], "no_workers_configured"
        )

    def test_a_submitted_task_id_carries_its_node(self):
        async def fake_pick(client, model_id=None, **kwargs):
            return WORKER, {"models": [model_id or ""], "load": 0}

        async def fake_submit(client, worker, path, payload):
            self.assertEqual(path, "/text2text")
            self.assertEqual(payload["prompt"], "hi")
            return "abc-123"

        with mock.patch.object(ai_vision_api, "_pick_worker", fake_pick), \
             mock.patch.object(ai_vision_api, "_submit", fake_submit):
            response = _app().post("/api/text2text", json={"prompt": "hi"})
        body = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(body["task_id_string"], "f1-pc.abc-123")
        self.assertEqual(body["status_url_string"], "/api/ai/status/f1-pc.abc-123")
        self.assertFalse(body["finished_bool"])
        # The accept response says what the node serves, not merely what was asked.
        self.assertEqual(body["served_model_string"], ai_vision_api.DEFAULT_MODEL_ID)

    def test_wait_returns_the_finished_answer(self):
        async def fake_pick(client, model_id=None, **kwargs):
            return WORKER, {"models": [model_id or ""], "load": 0}

        async def fake_submit(client, worker, path, payload):
            return "abc-123"

        async def fake_status(client, worker, worker_task_id):
            return {
                "status": "Completed",
                "answer": "a black lamp",
                "reasoning": "thinking",
                "elapsed_seconds": 6.2,
                "mode": "text",
            }

        with mock.patch.object(ai_vision_api, "_pick_worker", fake_pick), \
             mock.patch.object(ai_vision_api, "_submit", fake_submit), \
             mock.patch.object(ai_vision_api, "_fetch_status", fake_status):
            response = _app().post(
                "/api/text2text", json={"prompt": "hi", "wait_seconds": 10}
            )
        body = response.json()
        self.assertTrue(body["finished_bool"])
        self.assertEqual(body["answer_string"], "a black lamp")
        self.assertEqual(body["elapsed_seconds_float"], 6.2)

    def test_a_failed_task_is_reported_as_unsuccessful(self):
        async def fake_pick(client, model_id=None, **kwargs):
            return WORKER, {"models": [model_id or ""], "load": 0}

        async def fake_submit(client, worker, path, payload):
            return "abc-123"

        async def fake_status(client, worker, worker_task_id):
            return {"status": "Failed", "error": "model_returned_empty_answer"}

        with mock.patch.object(ai_vision_api, "_pick_worker", fake_pick), \
             mock.patch.object(ai_vision_api, "_submit", fake_submit), \
             mock.patch.object(ai_vision_api, "_fetch_status", fake_status):
            body = _app().post(
                "/api/text2text", json={"prompt": "hi", "wait_seconds": 10}
            ).json()
        self.assertFalse(body["success_bool"])
        self.assertEqual(body["error_string"], "model_returned_empty_answer")


class InlineImageTests(unittest.TestCase):
    def test_data_url_prefix_is_stripped(self):
        data = ai_vision_api._decode_inline_image("data:image/png;base64,aGk=")
        self.assertEqual(data, b"hi")

    def test_non_base64_is_rejected(self):
        with self.assertRaises(HTTPException) as caught:
            ai_vision_api._decode_inline_image("not base64 at all!!")
        self.assertEqual(caught.exception.detail["error_string"], "image_not_base64")

    def test_oversized_inline_image_is_rejected(self):
        import base64

        payload = base64.b64encode(b"x" * (ai_vision_api.MAX_INLINE_IMAGE_BYTES + 1))
        with self.assertRaises(HTTPException) as caught:
            ai_vision_api._decode_inline_image(payload.decode())
        self.assertEqual(caught.exception.status_code, 413)


class WorkerChoiceTests(unittest.TestCase):
    def _pick(self, workers, probe, model=None):
        import asyncio

        async def run():
            with mock.patch.object(ai_vision_api, "_load_ai_workers", return_value=workers), \
                 mock.patch.object(ai_vision_api, "_node_is_free", probe):
                return await ai_vision_api._pick_worker(None, model)

        return asyncio.run(run())

    def test_the_least_loaded_reachable_node_wins(self):
        busy = dict(WORKER, physical_node="busy", name="busy")
        idle = dict(WORKER, physical_node="idle", name="idle")

        async def probe(client, worker):
            load = 5 if worker["physical_node"] == "busy" else 0
            return True, {"load": load, "models": ["bonsai2-27b"], "loaded": ""}

        self.assertEqual(self._pick([busy, idle], probe)["physical_node"], "idle")

    def test_all_nodes_unreachable_is_a_retryable_503(self):
        async def probe(client, worker):
            return False, {}

        with self.assertRaises(HTTPException) as caught:
            self._pick([WORKER], probe)
        self.assertEqual(caught.exception.status_code, 503)
        self.assertEqual(caught.exception.detail["error_string"], "no_node_available")

    def test_only_a_node_carrying_the_model_is_chosen(self):
        plain = dict(WORKER, physical_node="plain", name="plain")
        wild = dict(WORKER, physical_node="wild", name="wild")

        async def probe(client, worker):
            models = (["bonsai2-27b"] if worker["physical_node"] == "plain"
                      else ["bonsai2-27b", "qwen35-9b-uncensored"])
            return True, {"load": 0, "models": models, "loaded": ""}

        chosen = self._pick([plain, wild], probe, model="qwen35-9b-uncensored")
        self.assertEqual(chosen["physical_node"], "wild")

    def test_a_model_no_node_carries_is_reported(self):
        async def probe(client, worker):
            return True, {"load": 0, "models": ["bonsai2-27b"], "loaded": ""}

        with self.assertRaises(HTTPException) as caught:
            self._pick([WORKER], probe, model="qwen35-9b-uncensored")
        self.assertEqual(caught.exception.status_code, 503)
        self.assertEqual(caught.exception.detail["error_string"], "model_unavailable")
        self.assertEqual(caught.exception.detail["served_models_array"], ["bonsai2-27b"])

    def test_a_busy_node_that_serves_the_model_beats_a_free_node_that_does_not(self):
        """Queue on the right model rather than answer fast with the wrong one."""
        busy = dict(WORKER, physical_node="busy", name="busy")
        free = dict(WORKER, physical_node="free", name="free")

        async def probe(client, worker):
            if worker["physical_node"] == "busy":
                return True, {"load": 9, "models": ["qwen35-9b-uncensored"], "loaded": ""}
            return True, {"load": 0, "models": ["bonsai2-27b"], "loaded": "bonsai2-27b"}

        chosen = self._pick([busy, free], probe, model="qwen35-9b-uncensored")
        self.assertEqual(chosen["physical_node"], "busy")

    def test_a_node_without_a_catalogue_is_not_guessed_at_for_a_named_model(self):
        """f2 answering Bonsai to a Qwen request is the defect this closes."""
        async def probe(client, worker):
            return True, {"load": 0, "models": [], "loaded": ""}

        with self.assertRaises(HTTPException) as caught:
            self._pick([WORKER], probe, model="qwen35-9b-uncensored")
        self.assertEqual(caught.exception.status_code, 503)
        self.assertEqual(caught.exception.detail["error_string"], "model_unavailable")
        self.assertEqual(caught.exception.detail["served_models_array"], [])

    def test_a_node_without_a_catalogue_still_serves_the_farm_default(self):
        async def probe(client, worker):
            return True, {"load": 0, "models": [], "loaded": ""}

        chosen = self._pick([WORKER], probe, model=ai_vision_api.DEFAULT_MODEL_ID)
        self.assertEqual(chosen["physical_node"], "f1-pc")
        self.assertEqual(self._pick([WORKER], probe)["physical_node"], "f1-pc")

    def test_the_chosen_node_can_report_what_it_serves(self):
        import asyncio

        async def probe(client, worker):
            return True, {"load": 0, "models": ["qwen35-9b-uncensored"], "loaded": ""}

        async def run():
            with mock.patch.object(ai_vision_api, "_load_ai_workers", return_value=[WORKER]),                  mock.patch.object(ai_vision_api, "_node_is_free", probe):
                return await ai_vision_api._pick_worker(
                    None, "qwen35-9b-uncensored", with_info=True)

        worker, info = asyncio.run(run())
        self.assertEqual(worker["physical_node"], "f1-pc")
        self.assertIn("qwen35-9b-uncensored", info["models"])

    def test_a_free_node_beats_a_warm_but_busy_one(self):
        """A weight swap costs seconds; a queue behind a conversion costs minutes."""
        warm = dict(WORKER, physical_node="warm", name="warm")
        idle = dict(WORKER, physical_node="idle", name="idle")

        async def probe(client, worker):
            if worker["physical_node"] == "warm":
                return True, {"load": 2, "models": ["m"], "loaded": "m"}
            return True, {"load": 0, "models": ["m"], "loaded": ""}

        self.assertEqual(self._pick([warm, idle], probe, model="m")["physical_node"], "idle")

    def test_warmth_breaks_a_tie_between_equally_free_nodes(self):
        warm = dict(WORKER, physical_node="warm", name="warm")
        cold = dict(WORKER, physical_node="cold", name="cold")

        async def probe(client, worker):
            loaded = "m" if worker["physical_node"] == "warm" else ""
            return True, {"load": 0, "models": ["m"], "loaded": loaded}

        self.assertEqual(self._pick([cold, warm], probe, model="m")["physical_node"], "warm")

    def test_a_node_that_publishes_no_catalogue_is_tried_for_the_default_only(self):
        """It used to be tried for anything, and answered Bonsai to Qwen."""
        async def probe(client, worker):
            return True, {"load": 0, "models": [], "loaded": ""}

        chosen = self._pick([WORKER], probe, model=ai_vision_api.DEFAULT_MODEL_ID)
        self.assertEqual(chosen["physical_node"], "f1-pc")
        with self.assertRaises(HTTPException):
            self._pick([WORKER], probe, model="qwen35-9b-uncensored")


if __name__ == "__main__":
    unittest.main()
