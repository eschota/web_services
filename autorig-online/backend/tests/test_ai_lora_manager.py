"""The /lora manager: registry, box sync protocol, catalogue merge, Civitai import."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import ai_lora_manager as lm  # noqa: E402
import ai_model_catalogue  # noqa: E402
from fastapi import FastAPI, HTTPException  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

SHA_A = "0d9bd1b873a7863e128b4672e3e245838858f71469a3cec58123c16c06f83bd7"
SHA_B = "e851c1007e0a85a52bbd8379252b002e545c08d737da9a862b363a0fe4b846a9"
CATALOGUE = [
    {"kind": "checkpoint", "family": "pony", "file": "CyberRealisticPony_V18.0_F16.safetensors",
     "base": "Pony", "usable": True, "services": ["image"], "default_for_families": ["pony", "sdxl"]},
    {"kind": "checkpoint", "family": "flux2", "file": "flux-2-klein-4b.safetensors",
     "base": "FLUX.2 klein 4B", "usable": True, "services": ["image"]},
    {"kind": "lora", "family": "flux", "file": "NSFW_master.safetensors", "base": "Flux.1 D",
     "usable": True, "services": ["image"]},
]


def entry(entry_id, file, sha, family="sdxl", base="SDXL 1.0", **extra):
    record = {"id": entry_id, "file": file, "sha256": sha, "size_bytes": 1000,
              "family": family, "base": base, "services": ["image"], "title": file,
              "state": "active", "mirror": {"state": "ready"},
              "source": {"kind": "civitai", "version_id": int(entry_id.split("-")[1])}}
    record.update(extra)
    return record


class Base(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        root = Path(self._dir.name)
        (root / "previews").mkdir()
        (root / "model_catalogue.json").write_text(json.dumps(CATALOGUE), encoding="utf-8")
        self._old = (ai_model_catalogue.CATALOGUE_DIR, ai_model_catalogue.CATALOGUE_FILE,
                     ai_model_catalogue.PREVIEW_DIR, lm.SYNC_KEYS_FILE)
        ai_model_catalogue.CATALOGUE_DIR = root
        ai_model_catalogue.CATALOGUE_FILE = root / "model_catalogue.json"
        ai_model_catalogue.PREVIEW_DIR = root / "previews"
        ai_model_catalogue._cache = []
        lm._catalogue_cache.update(at=0.0, entries=[], dir="")
        keys = root / "keys.json"
        keys.write_text(json.dumps({"f5": "k5", "Raptor": "kr"}), encoding="utf-8")
        lm.SYNC_KEYS_FILE = keys
        self.root = root
        self.env = mock.patch.dict(os.environ, {"AUTORIG_LORA_DIR": ""})
        self.env.start()
        # Renderfin's registry is stubbed: None means "unknown", the old
        # every-box behaviour; tests that need servers set them.
        self.servers = mock.patch.object(lm, "renderfin_servers", return_value=None)
        self.servers_mock = self.servers.start()

    def tearDown(self):
        self.servers.stop()
        self.env.stop()
        (ai_model_catalogue.CATALOGUE_DIR, ai_model_catalogue.CATALOGUE_FILE,
         ai_model_catalogue.PREVIEW_DIR, lm.SYNC_KEYS_FILE) = self._old
        ai_model_catalogue._cache = []
        lm._catalogue_cache.update(at=0.0, entries=[], dir="")
        self._dir.cleanup()

    def registry(self, loras, **extra):
        data = {"version": 1, "loras": loras, "cleanup": {}}
        data.update(extra)
        lm.save_registry(data)

    def report(self, box, files, age=0, items=None):
        path = lm._path("boxes")
        path.mkdir(parents=True, exist_ok=True)
        (path / f"{box}.json").write_text(json.dumps(
            {"box": box, "reported_at": int(time.time()) - age, "files": files,
             "items": items or {}}), encoding="utf-8")
        lm._catalogue_cache["at"] = 0.0


class FamilyAndUrlTests(unittest.TestCase):
    def test_civitai_base_models_map_to_catalogue_families(self):
        cases = {"Pony": "pony", "SDXL 1.0": "sdxl", "Illustrious": "illustrious",
                 "Flux.1 D": "flux", "Flux.2 Klein 4B": "flux2", "Flux.2 Klein 9B": "flux2_9b",
                 "ZImageTurbo": "zimage", "Krea 2": "krea2", "LTXV 2.3": "ltx23",
                 "LTXV 2.5": "ltx25", "Wan Video 2.2 I2V-A14B": "wan22_i2v_a14b",
                 "Wan Video 14B": "wan21_14b", "MiniMax H3": "minimax_h3", "Qwen": "qwen_image", "SD 1.5": "sd15"}
        for base, family in cases.items():
            with self.subTest(base=base):
                self.assertEqual(lm.family_for_base(base), family)

    def test_links(self):
        self.assertEqual(lm.parse_source_url("https://civitai.com/models/122359/detail-tweaker-xl"),
                         {"kind": "civitai", "model_id": 122359})
        self.assertEqual(lm.parse_source_url("https://civitai.com/models/1725313?modelVersionId=1071192"),
                         {"kind": "civitai", "model_id": 1725313, "version_id": 1071192})
        self.assertEqual(lm.parse_source_url("https://civitai.com/api/download/models/135867?type=Model"),
                         {"kind": "civitai", "version_id": 135867})
        self.assertEqual(lm.parse_source_url("135867"), {"kind": "civitai", "version_id": 135867})
        hf = lm.parse_source_url("https://huggingface.co/a/b/blob/main/sub/x%20y.safetensors")
        self.assertEqual((hf["repo"], hf["revision"], hf["path"]), ("a/b", "main", "sub/x y.safetensors"))
        for bad in ("https://example.com/x", "https://civitai.com/images/139610195",
                    "https://huggingface.co/a/b"):
            with self.subTest(bad=bad), self.assertRaises(lm.ResolveError):
                lm.parse_source_url(bad)

    def test_file_names_are_made_safe(self):
        self.assertEqual(lm.safe_file_name("../../evil name!.SafeTensors"), "evil name_.safetensors")

    def test_recommended_strength_from_description(self):
        self.assertEqual(lm._recommended_strength("<p>Use a weight of 0.7 for best results</p>"), 0.7)
        self.assertIsNone(lm._recommended_strength("no numbers here"))


class BoxStateTests(Base):
    def test_states_follow_the_box_reports(self):
        e = entry("civitai-135867", "add-detail-xl.safetensors", SHA_A)
        self.registry([e])
        self.report("f5", [{"name": "add-detail-xl.safetensors", "sha256": SHA_A}])
        self.report("f15", [], items={e["id"]: {"state": "downloading", "bytes": 5}})
        self.report("Raptor", [{"name": "add-detail-xl.safetensors", "sha256": SHA_B}])
        self.report("f12", [], age=3 * 3600)
        states = {box: lm.lora_box_state(e, box, {}, lm.box_report(box))["state"]
                  for box in lm.boxes()}
        self.assertEqual(states, {"f5": "ready", "f15": "downloading", "Raptor": "hash_mismatch",
                                  "f12": "offline", "worker-4090": "no_agent"})

    def test_a_lora_is_usable_once_one_box_holds_it(self):
        e = entry("civitai-135867", "add-detail-xl.safetensors", SHA_A)
        self.registry([e])
        managed = [x for x in ai_model_catalogue.entries() if x.get("managed")]
        self.assertFalse(managed[0]["usable"])
        self.assertIn("waiting", managed[0]["unusable_reason"])
        self.report("Raptor", [{"name": "add-detail-xl.safetensors", "sha256": SHA_A}])
        managed = [x for x in ai_model_catalogue.entries() if x.get("managed")]
        self.assertTrue(managed[0]["usable"])
        self.assertEqual(managed[0]["validated_workers"], ["Raptor"])
        self.assertEqual(managed[0]["triggers"], [])
        self.assertIsNotNone(ai_model_catalogue.known_file("add-detail-xl.safetensors", "lora"))

    def test_the_curated_catalogue_wins_a_name_clash(self):
        self.registry([entry("civitai-1", "NSFW_master.safetensors", SHA_A)])
        self.report("f5", [{"name": "NSFW_master.safetensors", "sha256": SHA_A}])
        clash = [x for x in ai_model_catalogue.entries() if x.get("file") == "NSFW_master.safetensors"]
        self.assertEqual(len(clash), 1)
        self.assertFalse(clash[0].get("managed"))

    def test_removed_loras_leave_the_catalogue(self):
        self.registry([entry("civitai-2", "x.safetensors", SHA_A, state="removed")])
        self.assertFalse([x for x in ai_model_catalogue.entries() if x.get("managed")])


class SyncProtocolTests(Base):
    def setUp(self):
        super().setUp()
        app = FastAPI()
        app.include_router(lm.router)
        self.client = TestClient(app)

    def test_every_box_call_needs_its_own_key(self):
        for headers in ({}, {"X-AutoRig-Box": "f5"}, {"X-AutoRig-Box": "f5", "Authorization": "Bearer kr"},
                        {"X-AutoRig-Box": "f15", "Authorization": "Bearer k5"}):
            with self.subTest(headers=headers):
                self.assertEqual(self.client.get("/api/ai/loras/sync/manifest", headers=headers).status_code, 401)

    def test_manifest_lists_mirrored_loras_with_peers(self):
        self.registry([entry("civitai-135867", "add-detail-xl.safetensors", SHA_A),
                       entry("civitai-3", "pending.safetensors", SHA_B, mirror={"state": "downloading"}),
                       entry("civitai-4", "only-raptor.safetensors", "c" * 64, boxes=["Raptor"]),
                       entry("civitai-5", "gone.safetensors", "d" * 64, state="removed")],
                      cleanup={"f5": [{"file": "old.safetensors", "sha256": ""}]})
        cdn = "https://b2.civitai.com/file/x/add-detail-xl.safetensors?Authorization=file-scoped"
        with mock.patch.object(lm, "_presigned_url", new=mock.AsyncMock(return_value=cdn)):
            body = self.client.get("/api/ai/loras/sync/manifest",
                                   headers={"X-AutoRig-Box": "f5", "Authorization": "Bearer k5"}).json()
        self.assertEqual([i["file"] for i in body["items_array"]], ["add-detail-xl.safetensors"])
        item = body["items_array"][0]
        self.assertTrue(item["url"].endswith("/api/ai/loras/sync/blob/" + SHA_A))
        # LAN first, then the CDN; the VPS mirror (`url`) is the last resort.
        self.assertEqual(item["peers"], ["http://192.168.0.115:18998/loras/add-detail-xl.safetensors", cdn])
        self.assertEqual(body["remove_array"], [{"file": "gone.safetensors", "sha256": "d" * 64}])
        self.assertEqual(body["cleanup_array"][0]["file"], "old.safetensors")

    def test_report_is_stored_and_clears_finished_cleanups(self):
        self.registry([], cleanup={"f5": [{"file": "old.safetensors"}, {"file": "kept.safetensors"}]})
        with mock.patch.object(lm, "_lookup_unknown_hashes", new=mock.AsyncMock()):
            response = self.client.post(
                "/api/ai/loras/sync/report",
                headers={"X-AutoRig-Box": "f5", "Authorization": "Bearer k5"},
                json={"files": [{"name": "kept.safetensors", "size": 3, "sha256": SHA_A.upper()}],
                      "items": {"civitai-1": {"state": "ready"}}, "free_bytes": 10})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(lm.box_report("f5")["files"][0]["sha256"], SHA_A)
        self.assertEqual(lm.load_registry()["cleanup"]["f5"], [{"file": "kept.safetensors"}])

    def test_blobs_are_served_only_for_registered_hashes(self):
        self.registry([entry("civitai-135867", "add-detail-xl.safetensors", SHA_A)])
        blobs = lm._path("blobs")
        blobs.mkdir(parents=True)
        (blobs / SHA_A).write_bytes(b"lora")
        headers = {"X-AutoRig-Box": "f5", "Authorization": "Bearer k5"}
        self.assertEqual(self.client.get(f"/api/ai/loras/sync/blob/{SHA_A}", headers=headers).content, b"lora")
        self.assertEqual(self.client.get(f"/api/ai/loras/sync/blob/{SHA_B}", headers=headers).status_code, 404)
        self.assertEqual(self.client.get(f"/api/ai/loras/sync/blob/{SHA_A}").status_code, 401)


H3_CATALOGUE = CATALOGUE + [
    {"kind": "checkpoint", "family": "minimax_h3", "file": "minimax_h3.safetensors",
     "base": "MiniMax H3", "usable": True, "services": ["video"],
     "workflow": "gen_video_minimax_h3_by_url.json"},
]
SERVERS = [
    {"name": "worker-4090", "status": "online", "workflows": {"gen_video_minimax_h3_by_url.json"}},
    {"name": "f5", "status": "online", "workflows": {"gen_image.json"}},
    {"name": "Raptor", "status": "online", "workflows": {"gen_image.json"}},
]
SHA_H3 = "c" * 64


class FamilyTargetTests(Base):
    def setUp(self):
        super().setUp()
        (self.root / "model_catalogue.json").write_text(json.dumps(H3_CATALOGUE), encoding="utf-8")
        ai_model_catalogue._cache = []
        self.servers_mock.return_value = SERVERS
        self.h3 = entry("civitai-3220766", "VBVR_H3_attn_only.safetensors", SHA_H3,
                        family="minimax_h3", base="MiniMax H3", services=["video"])

    def test_an_h3_lora_goes_only_to_boxes_that_run_h3(self):
        self.registry([self.h3])
        self.assertEqual(lm.target_boxes(self.h3), {"worker-4090"})
        states = {b: lm.lora_box_state(self.h3, b, {}, lm.box_report(b), lm.target_boxes(self.h3))["state"]
                  for b in ("f5", "worker-4090")}
        self.assertEqual(states, {"f5": "not_needed", "worker-4090": "no_agent"})

    def test_an_explicit_box_list_wins(self):
        pinned = dict(self.h3, boxes=["f12"])
        self.assertEqual(lm.target_boxes(pinned), {"f12"})

    def test_unknown_registry_falls_back_to_every_box(self):
        self.servers_mock.return_value = None
        self.assertEqual(lm.target_boxes(self.h3), set(lm.boxes()))

    def test_manifest_skips_boxes_without_the_family(self):
        self.registry([self.h3])
        app = FastAPI(); app.include_router(lm.router)
        with mock.patch.object(lm, "_presigned_url", new=mock.AsyncMock(return_value="")):
            body = TestClient(app).get("/api/ai/loras/sync/manifest",
                                       headers={"X-AutoRig-Box": "f5", "Authorization": "Bearer k5"}).json()
        self.assertEqual(body["items_array"], [])

    def test_dispatch_needs_a_box_that_runs_the_workflow_and_holds_the_lora(self):
        self.registry([self.h3])
        # On f5 (no H3) only: the render could never start.
        self.report("f5", [{"name": "VBVR_H3_attn_only.safetensors", "sha256": SHA_H3}])
        ok, reason, _ = lm.dispatch_check("gen_video_minimax_h3_by_url.json", ["VBVR_H3_attn_only.safetensors"])
        self.assertFalse(ok)
        self.assertIn("Waiting for the render computers to download it", reason)
        self.assertIn("worker-4090", reason)
        self.report("worker-4090", [{"name": "VBVR_H3_attn_only.safetensors", "sha256": SHA_H3}])
        self.assertEqual(lm.dispatch_check("gen_video_minimax_h3_by_url.json",
                                           ["VBVR_H3_attn_only.safetensors"]),
                         (True, "", ["worker-4090"]))
        # LoRAs from the curated catalogue are not tracked per box: no veto.
        self.assertTrue(lm.dispatch_check("gen_image.json", ["NSFW_master.safetensors"])[0])

    def test_catalogue_lists_ready_workers(self):
        self.registry([self.h3])
        self.report("f5", [{"name": "VBVR_H3_attn_only.safetensors", "sha256": SHA_H3}])
        managed = [x for x in ai_model_catalogue.entries() if x.get("managed")][0]
        self.assertEqual(managed["ready_workers"], ["f5"])


class PresignedTests(Base):
    def test_the_api_token_is_never_the_url_handed_out(self):
        import asyncio
        e = entry("civitai-135867", "add-detail-xl.safetensors", SHA_A)

        class Resp:
            is_redirect = True
            headers = {"location": "https://b2.civitai.com/file/x?Authorization=file-scoped"}

        class Client:
            def __init__(self, **kw): pass
            async def __aenter__(self): return self
            async def __aexit__(self, *a): return False
            async def get(self, url, headers=None, timeout=None):
                self.headers = headers
                return Resp()

        lm._presigned.clear()
        with mock.patch.dict(os.environ, {"CIVITAI_API_TOKEN": "secret-token"}),                 mock.patch.object(lm.httpx, "AsyncClient", Client):
            url = asyncio.run(lm._presigned_url(e))
        self.assertEqual(url, "https://b2.civitai.com/file/x?Authorization=file-scoped")
        self.assertNotIn("secret-token", url)
        lm._presigned.clear()
        with mock.patch.dict(os.environ, {"CIVITAI_API_TOKEN": ""}):
            self.assertEqual(asyncio.run(lm._presigned_url(e)), "")


class ProtectedFileTests(Base):
    def test_template_and_curated_loras_are_protected(self):
        protected = lm.protected_files()
        import re
        in_templates = set()
        for path in lm.WORKFLOWS_DIR.glob("*.json"):
            in_templates.update(re.findall(r'"lora_name"\s*:\s*"([^"$]+)"', path.read_text(encoding="utf-8")))
        self.assertTrue(in_templates, "some template should load a fixed LoRA")
        self.assertTrue(in_templates <= protected)
        self.assertIn("NSFW_master.safetensors", protected)


# The ComfyUI graph embedded in civitai.com/images/139610195 (trimmed titles).
REFERENCE_GRAPH = {
    "1": {"inputs": {"width": 1024, "height": 1024, "batch_size": 1}, "class_type": "EmptyLatentImage"},
    "2": {"inputs": {"lora_name": "add-detail-xl.safetensors", "strength_model": 0.6, "strength_clip": 0.6,
                     "model": ["24", 0], "clip": ["24", 1]}, "class_type": "LoraLoader"},
    "3": {"inputs": {"samples": ["7", 0], "vae": ["6", 2]}, "class_type": "VAEDecode"},
    "4": {"inputs": {"filename_prefix": "ComfyUI", "images": ["3", 0]}, "class_type": "SaveImage"},
    "6": {"inputs": {"ckpt_name": "cyberrealisticPony_v180Coreshift.safetensors"}, "class_type": "CheckpointLoaderSimple"},
    "7": {"inputs": {"seed": 728158258896843, "steps": 30, "cfg": 8.0, "sampler_name": "euler",
                     "scheduler": "sgm_uniform", "denoise": 1.0, "model": ["2", 0], "positive": ["28", 0],
                     "negative": ["27", 0], "latent_image": ["1", 0]}, "class_type": "KSampler"},
    "24": {"inputs": {"lora_name": "darth-vader-pxl.safetensors", "strength_model": 0.8, "strength_clip": 0.8,
                      "model": ["6", 0], "clip": ["6", 1]}, "class_type": "LoraLoader"},
    "27": {"inputs": {"text": "score_1, bad anatomy", "clip": ["2", 1]}, "class_type": "CLIPTextEncode"},
    "28": {"inputs": {"text": "score_9, darth vader", "clip": ["2", 1]}, "class_type": "CLIPTextEncode"},
}


class CivitaiImportTests(Base):
    def test_a_comfy_graph_is_read_into_request_fields(self):
        settings = lm.settings_from_comfy(REFERENCE_GRAPH)
        self.assertEqual(settings["notes"], [])
        self.assertEqual((settings["seed"], settings["steps"], settings["cfg"], settings["sampler"],
                          settings["scheduler"], settings["width"], settings["height"]),
                         (728158258896843, 30, 8.0, "euler", "sgm_uniform", 1024, 1024))
        self.assertEqual((settings["prompt"], settings["negative_prompt"]),
                         ("score_9, darth vader", "score_1, bad anatomy"))
        self.assertEqual([(l["name"], l["strength_model"]) for l in settings["loras"]],
                         [("darth-vader-pxl.safetensors", 0.8), ("add-detail-xl.safetensors", 0.6)])
        self.assertEqual(settings["checkpoint_name"], "cyberrealisticPony_v180Coreshift.safetensors")
        self.assertNotIn("clip_skip", settings)

    def test_a1111_metadata(self):
        settings = lm.settings_from_a1111({
            "prompt": "a <lora:x:0.5>", "negativePrompt": "b", "seed": 7, "steps": 25,
            "cfgScale": 6, "sampler": "DPM++ 2M Karras", "Size": "832x1216", "Clip skip": 2,
            "civitaiResources": [{"type": "lora", "weight": 0.6, "modelVersionId": 42}]})
        self.assertEqual((settings["sampler"], settings["scheduler"], settings["width"],
                          settings["height"], settings["clip_skip"]),
                         ("dpmpp_2m", "karras", 832, 1216, 2))
        self.assertEqual(settings["loras"][0]["version_id"], 42)
        self.assertTrue(settings["notes"])


class ApiValidationTests(Base):
    """The render API's use of the stack: unknown names and family mismatches are errors."""

    def setUp(self):
        super().setUp()
        self.registry([entry("civitai-135867", "add-detail-xl.safetensors", SHA_A),
                       entry("civitai-1071192", "darth-vader-pxl.safetensors", SHA_B,
                             family="pony", base="Pony"),
                       entry("civitai-9", "not-yet.safetensors", "e" * 64)])
        self.report("Raptor", [{"name": "add-detail-xl.safetensors", "sha256": SHA_A},
                               {"name": "darth-vader-pxl.safetensors", "sha256": SHA_B}])
        import ai_vision_api
        self.api = ai_vision_api

    def test_prompt_tags_resolve_and_are_cut_out(self):
        clean, stack, override = self.api._lora_stack_request(
            "image", "score_9, darth vader <lora:darth-vader-pxl:0.8> <lora:add-detail-xl:0.6>", None, None)
        self.assertEqual(clean, "score_9, darth vader")
        self.assertEqual([s.as_payload() for s in stack],
                         [{"name": "darth-vader-pxl.safetensors", "strength_model": 0.8, "strength_clip": 0.8},
                          {"name": "add-detail-xl.safetensors", "strength_model": 0.6, "strength_clip": 0.6}])
        self.assertIsNone(override)
        self.assertEqual(self.api._stack_default_checkpoint("image", stack),
                         "CyberRealisticPony_V18.0_F16.safetensors")
        self.api._check_stack_family(stack, "CyberRealisticPony_V18.0_F16.safetensors")

    def test_unknown_and_unready_loras_are_named_errors(self):
        with self.assertRaises(HTTPException) as ctx:
            self.api._lora_stack_request("image", "x <lora:add-detail:1>", None, None)
        self.assertEqual(ctx.exception.detail["error_string"], "unknown_lora")
        with self.assertRaises(HTTPException) as ctx:
            self.api._lora_stack_request("image", "x", "<lora:not-yet:1>", None)
        self.assertEqual(ctx.exception.detail["error_string"], "lora_not_ready")
        with self.assertRaises(HTTPException) as ctx:
            self.api._lora_stack_request("video", "x <lora:add-detail-xl>", None, None)
        self.assertEqual(ctx.exception.detail["error_string"], "lora_wrong_service")

    def test_a_pony_lora_does_not_go_onto_flux2(self):
        _, stack, _ = self.api._lora_stack_request("image", "x <lora:darth-vader-pxl>", None, None)
        with self.assertRaises(HTTPException) as ctx:
            self.api._check_stack_family(stack, "flux-2-klein-4b.safetensors")
        self.assertEqual(ctx.exception.detail["error_string"], "incompatible_model_pair")


if __name__ == "__main__":
    unittest.main()
