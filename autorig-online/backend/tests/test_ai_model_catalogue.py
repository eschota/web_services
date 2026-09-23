"""The picture catalogue of checkpoints and LoRAs, and choosing one."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import ai_model_catalogue  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

FIXTURE = [
    {"kind": "checkpoint", "family": "ltx", "file": "ltx10eros.safetensors",
     "title": "LTX 10Eros", "base": "LTXV 2.3", "usable": True,
     "services": ["video"], "preview": "/api/ai/model-preview/ltx10eros.jpg"},
    {"kind": "checkpoint", "family": "pony", "file": "cyberrealisticPony.safetensors",
     "title": "CyberRealistic Pony", "base": "Pony", "usable": False,
     "services": [], "unusable_reason": "The image workflow is Flux."},
    # An active FLUX checkpoint: a LoRA is served only when some active
    # checkpoint loads it (ai_model_defaults.compatible).
    {"kind": "checkpoint", "family": "flux", "file": "flux1-schnell.safetensors",
     "title": "FLUX.1 Schnell", "base": "FLUX.1 Schnell", "usable": True,
     "services": ["image"], "preview": ""},
    {"kind": "lora", "family": "flux", "file": "NSFW_master.safetensors",
     "title": "NSFW MASTER", "base": "Flux.1 D", "usable": True,
     "services": ["image"], "preview": "/api/ai/model-preview/NSFW_master.jpg"},
    {"kind": "lora", "family": "ltx", "file": "Pixar_Toon.safetensors",
     "title": "Pixar CGI Toon Style", "base": "LTXV 2.3", "usable": True,
     "services": ["video"], "preview": ""},
]


class CatalogueTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        root = Path(self._dir.name)
        (root / "previews").mkdir()
        (root / "model_catalogue.json").write_text(
            json.dumps(FIXTURE), encoding="utf-8")
        self._old = (ai_model_catalogue.CATALOGUE_DIR,
                     ai_model_catalogue.CATALOGUE_FILE,
                     ai_model_catalogue.PREVIEW_DIR)
        ai_model_catalogue.CATALOGUE_DIR = root
        ai_model_catalogue.CATALOGUE_FILE = root / "model_catalogue.json"
        ai_model_catalogue.PREVIEW_DIR = root / "previews"
        ai_model_catalogue._cache = []
        ai_model_catalogue._cache_at = 0.0
        app = FastAPI()
        app.include_router(ai_model_catalogue.router)
        self.client = TestClient(app)
        self.root = root

    def tearDown(self):
        (ai_model_catalogue.CATALOGUE_DIR,
         ai_model_catalogue.CATALOGUE_FILE,
         ai_model_catalogue.PREVIEW_DIR) = self._old
        ai_model_catalogue._cache = []
        ai_model_catalogue._cache_at = 0.0
        self._dir.cleanup()

    def test_the_whole_catalogue_is_served(self):
        body = self.client.get("/api/ai/model-catalogue").json()
        self.assertEqual(len(body["checkpoints_array"]), 3)
        self.assertEqual(len(body["loras_array"]), 2)

    def test_image_only_sees_flux_loras(self):
        body = self.client.get("/api/ai/model-catalogue?service=image").json()
        self.assertEqual([e["file"] for e in body["loras_array"]],
                         ["NSFW_master.safetensors"])

    def test_video_only_sees_ltx_loras(self):
        body = self.client.get("/api/ai/model-catalogue?service=video").json()
        self.assertEqual([e["file"] for e in body["loras_array"]],
                         ["Pixar_Toon.safetensors"])

    def test_a_model_the_farm_cannot_run_is_listed_with_its_reason(self):
        """Hiding it would look like it was never downloaded."""
        body = self.client.get("/api/ai/model-catalogue?service=image").json()
        pony = next(e for e in body["checkpoints_array"]
                    if e["file"] == "cyberrealisticPony.safetensors")
        self.assertFalse(pony["usable"])
        self.assertIn("Flux", pony["unusable_reason"])

    def test_every_entry_says_whether_it_has_a_picture(self):
        body = self.client.get("/api/ai/model-catalogue").json()
        for entry in body["checkpoints_array"] + body["loras_array"]:
            self.assertIn("preview", entry)

    def test_a_preview_is_served_from_our_own_disk(self):
        (self.root / "previews" / "ltx10eros.jpg").write_bytes(b"\xff\xd8\xff\xd9")
        response = self.client.get("/api/ai/model-preview/ltx10eros.jpg")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["content-type"], "image/jpeg")

    def test_a_preview_name_cannot_walk_out_of_the_directory(self):
        for bad in ("../secret.jpg", "..%2Fsecret.jpg", "sub/dir.jpg", "x.png"):
            response = self.client.get("/api/ai/model-preview/" + bad)
            self.assertIn(response.status_code, (400, 404), bad)

    def test_a_missing_preview_says_so(self):
        self.assertEqual(
            self.client.get("/api/ai/model-preview/nothing.jpg").status_code, 404)

    def test_known_file_finds_a_listed_model(self):
        entry = ai_model_catalogue.known_file("NSFW_master.safetensors", "lora")
        self.assertIsNotNone(entry)
        self.assertEqual(entry["family"], "flux")

    def test_known_file_refuses_anything_not_listed(self):
        """A name here reaches a worker's disk, so only listed ones count."""
        for bad in ("../../etc/passwd", "some_other.safetensors", ""):
            self.assertIsNone(ai_model_catalogue.known_file(bad, "lora"))

    def test_a_lora_is_not_mistaken_for_a_checkpoint(self):
        self.assertIsNone(
            ai_model_catalogue.known_file("NSFW_master.safetensors", "checkpoint"))

    def test_a_missing_catalogue_file_is_an_empty_list_not_a_crash(self):
        ai_model_catalogue.CATALOGUE_FILE = self.root / "gone.json"
        ai_model_catalogue._cache = []
        ai_model_catalogue._cache_at = 0.0
        body = self.client.get("/api/ai/model-catalogue").json()
        self.assertTrue(body["success_bool"])
        self.assertEqual(body["checkpoints_array"], [])


class WorkflowModelChoiceTests(unittest.TestCase):
    """Pointing a workflow's loaders at the chosen files."""

    def setUp(self):
        sys.path.insert(0, str(BACKEND))
        from renderfin import templating
        self.templating = templating

    def test_a_checkpoint_loader_is_repointed(self):
        wf = {"4": {"class_type": "CheckpointLoaderSimple",
                    "inputs": {"ckpt_name": "old.safetensors"}}}
        changed = self.templating.apply_model_choice(wf, checkpoint="new.safetensors")
        self.assertEqual(wf["4"]["inputs"]["ckpt_name"], "new.safetensors")
        self.assertEqual(changed["checkpoint"], ["4"])

    def test_a_flux_unet_loader_is_repointed_too(self):
        wf = {"9": {"class_type": "UNETLoader",
                    "inputs": {"unet_name": "flux1-schnell.safetensors"}}}
        self.templating.apply_model_choice(wf, checkpoint="krea.safetensors")
        self.assertEqual(wf["9"]["inputs"]["unet_name"], "krea.safetensors")

    def test_the_rgthree_power_loader_takes_a_lora_and_a_strength(self):
        wf = {"26": {"class_type": "Power Lora Loader (rgthree)",
                     "inputs": {"lora_1": {"on": False, "lora": "old.safetensors",
                                           "strength": 0.4}}}}
        self.templating.apply_model_choice(wf, lora="new.safetensors", lora_strength=0.8)
        entry = wf["26"]["inputs"]["lora_1"]
        self.assertEqual(entry["lora"], "new.safetensors")
        self.assertEqual(entry["strength"], 0.8)
        self.assertTrue(entry["on"])

    def test_a_baked_lora_is_kept_and_the_chosen_one_is_added(self):
        # H3's template bakes its 4-step turbo LoRA; the user's LoRA must be
        # chained on, not swapped in (that ran H3 without its turbo weights).
        wf = {"model": {"class_type": "UNETLoader", "inputs": {"unet_name": "h3.safetensors"}},
              "turbo": {"class_type": "LoraLoaderModelOnly",
                        "inputs": {"lora_name": "turbo.safetensors", "strength_model": 1.0,
                                   "model": ["model", 0]}},
              "sampler": {"class_type": "KSampler", "inputs": {"model": ["turbo", 0]}}}
        changed = self.templating.apply_model_choice(wf, lora="new.safetensors", lora_strength=0.5)
        self.assertEqual(wf["turbo"]["inputs"]["lora_name"], "turbo.safetensors")
        added = changed["lora"][0]
        self.assertEqual(wf[added]["inputs"], {"lora_name": "new.safetensors",
                                               "strength_model": 0.5, "model": ["model", 0]})
        self.assertEqual(wf["turbo"]["inputs"]["model"], [added, 0])

    def test_asking_for_nothing_changes_nothing(self):
        wf = {"4": {"class_type": "CheckpointLoaderSimple",
                    "inputs": {"ckpt_name": "keep.safetensors"}}}
        changed = self.templating.apply_model_choice(wf)
        self.assertEqual(wf["4"]["inputs"]["ckpt_name"], "keep.safetensors")
        self.assertEqual(changed, {"checkpoint": [], "lora": []})

    def test_a_workflow_without_the_loader_reports_nothing_swapped(self):
        """So a caller can tell "not asked for" from "asked for, not possible"."""
        wf = {"1": {"class_type": "KSampler", "inputs": {"steps": 20}}}
        changed = self.templating.apply_model_choice(wf, checkpoint="new.safetensors")
        self.assertEqual(changed["checkpoint"], [])

    def test_nodes_that_are_not_dictionaries_are_stepped_over(self):
        wf = {"extra_data": "something", "4": {"class_type": "CheckpointLoaderSimple",
                                               "inputs": {"ckpt_name": "old"}}}
        self.templating.apply_model_choice(wf, checkpoint="new")
        self.assertEqual(wf["4"]["inputs"]["ckpt_name"], "new")


if __name__ == "__main__":
    unittest.main()


class LongSideSubstitutionTests(unittest.TestCase):
    """`$long_side` was never substituted, so the HQ template did not parse.

    `gen_animation_hq_by_url.json` pairs it with "scale longer dimension", so
    the value is the longer edge. Left as a bare token the JSON is invalid and
    the render fails before it reaches a card, which is what `quality: hq` on
    /api/video was quietly doing.
    """

    def setUp(self):
        sys.path.insert(0, str(BACKEND))
        from renderfin import templating
        self.templating = templating

    def _render(self, template, **kwargs):
        args = dict(width=1024, height=576, prompt="x", negative_prompt="",
                    image_filename="in.png", output_prefix="out")
        args.update(kwargs)
        return self.templating.render_workflow_text(template, **args)

    def test_the_longer_edge_is_filled_in(self):
        template = ('{"1": {"class_type": "Resize", "inputs": '
                    '{"resize_type.longer_size": $long_side}}}')
        wf = self._render(template)
        self.assertEqual(wf["1"]["inputs"]["resize_type.longer_size"], 1024)

    def test_height_wins_when_it_is_the_longer_one(self):
        template = ('{"1": {"class_type": "Resize", "inputs": '
                    '{"resize_type.longer_size": $long_side}}}')
        wf = self._render(template, width=576, height=1024)
        self.assertEqual(wf["1"]["inputs"]["resize_type.longer_size"], 1024)

    def test_a_template_without_it_is_unaffected(self):
        wf = self._render('{"1": {"class_type": "KSampler", "inputs": {"steps": 20}}}')
        self.assertEqual(wf["1"]["inputs"]["steps"], 20)


class RecommendedSettingsTests(CatalogueTests):
    """Settings taken off each model's own page, and where they can land."""

    # Reuses CatalogueTests' temporary catalogue directory.

    def test_a_recommended_size_has_an_option_to_land_in(self):
        """A value with no matching option is dropped without a word.

        The picker only sets a select to a value the select offers, so a size
        the model page recommends and the control does not list is silently
        ignored — which looks like the recommendation was never read.
        """
        import ai_services
        sizes = {str(o["value"]) for p in ai_services.params_for("image")
                 if p["name"] == "width" for o in p["options"]}
        for common in ("832", "1216", "1248", "1024"):
            self.assertIn(common, sizes)

    def test_width_and_height_offer_the_same_sizes(self):
        import ai_services
        params = {p["name"]: p for p in ai_services.params_for("image")}
        self.assertEqual([o["value"] for o in params["width"]["options"]],
                         [o["value"] for o in params["height"]["options"]])

    def test_an_entry_may_carry_recommended_settings(self):
        """Not every model publishes any; the field is optional by design."""
        entries = [dict(e, recommended={"steps": 25, "strength": 0.8}) for e in FIXTURE[:1]]
        self.assertEqual(entries[0]["recommended"]["steps"], 25)

    def test_the_catalogue_passes_recommendations_through_untouched(self):
        import json as _json
        raw = _json.loads(_json.dumps(FIXTURE))
        raw[0]["recommended"] = {"steps": 9, "cfg": 1.0}
        raw[0]["recommended_from"] = "the author's example images"
        (self.root / "model_catalogue.json").write_text(_json.dumps(raw), encoding="utf-8")
        ai_model_catalogue._cache = []
        ai_model_catalogue._cache_at = 0.0
        body = self.client.get("/api/ai/model-catalogue").json()
        entry = next(e for e in body["checkpoints_array"] if e["file"] == raw[0]["file"])
        self.assertEqual(entry["recommended"]["steps"], 9)
        self.assertIn("example images", entry["recommended_from"])
