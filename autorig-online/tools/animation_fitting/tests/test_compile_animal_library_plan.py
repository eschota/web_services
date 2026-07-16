from __future__ import annotations

import builtins
import errno
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


TOOLS_ROOT = Path(__file__).resolve().parents[2]
if str(TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(TOOLS_ROOT))

from animation_fitting.compile_animal_library_plan import (  # noqa: E402
    EXPECTED_ACTION_IDS,
    PlanCompileError,
    compile_animal_library_plan,
)


BACKEND_ROOT = Path(__file__).resolve().parents[3] / "backend"
TAXONOMY = BACKEND_ROOT / "animal_animation_taxonomy.v1.json"
PROMPTS = BACKEND_ROOT / "animation_fitting" / "specs" / "action_prompts.v1.json"
WORKFLOWS = BACKEND_ROOT / "animation_fitting" / "specs" / "workflows"
LOOP_WORKFLOW = WORKFLOWS / "autorig_ltx2_animal_loop_v1_api.json"
ONE_SHOT_WORKFLOW = WORKFLOWS / "autorig_ltx2_animal_one_shot_v1_api.json"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _canonical_fingerprint(path: Path) -> str:
    value = json.loads(path.read_text(encoding="utf-8-sig"))
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class CompileAnimalLibraryPlanTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="autorig-library-plan-")
        self.root = Path(self._tmp.name)
        self.skeleton = self.root / "skeleton.json"
        self.bundle = self.root / "fitting_bundle.json"
        self.immutable_manifest = self.root / "immutable_manifest.json"
        self.skeleton.write_text(
            json.dumps(
                {
                    "armatures": [
                        {
                            "name": "Horse_2",
                            "bones": [
                                {"name": "root", "parent": None},
                                {"name": "spine", "parent": "root"},
                            ],
                        }
                    ]
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        skeleton_sha = _sha256(self.skeleton)
        self.model_sha = hashlib.sha256(b"externally-pinned-Horse_2.blend").hexdigest()
        self.bundle.write_text(
            json.dumps(
                {
                    "schema": "autorig-actionless-fitting-bundle.v1",
                    "revision": "autorig_actionless_bundle_v1",
                    "source": {
                        "filename": "Horse_2.blend",
                        "sha256": self.model_sha,
                        "species": "horse",
                        "rig_type": "HORSE_2",
                        "orientation": "canonical",
                    },
                    "actionless": {"actionless": True},
                    "artifacts": {
                        "skeleton": {
                            "filename": self.skeleton.name,
                            "sha256": skeleton_sha,
                            "bytes": self.skeleton.stat().st_size,
                        }
                    },
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        bundle_sha = _sha256(self.bundle)
        bundle_bytes = self.bundle.stat().st_size
        skeleton_bytes = self.skeleton.stat().st_size
        self.immutable_manifest.write_text(
            json.dumps(
                {
                    "schema": "autorig-fitting-immutable-copy.v1",
                    "source_model": {
                        "filename": "Horse_2.blend",
                        "sha256": self.model_sha,
                        "copied": False,
                    },
                    "bundle_manifest": {
                        "filename": self.bundle.name,
                        "sha256": bundle_sha,
                    },
                    "bundle_file_count": 2,
                    "bundle_total_bytes": bundle_bytes + skeleton_bytes,
                    "files": [
                        {
                            "filename": self.bundle.name,
                            "sha256": bundle_sha,
                            "bytes": bundle_bytes,
                        },
                        {
                            "filename": self.skeleton.name,
                            "sha256": skeleton_sha,
                            "bytes": skeleton_bytes,
                        },
                    ],
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _arguments(self, output_name: str = "horse-plan.json") -> dict:
        return {
            "rig_type": "horse",
            "species": "horse",
            "library_revision": "horse-base-30-v1",
            "taxonomy_path": TAXONOMY,
            "taxonomy_sha256": _sha256(TAXONOMY),
            "prompts_path": PROMPTS,
            "prompts_sha256": _sha256(PROMPTS),
            "loop_workflow_path": LOOP_WORKFLOW,
            "loop_workflow_sha256": _sha256(LOOP_WORKFLOW),
            "one_shot_workflow_path": ONE_SHOT_WORKFLOW,
            "one_shot_workflow_sha256": _sha256(ONE_SHOT_WORKFLOW),
            "immutable_manifest_path": self.immutable_manifest,
            "immutable_manifest_sha256": _sha256(self.immutable_manifest),
            "fitting_bundle_path": self.bundle,
            "fitting_bundle_sha256": _sha256(self.bundle),
            "skeleton_path": self.skeleton,
            "skeleton_sha256": _sha256(self.skeleton),
            "source_model_sha256": self.model_sha,
            "output_path": self.root / output_name,
        }

    def test_real_specs_compile_deterministically_with_one_read_per_input(self) -> None:
        args = self._arguments("plan-a.json")
        source_paths = {
            Path(args[key]).resolve()
            for key in (
                "taxonomy_path",
                "prompts_path",
                "loop_workflow_path",
                "one_shot_workflow_path",
                "immutable_manifest_path",
                "fitting_bundle_path",
                "skeleton_path",
            )
        }
        counts = {path: 0 for path in source_paths}
        original_open = builtins.open

        def counting_open(file, *open_args, **open_kwargs):
            path = Path(file).resolve()
            if path in counts:
                counts[path] += 1
            return original_open(file, *open_args, **open_kwargs)

        with patch("builtins.open", side_effect=counting_open):
            result_a = compile_animal_library_plan(**args)
        self.assertEqual(set(counts.values()), {1})
        plan_a_bytes = result_a.output_path.read_bytes()
        plan = json.loads(plan_a_bytes)
        self.assertTrue(plan["dry_run"])
        self.assertFalse(plan["side_effects_authorized"])
        self.assertEqual(plan["job_count"], 30)
        self.assertEqual(plan["candidate_count_per_action"], 8)
        self.assertEqual(plan["candidate_count_total"], 240)
        self.assertEqual(tuple(row["semantic_id"] for row in plan["jobs"]), EXPECTED_ACTION_IDS)
        self.assertEqual([row["order"] for row in plan["jobs"]], list(range(1, 31)))
        self.assertEqual(
            plan["workflow_contracts"]["loop"]["workflow_fingerprint_sha256"],
            _canonical_fingerprint(LOOP_WORKFLOW),
        )
        self.assertEqual(
            plan["workflow_contracts"]["one_shot"]["workflow_fingerprint_sha256"],
            _canonical_fingerprint(ONE_SHOT_WORKFLOW),
        )
        expected_wave_one = [
            "idle_neutral",
            "walk_forward",
            "trot_jog",
            "run",
            "jump_full",
            "attack_primary",
            "death",
        ]
        self.assertEqual(plan["priority_contract"]["wave_1"], expected_wave_one)
        self.assertEqual(
            [job["semantic_id"] for job in plan["jobs"] if job["priority_wave"] == 1],
            expected_wave_one,
        )
        all_seeds = []
        for job in plan["jobs"]:
            self.assertEqual((job["frame_count"] - 1) % 8, 0)
            self.assertNotIn("{{species}}", job["positive_prompt"])
            self.assertIn("horse", job["positive_prompt"].lower())
            self.assertEqual(len(job["candidates"]), 8)
            self.assertEqual(
                [row["candidate_index"] for row in job["candidates"]], list(range(8))
            )
            seeds = [row["seed"] for row in job["candidates"]]
            self.assertEqual(len(set(seeds)), 8)
            all_seeds.extend(seeds)
            expected_mode = "loop" if job["loop"] else "one_shot"
            self.assertEqual(job["generation_mode"], expected_mode)
            self.assertEqual(job["workflow_name"], plan["workflow_contracts"][expected_mode]["workflow_name"])
        self.assertEqual(len(set(all_seeds)), 240)
        self.assertEqual(result_a.output_sha256, hashlib.sha256(plan_a_bytes).hexdigest())

        args_b = self._arguments("plan-b.json")
        result_b = compile_animal_library_plan(**args_b)
        self.assertEqual(result_a.plan_identity_sha256, result_b.plan_identity_sha256)
        self.assertEqual(result_a.output_sha256, result_b.output_sha256)
        self.assertEqual(plan_a_bytes, result_b.output_path.read_bytes())

    def test_existing_output_is_never_overwritten(self) -> None:
        args = self._arguments("existing.json")
        output = Path(args["output_path"])
        output.write_bytes(b"owner-data")
        with self.assertRaisesRegex(PlanCompileError, "Refusing to overwrite"):
            compile_animal_library_plan(**args)
        self.assertEqual(output.read_bytes(), b"owner-data")
        self.assertEqual(list(self.root.glob(f".{output.name}.*.tmp")), [])

        race_args = self._arguments("race.json")
        race_output = Path(race_args["output_path"])

        def competing_link(_source, destination):
            Path(destination).write_bytes(b"competing-owner-data")
            raise FileExistsError(errno.EEXIST, "exists")

        with patch(
            "animation_fitting.compile_animal_library_plan.os.link",
            side_effect=competing_link,
        ):
            with self.assertRaisesRegex(PlanCompileError, "Refusing to overwrite"):
                compile_animal_library_plan(**race_args)
        self.assertEqual(race_output.read_bytes(), b"competing-owner-data")
        self.assertEqual(list(self.root.glob(f".{race_output.name}.*.tmp")), [])

    def test_byte_mutation_fails_the_external_pin_without_output(self) -> None:
        prompts_copy = self.root / "prompts-mutated.json"
        original = PROMPTS.read_bytes()
        prompts_copy.write_bytes(original)
        pinned = hashlib.sha256(original).hexdigest()
        prompts_copy.write_bytes(original + b"\n")
        args = self._arguments("pin-failure.json")
        args["prompts_path"] = prompts_copy
        args["prompts_sha256"] = pinned
        with self.assertRaisesRegex(PlanCompileError, "prompts SHA-256 mismatch"):
            compile_animal_library_plan(**args)
        self.assertFalse(Path(args["output_path"]).exists())

    def test_semantic_prompt_workflow_and_bundle_drift_fail_closed(self) -> None:
        prompt_value = json.loads(PROMPTS.read_text(encoding="utf-8-sig"))
        prompt_value["actions_array"][0], prompt_value["actions_array"][1] = (
            prompt_value["actions_array"][1],
            prompt_value["actions_array"][0],
        )
        prompt_drift = self.root / "prompt-drift.json"
        prompt_drift.write_text(json.dumps(prompt_value), encoding="utf-8")
        prompt_args = self._arguments("prompt-drift-output.json")
        prompt_args.update(prompts_path=prompt_drift, prompts_sha256=_sha256(prompt_drift))
        with self.assertRaisesRegex(PlanCompileError, "Prompt action order"):
            compile_animal_library_plan(**prompt_args)

        taxonomy_value = json.loads(TAXONOMY.read_text(encoding="utf-8-sig"))
        stop_index = next(
            index for index, row in enumerate(taxonomy_value["clips"]) if row["id"] == "stop_brake"
        )
        taxonomy_value["clips"][stop_index]["loop"] = True
        prompt_value = json.loads(PROMPTS.read_text(encoding="utf-8-sig"))
        prompt_value["actions_array"][stop_index]["generation_mode_string"] = "loop"
        taxonomy_drift = self.root / "taxonomy-stop-brake-loop-drift.json"
        prompt_mode_drift = self.root / "prompt-stop-brake-loop-drift.json"
        taxonomy_drift.write_text(json.dumps(taxonomy_value), encoding="utf-8")
        prompt_mode_drift.write_text(json.dumps(prompt_value), encoding="utf-8")
        mode_args = self._arguments("stop-brake-drift-output.json")
        mode_args.update(
            taxonomy_path=taxonomy_drift,
            taxonomy_sha256=_sha256(taxonomy_drift),
            prompts_path=prompt_mode_drift,
            prompts_sha256=_sha256(prompt_mode_drift),
        )
        with self.assertRaisesRegex(PlanCompileError, "stop_brake loop contract drifted"):
            compile_animal_library_plan(**mode_args)

        category_value = json.loads(TAXONOMY.read_text(encoding="utf-8-sig"))
        category_value["clips"][19]["category"] = "behavior"
        category_drift = self.root / "taxonomy-category-drift.json"
        category_drift.write_text(json.dumps(category_value), encoding="utf-8")
        category_args = self._arguments("category-drift-output.json")
        category_args.update(
            taxonomy_path=category_drift,
            taxonomy_sha256=_sha256(category_drift),
        )
        with self.assertRaisesRegex(PlanCompileError, "attack_primary category contract drifted"):
            compile_animal_library_plan(**category_args)

        pose_value = json.loads(TAXONOMY.read_text(encoding="utf-8-sig"))
        pose_value["clips"][25]["end_pose_id"] = "default_pose"
        pose_drift = self.root / "taxonomy-pose-drift.json"
        pose_drift.write_text(json.dumps(pose_value), encoding="utf-8")
        pose_args = self._arguments("pose-drift-output.json")
        pose_args.update(taxonomy_path=pose_drift, taxonomy_sha256=_sha256(pose_drift))
        with self.assertRaisesRegex(PlanCompileError, "death start/end pose contract drifted"):
            compile_animal_library_plan(**pose_args)

        loop_value = json.loads(LOOP_WORKFLOW.read_text(encoding="utf-8-sig"))
        del loop_value["900002"]
        loop_drift = self.root / "loop-drift.json"
        loop_drift.write_text(json.dumps(loop_value), encoding="utf-8")
        loop_args = self._arguments("loop-drift-output.json")
        loop_args.update(
            loop_workflow_path=loop_drift,
            loop_workflow_sha256=_sha256(loop_drift),
        )
        with self.assertRaisesRegex(PlanCompileError, "Loop workflow must contain exactly"):
            compile_animal_library_plan(**loop_args)

        bundle_value = json.loads(self.bundle.read_text(encoding="utf-8"))
        bundle_value["source"]["sha256"] = "0" * 64
        bundle_drift = self.root / "bundle-drift.json"
        bundle_drift.write_text(json.dumps(bundle_value), encoding="utf-8")
        manifest_value = json.loads(self.immutable_manifest.read_text(encoding="utf-8"))
        drift_sha = _sha256(bundle_drift)
        drift_bytes = bundle_drift.stat().st_size
        manifest_value["bundle_manifest"]["sha256"] = drift_sha
        bundle_row = next(
            row for row in manifest_value["files"] if row["filename"] == self.bundle.name
        )
        manifest_value["bundle_total_bytes"] += drift_bytes - bundle_row["bytes"]
        bundle_row["sha256"] = drift_sha
        bundle_row["bytes"] = drift_bytes
        manifest_drift = self.root / "manifest-for-bundle-drift.json"
        manifest_drift.write_text(json.dumps(manifest_value), encoding="utf-8")
        bundle_args = self._arguments("bundle-drift-output.json")
        bundle_args.update(
            immutable_manifest_path=manifest_drift,
            immutable_manifest_sha256=_sha256(manifest_drift),
            fitting_bundle_path=bundle_drift,
            fitting_bundle_sha256=drift_sha,
        )
        with self.assertRaisesRegex(PlanCompileError, "Fitting bundle source model SHA-256"):
            compile_animal_library_plan(**bundle_args)

        skeleton_value = json.loads(self.skeleton.read_text(encoding="utf-8"))
        skeleton_value["armatures"][0]["bones"].append({"name": "tail", "parent": "spine"})
        skeleton_drift = self.root / "skeleton-drift.json"
        skeleton_drift.write_text(json.dumps(skeleton_value), encoding="utf-8")
        skeleton_args = self._arguments("skeleton-drift-output.json")
        skeleton_args.update(
            skeleton_path=skeleton_drift,
            skeleton_sha256=_sha256(skeleton_drift),
        )
        with self.assertRaisesRegex(PlanCompileError, "Fitting bundle skeleton SHA-256"):
            compile_animal_library_plan(**skeleton_args)

        manifest_model_value = json.loads(self.immutable_manifest.read_text(encoding="utf-8"))
        manifest_model_value["source_model"]["sha256"] = "1" * 64
        manifest_model_drift = self.root / "manifest-model-drift.json"
        manifest_model_drift.write_text(json.dumps(manifest_model_value), encoding="utf-8")
        manifest_model_args = self._arguments("manifest-model-drift-output.json")
        manifest_model_args.update(
            immutable_manifest_path=manifest_model_drift,
            immutable_manifest_sha256=_sha256(manifest_model_drift),
        )
        with self.assertRaisesRegex(PlanCompileError, "Immutable manifest source model SHA-256"):
            compile_animal_library_plan(**manifest_model_args)

        self.assertFalse((self.root / "prompt-drift-output.json").exists())
        self.assertFalse((self.root / "loop-drift-output.json").exists())
        self.assertFalse((self.root / "bundle-drift-output.json").exists())
        self.assertFalse((self.root / "stop-brake-drift-output.json").exists())
        self.assertFalse((self.root / "category-drift-output.json").exists())
        self.assertFalse((self.root / "pose-drift-output.json").exists())
        self.assertFalse((self.root / "skeleton-drift-output.json").exists())
        self.assertFalse((self.root / "manifest-model-drift-output.json").exists())


if __name__ == "__main__":
    unittest.main()
