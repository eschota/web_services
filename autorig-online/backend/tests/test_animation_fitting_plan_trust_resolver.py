from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil
import sys
import tempfile
import types
import unittest
import unittest.mock
import uuid


BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import animation_fitting_candidate_job_plan as plan  # noqa: E402
import animation_fitting_plan_trust_resolver as resolver  # noqa: E402
from animation_fitting.controlled_experiment import (  # noqa: E402
    ControlledExperimentResult,
)
from animation_fitting.storage import StoredArtifact  # noqa: E402


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _seed(task_id: str, semantic_id: str, index: int) -> int:
    material = (
        f"{task_id}\n{semantic_id}\n{index}\nautorig.animation-fitting-prompts.v1"
    )
    return int(hashlib.sha256(material.encode()).hexdigest()[:16], 16) & ((1 << 63) - 1)


class ProductionPlanTrustResolverTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="autorig-plan-trust-")
        self.root = Path(self.temp.name)
        for name in ("jobs", "raw", "reference-manifests", "references"):
            (self.root / name).mkdir()
        self.task_id = str(uuid.uuid4())
        self.task_guid = str(uuid.uuid4())
        self.task = {
            "id": self.task_id,
            "guid": self.task_guid,
            "status": "done",
            "input_type": "animal",
        }
        self.model_sha = _sha(b"model")
        self.skeleton_sha = _sha(b"skeleton")
        self.reference_payload = b"\x89PNG\r\n\x1a\nserver-owned-reference"
        self.reference_relative = f"references/{self.task_id}/reference_rgb.png"
        reference_path = self.root / self.reference_relative
        reference_path.parent.mkdir(parents=True)
        reference_path.write_bytes(self.reference_payload)
        self.reference_content = {
            "schema": plan.REFERENCE_MANIFEST_SCHEMA,
            "task_id": self.task_id,
            "task_guid": self.task_guid,
            "source_rig_type": "HORSE_2",
            "species": "horse",
            "source_model_sha256": self.model_sha,
            "source_skeleton_sha256": self.skeleton_sha,
            "actionless": True,
            "geometry_uv_normals_mutated": False,
            "reference_artifact": {
                "path": self.reference_relative,
                "sha256": _sha(self.reference_payload),
                "bytes": len(self.reference_payload),
            },
        }
        reference_bytes = plan.canonical_json_bytes(self.reference_content)
        self.reference_sha = _sha(reference_bytes)
        self.reference_manifest_path = (
            self.root
            / "reference-manifests"
            / self.task_id
            / f"{self.reference_sha}.json"
        )
        self.reference_manifest_path.parent.mkdir(parents=True)
        self.reference_manifest_path.write_bytes(reference_bytes)
        self.fitting_job = {
            "id": str(uuid.uuid4()),
            "rig_type": "horse",
            "semantic_id": "walk_forward",
            "workflow_name": "",
            "workflow_fingerprint": "",
            "worker_url": "",
            "prompt_id": "",
            "candidate_target": 8,
            "candidate_limit": 16,
            "config_json": "{}",
        }
        self.job_directories: list[Path] = []
        self.controlled_results: list[ControlledExperimentResult] = []
        self._old_ingest = sys.modules.get("animation_fitting_candidate_ingest")
        fake_ingest = types.ModuleType("animation_fitting_candidate_ingest")
        fake_ingest.derive_browser_candidate_seed = _seed
        sys.modules["animation_fitting_candidate_ingest"] = fake_ingest

    def tearDown(self) -> None:
        if self._old_ingest is None:
            sys.modules.pop("animation_fitting_candidate_ingest", None)
        else:
            sys.modules["animation_fitting_candidate_ingest"] = self._old_ingest
        self.temp.cleanup()

    def _prompt_strings(self) -> tuple[str, str, dict, dict, dict]:
        clip, output_fps = plan._load_taxonomy_clip("walk_forward")
        prompt_contract, workflow_contract = (
            plan._canonical_prompt_and_workflow_contract(
                semantic_id="walk_forward", clip=clip, species="horse"
            )
        )
        prompt_doc = json.loads(
            (
                BACKEND / "animation_fitting" / "specs" / "action_prompts.v1.json"
            ).read_text(encoding="utf-8")
        )
        prompt_row = next(
            row
            for row in prompt_doc["actions_array"]
            if row["action_id_string"] == "walk_forward"
        )
        positive = plan._render_prompt(
            " ".join(
                (
                    prompt_doc["common_positive_prefix_string"],
                    prompt_row["motion_prompt_string"],
                    prompt_doc["loop_instruction_string"],
                )
            ),
            "horse",
            "positive prompt",
        )
        negative = plan._render_prompt(
            prompt_doc["common_negative_prompt_string"],
            "horse",
            "negative prompt",
        )
        return positive, negative, clip, prompt_contract, workflow_contract

    def _write_candidate(self, index: int) -> None:
        positive, negative, clip, prompt_contract, workflow = self._prompt_strings()
        seed = _seed(self.task_id, "walk_forward", index)
        experiment_id = f"horse_walk_trust_{self.task_id[:8]}_{index:02d}_v1"
        experiment_content = {
            "schema": "autorig.animation-fitting-experiment.v1",
            "experiment_id_string": experiment_id,
            "base_action_id_string": "walk_forward",
            "species_string": "horse",
            "generation_mode_string": "loop",
            "frame_count_int": int(clip["frame_profile"]),
            "input_fps_int": 24,
            "output_fps_int": 30,
            "seed_int": seed,
            "positive_prompt_string": positive,
            "negative_prompt_string": negative,
            "reference_object": {
                "immutable_manifest_sha256_string": self.reference_sha,
                "source_model_sha256_string": self.model_sha,
            },
            "workflow_object": {
                "workflow_name_string": workflow["workflow_name"],
                "workflow_fingerprint_sha256_string": workflow[
                    "workflow_fingerprint_sha256"
                ],
            },
        }
        experiment_payload = plan.canonical_json_bytes(experiment_content)
        experiment_sha = _sha(experiment_payload)
        experiment_path = (
            self.root
            / "animation_fitting"
            / "specs"
            / "experiments"
            / f"{experiment_id}.json"
        )
        experiment_path.parent.mkdir(parents=True, exist_ok=True)
        experiment_path.write_bytes(experiment_payload)
        identity = {
            "schema": plan.CONTROLLED_STATE_SCHEMA,
            "experiment_id_string": experiment_id,
            "experiment_sha256_string": experiment_sha,
            "runtime_authorization_string": f"explicit_cli:{experiment_id}",
            "reference_sha256_string": _sha(self.reference_payload),
            "positive_prompt_sha256_string": prompt_contract["positive_prompt_sha256"],
            "negative_prompt_sha256_string": prompt_contract["negative_prompt_sha256"],
            "seed_int": seed,
            "frame_count_int": int(clip["frame_profile"]),
            "input_fps_int": 24,
            "output_fps_int": 30,
            "start_guide_strength_float": 0.8,
            "end_guide_strength_float": 0.8,
            "worker_id_string": "local-4090",
            "worker_base_url_string": "http://127.0.0.1:8188",
            "workflow_name_string": workflow["workflow_name"],
            "workflow_fingerprint_string": workflow["workflow_fingerprint_sha256"],
            "approval_state_string": "generated_not_approved",
            "send_to_skeletal_fitting_bool": False,
        }
        job_id = _sha(
            json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
        )
        raw_payload = f"mp4-candidate-{index}".encode()
        raw_sha = _sha(raw_payload)
        raw_relative = f"raw/{raw_sha[:2]}/{raw_sha}.mp4"
        raw_path = self.root / raw_relative
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_bytes(raw_payload)
        state = {
            **identity,
            "sequence_int": 3,
            "recorded_at_unix_float": 1234.5 + index,
            "status_string": "completed",
            "prompt_id_string": plan.derive_controlled_prompt_id(job_id),
            "raw_video_path_string": str(raw_path.resolve()),
            "raw_video_sha256_string": raw_sha,
            "raw_video_bytes_int": len(raw_payload),
            "frame_paths_array": [f"frame-{frame}" for frame in range(49)],
            "frame_sha256_array": [
                _sha(f"frame-{frame}".encode()) for frame in range(49)
            ],
            "backend_output_object": {
                "filename_string": f"candidate-{index}.mp4",
                "subfolder_string": "animation_fitting",
                "type_string": "output",
            },
        }
        directory = self.root / "jobs" / job_id
        directory.mkdir()
        (directory / "000003.json").write_text(
            json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        self.job_directories.append(directory)
        self.controlled_results.append(
            ControlledExperimentResult(
                job_id=job_id,
                prompt_id=state["prompt_id_string"],
                raw_video=StoredArtifact(
                    sha256=raw_sha,
                    path=raw_path,
                    size_bytes=len(raw_payload),
                ),
                frames=(),
                resumed_existing_result=False,
            )
        )

    def _write_job_set(
        self,
        fitting_job: dict,
        *,
        count: int,
        batch_start: int,
        parent_fitting_job: dict | None = None,
    ) -> dict:
        self.assertEqual(count, 8 if batch_start == 0 else 16)
        published = resolver.publish_controlled_job_set_manifest(
            store_root=self.root,
            task_record=self.task,
            fitting_job_record=fitting_job,
            controlled_results=self.controlled_results[:count],
            parent_fitting_job_record=parent_fitting_job,
        )
        replay = resolver.publish_controlled_job_set_manifest(
            store_root=self.root,
            task_record=self.task,
            fitting_job_record=fitting_job,
            controlled_results=self.controlled_results[:count],
            parent_fitting_job_record=parent_fitting_job,
        )
        self.assertTrue(published.created)
        self.assertFalse(replay.created)
        return {"content": published.content, "pin": published.pin}

    def _prime_first(self) -> plan.BrowserCandidateJobPlan:
        self.fitting_job["candidate_limit"] = 8
        for index in range(8):
            self._write_candidate(index)
        job_set = self._write_job_set(
            self.fitting_job, count=8, batch_start=0
        )
        audit = resolver._StoreAudit(self.root)
        trusted_task, reference = resolver._discover_reference(audit, self.task)
        latest, receipts = resolver._discover_controlled_receipts(
            audit,
            semantic_id="walk_forward",
            trusted_task=trusted_task,
            reference_manifest=reference,
            job_set_manifest=job_set,
        )
        built = plan.build_production_browser_candidate_batch_job_plan(
            {
                "schema": plan.BATCH_PLAN_REQUEST_SCHEMA,
                "semantic_id": "walk_forward",
                "candidate_target": 8,
                "candidate_limit": 8,
                "batch_start": 0,
            },
            fitting_job_id=self.fitting_job["id"],
            trusted_task=trusted_task,
            trusted_reference_manifest=reference,
            trusted_job_set_manifest=job_set,
            trusted_latest_states=latest,
            trusted_retry_authorization=None,
            verified_receipts=receipts,
        )
        self.fitting_job.update(
            {
                "workflow_name": built.workflow_name,
                "workflow_fingerprint": built.workflow_fingerprint,
                "worker_url": built.worker_base_url,
                "prompt_id": built.prompt_id,
                "config_json": json.dumps(built.config),
            }
        )
        return built

    def _resolve(self, fitting_job=None, *, parent=None):
        return resolver.resolve_production_candidate_plan(
            store_root=self.root,
            task_record=self.task,
            fitting_job_record=fitting_job or self.fitting_job,
            parent_fitting_job_record=parent,
        )

    @staticmethod
    def _identity_receipt(binding: dict) -> bytes:
        identity = _sha(plan.canonical_json_bytes(binding))
        return plan.canonical_json_bytes({**binding, "identity_sha256": identity}) + b"\n"

    def _write_parent_evidence(
        self,
        parent_result,
        successor_id: str,
        *,
        eligible_indices: list[int] | None = None,
        contradictory_pass: bool = False,
    ) -> tuple[dict, dict]:
        parent_id = self.fitting_job["id"]
        admission_pins = [
            {"filename": "admission.json", "sha256": f"{index + 1:x}" * 64, "bytes": 900 + index}
            for index in range(8)
        ]
        closure_binding = {
            "schema": "autorig.browser-animation-candidate-generation-closure.v1",
            "job_id": parent_id,
            "lifecycle_identity_sha256": "d" * 64,
            "lifecycle": {"parent": parent_id},
            "admissions": admission_pins,
        }
        closure_payload = self._identity_receipt(closure_binding)
        closure = json.loads(closure_payload)
        closure_relative = (
            f"{parent_id}/browser-candidate-selection/generation-closure/"
            "generation-closure.json"
        )
        closure_path = self.root / closure_relative
        closure_path.parent.mkdir(parents=True)
        closure_path.write_bytes(closure_payload)
        outcomes = []
        for index in range(8):
            outcome_binding = {
                "schema": "autorig.browser-animation-candidate-outcome.v1",
                "job_id": parent_id,
                "candidate_index": index,
                "seed": _seed(self.task_id, "walk_forward", index),
                "candidate_identity_sha256": f"{index + 8:x}" * 64,
                "admission": admission_pins[index],
                "status": (
                    "VALIDATED_PASS"
                    if contradictory_pass and index == 0
                    else "VALIDATED_FAIL"
                ),
                "server_validation": None,
                "failure": (
                    None
                    if contradictory_pass and index == 0
                    else {"code": "visual_gate_failed"}
                ),
            }
            outcome_payload = self._identity_receipt(outcome_binding)
            relative = (
                f"{parent_id}/browser-candidate-selection/outcomes/"
                f"{index:02d}/outcome.json"
            )
            path = self.root / relative
            path.parent.mkdir(parents=True)
            path.write_bytes(outcome_payload)
            outcomes.append(
                {"path": relative, "sha256": _sha(outcome_payload), "bytes": len(outcome_payload)}
            )
        selection_candidates = []
        for index, outcome_pin in enumerate(outcomes):
            outcome_status = (
                "VALIDATED_PASS"
                if contradictory_pass and index == 0
                else "VALIDATED_FAIL"
            )
            selection_candidates.append(
                {
                    "candidate_index": index,
                    "seed": _seed(self.task_id, "walk_forward", index),
                    "candidate_identity_sha256": f"{index + 8:x}" * 64,
                    "candidate_manifest": {
                        "filename": "candidate-manifest.json",
                        "sha256": _sha(f"manifest-{index}".encode()),
                        "bytes": 800 + index,
                    },
                    "admission": admission_pins[index],
                    "human_review_lifecycle_binding_sha256": "d" * 64,
                    "server_outcome": {
                        "status": outcome_status,
                        "receipt": {
                            "filename": "outcome.json",
                            "sha256": outcome_pin["sha256"],
                            "bytes": outcome_pin["bytes"],
                        },
                        "validation_identity_sha256": None,
                        "validation_receipt": None,
                        "metrics": None,
                        "failure": (
                            None
                            if outcome_status == "VALIDATED_PASS"
                            else {"code": "visual_gate_failed"}
                        ),
                    },
                    "human_review": None,
                    "ranking": {
                        "eligible": False,
                        "failed_gates": ["visual_gate_failed"],
                        "missing_metric_keys": [],
                        "components": {},
                        "score": None,
                        "rank": None,
                        "provisional_order": None,
                    },
                }
            )
        selection_binding = {
            "schema": "autorig.browser-animation-candidate-selection.v1",
            "state": "OPEN",
            "mode": "production",
            "job": {
                "id": parent_id,
                "library_version_id": str(uuid.uuid4()),
                "library_revision": "horse-test-v1",
                "rig_type": "horse",
                "semantic_id": "walk_forward",
                "workflow_name": self.fitting_job["workflow_name"],
                "workflow_fingerprint": self.fitting_job["workflow_fingerprint"],
                "worker_url": self.fitting_job["worker_url"],
                "prompt_id": self.fitting_job["prompt_id"],
                "candidate_target": 8,
                "candidate_limit": 8,
                "adaptive_top_k": True,
                "lifecycle_identity_sha256": "d" * 64,
                "human_review_lifecycle_binding_sha256": "e" * 64,
            },
            "contracts": {"qa": "server-owned"},
            "inventory": {
                "candidate_set_sha256": "f" * 64,
                "admitted_count": 8,
                "terminal_count": 8,
                "eligible_count": 0,
                "pending_count": 0,
                "candidate_target_satisfied": True,
                "top_k_satisfied": False,
                "generation_closed": True,
                "generation_closure_identity_sha256": closure[
                    "identity_sha256"
                ],
            },
            "candidates": selection_candidates,
            "selection": {
                "top_candidate_identity_sha256": None,
                "top_k_candidate_identity_sha256": [],
                "provisional_order_candidate_identity_sha256": [],
                "comparative_selection": False,
                "production_eligible": False,
                "finalization_reason": "open_generation",
                "finalized_by": None,
            },
        }
        selection_payload = self._identity_receipt(selection_binding)
        selection = json.loads(selection_payload)
        selection_relative = (
            f"{parent_id}/browser-candidate-selection/snapshots/"
            f"{selection['identity_sha256']}/selection-receipt.json"
        )
        selection_path = self.root / selection_relative
        selection_path.parent.mkdir(parents=True)
        selection_path.write_bytes(selection_payload)
        content = {
            "schema": plan.RETRY_AUTHORIZATION_SCHEMA_V2,
            "status": "authorized",
            "task_id": self.task_id,
            "task_guid": self.task_guid,
            "semantic_id": "walk_forward",
            "parent_fitting_job_id": parent_id,
            "successor_fitting_job_id": successor_id,
            "parent_job_set_manifest_sha256": parent_result.job_set_manifest["pin"]["sha256"],
            "parent_config_sha256": _sha(plan.canonical_json_bytes(json.loads(self.fitting_job["config_json"]))),
            "parent_lifecycle_identity_sha256": "d" * 64,
            "parent_generation_closure": {
                "path": closure_relative,
                "sha256": _sha(closure_payload),
                "bytes": len(closure_payload),
            },
            "parent_generation_closure_identity_sha256": closure["identity_sha256"],
            "parent_selection_receipt": {
                "path": selection_relative,
                "sha256": _sha(selection_payload),
                "bytes": len(selection_payload),
            },
            "parent_selection_identity_sha256": selection[
                "identity_sha256"
            ],
            "first_batch_candidate_indices": list(range(8)),
            "first_batch_latest_state_sha256s": [row["pin"]["sha256"] for row in parent_result.latest_states],
            "first_batch_outcomes": outcomes,
            "first_batch_eligible_candidate_indices": eligible_indices or [],
            "first_batch_outcome": "no_candidate_passed",
            "authorized_candidate_indices": list(range(8, 16)),
        }
        payload = plan.canonical_json_bytes(content)
        digest = _sha(payload)
        relative = (
            f"retry-authorizations/{self.task_id}/walk_forward/{parent_id}/"
            f"{successor_id}/{digest}.json"
        )
        path = self.root / relative
        path.parent.mkdir(parents=True)
        path.write_bytes(payload)
        return (
            {"content": content, "pin": {"path": relative, "sha256": digest, "bytes": len(payload)}},
            closure,
        )

    def _prime_successor(
        self, *, eligible_indices=None, contradictory_pass=False
    ):
        parent_result = self._resolve()
        for index in range(8, 16):
            self._write_candidate(index)
        successor = {
            **self.fitting_job,
            "id": str(uuid.uuid4()),
            "candidate_limit": 16,
            "config_json": "{}",
        }
        authorization, closure = self._write_parent_evidence(
            parent_result,
            successor["id"],
            eligible_indices=eligible_indices,
            contradictory_pass=contradictory_pass,
        )
        job_set = self._write_job_set(
            successor,
            count=16,
            batch_start=8,
            parent_fitting_job=self.fitting_job,
        )
        audit = resolver._StoreAudit(self.root)
        trusted_task, reference = resolver._discover_reference(audit, self.task)
        latest, receipts = resolver._discover_controlled_receipts(
            audit,
            semantic_id="walk_forward",
            trusted_task=trusted_task,
            reference_manifest=reference,
            job_set_manifest=job_set,
        )
        retry = resolver._discover_retry_authorization(
            audit,
            semantic_id="walk_forward",
            trusted_task=trusted_task,
            latest_states=latest,
            job_set_manifest=job_set,
            fitting_job_record=successor,
            parent_fitting_job_record=self.fitting_job,
            parent_resolution=parent_result,
        )
        built = plan.build_production_browser_candidate_batch_job_plan(
            {
                "schema": plan.BATCH_PLAN_REQUEST_SCHEMA,
                "semantic_id": "walk_forward",
                "candidate_target": 8,
                "candidate_limit": 16,
                "batch_start": 8,
            },
            fitting_job_id=successor["id"],
            trusted_task=trusted_task,
            trusted_reference_manifest=reference,
            trusted_job_set_manifest=job_set,
            trusted_latest_states=latest,
            trusted_retry_authorization=retry,
            verified_receipts=receipts,
        )
        successor.update(
            {
                "workflow_name": built.workflow_name,
                "workflow_fingerprint": built.workflow_fingerprint,
                "worker_url": built.worker_base_url,
                "prompt_id": built.prompt_id,
                "config_json": json.dumps(built.config),
            }
        )
        return successor, built, authorization

    def test_resolves_immutable_first_eight_and_exact_db_plan(self) -> None:
        expected = self._prime_first()
        result = self._resolve()
        self.assertEqual(result.plan.config_sha256, expected.config_sha256)
        self.assertEqual((result.plan.batch_start, result.plan.candidate_limit), (0, 8))
        self.assertEqual(len(result.latest_states), 8)
        self.assertIsNone(result.retry_authorization)

    def test_job_set_publisher_is_create_exclusive_and_namespace_locked(self) -> None:
        self._prime_first()
        namespace = (
            self.root
            / "job-sets"
            / self.task_id
            / "walk_forward"
            / self.fitting_job["id"]
        )
        sibling = namespace / f"{'f' * 64}.json"
        sibling.write_bytes(b"{}")
        with self.assertRaisesRegex(
            resolver.AnimationFittingPlanTrustError,
            "already pinned to different bytes",
        ):
            resolver.publish_controlled_job_set_manifest(
                store_root=self.root,
                task_record=self.task,
                fitting_job_record=self.fitting_job,
                controlled_results=self.controlled_results[:8],
            )

    def test_namespaces_isolate_30_actions_and_exploratory_jobs(self) -> None:
        self._prime_first()
        taxonomy = json.loads((BACKEND / "animal_animation_taxonomy.v1.json").read_text(encoding="utf-8"))
        for clip in taxonomy["clips"]:
            if clip["id"] == "walk_forward":
                continue
            path = self.root / "job-sets" / self.task_id / clip["id"] / str(uuid.uuid4())
            path.mkdir(parents=True)
            (path / "exploratory.json").write_text("not authoritative", encoding="utf-8")
        exploratory = self.root / "jobs" / ("f" * 64)
        exploratory.mkdir()
        (exploratory / "garbage.txt").write_text("ignored", encoding="utf-8")
        result = self._resolve()
        self.assertEqual(len(result.latest_states), 8)

    def test_config_is_comparison_only(self) -> None:
        self._prime_first()
        forged = json.loads(self.fitting_job["config_json"])
        forged["browser_candidate_ingest"]["candidate_slots"][0]["seed"] += 1
        self.fitting_job["config_json"] = json.dumps(forged)
        with self.assertRaisesRegex(resolver.AnimationFittingPlanTrustError, "DB/config differs"):
            self._resolve()

    def test_successor_is_separate_and_admits_only_global_indices_8_to_15(self) -> None:
        parent_plan = self._prime_first()
        parent_config = self.fitting_job["config_json"]
        successor, expected, _ = self._prime_successor()
        result = self._resolve(successor, parent=self.fitting_job)
        self.assertEqual((result.plan.batch_start, result.plan.candidate_target, result.plan.candidate_limit), (8, 8, 16))
        self.assertEqual(
            [row["candidate_index"] for row in result.plan.config["browser_candidate_ingest"]["candidate_slots"]],
            list(range(8, 16)),
        )
        self.assertEqual(len(result.verified_receipts), 16)
        self.assertEqual(result.plan.config_sha256, expected.config_sha256)
        self.assertEqual(self.fitting_job["config_json"], parent_config)
        self.assertEqual(parent_plan.candidate_limit, 8)

    def test_successor_rejects_one_eligible_parent_candidate(self) -> None:
        self._prime_first()
        with self.assertRaisesRegex(
            resolver.AnimationFittingPlanTrustError, "zero-pass"
        ):
            self._prime_successor(eligible_indices=[0])

    def test_successor_rejects_two_eligible_parent_candidates(self) -> None:
        self._prime_first()
        with self.assertRaisesRegex(
            resolver.AnimationFittingPlanTrustError, "zero-pass"
        ):
            self._prime_successor(eligible_indices=[0, 1])

    def test_successor_rejects_pass_outcome_hidden_by_empty_eligible_list(self) -> None:
        self._prime_first()
        with self.assertRaisesRegex(
            resolver.AnimationFittingPlanTrustError,
            "contradicts the claimed zero-pass",
        ):
            self._prime_successor(contradictory_pass=True)

    def test_tamper_newer_sibling_and_stale_parent_fail_closed(self) -> None:
        self._prime_first()
        state = self.job_directories[0] / "000003.json"
        original = state.read_bytes()
        state.write_bytes(original + b" ")
        with self.assertRaises(resolver.AnimationFittingPlanTrustError):
            self._resolve()
        state.write_bytes(original)
        parsed = json.loads(original)
        parsed["sequence_int"] = 4
        parsed["status_string"] = "rendering"
        (self.job_directories[0] / "000004.json").write_text(json.dumps(parsed), encoding="utf-8")
        with self.assertRaisesRegex(resolver.AnimationFittingPlanTrustError, "not completed"):
            self._resolve()

        (self.job_directories[0] / "000004.json").unlink()
        successor, _, _ = self._prime_successor()
        self.fitting_job["config_json"] = "{}"
        with self.assertRaises(resolver.AnimationFittingPlanTrustError):
            self._resolve(successor, parent=self.fitting_job)

    def test_path_escape_symlink_and_toctou_fail_closed(self) -> None:
        self._prime_first()
        raw_path = next((self.root / "raw").glob("*/*.mp4"))
        backup = raw_path.with_suffix(".real")
        raw_path.rename(backup)
        try:
            raw_path.symlink_to(backup)
        except OSError:
            backup.rename(raw_path)
            self.skipTest("this Windows account cannot create symlinks")
        with self.assertRaisesRegex(resolver.AnimationFittingPlanTrustError, "symlink|safely readable"):
            self._resolve()
        raw_path.unlink()
        backup.rename(raw_path)

        original_builder = resolver._build_and_validate_plan
        target = self.job_directories[0]

        def append_sibling(**kwargs):
            result = original_builder(**kwargs)
            shutil.copyfile(target / "000003.json", target / "000004.json")
            return result

        with unittest.mock.patch.object(resolver, "_build_and_validate_plan", side_effect=append_sibling):
            with self.assertRaisesRegex(resolver.AnimationFittingPlanTrustError, "directory inventory changed"):
                self._resolve()


if __name__ == "__main__":
    unittest.main()
