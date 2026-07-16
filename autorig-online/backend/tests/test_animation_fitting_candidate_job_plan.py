from __future__ import annotations

import hashlib
import importlib
import json
from pathlib import Path
import re
import sys
import types
import unittest
from unittest.mock import patch
import uuid


BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

plan = importlib.import_module("animation_fitting_candidate_job_plan")

_DEFAULT_EXPERIMENT = object()
_AUTO_SERVER_INPUT = object()


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


class CandidateJobPlanTests(unittest.TestCase):
    def setUp(self) -> None:
        self.task_id = str(uuid.uuid4())
        self.guid = str(uuid.uuid4())
        self.task = {
            "schema": plan.TRUSTED_TASK_PINS_SCHEMA,
            "task_id": self.task_id,
            "task_guid": self.guid,
            "status": "done",
            "input_type": "animal",
            "source_rig_type": "HORSE_2",
            "source_model_sha256": plan.V14_SOURCE_MODEL_SHA256,
            "source_skeleton_sha256": plan.V14_SOURCE_SKELETON_SHA256,
        }
        self.request = {
            "schema": plan.PLAN_REQUEST_SCHEMA,
            "semantic_id": "walk_forward",
            "candidate_target": 8,
            "candidate_limit": 16,
        }
        clip, _ = plan._load_taxonomy_clip("walk_forward")
        self.prompt_contract, self.workflow_contract = (
            plan._canonical_prompt_and_workflow_contract(
                semantic_id="walk_forward", clip=clip, species="horse"
            )
        )
        reference_content = {
            "schema": plan.REFERENCE_MANIFEST_SCHEMA,
            "task_id": self.task_id,
            "task_guid": self.guid,
            "source_rig_type": "HORSE_2",
            "species": "horse",
            "source_model_sha256": plan.V14_SOURCE_MODEL_SHA256,
            "source_skeleton_sha256": plan.V14_SOURCE_SKELETON_SHA256,
            "actionless": True,
            "geometry_uv_normals_mutated": False,
            "reference_artifact": {
                "path": f"references/{self.task_id}/reference_rgb.png",
                "sha256": "9" * 64,
                "bytes": 12345,
            },
        }
        reference_bytes = plan.canonical_json_bytes(reference_content)
        reference_sha = _sha(reference_bytes)
        self.reference_manifest = {
            "content": reference_content,
            "pin": {
                "path": f"reference-manifests/{self.task_id}/{reference_sha}.json",
                "sha256": reference_sha,
                "bytes": len(reference_bytes),
            },
        }

    @staticmethod
    def seed(task_id: str, semantic_id: str, index: int) -> int:
        material = (
            f"{task_id}\n{semantic_id}\n{index}\nautorig.animation-fitting-prompts.v1"
        )
        return int(hashlib.sha256(material.encode()).hexdigest()[:16], 16) & (
            (1 << 63) - 1
        )

    @staticmethod
    def action_context(semantic_id: str) -> tuple[dict, dict, dict]:
        clip, _ = plan._load_taxonomy_clip(semantic_id)
        prompt, workflow = plan._canonical_prompt_and_workflow_contract(
            semantic_id=semantic_id, clip=clip, species="horse"
        )
        return clip, prompt, workflow

    def receipt(
        self,
        index: int,
        *,
        seed: int | None = None,
        status: str = "completed",
        descriptor_version: int = 2,
        experiment_id: str | None = None,
        experiment_sha: str | None = None,
        experiment_spec: dict | None | object = _DEFAULT_EXPERIMENT,
        semantic_id: str = "walk_forward",
        generation_mode: str = "loop",
        task: dict | None = None,
        prompt_contract: dict | None = None,
        reference_manifest: dict | None = None,
        frame_count: int = 49,
        input_fps: int = 24,
        output_fps: int = 30,
        worker_id: str = "local-4090",
        worker_base_url: str = "http://127.0.0.1:8188",
        workflow_name: str | None = None,
        workflow_sha: str | None = None,
        job_id: str | None = None,
        prompt_id: str | None = None,
    ) -> dict:
        job_id = job_id or hashlib.sha256(f"job-{index}".encode()).hexdigest()
        video_sha = hashlib.sha256(f"video-{index}".encode()).hexdigest()
        actual_seed = (
            self.seed(self.task_id, semantic_id, index) if seed is None else seed
        )
        actual_prompt = prompt_contract or self.prompt_contract
        actual_reference = reference_manifest or self.reference_manifest
        actual_workflow_name = workflow_name or self.workflow_contract["workflow_name"]
        actual_workflow_sha = (
            workflow_sha or self.workflow_contract["workflow_fingerprint_sha256"]
        )
        if descriptor_version == 1:
            state_sha = hashlib.sha256(f"state-{index}".encode()).hexdigest()
            return {
                "schema": plan.CONTROLLED_RECEIPT_SCHEMA_V1,
                "status": status,
                "candidate_index": index,
                "seed": actual_seed,
                "job_id": job_id,
                "prompt_id": prompt_id or plan.derive_controlled_prompt_id(job_id),
                "experiment_id": experiment_id or f"horse_walk_candidate_{index}_v1",
                "experiment_sha256": experiment_sha or (f"{index + 1:x}" * 64)[:64],
                "worker_id": worker_id,
                "worker_base_url": worker_base_url,
                "workflow_name": actual_workflow_name,
                "workflow_fingerprint_sha256": actual_workflow_sha,
                "frame_count": frame_count,
                "input_fps": input_fps,
                "output_fps": output_fps,
                "receipt": {
                    "path": f"jobs/{job_id}/000003.json",
                    "sha256": state_sha,
                    "bytes": 1200 + index,
                },
                "source_video": {
                    "path": f"raw/{video_sha[:2]}/{video_sha}.mp4",
                    "sha256": video_sha,
                    "bytes": 5000 + index,
                },
            }
        if experiment_spec is _DEFAULT_EXPERIMENT:
            experiment_spec = self.experiment_spec(
                index,
                semantic_id=semantic_id,
                generation_mode=generation_mode,
                frame_count=frame_count,
                seed=actual_seed,
                prompt_contract=actual_prompt,
                workflow_name=actual_workflow_name,
                workflow_sha=actual_workflow_sha,
                reference_manifest=actual_reference,
            )
        experiment_id_value = (
            experiment_spec["content"]["experiment_id_string"]
            if isinstance(experiment_spec, dict)
            else experiment_id or "missing_experiment"
        )
        experiment_sha_value = (
            experiment_spec["pin"]["sha256"]
            if isinstance(experiment_spec, dict)
            else experiment_sha or "0" * 64
        )
        base = {
            "schema": plan.CONTROLLED_RECEIPT_SCHEMA_V2,
            "status": status,
            "candidate_index": index,
            "seed": actual_seed,
            "job_id": job_id,
            "prompt_id": prompt_id or plan.derive_controlled_prompt_id(job_id),
            "semantic_id": semantic_id,
            "generation_mode": generation_mode,
            "task": task or self.task,
            "prompt_contract": actual_prompt,
            "reference_manifest": actual_reference,
            "experiment_id": experiment_id_value,
            "experiment_sha256": experiment_sha_value,
            "experiment_spec": experiment_spec,
            "worker_id": worker_id,
            "worker_base_url": worker_base_url,
            "workflow_name": actual_workflow_name,
            "workflow_fingerprint_sha256": actual_workflow_sha,
            "frame_count": frame_count,
            "input_fps": input_fps,
            "output_fps": output_fps,
            "source_video": {
                "path": f"raw/{video_sha[:2]}/{video_sha}.mp4",
                "sha256": video_sha,
                "bytes": 5000 + index,
            },
        }
        return base

    def experiment_spec(
        self,
        index: int,
        *,
        semantic_id: str = "walk_forward",
        generation_mode: str = "loop",
        frame_count: int = 49,
        seed: int | None = None,
        prompt_contract: dict | None = None,
        workflow_name: str | None = None,
        workflow_sha: str | None = None,
        reference_manifest: dict | None = None,
    ) -> dict:
        prompt_contract = prompt_contract or self.prompt_contract
        workflow_name = workflow_name or self.workflow_contract["workflow_name"]
        workflow_sha = (
            workflow_sha or self.workflow_contract["workflow_fingerprint_sha256"]
        )
        reference_manifest = reference_manifest or self.reference_manifest
        prompt_doc = json.loads(
            (
                BACKEND / "animation_fitting" / "specs" / "action_prompts.v1.json"
            ).read_text(encoding="utf-8")
        )
        prompt_row = next(
            row
            for row in prompt_doc["actions_array"]
            if row["action_id_string"] == semantic_id
        )
        instruction = prompt_doc[
            "loop_instruction_string"
            if generation_mode == "loop"
            else "one_shot_instruction_string"
        ]
        positive = plan._render_prompt(
            " ".join(
                (
                    prompt_doc["common_positive_prefix_string"],
                    prompt_row["motion_prompt_string"],
                    instruction,
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
        self.assertEqual(
            _sha(positive.encode()), prompt_contract["positive_prompt_sha256"]
        )
        self.assertEqual(
            _sha(negative.encode()), prompt_contract["negative_prompt_sha256"]
        )
        experiment_id = f"horse_{semantic_id}_candidate_{index}_v1"
        content = {
            "schema": "autorig.animation-fitting-experiment.v1",
            "experiment_id_string": experiment_id,
            "base_action_id_string": semantic_id,
            "species_string": "horse",
            "generation_mode_string": generation_mode,
            "frame_count_int": frame_count,
            "input_fps_int": 24,
            "output_fps_int": 30,
            "seed_int": self.seed(self.task_id, semantic_id, index)
            if seed is None
            else seed,
            "positive_prompt_string": positive,
            "negative_prompt_string": negative,
            "reference_object": {
                "immutable_manifest_sha256_string": reference_manifest["pin"]["sha256"],
                "source_model_sha256_string": self.task["source_model_sha256"],
            },
            "workflow_object": {
                "workflow_name_string": workflow_name,
                "workflow_fingerprint_sha256_string": workflow_sha,
            },
        }
        encoded = plan.canonical_json_bytes(content)
        digest = _sha(encoded)
        return {
            "content": content,
            "pin": {
                "path": f"animation_fitting/specs/experiments/{experiment_id}.json",
                "sha256": digest,
                "bytes": len(encoded),
            },
        }

    def v14_receipt(self) -> dict:
        return self.receipt(
            0,
            descriptor_version=1,
            seed=plan.V14_FIXED_SEED,
            experiment_id=plan.V14_EXPERIMENT_ID,
            experiment_sha=plan.V14_EXPERIMENT_SHA256,
            worker_id=plan.V14_WORKER_ID,
            worker_base_url=plan.V14_WORKER_BASE_URL,
            workflow_name=plan.V14_WORKFLOW_NAME,
            workflow_sha=plan.V14_WORKFLOW_FINGERPRINT_SHA256,
            job_id=plan.V14_CONTROLLED_JOB_ID,
            prompt_id=plan.V14_PROMPT_ID,
        )

    @staticmethod
    def latest_state(receipt: dict, *, sequence: int = 3) -> dict:
        index = receipt["candidate_index"]
        job_id = receipt["job_id"]
        if receipt["schema"] == plan.CONTROLLED_RECEIPT_SCHEMA_V1:
            pin = receipt["receipt"]
            filename = Path(pin["path"]).name
            sequence = int(filename[:6])
        else:
            filename = f"{sequence:06d}.json"
            pin = {
                "path": f"jobs/{job_id}/{filename}",
                "sha256": hashlib.sha256(f"state-{index}".encode()).hexdigest(),
                "bytes": 1200 + index,
            }
        return {
            "schema": plan.TRUSTED_LATEST_STATE_SCHEMA,
            "status": "completed",
            "latest": True,
            "job_id": job_id,
            "state_schema": plan.CONTROLLED_STATE_SCHEMA,
            "sequence": sequence,
            "filename": filename,
            "pin": pin,
        }

    def retry_authorization(
        self, receipts: list[dict], latest_states: list[dict]
    ) -> dict:
        states = {state["job_id"]: state for state in latest_states}
        first = sorted(receipts, key=lambda row: row["candidate_index"])[:8]
        content = {
            "schema": plan.RETRY_AUTHORIZATION_SCHEMA,
            "status": "authorized",
            "task_id": self.task_id,
            "task_guid": self.guid,
            "semantic_id": receipts[0]["semantic_id"],
            "first_batch_candidate_indices": list(range(8)),
            "first_batch_latest_state_sha256s": [
                states[row["job_id"]]["pin"]["sha256"] for row in first
            ],
            "first_batch_selection_closure": {
                "path": f"closures/{self.task_id}/first-batch-selection.json",
                "sha256": "a" * 64,
                "bytes": 901,
            },
            "first_batch_qa_closure": {
                "path": f"closures/{self.task_id}/first-batch-qa.json",
                "sha256": "b" * 64,
                "bytes": 902,
            },
            "first_batch_outcome": "no_candidate_passed",
            "authorized_candidate_indices": list(range(8, 16)),
        }
        encoded = plan.canonical_json_bytes(content)
        digest = _sha(encoded)
        return {
            "content": content,
            "pin": {
                "path": f"retry-authorizations/{self.task_id}/{digest}.json",
                "sha256": digest,
                "bytes": len(encoded),
            },
        }

    def build_production(
        self,
        request: dict,
        *,
        trusted_task: dict | None = None,
        trusted_reference_manifest: dict | None = None,
        trusted_latest_states: list[dict] | object = _AUTO_SERVER_INPUT,
        trusted_retry_authorization: dict | None | object = _AUTO_SERVER_INPUT,
        verified_receipts: list[dict],
    ):
        if trusted_latest_states is _AUTO_SERVER_INPUT:
            trusted_latest_states = [
                self.latest_state(receipt) for receipt in verified_receipts
            ]
        if trusted_retry_authorization is _AUTO_SERVER_INPUT:
            trusted_retry_authorization = (
                self.retry_authorization(verified_receipts, trusted_latest_states)
                if len(verified_receipts) == 16
                else None
            )
        return plan.build_production_browser_candidate_job_plan(
            request,
            trusted_task=trusted_task or self.task,
            trusted_reference_manifest=(
                trusted_reference_manifest or self.reference_manifest
            ),
            trusted_latest_states=trusted_latest_states,
            trusted_retry_authorization=trusted_retry_authorization,
            verified_receipts=verified_receipts,
        )

    def build_v14(self, receipt: dict, *, latest_state: dict | None = None):
        return plan.build_v14_nonproduction_canary_job_plan(
            trusted_task=self.task,
            trusted_latest_state=latest_state or self.latest_state(receipt),
            verified_receipt=receipt,
        )

    def fake_ingest_module(self):
        fake = types.ModuleType("animation_fitting_candidate_ingest")
        fake.derive_browser_candidate_seed = self.seed
        return patch.dict(sys.modules, {"animation_fitting_candidate_ingest": fake})

    def test_v2_is_server_owned_canonical_and_order_independent(self) -> None:
        receipts = [self.receipt(index) for index in reversed(range(8))]
        with self.fake_ingest_module():
            first = self.build_production(
                self.request,
                trusted_task=self.task,
                verified_receipts=receipts,
            )
            second = self.build_production(
                self.request,
                trusted_task=self.task,
                verified_receipts=list(reversed(receipts)),
            )
        self.assertEqual(first.schema, plan.PLAN_IDENTITY_SCHEMA_V2)
        self.assertTrue(first.production_eligible)
        self.assertEqual(first.selection_mode, "production")
        self.assertEqual((first.candidate_target, first.candidate_limit), (8, 16))
        self.assertEqual(first.config_json, second.config_json)
        self.assertEqual(first.idempotency_key, second.idempotency_key)
        self.assertEqual(_sha(first.config_json.encode()), first.config_sha256)
        self.assertEqual(first.prompt_id, first.identity_sha256)
        self.assertEqual(first.worker_id, "local-4090")
        self.assertEqual(first.worker_base_url, "http://127.0.0.1:8188")
        self.assertEqual(first.worker_url, first.worker_base_url)
        self.assertEqual(first.generation_mode, "loop")
        config = json.loads(first.config_json)
        ingest = config["browser_candidate_ingest"]
        self.assertEqual(ingest["schema"], plan.INGEST_SCHEMA_V2)
        self.assertEqual(
            [row["candidate_index"] for row in ingest["candidate_slots"]],
            list(range(8)),
        )
        self.assertEqual(
            [row["seed"] for row in ingest["candidate_slots"]],
            [self.seed(self.task_id, "walk_forward", index) for index in range(8)],
        )
        for slot in ingest["candidate_slots"]:
            self.assertEqual(
                (
                    slot["controlled_generation"]["worker_id"],
                    slot["controlled_generation"]["worker_base_url"],
                ),
                (first.worker_id, first.worker_base_url),
            )
            controlled = slot["controlled_generation"]
            self.assertEqual(controlled["semantic_id"], "walk_forward")
            self.assertEqual(controlled["generation_mode"], "loop")
            self.assertEqual(controlled["task"], self.task)
            self.assertEqual(controlled["prompt_contract"], self.prompt_contract)
            self.assertEqual(controlled["reference_manifest"], self.reference_manifest)
            self.assertEqual(controlled["trusted_latest_state"]["sequence"], 3)
            self.assertEqual(
                controlled["trusted_latest_state"]["filename"], "000003.json"
            )
            self.assertEqual(
                controlled["prompt_id"],
                plan.derive_controlled_prompt_id(controlled["job_id"]),
            )
            self.assertEqual(
                controlled["experiment_id"],
                controlled["experiment_spec"]["content"]["experiment_id_string"],
            )
            self.assertEqual(
                controlled["experiment_sha256"],
                controlled["experiment_spec"]["pin"]["sha256"],
            )
        self.assertEqual(ingest["semantic_id"], "walk_forward")
        self.assertEqual(ingest["generation_mode"], "loop")
        self.assertEqual(ingest["prompt_contract"], self.prompt_contract)
        self.assertEqual(ingest["reference_manifest"], self.reference_manifest)
        self.assertEqual(
            config["browser_candidate_selection"],
            {"schema": plan.SELECTION_CONFIG_SCHEMA, "mode": "production"},
        )
        binding = config["browser_candidate_job_plan"]
        self.assertEqual(binding["schema"], plan.PLAN_BINDING_SCHEMA_V2)
        self.assertTrue(binding["production_eligible"])
        self.assertEqual(binding["trusted_task"], self.task)
        self.assertEqual(binding["trusted_reference_manifest"], self.reference_manifest)
        self.assertEqual(len(binding["trusted_latest_states"]), 8)
        self.assertIsNone(binding["trusted_retry_authorization"])
        for receipt in binding["verified_controlled_generation_receipts"]:
            self.assertEqual(receipt["schema"], plan.CONTROLLED_RECEIPT_SCHEMA_V2)
            self.assertEqual(receipt["task"], self.task)
            self.assertEqual(receipt["prompt_contract"], self.prompt_contract)
            self.assertEqual(receipt["reference_manifest"], self.reference_manifest)

    def test_v2_accepts_only_the_complete_optional_second_batch_of_16(self) -> None:
        with self.fake_ingest_module():
            built = self.build_production(
                self.request,
                trusted_task=self.task,
                verified_receipts=[self.receipt(index) for index in range(16)],
            )
        self.assertEqual((built.candidate_target, built.candidate_limit), (8, 16))
        self.assertEqual(
            [
                row["candidate_index"]
                for row in built.config["browser_candidate_ingest"]["candidate_slots"]
            ],
            list(range(16)),
        )
        binding = built.config["browser_candidate_job_plan"]
        self.assertIsNotNone(binding["trusted_retry_authorization"])
        retry_sha = binding["trusted_retry_authorization"]["pin"]["sha256"]
        slots = built.config["browser_candidate_ingest"]["candidate_slots"]
        self.assertEqual(
            [
                slot["controlled_generation"]["retry_authorization_sha256"]
                for slot in slots
            ],
            [None] * 8 + [retry_sha] * 8,
        )

    def test_second_batch_requires_independent_failed_first_batch_authorization(
        self,
    ) -> None:
        receipts = [self.receipt(index) for index in range(16)]
        states = [self.latest_state(receipt) for receipt in receipts]
        with (
            self.fake_ingest_module(),
            self.assertRaisesRegex(
                plan.BrowserCandidateJobPlanError, "requires server retry authorization"
            ),
        ):
            self.build_production(
                self.request,
                trusted_latest_states=states,
                trusted_retry_authorization=None,
                verified_receipts=receipts,
            )
        authorization = self.retry_authorization(receipts, states)
        bad_authorizations = []
        for field, value in (
            ("first_batch_outcome", "candidate_passed"),
            ("authorized_candidate_indices", list(range(7, 15))),
            ("first_batch_latest_state_sha256s", ["f" * 64] * 8),
        ):
            bad = json.loads(json.dumps(authorization))
            bad["content"][field] = value
            encoded = plan.canonical_json_bytes(bad["content"])
            bad["pin"]["sha256"] = _sha(encoded)
            bad["pin"]["bytes"] = len(encoded)
            bad_authorizations.append((field, bad))
        for field, bad in bad_authorizations:
            with (
                self.subTest(field=field),
                self.fake_ingest_module(),
                self.assertRaisesRegex(
                    plan.BrowserCandidateJobPlanError, "failed first batch"
                ),
            ):
                self.build_production(
                    self.request,
                    trusted_latest_states=states,
                    trusted_retry_authorization=bad,
                    verified_receipts=receipts,
                )
        with (
            self.fake_ingest_module(),
            self.assertRaisesRegex(
                plan.BrowserCandidateJobPlanError,
                "first candidate batch must not carry",
            ),
        ):
            self.build_production(
                self.request,
                trusted_latest_states=states[:8],
                trusted_retry_authorization=authorization,
                verified_receipts=receipts[:8],
            )

    def test_rejects_any_client_supplied_ingest_or_config(self) -> None:
        bad = dict(self.request)
        bad["browser_candidate_ingest"] = {"seed": 1}
        with self.assertRaisesRegex(
            plan.BrowserCandidateJobPlanError,
            "client-supplied browser_candidate_ingest is forbidden",
        ):
            self.build_production(bad, trusted_task=self.task, verified_receipts=[])
        nested = dict(self.request)
        nested["config"] = {"browser_candidate_ingest": {}}
        with self.assertRaisesRegex(
            plan.BrowserCandidateJobPlanError,
            "client-supplied browser_candidate_ingest is forbidden",
        ):
            self.build_production(nested, trusted_task=self.task, verified_receipts=[])
        client_worker = {
            **self.request,
            "worker_base_url": "https://client-selected-worker.example:8188",
        }
        with self.assertRaisesRegex(
            plan.BrowserCandidateJobPlanError, "client_request must contain exactly"
        ):
            self.build_production(
                client_worker, trusted_task=self.task, verified_receipts=[]
            )

    def test_rejects_untrusted_task_state_and_bad_pins(self) -> None:
        for field, value in (
            ("status", "created"),
            ("input_type", "t_pose"),
            ("source_model_sha256", "not-a-sha"),
        ):
            bad = dict(self.task)
            bad[field] = value
            with (
                self.subTest(field=field),
                self.fake_ingest_module(),
                self.assertRaises(plan.BrowserCandidateJobPlanError),
            ):
                self.build_production(
                    self.request,
                    trusted_task=bad,
                    verified_receipts=[self.receipt(i) for i in range(8)],
                )
        fake_rig = {**self.task, "source_rig_type": "HORSE_NOT_A_REAL_RIG"}
        with (
            self.fake_ingest_module(),
            self.assertRaisesRegex(
                plan.BrowserCandidateJobPlanError, "not a canonical supported rig"
            ),
        ):
            self.build_production(
                self.request,
                trusted_task=fake_rig,
                verified_receipts=[self.receipt(i) for i in range(8)],
            )

    def test_server_owned_reference_and_latest_inputs_are_required_by_api(self) -> None:
        receipts = [self.receipt(i) for i in range(8)]
        states = [self.latest_state(receipt) for receipt in receipts]
        with self.assertRaises(TypeError):
            plan.build_production_browser_candidate_job_plan(
                self.request,
                trusted_task=self.task,
                trusted_latest_states=states,
                trusted_retry_authorization=None,
                verified_receipts=receipts,
            )
        with self.assertRaises(TypeError):
            plan.build_v14_nonproduction_canary_job_plan(
                trusted_task=self.task,
                verified_receipt=self.v14_receipt(),
            )

    def test_production_target_limit_and_inventory_gates(self) -> None:
        cases = (
            (
                {**self.request, "candidate_target": 3, "candidate_limit": 5},
                8,
                "exactly candidate_target=8 and candidate_limit=16",
            ),
            (
                {**self.request, "candidate_limit": 8},
                8,
                "exactly candidate_target=8 and candidate_limit=16",
            ),
            (
                {**self.request, "candidate_target": 7},
                8,
                "exactly candidate_target=8 and candidate_limit=16",
            ),
            (self.request, 7, "authorized candidate batch"),
            (self.request, 9, "authorized candidate batch"),
        )
        for request, count, message in cases:
            with (
                self.subTest(message=message),
                self.fake_ingest_module(),
                self.assertRaisesRegex(plan.BrowserCandidateJobPlanError, message),
            ):
                self.build_production(
                    request,
                    trusted_task=self.task,
                    verified_receipts=[self.receipt(i) for i in range(count)],
                )
        noncontiguous = [self.receipt(index) for index in range(7)] + [self.receipt(8)]
        with (
            self.fake_ingest_module(),
            self.assertRaisesRegex(plan.BrowserCandidateJobPlanError, "contiguous"),
        ):
            self.build_production(
                self.request,
                trusted_task=self.task,
                verified_receipts=noncontiguous,
            )

    def test_rejects_unverified_receipt_seed_workflow_and_timing_drift(self) -> None:
        def batch(index: int, **override) -> list[dict]:
            return [
                self.receipt(candidate, **(override if candidate == index else {}))
                for candidate in range(8)
            ]

        mutations = (
            (batch(0, status="rendering"), "completed"),
            (batch(0, seed=123), "server-derived"),
            (batch(1, worker_id="failover-4090"), "one pinned worker"),
            (
                batch(1, worker_base_url="https://worker.example:8188"),
                "one pinned worker",
            ),
            (batch(1, workflow_sha="d" * 64), "canonical action mode"),
            (batch(1, frame_count=65), "canonical action timing"),
            (batch(1, input_fps=30), "canonical action timing"),
        )
        for receipts, message in mutations:
            with (
                self.subTest(message=message),
                self.fake_ingest_module(),
                self.assertRaisesRegex(plan.BrowserCandidateJobPlanError, message),
            ):
                self.build_production(
                    self.request,
                    trusted_task=self.task,
                    verified_receipts=receipts,
                )

    def test_production_receipts_must_match_taxonomy_output_fps_30(self) -> None:
        for output_fps in (24, 1):
            with (
                self.subTest(output_fps=output_fps),
                self.fake_ingest_module(),
                self.assertRaisesRegex(
                    plan.BrowserCandidateJobPlanError, "canonical action timing"
                ),
            ):
                self.build_production(
                    self.request,
                    trusted_task=self.task,
                    verified_receipts=[
                        self.receipt(index, output_fps=output_fps) for index in range(8)
                    ],
                )

    def test_taxonomy_selects_exact_loop_and_one_shot_workflows_both_directions(
        self,
    ) -> None:
        cases = (
            (
                "walk_forward",
                True,
                "loop",
                "autorig_ltx2_animal_loop_v1_api.json",
                plan.CANONICAL_WORKFLOWS["loop"][1],
            ),
            (
                "attack_primary",
                False,
                "one_shot",
                "autorig_ltx2_animal_one_shot_v1_api.json",
                plan.CANONICAL_WORKFLOWS["one_shot"][1],
            ),
        )
        for semantic_id, loop, mode, workflow_name, workflow_sha in cases:
            clip, prompt_contract, workflow_contract = self.action_context(semantic_id)
            request = {**self.request, "semantic_id": semantic_id}
            receipts = [
                self.receipt(
                    index,
                    semantic_id=semantic_id,
                    generation_mode=mode,
                    prompt_contract=prompt_contract,
                    workflow_name=workflow_name,
                    workflow_sha=workflow_sha,
                    frame_count=clip["frame_profile"],
                )
                for index in range(8)
            ]
            with self.subTest(semantic_id=semantic_id), self.fake_ingest_module():
                built = self.build_production(
                    request,
                    verified_receipts=receipts,
                )
            self.assertEqual(clip["loop"], loop)
            self.assertEqual(built.generation_mode, mode)
            self.assertEqual(built.workflow_name, workflow_contract["workflow_name"])
            self.assertEqual(
                built.workflow_fingerprint,
                workflow_contract["workflow_fingerprint_sha256"],
            )
            self.assertEqual(
                built.config["browser_candidate_ingest"]["generation_mode"], mode
            )

    def test_rejects_action_task_prompt_reference_and_state_provenance_drift(
        self,
    ) -> None:
        mutations: list[tuple[str, dict, str]] = []
        unrelated_action = self.receipt(0)
        unrelated_action["semantic_id"] = "run"
        mutations.append(("semantic", unrelated_action, "action identity"))

        task_drift = self.receipt(0)
        task_drift["task"] = {**self.task, "task_guid": str(uuid.uuid4())}
        mutations.append(("task", task_drift, "task pins"))

        prompt_drift = self.receipt(0)
        prompt_drift["prompt_contract"] = {
            **self.prompt_contract,
            "positive_prompt_sha256": "1" * 64,
        }
        mutations.append(("prompt", prompt_drift, "prompt contract"))

        reference_drift = self.receipt(0)
        other_reference = json.loads(json.dumps(self.reference_manifest))
        other_reference["content"]["reference_artifact"]["sha256"] = "2" * 64
        other_bytes = plan.canonical_json_bytes(other_reference["content"])
        other_reference["pin"]["sha256"] = _sha(other_bytes)
        other_reference["pin"]["bytes"] = len(other_bytes)
        reference_drift["reference_manifest"] = other_reference
        mutations.append(("reference", reference_drift, "reference manifest"))

        for label, bad_receipt, message in mutations:
            receipts = [bad_receipt] + [self.receipt(i) for i in range(1, 8)]
            with (
                self.subTest(label=label),
                self.fake_ingest_module(),
                self.assertRaisesRegex(plan.BrowserCandidateJobPlanError, message),
            ):
                self.build_production(self.request, verified_receipts=receipts)

    def test_latest_state_is_independent_required_and_fabrication_is_rejected(
        self,
    ) -> None:
        receipts = [self.receipt(i) for i in range(8)]
        states = [self.latest_state(receipt) for receipt in receipts]
        with self.assertRaises(TypeError):
            plan.build_production_browser_candidate_job_plan(
                self.request,
                trusted_task=self.task,
                trusted_reference_manifest=self.reference_manifest,
                trusted_retry_authorization=None,
                verified_receipts=receipts,
            )
        fabricated_receipts = json.loads(json.dumps(receipts))
        fabricated_receipts[0]["receipt"] = {
            "path": f"jobs/{receipts[0]['job_id']}/000004.json",
            "sha256": "f" * 64,
            "bytes": 99,
        }
        with (
            self.fake_ingest_module(),
            self.assertRaisesRegex(
                plan.BrowserCandidateJobPlanError,
                "verified_receipt must contain exactly",
            ),
        ):
            self.build_production(
                self.request,
                trusted_latest_states=states,
                verified_receipts=fabricated_receipts,
            )
        missing = states[:-1]
        with (
            self.fake_ingest_module(),
            self.assertRaisesRegex(
                plan.BrowserCandidateJobPlanError, "no independent trusted latest-state"
            ),
        ):
            self.build_production(
                self.request,
                trusted_latest_states=missing,
                verified_receipts=receipts,
            )

    def test_rejects_reference_manifest_task_drift_even_when_repinned(self) -> None:
        wrong = json.loads(json.dumps(self.reference_manifest))
        wrong["content"]["task_guid"] = str(uuid.uuid4())
        encoded = plan.canonical_json_bytes(wrong["content"])
        wrong["pin"]["sha256"] = _sha(encoded)
        wrong["pin"]["bytes"] = len(encoded)
        with self.assertRaisesRegex(
            plan.BrowserCandidateJobPlanError, "exact task pins"
        ):
            self.build_production(
                self.request,
                trusted_reference_manifest=wrong,
                verified_receipts=[self.receipt(i) for i in range(8)],
            )

    def test_rejects_checked_in_prompt_or_workflow_spec_drift(self) -> None:
        for constant, message in (
            ("PROMPT_SPEC_SHA256", "prompt spec SHA-256 drifted"),
            ("WORKFLOW_CONTRACT_SHA256", "workflow contract spec SHA-256 drifted"),
        ):
            with (
                self.subTest(constant=constant),
                patch.object(plan, constant, "0" * 64),
                self.assertRaisesRegex(plan.BrowserCandidateJobPlanError, message),
            ):
                self.build_production(
                    self.request,
                    verified_receipts=[self.receipt(i) for i in range(8)],
                )

    def test_pinned_experiment_spec_is_bound_and_any_drift_is_rejected(self) -> None:
        experiments = [self.experiment_spec(i) for i in range(8)]
        receipts = [
            self.receipt(index, experiment_spec=experiments[index])
            for index in range(8)
        ]
        with self.fake_ingest_module():
            built = self.build_production(self.request, verified_receipts=receipts)
        slots = built.config["browser_candidate_ingest"]["candidate_slots"]
        self.assertEqual(
            [slot["controlled_generation"]["experiment_spec"] for slot in slots],
            experiments,
        )

        drifts: list[tuple[str, dict]] = []
        for field, value in (
            ("base_action_id_string", "run"),
            ("seed_int", 7),
        ):
            changed = json.loads(json.dumps(experiments[0]))
            changed["content"][field] = value
            encoded = plan.canonical_json_bytes(changed["content"])
            changed["pin"]["sha256"] = _sha(encoded)
            changed["pin"]["bytes"] = len(encoded)
            drifts.append((field, changed))
        bad_pin = json.loads(json.dumps(experiments[0]))
        bad_pin["pin"]["sha256"] = "f" * 64
        drifts.append(("pin", bad_pin))
        for label, changed in drifts:
            bad_receipts = [self.receipt(0, experiment_spec=changed)] + [
                self.receipt(i, experiment_spec=experiments[i]) for i in range(1, 8)
            ]
            with (
                self.subTest(label=label),
                self.fake_ingest_module(),
                self.assertRaises(plan.BrowserCandidateJobPlanError),
            ):
                self.build_production(self.request, verified_receipts=bad_receipts)

    def test_production_requires_exact_experiment_and_derived_prompt_id(self) -> None:
        missing = self.receipt(0, experiment_spec=None)
        receipts = [missing] + [self.receipt(i) for i in range(1, 8)]
        with (
            self.fake_ingest_module(),
            self.assertRaisesRegex(
                plan.BrowserCandidateJobPlanError, "requires a pinned experiment"
            ),
        ):
            self.build_production(self.request, verified_receipts=receipts)

        arbitrary = self.receipt(0)
        arbitrary_spec = json.loads(json.dumps(arbitrary["experiment_spec"]))
        arbitrary_spec["content"]["untrusted_extra"] = True
        encoded = plan.canonical_json_bytes(arbitrary_spec["content"])
        arbitrary_spec["pin"]["sha256"] = _sha(encoded)
        arbitrary_spec["pin"]["bytes"] = len(encoded)
        arbitrary["experiment_spec"] = arbitrary_spec
        arbitrary["experiment_sha256"] = arbitrary_spec["pin"]["sha256"]
        receipts = [arbitrary] + [self.receipt(i) for i in range(1, 8)]
        with (
            self.fake_ingest_module(),
            self.assertRaisesRegex(
                plan.BrowserCandidateJobPlanError,
                "experiment_spec.content must contain exactly",
            ),
        ):
            self.build_production(self.request, verified_receipts=receipts)

        wrong_prompt = self.receipt(0, prompt_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")
        receipts = [wrong_prompt] + [self.receipt(i) for i in range(1, 8)]
        with (
            self.fake_ingest_module(),
            self.assertRaisesRegex(
                plan.BrowserCandidateJobPlanError, "prompt_id is not derived"
            ),
        ):
            self.build_production(self.request, verified_receipts=receipts)
        wrong_experiment = self.receipt(0)
        wrong_experiment["experiment_id"] = "unrelated_experiment_v1"
        receipts = [wrong_experiment] + [self.receipt(i) for i in range(1, 8)]
        with (
            self.fake_ingest_module(),
            self.assertRaisesRegex(
                plan.BrowserCandidateJobPlanError,
                "experiment identity does not match",
            ),
        ):
            self.build_production(self.request, verified_receipts=receipts)
        self.assertEqual(
            plan.derive_controlled_prompt_id(plan.V14_CONTROLLED_JOB_ID),
            plan.V14_PROMPT_ID,
        )

    def test_taxonomy_contract_drift_is_rejected(self) -> None:
        canonical = json.loads(
            (BACKEND / "animal_animation_taxonomy.v1.json").read_text(encoding="utf-8")
        )
        invalid_variants = (
            {**canonical, "schema": "animal-animation-taxonomy.v2"},
            {**canonical, "revision": "animal-base-30-v2"},
            {**canonical, "source_fps": 30},
            {**canonical, "output_fps": 24},
            {**canonical, "rig_types": [*canonical["rig_types"], "unicorn"]},
            {
                **canonical,
                "clips": [
                    {**row, "frame_profile": 0}
                    if row.get("id") == "walk_forward"
                    else row
                    for row in canonical["clips"]
                ],
            },
        )
        for invalid in invalid_variants:
            with self.subTest(
                schema=invalid.get("schema"), output=invalid.get("output_fps")
            ):
                with patch.object(
                    plan.Path,
                    "read_text",
                    return_value=json.dumps(invalid),
                ):
                    with self.assertRaises(plan.BrowserCandidateJobPlanError):
                        plan._load_taxonomy_clip("walk_forward")
        with patch.object(
            plan.Path,
            "read_text",
            return_value=json.dumps(canonical, indent=7, ensure_ascii=False),
        ):
            clip, output_fps = plan._load_taxonomy_clip("walk_forward")
        self.assertEqual(clip["id"], "walk_forward")
        self.assertEqual(output_fps, 30)

    def test_rejects_duplicate_generation_and_unsafe_server_paths(self) -> None:
        duplicate = self.receipt(1)
        duplicate["job_id"] = self.receipt(0)["job_id"]
        duplicate["source_video"] = self.receipt(0)["source_video"]
        with (
            self.fake_ingest_module(),
            self.assertRaisesRegex(plan.BrowserCandidateJobPlanError, "unique jobs"),
        ):
            self.build_production(
                self.request,
                trusted_task=self.task,
                verified_receipts=[self.receipt(0), duplicate]
                + [self.receipt(index) for index in range(2, 8)],
            )
        canonical = self.v14_receipt()
        latest = self.latest_state(canonical)
        unsafe = json.loads(json.dumps(canonical))
        unsafe["receipt"] = {
            **unsafe["receipt"],
            "path": ".",
        }
        with self.assertRaisesRegex(
            plan.BrowserCandidateJobPlanError, "canonical relative server path"
        ):
            plan.build_v14_nonproduction_canary_job_plan(
                trusted_task=self.task,
                trusted_latest_state=latest,
                verified_receipt=unsafe,
            )

    def test_v14_v1_canary_is_fixed_seed_and_explicitly_nonproduction(self) -> None:
        receipt = self.v14_receipt()
        first = self.build_v14(receipt)
        second = self.build_v14(receipt)
        self.assertEqual(first.config_json, second.config_json)
        self.assertEqual(first.schema, plan.PLAN_IDENTITY_SCHEMA_V1)
        self.assertFalse(first.production_eligible)
        self.assertEqual(first.selection_mode, "canary_single_candidate")
        self.assertEqual((first.candidate_target, first.candidate_limit), (1, 1))
        config = first.config
        ingest = config["browser_candidate_ingest"]
        self.assertEqual(ingest["schema"], plan.INGEST_SCHEMA_V1)
        self.assertEqual(ingest["candidate_seed"], plan.V14_FIXED_SEED)
        self.assertEqual(
            config["browser_candidate_selection"]["mode"],
            "canary_single_candidate",
        )
        self.assertFalse(config["browser_candidate_job_plan"]["production_eligible"])

    def test_v14_receipt_uses_authoritative_controlled_store_layout(self) -> None:
        receipt = self.v14_receipt()
        latest = self.latest_state(receipt)
        video_sha = receipt["source_video"]["sha256"]
        self.assertEqual(
            receipt["source_video"]["path"],
            f"raw/{video_sha[:2]}/{video_sha}.mp4",
        )
        self.assertEqual(
            receipt["receipt"]["path"],
            f"jobs/{plan.V14_CONTROLLED_JOB_ID}/000003.json",
        )
        built = self.build_v14(receipt)
        self.assertEqual(
            built.config["browser_candidate_ingest"]["source_video"],
            receipt["source_video"],
        )
        legacy_nested = {
            **receipt,
            "source_video": {
                **receipt["source_video"],
                "path": (
                    f"controlled-generation/{plan.V14_CONTROLLED_JOB_ID}/"
                    f"raw/{video_sha[:2]}/{video_sha}.mp4"
                ),
            },
        }
        with self.assertRaisesRegex(
            plan.BrowserCandidateJobPlanError,
            "raw/<sha-prefix>/<sha>.mp4",
        ):
            plan.build_v14_nonproduction_canary_job_plan(
                trusted_task=self.task,
                trusted_latest_state=self.latest_state(receipt),
                verified_receipt=legacy_nested,
            )
        fabricated = json.loads(json.dumps(receipt))
        fabricated["receipt"]["path"] = fabricated["receipt"]["path"].replace(
            "000003.json", "000004.json"
        )
        fabricated["receipt"]["sha256"] = "f" * 64
        with self.assertRaisesRegex(
            plan.BrowserCandidateJobPlanError, "authoritative trusted latest-state"
        ):
            plan.build_v14_nonproduction_canary_job_plan(
                trusted_task=self.task,
                trusted_latest_state=latest,
                verified_receipt=fabricated,
            )

    def test_v14_pins_match_both_authoritative_repo_contracts(self) -> None:
        tools = BACKEND.parent / "tools" / "animation_fitting"
        author = (tools / "author_v14_browser_fitting_spec.mjs").read_text(
            encoding="utf-8"
        )
        runner = (tools / "run_v14_browser_fitting_pipeline.mjs").read_text(
            encoding="utf-8"
        )
        shared = (
            plan.V14_CONTROLLED_JOB_ID,
            plan.V14_PROMPT_ID,
            plan.V14_EXPERIMENT_ID,
            plan.V14_EXPERIMENT_SHA256,
            plan.V14_WORKER_ID,
            plan.V14_WORKER_BASE_URL,
            plan.V14_WORKFLOW_NAME,
            plan.V14_WORKFLOW_FINGERPRINT_SHA256,
        )
        for value in shared:
            with self.subTest(value=value):
                self.assertIn(value, author)
                self.assertIn(value, runner)
        self.assertIn(plan.V14_SOURCE_MODEL_SHA256, author)
        self.assertIn(plan.V14_SOURCE_SKELETON_SHA256, author)
        contract_match = re.search(
            r"export const REAL_V14_CONTRACT = Object\.freeze\(\{(?P<body>.*?)\n\}\);",
            author,
            re.DOTALL,
        )
        self.assertIsNotNone(contract_match)
        contract = contract_match.group("body")

        def exact_integer(field: str, *, bigint: bool = False) -> int:
            suffix = "n" if bigint else ""
            match = re.search(
                rf"^\s*{field}:\s*(\d+){suffix},\s*$", contract, re.MULTILINE
            )
            self.assertIsNotNone(match, field)
            return int(match.group(1))

        self.assertEqual(exact_integer("seed", bigint=True), plan.V14_FIXED_SEED)
        self.assertEqual(exact_integer("frameCount"), 49)
        self.assertEqual(exact_integer("outputFps"), 30)
        self.assertEqual(
            plan.derive_controlled_prompt_id(plan.V14_CONTROLLED_JOB_ID),
            plan.V14_PROMPT_ID,
        )

    def test_v14_rejects_any_identity_drift(self) -> None:
        valid = self.v14_receipt()
        wrong_job_id = "9" * 64
        wrong_job = {**valid, "job_id": wrong_job_id}
        wrong_job["receipt"] = {
            **valid["receipt"],
            "path": valid["receipt"]["path"].replace(
                plan.V14_CONTROLLED_JOB_ID, wrong_job_id
            ),
        }
        mutations = (
            {**valid, "seed": plan.V14_FIXED_SEED + 1},
            wrong_job,
            {**valid, "prompt_id": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"},
            {**valid, "experiment_id": "horse_walk_v13"},
            {**valid, "experiment_sha256": "f" * 64},
            {**valid, "worker_id": "failover-4090"},
            {**valid, "worker_base_url": "http://127.0.0.1:8288"},
            {**valid, "workflow_name": "autorig_ltx2_animal_one_shot_v1_api.json"},
            {**valid, "workflow_fingerprint_sha256": "8" * 64},
            {**valid, "frame_count": 65},
            {**valid, "input_fps": 30},
            {**valid, "output_fps": 24},
        )
        for receipt in mutations:
            with self.assertRaisesRegex(
                plan.BrowserCandidateJobPlanError,
                "exact immutable V14 runtime identity",
            ):
                plan.build_v14_nonproduction_canary_job_plan(
                    trusted_task=self.task,
                    trusted_latest_state=self.latest_state(valid),
                    verified_receipt=receipt,
                )
        wrong_rig = dict(self.task)
        wrong_rig["source_rig_type"] = "DOG_1"
        with self.assertRaisesRegex(plan.BrowserCandidateJobPlanError, "HORSE_2"):
            plan.build_v14_nonproduction_canary_job_plan(
                trusted_task=wrong_rig,
                trusted_latest_state=self.latest_state(valid),
                verified_receipt=valid,
            )
        for field, value in (
            ("source_model_sha256", "1" * 64),
            ("source_skeleton_sha256", "2" * 64),
        ):
            wrong_source = dict(self.task)
            wrong_source[field] = value
            with (
                self.subTest(field=field),
                self.assertRaisesRegex(
                    plan.BrowserCandidateJobPlanError,
                    "exact canonical V14 model and skeleton",
                ),
            ):
                plan.build_v14_nonproduction_canary_job_plan(
                    trusted_task=wrong_source,
                    trusted_latest_state=self.latest_state(valid),
                    verified_receipt=valid,
                )


if __name__ == "__main__":
    unittest.main()
