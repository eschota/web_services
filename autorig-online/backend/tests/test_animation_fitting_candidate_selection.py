from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
import unittest.mock
import uuid


def _canonical(value) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        + b"\n"
    )


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _pin(payload: bytes, filename: str) -> dict:
    return {"filename": filename, "bytes": len(payload), "sha256": _sha(payload)}


class BrowserCandidateSelectionTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory(
            prefix="autorig-browser-candidate-selection-"
        )
        cls.root = Path(cls._tmp.name)
        cls.jobs_root = cls.root / "jobs"
        cls.library_root = cls.root / "library"
        cls.jobs_root.mkdir()
        cls.library_root.mkdir()
        os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{cls.root / 'test.db'}"
        os.environ["ANIMATION_FITTING_JOBS_ROOT"] = str(cls.jobs_root)
        os.environ["ANIMATION_LIBRARY_ROOT"] = str(cls.library_root)
        backend = str(Path(__file__).resolve().parents[1])
        if backend not in sys.path:
            sys.path.insert(0, backend)
        for name in (
            "animation_fitting_candidate_selection",
            "animation_fitting_candidate_review",
            "animation_fitting_candidate_ingest",
            "animation_fitting_candidate_job_plan",
            "animal_animation_library",
            "database",
            "config",
        ):
            sys.modules.pop(name, None)
        cls.database = importlib.import_module("database")
        cls.ingest = importlib.import_module("animation_fitting_candidate_ingest")
        cls.job_plan = importlib.import_module(
            "animation_fitting_candidate_job_plan"
        )
        cls.review = importlib.import_module("animation_fitting_candidate_review")
        cls.library = importlib.import_module("animal_animation_library")
        cls.selection = importlib.import_module(
            "animation_fitting_candidate_selection"
        )
        qa = json.loads(cls.selection.QA_PROFILE_PATH.read_text(encoding="utf-8"))
        qa["calibration_state_string"] = "production-horse-v1"
        cls.production_qa_path = cls.root / "qa-profile-production.v1.json"
        cls.production_qa_path.write_text(
            json.dumps(qa, indent=2) + "\n", encoding="utf-8"
        )

    @classmethod
    def tearDownClass(cls):
        asyncio.run(cls.database.engine.dispose())
        cls._tmp.cleanup()

    async def asyncSetUp(self):
        await self.database.init_db()
        self.task_id = str(uuid.uuid4())
        self.task_guid = str(uuid.uuid4())
        self.workflow_sha = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
        self.skeleton_sha = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
        self.model_sha = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
        self.workflow_name = "autorig_ltx2_animal_loop_v1_api.json"
        self.worker_id = "local-4090"
        self.worker_url = "http://127.0.0.1:8188"
        self.production_task = {
            "schema": self.job_plan.TRUSTED_TASK_PINS_SCHEMA,
            "task_id": self.task_id,
            "task_guid": self.task_guid,
            "status": "done",
            "input_type": "animal",
            "source_rig_type": "HORSE_2",
            "source_model_sha256": self.model_sha,
            "source_skeleton_sha256": self.skeleton_sha,
        }
        clip, _ = self.job_plan._load_taxonomy_clip("walk_forward")
        (
            self.production_prompt_contract,
            self.production_workflow_contract,
        ) = self.job_plan._canonical_prompt_and_workflow_contract(
            semantic_id="walk_forward", clip=clip, species="horse"
        )
        reference_artifact = b"trusted-selection-reference-rgb"
        reference_path = (
            self.jobs_root / "references" / self.task_id / "reference_rgb.png"
        )
        reference_path.parent.mkdir(parents=True, exist_ok=True)
        reference_path.write_bytes(reference_artifact)
        reference_content = {
            "schema": self.job_plan.REFERENCE_MANIFEST_SCHEMA,
            "task_id": self.task_id,
            "task_guid": self.task_guid,
            "source_rig_type": "HORSE_2",
            "species": "horse",
            "source_model_sha256": self.model_sha,
            "source_skeleton_sha256": self.skeleton_sha,
            "actionless": True,
            "geometry_uv_normals_mutated": False,
            "reference_artifact": {
                "path": f"references/{self.task_id}/reference_rgb.png",
                "sha256": _sha(reference_artifact),
                "bytes": len(reference_artifact),
            },
        }
        reference_bytes = self.job_plan.canonical_json_bytes(reference_content)
        reference_sha = _sha(reference_bytes)
        self.production_reference_manifest = {
            "content": reference_content,
            "pin": {
                "path": (
                    f"reference-manifests/{self.task_id}/{reference_sha}.json"
                ),
                "sha256": reference_sha,
                "bytes": len(reference_bytes),
            },
        }
        manifest_path = (
            self.jobs_root / self.production_reference_manifest["pin"]["path"]
        )
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_bytes(reference_bytes)
        self.production_latest_states = {}
        async with self.database.AsyncSessionLocal() as db:
            db.add(
                self.database.Task(
                    id=self.task_id,
                    owner_type="user",
                    owner_id="owner@example.com",
                    guid=self.task_guid,
                    status="done",
                    input_type="animal",
                )
            )
            await db.commit()

    def _generation(self, index, *, with_worker=True):
        result = {
            "job_id": hashlib.sha256(f"generation-{index}".encode()).hexdigest(),
            "prompt_id": f"prompt-{index}",
            "experiment_id": "horse-walk-selection-test",
            "experiment_sha256": "d" * 64,
            "workflow_fingerprint_sha256": self.workflow_sha,
        }
        if with_worker:
            result.update(
                {"worker_id": self.worker_id, "worker_base_url": self.worker_url}
            )
        return result

    def _planned_video(self, index):
        payload = f"planned-source-video-{index}".encode()
        return {
            "path": f"raw/{_sha(payload)[:2]}/{_sha(payload)}.mp4",
            "sha256": _sha(payload),
            "bytes": len(payload),
        }

    def _experiment_spec(self, index, seed, batch_nonce):
        prompt_doc = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "animation_fitting"
                / "specs"
                / "action_prompts.v1.json"
            ).read_text(encoding="utf-8")
        )
        prompt_row = next(
            row
            for row in prompt_doc["actions_array"]
            if row["action_id_string"] == "walk_forward"
        )
        positive = self.job_plan._render_prompt(
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
        negative = self.job_plan._render_prompt(
            prompt_doc["common_negative_prompt_string"],
            "horse",
            "negative prompt",
        )
        experiment_id = f"horse_walk_selection_{batch_nonce}_{index}_v1"
        content = {
            "schema": "autorig.animation-fitting-experiment.v1",
            "experiment_id_string": experiment_id,
            "base_action_id_string": "walk_forward",
            "species_string": "horse",
            "generation_mode_string": "loop",
            "frame_count_int": 49,
            "input_fps_int": 24,
            "output_fps_int": 30,
            "seed_int": seed,
            "positive_prompt_string": positive,
            "negative_prompt_string": negative,
            "reference_object": {
                "immutable_manifest_sha256_string": (
                    self.production_reference_manifest["pin"]["sha256"]
                ),
                "source_model_sha256_string": self.model_sha,
            },
            "workflow_object": {
                "workflow_name_string": self.production_workflow_contract[
                    "workflow_name"
                ],
                "workflow_fingerprint_sha256_string": (
                    self.production_workflow_contract[
                        "workflow_fingerprint_sha256"
                    ]
                ),
            },
        }
        payload = self.job_plan.canonical_json_bytes(content)
        digest = _sha(payload)
        wrapper = {
            "content": content,
            "pin": {
                "path": f"animation_fitting/specs/experiments/{experiment_id}.json",
                "sha256": digest,
                "bytes": len(payload),
            },
        }
        path = self.jobs_root / wrapper["pin"]["path"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        return wrapper

    def _verified_generation_receipt(self, index, batch_nonce):
        job_id = hashlib.sha256(
            f"generation-{batch_nonce}-{index}".encode()
        ).hexdigest()
        seed = self.selection.derive_candidate_seed(
            self.task_id, "walk_forward", index
        )
        experiment_spec = self._experiment_spec(index, seed, batch_nonce)
        state_payload = json.dumps(
            {
                "schema": self.job_plan.CONTROLLED_STATE_SCHEMA,
                "sequence_int": 3,
                "job_id_string": job_id,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        state_path = self.jobs_root / "jobs" / job_id / "000003.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_bytes(state_payload)
        self.production_latest_states[job_id] = {
            "schema": self.job_plan.TRUSTED_LATEST_STATE_SCHEMA,
            "status": "completed",
            "latest": True,
            "job_id": job_id,
            "state_schema": self.job_plan.CONTROLLED_STATE_SCHEMA,
            "sequence": 3,
            "filename": "000003.json",
            "pin": {
                "path": f"jobs/{job_id}/000003.json",
                "sha256": _sha(state_payload),
                "bytes": len(state_payload),
            },
        }
        return {
            "schema": self.job_plan.CONTROLLED_RECEIPT_SCHEMA_V2,
            "status": "completed",
            "candidate_index": index,
            "seed": seed,
            "job_id": job_id,
            "prompt_id": self.job_plan.derive_controlled_prompt_id(job_id),
            "semantic_id": "walk_forward",
            "generation_mode": "loop",
            "task": self.production_task,
            "prompt_contract": self.production_prompt_contract,
            "reference_manifest": self.production_reference_manifest,
            "experiment_id": experiment_spec["content"]["experiment_id_string"],
            "experiment_sha256": experiment_spec["pin"]["sha256"],
            "experiment_spec": experiment_spec,
            "worker_id": self.worker_id,
            "worker_base_url": self.worker_url,
            "workflow_name": self.production_workflow_contract["workflow_name"],
            "workflow_fingerprint_sha256": self.production_workflow_contract[
                "workflow_fingerprint_sha256"
            ],
            "frame_count": 49,
            "input_fps": 24,
            "output_fps": 30,
            "source_video": self._planned_video(index),
        }

    def _production_trust(self, receipts):
        return self.ingest.BrowserCandidatePlanTrust(
            reference_manifest=self.production_reference_manifest,
            latest_states=tuple(
                self.production_latest_states[row["job_id"]] for row in receipts
            ),
            retry_authorization=None,
        )

    async def _job(self, *, target=8, limit=16, mode="production"):
        job_id = str(uuid.uuid4())
        if mode == "canary_single_candidate":
            target, limit = 1, 1
            workflow_name = self.workflow_name
            workflow_fingerprint = self.workflow_sha
            prompt_id = uuid.uuid4().hex
            config = {
                "browser_candidate_ingest": {
                    "schema": self.ingest.JOB_BINDING_SCHEMA,
                    "task_id": self.task_id,
                    "task_guid": self.task_guid,
                    "source_rig_type": "HORSE_2",
                    "source_model_sha256": self.model_sha,
                    "source_skeleton_sha256": self.skeleton_sha,
                    "frame_count": 49,
                    "output_fps": 30,
                    "candidate_seed": self.selection.derive_candidate_seed(
                        self.task_id, "walk_forward", 0
                    ),
                    "source_video": self._planned_video(0),
                    "controlled_generation": self._generation(
                        0, with_worker=False
                    ),
                },
                "browser_candidate_selection": {
                    "schema": self.selection.SELECTION_CONFIG_SCHEMA,
                    "mode": mode,
                },
            }
            with_worker = False
            trust = None
        else:
            if (target, limit) != (8, 16):
                raise AssertionError("production tests must use target=8 limit=16")
            batch_nonce = uuid.uuid4().hex
            receipts = [
                self._verified_generation_receipt(index, batch_nonce)
                for index in range(8)
            ]
            trust = self._production_trust(receipts)
            plan = self.job_plan.build_production_browser_candidate_job_plan(
                {
                    "schema": self.job_plan.PLAN_REQUEST_SCHEMA,
                    "semantic_id": "walk_forward",
                    "candidate_target": 8,
                    "candidate_limit": 16,
                },
                trusted_task=self.production_task,
                trusted_reference_manifest=self.production_reference_manifest,
                trusted_latest_states=trust.latest_states,
                trusted_retry_authorization=None,
                verified_receipts=receipts,
            )
            config = plan.config
            workflow_name = plan.workflow_name
            workflow_fingerprint = plan.workflow_fingerprint
            prompt_id = plan.prompt_id
            with_worker = True
        async with self.database.AsyncSessionLocal() as db:
            version = self.database.AnimalAnimationLibraryVersion(
                rig_type="horse",
                revision=f"horse-selection-{uuid.uuid4().hex}",
                status="draft",
                template_skeleton_sha256=self.skeleton_sha,
                qa_profile_revision="horse-qa-production-v1",
                created_by="admin@example.com",
            )
            db.add(version)
            await db.flush()
            job = self.database.AnimalAnimationFittingJob(
                id=job_id,
                library_version_id=version.id,
                rig_type="horse",
                semantic_id="walk_forward",
                status="review",
                workflow_name=workflow_name,
                workflow_fingerprint=workflow_fingerprint,
                worker_url=self.worker_url,
                prompt_id=prompt_id,
                prompt="horse walk",
                candidate_target=target,
                candidate_limit=limit,
                config_json=json.dumps(config),
                created_by="admin@example.com",
            )
            db.add(job)
            await db.commit()
            version_id = version.id
            revision = version.revision
        async with self.database.AsyncSessionLocal() as db:
            snapshot = await self.selection._load_job(
                db,
                job_id,
                fitting_jobs_root=str(self.jobs_root),
                trusted_plan_inputs=trust,
            )
        return {
            "id": job_id,
            "version_id": version_id,
            "revision": revision,
            "target": target,
            "limit": limit,
            "mode": mode,
            "workflow_name": workflow_name,
            "workflow_fingerprint": workflow_fingerprint,
            "with_worker": with_worker,
            "config": config,
            "trust": trust,
            "human_review_lifecycle_binding_sha256": (
                snapshot.human_review_lifecycle_binding_sha256
            ),
        }

    def _bundle(self, job, index, *, forged_rank=99):
        seed = self.selection.derive_candidate_seed(
            self.task_id, "walk_forward", index
        )
        artifacts = {}
        files = {}
        for name in self.review.UPLOAD_ARTIFACT_NAMES:
            payload = (
                f"planned-source-video-{index}".encode()
                if name == "source-video.mp4"
                else f"candidate-{index}-{name}-{uuid.uuid4().hex}".encode()
            )
            files[name] = payload
            artifacts[name] = _pin(payload, name)
        manifest = {
            "schema": self.ingest.BUNDLE_SCHEMA,
            "library": {
                "version_id": job["version_id"],
                "revision": job["revision"],
                "rig_type": "horse",
                "template_skeleton_sha256": self.skeleton_sha,
            },
            "fitting_job": {
                "id": job["id"],
                "semantic_id": "walk_forward",
                "workflow_name": job["workflow_name"],
                "workflow_fingerprint": job["workflow_fingerprint"],
            },
            "source_task": {"id": self.task_id, "guid": self.task_guid},
            "candidate": {
                "candidate_index": index,
                "seed": seed,
                "frame_count": 49,
                **({"input_fps": 24} if job["with_worker"] else {}),
                "fps": 30,
                "duration_seconds": 1.6,
                "review_state": "uploaded_pending_server_validation",
                "uploaded_qa_assertions_trusted": False,
                "server_validation": {
                    "status": "pending",
                    "required": [
                        "task_model_sha256_binding",
                        "task_skeleton_sha256_binding",
                        "media_decode_and_phase_extraction",
                        "deformation_recompute",
                        "visual_review",
                    ],
                },
                # These browser-controlled fields must have no selection effect.
                "rank": forged_rank,
                "rank_score": 999.0,
                "metrics": {"prompt_alignment_float": 1.0},
            },
            "controlled_generation": (
                next(
                    slot
                    for slot in job["config"]["browser_candidate_ingest"].get(
                        "candidate_slots", ()
                    )
                    if slot["candidate_index"] == index
                )["controlled_generation"]
                if job["with_worker"]
                else self._generation(index, with_worker=False)
            ),
            "artifacts": artifacts,
        }
        unsigned = dict(manifest)
        identity = _sha(
            json.dumps(
                unsigned,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        )
        manifest["identity_sha256"] = identity
        manifest_bytes = _canonical(manifest)
        directory = (
            self.jobs_root
            / job["id"]
            / "browser-candidates"
            / identity[:2]
            / identity
        )
        directory.mkdir(parents=True)
        for name, payload in files.items():
            (directory / name).write_bytes(payload)
        (directory / "candidate-manifest.json").write_bytes(manifest_bytes)
        return {
            "identity": identity,
            "seed": seed,
            "manifest": manifest,
            "manifest_bytes": manifest_bytes,
        }

    def _validation(self, job, bundle, metrics):
        candidate_identity = bundle["identity"]
        manifest_pin = _pin(
            bundle["manifest_bytes"], "candidate-manifest.json"
        )
        evidence = {
            name: f"trusted-{name}-{candidate_identity}".encode()
            for name in self.review.SERVER_EVIDENCE_NAMES
        }
        phase_pins = {
            phase: _pin(evidence[f"phase-{phase}.png"], f"phase-{phase}.png")
            for phase in self.ingest.PHASES
        }
        camera_pin = _pin(evidence["camera-settings.json"], "camera-settings.json")
        deformation_pin = _pin(
            evidence["deformation-report.json"], "deformation-report.json"
        )
        metrics = dict(metrics)
        metrics["visual_phase_gate"] = {
            "schema": "autorig.animation-visual-phase-qa.v1",
            "version": 1,
            "rig_type": "horse",
            "semantic_id": "walk_forward",
            "fitted_clip_sha256": bundle["manifest"]["artifacts"][
                "three-clip.json"
            ]["sha256"],
            "decision": None,
            "camera": {
                "static": True,
                "projection": "perspective",
                "view": "fixed-side",
                "root_motion_locked": True,
                "settings_sha256": camera_pin["sha256"],
            },
            "coincident_rest_vertex_separation": {
                "measured": True,
                "pass": True,
                "threshold_m": 0.04,
                "max_separation_m": 0.01,
                "sample_count": 12,
                "group_count": 3,
                "report_url": "https://autorig.online/evidence/deformation.json",
                "report_sha256": deformation_pin["sha256"],
            },
            "required_phases": list(self.ingest.PHASES),
            "frames": [
                {
                    "phase": phase,
                    "frame_index": frame_index,
                    "evidence_url": f"https://autorig.online/evidence/{phase}.png",
                    "sha256": phase_pins[phase]["sha256"],
                }
                for phase, frame_index in zip(self.ingest.PHASES, (0, 24, 36))
            ],
            "reviewer": {"id": None, "reviewed_at": None},
        }
        metrics_bytes = _canonical(metrics)
        runtime_pins = {
            name: {
                "filename": name,
                "bytes": 10 + offset,
                "sha256": hashlib.sha256(f"runtime-{name}".encode()).hexdigest(),
            }
            for offset, name in enumerate(
                ("node", "chrome", "ffmpeg", "ffprobe", "three_module")
            )
        }
        runtime_pins["three_module"]["sha256"] = "a" * 64
        binding = {
            "schema": self.review.SERVER_VALIDATION_SCHEMA,
            "candidate": {
                "identity_sha256": candidate_identity,
                "manifest": manifest_pin,
                "uploaded_qa_assertions_trusted": False,
                "uploaded_qa_artifact_used": False,
            },
            "lifecycle": {
                "binding_sha256": job[
                    "human_review_lifecycle_binding_sha256"
                ],
                "job_id": job["id"],
                "library_version_id": job["version_id"],
                "library_revision": job["revision"],
                "rig_type": "horse",
                "semantic_id": "walk_forward",
                "source_task": {"id": self.task_id, "guid": self.task_guid},
            },
            "task_artifacts": {
                "task_model": {
                    "filename": "prepared.glb",
                    "bytes": 100,
                    "sha256": hashlib.sha256(b"model").hexdigest(),
                },
                "task_skeleton": {
                    "filename": "skeleton.json",
                    "bytes": 50,
                    "sha256": self.skeleton_sha,
                },
            },
            "candidate_artifacts": {
                "three_clip": bundle["manifest"]["artifacts"]["three-clip.json"],
                "source_video": bundle["manifest"]["artifacts"]["source-video.mp4"],
            },
            "trusted_qa": {
                "runner": {"name": "selection-test", "revision": "v1"},
                "runtime": {
                    "three_revision": "160",
                    "three_expected_sha256": "a" * 64,
                    "artifacts": runtime_pins,
                },
                "status": "PASS",
                "metrics": _pin(metrics_bytes, "server-qa-metrics.json"),
                "evidence": {
                    name: _pin(payload, name) for name, payload in evidence.items()
                },
            },
        }
        validation_identity = _sha(
            json.dumps(
                binding,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        )
        receipt = {**binding, "identity_sha256": validation_identity}
        receipt_bytes = _canonical(receipt)
        directory = (
            self.jobs_root
            / job["id"]
            / "browser-candidate-reviews"
            / candidate_identity[:2]
            / candidate_identity
            / "server-validations"
            / validation_identity
        )
        directory.mkdir(parents=True)
        for name, payload in evidence.items():
            (directory / name).write_bytes(payload)
        (directory / "server-qa-metrics.json").write_bytes(metrics_bytes)
        (directory / "server-validation-receipt.json").write_bytes(receipt_bytes)
        return {"identity": validation_identity, "directory": directory}

    def _human_review_package(self, job, bundle, outcome, *, decision="PASS"):
        candidate_identity = bundle["identity"]
        validation_identity = outcome.receipt["server_validation"][
            "identity_sha256"
        ]
        validation_path = (
            self.jobs_root
            / job["id"]
            / "browser-candidate-reviews"
            / candidate_identity[:2]
            / candidate_identity
            / "server-validations"
            / validation_identity
            / "server-validation-receipt.json"
        )
        validation_bytes = validation_path.read_bytes()
        validation = json.loads(validation_bytes)
        manifest_pin = _pin(
            bundle["manifest_bytes"], "candidate-manifest.json"
        )
        review_binding = {
            "schema": self.review.HUMAN_REVIEW_SCHEMA,
            "candidate": {
                "identity_sha256": candidate_identity,
                "manifest": manifest_pin,
            },
            "server_validation": {
                "identity_sha256": validation_identity,
                "receipt": _pin(
                    validation_bytes, "server-validation-receipt.json"
                ),
                "trusted_qa_metrics": validation["trusted_qa"]["metrics"],
            },
            "lifecycle_binding_sha256": validation["lifecycle"][
                "binding_sha256"
            ],
            "review": {
                "decision": decision,
                "reviewer_id": "admin@example.com",
                "reviewed_at": "2026-07-16T12:30:00+07:00",
                "reason": (
                    None if decision == "PASS" else f"Operator {decision.lower()}"
                ),
            },
        }
        review_identity = _sha(
            json.dumps(
                review_binding,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        )
        review = {**review_binding, "identity_sha256": review_identity}
        review_bytes = _canonical(review)
        review_pin = _pin(review_bytes, "human-review-receipt.json")
        directory = (
            self.jobs_root
            / job["id"]
            / "browser-candidate-reviews"
            / candidate_identity[:2]
            / candidate_identity
            / "human-review"
        )
        directory.mkdir()
        (directory / "human-review-receipt.json").write_bytes(review_bytes)
        if decision != "PASS":
            return None
        candidate_id = str(
            uuid.uuid5(
                self.review.PACKAGE_NAMESPACE,
                f"{job['id']}:{bundle['seed']}:{manifest_pin['sha256']}",
            )
        )
        descriptor = {
            "schema": self.review.PACKAGE_DESCRIPTOR_SCHEMA,
            "package_id": candidate_id,
            "candidate_id": candidate_id,
            "candidate_bundle_sha256": manifest_pin["sha256"],
            "human_review_sha256": review_pin["sha256"],
            "semantic_id": "walk_forward",
            "clip": bundle["manifest"]["artifacts"]["three-clip.json"],
            "review_identity_sha256": review_identity,
            "library": bundle["manifest"]["library"],
            "fitting_job": bundle["manifest"]["fitting_job"],
            "source_task": bundle["manifest"]["source_task"],
            "candidate_identity_sha256": candidate_identity,
            "server_validation_identity_sha256": validation_identity,
            "review": review["review"],
            "pins": {
                "candidate_manifest": manifest_pin,
                "three_clip": bundle["manifest"]["artifacts"]["three-clip.json"],
                "task_model": validation["task_artifacts"]["task_model"],
                "task_skeleton": validation["task_artifacts"]["task_skeleton"],
                "server_validation_receipt": _pin(
                    validation_bytes, "server-validation-receipt.json"
                ),
                "server_qa_metrics": validation["trusted_qa"]["metrics"],
                "human_review_receipt": review_pin,
            },
        }
        (directory / "package-descriptor.json").write_bytes(
            _canonical(descriptor)
        )
        return candidate_id

    def _passing_metrics(self, score):
        qa = json.loads(self.production_qa_path.read_text(encoding="utf-8"))
        result = {key: True for key in qa["hard_gate_metric_keys_array"]}
        result.update({key: True for key in qa["loop_hard_gate_metric_keys_array"]})
        result.update({key: score for key in qa["ranking_weights_object"]})
        return result

    async def _admit_and_validate(self, job, index, score, *, forged_rank=99):
        bundle = self._bundle(job, index, forged_rank=forged_rank)
        validation = self._validation(job, bundle, self._passing_metrics(score))
        async with self.database.AsyncSessionLocal() as db:
            admission = await self.selection.admit_browser_candidate(
                db,
                job_id=job["id"],
                candidate_index=index,
                candidate_identity_sha256=bundle["identity"],
                fitting_jobs_root=str(self.jobs_root),
                trusted_plan_inputs=job["trust"],
            )
        async with self.database.AsyncSessionLocal() as db:
            outcome = await self.selection.record_candidate_validation_outcome(
                db,
                job_id=job["id"],
                candidate_identity_sha256=bundle["identity"],
                server_validation_identity_sha256=validation["identity"],
                fitting_jobs_root=str(self.jobs_root),
                trusted_plan_inputs=job["trust"],
            )
        return bundle, admission, outcome

    async def _snapshot(self, job):
        async with self.database.AsyncSessionLocal() as db:
            return await self.selection.create_candidate_selection_snapshot(
                db,
                job_id=job["id"],
                fitting_jobs_root=str(self.jobs_root),
                trusted_plan_inputs=job["trust"],
            )

    async def _close(self, job):
        async with self.database.AsyncSessionLocal() as db:
            return await self.selection.close_candidate_generation(
                db,
                job_id=job["id"],
                fitting_jobs_root=str(self.jobs_root),
                trusted_plan_inputs=job["trust"],
            )

    async def _finalize(self, job, expected=None):
        async with self.database.AsyncSessionLocal() as db:
            return await self.selection.finalize_candidate_selection(
                db,
                job_id=job["id"],
                admin_email="admin@example.com",
                expected_snapshot_identity_sha256=expected,
                fitting_jobs_root=str(self.jobs_root),
                trusted_plan_inputs=job["trust"],
            )

    async def test_deterministic_server_ranking_ignores_uploaded_rank_and_metrics(self):
        job = await self._job()
        with unittest.mock.patch.object(
            self.selection, "QA_PROFILE_PATH", self.production_qa_path
        ):
            c0, _, o0 = await self._admit_and_validate(job, 0, 0.8, forged_rank=3)
            c1, _, o1 = await self._admit_and_validate(job, 1, 0.8, forged_rank=1)
            c2, _, o2 = await self._admit_and_validate(job, 2, 0.9, forged_rank=2)
            lower = []
            for index, score in enumerate((0.7, 0.6, 0.5, 0.4, 0.3), 3):
                lower.append(await self._admit_and_validate(job, index, score))
            opened = await self._snapshot(job)
            self.assertEqual(
                opened.receipt["selection"][
                    "provisional_order_candidate_identity_sha256"
                ][:3],
                [c2["identity"], c0["identity"], c1["identity"]],
            )
            self.assertTrue(
                all(row["ranking"]["rank"] is None for row in opened.receipt["candidates"])
            )
            await self._close(job)
            current_open = await self._snapshot(job)
            expected_ids = {
                c0["identity"]: self._human_review_package(job, c0, o0),
                c1["identity"]: self._human_review_package(job, c1, o1),
                c2["identity"]: self._human_review_package(job, c2, o2),
            }
            with self.assertRaisesRegex(
                self.selection.CandidateSelectionError,
                "OPEN snapshot changed",
            ):
                await self._finalize(job, current_open.identity_sha256)
            reviewed_open = await self._snapshot(job)
            final = await self._finalize(job, reviewed_open.identity_sha256)
            replay = await self._finalize(job, reviewed_open.identity_sha256)
            self.assertTrue(final.created)
            self.assertFalse(replay.created)
            self.assertEqual(final.identity_sha256, replay.identity_sha256)
            self.assertEqual(
                final.receipt["selection"]["top_k_candidate_identity_sha256"],
                [c2["identity"], c0["identity"], c1["identity"]],
            )
            self.assertTrue(final.receipt["selection"]["production_eligible"])
            accepted = self.selection.assert_production_selection(
                job_id=job["id"],
                selection_identity_sha256=final.identity_sha256,
                candidate_identity_sha256=c0["identity"],
                fitting_jobs_root=str(self.jobs_root),
            )
            self.assertEqual(accepted["identity_sha256"], final.identity_sha256)
            async with self.database.AsyncSessionLocal() as db:
                materialized = await self.selection.materialize_selected_candidates(
                    db,
                    job_id=job["id"],
                    selection_identity_sha256=final.identity_sha256,
                    fitting_jobs_root=str(self.jobs_root),
                    trusted_plan_inputs=job["trust"],
                )
            self.assertEqual(
                [item.id for item in materialized],
                [expected_ids[c2["identity"]], expected_ids[c0["identity"]], expected_ids[c1["identity"]]],
            )
            self.assertEqual([item.rank for item in materialized], [1, 2, 3])
            self.assertEqual(
                [item.seed for item in materialized],
                [c2["seed"], c0["seed"], c1["seed"]],
            )
            async with self.database.AsyncSessionLocal() as db:
                approved = await self.library.decide_fitting_candidate(
                    db,
                    candidate_id=materialized[0].id,
                    request=self.library.AnimationCandidateDecisionRequest(
                        decision="approve", reason="Pinned selection PASS"
                    ),
                    admin_email="admin@example.com",
                )
            self.assertEqual(approved.decision, "approved")
            self.assertEqual(approved.id, expected_ids[c2["identity"]])
            original_final_bytes = final.receipt_path.read_bytes()

            def publish_forgery(forged):
                forged.pop("identity_sha256", None)
                identity = _sha(
                    json.dumps(
                        forged,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode()
                )
                forged["identity_sha256"] = identity
                final.receipt_path.write_bytes(_canonical(forged))
                return identity

            hidden = json.loads(original_final_bytes)
            hidden_row = next(
                row
                for row in hidden["candidates"]
                if row["candidate_identity_sha256"] == c2["identity"]
            )
            hidden_row["server_outcome"] = {
                "status": "PENDING",
                "receipt": None,
                "validation_identity_sha256": None,
                "validation_receipt": None,
                "metrics": None,
                "failure": None,
            }
            hidden_row["human_review"] = None
            hidden_row["ranking"] = {
                "eligible": False,
                "failed_gates": [],
                "missing_metric_keys": [],
                "components": {},
                "score": None,
                "rank": None,
                "provisional_order": None,
            }
            for rank, identity in enumerate((c0["identity"], c1["identity"]), 1):
                surviving = next(
                    row
                    for row in hidden["candidates"]
                    if row["candidate_identity_sha256"] == identity
                )
                surviving["ranking"]["rank"] = rank
                surviving["ranking"]["provisional_order"] = rank
            hidden["inventory"].update(
                {
                    "terminal_count": 2,
                    "eligible_count": 2,
                    "pending_count": 1,
                    "top_k_satisfied": False,
                }
            )
            hidden["selection"].update(
                {
                    "top_candidate_identity_sha256": c0["identity"],
                    "top_k_candidate_identity_sha256": [
                        c0["identity"],
                        c1["identity"],
                    ],
                    "production_eligible": False,
                    "provisional_order_candidate_identity_sha256": [
                        c0["identity"],
                        c1["identity"],
                    ],
                }
            )
            hidden_candidate_set = [
                {
                    "candidate_index": row["candidate_index"],
                    "seed": row["seed"],
                    "candidate_identity_sha256": row[
                        "candidate_identity_sha256"
                    ],
                    "admission": row["admission"],
                    "outcome": row["server_outcome"]["receipt"],
                    "human_review": row["human_review"],
                }
                for row in hidden["candidates"]
            ]
            hidden["inventory"]["candidate_set_sha256"] = _sha(
                json.dumps(
                    hidden_candidate_set,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            )
            hidden_identity = publish_forgery(hidden)
            with self.assertRaisesRegex(
                self.selection.CandidateSelectionError, "outcome pin drifted"
            ):
                self.selection.verify_candidate_selection_receipt(
                    job_id=job["id"],
                    selection_identity_sha256=hidden_identity,
                    fitting_jobs_root=str(self.jobs_root),
                )

            closure_hidden = json.loads(original_final_bytes)
            closure_hidden["inventory"]["generation_closed"] = False
            closure_hidden["inventory"][
                "generation_closure_identity_sha256"
            ] = None
            closure_identity = publish_forgery(closure_hidden)
            with self.assertRaisesRegex(
                self.selection.CandidateSelectionError,
                "semantic content|generation closure|FINAL selection",
            ):
                self.selection.verify_candidate_selection_receipt(
                    job_id=job["id"],
                    selection_identity_sha256=closure_identity,
                    fitting_jobs_root=str(self.jobs_root),
                )

            score_forged = json.loads(original_final_bytes)
            score_forged["candidates"][0]["ranking"]["score"] = 0.12345678
            score_forged["inventory"]["eligible_count"] = 99
            score_identity = publish_forgery(score_forged)
            with self.assertRaisesRegex(
                self.selection.CandidateSelectionError, "semantic content"
            ):
                self.selection.verify_candidate_selection_receipt(
                    job_id=job["id"],
                    selection_identity_sha256=score_identity,
                    fitting_jobs_root=str(self.jobs_root),
                )
            final.receipt_path.write_bytes(original_final_bytes)

            # Deleting an admitted high-score bundle cannot be hidden by a
            # freshly self-hashed receipt which removes the same row: the
            # admission and generation-closure inventories remain immutable.
            import shutil

            deleted_dir = (
                self.jobs_root
                / job["id"]
                / "browser-candidates"
                / c2["identity"][:2]
                / c2["identity"]
            )
            shutil.rmtree(deleted_dir)
            deletion_forged = json.loads(original_final_bytes)
            deletion_forged["candidates"] = [
                row
                for row in deletion_forged["candidates"]
                if row["candidate_identity_sha256"] != c2["identity"]
            ]
            deletion_forged["inventory"]["admitted_count"] = 7
            deletion_forged["inventory"]["terminal_count"] = 7
            deletion_forged["selection"]["top_k_candidate_identity_sha256"] = [
                c0["identity"],
                c1["identity"],
                lower[0][0]["identity"],
            ]
            deletion_forged["selection"][
                "top_candidate_identity_sha256"
            ] = c0["identity"]
            deletion_identity = publish_forgery(deletion_forged)
            with self.assertRaisesRegex(
                self.selection.CandidateSelectionError,
                "rows, admissions, and immutable bundle inventory",
            ):
                self.selection.verify_candidate_selection_receipt(
                    job_id=job["id"],
                    selection_identity_sha256=deletion_identity,
                    fitting_jobs_root=str(self.jobs_root),
                )

    async def test_human_hold_promotes_next_machine_candidate(self):
        job = await self._job()
        with unittest.mock.patch.object(
            self.selection, "QA_PROFILE_PATH", self.production_qa_path
        ):
            rows = []
            for index, score in enumerate(
                (0.95, 0.90, 0.80, 0.70, 0.60, 0.50, 0.40, 0.30)
            ):
                rows.append(await self._admit_and_validate(job, index, score))
            await self._close(job)
            opened = await self._snapshot(job)
            self._human_review_package(
                job, rows[0][0], rows[0][2], decision="HOLD"
            )
            expected = []
            for bundle, _, outcome in rows[1:]:
                expected.append(
                    (
                        bundle["identity"],
                        self._human_review_package(job, bundle, outcome),
                    )
                )
            reviewed_open = await self._snapshot(job)
            self.assertNotEqual(opened.identity_sha256, reviewed_open.identity_sha256)
            final = await self._finalize(job, reviewed_open.identity_sha256)
            self.assertEqual(
                final.receipt["selection"]["top_k_candidate_identity_sha256"],
                [identity for identity, _ in expected[:3]],
            )
            held = final.receipt["candidates"][0]
            self.assertEqual(held["human_review"]["decision"], "HOLD")
            self.assertIsNone(held["ranking"]["rank"])
            self.assertEqual(
                [
                    row["ranking"]["rank"]
                    for row in final.receipt["candidates"][1:4]
                ],
                [1, 2, 3],
            )

            pending_job = await self._job()
            pending_rows = []
            for index, score in enumerate(
                (0.95, 0.90, 0.80, 0.70, 0.60, 0.50, 0.40, 0.30)
            ):
                pending_rows.append(
                    await self._admit_and_validate(pending_job, index, score)
                )
            await self._close(pending_job)
            for bundle, _, outcome in pending_rows[1:]:
                self._human_review_package(pending_job, bundle, outcome)
            with self.assertRaisesRegex(
                self.selection.CandidateSelectionError, "pending human review"
            ):
                await self._finalize(pending_job)

    async def test_late_arrival_creates_new_open_snapshot_and_old_is_immutable(self):
        job = await self._job()
        with unittest.mock.patch.object(
            self.selection, "QA_PROFILE_PATH", self.production_qa_path
        ):
            await self._admit_and_validate(job, 0, 0.7)
            first = await self._snapshot(job)
            await self._admit_and_validate(job, 1, 0.8)
            second = await self._snapshot(job)
            self.assertNotEqual(first.identity_sha256, second.identity_sha256)
            self.assertEqual(first.receipt["inventory"]["admitted_count"], 1)
            self.assertEqual(second.receipt["inventory"]["admitted_count"], 2)
            # Verification is deliberately complete-inventory based, so a stale
            # OPEN snapshot cannot be used after a later admission.
            with self.assertRaisesRegex(
                self.selection.CandidateSelectionError,
                "rows, admissions, and immutable bundle inventory",
            ):
                self.selection.verify_candidate_selection_receipt(
                    job_id=job["id"],
                    selection_identity_sha256=first.identity_sha256,
                    fitting_jobs_root=str(self.jobs_root),
                )

    async def test_human_review_and_final_share_publication_lock(self):
        job = await self._job(target=1, limit=1, mode="canary_single_candidate")
        candidate, _, outcome = await self._admit_and_validate(job, 0, 0.95)
        await self._close(job)
        opened = await self._snapshot(job)
        review_holds_lock = asyncio.Event()
        release_review = asyncio.Event()

        async def publish_review_while_locked(*_args, **_kwargs):
            review_holds_lock.set()
            await release_review.wait()
            self._human_review_package(job, candidate, outcome)
            return object()

        async with self.database.AsyncSessionLocal() as review_db:
            with unittest.mock.patch.object(
                self.review,
                "_create_human_review_receipt_locked",
                new=publish_review_while_locked,
            ):
                review_task = asyncio.create_task(
                    self.review.create_human_review_receipt(
                        review_db,
                        job_id=job["id"],
                        candidate_identity_sha256=candidate["identity"],
                        server_validation_identity_sha256=(
                            outcome.receipt["server_validation"]["identity_sha256"]
                        ),
                        review=object(),
                        task_artifact_resolver=lambda _request: None,
                        fitting_jobs_root=str(self.jobs_root),
                        trusted_plan_inputs=job["trust"],
                    )
                )
                await asyncio.wait_for(review_holds_lock.wait(), timeout=1.0)
                final_task = asyncio.create_task(
                    self._finalize(job, opened.identity_sha256)
                )
                await asyncio.sleep(0.1)
                self.assertFalse(
                    final_task.done(),
                    "FINAL must wait while human review owns the publication lock",
                )
                release_review.set()
                await review_task
                with self.assertRaisesRegex(
                    self.selection.CandidateSelectionError,
                    "OPEN snapshot changed",
                ):
                    await final_task

    async def test_pending_missing_metrics_and_provisional_policy_block_production_final(self):
        pending_job = await self._job()
        with unittest.mock.patch.object(
            self.selection, "QA_PROFILE_PATH", self.production_qa_path
        ):
            for index in range(7):
                await self._admit_and_validate(
                    pending_job, index, 0.9 - (index * 0.05)
                )
            pending_bundle = self._bundle(pending_job, 7)
            async with self.database.AsyncSessionLocal() as db:
                await self.selection.admit_browser_candidate(
                    db,
                    job_id=pending_job["id"],
                    candidate_index=7,
                    candidate_identity_sha256=pending_bundle["identity"],
                    fitting_jobs_root=str(self.jobs_root),
                    trusted_plan_inputs=pending_job["trust"],
                )
            await self._close(pending_job)
            with self.assertRaisesRegex(
                self.selection.CandidateSelectionError, "pending candidates"
            ):
                await self._finalize(pending_job)

        provisional_job = await self._job()
        for index, score in enumerate(
            (0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2)
        ):
            await self._admit_and_validate(provisional_job, index, score)
        await self._close(provisional_job)
        with self.assertRaisesRegex(
            self.selection.CandidateSelectionError, "provisional QA calibration"
        ):
            await self._finalize(provisional_job)

        missing_job = await self._job()
        with unittest.mock.patch.object(
            self.selection, "QA_PROFILE_PATH", self.production_qa_path
        ):
            for index in range(8):
                bundle = self._bundle(missing_job, index)
                metrics = self._passing_metrics(0.8)
                metrics.pop("contact_stability_float")
                validation = self._validation(missing_job, bundle, metrics)
                async with self.database.AsyncSessionLocal() as db:
                    await self.selection.admit_browser_candidate(
                        db,
                        job_id=missing_job["id"],
                        candidate_index=index,
                        candidate_identity_sha256=bundle["identity"],
                        fitting_jobs_root=str(self.jobs_root),
                        trusted_plan_inputs=missing_job["trust"],
                    )
                async with self.database.AsyncSessionLocal() as db:
                    await self.selection.record_candidate_validation_outcome(
                        db,
                        job_id=missing_job["id"],
                        candidate_identity_sha256=bundle["identity"],
                        server_validation_identity_sha256=validation["identity"],
                        fitting_jobs_root=str(self.jobs_root),
                        trusted_plan_inputs=missing_job["trust"],
                    )
            await self._close(missing_job)
            with self.assertRaisesRegex(
                self.selection.CandidateSelectionError,
                "insufficient eligible|ranking metrics",
            ):
                await self._finalize(missing_job)

    async def test_canary_final_is_non_comparative_and_never_production_eligible(self):
        job = await self._job(
            target=1, limit=1, mode="canary_single_candidate"
        )
        candidate, _, _ = await self._admit_and_validate(job, 0, 0.95)
        await self._close(job)
        final = await self._finalize(job)
        self.assertEqual(final.receipt["state"], "FINAL")
        self.assertFalse(final.receipt["selection"]["comparative_selection"])
        self.assertFalse(final.receipt["selection"]["production_eligible"])
        self.assertEqual(
            final.receipt["selection"]["finalization_reason"],
            "canary_single_candidate",
        )
        with self.assertRaisesRegex(
            self.selection.CandidateSelectionError, "production-eligible FINAL top-3"
        ):
            self.selection.assert_production_selection(
                job_id=job["id"],
                selection_identity_sha256=final.identity_sha256,
                candidate_identity_sha256=candidate["identity"],
                fitting_jobs_root=str(self.jobs_root),
            )
        with self.assertRaisesRegex(
            self.selection.CandidateSelectionError, "FINAL|closed"
        ):
            async with self.database.AsyncSessionLocal() as db:
                await self.selection.admit_browser_candidate(
                    db,
                    job_id=job["id"],
                    candidate_index=0,
                    candidate_identity_sha256=candidate["identity"],
                    fitting_jobs_root=str(self.jobs_root),
                    trusted_plan_inputs=job["trust"],
                )

    async def test_duplicate_slot_and_tampered_metrics_fail_closed(self):
        job = await self._job()
        with unittest.mock.patch.object(
            self.selection, "QA_PROFILE_PATH", self.production_qa_path
        ):
            candidate, _, outcome = await self._admit_and_validate(job, 0, 0.9)
            duplicate = self._bundle(job, 0)
            async with self.database.AsyncSessionLocal() as db:
                with self.assertRaises(self.selection.CandidateSelectionError):
                    await self.selection.admit_browser_candidate(
                        db,
                        job_id=job["id"],
                        candidate_index=0,
                        candidate_identity_sha256=duplicate["identity"],
                        fitting_jobs_root=str(self.jobs_root),
                        trusted_plan_inputs=job["trust"],
                    )
            duplicate_dir = (
                self.jobs_root
                / job["id"]
                / "browser-candidates"
                / duplicate["identity"][:2]
                / duplicate["identity"]
            )
            import shutil

            shutil.rmtree(duplicate_dir)
            if not any(duplicate_dir.parent.iterdir()):
                duplicate_dir.parent.rmdir()
            snapshot = await self._snapshot(job)
            metrics_pin = outcome.receipt["server_validation"]["metrics"]
            validation_id = outcome.receipt["server_validation"]["identity_sha256"]
            metrics_path = (
                self.jobs_root
                / job["id"]
                / "browser-candidate-reviews"
                / candidate["identity"][:2]
                / candidate["identity"]
                / "server-validations"
                / validation_id
                / metrics_pin["filename"]
            )
            metrics_path.write_bytes(metrics_path.read_bytes() + b" ")
            with self.assertRaisesRegex(
                self.selection.AnimationLibraryError,
                "changed|drifted|differs|exceeds",
            ):
                self.selection.verify_candidate_selection_receipt(
                    job_id=job["id"],
                    selection_identity_sha256=snapshot.identity_sha256,
                    fitting_jobs_root=str(self.jobs_root),
                )

    async def test_human_review_must_pin_current_lifecycle(self):
        job = await self._job()
        with unittest.mock.patch.object(
            self.selection, "QA_PROFILE_PATH", self.production_qa_path
        ):
            bundle, _, outcome = await self._admit_and_validate(job, 0, 0.9)
            self._human_review_package(job, bundle, outcome)
            review_path = (
                self.jobs_root
                / job["id"]
                / "browser-candidate-reviews"
                / bundle["identity"][:2]
                / bundle["identity"]
                / "human-review"
                / "human-review-receipt.json"
            )
            review = json.loads(review_path.read_bytes())
            review["lifecycle_binding_sha256"] = "f" * 64
            review.pop("identity_sha256")
            review["identity_sha256"] = _sha(
                json.dumps(
                    review,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            )
            review_path.write_bytes(_canonical(review))
            with self.assertRaisesRegex(
                self.selection.CandidateSelectionError,
                "human review receipt does not bind",
            ):
                await self._snapshot(job)


if __name__ == "__main__":
    unittest.main()
