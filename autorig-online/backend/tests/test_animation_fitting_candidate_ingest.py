from __future__ import annotations

import asyncio
from collections import OrderedDict
import hashlib
import importlib
import io
import json
import math
import os
from pathlib import Path
import sys
import tempfile
import unittest
import unittest.mock
import uuid


def _json(value) -> bytes:
    return (json.dumps(value, indent=2) + "\n").encode()


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


class BrowserCandidateIngestTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory(
            prefix="autorig-browser-candidate-ingest-"
        )
        cls.root = Path(cls._tmp.name)
        os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{cls.root / 'test.db'}"
        os.environ["ANIMATION_FITTING_JOBS_ROOT"] = str(cls.root / "jobs")
        os.environ["ANIMATION_LIBRARY_ROOT"] = str(cls.root / "library")
        (cls.root / "jobs").mkdir()
        (cls.root / "library").mkdir()
        backend = str(Path(__file__).resolve().parents[1])
        if backend not in sys.path:
            sys.path.insert(0, backend)
        for name in (
            "animation_fitting_candidate_selection",
            "animation_fitting_candidate_ingest",
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
        cls.selection = importlib.import_module(
            "animation_fitting_candidate_selection"
        )

    @classmethod
    def tearDownClass(cls):
        asyncio.run(cls.database.engine.dispose())
        cls._tmp.cleanup()

    async def asyncSetUp(self):
        await self.database.init_db()
        self.task_id = str(uuid.uuid4())
        self.task_guid = str(uuid.uuid4())
        self.job_id = str(uuid.uuid4())
        self.seed = 6550110377254033429
        self.skeleton_sha = "5" * 64
        self.model_sha = "4" * 64
        self.workflow_sha = "e" * 64
        self.generation_job_id = "c" * 64
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
        reference_artifact = b"trusted-actionless-reference-rgb"
        reference_path = (
            self.root / "jobs" / "references" / self.task_id / "reference_rgb.png"
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
        reference_manifest_path = (
            self.root
            / "jobs"
            / self.production_reference_manifest["pin"]["path"]
        )
        reference_manifest_path.parent.mkdir(parents=True, exist_ok=True)
        reference_manifest_path.write_bytes(reference_bytes)
        self.production_latest_states = {}
        video = b"synthetic-ltx-video" * 20
        video_sha = _sha(video)
        video_path = (
            self.root
            / "jobs"
            / "raw"
            / video_sha[:2]
            / f"{video_sha}.mp4"
        )
        video_path.parent.mkdir(parents=True, exist_ok=True)
        video_path.write_bytes(video)
        self.video_path = video_path
        self.binding = {
            "schema": self.ingest.JOB_BINDING_SCHEMA,
            "task_id": self.task_id,
            "task_guid": self.task_guid,
            "candidate_seed": self.seed,
            "source_rig_type": "HORSE_2",
            "source_model_sha256": self.model_sha,
            "source_skeleton_sha256": self.skeleton_sha,
            "frame_count": 49,
            "output_fps": 30,
            "source_video": {
                "path": str(video_path.relative_to(self.root / "jobs")),
                "sha256": video_sha,
                "bytes": len(video),
            },
            "controlled_generation": {
                "job_id": self.generation_job_id,
                "prompt_id": "prompt-v14",
                "experiment_id": "horse_walk_v14",
                "experiment_sha256": "d" * 64,
                "workflow_fingerprint_sha256": self.workflow_sha,
            },
        }
        async with self.database.AsyncSessionLocal() as db:
            task = self.database.Task(
                id=self.task_id,
                owner_type="user",
                owner_id="owner@example.com",
                guid=self.task_guid,
                status="done",
                input_type="animal",
            )
            version = self.database.AnimalAnimationLibraryVersion(
                rig_type="horse",
                revision=f"horse-ingest-{uuid.uuid4().hex}",
                status="draft",
                template_skeleton_sha256=self.skeleton_sha,
                qa_profile_revision="horse-qa-v1",
                created_by="admin@example.com",
            )
            db.add_all([task, version])
            await db.flush()
            job = self.database.AnimalAnimationFittingJob(
                id=self.job_id,
                library_version_id=version.id,
                rig_type="horse",
                semantic_id="walk_forward",
                status="review",
                workflow_name="autorig_ltx2_animal_loop_v1",
                workflow_fingerprint=self.workflow_sha,
                worker_url="https://worker.example",
                prompt_id=uuid.uuid4().hex,
                prompt="horse walk",
                candidate_target=1,
                candidate_limit=1,
                config_json=json.dumps(
                    {
                        "browser_candidate_ingest": self.binding,
                    }
                ),
                created_by="admin@example.com",
            )
            db.add(job)
            await db.commit()

    def _artifacts(self, *, static: bool = False, bad_quaternion: bool = False):
        frame_count = 49
        fps = 30
        duration = (frame_count - 1) / fps
        times = [index / fps for index in range(frame_count)]
        quaternion_values = []
        for index in range(frame_count):
            angle = (
                0.0
                if static
                else 0.2 * math.sin(2 * math.pi * index / (frame_count - 1))
            )
            quaternion_values.extend((math.sin(angle / 2), 0, 0, math.cos(angle / 2)))
        if bad_quaternion:
            quaternion_values[4:8] = (0, 0, 0, 2)
        quaternion = {
            "name": "thigh.l.quaternion",
            "type": "quaternion",
            "times": times,
            "values": quaternion_values,
        }
        position = {
            "name": "thigh.l.position",
            "type": "vector",
            "times": times,
            "values": [value for _ in times for value in (0, 0, 0)],
        }
        fitted_bytes = _json(
            {
                "schema": self.ingest.FITTED_SCHEMA,
                "loop": True,
                "frameCount": frame_count,
                "fps": fps,
                "durationSeconds": duration,
                "tracks": [quaternion],
                "positionTracks": [position],
                "qa": {
                    "targetSamples": 100,
                    "initialMeanTargetErrorPx": 2,
                    "finalMeanTargetErrorPx": 1,
                    "maximumTargetErrorPx": 2,
                    "maximumBoneLengthErrorPx": 0,
                    "maximumJointLimitViolationRad": 0,
                    "maximumContactSlidePx": 0,
                    "loopEndpointError": 0,
                },
                "frames": [{"frame": index} for index in range(frame_count)],
            }
        )
        clip_bytes = _json(
            {
                "name": "Horse_Walk_V14",
                "duration": duration,
                "uuid": "clip-v14",
                "blendMode": 2500,
                "tracks": [quaternion, position],
            }
        )
        camera_bytes = _json({"schema": "camera", "static": True})
        deformation_bytes = _json(
            {
                "schema": "autorig.browser-horse-target-deformation-qa.v1",
                "passed": True,
                "inputs": {"threeClipSha256": _sha(clip_bytes)},
            }
        )
        preview_bytes = b"\x00\x00\x00\x18ftypmp42" + b"preview" * 20
        phases = OrderedDict(
            (phase, b"\x89PNG\r\n\x1a\n" + phase.encode() * 8)
            for phase in self.ingest.PHASES
        )

        def pin(payload: bytes) -> dict:
            return {"bytes": len(payload), "sha256": _sha(payload)}

        local_phases = [
            {
                "phase": phase,
                "frame_index": index,
                "path": f"old/{phase}.png",
                **pin(payload),
            }
            for index, (phase, payload) in zip((0, 24, 36), phases.items())
        ]
        visual_bytes = _json(
            {
                "schema": self.ingest.VISUAL_QA_ENVELOPE_SCHEMA,
                "visual_phase_gate": {
                    "schema": self.ingest.VISUAL_QA_SCHEMA,
                    "version": 1,
                    "rig_type": "horse",
                    "semantic_id": "walk_forward",
                    "fitted_clip_sha256": _sha(clip_bytes),
                    "decision": None,
                    "required_phases": list(self.ingest.PHASES),
                    "camera": {
                        "static": True,
                        "root_motion_locked": True,
                        "settings_sha256": _sha(camera_bytes),
                    },
                    "coincident_rest_vertex_separation": {
                        "measured": True,
                        "pass": True,
                        "report_sha256": _sha(deformation_bytes),
                    },
                    "frames": [
                        {
                            "phase": row["phase"],
                            "frame_index": row["frame_index"],
                            "evidence_url": None,
                            "sha256": row["sha256"],
                        }
                        for row in local_phases
                    ],
                    "reviewer": {"id": None, "reviewed_at": None},
                },
                "local_evidence": {
                    "source_rig_type": "HORSE_2",
                    "browser_only": True,
                    "blender_used": False,
                    "animation_evaluation": "Three.AnimationMixer",
                    "immutable_inputs": {
                        "three_clip": {"sha256": _sha(clip_bytes)},
                        "skeleton": {"sha256": self.skeleton_sha},
                        "source_model": {"sha256": self.model_sha},
                    },
                    "camera_settings": pin(camera_bytes),
                    "target_mesh_deformation_qa": {"report": pin(deformation_bytes)},
                    "video": {
                        **pin(preview_bytes),
                        "fixed_camera": True,
                        "root_motion_locked": True,
                    },
                    "phase_frames": local_phases,
                    "human_review": {
                        "decision": None,
                        "reviewer_id": None,
                        "reviewed_at": None,
                    },
                    "approvals": {
                        "machine_qa_passed": True,
                        "ready_for_human_review": True,
                        "approved_for_animation_library": False,
                        "release_ready": False,
                    },
                },
            }
        )
        return self.ingest.BrowserCandidateArtifactSet(
            fitted_animation_json=fitted_bytes,
            three_clip_json=clip_bytes,
            visual_phase_qa_json=visual_bytes,
            camera_settings_json=camera_bytes,
            deformation_report_json=deformation_bytes,
            fixed_camera_preview_mp4=preview_bytes,
            phase_frames=phases,
        )

    def _production_receipt(
        self,
        index: int,
        *,
        state_input_fps: int = 24,
        descriptor_input_fps: int = 24,
    ) -> dict:
        seed = self.ingest.derive_browser_candidate_seed(
            self.task_id, "walk_forward", index
        )
        worker_id = "local-4090"
        worker_url = "http://127.0.0.1:8188"
        workflow_name = self.production_workflow_contract["workflow_name"]
        workflow_sha = self.production_workflow_contract[
            "workflow_fingerprint_sha256"
        ]
        experiment_id = f"horse-walk-production-{index}"
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
        positive_prompt = self.job_plan._render_prompt(
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
        negative_prompt = self.job_plan._render_prompt(
            prompt_doc["common_negative_prompt_string"],
            "horse",
            "negative prompt",
        )
        experiment_content = {
            "schema": "autorig.animation-fitting-experiment.v1",
            "experiment_id_string": experiment_id,
            "base_action_id_string": "walk_forward",
            "species_string": "horse",
            "generation_mode_string": "loop",
            "frame_count_int": 49,
            "input_fps_int": descriptor_input_fps,
            "output_fps_int": 30,
            "seed_int": seed,
            "positive_prompt_string": positive_prompt,
            "negative_prompt_string": negative_prompt,
            "reference_object": {
                "immutable_manifest_sha256_string": (
                    self.production_reference_manifest["pin"]["sha256"]
                ),
                "source_model_sha256_string": self.model_sha,
            },
            "workflow_object": {
                "workflow_name_string": workflow_name,
                "workflow_fingerprint_sha256_string": workflow_sha,
            },
        }
        experiment_bytes = self.job_plan.canonical_json_bytes(experiment_content)
        experiment_sha = _sha(experiment_bytes)
        experiment_spec = {
            "content": experiment_content,
            "pin": {
                "path": f"animation_fitting/specs/experiments/{experiment_id}.json",
                "sha256": experiment_sha,
                "bytes": len(experiment_bytes),
            },
        }
        experiment_path = self.root / "jobs" / experiment_spec["pin"]["path"]
        experiment_path.parent.mkdir(parents=True, exist_ok=True)
        experiment_path.write_bytes(experiment_bytes)
        identity = {
            "schema": "autorig.animation-fitting-controlled-job-identity.v1",
            "experiment_id_string": experiment_id,
            "experiment_sha256_string": experiment_sha,
            "runtime_authorization_string": f"explicit_cli:{experiment_id}",
            "reference_sha256_string": self.production_reference_manifest[
                "content"
            ]["reference_artifact"]["sha256"],
            "positive_prompt_sha256_string": self.production_prompt_contract[
                "positive_prompt_sha256"
            ],
            "negative_prompt_sha256_string": self.production_prompt_contract[
                "negative_prompt_sha256"
            ],
            "seed_int": seed,
            "frame_count_int": 49,
            "input_fps_int": state_input_fps,
            "output_fps_int": 30,
            "start_guide_strength_float": 0.8,
            "end_guide_strength_float": 0.8,
            "worker_id_string": worker_id,
            "worker_base_url_string": worker_url,
            "workflow_name_string": workflow_name,
            "workflow_fingerprint_string": workflow_sha,
            "approval_state_string": "generated_not_approved",
            "send_to_skeletal_fitting_bool": False,
        }
        generation_job_id = _sha(
            json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
        )
        prompt_raw = hashlib.sha256(
            f"autorig-controlled-animation-fitting:{generation_job_id}".encode()
        ).hexdigest()[:32]
        prompt_id = (
            f"{prompt_raw[:8]}-{prompt_raw[8:12]}-4{prompt_raw[13:16]}-"
            f"8{prompt_raw[17:20]}-{prompt_raw[20:32]}"
        )
        video = f"production-controlled-video-{index}".encode() * 20
        video_sha = _sha(video)
        video_path = (
            self.root
            / "jobs"
            / "raw"
            / video_sha[:2]
            / f"{video_sha}.mp4"
        )
        video_path.parent.mkdir(parents=True, exist_ok=True)
        video_path.write_bytes(video)
        state = {
            **identity,
            "sequence_int": 3,
            "recorded_at_unix_float": 1.0,
            "status_string": "completed",
            "prompt_id_string": prompt_id,
            "resumed_existing_prompt_bool": False,
            "raw_video_path_string": str(video_path),
            "raw_video_sha256_string": video_sha,
            "raw_video_bytes_int": len(video),
            "frame_paths_array": [],
            "frame_sha256_array": [],
            "backend_output_object": {
                "filename_string": "candidate.mp4",
                "subfolder_string": "",
                "type_string": "output",
            },
        }
        state_bytes = json.dumps(state, indent=2, sort_keys=True).encode() + b"\n"
        state_path = (
            self.root
            / "jobs"
            / "jobs"
            / generation_job_id
            / "000003.json"
        )
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_bytes(state_bytes)
        self.production_latest_states[generation_job_id] = {
            "schema": self.job_plan.TRUSTED_LATEST_STATE_SCHEMA,
            "status": "completed",
            "latest": True,
            "job_id": generation_job_id,
            "state_schema": self.job_plan.CONTROLLED_STATE_SCHEMA,
            "sequence": 3,
            "filename": "000003.json",
            "pin": {
                "path": str(state_path.relative_to(self.root / "jobs")).replace(
                    "\\", "/"
                ),
                "sha256": _sha(state_bytes),
                "bytes": len(state_bytes),
            },
        }
        self.assertEqual(
            str(video_path.relative_to(self.root / "jobs")).replace("\\", "/"),
            f"raw/{video_sha[:2]}/{video_sha}.mp4",
        )
        self.assertEqual(
            str(state_path.relative_to(self.root / "jobs")).replace("\\", "/"),
            f"jobs/{generation_job_id}/000003.json",
        )
        return {
            "schema": self.job_plan.CONTROLLED_RECEIPT_SCHEMA_V2,
            "status": "completed",
            "candidate_index": index,
            "seed": seed,
            "job_id": generation_job_id,
            "prompt_id": prompt_id,
            "semantic_id": "walk_forward",
            "generation_mode": "loop",
            "task": self.production_task,
            "prompt_contract": self.production_prompt_contract,
            "reference_manifest": self.production_reference_manifest,
            "experiment_id": experiment_id,
            "experiment_sha256": experiment_sha,
            "experiment_spec": experiment_spec,
            "worker_id": worker_id,
            "worker_base_url": worker_url,
            "workflow_name": workflow_name,
            "workflow_fingerprint_sha256": workflow_sha,
            "frame_count": 49,
            "input_fps": descriptor_input_fps,
            "output_fps": 30,
            "source_video": {
                "path": str(video_path.relative_to(self.root / "jobs")).replace(
                    "\\", "/"
                ),
                "sha256": video_sha,
                "bytes": len(video),
            },
        }

    def _production_trust(self, receipts: list[dict]):
        return self.ingest.BrowserCandidatePlanTrust(
            reference_manifest=self.production_reference_manifest,
            latest_states=tuple(
                self.production_latest_states[row["job_id"]] for row in receipts
            ),
            retry_authorization=None,
        )

    async def test_server_computes_pins_publishes_atomically_and_replays(self):
        artifacts = self._artifacts()
        async with self.database.AsyncSessionLocal() as db:
            first = await self.ingest.ingest_browser_candidate_artifacts(
                db,
                job_id=self.job_id,
                seed=self.seed,
                artifacts=artifacts,
                fitting_jobs_root=str(self.root / "jobs"),
            )
        self.assertTrue(first.created)
        self.assertTrue(first.manifest_path.is_file())
        self.assertEqual(first.manifest["identity_sha256"], first.identity_sha256)
        self.assertEqual(
            first.manifest["source_task"], {"id": self.task_id, "guid": self.task_guid}
        )
        self.assertEqual(
            first.manifest["candidate"]["review_state"],
            "uploaded_pending_server_validation",
        )
        self.assertFalse(first.manifest["candidate"]["uploaded_qa_assertions_trusted"])
        self.assertEqual(
            first.manifest["candidate"]["server_validation"]["status"], "pending"
        )
        self.assertEqual(
            first.manifest["artifacts"]["three-clip.json"]["sha256"],
            _sha(artifacts.three_clip_json),
        )
        self.assertEqual(
            (first.directory / "source-video.mp4").read_bytes(),
            self.video_path.read_bytes(),
        )
        async with self.database.AsyncSessionLocal() as db:
            second = await self.ingest.ingest_browser_candidate_artifacts(
                db,
                job_id=self.job_id,
                seed=self.seed,
                artifacts=self._artifacts(),
                fitting_jobs_root=str(self.root / "jobs"),
            )
            candidates = (
                (
                    await db.execute(
                        self.ingest.select(self.database.AnimalAnimationCandidate)
                    )
                )
                .scalars()
                .all()
            )
        self.assertFalse(second.created)
        self.assertEqual(second.identity_sha256, first.identity_sha256)
        self.assertEqual(
            candidates,
            [],
            "machine evidence must not create an approvable DB candidate",
        )

    async def test_concurrent_replay_and_generation_closure_do_not_deadlock(self):
        async with self.database.AsyncSessionLocal() as db:
            await self.ingest.ingest_browser_candidate_artifacts(
                db,
                job_id=self.job_id,
                seed=self.seed,
                artifacts=self._artifacts(),
                fitting_jobs_root=str(self.root / "jobs"),
            )

        async def replay():
            async with self.database.AsyncSessionLocal() as db:
                return await self.ingest.ingest_browser_candidate_artifacts(
                    db,
                    job_id=self.job_id,
                    seed=self.seed,
                    artifacts=self._artifacts(),
                    fitting_jobs_root=str(self.root / "jobs"),
                )

        async def close():
            async with self.database.AsyncSessionLocal() as db:
                return await self.selection.close_candidate_generation(
                    db,
                    job_id=self.job_id,
                    fitting_jobs_root=str(self.root / "jobs"),
                )

        replay_result, close_result = await asyncio.wait_for(
            asyncio.gather(replay(), close(), return_exceptions=True), timeout=5
        )
        self.assertFalse(isinstance(close_result, Exception), close_result)
        if isinstance(replay_result, Exception):
            self.assertEqual(getattr(replay_result, "status_code", None), 409)
            self.assertRegex(str(replay_result), "closed|FINAL")
        else:
            self.assertFalse(replay_result.created)

    async def test_rename_before_admission_crash_is_reconciled_at_closure(self):
        class SimulatedProcessCrash(BaseException):
            pass

        original = self.selection._admit_browser_candidate_locked
        with unittest.mock.patch.object(
            self.selection,
            "_admit_browser_candidate_locked",
            side_effect=SimulatedProcessCrash("crash after rename"),
        ):
            async with self.database.AsyncSessionLocal() as db:
                with self.assertRaises(SimulatedProcessCrash):
                    await self.ingest.ingest_browser_candidate_artifacts(
                        db,
                        job_id=self.job_id,
                        seed=self.seed,
                        artifacts=self._artifacts(),
                        fitting_jobs_root=str(self.root / "jobs"),
                    )
        self.assertIs(self.selection._admit_browser_candidate_locked, original)
        bundle_root = self.root / "jobs" / self.job_id / "browser-candidates"
        self.assertEqual(len(list(bundle_root.glob("*/*"))), 1)
        admissions = (
            self.root
            / "jobs"
            / self.job_id
            / "browser-candidate-selection"
            / "admissions"
        )
        self.assertFalse(admissions.exists())
        async with self.database.AsyncSessionLocal() as db:
            closure = await self.selection.close_candidate_generation(
                db,
                job_id=self.job_id,
                fitting_jobs_root=str(self.root / "jobs"),
            )
        self.assertTrue(closure.created)
        self.assertTrue((admissions / "00" / "admission.json").is_file())
        self.assertEqual(len(closure.receipt["admissions"]), 1)

    async def test_v2_multi_slot_plan_publishes_bundle_and_admission_together(self):
        receipts = [self._production_receipt(index) for index in range(8)]
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
        seeds = [row["seed"] for row in receipts[:2]]
        async with self.database.AsyncSessionLocal() as db:
            job = (
                await db.execute(
                    self.ingest.select(self.database.AnimalAnimationFittingJob).where(
                        self.database.AnimalAnimationFittingJob.id == self.job_id
                    )
                )
            ).scalar_one()
            job.config_json = plan.config_json
            job.candidate_target = 8
            job.candidate_limit = 16
            job.workflow_name = plan.workflow_name
            job.workflow_fingerprint = plan.workflow_fingerprint
            job.worker_url = "http://127.0.0.1:8188"
            job.prompt_id = plan.prompt_id
            await db.commit()
        results = []
        for seed in seeds:
            async with self.database.AsyncSessionLocal() as db:
                results.append(
                    await self.ingest.ingest_browser_candidate_artifacts(
                        db,
                        job_id=self.job_id,
                        seed=seed,
                        artifacts=self._artifacts(),
                        fitting_jobs_root=str(self.root / "jobs"),
                        trusted_plan_inputs=trust,
                    )
                )
        self.assertEqual(
            [row.manifest["candidate"]["candidate_index"] for row in results],
            [0, 1],
        )
        for index, row in enumerate(results):
            admission_path = (
                self.root
                / "jobs"
                / self.job_id
                / "browser-candidate-selection"
                / "admissions"
                / f"{index:02d}"
                / "admission.json"
            )
            admission = json.loads(admission_path.read_bytes())
            self.assertEqual(admission["candidate_identity_sha256"], row.identity_sha256)
            self.assertEqual(admission["seed"], seeds[index])

        forged_receipts = json.loads(json.dumps(receipts))
        forged_video = b"alternate-server-owned-video"
        forged_sha = _sha(forged_video)
        forged_receipts[0]["source_video"] = {
            "path": f"raw/{forged_sha[:2]}/{forged_sha}.mp4",
            "sha256": forged_sha,
            "bytes": len(forged_video),
        }
        forged = self.job_plan.build_production_browser_candidate_job_plan(
            {
                "schema": self.job_plan.PLAN_REQUEST_SCHEMA,
                "semantic_id": "walk_forward",
                "candidate_target": 8,
                "candidate_limit": 16,
            },
            trusted_task=plan.config["browser_candidate_job_plan"]["trusted_task"],
            trusted_reference_manifest=self.production_reference_manifest,
            trusted_latest_states=trust.latest_states,
            trusted_retry_authorization=None,
            verified_receipts=forged_receipts,
        )
        async with self.database.AsyncSessionLocal() as db:
            job = (
                await db.execute(
                    self.ingest.select(self.database.AnimalAnimationFittingJob).where(
                        self.database.AnimalAnimationFittingJob.id == self.job_id
                    )
                )
            ).scalar_one()
            job.config_json = forged.config_json
            await db.commit()
            with self.assertRaisesRegex(
                self.ingest.BrowserCandidateIngestError,
                "canonical server-owned browser candidate plan",
            ):
                await self.ingest.ingest_browser_candidate_artifacts(
                    db,
                    job_id=self.job_id,
                    seed=seeds[0],
                    artifacts=self._artifacts(),
                    fitting_jobs_root=str(self.root / "jobs"),
                    trusted_plan_inputs=trust,
                )

    async def test_v2_rejects_completed_state_with_30_input_fps(self):
        receipts = [
            self._production_receipt(
                index,
                state_input_fps=30 if index == 0 else 24,
            )
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
        async with self.database.AsyncSessionLocal() as db:
            job = (
                await db.execute(
                    self.ingest.select(self.database.AnimalAnimationFittingJob).where(
                        self.database.AnimalAnimationFittingJob.id == self.job_id
                    )
                )
            ).scalar_one()
            job.config_json = plan.config_json
            job.candidate_target = 8
            job.candidate_limit = 16
            job.workflow_name = plan.workflow_name
            job.workflow_fingerprint = plan.workflow_fingerprint
            job.worker_url = "http://127.0.0.1:8188"
            job.prompt_id = plan.prompt_id
            await db.commit()
            with self.assertRaisesRegex(
                self.ingest.BrowserCandidateIngestError,
                "controlled-generation state differs",
            ):
                await self.ingest.ingest_browser_candidate_artifacts(
                    db,
                    job_id=self.job_id,
                    seed=receipts[0]["seed"],
                    artifacts=self._artifacts(),
                    fitting_jobs_root=str(self.root / "jobs"),
                    trusted_plan_inputs=trust,
                )

    async def test_v2_trust_gate_and_first_admission_transition_are_atomic(self):
        receipts = [self._production_receipt(index) for index in range(8)]
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
        async with self.database.AsyncSessionLocal() as db:
            job = (
                await db.execute(
                    self.ingest.select(self.database.AnimalAnimationFittingJob).where(
                        self.database.AnimalAnimationFittingJob.id == self.job_id
                    )
                )
            ).scalar_one()
            job.status = "generating"
            job.config_json = plan.config_json
            job.candidate_target = 8
            job.candidate_limit = 16
            job.workflow_name = plan.workflow_name
            job.workflow_fingerprint = plan.workflow_fingerprint
            job.worker_url = "http://127.0.0.1:8188"
            job.prompt_id = plan.prompt_id
            await db.commit()

        async with self.database.AsyncSessionLocal() as db:
            with self.assertRaisesRegex(
                self.ingest.BrowserCandidateIngestError, "resolver inputs are required"
            ):
                await self.ingest.ingest_browser_candidate_artifacts(
                    db,
                    job_id=self.job_id,
                    seed=receipts[0]["seed"],
                    artifacts=self._artifacts(),
                    fitting_jobs_root=str(self.root / "jobs"),
                )

        reference_path = (
            self.root / "jobs" / self.production_reference_manifest["pin"]["path"]
        )
        reference_bytes = self.job_plan.canonical_json_bytes(
            self.production_reference_manifest["content"]
        )
        reference_path.write_bytes(b"tampered-reference-manifest")
        async with self.database.AsyncSessionLocal() as db:
            with self.assertRaisesRegex(
                self.ingest.BrowserCandidateIngestError,
                "immutable browser candidate artifact changed|trusted_reference_manifest",
            ):
                await self.ingest.ingest_browser_candidate_artifacts(
                    db,
                    job_id=self.job_id,
                    seed=receipts[0]["seed"],
                    artifacts=self._artifacts(),
                    fitting_jobs_root=str(self.root / "jobs"),
                    trusted_plan_inputs=trust,
                )
        reference_path.write_bytes(reference_bytes)

        async with self.database.AsyncSessionLocal() as db:
            job = await db.get(self.database.AnimalAnimationFittingJob, self.job_id)
            self.assertEqual(job.status, "generating")
        self.assertEqual(
            self.selection._scan_bundle_identities(
                self.root / "jobs", self.job_id
            ),
            set(),
        )
        self.assertFalse(
            (self.root / "jobs" / self.job_id / "selection" / "admissions").exists()
        )

        with unittest.mock.patch.object(
            self.selection,
            "_admit_browser_candidate_locked",
            side_effect=RuntimeError("simulated admission failure"),
        ):
            async with self.database.AsyncSessionLocal() as db:
                with self.assertRaisesRegex(RuntimeError, "simulated admission failure"):
                    await self.ingest.ingest_browser_candidate_artifacts(
                        db,
                        job_id=self.job_id,
                        seed=receipts[0]["seed"],
                        artifacts=self._artifacts(),
                        fitting_jobs_root=str(self.root / "jobs"),
                        trusted_plan_inputs=trust,
                    )
        async with self.database.AsyncSessionLocal() as db:
            job = await db.get(self.database.AnimalAnimationFittingJob, self.job_id)
            self.assertEqual(job.status, "generating")
        self.assertEqual(
            self.selection._scan_bundle_identities(
                self.root / "jobs", self.job_id
            ),
            set(),
        )
        self.assertFalse(
            (self.root / "jobs" / self.job_id / "selection" / "admissions").exists()
        )

        async with self.database.AsyncSessionLocal() as db:
            result = await self.ingest.ingest_browser_candidate_artifacts(
                db,
                job_id=self.job_id,
                seed=receipts[0]["seed"],
                artifacts=self._artifacts(),
                fitting_jobs_root=str(self.root / "jobs"),
                trusted_plan_inputs=trust,
            )
        self.assertTrue(result.created)
        async with self.database.AsyncSessionLocal() as db:
            job = await db.get(self.database.AnimalAnimationFittingJob, self.job_id)
            self.assertEqual(job.status, "review")

    async def test_v2_rejects_newer_state_revision_and_worker_db_drift(self):
        receipts = [self._production_receipt(index) for index in range(8)]
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
        async with self.database.AsyncSessionLocal() as db:
            job = await db.get(self.database.AnimalAnimationFittingJob, self.job_id)
            job.config_json = plan.config_json
            job.candidate_target = 8
            job.candidate_limit = 16
            job.workflow_name = plan.workflow_name
            job.workflow_fingerprint = plan.workflow_fingerprint
            job.worker_url = "http://127.0.0.1:8188"
            job.prompt_id = plan.prompt_id
            await db.commit()

        state_path = (
            self.root / "jobs" / trust.latest_states[0]["pin"]["path"]
        )
        newer = state_path.with_name("000004.json")
        newer.write_text("{}", encoding="utf-8")
        async with self.database.AsyncSessionLocal() as db:
            with self.assertRaisesRegex(
                self.ingest.BrowserCandidateIngestError, "not the latest state revision"
            ):
                await self.ingest.ingest_browser_candidate_artifacts(
                    db,
                    job_id=self.job_id,
                    seed=receipts[0]["seed"],
                    artifacts=self._artifacts(),
                    fitting_jobs_root=str(self.root / "jobs"),
                    trusted_plan_inputs=trust,
                )
        newer.unlink()

        async with self.database.AsyncSessionLocal() as db:
            job = await db.get(self.database.AnimalAnimationFittingJob, self.job_id)
            job.worker_url = "http://127.0.0.1:8288"
            await db.commit()
        async with self.database.AsyncSessionLocal() as db:
            with self.assertRaisesRegex(
                self.ingest.BrowserCandidateIngestError, "another worker"
            ):
                await self.ingest.ingest_browser_candidate_artifacts(
                    db,
                    job_id=self.job_id,
                    seed=receipts[0]["seed"],
                    artifacts=self._artifacts(),
                    fitting_jobs_root=str(self.root / "jobs"),
                    trusted_plan_inputs=trust,
                )

    async def test_rejects_cross_candidate_visual_clip_before_publish(self):
        artifacts = self._artifacts()
        visual = json.loads(artifacts.visual_phase_qa_json)
        visual["visual_phase_gate"]["fitted_clip_sha256"] = "f" * 64
        forged = self.ingest.BrowserCandidateArtifactSet(
            **{**artifacts.__dict__, "visual_phase_qa_json": _json(visual)}
        )
        async with self.database.AsyncSessionLocal() as db:
            with self.assertRaisesRegex(
                self.ingest.BrowserCandidateIngestError, "uploaded Three clip"
            ):
                await self.ingest.ingest_browser_candidate_artifacts(
                    db,
                    job_id=self.job_id,
                    seed=self.seed,
                    artifacts=forged,
                    fitting_jobs_root=str(self.root / "jobs"),
                )
        candidate_root = self.root / "jobs" / self.job_id / "browser-candidates"
        self.assertFalse(candidate_root.exists())

    async def test_rejects_task_skeleton_video_and_seed_binding_drift(self):
        cases = (
            ("candidate_seed", self.seed + 1, "pinned job binding"),
            ("source_skeleton_sha256", "a" * 64, "template skeleton"),
        )
        for field, value, message in cases:
            with self.subTest(field=field):
                async with self.database.AsyncSessionLocal() as db:
                    job = (
                        await db.execute(
                            self.ingest.select(
                                self.database.AnimalAnimationFittingJob
                            ).where(
                                self.database.AnimalAnimationFittingJob.id
                                == self.job_id
                            )
                        )
                    ).scalar_one()
                    config = json.loads(job.config_json)
                    original = config["browser_candidate_ingest"][field]
                    config["browser_candidate_ingest"][field] = value
                    job.config_json = json.dumps(config)
                    await db.commit()
                    with self.assertRaisesRegex(
                        self.ingest.BrowserCandidateIngestError, message
                    ):
                        await self.ingest.ingest_browser_candidate_artifacts(
                            db,
                            job_id=self.job_id,
                            seed=self.seed,
                            artifacts=self._artifacts(),
                            fitting_jobs_root=str(self.root / "jobs"),
                        )
                    config["browser_candidate_ingest"][field] = original
                    job.config_json = json.dumps(config)
                    await db.commit()

        async with self.database.AsyncSessionLocal() as db:
            job = (
                await db.execute(
                    self.ingest.select(self.database.AnimalAnimationFittingJob).where(
                        self.database.AnimalAnimationFittingJob.id == self.job_id
                    )
                )
            ).scalar_one()
            config = json.loads(job.config_json)
            config["browser_candidate_ingest"]["source_video"]["sha256"] = "b" * 64
            job.config_json = json.dumps(config)
            await db.commit()
            with self.assertRaisesRegex(
                self.ingest.BrowserCandidateIngestError,
                "source video integrity|content addressing",
            ):
                await self.ingest.ingest_browser_candidate_artifacts(
                    db,
                    job_id=self.job_id,
                    seed=self.seed,
                    artifacts=self._artifacts(),
                    fitting_jobs_root=str(self.root / "jobs"),
                )

    async def test_rejects_static_motion_and_non_normalized_quaternion(self):
        for kwargs, message in (
            ({"static": True}, "no nonzero animation"),
            ({"bad_quaternion": True}, "not normalized"),
        ):
            with self.subTest(message=message):
                async with self.database.AsyncSessionLocal() as db:
                    with self.assertRaisesRegex(
                        self.ingest.BrowserCandidateIngestError, message
                    ):
                        await self.ingest.ingest_browser_candidate_artifacts(
                            db,
                            job_id=self.job_id,
                            seed=self.seed,
                            artifacts=self._artifacts(**kwargs),
                            fitting_jobs_root=str(self.root / "jobs"),
                        )

    async def test_rejects_wrong_job_state_and_non_animal_task(self):
        async with self.database.AsyncSessionLocal() as db:
            job = (
                await db.execute(
                    self.ingest.select(self.database.AnimalAnimationFittingJob).where(
                        self.database.AnimalAnimationFittingJob.id == self.job_id
                    )
                )
            ).scalar_one()
            job.status = "failed"
            await db.commit()
            with self.assertRaisesRegex(
                self.ingest.BrowserCandidateIngestError, "review job"
            ):
                await self.ingest.ingest_browser_candidate_artifacts(
                    db,
                    job_id=self.job_id,
                    seed=self.seed,
                    artifacts=self._artifacts(),
                    fitting_jobs_root=str(self.root / "jobs"),
                )
            job.status = "review"
            task = (
                await db.execute(
                    self.ingest.select(self.database.Task).where(
                        self.database.Task.id == self.task_id
                    )
                )
            ).scalar_one()
            task.input_type = "t_pose"
            await db.commit()
            with self.assertRaisesRegex(
                self.ingest.BrowserCandidateIngestError, "completed animal task"
            ):
                await self.ingest.ingest_browser_candidate_artifacts(
                    db,
                    job_id=self.job_id,
                    seed=self.seed,
                    artifacts=self._artifacts(),
                    fitting_jobs_root=str(self.root / "jobs"),
                )

    async def test_rejects_non_content_addressed_source_video_path(self):
        foreign_job_id = "b" * 64
        foreign_path = (
            self.root
            / "jobs"
            / "controlled-generation"
            / "raw"
            / foreign_job_id[:2]
            / self.video_path.name
        )
        foreign_path.parent.mkdir(parents=True)
        foreign_path.write_bytes(self.video_path.read_bytes())
        async with self.database.AsyncSessionLocal() as db:
            job = (
                await db.execute(
                    self.ingest.select(self.database.AnimalAnimationFittingJob).where(
                        self.database.AnimalAnimationFittingJob.id == self.job_id
                    )
                )
            ).scalar_one()
            config = json.loads(job.config_json)
            config["browser_candidate_ingest"]["source_video"]["path"] = str(
                foreign_path.relative_to(self.root / "jobs")
            )
            job.config_json = json.dumps(config)
            await db.commit()
            with self.assertRaisesRegex(
                self.ingest.BrowserCandidateIngestError,
                "content addressing",
            ):
                await self.ingest.ingest_browser_candidate_artifacts(
                    db,
                    job_id=self.job_id,
                    seed=self.seed,
                    artifacts=self._artifacts(),
                    fitting_jobs_root=str(self.root / "jobs"),
                )

    async def test_rejects_symlinked_candidate_ancestor(self):
        outside = self.root / "outside-candidate-root"
        outside.mkdir()
        job_directory = self.root / "jobs" / self.job_id
        try:
            os.symlink(outside, job_directory, target_is_directory=True)
        except (OSError, NotImplementedError) as exc:
            self.skipTest(f"directory symlinks unavailable: {exc}")
        async with self.database.AsyncSessionLocal() as db:
            with self.assertRaisesRegex(
                self.ingest.BrowserCandidateIngestError, "traverses a symlink"
            ):
                await self.ingest.ingest_browser_candidate_artifacts(
                    db,
                    job_id=self.job_id,
                    seed=self.seed,
                    artifacts=self._artifacts(),
                    fitting_jobs_root=str(self.root / "jobs"),
                )
        self.assertEqual(tuple(outside.iterdir()), ())

    def test_transport_is_raw_stream_only_and_bounded(self):
        self.assertFalse(hasattr(self.ingest, "BrowserCandidateIngestRequest"))
        self.assertFalse(
            hasattr(self.ingest, "decode_browser_candidate_ingest_request")
        )
        self.assertFalse(
            (
                Path(__file__).resolve().parents[1]
                / "browser_animation_candidate_ingest.v1.schema.json"
            ).exists()
        )

        class ReadOnce(io.BytesIO):
            calls = 0

            def read(self, size=-1):
                self.calls += 1
                if self.calls > 1:
                    raise AssertionError("artifact stream was read more than once")
                return super().read(size)

        stream = ReadOnce(b"streamed-artifact")
        self.assertEqual(
            self.ingest._read_artifact(stream, "stream", 64),
            b"streamed-artifact",
        )
        self.assertEqual(stream.calls, 1)
        with self.assertRaisesRegex(
            self.ingest.BrowserCandidateIngestError, "server size limit"
        ):
            self.ingest._read_artifact(io.BytesIO(b"x" * 65), "stream", 64)


if __name__ == "__main__":
    unittest.main()
