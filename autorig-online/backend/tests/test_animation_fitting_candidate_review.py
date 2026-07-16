from __future__ import annotations

import asyncio
from collections import OrderedDict
import hashlib
import importlib
import json
import math
import os
from pathlib import Path
import sys
import tempfile
import unittest
import uuid


def _json(value) -> bytes:
    return (json.dumps(value, indent=2) + "\n").encode()


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


class BrowserCandidateReviewTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory(
            prefix="autorig-browser-candidate-review-"
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
            "animation_fitting_candidate_review",
            "animation_fitting_candidate_ingest",
            "animal_animation_library",
            "database",
            "config",
        ):
            sys.modules.pop(name, None)
        cls.database = importlib.import_module("database")
        cls.select = staticmethod(importlib.import_module("sqlalchemy").select)
        cls.ingest = importlib.import_module("animation_fitting_candidate_ingest")
        cls.review = importlib.import_module("animation_fitting_candidate_review")

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
        self.workflow_sha = "e" * 64
        self.generation_job_id = "c" * 64
        self.model_bytes = (b"trusted-task-model" * 37) + self.task_id.encode()
        self.skeleton_bytes = (b"trusted-task-skeleton" * 19) + self.task_guid.encode()
        self.model_sha = _sha(self.model_bytes)
        self.skeleton_sha = _sha(self.skeleton_bytes)
        task_artifacts = self.root / "task-artifacts" / self.task_id
        task_artifacts.mkdir(parents=True)
        self.model_path = task_artifacts / "prepared.glb"
        self.skeleton_path = task_artifacts / "skeleton.json"
        self.model_path.write_bytes(self.model_bytes)
        self.skeleton_path.write_bytes(self.skeleton_bytes)
        runtime_root = self.root / "qa-runtime" / self.task_id
        runtime_root.mkdir(parents=True)
        self.runtime_paths = {}
        for name in ("node", "chrome", "ffmpeg", "ffprobe", "three"):
            path = runtime_root / name
            path.write_bytes((f"trusted-{name}-runtime-160".encode()) * 7)
            self.runtime_paths[name] = path
        self.qa_runtime = self.review.TrustedQARuntime(
            node_path=self.runtime_paths["node"],
            chrome_path=self.runtime_paths["chrome"],
            ffmpeg_path=self.runtime_paths["ffmpeg"],
            ffprobe_path=self.runtime_paths["ffprobe"],
            three_module_path=self.runtime_paths["three"],
            three_revision="160",
            three_expected_sha256=_sha(self.runtime_paths["three"].read_bytes()),
        )
        video = b"synthetic-ltx-video" * 20
        video_sha = _sha(video)
        self.video_path = (
            self.jobs_root
            / "controlled-generation"
            / self.generation_job_id
            / "raw"
            / video_sha[:2]
            / f"{video_sha}.mp4"
        )
        self.video_path.parent.mkdir(parents=True, exist_ok=True)
        self.video_path.write_bytes(video)
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
                "path": str(self.video_path.relative_to(self.jobs_root)),
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
                revision=f"horse-review-{uuid.uuid4().hex}",
                status="draft",
                template_skeleton_sha256=self.skeleton_sha,
                qa_profile_revision="horse-qa-v1",
                created_by="admin@example.com",
            )
            db.add_all([task, version])
            await db.flush()
            self.library_version_id = version.id
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
                config_json=json.dumps({"browser_candidate_ingest": self.binding}),
                created_by="admin@example.com",
            )
            db.add(job)
            await db.commit()
        self.upload = await self._ingest()

    def _upload_artifacts(self):
        frame_count = 49
        fps = 30
        duration = (frame_count - 1) / fps
        times = [index / fps for index in range(frame_count)]
        quaternion_values = []
        for index in range(frame_count):
            angle = 0.2 * math.sin(2 * math.pi * index / (frame_count - 1))
            quaternion_values.extend(
                (math.sin(angle / 2), 0, 0, math.cos(angle / 2))
            )
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
        camera_bytes = _json({"schema": "uploaded-camera", "static": True})
        deformation_bytes = _json(
            {
                "schema": "autorig.browser-horse-target-deformation-qa.v1",
                "passed": True,
                "inputs": {"threeClipSha256": _sha(clip_bytes)},
            }
        )
        preview_bytes = b"\x00\x00\x00\x18ftypmp42" + b"uploaded-preview" * 10
        phases = OrderedDict(
            (phase, b"\x89PNG\r\n\x1a\n" + f"uploaded-{phase}".encode() * 4)
            for phase in self.ingest.PHASES
        )

        def pin(payload: bytes) -> dict:
            return {"bytes": len(payload), "sha256": _sha(payload)}

        local_phases = [
            {
                "phase": phase,
                "frame_index": frame_index,
                "path": f"untrusted/{phase}.png",
                **pin(payload),
            }
            for frame_index, (phase, payload) in zip((0, 24, 36), phases.items())
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
                    "target_mesh_deformation_qa": {
                        "report": pin(deformation_bytes)
                    },
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

    async def _ingest(self):
        async with self.database.AsyncSessionLocal() as db:
            return await self.ingest.ingest_browser_candidate_artifacts(
                db,
                job_id=self.job_id,
                seed=self.seed,
                artifacts=self._upload_artifacts(),
                fitting_jobs_root=str(self.jobs_root),
            )

    def _resolver(self, _request):
        return self.review.TrustedTaskArtifacts(
            model_path=self.model_path,
            skeleton_path=self.skeleton_path,
        )

    def _qa_evidence(self, *, max_separation=0.01, cross_clip=False, reviewed=False):
        camera = _json({"schema": "trusted-camera", "static": True})
        deformation = _json(
            {
                "schema": "trusted-deformation-v1",
                "max_separation_m": max_separation,
                "sample_count": 12,
            }
        )
        preview = b"\x00\x00\x00\x18ftypmp42" + b"server-preview" * 12
        phases = {
            phase: b"\x89PNG\r\n\x1a\n" + f"server-{phase}".encode() * 6
            for phase in self.ingest.PHASES
        }
        clip_sha = self.upload.manifest["artifacts"]["three-clip.json"]["sha256"]
        if cross_clip:
            clip_sha = "f" * 64
        metrics = {
            "max_edge_stretch": 4.5,
            "p99_edge_stretch": 2.0,
            "zero_deform_vertices": 0,
            "visual_phase_gate": {
                "schema": "autorig.animation-visual-phase-qa.v1",
                "version": 1,
                "rig_type": "horse",
                "semantic_id": "walk_forward",
                "fitted_clip_sha256": clip_sha,
                "decision": "PASS" if reviewed else None,
                "camera": {
                    "static": True,
                    "projection": "perspective",
                    "view": "fixed-side",
                    "root_motion_locked": True,
                    "settings_sha256": _sha(camera),
                },
                "coincident_rest_vertex_separation": {
                    "measured": True,
                    "pass": max_separation <= 0.04,
                    "threshold_m": 0.04,
                    "max_separation_m": max_separation,
                    "sample_count": 12,
                    "group_count": 3,
                    "report_url": "https://autorig.online/server-evidence/deformation.json",
                    "report_sha256": _sha(deformation),
                },
                "required_phases": list(self.ingest.PHASES),
                "frames": [
                    {
                        "phase": phase,
                        "frame_index": frame_index,
                        "evidence_url": f"https://autorig.online/server-evidence/{phase}.png",
                        "sha256": _sha(phases[phase]),
                    }
                    for phase, frame_index in zip(
                        self.ingest.PHASES, (0, 24, 36)
                    )
                ],
                "reviewer": (
                    {"id": "forged-client", "reviewed_at": "2026-07-16T00:00:00Z"}
                    if reviewed
                    else {"id": None, "reviewed_at": None}
                ),
            },
        }
        return self.review.TrustedQAEvidence(
            runner_name="browser-three-qa",
            runner_revision="v14.1",
            metrics=metrics,
            artifacts={
                "camera-settings.json": camera,
                "deformation-report.json": deformation,
                "fixed-camera-preview.mp4": preview,
                **{f"phase-{phase}.png": payload for phase, payload in phases.items()},
            },
        )

    def _qa_runner(self, _context):
        return self._qa_evidence()

    async def _validate(self, qa_runner=None):
        async with self.database.AsyncSessionLocal() as db:
            return await self.review.create_server_validation_receipt(
                db,
                job_id=self.job_id,
                candidate_identity_sha256=self.upload.identity_sha256,
                task_artifact_resolver=self._resolver,
                qa_runner=qa_runner or self._qa_runner,
                qa_runtime=self.qa_runtime,
                fitting_jobs_root=str(self.jobs_root),
            )

    async def _human_review(self, validation, decision="PASS"):
        async with self.database.AsyncSessionLocal() as db:
            return await self.review.create_human_review_receipt(
                db,
                job_id=self.job_id,
                candidate_identity_sha256=self.upload.identity_sha256,
                server_validation_identity_sha256=validation.identity_sha256,
                review=self.review.HumanReviewDecision(
                    decision=decision,
                    reviewer_id="admin@example.com",
                    reviewed_at="2026-07-16T12:30:00+07:00",
                    reason=(
                        None
                        if decision == "PASS"
                        else f"Operator visual review decision: {decision}"
                    ),
                ),
                task_artifact_resolver=self._resolver,
                fitting_jobs_root=str(self.jobs_root),
            )

    async def test_server_validation_and_pass_review_are_immutable_and_idempotent(self):
        validation = await self._validate()
        replay = await self._validate()
        self.assertTrue(validation.created)
        self.assertFalse(replay.created)
        self.assertEqual(validation.identity_sha256, replay.identity_sha256)
        self.assertFalse(
            validation.receipt["candidate"]["uploaded_qa_assertions_trusted"]
        )
        self.assertFalse(validation.receipt["candidate"]["uploaded_qa_artifact_used"])
        passed = await self._human_review(validation)
        passed_replay = await self._human_review(validation)
        self.assertTrue(passed.created)
        self.assertFalse(passed_replay.created)
        self.assertEqual(passed.package_id, passed_replay.package_id)
        expected_package_id = str(
            uuid.uuid5(
                self.review.PACKAGE_NAMESPACE,
                f"{self.job_id}:{self.seed}:{self.upload.manifest_sha256}",
            )
        )
        self.assertEqual(passed.package_id, expected_package_id)
        descriptor = json.loads(passed.package_descriptor_path.read_bytes())
        self.assertEqual(descriptor["package_id"], expected_package_id)
        self.assertEqual(descriptor["candidate_id"], expected_package_id)
        self.assertEqual(
            descriptor["candidate_bundle_sha256"], self.upload.manifest_sha256
        )
        self.assertEqual(
            descriptor["human_review_sha256"], passed.receipt_sha256
        )
        self.assertEqual(
            descriptor["pins"]["human_review_receipt"]["sha256"],
            passed.receipt_sha256,
        )
        self.assertNotIn("package_descriptor", passed.receipt)
        self.assertEqual(descriptor["semantic_id"], "walk_forward")
        self.assertEqual(
            descriptor["clip"], self.upload.manifest["artifacts"]["three-clip.json"]
        )
        self.assertEqual(
            descriptor["pins"]["task_model"]["sha256"], self.model_sha
        )
        self.assertEqual(
            descriptor["pins"]["task_skeleton"]["sha256"], self.skeleton_sha
        )
        async with self.database.AsyncSessionLocal() as db:
            candidates = (
                (
                    await db.execute(
                        self.select(
                            self.database.AnimalAnimationCandidate
                        )
                    )
                )
                .scalars()
                .all()
            )
            approved = (
                (
                    await db.execute(
                        self.select(
                            self.database.AnimalAnimationApprovedClip
                        )
                    )
                )
                .scalars()
                .all()
            )
            activations = (
                (
                    await db.execute(
                        self.select(
                            self.database.AnimalAnimationLibraryActivation
                        )
                    )
                )
                .scalars()
                .all()
            )
        self.assertEqual((candidates, approved, activations), ([], [], []))

    async def test_actual_task_artifact_sha_and_symlink_are_fail_closed(self):
        original = self.model_path.read_bytes()
        self.model_path.write_bytes(original + b"tampered")
        with self.assertRaisesRegex(
            self.review.CandidateReviewError, "actual task model SHA"
        ):
            await self._validate()
        self.model_path.write_bytes(original)
        link = self.root / "task-artifacts" / self.task_id / "skeleton-link.json"
        try:
            os.symlink(self.skeleton_path, link)
        except (OSError, NotImplementedError) as exc:
            self.skipTest(f"file symlinks unavailable: {exc}")

        def symlink_resolver(_request):
            return self.review.TrustedTaskArtifacts(self.model_path, link)

        async with self.database.AsyncSessionLocal() as db:
            with self.assertRaisesRegex(
                self.review.CandidateReviewError, "must not traverse a symlink"
            ):
                await self.review.create_server_validation_receipt(
                    db,
                    job_id=self.job_id,
                    candidate_identity_sha256=self.upload.identity_sha256,
                    task_artifact_resolver=symlink_resolver,
                    qa_runner=self._qa_runner,
                    qa_runtime=self.qa_runtime,
                    fitting_jobs_root=str(self.jobs_root),
                )

    async def test_uploaded_machine_pass_cannot_override_server_qa_failure(self):
        def failing_runner(_context):
            return self._qa_evidence(max_separation=0.18)

        with self.assertRaisesRegex(
            self.review.AnimationLibraryError,
            "separation gate did not pass|exceeds threshold",
        ):
            await self._validate(failing_runner)
        receipt_root = (
            self.jobs_root
            / self.job_id
            / "browser-candidate-reviews"
            / self.upload.identity_sha256[:2]
            / self.upload.identity_sha256
        )
        self.assertFalse(receipt_root.exists())

    async def test_cross_clip_prefilled_review_and_evidence_pin_are_rejected(self):
        cases = (
            (lambda: self._qa_evidence(cross_clip=True), "different candidate"),
            (lambda: self._qa_evidence(reviewed=True), "must be unreviewed"),
        )
        for factory, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(self.review.CandidateReviewError, message):
                    await self._validate(lambda _context, f=factory: f())

        def bad_pin_runner(_context):
            evidence = self._qa_evidence()
            metrics = json.loads(json.dumps(evidence.metrics))
            metrics["visual_phase_gate"]["camera"]["settings_sha256"] = "a" * 64
            return self.review.TrustedQAEvidence(
                evidence.runner_name,
                evidence.runner_revision,
                metrics,
                evidence.artifacts,
            )

        with self.assertRaisesRegex(
            self.review.CandidateReviewError, "camera SHA"
        ):
            await self._validate(bad_pin_runner)

    async def test_lifecycle_and_task_artifacts_are_rechecked_after_qa(self):
        async def lifecycle_drift_runner(_context):
            async with self.database.AsyncSessionLocal() as other:
                job = (
                    await other.execute(
                        self.select(
                            self.database.AnimalAnimationFittingJob
                        ).where(
                            self.database.AnimalAnimationFittingJob.id == self.job_id
                        )
                    )
                ).scalar_one()
                job.status = "failed"
                await other.commit()
            return self._qa_evidence()

        with self.assertRaisesRegex(
            self.review.CandidateReviewError, "lifecycle differs|lifecycle changed"
        ):
            await self._validate(lifecycle_drift_runner)

    async def test_runtime_files_are_pinned_and_rechecked_after_qa(self):
        original = self.runtime_paths["node"].read_bytes()

        def runtime_drift_runner(context):
            self.assertEqual(context.runtime.three_revision, "160")
            self.assertEqual(
                context.runtime_pins["three_module"]["sha256"],
                self.qa_runtime.three_expected_sha256,
            )
            self.runtime_paths["node"].write_bytes(original + b"drift")
            return self._qa_evidence()

        with self.assertRaisesRegex(
            self.review.CandidateReviewError, "runtime changed"
        ):
            await self._validate(runtime_drift_runner)

    async def test_bundle_inventory_drift_and_receipt_collision_are_rejected(self):
        extra = self.upload.directory / "unexpected.txt"
        extra.write_text("drift", encoding="utf-8")
        with self.assertRaisesRegex(
            self.review.CandidateReviewError, "inventory drifted"
        ):
            await self._validate()
        extra.unlink()
        validation = await self._validate()
        validation.receipt_path.write_bytes(b"collision")
        with self.assertRaisesRegex(
            self.review.CandidateReviewError,
            "changed while|not strict|collision|regular file",
        ) as raised:
            await self._validate()
        self.assertEqual(raised.exception.status_code, 409)

    async def test_review_slot_is_single_decision_and_non_pass_has_no_descriptor(self):
        validation = await self._validate()
        result = await self._human_review(validation, "HOLD")
        self.assertIsNone(result.package_id)
        self.assertIsNone(result.package_descriptor_path)
        self.assertEqual(
            tuple(path.name for path in result.directory.iterdir()),
            ("human-review-receipt.json",),
        )
        replay = await self._human_review(validation, "HOLD")
        self.assertFalse(replay.created)
        with self.assertRaisesRegex(
            self.review.CandidateReviewError, "collision"
        ) as raised:
            await self._human_review(validation, "REJECT")
        self.assertEqual(raised.exception.status_code, 409)

    async def test_hold_and_reject_require_a_bounded_reason(self):
        validation = await self._validate()
        for reason in (None, "x" * 2001):
            with self.subTest(reason_length=None if reason is None else len(reason)):
                async with self.database.AsyncSessionLocal() as db:
                    with self.assertRaisesRegex(
                        self.review.CandidateReviewError,
                        "requires a reason|reason is invalid",
                    ):
                        await self.review.create_human_review_receipt(
                            db,
                            job_id=self.job_id,
                            candidate_identity_sha256=self.upload.identity_sha256,
                            server_validation_identity_sha256=(
                                validation.identity_sha256
                            ),
                            review=self.review.HumanReviewDecision(
                                decision="HOLD",
                                reviewer_id="admin@example.com",
                                reviewed_at="2026-07-16T12:30:00+07:00",
                                reason=reason,
                            ),
                            task_artifact_resolver=self._resolver,
                            fitting_jobs_root=str(self.jobs_root),
                        )

    async def test_human_pass_revalidates_server_gate_and_lifecycle(self):
        validation = await self._validate()
        metrics_path = validation.directory / "server-qa-metrics.json"
        metrics = json.loads(metrics_path.read_bytes())
        metrics["visual_phase_gate"]["camera"]["static"] = False
        metrics_path.write_bytes(
            json.dumps(metrics, sort_keys=True, separators=(",", ":")).encode()
            + b"\n"
        )
        with self.assertRaisesRegex(
            self.review.CandidateReviewError,
            "SHA drifted|changed while|exceeds the server size limit",
        ):
            await self._human_review(validation)

    async def test_api_has_no_client_artifact_or_qa_flag_parameters(self):
        import inspect

        validation_parameters = set(
            inspect.signature(
                self.review.create_server_validation_receipt
            ).parameters
        )
        review_parameters = set(
            inspect.signature(self.review.create_human_review_receipt).parameters
        )
        forbidden = {
            "model_url",
            "model_path",
            "skeleton_url",
            "skeleton_path",
            "evidence_url",
            "qa_passed",
            "machine_qa_passed",
            "deformation_passed",
        }
        self.assertFalse(validation_parameters & forbidden)
        self.assertFalse(review_parameters & forbidden)


if __name__ == "__main__":
    unittest.main()
