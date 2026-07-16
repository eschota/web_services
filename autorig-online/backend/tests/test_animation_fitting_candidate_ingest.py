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
            "animation_fitting_candidate_ingest",
            "animal_animation_library",
            "database",
            "config",
        ):
            sys.modules.pop(name, None)
        cls.database = importlib.import_module("database")
        cls.ingest = importlib.import_module("animation_fitting_candidate_ingest")

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
        video = b"synthetic-ltx-video" * 20
        video_sha = _sha(video)
        video_path = (
            self.root
            / "jobs"
            / "controlled-generation"
            / self.generation_job_id
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
                self.ingest.BrowserCandidateIngestError, "source video integrity"
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

    async def test_rejects_source_video_from_foreign_generation_job(self):
        foreign_job_id = "b" * 64
        foreign_path = (
            self.root
            / "jobs"
            / "controlled-generation"
            / foreign_job_id
            / "raw"
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
                "controlled generation job",
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
