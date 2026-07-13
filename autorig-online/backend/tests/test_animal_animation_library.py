import asyncio
from datetime import timedelta
import hashlib
import importlib
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
import uuid
from unittest.mock import AsyncMock, patch

from sqlalchemy import delete, select
from starlette.requests import Request


class AnimalAnimationLibraryTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory(prefix="autorig-animal-animation-library-")
        root = Path(cls._tmp.name)
        os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{root / 'test.db'}"
        os.environ["ANIMATION_LIBRARY_ROOT"] = str(root / "library")
        os.environ["ANIMATION_FITTING_JOBS_ROOT"] = str(root / "jobs")
        (root / "library").mkdir(parents=True)
        (root / "jobs").mkdir(parents=True)

        backend_dir = str(Path(__file__).resolve().parents[1])
        if backend_dir not in sys.path:
            sys.path.insert(0, backend_dir)
        for name in (
            "main",
            "animal_animation_library",
            "database",
            "models",
            "config",
            "tasks",
            "auth",
        ):
            sys.modules.pop(name, None)
        cls.database = importlib.import_module("database")
        cls.library = importlib.import_module("animal_animation_library")
        cls.models = importlib.import_module("models")
        cls.main = importlib.import_module("main")

    @classmethod
    def tearDownClass(cls):
        try:
            asyncio.run(cls.database.engine.dispose())
        except Exception:
            pass
        cls._tmp.cleanup()

    async def asyncSetUp(self):
        await self.database.init_db()
        async with self.database.AsyncSessionLocal() as db:
            for model in (
                self.database.AnimalAnimationApprovedClip,
                self.database.AnimalAnimationCandidate,
                self.database.AnimalAnimationFittingJob,
                self.database.AnimalAnimationLibraryActivation,
                self.database.AnimalAnimationLibraryArtifact,
                self.database.AnimalAnimationLibraryVersion,
            ):
                await db.execute(delete(model))
            await db.commit()

    def _glb(self, name: str) -> tuple[Path, str]:
        path = Path(os.environ["ANIMATION_LIBRARY_ROOT"]) / name
        gltf = {
            "asset": {"version": "2.0"},
            "meshes": [{"primitives": [{"attributes": {"POSITION": 0, "JOINTS_0": 1, "WEIGHTS_0": 2}}]}],
            "skins": [{"joints": [0]}],
            "nodes": [{"mesh": 0, "skin": 0}],
            "accessors": [{"min": [0.0], "max": [1.0]} for _ in self.library.ANIMAL_CLIP_IDS],
            "animations": [
                {
                    "name": semantic_id,
                    "samplers": [{"input": index, "output": 0}],
                    "channels": [],
                }
                for index, semantic_id in enumerate(self.library.ANIMAL_CLIP_IDS)
            ],
        }
        json_chunk = json.dumps(gltf, separators=(",", ":")).encode("utf-8")
        json_chunk += b" " * ((4 - len(json_chunk) % 4) % 4)
        total_length = 12 + 8 + len(json_chunk)
        payload = (
            b"glTF"
            + (2).to_bytes(4, "little")
            + total_length.to_bytes(4, "little")
            + len(json_chunk).to_bytes(4, "little")
            + (0x4E4F534A).to_bytes(4, "little")
            + json_chunk
        )
        path.write_bytes(payload)
        return path, hashlib.sha256(payload).hexdigest()

    def _manifest(self, revision: str, orientation: str, artifact_sha: str) -> dict:
        clips = []
        for canonical in self.library.ANIMAL_CLIPS:
            clips.append({
                "id": canonical["id"],
                "category": canonical["category"],
                "order": canonical["order"],
                "loop": canonical["loop"],
                "duration": 1.0,
                "fps": 30.0,
                "start_pose_id": canonical["start_pose_id"],
                "end_pose_id": canonical["end_pose_id"],
                "root_motion_available": False,
                "qa_profile_revision": "horse_qa_profile_v1",
                "provenance": {
                    "candidate_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{revision}:{canonical['id']}"))
                },
                "fbx_url": f"https://worker.example/{canonical['id']}.fbx",
            })
        return {
            "schema": "animal-animation-manifest.v1",
            "library_revision": revision,
            "rig_type": "horse",
            "orientation": orientation,
            "template_skeleton_sha256": "1" * 64,
            "artifact_sha256": artifact_sha,
            "clips": clips,
            "poses": list(self.library.TAXONOMY["poses"]),
        }

    @staticmethod
    def _request(*, if_none_match: str = "") -> Request:
        headers = []
        if if_none_match:
            headers.append((b"if-none-match", if_none_match.encode("ascii")))
        return Request({
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "scheme": "https",
            "path": "/",
            "raw_path": b"/",
            "query_string": b"",
            "headers": headers,
            "client": ("127.0.0.1", 12345),
            "server": ("autorig.test", 443),
        })

    async def _create_complete_version(self, db, revision: str):
        version = await self.library.create_library_version(
            db,
            self.library.AnimationLibraryCreateRequest(
                rig_type="horse",
                revision=revision,
                template_skeleton_sha256="1" * 64,
                qa_profile_revision="horse_qa_profile_v1",
            ),
            admin_email="admin@example.com",
        )
        for orientation in self.library.ANIMAL_ORIENTATIONS:
            glb_path, digest = self._glb(f"{revision}-{orientation}.glb")
            await self.library.put_library_artifact(
                db,
                rig_type="horse",
                revision=revision,
                orientation=orientation,
                request=self.library.AnimationLibraryArtifactPutRequest(
                    manifest=self._manifest(revision, orientation, digest),
                    animation_glb_url=f"https://worker.example/{revision}/{orientation}/animations.glb",
                    animation_glb_path=str(glb_path),
                    artifact_sha256=digest,
                ),
                library_root=os.environ["ANIMATION_LIBRARY_ROOT"],
            )
        for clip in self.library.ANIMAL_CLIPS:
            db.add(self.database.AnimalAnimationApprovedClip(
                library_version_id=version.id,
                candidate_id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"{revision}:{clip['id']}")),
                semantic_id=clip["id"],
                category=clip["category"],
                clip_order=clip["order"],
                loop=clip["loop"],
                duration=1.0,
                fps=30.0,
                start_pose_id=clip["start_pose_id"],
                end_pose_id=clip["end_pose_id"],
                root_motion_available=False,
                qa_profile_revision="horse_qa_profile_v1",
                fbx_url=f"https://worker.example/{clip['id']}.fbx",
                fbx_sha256=f"{clip['order']:064x}",
                metrics_json="{}",
                provenance_json="{}",
                approved_by="admin@example.com",
            ))
        await db.commit()
        return version

    def test_taxonomy_is_exact_and_aliases_are_explicit(self):
        self.assertEqual(len(self.library.ANIMAL_RIG_TYPES), 12)
        self.assertEqual(len(self.library.ANIMAL_CLIP_IDS), 30)
        self.assertEqual(len(set(self.library.ANIMAL_CLIP_IDS)), 30)
        self.assertNotIn("default_pose", self.library.ANIMAL_CLIP_IDS)
        self.assertEqual([clip["order"] for clip in self.library.ANIMAL_CLIPS], list(range(1, 31)))
        self.assertTrue(all((clip["frame_profile"] - 1) % 8 == 0 for clip in self.library.ANIMAL_CLIPS))
        self.assertEqual(self.library.canonical_animation_id("Horse_gallop", "horse"), "run")
        self.assertEqual(self.library.canonical_animation_id("dog_default", "dog"), "idle_neutral")
        self.assertIsNone(self.library.canonical_animation_id("roughly_walking_fast"))
        matrix_schema = json.loads(
            (Path(self.library.__file__).parent / "animal_variant_matrix.animation.v1.schema.json").read_text("utf-8")
        )
        required = matrix_schema["properties"]["variants"]["items"]["required"]
        self.assertIn("animation_library_revision", required)
        self.assertIn("animation_glb_sha256", required)

    def test_manifest_and_matrix_contracts_are_strict(self):
        _, digest = self._glb("contract.glb")
        manifest = self._manifest("horse-v1", "front", digest)
        validated = self.library.validate_animation_manifest(manifest)
        self.assertEqual([item["id"] for item in validated["clips"]], list(self.library.ANIMAL_CLIP_IDS))
        invalid = json.loads(json.dumps(manifest))
        invalid["clips"][0], invalid["clips"][1] = invalid["clips"][1], invalid["clips"][0]
        with self.assertRaises(self.library.AnimationLibraryError):
            self.library.validate_animation_manifest(invalid)

        row = {
            "animation_manifest_url": "https://worker.example/manifest.json",
            "animation_glb_url": "https://worker.example/animations.glb",
            "animation_library_revision": "horse-v1",
            "animation_clip_count": 30,
            "animation_glb_sha256": digest,
            "animation_manifest_sha256": self.library.manifest_sha256(manifest),
        }
        parsed = self.library.parse_matrix_animation_artifact(
            row,
            rig_type="horse",
            orientation="front",
            expected_revision="horse-v1",
        )
        self.assertEqual(parsed.animation_glb_sha256, digest)
        row["animation_clip_count"] = 29
        with self.assertRaises(self.library.AnimationLibraryError):
            self.library.parse_matrix_animation_artifact(row, rig_type="horse", orientation="front")

    async def test_job_candidate_approval_is_idempotent_and_qa_gated(self):
        async with self.database.AsyncSessionLocal() as db:
            version = await self.library.create_library_version(
                db,
                self.library.AnimationLibraryCreateRequest(
                    rig_type="horse",
                    revision="horse-review-v1",
                    template_skeleton_sha256="1" * 64,
                    qa_profile_revision="horse_qa_profile_v1",
                ),
                admin_email="admin@example.com",
            )
            job_request = self.library.AnimationFittingJobCreateRequest(
                rig_type="horse",
                semantic_id="idle_neutral",
                library_revision=version.revision,
                workflow_name="autorig_animal_loop_ltx2_19b_v1",
                workflow_fingerprint="2" * 64,
                worker_url="https://worker-4090.example",
                prompt="A horse stands in a neutral idle animation.",
            )
            job = await self.library.create_fitting_job(db, job_request, admin_email="admin@example.com")
            same_job = await self.library.create_fitting_job(db, job_request, admin_email="admin@example.com")
            self.assertEqual(job.id, same_job.id)
            with self.assertRaises(self.library.AnimationLibraryError):
                await self.library.create_fitting_job(
                    db,
                    self.library.AnimationFittingJobCreateRequest(
                        rig_type="horse",
                        semantic_id="idle_neutral",
                        library_revision=version.revision,
                        workflow_name="autorig_animal_loop_ltx2_19b_v1",
                        workflow_fingerprint="2" * 64,
                        worker_url="https://different-worker.example",
                        prompt="Different pinned payload.",
                        prompt_id=job.prompt_id,
                    ),
                    admin_email="admin@example.com",
                )

            failed = await self.library.add_fitting_candidate(
                db,
                job_id=job.id,
                request=self.library.AnimationCandidateCreateRequest(
                    seed=1,
                    fitted_clip_url="https://worker.example/failed.fbx",
                    fitted_clip_sha256="3" * 64,
                    duration=1.0,
                    fps=30,
                    qa_passed=False,
                ),
            )
            with self.assertRaises(self.library.AnimationLibraryError):
                await self.library.add_fitting_candidate(
                    db,
                    job_id=job.id,
                    request=self.library.AnimationCandidateCreateRequest(
                        seed=1,
                        fitted_clip_url="https://worker.example/different.fbx",
                        fitted_clip_sha256="5" * 64,
                        duration=1.0,
                        fps=30,
                        qa_passed=False,
                    ),
                )
            with self.assertRaises(self.library.AnimationLibraryError):
                await self.library.decide_fitting_candidate(
                    db,
                    candidate_id=failed.id,
                    request=self.library.AnimationCandidateDecisionRequest(decision="approve"),
                    admin_email="admin@example.com",
                )

            passed = await self.library.add_fitting_candidate(
                db,
                job_id=job.id,
                request=self.library.AnimationCandidateCreateRequest(
                    seed=2,
                    fitted_clip_url="https://worker.example/passed.fbx",
                    fitted_clip_sha256="4" * 64,
                    duration=1.2,
                    fps=30,
                    qa_passed=True,
                    rank=1,
                    metrics={"foot_slide": 0.01},
                ),
            )
            approved = await self.library.decide_fitting_candidate(
                db,
                candidate_id=passed.id,
                request=self.library.AnimationCandidateDecisionRequest(decision="approve"),
                admin_email="admin@example.com",
            )
            self.assertEqual(approved.decision, "approved")
            await self.library.decide_fitting_candidate(
                db,
                candidate_id=passed.id,
                request=self.library.AnimationCandidateDecisionRequest(decision="approve"),
                admin_email="admin@example.com",
            )
            rows = (await db.execute(select(self.database.AnimalAnimationApprovedClip))).scalars().all()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].semantic_id, "idle_neutral")

    async def test_activation_history_prevents_backfill_and_supports_rollback(self):
        async with self.database.AsyncSessionLocal() as db:
            before_any_activation = self.database.datetime.utcnow()
            first = await self._create_complete_version(db, "horse-library-v1")
            first_activation = await self.library.activate_library_version(
                db,
                rig_type="horse",
                revision=first.revision,
                admin_email="admin@example.com",
                library_root=os.environ["ANIMATION_LIBRARY_ROOT"],
            )
            self.assertIsNone(await self.library.activation_for_task(
                db,
                rig_type="horse",
                task_created_at=before_any_activation,
            ))
            bound_first = await self.library.activation_for_task(
                db,
                rig_type="horse",
                task_created_at=first_activation.activated_at + timedelta(microseconds=1),
            )
            self.assertEqual(bound_first[1].revision, first.revision)

            second = await self._create_complete_version(db, "horse-library-v2")
            second_activation = await self.library.activate_library_version(
                db,
                rig_type="horse",
                revision=second.revision,
                admin_email="admin@example.com",
                library_root=os.environ["ANIMATION_LIBRARY_ROOT"],
            )
            bound_second = await self.library.activation_for_task(
                db,
                rig_type="horse",
                task_created_at=second_activation.activated_at + timedelta(microseconds=1),
            )
            self.assertEqual(bound_second[1].revision, second.revision)

            rollback = await self.library.rollback_library_version(
                db,
                rig_type="horse",
                target_revision=None,
                admin_email="admin@example.com",
                library_root=os.environ["ANIMATION_LIBRARY_ROOT"],
            )
            current = await self.library.current_activation(db, "horse")
            self.assertEqual(rollback.reason, "rollback")
            self.assertEqual(current[1].revision, first.revision)

    async def test_public_manifest_and_glb_use_matrix_revision_hash_and_etag(self):
        async with self.database.AsyncSessionLocal() as db:
            version = await self._create_complete_version(db, "horse-public-v1")
            activation = await self.library.activate_library_version(
                db,
                rig_type="horse",
                revision=version.revision,
                admin_email="admin@example.com",
                library_root=os.environ["ANIMATION_LIBRARY_ROOT"],
            )
            glb_path, glb_sha = self._glb("public-task.glb")
            manifest = self._manifest(version.revision, "front", glb_sha)
            row = {
                "animation_manifest_url": "https://worker.example/manifest.json",
                "animation_glb_url": "https://worker.example/animations.glb",
                "animation_library_revision": version.revision,
                "animation_clip_count": 30,
                "animation_glb_sha256": glb_sha,
                "animation_manifest_sha256": self.library.manifest_sha256(manifest),
            }
            task = self.database.Task(
                id="77777777-2222-3333-4444-555555555555",
                owner_type="user",
                owner_id="owner@example.com",
                input_type="animal",
                status="done",
                guid="77777777-2222-3333-4444-555555555555",
                created_at=activation.activated_at + timedelta(microseconds=1),
                viewer_settings='{"rig_v2_animal_detection":{"animal_type":"horse"}}',
            )
            task.ready_urls = ["https://worker.example/legacy_all_animations.glb"]
            db.add(task)
            await db.commit()

            class DummyResponse:
                status_code = 200
                content = self.library.canonical_json_bytes(manifest)

                @staticmethod
                def json():
                    return manifest

            class DummyClient:
                async def __aenter__(self):
                    return self

                async def __aexit__(self, *_args):
                    return False

                async def get(self, _url):
                    return DummyResponse()

            matrix = {"horse:front": row}
            with (
                patch.object(self.main, "_fetch_animal_variant_matrix", new=AsyncMock(return_value=matrix)),
                patch.object(self.main.httpx, "AsyncClient", return_value=DummyClient()),
            ):
                response = await self.main.api_task_animation_manifest(
                    task.id,
                    request=self._request(),
                    rig_type="horse",
                    orientation="front",
                    db=db,
                )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.headers["x-animation-library-revision"], version.revision)
            manifest_etag = response.headers["etag"]

            with (
                patch.object(self.main, "_fetch_animal_variant_matrix", new=AsyncMock(return_value=matrix)),
                patch.object(self.main.httpx, "AsyncClient", return_value=DummyClient()),
            ):
                not_modified = await self.main.api_task_animation_manifest(
                    task.id,
                    request=self._request(if_none_match=manifest_etag),
                    rig_type="horse",
                    orientation="front",
                    db=db,
                )
            self.assertEqual(not_modified.status_code, 304)

            cache_dir = Path(self._tmp.name) / "glb-cache"
            cache_dir.mkdir()
            cache_name = (
                f"{task.id}_animations_horse_front_{version.revision}_{glb_sha[:16]}.glb"
            )
            (cache_dir / cache_name).write_bytes(glb_path.read_bytes())
            glb_etag = f'"sha256:{glb_sha}"'
            with (
                patch.object(self.main, "GLB_CACHE_DIR", cache_dir),
                patch.object(self.main, "_fetch_animal_variant_matrix", new=AsyncMock(return_value=matrix)),
                patch.object(self.main.httpx, "AsyncClient", return_value=DummyClient()),
            ):
                glb_response = await self.main.api_proxy_animations_glb(
                    task.id,
                    request=self._request(if_none_match=glb_etag),
                    rig_type="horse",
                    orientation="front",
                    db=db,
                )
            self.assertEqual(glb_response.status_code, 304)
            self.assertEqual(glb_response.headers["x-animation-clip-count"], "30")

            # A post-activation task cannot fall back to a stale legacy ready_url.
            with patch.object(self.main, "_fetch_animal_variant_matrix", new=AsyncMock(return_value={})):
                with self.assertRaises(self.main.HTTPException) as missing:
                    await self.main.api_proxy_animations_glb(
                        task.id,
                        request=self._request(),
                        rig_type="horse",
                        orientation="front",
                        db=db,
                    )
            self.assertEqual(missing.exception.status_code, 404)


if __name__ == "__main__":
    unittest.main()
