import asyncio
import hashlib
import importlib
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
import uuid

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import create_async_engine


class AnimalAnimationGenericClipContractTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory(prefix="autorig-animal-generic-clips-")
        cls.root = Path(cls._tmp.name)
        os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{cls.root / 'test.db'}"
        os.environ["ANIMATION_LIBRARY_ROOT"] = str(cls.root / "library")
        os.environ["ANIMATION_FITTING_JOBS_ROOT"] = str(cls.root / "jobs")
        (cls.root / "library").mkdir(parents=True)
        (cls.root / "jobs").mkdir(parents=True)
        backend_dir = str(Path(__file__).resolve().parents[1])
        if backend_dir not in sys.path:
            sys.path.insert(0, backend_dir)
        for name in ("animal_animation_library", "database", "config"):
            sys.modules.pop(name, None)
        cls.database = importlib.import_module("database")
        cls.library = importlib.import_module("animal_animation_library")

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

    @staticmethod
    def _sha(label: str) -> str:
        return hashlib.sha256(label.encode("utf-8")).hexdigest()

    def _clip_contract(self, revision: str, index: int) -> dict:
        canonical = self.library.ANIMAL_CLIPS[index]
        return {
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
                "candidate_id": str(
                    uuid.uuid5(uuid.NAMESPACE_URL, f"{revision}:{canonical['id']}")
                ),
                "candidate_bundle_sha256": self._sha(f"bundle:{revision}:{index}"),
                "human_review_sha256": self._sha(f"review:{revision}:{index}"),
                "selection_receipt_sha256": self._sha(f"selection:{revision}:{index}"),
                "validation_receipt_sha256": self._sha(f"validation:{revision}:{index}"),
            },
            "clip_artifact": {
                "format": self.library.CLIP_ARTIFACT_FORMAT_THREEJS_JSON,
                "sha256": self._sha(f"clip:{revision}:{index}"),
            },
        }

    def _manifest_v2(
        self,
        revision: str,
        orientation: str,
        artifact_sha256: str,
        package_result_sha256: str,
    ) -> dict:
        return {
            "schema": self.library.MANIFEST_V2_SCHEMA_ID,
            "library_revision": revision,
            "rig_type": "horse",
            "orientation": orientation,
            "template_skeleton_sha256": "1" * 64,
            "artifact_sha256": artifact_sha256,
            "package_result_sha256": package_result_sha256,
            "clips": [self._clip_contract(revision, index) for index in range(30)],
            "poses": list(self.library.TAXONOMY["poses"]),
        }

    def _manifest_v1(self, revision: str, orientation: str, artifact_sha256: str) -> dict:
        clips = []
        for index in range(30):
            row = self._clip_contract(revision, index)
            row.pop("clip_artifact")
            row["provenance"] = {"candidate_id": row["provenance"]["candidate_id"]}
            row["fbx_url"] = f"https://worker.example/{row['id']}.fbx"
            clips.append(row)
        return {
            "schema": self.library.MANIFEST_V1_SCHEMA_ID,
            "library_revision": revision,
            "rig_type": "horse",
            "orientation": orientation,
            "template_skeleton_sha256": "1" * 64,
            "artifact_sha256": artifact_sha256,
            "clips": clips,
            "poses": list(self.library.TAXONOMY["poses"]),
        }

    def _visual_metrics(self, semantic_id: str, clip_sha256: str) -> dict:
        return {
            "visual_phase_gate": {
                "schema": self.library.VISUAL_PHASE_QA_SCHEMA_ID,
                "version": self.library.VISUAL_PHASE_QA_VERSION,
                "rig_type": "horse",
                "semantic_id": semantic_id,
                "fitted_clip_sha256": clip_sha256,
                "decision": "PASS",
                "camera": {
                    "static": True,
                    "projection": "orthographic",
                    "view": "side",
                    "root_motion_locked": True,
                    "settings_sha256": "a" * 64,
                },
                "coincident_rest_vertex_separation": {
                    "measured": True,
                    "pass": True,
                    "threshold_m": 0.04,
                    "max_separation_m": 0.001,
                    "sample_count": 5,
                    "group_count": 1,
                    "report_url": "https://worker.example/separation.json",
                    "report_sha256": "b" * 64,
                },
                "required_phases": ["start", "middle", "three_quarter"],
                "frames": [
                    {
                        "phase": phase,
                        "frame_index": index * 10,
                        "evidence_url": f"https://worker.example/{semantic_id}/{phase}.png",
                        "sha256": f"{index + 11:064x}",
                    }
                    for index, phase in enumerate(("start", "middle", "three_quarter"))
                ],
                "reviewer": {
                    "id": "reviewer@example.com",
                    "reviewed_at": "2026-07-16T00:00:00Z",
                },
            }
        }

    def _glb(self, name: str) -> tuple[Path, str]:
        path = self.root / "library" / name
        gltf = {
            "asset": {"version": "2.0"},
            "meshes": [
                {"primitives": [{"attributes": {"POSITION": 0, "JOINTS_0": 1, "WEIGHTS_0": 2}}]}
            ],
            "skins": [{"joints": [0]}],
            "nodes": [{"mesh": 0, "skin": 0}],
            "accessors": [{"min": [0.0], "max": [1.0]} for _ in range(30)],
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
        total_length = 20 + len(json_chunk)
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

    @staticmethod
    def _descriptor(path: Path) -> dict:
        payload = path.read_bytes()
        return {
            "path": str(path.resolve()),
            "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
        }

    def _package_result(
        self,
        revision: str,
        orientation: str,
        artifact_path: Path,
    ) -> dict:
        source_path = self.root / "library" / f"{revision}-{orientation}-source.glb"
        input_manifest_path = (
            self.root / "library" / f"{revision}-{orientation}-package-input.json"
        )
        source_path.write_bytes(f"source:{revision}:{orientation}".encode("utf-8"))
        input_manifest_path.write_bytes(
            f'{{"revision":"{revision}","orientation":"{orientation}"}}\n'.encode(
                "utf-8"
            )
        )
        return {
            "schema": self.library.PACKAGE_RESULT_SCHEMA_ID,
            "library_revision": revision,
            "rig_type": "horse",
            "orientation": orientation,
            "template_skeleton_sha256": "1" * 64,
            "taxonomy": self._descriptor(Path(self.library.TAXONOMY_PATH)),
            "source": self._descriptor(source_path),
            "input_manifest": self._descriptor(input_manifest_path),
            "animation_count": 30,
            "source_bin_prefix_bytes": 1,
            "output": self._descriptor(artifact_path),
            "clips": [
                {
                    "semantic_id": self.library.ANIMAL_CLIP_IDS[index],
                    "path": str(
                        (self.root / "jobs" / f"{self.library.ANIMAL_CLIP_IDS[index]}.json").resolve()
                    ),
                    "bytes": 100 + index,
                    "sha256": self._sha(f"clip:{revision}:{index}"),
                    "duration": 1.0,
                    "track_count": index + 1,
                    "candidate_id": str(
                        uuid.uuid5(
                            uuid.NAMESPACE_URL,
                            f"{revision}:{self.library.ANIMAL_CLIP_IDS[index]}",
                        )
                    ),
                    "candidate_bundle_sha256": self._sha(f"bundle:{revision}:{index}"),
                    "human_review_sha256": self._sha(f"review:{revision}:{index}"),
                }
                for index in range(30)
            ],
        }

    def test_manifest_dispatch_keeps_v1_fbx_only_and_v2_url_optional(self):
        package_sha = "2" * 64
        manifest = self._manifest_v2("horse-browser-v2", "front", "3" * 64, package_sha)
        validated = self.library.validate_animation_manifest(manifest)
        self.assertNotIn("url", validated["clips"][0]["clip_artifact"])
        self.assertEqual(
            validated["clips"][0]["clip_artifact"]["format"],
            self.library.CLIP_ARTIFACT_FORMAT_THREEJS_JSON,
        )

        invalid_format = json.loads(json.dumps(manifest))
        invalid_format["clips"][0]["clip_artifact"]["format"] = "glb"
        with self.assertRaises(self.library.AnimationLibraryError):
            self.library.validate_animation_manifest(invalid_format)

        unsupported = json.loads(json.dumps(manifest))
        unsupported["schema"] = "animal-animation-manifest.v3"
        with self.assertRaises(self.library.AnimationLibraryError):
            self.library.validate_animation_manifest(unsupported)

        v1 = {
            key: value
            for key, value in manifest.items()
            if key != "package_result_sha256"
        }
        v1["schema"] = self.library.MANIFEST_V1_SCHEMA_ID
        for clip in v1["clips"]:
            clip.pop("clip_artifact")
            clip["provenance"] = {"candidate_id": clip["provenance"]["candidate_id"]}
        with self.assertRaisesRegex(self.library.AnimationLibraryError, "fbx_url is required"):
            self.library.validate_animation_manifest(v1)
        for clip in v1["clips"]:
            clip["fbx_url"] = f"https://worker.example/{clip['id']}.fbx"
        self.assertEqual(
            self.library.validate_animation_manifest(v1)["schema"],
            self.library.MANIFEST_V1_SCHEMA_ID,
        )

    def test_candidate_binding_preserves_legacy_aliases_and_requires_review_pins(self):
        clip_sha = self._sha("browser-clip")
        bundle_sha = self._sha("candidate-bundle")
        review_sha = self._sha("human-review")
        candidate = self.database.AnimalAnimationCandidate(
            id=str(uuid.uuid4()),
            job_id=str(uuid.uuid4()),
            seed=1,
            fitted_clip_path=str(self.root / "jobs" / "walk.json"),
            fitted_clip_sha256=clip_sha,
            fitted_clip_format=self.library.CLIP_ARTIFACT_FORMAT_THREEJS_JSON,
            candidate_bundle_sha256=bundle_sha,
            human_review_sha256=review_sha,
        )
        approved = self.database.AnimalAnimationApprovedClip()
        contract = self.library.bind_approved_clip_artifact(approved, candidate)
        self.assertEqual(contract.sha256, clip_sha)
        self.assertEqual(approved.clip_artifact_sha256, clip_sha)
        self.assertEqual(approved.fbx_sha256, clip_sha)
        self.assertEqual(approved.clip_artifact_path, approved.fbx_path)

        candidate.human_review_sha256 = None
        with self.assertRaisesRegex(
            self.library.AnimationLibraryError, "must be provided together"
        ):
            self.library.resolve_candidate_clip_artifact(candidate)

        legacy = self.database.AnimalAnimationApprovedClip(
            fbx_url="https://worker.example/legacy.fbx",
            fbx_sha256="c" * 64,
        )
        legacy_contract = self.library.resolve_approved_clip_artifact(legacy)
        self.assertEqual(legacy_contract.format, self.library.CLIP_ARTIFACT_FORMAT_FBX)
        self.assertEqual(legacy_contract.sha256, "c" * 64)

    async def test_legacy_sqlite_migration_is_idempotent_and_backfills_aliases(self):
        legacy_path = self.root / "legacy.db"
        engine = create_async_engine(f"sqlite+aiosqlite:///{legacy_path}")
        try:
            async with engine.begin() as conn:
                await conn.exec_driver_sql(
                    """
                    CREATE TABLE animal_animation_candidates (
                        id VARCHAR(36) PRIMARY KEY,
                        fitted_clip_url VARCHAR(2048),
                        fitted_clip_path VARCHAR(2048),
                        fitted_clip_sha256 VARCHAR(64)
                    )
                    """
                )
                await conn.exec_driver_sql(
                    """
                    CREATE TABLE animal_animation_approved_clips (
                        id INTEGER PRIMARY KEY,
                        fbx_url VARCHAR(2048),
                        fbx_path VARCHAR(2048),
                        fbx_sha256 VARCHAR(64) NOT NULL
                    )
                    """
                )
                await conn.exec_driver_sql(
                    "CREATE TABLE animal_animation_library_artifacts (id INTEGER PRIMARY KEY)"
                )
                await conn.exec_driver_sql(
                    "INSERT INTO animal_animation_candidates VALUES ('candidate', 'https://old/c.fbx', NULL, ?)",
                    ("a" * 64,),
                )
                await conn.exec_driver_sql(
                    "INSERT INTO animal_animation_approved_clips VALUES (1, 'https://old/c.fbx', '/old/c.fbx', ?)",
                    ("b" * 64,),
                )
                await self.database.migrate_animal_animation_contract(conn)
                await self.database.migrate_animal_animation_contract(conn)
                candidate = (
                    await conn.exec_driver_sql(
                        "SELECT fitted_clip_format, candidate_bundle_sha256, human_review_sha256 "
                        "FROM animal_animation_candidates"
                    )
                ).one()
                approved = (
                    await conn.exec_driver_sql(
                        "SELECT clip_artifact_format, clip_artifact_url, "
                        "clip_artifact_path, clip_artifact_sha256 "
                        "FROM animal_animation_approved_clips"
                    )
                ).one()
                approved_table_info = {
                    row[1]: row
                    for row in (
                        await conn.exec_driver_sql(
                            "PRAGMA table_info(animal_animation_approved_clips)"
                        )
                    ).all()
                }
                artifact_columns = {
                    row[1]
                    for row in (
                        await conn.exec_driver_sql(
                            "PRAGMA table_info(animal_animation_library_artifacts)"
                        )
                    ).all()
                }
            self.assertEqual(candidate, ("fbx", None, None))
            self.assertEqual(approved, ("fbx", "https://old/c.fbx", "/old/c.fbx", "b" * 64))
            self.assertEqual(approved_table_info["clip_artifact_format"][3], 1)
            # SQLite cannot add NOT NULL SHA to a populated legacy table; the
            # value is backfilled and ORM/activation enforce the invariant.
            self.assertEqual(approved_table_info["clip_artifact_sha256"][3], 0)
            self.assertFalse(
                self.database.AnimalAnimationApprovedClip.__table__
                .c.clip_artifact_sha256.nullable
            )
            self.assertIn("package_result_path", artifact_columns)
            self.assertIn("package_result_sha256", artifact_columns)
        finally:
            await engine.dispose()

    async def test_v2_activation_crosspins_generic_clip_and_package_result(self):
        revision = "horse-browser-activation-v2"
        async with self.database.AsyncSessionLocal() as db:
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
            for index, canonical in enumerate(self.library.ANIMAL_CLIPS):
                clip = self._clip_contract(revision, index)
                clip_path = self.root / "jobs" / f"{canonical['id']}.json"
                db.add(
                    self.database.AnimalAnimationApprovedClip(
                        library_version_id=version.id,
                        candidate_id=clip["provenance"]["candidate_id"],
                        semantic_id=canonical["id"],
                        category=canonical["category"],
                        clip_order=canonical["order"],
                        loop=canonical["loop"],
                        duration=1.0,
                        fps=30.0,
                        start_pose_id=canonical["start_pose_id"],
                        end_pose_id=canonical["end_pose_id"],
                        root_motion_available=False,
                        qa_profile_revision="horse_qa_profile_v1",
                        clip_artifact_format=self.library.CLIP_ARTIFACT_FORMAT_THREEJS_JSON,
                        clip_artifact_path=str(clip_path),
                        clip_artifact_sha256=clip["clip_artifact"]["sha256"],
                        candidate_bundle_sha256=clip["provenance"]["candidate_bundle_sha256"],
                        human_review_sha256=clip["provenance"]["human_review_sha256"],
                        fbx_path=str(clip_path),
                        fbx_sha256=clip["clip_artifact"]["sha256"],
                        metrics_json=json.dumps(
                            self._visual_metrics(
                                canonical["id"], clip["clip_artifact"]["sha256"]
                            ),
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        provenance_json=json.dumps(
                            clip["provenance"], sort_keys=True, separators=(",", ":")
                        ),
                        approved_by="admin@example.com",
                    )
                )
            await db.commit()

            artifact_inputs = []
            for orientation in self.library.ANIMAL_ORIENTATIONS:
                glb_path, glb_sha = self._glb(f"{revision}-{orientation}.glb")
                package_result = self._package_result(revision, orientation, glb_path)
                package_bytes = (
                    json.dumps(package_result, sort_keys=True, separators=(",", ":")) + "\n"
                ).encode("utf-8")
                package_path = self.root / "library" / f"{revision}-{orientation}.result.json"
                package_path.write_bytes(package_bytes)
                package_sha = hashlib.sha256(package_bytes).hexdigest()
                artifact_inputs.append({
                    "orientation": orientation,
                    "glb_path": glb_path,
                    "glb_sha": glb_sha,
                    "package_path": package_path,
                    "package_sha": package_sha,
                })
                await self.library.put_library_artifact(
                    db,
                    rig_type="horse",
                    revision=revision,
                    orientation=orientation,
                    request=self.library.AnimationLibraryArtifactPutRequest(
                        manifest=self._manifest_v2(
                            revision, orientation, glb_sha, package_sha
                        ),
                        animation_glb_path=str(glb_path),
                        animation_glb_url=f"https://worker.example/{orientation}/animations.glb",
                        artifact_sha256=glb_sha,
                        package_result_path=str(package_path),
                        package_result_sha256=package_sha,
                    ),
                    library_root=str(self.root / "library"),
                )

            approved_rows = list((
                await db.execute(
                    select(self.database.AnimalAnimationApprovedClip)
                    .where(
                        self.database.AnimalAnimationApprovedClip.library_version_id
                        == version.id
                    )
                    .order_by(self.database.AnimalAnimationApprovedClip.clip_order)
                )
            ).scalars().all())
            first = approved_rows[0]
            front_artifact = (
                await db.execute(
                    select(self.database.AnimalAnimationLibraryArtifact).where(
                        self.database.AnimalAnimationLibraryArtifact.library_version_id
                        == version.id,
                        self.database.AnimalAnimationLibraryArtifact.orientation == "front",
                    )
                )
            ).scalar_one()
            full_package_result = json.loads(
                Path(front_artifact.package_result_path).read_text("utf-8")
            )
            self.library.validate_package_result_contract(
                full_package_result,
                version=version,
                artifact=front_artifact,
                approved=approved_rows,
            )
            reduced_package_result = json.loads(json.dumps(full_package_result))
            reduced_package_result.pop("taxonomy")
            with self.assertRaisesRegex(
                self.library.AnimationLibraryError, "invalid fields"
            ):
                self.library.validate_package_result_contract(
                    reduced_package_result,
                    version=version,
                    artifact=front_artifact,
                    approved=approved_rows,
                )
            tampered_taxonomy = json.loads(json.dumps(full_package_result))
            tampered_taxonomy["taxonomy"]["sha256"] = "d" * 64
            with self.assertRaisesRegex(
                self.library.AnimationLibraryError, "checked-in taxonomy"
            ):
                self.library.validate_package_result_contract(
                    tampered_taxonomy,
                    version=version,
                    artifact=front_artifact,
                    approved=approved_rows,
                )
            tampered_output = json.loads(json.dumps(full_package_result))
            tampered_output["output"]["bytes"] += 1
            with self.assertRaisesRegex(
                self.library.AnimationLibraryError, "GLB byte count mismatch"
            ):
                self.library.validate_package_result_contract(
                    tampered_output,
                    version=version,
                    artifact=front_artifact,
                    approved=approved_rows,
                )
            tampered_clip_descriptor = json.loads(json.dumps(full_package_result))
            tampered_clip_descriptor["clips"][0]["sha256"] = "d" * 64
            with self.assertRaisesRegex(
                self.library.AnimationLibraryError, "sha256 differs"
            ):
                self.library.validate_package_result_contract(
                    tampered_clip_descriptor,
                    version=version,
                    artifact=front_artifact,
                    approved=approved_rows,
                )
            incomplete_clip_descriptor = json.loads(json.dumps(full_package_result))
            incomplete_clip_descriptor["clips"][0].pop("track_count")
            with self.assertRaisesRegex(
                self.library.AnimationLibraryError, "invalid fields"
            ):
                self.library.validate_package_result_contract(
                    incomplete_clip_descriptor,
                    version=version,
                    artifact=front_artifact,
                    approved=approved_rows,
                )

            first.clip_artifact_format = self.library.CLIP_ARTIFACT_FORMAT_FBX
            await db.commit()
            with self.assertRaisesRegex(
                self.library.AnimationLibraryError,
                "animal-animation-manifest.v2 cannot activate fbx clips",
            ):
                await self.library.activate_library_version(
                    db,
                    rig_type="horse",
                    revision=revision,
                    admin_email="admin@example.com",
                    library_root=str(self.root / "library"),
                )
            first.clip_artifact_format = self.library.CLIP_ARTIFACT_FORMAT_THREEJS_JSON
            await db.commit()

            for item in artifact_inputs:
                await self.library.put_library_artifact(
                    db,
                    rig_type="horse",
                    revision=revision,
                    orientation=item["orientation"],
                    request=self.library.AnimationLibraryArtifactPutRequest(
                        manifest=self._manifest_v1(
                            revision, item["orientation"], item["glb_sha"]
                        ),
                        animation_glb_path=str(item["glb_path"]),
                        animation_glb_url=(
                            f"https://worker.example/{item['orientation']}/animations.glb"
                        ),
                        artifact_sha256=item["glb_sha"],
                    ),
                    library_root=str(self.root / "library"),
                )
            with self.assertRaisesRegex(
                self.library.AnimationLibraryError,
                "animal-animation-manifest.v1 cannot activate threejs-animation-json.v1 clips",
            ):
                await self.library.activate_library_version(
                    db,
                    rig_type="horse",
                    revision=revision,
                    admin_email="admin@example.com",
                    library_root=str(self.root / "library"),
                )
            for approved_clip in approved_rows:
                approved_clip.clip_artifact_format = self.library.CLIP_ARTIFACT_FORMAT_FBX
            first.qa_profile_revision = "tampered-v1-qa-profile"
            await db.commit()
            with self.assertRaisesRegex(
                self.library.AnimationLibraryError, "QA profile revision differs"
            ):
                await self.library.activate_library_version(
                    db,
                    rig_type="horse",
                    revision=revision,
                    admin_email="admin@example.com",
                    library_root=str(self.root / "library"),
                )
            first.qa_profile_revision = "horse_qa_profile_v1"
            for approved_clip in approved_rows:
                approved_clip.clip_artifact_format = (
                    self.library.CLIP_ARTIFACT_FORMAT_THREEJS_JSON
                )
            await db.commit()
            for item in artifact_inputs:
                await self.library.put_library_artifact(
                    db,
                    rig_type="horse",
                    revision=revision,
                    orientation=item["orientation"],
                    request=self.library.AnimationLibraryArtifactPutRequest(
                        manifest=self._manifest_v2(
                            revision,
                            item["orientation"],
                            item["glb_sha"],
                            item["package_sha"],
                        ),
                        animation_glb_path=str(item["glb_path"]),
                        animation_glb_url=(
                            f"https://worker.example/{item['orientation']}/animations.glb"
                        ),
                        artifact_sha256=item["glb_sha"],
                        package_result_path=str(item["package_path"]),
                        package_result_sha256=item["package_sha"],
                    ),
                    library_root=str(self.root / "library"),
                )

            original_qa_profile_revision = first.qa_profile_revision
            first.qa_profile_revision = "tampered-qa-profile"
            await db.commit()
            with self.assertRaisesRegex(
                self.library.AnimationLibraryError, "QA profile revision differs"
            ):
                await self.library.activate_library_version(
                    db,
                    rig_type="horse",
                    revision=revision,
                    admin_email="admin@example.com",
                    library_root=str(self.root / "library"),
                )
            first.qa_profile_revision = original_qa_profile_revision
            await db.commit()

            original_provenance_json = first.provenance_json
            missing_extra_provenance = json.loads(original_provenance_json)
            missing_extra_provenance.pop("selection_receipt_sha256")
            first.provenance_json = json.dumps(
                missing_extra_provenance, sort_keys=True, separators=(",", ":")
            )
            await db.commit()
            with self.assertRaisesRegex(
                self.library.AnimationLibraryError, "full provenance differs"
            ):
                await self.library.activate_library_version(
                    db,
                    rig_type="horse",
                    revision=revision,
                    admin_email="admin@example.com",
                    library_root=str(self.root / "library"),
                )

            tampered_extra_provenance = json.loads(original_provenance_json)
            tampered_extra_provenance["validation_receipt_sha256"] = "e" * 64
            first.provenance_json = json.dumps(
                tampered_extra_provenance, sort_keys=True, separators=(",", ":")
            )
            await db.commit()
            with self.assertRaisesRegex(
                self.library.AnimationLibraryError, "full provenance differs"
            ):
                await self.library.activate_library_version(
                    db,
                    rig_type="horse",
                    revision=revision,
                    admin_email="admin@example.com",
                    library_root=str(self.root / "library"),
                )

            first.provenance_json = original_provenance_json
            await db.commit()
            original_bundle_sha = first.candidate_bundle_sha256
            first.candidate_bundle_sha256 = "f" * 64
            await db.commit()
            with self.assertRaisesRegex(
                self.library.AnimationLibraryError, "provenance pins"
            ):
                await self.library.activate_library_version(
                    db,
                    rig_type="horse",
                    revision=revision,
                    admin_email="admin@example.com",
                    library_root=str(self.root / "library"),
                )

            first.candidate_bundle_sha256 = original_bundle_sha
            await db.commit()
            activation = await self.library.activate_library_version(
                db,
                rig_type="horse",
                revision=revision,
                admin_email="admin@example.com",
                library_root=str(self.root / "library"),
            )
            self.assertEqual(activation.library_version_id, version.id)
            resolved = self.library.resolve_approved_clip_artifact(first)
            self.assertEqual(
                resolved.format, self.library.CLIP_ARTIFACT_FORMAT_THREEJS_JSON
            )
            self.assertIsNone(resolved.url)


if __name__ == "__main__":
    unittest.main()
