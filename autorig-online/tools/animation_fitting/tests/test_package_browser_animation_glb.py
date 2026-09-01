from __future__ import annotations

import hashlib
import io
import json
import math
from pathlib import Path
import struct
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch


TOOLS_ROOT = Path(__file__).resolve().parents[2]
if str(TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(TOOLS_ROOT))

import animation_fitting.package_browser_animation_glb as packager_module  # noqa: E402
from animation_fitting.package_browser_animation_glb import (  # noqa: E402
    INPUT_SCHEMA,
    OUTPUT_SCHEMA,
    PackageError,
    TAXONOMY_PATH,
    load_animal_taxonomy,
    main,
    package_browser_animation_glb,
)


TAXONOMY_PIN, ANIMAL_CLIP_IDS, TAXONOMY_VALUE = load_animal_taxonomy()
LIBRARY_REVISION = "horse-browser-library-v1"
TEMPLATE_SKELETON_SHA256 = "a" * 64


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _pin(path: Path) -> dict:
    data = path.read_bytes()
    return {"path": path.name, "bytes": len(data), "sha256": _sha(data)}


def _glb(gltf: dict, binary: bytes) -> bytes:
    json_bytes = json.dumps(gltf, sort_keys=True, separators=(",", ":")).encode("utf-8")
    json_bytes += b" " * ((-len(json_bytes)) % 4)
    binary += b"\x00" * ((-len(binary)) % 4)
    length = 12 + 8 + len(json_bytes) + 8 + len(binary)
    return b"".join(
        (
            struct.pack("<4sII", b"glTF", 2, length),
            struct.pack("<I4s", len(json_bytes), b"JSON"),
            json_bytes,
            struct.pack("<I4s", len(binary), b"BIN\x00"),
            binary,
        )
    )


def _parse_glb(data: bytes) -> tuple[dict, bytes]:
    assert data[:4] == b"glTF"
    offset = 12
    json_length, json_type = struct.unpack_from("<I4s", data, offset)
    assert json_type == b"JSON"
    offset += 8
    gltf = json.loads(data[offset : offset + json_length].rstrip(b" \x00"))
    offset += json_length
    binary_length, binary_type = struct.unpack_from("<I4s", data, offset)
    assert binary_type == b"BIN\x00"
    offset += 8
    return gltf, data[offset : offset + binary_length]


def _source_gltf(
    marker: str = "front",
    *,
    rig_type: str = "horse",
    template_skeleton_sha256: str = TEMPLATE_SKELETON_SHA256,
) -> tuple[dict, bytes]:
    binary = b"SOURCE-BIN-00001"
    assert len(binary) == 16
    gltf = {
        "asset": {
            "version": "2.0",
            "generator": "unit",
            "extras": {
                "sourceRigType": rig_type,
                "sourceOrientation": marker,
                "templateSkeletonSha256": template_skeleton_sha256,
                "fixtureMarker": "must-be-preserved",
            },
        },
        "scene": 0,
        "scenes": [{"nodes": [0]}],
        "nodes": [
            {"name": "Model", "mesh": 0, "skin": 0, "children": [1]},
            {"name": "Root", "children": [2]},
            {"name": "Spine", "translation": [0, 0, 1]},
        ],
        "skins": [
            {"name": "Rig", "joints": [1, 2], "skeleton": 1, "inverseBindMatrices": 0}
        ],
        "meshes": [
            {
                "name": "Body",
                "primitives": [
                    {
                        "attributes": {
                            "POSITION": 1,
                            "NORMAL": 2,
                            "TEXCOORD_0": 3,
                            "JOINTS_0": 4,
                            "WEIGHTS_0": 5,
                        },
                        "indices": 6,
                        "material": 0,
                    }
                ],
            }
        ],
        "materials": [
            {"name": "Coat", "pbrMetallicRoughness": {"baseColorTexture": {"index": 0}}}
        ],
        "textures": [{"source": 0, "sampler": 0}],
        "samplers": [{"magFilter": 9729, "minFilter": 9987}],
        "images": [{"name": "CoatImage", "bufferView": 0, "mimeType": "image/png"}],
        "buffers": [{"byteLength": len(binary)}],
        "bufferViews": [{"buffer": 0, "byteOffset": 0, "byteLength": len(binary)}],
        "accessors": [
            {"bufferView": 0, "componentType": 5126, "count": 1, "type": "MAT4"},
            {"bufferView": 0, "componentType": 5126, "count": 1, "type": "VEC3"},
            {"bufferView": 0, "componentType": 5126, "count": 1, "type": "VEC3"},
            {"bufferView": 0, "componentType": 5126, "count": 1, "type": "VEC2"},
            {"bufferView": 0, "componentType": 5123, "count": 1, "type": "VEC4"},
            {"bufferView": 0, "componentType": 5126, "count": 1, "type": "VEC4"},
            {"bufferView": 0, "componentType": 5123, "count": 3, "type": "SCALAR"},
        ],
        "extras": {"immutableSourceMarker": marker},
    }
    return gltf, binary


def _clip(semantic_id: str, index: int) -> dict:
    angle = 0.1 + index * 0.001
    return {
        "name": f"Browser_{semantic_id}",
        "duration": 1.0,
        "uuid": f"clip-{index}",
        "blendMode": 2500,
        "userData": {"approved": True},
        "tracks": [
            {
                "name": "Spine.quaternion",
                "type": "quaternion",
                "times": [0.0, 1.0],
                "values": [0, 0, 0, 1, 0, 0, math.sin(angle / 2), math.cos(angle / 2)],
            },
            {
                "name": "Root.position",
                "type": "vector",
                "times": [0.0, 1.0],
                "values": [0, 0, 0, 0.01 + index * 0.0001, 0, 0],
            },
        ],
    }


def _approval(index: int) -> dict:
    return {
        "candidate_id": f"00000000-0000-4000-8000-{index + 1:012x}",
        "candidate_bundle_sha256": _sha(f"candidate-bundle-{index}".encode()),
        "human_review_sha256": _sha(f"human-review-{index}".encode()),
    }


class Fixture:
    def __init__(
        self,
        root: Path,
        marker: str = "front",
        *,
        rig_type: str = "horse",
        template_skeleton_sha256: str = TEMPLATE_SKELETON_SHA256,
    ) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.rig_type = rig_type
        self.template_skeleton_sha256 = template_skeleton_sha256
        self.source_json, self.source_bin = _source_gltf(
            marker,
            rig_type=rig_type,
            template_skeleton_sha256=template_skeleton_sha256,
        )
        self.source = root / f"source-{marker}.glb"
        self.source.write_bytes(_glb(self.source_json, self.source_bin))
        self.clip_paths: list[Path] = []
        for index, semantic_id in enumerate(ANIMAL_CLIP_IDS):
            path = root / f"{index:02d}-{semantic_id}.json"
            path.write_text(
                json.dumps(_clip(semantic_id, index), sort_keys=True), encoding="utf-8"
            )
            self.clip_paths.append(path)

    def manifest(
        self,
        *,
        orientation: str = "front",
        rig_type: str | None = None,
        library_revision: str = LIBRARY_REVISION,
        template_skeleton_sha256: str | None = None,
        rows: list[dict] | None = None,
        name: str = "input.json",
    ) -> tuple[Path, str]:
        source_pin = _pin(self.source)
        source_pin["orientation"] = orientation
        clip_rows = rows or [
            {"semantic_id": semantic_id, **_pin(path), **_approval(index)}
            for index, (semantic_id, path) in enumerate(
                zip(ANIMAL_CLIP_IDS, self.clip_paths)
            )
        ]
        path = self.root / name
        path.write_text(
            json.dumps(
                {
                    "schema": INPUT_SCHEMA,
                    "taxonomy": {
                        "path": str(TAXONOMY_PATH),
                        "bytes": TAXONOMY_PIN.size,
                        "sha256": TAXONOMY_PIN.sha256,
                    },
                    "library_revision": library_revision,
                    "rig_type": rig_type or self.rig_type,
                    "template_skeleton_sha256": (
                        template_skeleton_sha256 or self.template_skeleton_sha256
                    ),
                    "source": source_pin,
                    "clips": clip_rows,
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        return path, _sha(path.read_bytes())


def _rows(fixture: Fixture) -> list[dict]:
    return [
        {"semantic_id": semantic_id, **_pin(path), **_approval(index)}
        for index, (semantic_id, path) in enumerate(
            zip(ANIMAL_CLIP_IDS, fixture.clip_paths)
        )
    ]


class PackageBrowserAnimationGlbTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="autorig-browser-glb-")
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_packages_exact_30_deterministically_and_preserves_source(self) -> None:
        fixture = Fixture(self.root)
        manifest, manifest_sha = fixture.manifest()
        first_path = self.root / "front-animations.glb"
        second_path = self.root / "front-animations-copy.glb"
        first_manifest = self.root / "front-animations.result.json"
        second_manifest = self.root / "front-animations-copy.result.json"
        first = package_browser_animation_glb(
            input_manifest=manifest,
            input_manifest_sha256=manifest_sha,
            output=first_path,
            result_manifest=first_manifest,
        )
        second = package_browser_animation_glb(
            input_manifest=manifest,
            input_manifest_sha256=manifest_sha,
            output=second_path,
            result_manifest=second_manifest,
        )
        self.assertEqual(first["schema"], OUTPUT_SCHEMA)
        self.assertEqual(first["orientation"], "front")
        self.assertEqual(first["animation_count"], 30)
        self.assertEqual(first_path.read_bytes(), second_path.read_bytes())
        self.assertEqual(first["output"]["sha256"], second["output"]["sha256"])
        persisted = json.loads(first_manifest.read_text(encoding="utf-8"))
        self.assertEqual(persisted["schema"], OUTPUT_SCHEMA)
        self.assertEqual(persisted["taxonomy"]["sha256"], TAXONOMY_PIN.sha256)
        self.assertEqual(persisted["library_revision"], LIBRARY_REVISION)
        self.assertNotEqual(LIBRARY_REVISION, TAXONOMY_VALUE["revision"])
        self.assertEqual(persisted["rig_type"], "horse")
        self.assertEqual(
            persisted["template_skeleton_sha256"], TEMPLATE_SKELETON_SHA256
        )
        self.assertEqual(len(persisted["clips"]), 30)
        self.assertEqual(
            persisted["clips"][0]["candidate_id"], _approval(0)["candidate_id"]
        )
        self.assertEqual(
            persisted["clips"][0]["candidate_bundle_sha256"],
            _approval(0)["candidate_bundle_sha256"],
        )
        self.assertEqual(
            persisted["clips"][0]["human_review_sha256"],
            _approval(0)["human_review_sha256"],
        )
        self.assertEqual(persisted["output"]["sha256"], _sha(first_path.read_bytes()))
        self.assertEqual(
            first["result_manifest"]["sha256"], _sha(first_manifest.read_bytes())
        )

        output_json, output_bin = _parse_glb(first_path.read_bytes())
        source_json, source_bin = _parse_glb(fixture.source.read_bytes())
        self.assertTrue(output_bin.startswith(source_bin))
        self.assertEqual(first["source_bin_prefix_bytes"], len(source_bin))
        for key in source_json:
            if key not in {"accessors", "bufferViews", "buffers", "animations"}:
                self.assertEqual(output_json[key], source_json[key], key)
        self.assertEqual(
            output_json["accessors"][: len(source_json["accessors"])],
            source_json["accessors"],
        )
        self.assertEqual(
            output_json["bufferViews"][: len(source_json["bufferViews"])],
            source_json["bufferViews"],
        )
        self.assertEqual(
            [row["name"] for row in output_json["animations"]], list(ANIMAL_CLIP_IDS)
        )
        self.assertEqual(
            output_json["asset"]["extras"]["fixtureMarker"], "must-be-preserved"
        )
        self.assertTrue(
            all(
                row["samplers"] and row["channels"] for row in output_json["animations"]
            )
        )
        self.assertEqual(
            output_json["animations"][0]["channels"][0]["target"]["node"], 1
        )
        self.assertEqual(
            output_json["animations"][0]["channels"][1]["target"]["node"], 2
        )
        self.assertEqual(output_json["buffers"][0]["byteLength"], len(output_bin))

    def test_front_and_back_are_separate_source_invocations_without_relabel_option(
        self,
    ) -> None:
        front = Fixture(self.root / "front", "front")
        back = Fixture(self.root / "back", "back")
        front_manifest, front_sha = front.manifest(orientation="front")
        back_manifest, back_sha = back.manifest(orientation="back")
        front_output = front.root / "animations.glb"
        back_output = back.root / "animations.glb"
        front_result = package_browser_animation_glb(
            input_manifest=front_manifest,
            input_manifest_sha256=front_sha,
            output=front_output,
            result_manifest=front.root / "animations.result.json",
        )
        back_result = package_browser_animation_glb(
            input_manifest=back_manifest,
            input_manifest_sha256=back_sha,
            output=back_output,
            result_manifest=back.root / "animations.result.json",
        )
        self.assertEqual(front_result["orientation"], "front")
        self.assertEqual(back_result["orientation"], "back")
        self.assertNotEqual(
            front_result["source"]["sha256"], back_result["source"]["sha256"]
        )
        front_json, _ = _parse_glb(front_output.read_bytes())
        back_json, _ = _parse_glb(back_output.read_bytes())
        self.assertEqual(front_json["asset"]["extras"]["sourceOrientation"], "front")
        self.assertEqual(back_json["asset"]["extras"]["sourceOrientation"], "back")
        self.assertNotIn("target_orientation", front_result)

    def test_output_is_accepted_by_the_real_backend_glb_validator_when_available(
        self,
    ) -> None:
        backend_root = Path(__file__).resolve().parents[3] / "backend"
        if str(backend_root) not in sys.path:
            sys.path.insert(0, str(backend_root))
        try:
            from animal_animation_library import validate_glb_animation_contract
        except ModuleNotFoundError as exc:
            if exc.name == "sqlalchemy":
                self.skipTest("configured backend runtime with SQLAlchemy is required")
            raise

        fixture = Fixture(self.root)
        manifest, manifest_sha = fixture.manifest()
        output = self.root / "backend-validator.glb"
        package_browser_animation_glb(
            input_manifest=manifest,
            input_manifest_sha256=manifest_sha,
            output=output,
            result_manifest=self.root / "backend-validator.result.json",
        )
        validate_glb_animation_contract(
            output,
            {
                "clips": [
                    {"id": semantic_id, "duration": 1.0, "fps": 30}
                    for semantic_id in ANIMAL_CLIP_IDS
                ]
            },
        )

    def test_rejects_unknown_taxonomy_rig_and_duplicate_clip_content(self) -> None:
        unknown_root = self.root / "unknown-rig"
        unknown = Fixture(unknown_root)
        manifest, manifest_sha = unknown.manifest(rig_type="dragon")
        with self.assertRaisesRegex(PackageError, "pinned non-T-pose taxonomy"):
            package_browser_animation_glb(
                input_manifest=manifest,
                input_manifest_sha256=manifest_sha,
                output=unknown_root / "must-not-exist.glb",
                result_manifest=unknown_root / "must-not-exist.json",
            )
        self.assertFalse((unknown_root / "must-not-exist.glb").exists())

        duplicate_root = self.root / "duplicate-content"
        duplicate = Fixture(duplicate_root)
        duplicate_path = duplicate_root / "duplicate-clip.json"
        duplicate_path.write_bytes(duplicate.clip_paths[0].read_bytes())
        rows = _rows(duplicate)
        rows[1] = {
            "semantic_id": ANIMAL_CLIP_IDS[1],
            **_pin(duplicate_path),
            **_approval(1),
        }
        manifest, manifest_sha = duplicate.manifest(rows=rows)
        with self.assertRaisesRegex(PackageError, "repeats clip SHA-256/content"):
            package_browser_animation_glb(
                input_manifest=manifest,
                input_manifest_sha256=manifest_sha,
                output=duplicate_root / "must-not-exist.glb",
                result_manifest=duplicate_root / "must-not-exist.json",
            )
        self.assertFalse((duplicate_root / "must-not-exist.glb").exists())
        self.assertFalse((duplicate_root / "must-not-exist.json").exists())

    def test_rejects_reused_or_invalid_approval_provenance(self) -> None:
        cases = (
            ("candidate_id", "candidate_id"),
            ("candidate_bundle_sha256", "candidate_bundle_sha256"),
            ("human_review_sha256", "human_review_sha256"),
        )
        for case_index, (field, message) in enumerate(cases):
            with self.subTest(field=field):
                case_root = self.root / f"approval-{case_index}"
                fixture = Fixture(case_root)
                rows = _rows(fixture)
                rows[1][field] = rows[0][field]
                manifest, manifest_sha = fixture.manifest(rows=rows)
                with self.assertRaisesRegex(PackageError, f"repeats {message}"):
                    package_browser_animation_glb(
                        input_manifest=manifest,
                        input_manifest_sha256=manifest_sha,
                        output=case_root / "must-not-exist.glb",
                        result_manifest=case_root / "must-not-exist.json",
                    )
                self.assertFalse((case_root / "must-not-exist.glb").exists())

        invalid_root = self.root / "invalid-candidate-id"
        invalid = Fixture(invalid_root)
        rows = _rows(invalid)
        rows[0]["candidate_id"] = "not-a-uuid"
        manifest, manifest_sha = invalid.manifest(rows=rows)
        with self.assertRaisesRegex(PackageError, "canonical lowercase UUID"):
            package_browser_animation_glb(
                input_manifest=manifest,
                input_manifest_sha256=manifest_sha,
                output=invalid_root / "must-not-exist.glb",
                result_manifest=invalid_root / "must-not-exist.json",
            )

    def test_source_provenance_and_four_influence_gate_fail_before_publish(
        self,
    ) -> None:
        mismatch_cases = (
            (
                "orientation",
                lambda fixture: fixture.manifest(orientation="back"),
                "sourceOrientation",
            ),
            (
                "rig",
                lambda fixture: fixture.manifest(rig_type="dog"),
                "sourceRigType",
            ),
            (
                "skeleton",
                lambda fixture: fixture.manifest(template_skeleton_sha256="b" * 64),
                "templateSkeletonSha256",
            ),
        )
        for case_index, (name, make_manifest, message) in enumerate(mismatch_cases):
            with self.subTest(case=name):
                case_root = self.root / f"source-provenance-{case_index}"
                fixture = Fixture(case_root)
                manifest, manifest_sha = make_manifest(fixture)
                with self.assertRaisesRegex(PackageError, message):
                    package_browser_animation_glb(
                        input_manifest=manifest,
                        input_manifest_sha256=manifest_sha,
                        output=case_root / "must-not-exist.glb",
                        result_manifest=case_root / "must-not-exist.json",
                    )
                self.assertFalse((case_root / "must-not-exist.glb").exists())

        missing_root = self.root / "source-provenance-missing"
        missing = Fixture(missing_root)
        del missing.source_json["asset"]["extras"]["sourceRigType"]
        missing.source.write_bytes(_glb(missing.source_json, missing.source_bin))
        manifest, manifest_sha = missing.manifest()
        with self.assertRaisesRegex(PackageError, "missing required provenance keys"):
            package_browser_animation_glb(
                input_manifest=manifest,
                input_manifest_sha256=manifest_sha,
                output=missing_root / "must-not-exist.glb",
                result_manifest=missing_root / "must-not-exist.json",
            )
        self.assertFalse((missing_root / "must-not-exist.glb").exists())

        influence_root = self.root / "extra-influences"
        influence = Fixture(influence_root)
        influence.source_json["meshes"][0]["primitives"][0]["attributes"][
            "JOINTS_1"
        ] = 4
        influence.source.write_bytes(_glb(influence.source_json, influence.source_bin))
        manifest, manifest_sha = influence.manifest()
        with self.assertRaisesRegex(PackageError, "four-influence"):
            package_browser_animation_glb(
                input_manifest=manifest,
                input_manifest_sha256=manifest_sha,
                output=influence_root / "must-not-exist.glb",
                result_manifest=influence_root / "must-not-exist.json",
            )
        self.assertFalse((influence_root / "must-not-exist.glb").exists())
        self.assertFalse((influence_root / "must-not-exist.json").exists())

    def test_pin_order_and_output_collision_fail_closed(self) -> None:
        fixture = Fixture(self.root)
        manifest, manifest_sha = fixture.manifest()
        with self.assertRaisesRegex(PackageError, "input manifest SHA-256 mismatch"):
            package_browser_animation_glb(
                input_manifest=manifest,
                input_manifest_sha256="0" * 64,
                output=self.root / "bad.glb",
                result_manifest=self.root / "bad.json",
            )
        rows = [
            {"semantic_id": semantic_id, **_pin(path), **_approval(index)}
            for index, (semantic_id, path) in enumerate(
                zip(ANIMAL_CLIP_IDS, fixture.clip_paths)
            )
        ]
        rows[0], rows[1] = rows[1], rows[0]
        wrong_manifest, wrong_sha = fixture.manifest(rows=rows, name="wrong-order.json")
        with self.assertRaisesRegex(PackageError, "taxonomy order"):
            package_browser_animation_glb(
                input_manifest=wrong_manifest,
                input_manifest_sha256=wrong_sha,
                output=self.root / "wrong.glb",
                result_manifest=self.root / "wrong.json",
            )
        output = self.root / "first.glb"
        result_manifest = self.root / "first.result.json"
        package_browser_animation_glb(
            input_manifest=manifest,
            input_manifest_sha256=manifest_sha,
            output=output,
            result_manifest=result_manifest,
        )
        with self.assertRaisesRegex(PackageError, "output collision"):
            package_browser_animation_glb(
                input_manifest=manifest,
                input_manifest_sha256=manifest_sha,
                output=output,
                result_manifest=self.root / "unused-result.json",
            )
        colliding_output = self.root / "must-not-publish.glb"
        with self.assertRaisesRegex(PackageError, "result manifest collision"):
            package_browser_animation_glb(
                input_manifest=manifest,
                input_manifest_sha256=manifest_sha,
                output=colliding_output,
                result_manifest=result_manifest,
            )
        self.assertFalse(colliding_output.exists())

    def test_track_joint_quaternion_and_motion_validation_fail_before_output(
        self,
    ) -> None:
        cases = (
            (
                lambda clip: clip["tracks"][0].update(name="Missing.quaternion"),
                "does not resolve",
            ),
            (
                lambda clip: clip["tracks"][0].update(values=[0, 0, 0, 2, 0, 0, 0, 2]),
                "not normalized",
            ),
            (
                lambda clip: clip.update(
                    tracks=[
                        {
                            "name": "Root.position",
                            "type": "vector",
                            "times": [0, 1],
                            "values": [0, 0, 0, 0, 0, 0],
                        }
                    ]
                ),
                "no nonzero animation",
            ),
        )
        for case_index, (mutate, message) in enumerate(cases):
            with self.subTest(case=message):
                case_root = self.root / f"case-{case_index}"
                case_root.mkdir()
                fixture = Fixture(case_root)
                value = json.loads(fixture.clip_paths[0].read_text(encoding="utf-8"))
                mutate(value)
                fixture.clip_paths[0].write_text(
                    json.dumps(value, sort_keys=True), encoding="utf-8"
                )
                manifest, manifest_sha = fixture.manifest()
                output = case_root / "must-not-exist.glb"
                with self.assertRaisesRegex(PackageError, message):
                    package_browser_animation_glb(
                        input_manifest=manifest,
                        input_manifest_sha256=manifest_sha,
                        output=output,
                        result_manifest=case_root / "must-not-exist.json",
                    )
                self.assertFalse(output.exists())
                self.assertFalse((case_root / "must-not-exist.json").exists())

    def test_cli_emits_one_machine_readable_result(self) -> None:
        fixture = Fixture(self.root)
        manifest, manifest_sha = fixture.manifest()
        output = self.root / "cli.glb"
        result_manifest = self.root / "cli.result.json"
        stdout = io.StringIO()
        publications: list[Path] = []
        real_publish = packager_module._publish_exclusive

        def traced_publish(filename, payload):
            publications.append(Path(filename).resolve())
            return real_publish(filename, payload)

        with (
            patch.object(
                packager_module, "_publish_exclusive", side_effect=traced_publish
            ),
            redirect_stdout(stdout),
        ):
            code = main(
                [
                    "--input-manifest",
                    str(manifest),
                    "--input-manifest-sha256",
                    manifest_sha,
                    "--output",
                    str(output),
                    "--result-manifest",
                    str(result_manifest),
                ]
            )
        self.assertEqual(code, 0)
        result = json.loads(stdout.getvalue())
        self.assertEqual(result["schema"], OUTPUT_SCHEMA)
        self.assertEqual(result["output"]["sha256"], _sha(output.read_bytes()))
        self.assertEqual(publications, [output.resolve(), result_manifest.resolve()])
        persisted = json.loads(result_manifest.read_text(encoding="utf-8"))
        self.assertEqual(persisted["output"]["sha256"], result["output"]["sha256"])


if __name__ == "__main__":
    unittest.main()
