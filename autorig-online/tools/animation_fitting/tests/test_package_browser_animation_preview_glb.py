from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
import struct
import sys
import tempfile
import unittest


MODULE_ROOT = Path(__file__).resolve().parents[1]
if str(MODULE_ROOT) not in sys.path:
    sys.path.insert(0, str(MODULE_ROOT))

from package_browser_animation_glb import PackageError  # noqa: E402
from package_browser_animation_preview_glb import (  # noqa: E402
    INPUT_SCHEMA,
    package_browser_animation_preview_glb,
)


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _pin(path: Path) -> dict:
    data = path.read_bytes()
    return {"path": path.name, "bytes": len(data), "sha256": _sha(data)}


def _glb(gltf: dict, binary: bytes) -> bytes:
    json_bytes = json.dumps(gltf, sort_keys=True, separators=(",", ":")).encode()
    json_bytes += b" " * ((-len(json_bytes)) % 4)
    binary += b"\x00" * ((-len(binary)) % 4)
    total = 12 + 8 + len(json_bytes) + 8 + len(binary)
    return b"".join(
        (
            struct.pack("<4sII", b"glTF", 2, total),
            struct.pack("<I4s", len(json_bytes), b"JSON"),
            json_bytes,
            struct.pack("<I4s", len(binary), b"BIN\x00"),
            binary,
        )
    )


def _parse_glb(data: bytes) -> tuple[dict, bytes]:
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


def _source() -> tuple[dict, bytes]:
    binary = b"SOURCE-PREVIEW-BIN-PREFIX-000000"
    gltf = {
        "asset": {"version": "2.0", "generator": "preview-test"},
        "scene": 0,
        "scenes": [{"nodes": [0]}],
        "nodes": [
            {"name": "Model", "mesh": 0, "skin": 0, "children": [1]},
            {"name": "Root", "children": [2]},
            {"name": "Spine", "translation": [0, 0, 1]},
        ],
        "skins": [{"joints": [1, 2], "skeleton": 1, "inverseBindMatrices": 0}],
        "meshes": [{
            "name": "Body",
            "primitives": [{
                "attributes": {
                    "POSITION": 1,
                    "NORMAL": 2,
                    "TEXCOORD_0": 3,
                    "JOINTS_0": 4,
                    "WEIGHTS_0": 5,
                    "JOINTS_1": 6,
                    "WEIGHTS_1": 7,
                },
                "indices": 8,
                "material": 0,
            }],
        }],
        "materials": [{"name": "Coat"}],
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
            {"bufferView": 0, "componentType": 5123, "count": 1, "type": "VEC4"},
            {"bufferView": 0, "componentType": 5126, "count": 1, "type": "VEC4"},
            {"bufferView": 0, "componentType": 5123, "count": 3, "type": "SCALAR"},
        ],
        "animations": [{"name": "Old_Action", "samplers": [], "channels": []}],
        "extras": {"mustRemain": True},
    }
    return gltf, binary


def _clip(name: str, angle: float = 0.2) -> dict:
    return {
        "name": name,
        "duration": 1.0,
        "tracks": [
            {
                "name": "Spine.quaternion",
                "type": "quaternion",
                "times": [0.0, 1.0],
                "values": [
                    0, 0, 0, 1,
                    0, 0, math.sin(angle / 2), math.cos(angle / 2),
                ],
            },
            {
                "name": "Root.position",
                "type": "vector",
                "times": [0.0, 1.0],
                "values": [0, 0, 0, 0.1, 0, 0],
            },
        ],
    }


def _qa(semantic_id: str, clip_sha256: str, *, passed: bool = True) -> dict:
    return {
        "schema": "autorig.browser-horse-visual-phase-evidence-envelope.v1",
        "visual_phase_gate": {
            "semantic_id": semantic_id,
            "fitted_clip_sha256": clip_sha256,
            "camera": {"static": True},
        },
        "local_evidence": {
            "browser_only": True,
            "blender_used": False,
            "animation_evaluation": "Three.AnimationMixer",
            "target_mesh_deformation_qa": {
                "measured_every_frame": True,
                "passed": passed,
            },
        },
    }


class Fixture:
    def __init__(self, root: Path, count: int = 2) -> None:
        self.root = root
        self.source_json, self.source_bin = _source()
        self.source = root / "source.glb"
        self.source.write_bytes(_glb(self.source_json, self.source_bin))
        self.rows = []
        for index in range(count):
            clip_id = f"CLIP_{index + 1:02d}"
            semantic_id = f"semantic_{index + 1:02d}"
            clip_path = root / f"clip-{index}.json"
            clip_path.write_text(json.dumps(_clip(clip_id, 0.2 + index * 0.01)))
            qa_path = root / f"qa-{index}.json"
            qa_path.write_text(json.dumps(_qa(semantic_id, _sha(clip_path.read_bytes()))))
            self.rows.append({
                "id": clip_id,
                "semantic_id": semantic_id,
                **_pin(clip_path),
                "machine_qa": _pin(qa_path),
                "human_decision": "approved" if index == 0 else "pending",
            })

    def write_manifest(self, rows: list[dict] | None = None) -> tuple[Path, str]:
        path = self.root / "input.json"
        path.write_text(json.dumps({
            "schema": INPUT_SCHEMA,
            "source": _pin(self.source),
            "clips": self.rows if rows is None else rows,
        }))
        return path, _sha(path.read_bytes())


class PreviewPackagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="autorig-preview-glb-")
        self.root = Path(self.temp.name)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def package(self, fixture: Fixture, rows: list[dict] | None = None) -> dict:
        manifest, pin = fixture.write_manifest(rows)
        return package_browser_animation_preview_glb(
            input_manifest=manifest,
            input_manifest_sha256=pin,
            output=self.root / "preview.glb",
            result_manifest=self.root / "result.json",
        )

    def test_replaces_old_animations_and_preserves_source_payload(self) -> None:
        fixture = Fixture(self.root)
        source_before = fixture.source.read_bytes()
        result = self.package(fixture)
        output_json, output_bin = _parse_glb((self.root / "preview.glb").read_bytes())
        source_json, source_bin = _parse_glb(source_before)

        self.assertEqual([row["name"] for row in output_json["animations"]], [
            "CLIP_01", "CLIP_02",
        ])
        self.assertEqual(result["replaced_source_animations"], ["Old_Action"])
        self.assertEqual(result["animation_count"], 2)
        self.assertFalse(result["release_ready"])
        self.assertFalse(result["catalog_admission"])
        self.assertFalse(result["blender_fitting_used"])
        for key in ("asset", "nodes", "skins", "meshes", "materials", "images", "extras"):
            self.assertEqual(output_json[key], source_json[key])
        self.assertEqual(output_bin[: len(source_bin)], source_bin)
        self.assertEqual(fixture.source.read_bytes(), source_before)

    def test_requires_between_one_and_thirty_clips(self) -> None:
        fixture = Fixture(self.root, count=31)
        for rows in ([], fixture.rows):
            with self.subTest(count=len(rows)), self.assertRaisesRegex(
                PackageError, "between 1 and 30"
            ):
                manifest, pin = fixture.write_manifest(rows)
                package_browser_animation_preview_glb(
                    input_manifest=manifest,
                    input_manifest_sha256=pin,
                    output=self.root / f"preview-{len(rows)}.glb",
                    result_manifest=self.root / f"result-{len(rows)}.json",
                )

    def test_rejects_machine_qa_tamper(self) -> None:
        fixture = Fixture(self.root, count=1)
        (self.root / "qa-0.json").write_text("{}")
        with self.assertRaisesRegex(PackageError, "byte count mismatch"):
            self.package(fixture)

    def test_rejects_machine_qa_fail_even_when_re_pinned(self) -> None:
        fixture = Fixture(self.root, count=1)
        clip_pin = fixture.rows[0]["sha256"]
        qa_path = self.root / "qa-0.json"
        qa_path.write_text(json.dumps(_qa("semantic_01", clip_pin, passed=False)))
        fixture.rows[0]["machine_qa"] = _pin(qa_path)
        with self.assertRaisesRegex(PackageError, "does not prove a machine PASS"):
            self.package(fixture)

    def test_rejects_unbound_clip_track(self) -> None:
        fixture = Fixture(self.root, count=1)
        clip_path = self.root / "clip-0.json"
        value = json.loads(clip_path.read_text())
        value["tracks"][0]["name"] = "Missing.quaternion"
        clip_path.write_text(json.dumps(value))
        fixture.rows[0].update(_pin(clip_path))
        qa_path = self.root / "qa-0.json"
        qa_path.write_text(json.dumps(_qa("semantic_01", fixture.rows[0]["sha256"])))
        fixture.rows[0]["machine_qa"] = _pin(qa_path)
        with self.assertRaisesRegex(PackageError, "Missing.*does not resolve"):
            self.package(fixture)


if __name__ == "__main__":
    unittest.main()
