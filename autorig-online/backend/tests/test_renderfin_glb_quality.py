import json
import math
from pathlib import Path
import struct
import tempfile
import unittest

from renderfin.glb_quality import (
    GLB_QUALITY_SCHEMA,
    HUNYUAN_STANDARD_PBR_POLICY,
    GlbQualityError,
    GlbQualityPolicy,
    validate_glb_bytes,
    validate_glb_path,
)


PNG_1X1 = bytes.fromhex(
    "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c489"
    "0000000d49444154789c63f8cfc0f01f00050001ff89993d1d0000000049454e44ae426082"
)


def _pad(value: bytes, byte: bytes) -> bytes:
    return value + byte * ((-len(value)) % 4)


def _encode_glb(root, binary: bytes, *, extra_chunks=()) -> bytes:
    root = dict(root)
    root["buffers"] = [dict(root["buffers"][0], byteLength=len(binary))]
    json_payload = _pad(
        json.dumps(root, separators=(",", ":"), allow_nan=False).encode("utf-8"), b" "
    )
    bin_payload = _pad(binary, b"\x00")
    chunks = [(0x4E4F534A, json_payload), (0x004E4942, bin_payload), *extra_chunks]
    body = b"".join(
        struct.pack("<II", len(payload), chunk_type) + payload
        for chunk_type, payload in chunks
    )
    return struct.pack("<4sII", b"glTF", 2, 12 + len(body)) + body


def _document(positions=None, indices=None, *, image_bytes=None):
    positions = positions or [
        (-1.0, -1.0, -1.0),
        (1.0, -1.0, -1.0),
        (1.0, 1.0, -1.0),
        (-1.0, 1.0, -1.0),
        (-1.0, -1.0, 1.0),
        (1.0, -1.0, 1.0),
        (1.0, 1.0, 1.0),
        (-1.0, 1.0, 1.0),
    ]
    indices = indices or [
        0, 1, 2, 0, 2, 3,
        4, 6, 5, 4, 7, 6,
        0, 4, 5, 0, 5, 1,
        1, 5, 6, 1, 6, 2,
        2, 6, 7, 2, 7, 3,
        3, 7, 4, 3, 4, 0,
    ]
    position_bytes = b"".join(struct.pack("<3f", *position) for position in positions)
    index_offset = len(position_bytes)
    index_bytes = b"".join(struct.pack("<H", index) for index in indices)
    binary = position_bytes + index_bytes
    views = [
        {"buffer": 0, "byteOffset": 0, "byteLength": len(position_bytes), "target": 34962},
        {"buffer": 0, "byteOffset": index_offset, "byteLength": len(index_bytes), "target": 34963},
    ]
    root = {
        "asset": {"version": "2.0", "generator": "unit-test"},
        "scene": 0,
        "scenes": [{"nodes": [0]}],
        "nodes": [{"mesh": 0}],
        "meshes": [{"primitives": [{"attributes": {"POSITION": 0}, "indices": 1}]}],
        "buffers": [{"byteLength": len(binary)}],
        "bufferViews": views,
        "accessors": [
            {
                "bufferView": 0,
                "componentType": 5126,
                "count": len(positions),
                "type": "VEC3",
                "min": [min(item[axis] for item in positions) for axis in range(3)],
                "max": [max(item[axis] for item in positions) for axis in range(3)],
            },
            {
                "bufferView": 1,
                "componentType": 5123,
                "count": len(indices),
                "type": "SCALAR",
                "min": [min(indices)],
                "max": [max(indices)],
            },
        ],
    }
    if image_bytes is not None:
        padding = b"\x00" * ((-len(binary)) % 4)
        image_offset = len(binary) + len(padding)
        binary += padding + image_bytes
        root["bufferViews"].append(
            {"buffer": 0, "byteOffset": image_offset, "byteLength": len(image_bytes)}
        )
        root["images"] = [{"bufferView": 2, "mimeType": "image/png"}]
        root["textures"] = [{"source": 0}]
        root["materials"] = [
            {"pbrMetallicRoughness": {"baseColorTexture": {"index": 0}}}
        ]
        root["meshes"][0]["primitives"][0]["material"] = 0
    root["buffers"][0]["byteLength"] = len(binary)
    return root, binary


def _valid_glb(**kwargs) -> bytes:
    return _encode_glb(*_document(**kwargs))


class GlbQualityTests(unittest.TestCase):
    def assert_error(self, code, action):
        with self.assertRaises(GlbQualityError) as caught:
            action()
        self.assertEqual(caught.exception.code, code)
        return caught.exception

    def test_valid_cube_returns_geometry_silhouette_and_resource_evidence(self):
        report = validate_glb_bytes(_valid_glb(image_bytes=PNG_1X1))
        self.assertTrue(report["ok"])
        self.assertEqual(report["schema"], GLB_QUALITY_SCHEMA)
        self.assertEqual(report["geometry"]["triangles_total_instances"], 12)
        self.assertAlmostEqual(report["geometry"]["surface_area"], 24.0)
        self.assertAlmostEqual(report["geometry"]["normalized_surface_area"], 2.0)
        self.assertEqual(report["geometry"]["bbox_extents"], [2.0, 2.0, 2.0])
        self.assertGreater(report["silhouette"]["max_occupancy"], 0.5)
        self.assertGreater(report["silhouette"]["max_bbox_fill"], 0.9)
        self.assertEqual(report["resources"]["images"][0]["mime_type"], "image/png")
        self.assertTrue(report["resources"]["all_embedded"])

    def test_path_api_reads_without_mutating_the_file(self):
        payload = _valid_glb()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "model.glb"
            path.write_bytes(payload)
            report = validate_glb_path(path)
            self.assertEqual(report["path"], str(path))
            self.assertEqual(report["sha256"], __import__("hashlib").sha256(payload).hexdigest())
            self.assertEqual(path.read_bytes(), payload)

    def test_declared_length_must_match_exactly(self):
        payload = _valid_glb() + b"x"
        self.assert_error("glb_length_mismatch", lambda: validate_glb_bytes(payload))

    def test_chunk_bounds_and_types_are_fail_closed(self):
        payload = bytearray(_valid_glb())
        payload = payload[:-1]
        struct.pack_into("<I", payload, 8, len(payload))
        self.assert_error("truncated_glb_chunk", lambda: validate_glb_bytes(bytes(payload)))

        root, binary = _document()
        unknown = _encode_glb(root, binary, extra_chunks=((0x12345678, b"\x00" * 4),))
        self.assert_error("unsupported_glb_chunk", lambda: validate_glb_bytes(unknown))

    def test_accessor_and_buffer_view_bounds_are_validated(self):
        root, binary = _document()
        root["accessors"][0]["count"] += 100
        payload = _encode_glb(root, binary)
        self.assert_error("accessor_out_of_bounds", lambda: validate_glb_bytes(payload))

        root, binary = _document()
        root["bufferViews"][0]["byteLength"] = len(binary) + 1
        payload = _encode_glb(root, binary)
        self.assert_error("buffer_view_out_of_bounds", lambda: validate_glb_bytes(payload))

    def test_declared_accessor_bounds_must_match_payload(self):
        root, binary = _document()
        root["accessors"][0]["max"][0] = 99
        self.assert_error(
            "accessor_bounds_mismatch",
            lambda: validate_glb_bytes(_encode_glb(root, binary)),
        )

    def test_nonfinite_position_is_rejected(self):
        root, binary = _document()
        binary = bytearray(binary)
        struct.pack_into("<f", binary, 0, math.nan)
        root["accessors"][0].pop("min")
        root["accessors"][0].pop("max")
        self.assert_error(
            "nonfinite_accessor",
            lambda: validate_glb_bytes(_encode_glb(root, bytes(binary))),
        )

    def test_indices_are_unsigned_scalar_and_in_range(self):
        root, binary = _document()
        binary = bytearray(binary)
        index_offset = root["bufferViews"][1]["byteOffset"]
        struct.pack_into("<H", binary, index_offset, 999)
        root["accessors"][1].pop("max")
        self.assert_error(
            "index_out_of_bounds",
            lambda: validate_glb_bytes(_encode_glb(root, bytes(binary))),
        )

        root, binary = _document()
        root["accessors"][1]["componentType"] = 5122
        self.assert_error(
            "invalid_indices",
            lambda: validate_glb_bytes(_encode_glb(root, binary)),
        )

    def test_scene_reachability_and_cycles_are_rejected(self):
        root, binary = _document()
        root["nodes"] = [{}, {"mesh": 0}]
        root["scenes"] = [{"nodes": [0]}]
        self.assert_error(
            "no_reachable_mesh",
            lambda: validate_glb_bytes(_encode_glb(root, binary)),
        )

        root, binary = _document()
        root["nodes"] = [{"mesh": 0, "children": [1]}, {"children": [0]}]
        self.assert_error(
            "node_cycle", lambda: validate_glb_bytes(_encode_glb(root, binary))
        )

    def test_node_transform_is_applied_to_bbox_and_area(self):
        root, binary = _document()
        root["nodes"][0]["translation"] = [10, 20, 30]
        root["nodes"][0]["scale"] = [2, 3, 4]
        report = validate_glb_bytes(_encode_glb(root, binary))
        self.assertEqual(report["geometry"]["bbox_min"], [8.0, 17.0, 26.0])
        self.assertEqual(report["geometry"]["bbox_max"], [12.0, 23.0, 34.0])
        self.assertAlmostEqual(report["geometry"]["surface_area"], 208.0)

    def test_external_and_corrupt_images_are_rejected(self):
        root, binary = _document()
        root["images"] = [{"uri": "https://example.invalid/texture.png"}]
        self.assert_error(
            "external_glb_resource",
            lambda: validate_glb_bytes(_encode_glb(root, binary)),
        )

        self.assert_error(
            "invalid_embedded_image",
            lambda: validate_glb_bytes(_valid_glb(image_bytes=b"not-a-png")),
        )

    def test_required_embedded_image_policy_is_explicit(self):
        policy = HUNYUAN_STANDARD_PBR_POLICY
        self.assert_error(
            "missing_embedded_image", lambda: validate_glb_bytes(_valid_glb(), policy=policy)
        )
        self.assertTrue(validate_glb_bytes(_valid_glb(image_bytes=PNG_1X1), policy=policy)["ok"])

        image_only = GlbQualityPolicy(require_embedded_image=True, require_texture=True)
        root, binary = _document(image_bytes=PNG_1X1)
        root["textures"] = []
        root["materials"] = []
        root["meshes"][0]["primitives"][0].pop("material")
        self.assert_error(
            "missing_texture",
            lambda: validate_glb_bytes(_encode_glb(root, binary), policy=image_only),
        )

    def test_excessive_degenerate_triangles_fail(self):
        positions = [(0.0, 0.0, 0.0), (1.0, 0.0, 0.0), (2.0, 0.0, 0.0)]
        self.assert_error(
            "excessive_degenerate_triangles",
            lambda: validate_glb_bytes(_valid_glb(positions=positions, indices=[0, 1, 2])),
        )

    def test_welded_connected_component_diagnostics_do_not_fail_alone(self):
        positions = [
            (0, 0, 0), (1, 0, 0), (0, 1, 0), (0, 0, 1),
            (3, 0, 0), (4, 0, 0), (3, 1, 0), (3, 0, 1),
        ]
        indices = [
            0, 2, 1, 0, 1, 3, 0, 3, 2, 1, 2, 3,
            4, 6, 5, 4, 5, 7, 4, 7, 6, 5, 6, 7,
        ]
        report = validate_glb_bytes(_valid_glb(positions=positions, indices=indices))
        primitive = report["primitives"][0]
        self.assertEqual(primitive["welded_component_count"], 2)
        self.assertAlmostEqual(primitive["welded_largest_component_vertex_fraction"], 0.5)
        self.assertTrue(report["ok"])

    def test_calibrated_sparse_semantic_geometry_is_rejected_with_report(self):
        positions = []
        indices = []
        anchors = [
            (-1, -1, -1), (1, -1, -1), (-1, 1, -1), (1, 1, -1),
            (-1, -1, 1), (1, -1, 1), (-1, 1, 1), (1, 1, 1),
        ]
        for anchor in anchors:
            base = len(positions)
            x, y, z = anchor
            positions.extend([(x, y, z), (x + 0.01, y, z), (x, y + 0.01, z)])
            indices.extend([base, base + 1, base + 2])
        error = self.assert_error(
            "semantic_geometry_corrupt",
            lambda: validate_glb_bytes(_valid_glb(positions=positions, indices=indices)),
        )
        report = error.details["report"]
        self.assertFalse(report["ok"])
        self.assertLess(report["geometry"]["normalized_surface_area"], 0.10)
        self.assertLess(report["silhouette"]["max_occupancy"], 0.06)
        warning_codes = {item["code"] for item in report["warnings"]}
        self.assertIn("low_normalized_surface_area", warning_codes)
        self.assertIn("low_triangle_count", warning_codes)
        self.assertIn("fragmented_welded_components", warning_codes)

    def test_low_triangle_count_is_only_a_diagnostic(self):
        report = validate_glb_bytes(_valid_glb())
        self.assertTrue(report["ok"])
        self.assertIn("low_triangle_count", {item["code"] for item in report["warnings"]})


if __name__ == "__main__":
    unittest.main()
