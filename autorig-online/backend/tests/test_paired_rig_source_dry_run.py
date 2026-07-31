import base64
import hashlib
import json
import struct
import unittest

from paired_rig_source_dry_run import (
    DRY_RUN_PATH,
    POSITION_TOLERANCE_M,
    PairedRigSourceDryRunError,
    build_paired_rig_source_dry_run_capability,
    validate_paired_rig_source_dry_run,
)


TETRA_OBJ = b"""\
v 0 0 0
v 1 0 0
v 0 1 0
v 0 0 1
f 1 3 2
f 1 2 4
f 1 4 3
f 2 3 4
"""

TWO_TETRA_OBJ = TETRA_OBJ + b"""\
v 3 0 0
v 4 0 0
v 3 1 0
v 3 0 1
f 5 7 6
f 5 6 8
f 5 8 7
f 6 7 8
"""


def _pad4(data, padding=b"\x00"):
    return data + padding * ((-len(data)) % 4)


def _synthetic_textured_glb(
    *,
    omit_uv=False,
    alter_face=False,
    alter_topology=False,
    omit_image=False,
    image_data_uri=False,
    second_component=False,
):
    vertices = [
        (0, 0, 0), (0, 1, 0), (1, 0, 0),
        (0, 0, 0), (1, 0, 0), (0, 0, 1),
        (0, 0, 0), (0, 0, 1), (0, 1, 0),
        (1, 0, 0), (0, 1, 0), (0, 0, 1),
    ]
    if alter_face:
        vertices[-1] = (2, 0, 0)
    if alter_topology:
        vertices[-3:] = [(0, 0, 0), (0, 0, 1), (0, 1, 0)]
    if second_component:
        vertices.extend(
            tuple(coordinate + (3 if axis == 0 else 0) for axis, coordinate in enumerate(vertex))
            for vertex in vertices[:12]
        )
    parts = bytearray()
    views = []
    accessors = []

    def add_view(payload, *, target=None):
        offset = len(parts)
        parts.extend(_pad4(payload))
        view = {"buffer": 0, "byteOffset": offset, "byteLength": len(payload)}
        if target is not None:
            view["target"] = target
        views.append(view)
        return len(views) - 1

    primitives = []
    for primitive_index in range(len(vertices) // 6):
        primitive_vertices = vertices[primitive_index * 6 : primitive_index * 6 + 6]
        position_view = add_view(
            b"".join(struct.pack("<3f", *value) for value in primitive_vertices),
            target=34962,
        )
        position_accessor = len(accessors)
        accessors.append(
            {
                "bufferView": position_view,
                "componentType": 5126,
                "count": 6,
                "type": "VEC3",
            }
        )
        uv_view = add_view(
            b"".join(struct.pack("<2f", float(index % 2), float(index // 2)) for index in range(6)),
            target=34962,
        )
        uv_accessor = len(accessors)
        accessors.append(
            {
                "bufferView": uv_view,
                "componentType": 5126,
                "count": 6,
                "type": "VEC2",
            }
        )
        index_view = add_view(struct.pack("<6H", 0, 1, 2, 3, 4, 5), target=34963)
        index_accessor = len(accessors)
        accessors.append(
            {
                "bufferView": index_view,
                "componentType": 5123,
                "count": 6,
                "type": "SCALAR",
            }
        )
        attributes = {"POSITION": position_accessor}
        if not omit_uv:
            attributes["TEXCOORD_0"] = uv_accessor
        primitives.append(
            {
                "attributes": attributes,
                "indices": index_accessor,
                "material": 0,
                "mode": 4,
            }
        )

    image_bytes = b"\x89PNG\r\n\x1a\nsynthetic-pixel"
    image_view = add_view(image_bytes)
    root = {
        "asset": {"version": "2.0"},
        "buffers": [{"byteLength": len(parts)}],
        "bufferViews": views,
        "accessors": accessors,
        "meshes": [{"primitives": primitives}],
        "nodes": [{"mesh": 0}],
        "scenes": [{"nodes": [0]}],
        "scene": 0,
        "materials": [
            {"pbrMetallicRoughness": {"baseColorTexture": {"index": 0}}}
        ],
        "textures": [{"source": 0}],
        "images": (
            []
            if omit_image
            else [
                {
                    **(
                        {
                            "uri": "data:image/png;base64,"
                            + base64.b64encode(image_bytes).decode("ascii")
                        }
                        if image_data_uri
                        else {"bufferView": image_view, "mimeType": "image/png"}
                    )
                }
            ]
        ),
    }
    json_chunk = _pad4(json.dumps(root, separators=(",", ":")).encode("utf-8"), b" ")
    bin_chunk = _pad4(bytes(parts))
    total_length = 12 + 8 + len(json_chunk) + 8 + len(bin_chunk)
    return b"".join(
        (
            struct.pack("<4sII", b"glTF", 2, total_length),
            struct.pack("<II", len(json_chunk), 0x4E4F534A),
            json_chunk,
            struct.pack("<II", len(bin_chunk), 0x004E4942),
            bin_chunk,
        )
    )


def _validate(obj=TETRA_OBJ, glb=None, **overrides):
    glb = _synthetic_textured_glb() if glb is None else glb
    arguments = {
        "connected_source_obj": obj,
        "appearance_target_glb": glb,
        "expected_connected_source_sha256": hashlib.sha256(obj).hexdigest(),
        "expected_connected_source_bytes": len(obj),
        "expected_appearance_target_sha256": hashlib.sha256(glb).hexdigest(),
        "expected_appearance_target_bytes": len(glb),
    }
    arguments.update(overrides)
    return validate_paired_rig_source_dry_run(**arguments)


class PairedRigSourceDryRunTests(unittest.TestCase):
    def test_capability_is_explicitly_no_mutation_and_field_mapped(self):
        capability = build_paired_rig_source_dry_run_capability()
        self.assertEqual(capability["path"], DRY_RUN_PATH)
        self.assertEqual(capability["method"], "POST")
        self.assertFalse(capability["createsTask"])
        self.assertFalse(capability["chargesCredits"])
        self.assertFalse(capability["persistsUpload"])
        self.assertEqual(
            list(capability["fieldMap"]),
            [
                "dry_run",
                "rig_source_file",
                "appearance_target_file",
                "rig_source_transfer",
                "expected_connected_source_sha256",
                "expected_connected_source_bytes",
                "expected_appearance_target_sha256",
                "expected_appearance_target_bytes",
            ],
        )
        self.assertEqual(capability["requiredExpectedClips"], ["Idle", "Walk", "Run"])
        self.assertTrue(capability["preservationDeclarations"]["appearanceTargetUvsUntouched"])
        paired = capability["pairedAnimalDryRun"]
        self.assertEqual(
            paired["formFields"],
            {
                "dryRun": "dry_run",
                "connectedRigSource": "rig_source_file",
                "texturedTarget": "appearance_target_file",
                "transferMode": "rig_source_transfer",
                "expectedConnectedRigSourceSha256": "expected_connected_source_sha256",
                "expectedConnectedRigSourceBytes": "expected_connected_source_bytes",
                "expectedTexturedTargetSha256": "expected_appearance_target_sha256",
                "expectedTexturedTargetBytes": "expected_appearance_target_bytes",
            },
        )
        self.assertEqual(paired["transferModeValue"], "position-and-triangle-topology")
        self.assertEqual(
            paired["dryRunGuarantees"],
            {"createsTask": False, "chargesCredits": False, "persistsUpload": False},
        )
        json.dumps(capability, allow_nan=False)

    def test_synthetic_multprimitive_uv_split_pair_passes(self):
        evidence = _validate()
        self.assertTrue(evidence["valid"])
        self.assertEqual(evidence["sourceTopology"]["componentCount"], 1)
        self.assertTrue(evidence["sourceTopology"]["eachComponentWatertight"])
        self.assertEqual(evidence["appearanceTarget"]["primitiveCount"], 2)
        self.assertEqual(evidence["mapping"]["sourceTriangleCount"], 4)
        self.assertEqual(evidence["mapping"]["targetTriangleCount"], 4)
        self.assertEqual(evidence["mapping"]["sourcePositionGroupCount"], 4)
        self.assertEqual(evidence["mapping"]["targetVertexOccurrenceCount"], 12)
        self.assertEqual(evidence["mapping"]["uvSplitExtraTargetVertexCount"], 8)
        self.assertTrue(evidence["mapping"]["faceTopologyMultisetIdentity"])
        self.assertLessEqual(evidence["mapping"]["maxPositionDeltaM"], POSITION_TOLERANCE_M)
        self.assertFalse(evidence["createsTask"])
        self.assertTrue(evidence["dry_run"])
        self.assertEqual(evidence["credits_charged"], 0)
        self.assertFalse(evidence["task_created"])
        self.assertFalse(evidence["persisted_uploads"])
        self.assertTrue(evidence["temporary_buffers_discarded"])
        self.assertEqual(
            evidence["prediction"],
            {
                "model_type": "animal",
                "animal_type": "horse",
                "task_type": "only_rig",
                "view": "front",
                "expected_clips": ["Idle", "Walk", "Run"],
            },
        )
        self.assertEqual(evidence["transfer"]["mode"], "position-and-triangle-topology")
        self.assertTrue(evidence["transfer"]["correspondence"]["full_source_position_coverage"])
        self.assertNotIn("task_id", evidence)
        json.dumps(evidence, allow_nan=False)

    def test_embedded_data_image_uri_does_not_require_separate_mime_type(self):
        evidence = _validate(glb=_synthetic_textured_glb(image_data_uri=True))
        self.assertTrue(evidence["appearanceTarget"]["hasEmbeddedPbrImage"])

    def test_multiple_watertight_source_components_are_supported(self):
        evidence = _validate(
            obj=TWO_TETRA_OBJ,
            glb=_synthetic_textured_glb(second_component=True),
        )
        self.assertEqual(evidence["sourceTopology"]["componentCount"], 2)
        self.assertEqual(evidence["appearanceTarget"]["primitiveCount"], 4)
        self.assertEqual(evidence["mapping"]["sourceTriangleCount"], 8)
        self.assertTrue(evidence["mapping"]["faceTopologyMultisetIdentity"])

    def test_hash_and_size_pins_fail_closed_before_geometry(self):
        with self.assertRaisesRegex(PairedRigSourceDryRunError, "SHA-256 mismatch") as caught:
            _validate(expected_connected_source_sha256="0" * 64)
        self.assertEqual(caught.exception.code, "artifact_sha256_mismatch")
        with self.assertRaisesRegex(PairedRigSourceDryRunError, "bytes mismatch") as caught:
            _validate(expected_connected_source_bytes=len(TETRA_OBJ) + 1)
        self.assertEqual(caught.exception.code, "artifact_bytes_mismatch")

    def test_open_or_non_manifold_source_component_fails(self):
        open_obj = TETRA_OBJ.rsplit(b"f ", 1)[0]
        with self.assertRaises(PairedRigSourceDryRunError) as caught:
            _validate(obj=open_obj)
        self.assertEqual(caught.exception.code, "source_component_not_watertight")

    def test_non_triangular_source_fails(self):
        quad = b"v 0 0 0\nv 1 0 0\nv 1 1 0\nv 0 1 0\nf 1 2 3 4\n"
        with self.assertRaises(PairedRigSourceDryRunError) as caught:
            _validate(obj=quad)
        self.assertEqual(caught.exception.code, "non_triangular_obj")

    def test_unmapped_position_or_changed_face_topology_fails(self):
        glb = _synthetic_textured_glb(alter_face=True)
        with self.assertRaises(PairedRigSourceDryRunError) as caught:
            _validate(glb=glb)
        self.assertEqual(caught.exception.code, "target_position_unmapped")

        glb = _synthetic_textured_glb(alter_topology=True)
        with self.assertRaises(PairedRigSourceDryRunError) as caught:
            _validate(glb=glb)
        self.assertEqual(caught.exception.code, "face_topology_mismatch")

    def test_missing_uv_or_embedded_pbr_image_fails(self):
        for glb, expected_code in (
            (_synthetic_textured_glb(omit_uv=True), "missing_target_uv"),
            (_synthetic_textured_glb(omit_image=True), "missing_pbr_image"),
        ):
            with self.subTest(expected_code=expected_code):
                with self.assertRaises(PairedRigSourceDryRunError) as caught:
                    _validate(glb=glb)
                self.assertEqual(caught.exception.code, expected_code)


if __name__ == "__main__":
    unittest.main()
