from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from animation_fitting.derive_semantic_reference import (
    compose_semantic_reference,
    decode_face_id_labels,
    derive_semantic_reference,
    encode_face_id_rgb,
    rasterize_topology_labels,
    snapshot_source_files,
    validate_face_id_projection_alignment,
    verify_source_snapshot,
)
from animation_fitting.semantic_ltx_reference import (
    SemanticLtxContractError,
    load_semantic_ltx_profile,
)


def _profile_payload(*, minimum_pixels: int = 4) -> dict:
    return {
        "schema": "autorig-semantic-ltx-profile.v1",
        "profile_id": "synthetic.semantic_limbs.v1",
        "source": {
            "rig_type": "SYNTHETIC",
            "filename": "synthetic.blend",
            "sha256": "1" * 64,
            "armature_name": "Rig",
            "mesh_names": ["Mesh"],
        },
        "limb_groups": {
            "fore_left": {"anatomy": "fore", "side": "left", "bones": ["FL"]},
            "fore_right": {"anatomy": "fore", "side": "right", "bones": ["FR"]},
            "hind_left": {"anatomy": "hind", "side": "left", "bones": ["HL"]},
            "hind_right": {"anatomy": "hind", "side": "right", "bones": ["HR"]},
        },
        "palette_linear": {
            "body": [0.46, 0.50, 0.56],
            "fore_near": [0.00, 0.85, 1.00],
            "fore_far": [0.12, 0.22, 1.00],
            "hind_near": [1.00, 0.72, 0.02],
            "hind_far": [1.00, 0.08, 0.55],
        },
        "gates": {
            "minimum_face_limb_weight": 0.60,
            "minimum_face_dominance": 0.95,
            "minimum_faces_per_source_group": 1,
            "minimum_group_weight_mass": 1.0,
            "minimum_near_far_depth_separation": 0.05,
            "minimum_pixels_per_output_label": minimum_pixels,
            "minimum_pixel_fraction_of_mask": 0.01,
            "maximum_mask_mismatch_pixels": 0,
            "mask_threshold": 0.50,
            "pixel_color_tolerance": 0.08,
            "minimum_palette_distance": 0.55,
        },
    }


def _write_profile(tmp_path: Path, *, minimum_pixels: int = 4):
    path = tmp_path / f"profile-{minimum_pixels}.json"
    path.write_text(
        json.dumps(_profile_payload(minimum_pixels=minimum_pixels), indent=2),
        encoding="utf-8",
    )
    return load_semantic_ltx_profile(path)


def _synthetic_face_raster() -> tuple[np.ndarray, np.ndarray]:
    alpha = np.zeros((22, 22), dtype=np.float64)
    alpha[1:21, 1:21] = 1.0
    rgb = np.zeros((22, 22, 3), dtype=np.float64)
    regions = {
        1: (slice(1, 11), slice(1, 11)),
        2: (slice(1, 11), slice(11, 21)),
        3: (slice(11, 21), slice(1, 11)),
        4: (slice(11, 21), slice(11, 21)),
    }
    for face_id, region in regions.items():
        rgb[region] = encode_face_id_rgb(face_id)
    # Exercise coverage unpremultiplication and one bounded nearest-label fill.
    alpha[2, 2] = 0.8
    rgb[2, 2] *= 0.8
    rgb[10, 10] = encode_face_id_rgb(999)
    return rgb, alpha


def _decode() -> object:
    rgb, alpha = _synthetic_face_raster()
    return decode_face_id_labels(
        rgb,
        alpha,
        topology_face_ids=[1, 2, 3, 4],
        face_labels={
            1: "fore_near",
            2: "fore_far",
            3: "hind_near",
            4: "hind_far",
        },
        mask_threshold=0.5,
    )


def test_face_id_decode_unpremultiplies_and_bounded_fills_synthetic_pixels() -> None:
    decoded = _decode()
    assert decoded.stats["foreground_pixels"] == 400
    assert decoded.stats["directly_decoded_pixels"] == 399
    assert decoded.stats["filled_pixels"] == 1
    assert decoded.stats["fill_fraction"] == pytest.approx(1 / 400)
    assert decoded.stats["maximum_fill_distance_pixels"] == 1.0
    assert decoded.stats["decoded_unique_face_count"] == 4
    assert not np.any(decoded.label_indices[decoded.foreground_mask] < 0)


def test_semantic_composition_preserves_background_and_passes_four_label_gate(
    tmp_path: Path,
) -> None:
    profile = _write_profile(tmp_path)
    decoded = _decode()
    canonical = np.full((22, 22, 3), [31, 47, 63], dtype=np.uint8)
    output, pixels = compose_semantic_reference(canonical, decoded, profile)
    assert np.array_equal(
        output[~decoded.foreground_mask],
        canonical[~decoded.foreground_mask],
    )
    assert pixels["semantic_mask_mismatch_pixels"] == 0
    assert set(pixels["output_label_pixel_counts"]) == {
        "fore_near",
        "fore_far",
        "hind_near",
        "hind_far",
    }
    assert len({tuple(color) for color in pixels["palette_srgb8"].values()}) == 5


def test_collision_fill_and_label_gates_fail_closed(tmp_path: Path) -> None:
    rgb, alpha = _synthetic_face_raster()
    labels = {
        1: "fore_near",
        2: "fore_far",
        3: "hind_near",
        4: "hind_far",
    }
    with pytest.raises(SemanticLtxContractError, match="face-ID collision"):
        decode_face_id_labels(
            rgb,
            alpha,
            topology_face_ids=[1, 2, 2, 4],
            face_labels=labels,
            mask_threshold=0.5,
        )

    too_many_invalid = rgb.copy()
    invalid_region = (slice(1, 3), slice(1, 21))
    too_many_invalid[invalid_region] = (
        encode_face_id_rgb(999) * alpha[invalid_region][:, :, None]
    )
    with pytest.raises(SemanticLtxContractError, match="fill gate"):
        decode_face_id_labels(
            too_many_invalid,
            alpha,
            topology_face_ids=[1, 2, 3, 4],
            face_labels=labels,
            mask_threshold=0.5,
        )

    strict_profile = _write_profile(tmp_path, minimum_pixels=101)
    with pytest.raises(SemanticLtxContractError, match="insufficient pixels"):
        compose_semantic_reference(
            np.zeros((22, 22, 3), dtype=np.uint8),
            _decode(),
            strict_profile,
        )


def test_source_tamper_and_output_collision_are_rejected(tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"immutable")
    snapshot = snapshot_source_files([source])
    source.write_bytes(b"tampered")
    with pytest.raises(SemanticLtxContractError, match="changed during derivation"):
        verify_source_snapshot(snapshot)

    occupied = tmp_path / "semantic-output"
    occupied.mkdir()
    with pytest.raises(SemanticLtxContractError, match="Output directory collision"):
        derive_semantic_reference(
            tmp_path / "missing-bundle",
            tmp_path / "missing-profile.json",
            occupied,
        )

    source_bundle = tmp_path / "immutable-source"
    source_bundle.mkdir()
    nested_output = source_bundle / "derived-reference"
    with pytest.raises(SemanticLtxContractError, match="inside the immutable source"):
        derive_semantic_reference(
            source_bundle,
            tmp_path / "missing-profile.json",
            nested_output,
        )
    assert not nested_output.exists()


def test_topology_raster_and_independent_face_id_alignment_use_synthetic_arrays() -> None:
    coordinates = (-10.0, 0.0, 10.0)
    skin_rows = []
    for row, y in enumerate(reversed(coordinates)):
        for column, x in enumerate(coordinates):
            skin_rows.append(
                {
                    "vertex_id": row * 3 + column,
                    "world": [x, y, -10.0],
                }
            )
    topology_rows = [
        {"face_id": 1, "vertex_ids": [0, 1, 4, 3]},
        {"face_id": 2, "vertex_ids": [1, 2, 5, 4]},
        {"face_id": 3, "vertex_ids": [3, 4, 7, 6]},
        {"face_id": 4, "vertex_ids": [4, 5, 8, 7]},
    ]
    mask = np.zeros((22, 22), dtype=bool)
    mask[1:21, 1:21] = True
    raster = rasterize_topology_labels(
        skin_rows=skin_rows,
        topology_rows=topology_rows,
        camera={
            "resolution": [22, 22],
            "lens_mm": 10.0,
            "sensor_width_mm": 22.0,
            "world_to_camera": np.eye(4).reshape(-1).tolist(),
            "intrinsics": {"fx": 10.0, "fy": 8.0, "cx": 11.0, "cy": 11.0},
        },
        canonical_mask=mask,
        face_labels={
            1: "fore_near",
            2: "fore_far",
            3: "hind_near",
            4: "hind_far",
        },
    )
    assert raster.stats["direct_raster_pixels"] == 400
    assert raster.stats["filled_pixels"] == 0
    assert raster.stats["outside_canonical_mask_pixels"] == 0
    assert {int(value) for value in np.unique(raster.face_ids[mask])} == {1, 2, 3, 4}

    face_rgb = np.zeros((22, 22, 3), dtype=np.float64)
    for face_id in range(1, 5):
        face_rgb[raster.face_ids == face_id] = encode_face_id_rgb(face_id)
    decoded = decode_face_id_labels(
        face_rgb,
        mask.astype(np.float64),
        topology_face_ids=[1, 2, 3, 4],
        face_labels={
            1: "fore_near",
            2: "fore_far",
            3: "hind_near",
            4: "hind_far",
        },
        mask_threshold=0.5,
    )
    alignment = validate_face_id_projection_alignment(decoded, raster)
    assert alignment["exact_face_id_agreement_fraction"] == 1.0
    assert alignment["semantic_label_agreement_fraction"] == 1.0
