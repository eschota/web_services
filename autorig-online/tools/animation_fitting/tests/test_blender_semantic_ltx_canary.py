from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess

import numpy as np
from PIL import Image
import pytest

from animation_fitting.errors import ContractError
from animation_fitting.rig import load_rig_bundle


BLENDER_43 = Path(
    os.environ.get(
        "AUTORIG_BLENDER_43",
        r"C:\Program Files\Blender Foundation\Blender 4.3\blender.exe",
    )
)
ROOT = Path(__file__).resolve().parents[1]


def _run(*arguments: str, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        [str(BLENDER_43), *arguments],
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"Blender failed ({result.returncode})\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return result


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


@pytest.mark.skipif(not BLENDER_43.is_file(), reason="Local Blender 4.3 is not installed")
def test_blender_43_semantic_ltx_reference_is_immutable_and_rest_safe(
    tmp_path: Path,
) -> None:
    create = _run(
        "--background",
        "--factory-startup",
        "--python",
        str(ROOT / "tests" / "blender_create_semantic_ltx_canary.py"),
        "--",
        "--output-dir",
        str(tmp_path),
    )
    assert "AUTORIG_SEMANTIC_CANARY=" in create.stdout
    source = tmp_path / "semantic_horse_source.blend"
    profile = tmp_path / "semantic_horse_profile.v1.json"
    source_sha = _sha256(source)
    output = tmp_path / "bundle"
    render = _run(
        "--background",
        "--factory-startup",
        "--python",
        str(ROOT / "render_actionless_bundle.py"),
        "--",
        "--input",
        str(source),
        "--output-dir",
        str(output),
        "--species",
        "horse",
        "--rig-type",
        "SEMANTIC_CANARY",
        "--orientation",
        "canonical",
        "--width",
        "320",
        "--height",
        "180",
        "--samples",
        "1",
        "--semantic-profile",
        str(profile),
    )
    assert "AUTORIG_FITTING_BUNDLE=" in render.stdout
    assert "AUTORIG_FITTING_BUNDLE_ERROR=" not in render.stdout + render.stderr
    assert _sha256(source) == source_sha
    assert not (output / ".reference_ltx_semantic_overlay.png").exists()

    rig = load_rig_bundle(output)
    metadata = rig.metadata
    assert metadata["revision"] == "autorig_actionless_bundle_v3"
    assert rig.artifacts["ltx_semantic"] == (
        output / "reference_ltx_semantic.png"
    ).resolve()
    semantic = metadata["semantic_ltx_reference"]
    assert semantic["schema"] == "autorig-ltx-semantic-reference.v1"
    assert semantic["profile"]["sha256"] == _sha256(profile)
    assert semantic["restoration_verified"] is True
    assert semantic["render_order"] == "after_reference_rgb_before_face_id_override"
    assert semantic["composition"] == "semantic_animal_over_unchanged_canonical_rgb"
    assert semantic["classification"]["source_group_face_counts"] == {
        "fore_left": 6,
        "fore_right": 6,
        "hind_left": 6,
        "hind_right": 6,
    }
    for anatomy in ("fore", "hind"):
        assignment = semantic["classification"]["near_far_assignment"][anatomy]
        assert assignment["near_source_group"] == f"{anatomy}_left"
        assert assignment["far_source_group"] == f"{anatomy}_right"
        assert assignment["near_camera_z"] < assignment["far_camera_z"]
        assert assignment["separation"] >= semantic["gates"][
            "minimum_near_far_depth_separation"
        ]
    pixels = semantic["pixels"]
    assert pixels["semantic_mask_mismatch_pixels"] == 0
    assert set(pixels["output_label_pixel_counts"]) == {
        "fore_near",
        "fore_far",
        "hind_near",
        "hind_far",
    }
    for label, count in pixels["output_label_pixel_counts"].items():
        assert count >= semantic["gates"]["minimum_pixels_per_output_label"], label
        assert pixels["output_label_mask_fractions"][label] >= semantic["gates"][
            "minimum_pixel_fraction_of_mask"
        ]

    rgb = np.asarray(Image.open(output / "reference_rgb.png").convert("RGB"))
    semantic_rgb = np.asarray(
        Image.open(output / "reference_ltx_semantic.png").convert("RGB")
    )
    mask = np.asarray(Image.open(output / "reference_mask.png").convert("L")) >= 128
    assert rgb.shape == semantic_rgb.shape == (180, 320, 3)
    assert np.array_equal(rgb[~mask], semantic_rgb[~mask])
    assert np.all(np.any(rgb[mask] != semantic_rgb[mask], axis=1))
    assert semantic["canonical_rgb"]["sha256"] == _sha256(
        output / "reference_rgb.png"
    )
    assert semantic["canonical_mask"]["sha256"] == _sha256(
        output / "reference_mask.png"
    )

    manifest = json.loads(
        (output / "immutable_manifest.json").read_text(encoding="utf-8")
    )
    manifest_rows = {row["filename"]: row for row in manifest["files"]}
    assert "reference_ltx_semantic.png" in manifest_rows
    semantic_row = manifest_rows["reference_ltx_semantic.png"]
    assert semantic_row["sha256"] == _sha256(output / "reference_ltx_semantic.png")
    assert semantic_row["bytes"] == (output / "reference_ltx_semantic.png").stat().st_size

    with (output / "reference_ltx_semantic.png").open("ab") as stream:
        stream.write(b"tamper")
    with pytest.raises(ContractError, match="Immutable artifact size mismatch"):
        load_rig_bundle(output)
