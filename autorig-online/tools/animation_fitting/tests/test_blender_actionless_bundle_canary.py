from __future__ import annotations

import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess

import numpy as np
from PIL import Image
import pytest

from animation_fitting.rig import load_rig_bundle


BLENDER_43 = Path(
    os.environ.get(
        "AUTORIG_BLENDER_43",
        r"C:\Program Files\Blender Foundation\Blender 4.3\blender.exe",
    )
)
ROOT = Path(__file__).resolve().parents[1]


def _raw_run(*arguments: str, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(BLENDER_43), *arguments],
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )


def _run(*arguments: str, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    result = _raw_run(*arguments, timeout=timeout)
    if result.returncode != 0:
        raise AssertionError(
            f"Blender failed ({result.returncode})\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return result


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _render(source: Path, output: Path) -> tuple[str, ...]:
    return (
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
        "quadruped",
        "--orientation",
        "side",
        "--width",
        "320",
        "--height",
        "180",
        "--samples",
        "1",
    )


@pytest.mark.skipif(not BLENDER_43.is_file(), reason="Local Blender 4.3 is not installed")
def test_blender_43_actionless_bundle_and_fail_closed_contract(tmp_path: Path) -> None:
    create = _run(
        "--background",
        "--factory-startup",
        "--python",
        str(ROOT / "tests" / "blender_create_actionless_canary.py"),
        "--",
        "--output-dir",
        str(tmp_path),
    )
    assert "AUTORIG_ACTIONLESS_CANARY=" in create.stdout

    sources = {
        "valid": tmp_path / "actionless_valid_source.blend",
        "geometry_modifier": tmp_path / "actionless_geometry_modifier_source.blend",
        "two_armatures": tmp_path / "actionless_two_armatures_source.blend",
        "five_weights": tmp_path / "actionless_five_weights_source.blend",
    }
    source_hashes = {name: _sha256(path) for name, path in sources.items()}

    output = tmp_path / "valid-output"
    render = _run(*_render(sources["valid"], output))
    assert "AUTORIG_FITTING_BUNDLE=" in render.stdout
    assert "AUTORIG_FITTING_BUNDLE_ERROR=" not in render.stdout + render.stderr

    rig = load_rig_bundle(output)
    metadata = json.loads((output / "fitting_bundle.json").read_text(encoding="utf-8"))
    manifest_path = output / "immutable_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert metadata["revision"] == "autorig_actionless_bundle_v2"
    assert metadata["counts"]["armatures"] == 1
    assert rig.immutable_manifest_path == manifest_path.resolve()
    assert rig.immutable_manifest_sha256 == _sha256(manifest_path)

    actionless = metadata["actionless"]
    assert actionless["frame"] == 0
    assert actionless["armature_pose_position"] == "REST"
    assert actionless["actionless"] is True
    assert any("PoisonObjectAction" in value for value in actionless["detached_actions"])
    assert any("PoisonDataAction" in value for value in actionless["detached_actions"])
    assert actionless["muted_nla_tracks"] >= 1
    assert actionless["muted_drivers"] >= 2
    assert actionless["muted_object_constraints"] >= 1
    assert actionless["muted_pose_constraints"] >= 1
    assert actionless["reset_shape_keys"] >= 1
    assert actionless["unresolved_animated_rna_channels"] == []
    assert {
        channel.rsplit(":", 1)[-1]
        for channel in actionless["reset_animated_rna_channels"]
    } >= {"location[0]", "scale[0]", "location[1]", 'key_blocks["PoisonShape"].value[0]'}

    skeleton = json.loads((output / "skeleton.json").read_text(encoding="utf-8"))
    assert skeleton["armatures"][0]["matrix_world"] == pytest.approx(
        [
            1.0,
            0.0,
            0.0,
            0.0,
            0.0,
            1.0,
            0.0,
            0.0,
            0.0,
            0.0,
            1.0,
            0.0,
            0.0,
            0.0,
            0.0,
            1.0,
        ],
        abs=1e-7,
    )
    geometry = metadata["renderer"]["geometry_contract"]
    assert geometry["allowed_modifier"] == "single_armature_only"
    assert geometry["raw_evaluated_vertex_identity"] is True
    assert geometry["maximum_world_delta"] <= 1e-5

    camera_z = np.load(output / "reference_camera_z.npy", allow_pickle=False)
    mask = np.asarray(Image.open(output / "reference_mask.png").convert("L")) >= 128
    assert camera_z.dtype == np.float32
    assert camera_z.shape == (180, 320)
    assert np.array_equal(np.isfinite(camera_z), mask)
    assert np.all(np.isnan(camera_z[~mask]))
    assert np.all(camera_z[mask] > 0.0)
    camera_z_contract = metadata["camera"]["camera_z_contract"]
    finite = camera_z[mask]
    assert camera_z_contract["valid_pixels"] == int(mask.sum())
    assert camera_z_contract["minimum"] == pytest.approx(float(np.min(finite)))
    assert camera_z_contract["median"] == pytest.approx(float(np.median(finite)))
    assert camera_z_contract["maximum"] == pytest.approx(float(np.max(finite)))

    with gzip.open(output / "skin_weights.json.gz", "rt", encoding="utf-8") as handle:
        skin = json.load(handle)
    for vertex in skin["vertices"]:
        weights = vertex["weights"]
        assert 1 <= len(weights) <= 4
        assert sum(item["weight"] for item in weights) == pytest.approx(1.0, abs=1e-8)
        assert sorted(item["weight"] for item in weights) == pytest.approx([0.4, 0.6])

    rows = {row["filename"]: row for row in manifest["files"]}
    expected_files = {"fitting_bundle.json"} | {
        record["filename"] for record in metadata["artifacts"].values()
    }
    assert set(rows) == expected_files
    assert manifest["bundle_file_count"] == len(expected_files)
    assert manifest["bundle_total_bytes"] == sum(row["bytes"] for row in rows.values())
    for filename, row in rows.items():
        artifact = output / filename
        assert row["bytes"] == artifact.stat().st_size
        assert row["sha256"] == _sha256(artifact)

    failing_cases = {
        "two_armatures": "exactly one armature",
        "geometry_modifier": "geometry-changing modifiers",
        "five_weights": "5 nonzero deform weights",
    }
    for name, message in failing_cases.items():
        failed_output = tmp_path / f"failed-{name}"
        failed = _raw_run(*_render(sources[name], failed_output))
        combined = failed.stdout + failed.stderr
        assert failed.returncode != 0
        assert "AUTORIG_FITTING_BUNDLE_ERROR=" in combined
        assert "AUTORIG_FITTING_BUNDLE={" not in combined
        assert message in combined
        assert not (failed_output / "immutable_manifest.json").exists()

    assert {name: _sha256(path) for name, path in sources.items()} == source_hashes
