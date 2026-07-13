from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess

from jsonschema import Draft202012Validator
import pytest


BLENDER_43 = Path(
    os.environ.get(
        "AUTORIG_BLENDER_43",
        r"C:\Program Files\Blender Foundation\Blender 4.3\blender.exe",
    )
)
ROOT = Path(__file__).resolve().parents[1]


def _run(*arguments: str, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    result = _raw_run(*arguments, timeout=timeout)
    if result.returncode != 0:
        raise AssertionError(
            f"Blender failed ({result.returncode})\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return result


def _raw_run(*arguments: str, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(BLENDER_43), *arguments],
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.skipif(not BLENDER_43.is_file(), reason="Local Blender 4.3 is not installed")
def test_blender_43_two_bone_motion_applier_canary(tmp_path: Path) -> None:
    create = _run(
        "--background",
        "--factory-startup",
        "--python",
        str(ROOT / "tests" / "blender_create_two_bone_canary.py"),
        "--",
        "--output-dir",
        str(tmp_path),
    )
    assert "AUTORIG_TWO_BONE_CANARY=" in create.stdout
    source = tmp_path / "horse_source.blend"
    source_sha = _sha(source)
    output = tmp_path / "output"
    apply = _run(
        "--background",
        "--factory-startup",
        "--python",
        str(ROOT / "apply_fitted_motion.py"),
        "--",
        "--source",
        str(source),
        "--motion",
        str(tmp_path / "horse_motion.json"),
        "--semantic-action-id",
        "horse_walk_forward",
        "--output-dir",
        str(output),
        "--fps",
        "24",
        "--target-manifest",
        str(tmp_path / "horse_target.json"),
    )
    assert "AUTORIG_FITTED_MOTION=" in apply.stdout
    assert "AUTORIG_FITTED_MOTION_ERROR=" not in apply.stdout
    assert _sha(source) == source_sha

    blend = output / "horse_walk_forward.blend"
    fbx = output / "horse_walk_forward.fbx"
    glb = output / "horse_walk_forward.glb"
    manifest_path = output / "horse_walk_forward.animation-manifest.json"
    assert all(path.is_file() and path.stat().st_size > 0 for path in (blend, fbx, glb, manifest_path))
    assert not list(output.glob(".*.staging-*"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    schema_path = ROOT / "schemas" / "fitted-asset-bundle.v1.schema.json"
    Draft202012Validator(json.loads(schema_path.read_text(encoding="utf-8"))).validate(manifest)
    assert manifest["semantic_action_id"] == "horse_walk_forward"
    assert manifest["skin"]["max_influences"] == 1
    assert manifest["fbx_validation"]["semantic_take_name_present"] is True
    assert manifest["glb_validation"]["animation_count"] == 1
    assert manifest["glb_validation"]["duration_seconds"] == pytest.approx(2.0 / 24.0, abs=1e-6)
    for key, path in (("blend", blend), ("fbx", fbx), ("glb", glb)):
        assert manifest["artifacts"][key]["sha256"] == _sha(path)

    validate_blend = _run(
        "--background",
        "--factory-startup",
        "--python",
        str(ROOT / "tests" / "blender_validate_applied_canary.py"),
        "--",
        "--source",
        str(blend),
        "--action-id",
        "horse_walk_forward",
        "--mode",
        "blend",
    )
    assert "AUTORIG_APPLIED_CANARY_OK=blend" in validate_blend.stdout
    validate_fbx = _run(
        "--background",
        "--factory-startup",
        "--python",
        str(ROOT / "tests" / "blender_validate_applied_canary.py"),
        "--",
        "--source",
        str(fbx),
        "--action-id",
        "horse_walk_forward",
        "--mode",
        "fbx",
    )
    assert "AUTORIG_APPLIED_CANARY_OK=fbx" in validate_fbx.stdout

    failed_output = tmp_path / "failed-output"
    fail_closed = _raw_run(
        "--background",
        "--factory-startup",
        "--python",
        str(ROOT / "apply_fitted_motion.py"),
        "--",
        "--source",
        str(source),
        "--motion",
        str(tmp_path / "horse_motion.json"),
        "--semantic-action-id",
        "animation",
        "--output-dir",
        str(failed_output),
        "--fps",
        "24",
        "--target-manifest",
        str(tmp_path / "horse_target.json"),
    )
    assert fail_closed.returncode != 0
    assert "AUTORIG_FITTED_MOTION_ERROR=" in fail_closed.stdout
    assert "AUTORIG_FITTED_MOTION=" not in fail_closed.stdout
    assert not failed_output.exists() or not list(failed_output.iterdir())
    assert _sha(source) == source_sha
