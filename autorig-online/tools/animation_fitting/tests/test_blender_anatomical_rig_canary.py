from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess

import pytest


BLENDER_51 = Path(
    os.environ.get(
        "AUTORIG_BLENDER_51",
        r"C:\Program Files\Blender Foundation\Blender 5.1\blender.exe",
    )
)
ROOT = Path(__file__).resolve().parents[1]
HORSE_PROFILE = ROOT / "profiles" / "horse_arp_deform_v1.json"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _raw_run(*arguments: str, timeout: int = 180) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(BLENDER_51), *arguments],
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )


def _run(*arguments: str, timeout: int = 180) -> subprocess.CompletedProcess[str]:
    result = _raw_run(*arguments, timeout=timeout)
    if result.returncode != 0:
        raise AssertionError(
            f"Blender failed ({result.returncode})\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return result


@pytest.mark.skipif(not BLENDER_51.is_file(), reason="Blender 5.1 is not installed")
def test_blender_51_anatomical_rig_reference_canary(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    create = _run(
        "--background",
        "--factory-startup",
        "--python",
        str(ROOT / "tests" / "blender_create_anatomical_rig_canary.py"),
        "--",
        "--output-dir",
        str(source_dir),
        "--profile-template",
        str(HORSE_PROFILE),
    )
    assert "AUTORIG_ANATOMICAL_SOURCE_CANARY=" in create.stdout
    source = source_dir / "Horse_2.blend"
    profile = source_dir / "canary_profile.json"
    source_sha = _sha256(source)

    default_output = tmp_path / "default-output"
    default = _raw_run(
        "--background",
        "--factory-startup",
        "--python",
        str(ROOT / "build_anatomical_rig.py"),
        "--",
        "--input",
        str(source),
        "--profile",
        str(profile),
        "--output-dir",
        str(default_output),
    )
    assert default.returncode != 0
    assert "AUTORIG_ANATOMICAL_RIG_ERROR=" in default.stdout + default.stderr
    assert "not fitting-ready" in default.stdout + default.stderr
    assert not default_output.exists()

    output = tmp_path / "reference-output"
    built = _run(
        "--background",
        "--factory-startup",
        "--python",
        str(ROOT / "build_anatomical_rig.py"),
        "--",
        "--input",
        str(source),
        "--profile",
        str(profile),
        "--output-dir",
        str(output),
        "--reference-only",
    )
    assert "AUTORIG_ANATOMICAL_RIG=" in built.stdout
    assert "AUTORIG_ANATOMICAL_RIG_ERROR=" not in built.stdout + built.stderr
    report = json.loads((output / "build_report.json").read_text(encoding="utf-8"))
    assert report["approval"]["artifact_fitting_ready"] is False
    assert report["approval"]["blocker_state"] == "blocked"

    validate = _run(
        "--background",
        "--factory-startup",
        "--python",
        str(ROOT / "tests" / "blender_validate_anatomical_rig_canary.py"),
        "--",
        "--source",
        str(source),
        "--profile",
        str(profile),
        "--output-dir",
        str(output),
    )
    assert "AUTORIG_ANATOMICAL_RIG_CANARY=OK" in validate.stdout
    assert _sha256(source) == source_sha
