from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
import textwrap
from dataclasses import replace
from pathlib import Path

import pytest

import animation_fitting_browser_qa_runner as browser_qa
from animation_fitting_browser_qa_runner import (
    BrowserHorseQaContractError,
    BrowserHorseQaPathError,
    BrowserHorseQaRequest,
    BrowserHorseQaRunnerConfig,
    BrowserHorseQaStaleOutputError,
    BrowserHorseQaSubprocessError,
    BrowserHorseQaTimeoutError,
    RUN_RECEIPT_SCHEMA,
    V14_HORSE_QA_SCOPE,
    run_browser_horse_qa,
)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def write_json(path: Path, value: object) -> bytes:
    data = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    path.write_bytes(data)
    return data


FAKE_RUNNER_BODY = r"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import time
import zlib


def digest(data):
    return hashlib.sha256(data).hexdigest()


def pin(path):
    path = Path(path).resolve()
    data = path.read_bytes()
    return {"path": os.fspath(path), "bytes": len(data), "sha256": digest(data)}


def write_json(path, value):
    data = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    Path(path).write_bytes(data)


def png_fixture():
    def chunk(kind, data):
        checksum = zlib.crc32(kind)
        checksum = zlib.crc32(data, checksum) & 0xFFFFFFFF
        return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", checksum)
    scanline = b"\x00" + b"\x20\x40\x60" * 768
    raw = scanline * 448
    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", 768, 448, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(raw, 9))
        + chunk(b"IEND", b"")
    )


parser = argparse.ArgumentParser()
parser.add_argument("--bundle-dir", required=True)
parser.add_argument("--immutable-manifest-sha256", required=True)
parser.add_argument("--fitting-bundle-sha256", required=True)
parser.add_argument("--source-model-sha256", required=True)
parser.add_argument("--three-clip", required=True)
parser.add_argument("--three-clip-sha256", required=True)
parser.add_argument("--semantic-id", required=True)
parser.add_argument("--three-module", required=True)
parser.add_argument("--three-module-sha256", required=True)
parser.add_argument("--three-revision", required=True)
parser.add_argument("--chrome", required=True)
parser.add_argument("--ffmpeg", required=True)
parser.add_argument("--ffprobe", required=True)
parser.add_argument("--output-dir", required=True)
args = parser.parse_args()

mode = MODE
if mode == "timeout":
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    (Path(args.output_dir).parent / "descendant.pid").write_text(str(child.pid), encoding="ascii")
    time.sleep(10)
if mode == "subprocess_failure":
    sys.stderr.write("synthetic runner failure\n")
    raise SystemExit(7)
if mode == "oversized_output":
    sys.stdout.write("x" * (2 * 1024 * 1024))
    sys.stdout.flush()
    time.sleep(10)

bundle = Path(args.bundle_dir).resolve()
clip = Path(args.three_clip).resolve()
three = Path(args.three_module).resolve()
output = Path(args.output_dir).resolve()
output.mkdir()
frames = output / "frames"
frames.mkdir()
png = png_fixture()
for index in range(49):
    (frames / f"frame_{index:04d}.png").write_bytes(png)
video_path = output / "fixed-camera-preview.mp4"
encoded = subprocess.run([
    args.ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin", "-y",
    "-framerate", "30", "-start_number", "0", "-i", str(frames / "frame_%04d.png"),
    "-frames:v", "49", "-an", "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28",
    "-pix_fmt", "yuv420p", "-movflags", "+faststart", str(video_path),
], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
if encoded.returncode:
    sys.stderr.buffer.write(encoded.stderr)
    raise SystemExit(8)
probed = subprocess.run([
    args.ffprobe, "-v", "error", "-count_frames", "-show_entries",
    "format=format_name,duration:stream=index,codec_type,codec_name,pix_fmt,width,height,r_frame_rate,nb_read_frames",
    "-of", "json", str(video_path),
], check=True, stdout=subprocess.PIPE)
probe = json.loads(probed.stdout)
video_stream = next(stream for stream in probe["streams"] if stream["codec_type"] == "video")
rate_numerator, rate_denominator = map(float, video_stream["r_frame_rate"].split("/"))

maximum_edge_stretch = 5.000001 if mode in {"machine_fail", "forged_gate"} else 5.0
passed = mode not in {"machine_fail"}
machine_gates = {
    "maximumEdgeStretch": mode != "machine_fail",
    "p99EdgeStretch": True,
    "zeroWeightVertices": True,
    "coincidentRestSeparation": True,
    "rootMotionLocked": True,
    "cameraStatic": True,
}
if mode == "forged_gate":
    machine_gates["maximumEdgeStretch"] = True
frame_rows = [
    {
        "frameIndex": index,
        "timeSeconds": index / 30,
        "maximumEdgeStretch": maximum_edge_stretch,
        "p99EdgeStretch": 2.5,
        "collapsedEdgeSampleCount": 0,
        "maximumCoincidentRestSeparationM": 0.04,
        "rootMotionLocked": True,
        "cameraStatic": True,
    }
    for index in range(49)
]
deformation = {
    "schema": "autorig.browser-horse-target-deformation-qa.v1",
    "measuredEveryFrame": True,
    "frameCount": 49,
    "vertexCount": 344,
    "edgeCount": 10,
    "edgeSampleCount": 490,
    "collapsedEdgeSampleCount": 0,
    "coincidentRestGroupCount": 1,
    "coincidentRestSampleCount": 49,
    "maximumEdgeStretch": maximum_edge_stretch,
    "p99EdgeStretch": 2.5,
    "zeroWeightVertices": 0,
    "maximumCoincidentRestSeparationM": 0.04,
    "thresholds": {
        "maximumEdgeStretch": 5.0,
        "p99EdgeStretch": 2.5,
        "zeroWeightVertices": 0,
        "coincidentRestSeparationM": 0.04,
    },
    "rootMotionLocked": True,
    "cameraStatic": True,
    "gates": machine_gates,
    "passed": passed,
    "frames": frame_rows,
    "inputs": {
        "fittingBundleSha256": args.fitting_bundle_sha256,
        "threeClipSha256": args.three_clip_sha256,
        "skinWeightsSha256": pin(bundle / "skin_weights.json.gz")["sha256"],
        "topologySha256": pin(bundle / "surface_topology.json.gz")["sha256"],
    },
}
write_json(output / "deformation-report.json", deformation)
camera = {
    "schema": "autorig.browser-horse-fixed-camera.v1",
    "camera": {"name": "canonical"},
    "rootMotionPolicy": "suppress_armature_root_tracks_and_lock_model_transform",
    "resolution": [768, 448],
    "renderer": "fake",
}
write_json(output / "camera-settings.json", camera)

phase_names = ("start", "middle", "three_quarter")
phase_indices = (0, 24, 36)
phase_pins = []
for phase, index in zip(phase_names, phase_indices):
    row = pin(frames / f"frame_{index:04d}.png")
    row.update({"phase": phase, "frame_index": index})
    phase_pins.append(row)

deformation_pin = pin(output / "deformation-report.json")
camera_pin = pin(output / "camera-settings.json")
video_pin = pin(output / "fixed-camera-preview.mp4")
video_pin.update({
    "fixed_camera": True,
    "root_motion_locked": True,
    "container": "mp4",
    "codec": "h264",
    "pixel_format": "yuv420p",
    "width": 768,
    "height": 448,
    "fps": rate_numerator / rate_denominator,
    "frame_count": int(video_stream["nb_read_frames"]),
    "audio_stream_count": 0,
    "duration_seconds": float(probe["format"]["duration"]),
})
semantic_id = "run_forward" if mode == "tamper_action" else args.semantic_id
visual_frames = [
    {
        "phase": row["phase"],
        "frame_index": row["frame_index"],
        "evidence_url": None,
        "sha256": row["sha256"],
    }
    for row in phase_pins
]
evidence = {
    "schema": "autorig.browser-horse-visual-phase-evidence-envelope.v1",
    "visual_phase_gate": {
        "schema": "autorig.animation-visual-phase-qa.v1",
        "version": 1,
        "rig_type": "horse",
        "semantic_id": semantic_id,
        "fitted_clip_sha256": args.three_clip_sha256,
        "decision": None,
        "camera": {
            "static": True,
            "projection": "perspective",
            "view": "canonical_fitting_bundle",
            "root_motion_locked": True,
            "settings_sha256": camera_pin["sha256"],
        },
        "coincident_rest_vertex_separation": {
            "measured": True,
            "pass": True,
            "threshold_m": 0.04,
            "max_separation_m": 0.04,
            "sample_count": 49,
            "group_count": 1,
            "report_url": None,
            "report_sha256": deformation_pin["sha256"],
        },
        "required_phases": list(phase_names),
        "frames": visual_frames,
        "reviewer": {"id": None, "reviewed_at": None},
    },
    "local_evidence": {
        "source_rig_type": "HORSE_2",
        "browser_only": True,
        "blender_used": False,
        "animation_evaluation": "Three.AnimationMixer",
        "immutable_inputs": {
            "source_model": {"filename": "Horse_2.blend", "sha256": args.source_model_sha256},
            "immutable_manifest": pin(bundle / "immutable_manifest.json"),
            "fitting_bundle": pin(bundle / "fitting_bundle.json"),
            "skeleton": pin(bundle / "skeleton.json"),
            "skin_weights": pin(bundle / "skin_weights.json.gz"),
            "surface_topology": pin(bundle / "surface_topology.json.gz"),
            "three_clip": pin(clip),
        },
        "camera_settings": camera_pin,
        "target_mesh_deformation_qa": {
            "measured_every_frame": True,
            "passed": passed,
            "maximum_edge_stretch": maximum_edge_stretch,
            "p99_edge_stretch": 2.5,
            "zero_weight_vertices": 0,
            "thresholds": {
                "maximum_edge_stretch": 5.0,
                "p99_edge_stretch": 2.5,
                "zero_weight_vertices": 0,
            },
            "report": deformation_pin,
        },
        "phase_frames": phase_pins,
        "video": video_pin,
        "renderer": {
            "browser": "headless_chrome_cdp",
            "three_revision": "160",
            "three_module": pin(three),
            "runtime": {"renderer": "fake"},
        },
        "human_review": {
            "decision": None,
            "reviewer_id": None,
            "reviewed_at": None,
            "required": True,
        },
        "approvals": {
            "machine_qa_passed": passed,
            "ready_for_human_review": passed,
            "approved_for_animation_library": False,
            "release_ready": False,
            "fail_closed_reason": "human_pending" if passed else "machine_failed",
        },
    },
}
if mode == "path_escape":
    evidence["local_evidence"]["video"]["path"] = os.fspath(bundle / "immutable_manifest.json")
write_json(output / "visual-phase-qa.json", evidence)
if mode == "extra_artifact":
    (output / "untrusted.txt").write_text("extra", encoding="utf-8")
if mode == "mutate_input":
    clip.write_bytes(clip.read_bytes() + b" ")

status = "PASS_MACHINE_QA_AWAITING_HUMAN" if passed else "FAIL_MACHINE_QA"
print(json.dumps({
    "status": status,
    "approvedForAnimationLibrary": False,
    "evidencePath": os.fspath(output / "visual-phase-qa.json"),
    "videoPath": os.fspath(output / "fixed-camera-preview.mp4"),
}, separators=(",", ":")))
raise SystemExit(0 if passed else 3)
"""


def build_fixture(tmp_path: Path, *, mode: str = "pass", timeout_seconds: float = 5.0):
    input_root = tmp_path / "inputs"
    output_root = tmp_path / "outputs"
    bundle = tmp_path / "bundle"
    runtime = tmp_path / "runtime"
    candidate = input_root / "jobs" / "job-1" / "candidates" / "candidate-1"
    for directory in (output_root, bundle, runtime, candidate):
        directory.mkdir(parents=True, exist_ok=True)

    clip_data = write_json(
        candidate / "three-clip.json", {"name": "Horse_Walk_BrowserFit"}
    )
    fitting_data = write_json(
        bundle / "fitting_bundle.json", {"schema": "test-fitting"}
    )
    skeleton_data = write_json(bundle / "skeleton.json", {"bones": ["root"]})
    skin_data = b"synthetic-gzip-skin"
    topology_data = b"synthetic-gzip-topology"
    (bundle / "skin_weights.json.gz").write_bytes(skin_data)
    (bundle / "surface_topology.json.gz").write_bytes(topology_data)
    bundle_files = {
        "fitting_bundle.json": fitting_data,
        "skeleton.json": skeleton_data,
        "skin_weights.json.gz": skin_data,
        "surface_topology.json.gz": topology_data,
    }
    immutable_data = write_json(
        bundle / "immutable_manifest.json",
        {
            "schema": "test-immutable",
            "files": [
                {"filename": name, "bytes": len(data), "sha256": sha256(data)}
                for name, data in bundle_files.items()
            ],
        },
    )

    profile_data = write_json(
        runtime / "qa_profile.v1.json",
        {
            "schema": "autorig.animation-fitting-qa.v1",
            "calibration_state_string": "provisional-horse-v1",
            "hard_gate_metric_keys_array": ["camera_locked_ok"],
            "ranking_weights_object": {"prompt_alignment_float": 1.0},
        },
    )
    three_data = b"export const REVISION = '160';\n"
    (runtime / "three.module.js").write_bytes(three_data)
    chrome = runtime / "chrome.exe"
    chrome.write_bytes(b"fake pinned Chrome executable")
    ffmpeg = Path(shutil.which("ffmpeg") or "C:/API/ffmpeg/bin/ffmpeg.exe")
    ffprobe = Path(shutil.which("ffprobe") or "C:/API/ffmpeg/bin/ffprobe.exe")
    if not ffmpeg.is_file() or not ffprobe.is_file():
        pytest.skip("focused browser QA adapter tests require ffmpeg and ffprobe")
    runner = runtime / "fake_browser_qa.py"
    runner_source = "MODE = " + repr(mode) + "\n" + textwrap.dedent(FAKE_RUNNER_BODY)
    runner.write_text(runner_source, encoding="utf-8")

    config = BrowserHorseQaRunnerConfig(
        input_root=input_root,
        output_root=output_root,
        bundle_directory=bundle,
        runner_executable=Path(sys.executable),
        expected_runner_executable_sha256=sha256(Path(sys.executable).read_bytes()),
        runner_script=runner,
        expected_runner_script_sha256=sha256(runner.read_bytes()),
        qa_profile_path=runtime / "qa_profile.v1.json",
        expected_qa_profile_sha256=sha256(profile_data),
        three_module=runtime / "three.module.js",
        expected_three_module_sha256=sha256(three_data),
        chrome_executable=chrome,
        expected_chrome_executable_sha256=sha256(chrome.read_bytes()),
        ffmpeg_executable=ffmpeg,
        expected_ffmpeg_executable_sha256=sha256(ffmpeg.read_bytes()),
        ffprobe_executable=ffprobe,
        expected_ffprobe_executable_sha256=sha256(ffprobe.read_bytes()),
        expected_immutable_manifest_sha256=sha256(immutable_data),
        expected_fitting_bundle_sha256=sha256(fitting_data),
        expected_source_model_sha256="a" * 64,
        timeout_seconds=timeout_seconds,
    )
    request = BrowserHorseQaRequest(
        job_id="job-1",
        candidate_id="candidate-1",
        attempt_id="attempt-1",
        semantic_id="walk_forward",
        expected_three_clip_sha256=sha256(clip_data),
    )
    return config, request


def test_browser_runner_pass_writes_canonical_pinned_receipt_without_ranking_values(
    tmp_path: Path,
):
    config, request = build_fixture(tmp_path)

    result = run_browser_horse_qa(config, request)

    assert result.machine_qa_passed is True
    assert result.ready_for_human_review is True
    assert result.human_visual_decision is None
    assert result.approved_for_animation_library is False
    assert result.machine_gates["maximumEdgeStretch"] is True
    receipt_bytes = result.run_receipt_path.read_bytes()
    assert sha256(receipt_bytes) == result.run_receipt_sha256
    assert len(receipt_bytes) == result.run_receipt_bytes
    receipt = json.loads(receipt_bytes)
    assert receipt["schema"] == RUN_RECEIPT_SCHEMA
    assert receipt["execution"]["browser_only"] is True
    assert receipt["execution"]["blender_used"] is False
    assert receipt["execution"]["fixed_camera_mp4_probe"] == {
        "audio_stream_count": 0,
        "codec": "h264",
        "container": "mp4",
        "duration_seconds": 1.633333,
        "fps": 30.0,
        "frame_count": 49,
        "height": 448,
        "pixel_format": "yuv420p",
        "width": 768,
    }
    assert receipt["qa_profile"]["calibration_state_string"] == "provisional-horse-v1"
    assert receipt["qa_profile"]["adapter_scope"] == V14_HORSE_QA_SCOPE
    assert receipt["qa_profile"]["production_eligible"] is False
    assert receipt["qa_profile"]["semantic_id"] == "walk_forward"
    assert receipt["qa_profile"]["frame_count"] == 49
    assert receipt["qa_profile"]["output_fps"] == 30.0
    assert receipt["qa_profile"]["deformation_thresholds"] == {
        "coincidentRestSeparationM": 0.04,
        "maximumEdgeStretch": 5.0,
        "p99EdgeStretch": 2.5,
        "zeroWeightVertices": 0,
    }
    assert (
        receipt["qa_profile"]["profile_pin"]["sha256"]
        == config.expected_qa_profile_sha256
    )
    assert receipt["qa_profile"]["ranking_metrics_emitted"] is False
    assert "ranking_metrics" not in receipt
    assert receipt["gates"]["human_visual_decision"] is None
    assert receipt["gates"]["approved_for_animation_library"] is False
    assert len([key for key in receipt["artifacts"] if key.startswith("frames/")]) == 49
    assert set(receipt["required_phase_artifacts"]) == {
        "start",
        "middle",
        "three_quarter",
    }
    assert receipt_bytes == (
        json.dumps(
            receipt, ensure_ascii=False, allow_nan=False, indent=2, sort_keys=True
        )
        + "\n"
    ).encode("utf-8")


def test_machine_gate_failure_is_a_pinned_non_approving_result(tmp_path: Path):
    config, request = build_fixture(tmp_path, mode="machine_fail")

    result = run_browser_horse_qa(config, request)

    assert result.machine_qa_passed is False
    assert result.ready_for_human_review is False
    assert result.approved_for_animation_library is False
    assert result.machine_gates["maximumEdgeStretch"] is False
    receipt = json.loads(result.run_receipt_path.read_text(encoding="utf-8"))
    assert receipt["status"] == "FAIL_MACHINE_QA"
    assert receipt["execution"]["subprocess_exit_code"] == 3
    assert receipt["gates"]["machine"]["maximumEdgeStretch"] is False


def test_exact_horse_v14_thresholds_pass_but_boolean_forgery_is_rejected(
    tmp_path: Path,
):
    boundary_config, boundary_request = build_fixture(tmp_path / "boundary")
    boundary = run_browser_horse_qa(boundary_config, boundary_request)
    assert boundary.machine_qa_passed is True

    forged_config, forged_request = build_fixture(
        tmp_path / "forged", mode="forged_gate"
    )
    with pytest.raises(
        BrowserHorseQaContractError,
        match="gate booleans disagree with trusted Horse V14 thresholds",
    ):
        run_browser_horse_qa(forged_config, forged_request)
    assert not (
        forged_config.output_root / "jobs/job-1/candidates/candidate-1/attempt-1"
    ).exists()


def test_non_v14_semantic_is_rejected_without_attempt(tmp_path: Path):
    config, request = build_fixture(tmp_path)
    request = replace(request, semantic_id="run_forward")

    with pytest.raises(
        BrowserHorseQaContractError, match="only the nonproduction Horse V14"
    ):
        run_browser_horse_qa(config, request)
    assert not (config.output_root / "jobs").exists()


def test_runtime_executable_sha_pins_are_server_enforced(tmp_path: Path):
    config, request = build_fixture(tmp_path)
    config = replace(config, expected_ffprobe_executable_sha256="0" * 64)

    with pytest.raises(BrowserHorseQaContractError, match="server-owned SHA-256"):
        run_browser_horse_qa(config, request)


@pytest.mark.parametrize(
    ("mode", "message"),
    [
        ("tamper_action", "action semantic"),
        ("path_escape", "canonical server path"),
        ("extra_artifact", "artifact inventory"),
    ],
)
def test_tampered_or_extra_evidence_fails_closed(
    tmp_path: Path, mode: str, message: str
):
    config, request = build_fixture(tmp_path, mode=mode)

    with pytest.raises(BrowserHorseQaContractError, match=message):
        run_browser_horse_qa(config, request)


def test_stale_attempt_is_rejected_before_subprocess(tmp_path: Path):
    config, request = build_fixture(tmp_path)
    stale = (
        config.output_root
        / "jobs"
        / request.job_id
        / "candidates"
        / request.candidate_id
        / request.attempt_id
    )
    stale.mkdir(parents=True)
    (stale / "old.txt").write_text("must not be overwritten", encoding="utf-8")

    with pytest.raises(BrowserHorseQaStaleOutputError, match="already exists"):
        run_browser_horse_qa(config, request)
    assert (stale / "old.txt").read_text(encoding="utf-8") == "must not be overwritten"


def test_clip_pin_mismatch_does_not_create_an_attempt(tmp_path: Path):
    config, request = build_fixture(tmp_path)
    request = replace(request, expected_three_clip_sha256="0" * 64)
    attempt = (
        config.output_root
        / "jobs"
        / request.job_id
        / "candidates"
        / request.candidate_id
        / request.attempt_id
    )

    with pytest.raises(
        BrowserHorseQaContractError, match="externally supplied SHA-256"
    ):
        run_browser_horse_qa(config, request)
    assert not attempt.exists()


def test_input_toctou_after_subprocess_is_rejected_and_attempt_is_retryable(
    tmp_path: Path,
):
    config, request = build_fixture(tmp_path, mode="mutate_input")
    attempt = (
        config.output_root
        / "jobs"
        / request.job_id
        / "candidates"
        / request.candidate_id
        / request.attempt_id
    )

    with pytest.raises(
        BrowserHorseQaContractError, match="changed during the QA attempt"
    ):
        run_browser_horse_qa(config, request)
    assert not attempt.exists()


def test_final_output_resnapshot_detects_post_verification_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    config, request = build_fixture(tmp_path)
    original_verify = browser_qa._verify_evidence

    def mutate_after_verify(**kwargs):
        result = original_verify(**kwargs)
        video = kwargs["paths"].qa_output_directory / "fixed-camera-preview.mp4"
        video.write_bytes(video.read_bytes() + b"post-verification-tamper")
        return result

    monkeypatch.setattr(browser_qa, "_verify_evidence", mutate_after_verify)
    with pytest.raises(
        BrowserHorseQaContractError,
        match="output inventory changed after evidence verification",
    ):
        run_browser_horse_qa(config, request)
    assert not (
        config.output_root / "jobs/job-1/candidates/candidate-1/attempt-1"
    ).exists()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("job_id", "../escape"),
        ("candidate_id", "candidate/escape"),
        ("attempt_id", ".."),
        ("attempt_id", "CON"),
        ("attempt_id", "attempt."),
    ],
)
def test_identifier_path_escape_is_rejected(tmp_path: Path, field: str, value: str):
    config, request = build_fixture(tmp_path)
    request = replace(request, **{field: value})

    with pytest.raises(BrowserHorseQaPathError, match="safe server identifier"):
        run_browser_horse_qa(config, request)


def test_symlink_input_is_rejected_when_supported(tmp_path: Path):
    config, request = build_fixture(tmp_path)
    clip = (
        config.input_root
        / "jobs"
        / request.job_id
        / "candidates"
        / request.candidate_id
        / "three-clip.json"
    )
    target = tmp_path / "external-clip.json"
    target.write_bytes(clip.read_bytes())
    clip.unlink()
    try:
        os.symlink(target, clip)
    except OSError:
        pytest.skip("symlink creation is not available in this test runtime")

    with pytest.raises(BrowserHorseQaPathError, match="symlink/reparse"):
        run_browser_horse_qa(config, request)


def test_subprocess_failure_and_timeout_are_distinct_fail_closed_errors(tmp_path: Path):
    failure_config, failure_request = build_fixture(
        tmp_path / "failure", mode="subprocess_failure"
    )
    with pytest.raises(BrowserHorseQaSubprocessError, match="exited 7"):
        run_browser_horse_qa(failure_config, failure_request)
    assert not (
        failure_config.output_root / "jobs/job-1/candidates/candidate-1/attempt-1"
    ).exists()

    timeout_config, timeout_request = build_fixture(
        tmp_path / "timeout",
        mode="timeout",
        timeout_seconds=0.1,
    )
    with pytest.raises(BrowserHorseQaTimeoutError, match="exceeded"):
        run_browser_horse_qa(timeout_config, timeout_request)
    assert not (
        timeout_config.output_root / "jobs/job-1/candidates/candidate-1/attempt-1"
    ).exists()

    noisy_config, noisy_request = build_fixture(
        tmp_path / "noisy", mode="oversized_output"
    )
    with pytest.raises(
        BrowserHorseQaSubprocessError, match="exceeded the trusted capture limit"
    ):
        run_browser_horse_qa(noisy_config, noisy_request)
    assert not (
        noisy_config.output_root / "jobs/job-1/candidates/candidate-1/attempt-1"
    ).exists()


def test_windows_nonzero_taskkill_falls_back_to_direct_kill(
    monkeypatch: pytest.MonkeyPatch,
):
    class FakeProcess:
        pid = 1234

        def __init__(self):
            self.killed = False
            self.waited = False

        def poll(self):
            return 0 if self.killed else None

        def kill(self):
            self.killed = True

        def wait(self, timeout):
            self.waited = True
            return 0

    class NonzeroResult:
        returncode = 1

    process = FakeProcess()
    monkeypatch.setattr(
        browser_qa.subprocess, "run", lambda *args, **kwargs: NonzeroResult()
    )

    browser_qa._terminate_process_tree(process, platform_name="nt")

    assert process.killed is True
    assert process.waited is True


def test_blender_named_runner_is_rejected_without_execution(tmp_path: Path):
    config, request = build_fixture(tmp_path)
    blender_runner = config.runner_script.with_name("blender_browser_qa.py")
    blender_runner.write_bytes(config.runner_script.read_bytes())
    config = replace(
        config,
        runner_script=blender_runner,
        expected_runner_script_sha256=sha256(blender_runner.read_bytes()),
    )

    with pytest.raises(BrowserHorseQaContractError, match="Blender"):
        run_browser_horse_qa(config, request)


def test_real_node_chrome_three_ffmpeg_adapter_end_to_end_when_pinned_runtime_exists(
    tmp_path: Path,
):
    repo_root = Path(__file__).resolve().parents[3]
    runner = (
        repo_root
        / "autorig-online/tools/animation_fitting/browser_horse_visual_phase_qa.mjs"
    )
    profile = (
        repo_root / "autorig-online/backend/animation_fitting/specs/qa_profile.v1.json"
    )
    bundle = Path("R:/ComfyUI-data/autorig-fitting/horse-canonical-f1")
    source_clip = Path(
        "R:/ComfyUI-data/autorig-fitting/canonical-candidates/experiments/"
        "horse-walk-rgb-v8-native-768x448-seed-4373011867009528156-guide-080-controlled-v1/"
        "qa/browser-fit-real-bundle-v1/three-clip.json"
    )
    node_text = shutil.which("node")
    node = Path(node_text) if node_text else Path("node")
    chrome = Path("C:/Program Files/Google/Chrome/Application/chrome.exe")
    three = Path("R:/ComfyUI-data/autorig-fitting/runtimes/three-r160/three.module.js")
    ffmpeg = Path(shutil.which("ffmpeg") or "C:/API/ffmpeg/bin/ffmpeg.exe")
    ffprobe = Path(shutil.which("ffprobe") or "C:/API/ffmpeg/bin/ffprobe.exe")
    required = (
        runner,
        profile,
        bundle,
        source_clip,
        node,
        chrome,
        three,
        ffmpeg,
        ffprobe,
    )
    missing = [os.fspath(path) for path in required if not path.exists()]
    if missing:
        pytest.skip(
            "real browser QA E2E requires the pinned local V14 runtime; missing: "
            + ", ".join(missing)
        )

    immutable_path = bundle / "immutable_manifest.json"
    fitting_path = bundle / "fitting_bundle.json"
    immutable = json.loads(immutable_path.read_text(encoding="utf-8"))
    source_sha = immutable["source_model"]["sha256"]
    input_root = tmp_path / "inputs"
    candidate = input_root / "jobs/real-v14/candidates/real-browser"
    output_root = tmp_path / "outputs"
    candidate.mkdir(parents=True)
    output_root.mkdir()
    clip = candidate / "three-clip.json"
    clip_value = json.loads(source_clip.read_text(encoding="utf-8"))
    # The historical v8 fixture stores its final Float32 time as
    # 1.6000000238. Normalize only that serialization artifact so the current
    # exact 0..1.6 V14 interval validator can exercise the real browser path.
    for track in clip_value["tracks"]:
        track["times"][-1] = clip_value["duration"]
    write_json(clip, clip_value)

    config = BrowserHorseQaRunnerConfig(
        input_root=input_root,
        output_root=output_root,
        bundle_directory=bundle,
        runner_executable=node,
        expected_runner_executable_sha256=sha256(node.read_bytes()),
        runner_script=runner,
        expected_runner_script_sha256=sha256(runner.read_bytes()),
        qa_profile_path=profile,
        expected_qa_profile_sha256=sha256(profile.read_bytes()),
        three_module=three,
        expected_three_module_sha256=sha256(three.read_bytes()),
        chrome_executable=chrome,
        expected_chrome_executable_sha256=sha256(chrome.read_bytes()),
        ffmpeg_executable=ffmpeg,
        expected_ffmpeg_executable_sha256=sha256(ffmpeg.read_bytes()),
        ffprobe_executable=ffprobe,
        expected_ffprobe_executable_sha256=sha256(ffprobe.read_bytes()),
        expected_immutable_manifest_sha256=sha256(immutable_path.read_bytes()),
        expected_fitting_bundle_sha256=sha256(fitting_path.read_bytes()),
        expected_source_model_sha256=source_sha,
        timeout_seconds=180,
    )
    request = BrowserHorseQaRequest(
        job_id="real-v14",
        candidate_id="real-browser",
        attempt_id="e2e-1",
        semantic_id="walk_forward",
        expected_three_clip_sha256=sha256(clip.read_bytes()),
    )

    result = run_browser_horse_qa(config, request)

    assert result.run_receipt_path.is_file()
    assert result.video_path.stat().st_size > 0
    receipt = json.loads(result.run_receipt_path.read_text(encoding="utf-8"))
    assert receipt["qa_profile"]["adapter_scope"] == V14_HORSE_QA_SCOPE
    assert receipt["execution"]["fixed_camera_mp4_probe"]["frame_count"] == 49
    assert receipt["execution"]["fixed_camera_mp4_probe"]["audio_stream_count"] == 0
