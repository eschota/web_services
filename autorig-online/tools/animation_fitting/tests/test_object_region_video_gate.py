from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil
import subprocess

import cv2
import numpy as np
import pytest

import animation_fitting.object_region_video_gate as object_region_gate
from animation_fitting.errors import ContractError
from animation_fitting.object_region_video_gate import (
    FRAME_COUNT,
    HEIGHT,
    KEY_FRAMES,
    SCHEMA,
    WIDTH,
    frame_set_digest,
    main,
    run_object_region_video_gate,
)


def _png(image: np.ndarray) -> bytes:
    ok, encoded = cv2.imencode(".png", image)
    assert ok
    return encoded.tobytes()


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _endpoint() -> np.ndarray:
    image = np.full((HEIGHT, WIDTH, 3), (216, 210, 205), dtype=np.uint8)
    color = (238, 238, 238)
    cv2.rectangle(image, (285, 135), (500, 250), color, -1)
    cv2.fillPoly(
        image,
        [np.asarray(((225, 115), (300, 105), (330, 180), (295, 235), (245, 205)))],
        color,
    )
    for x in (300, 350, 430, 475):
        cv2.rectangle(image, (x, 230), (x + 24, 350), color, -1)
        cv2.rectangle(image, (x - 4, 342), (x + 30, 361), color, -1)
    cv2.rectangle(image, (490, 155), (520, 215), color, -1)
    cv2.line(image, (500, 165), (548, 205), color, 10)
    cv2.circle(image, (250, 155), 6, (55, 55, 55), -1)
    return image


def _swing(endpoint: np.ndarray, leg_x: int, direction: int) -> np.ndarray:
    image = endpoint.copy()
    background = (216, 210, 205)
    color = (238, 238, 238)
    cv2.rectangle(image, (leg_x - 6, 228), (leg_x + 36, 365), background, -1)
    cv2.line(image, (leg_x + 12, 238), (leg_x + direction * 25, 292), color, 23)
    cv2.line(
        image,
        (leg_x + direction * 25, 292),
        (leg_x + direction * 42, 330),
        color,
        19,
    )
    cv2.rectangle(
        image,
        (leg_x + direction * 42 - 9, 324),
        (leg_x + direction * 42 + 20, 341),
        color,
        -1,
    )
    return image


def _without_hoof(image: np.ndarray, leg_x: int = 475) -> np.ndarray:
    result = image.copy()
    cv2.rectangle(result, (leg_x - 4, 342), (leg_x + 30, 361), (216, 210, 205), -1)
    return result


def _write_candidate(directory: Path, frames: list[np.ndarray]) -> tuple[str, int]:
    directory.mkdir()
    payloads = []
    for index, frame in enumerate(frames):
        filename = f"frame_{index:06d}.png"
        payload = _png(frame)
        (directory / filename).write_bytes(payload)
        payloads.append((filename, payload))
    return frame_set_digest(payloads)


def _ffmpeg_binary() -> str:
    preferred = Path(r"C:\API\ffmpeg\bin\ffmpeg.exe")
    if preferred.is_file():
        return str(preferred)
    discovered = shutil.which("ffmpeg")
    if discovered is None:
        pytest.skip("ffmpeg with libx264 is unavailable")
    return discovered


def _write_h264_candidate(path: Path, frames: list[np.ndarray]) -> bytes:
    completed = subprocess.run(
        [
            _ffmpeg_binary(),
            "-v",
            "error",
            "-y",
            "-f",
            "rawvideo",
            "-pix_fmt",
            "bgr24",
            "-s:v",
            f"{WIDTH}x{HEIGHT}",
            "-r",
            "30",
            "-i",
            "pipe:0",
            "-frames:v",
            str(FRAME_COUNT),
            "-an",
            "-c:v",
            "libx264",
            "-preset",
            "medium",
            "-crf",
            "23",
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
            str(path),
        ],
        input=b"".join(frame.tobytes() for frame in frames),
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        pytest.fail(
            "ffmpeg libx264 golden encode failed: "
            + completed.stderr.decode("utf-8", errors="replace")
        )
    payload = path.read_bytes()
    assert payload
    return payload


def _write_bundle(directory: Path, guide_images: dict[int, np.ndarray]) -> tuple[Path, str, int, str]:
    directory.mkdir()
    endpoint_payload = _png(guide_images[0])
    endpoint_sha = _sha(endpoint_payload)
    entries = []
    for index in KEY_FRAMES:
        payload = endpoint_payload if index in (0, 12, 24, 36, 48) else _png(guide_images[index])
        filename = f"guide_{index:03d}.png"
        (directory / filename).write_bytes(payload)
        entries.append(
            {
                "frame_index_int": index,
                "filename_string": filename,
                "sha256_string": _sha(payload),
                "bytes_int": len(payload),
                "strength_float": 0.8,
            }
        )
    manifest = {
        "schema": "autorig-browser-ltx-recovery-guide-bundle.v1",
        "resolution": [WIDTH, HEIGHT],
        "cycle_frame_count_int": FRAME_COUNT,
        "guide_count_int": len(KEY_FRAMES),
        "recovery_frame_indices_array": [12, 24, 36],
        "recovery_guides_byte_identical_endpoint_bool": True,
        "endpoint_guide_sha256_string": endpoint_sha,
        "frames_array": entries,
    }
    manifest_payload = (json.dumps(manifest, indent=2) + "\n").encode()
    (directory / "immutable_manifest.json").write_bytes(manifest_payload)
    return directory / "guide_000.png", endpoint_sha, len(endpoint_payload), _sha(manifest_payload)


def _write_interval_bundle(
    directory: Path, guide_images: dict[int, np.ndarray]
) -> tuple[Path, str, int, str]:
    directory.mkdir()
    endpoint_payload = _png(guide_images[0])
    endpoint_sha = _sha(endpoint_payload)
    entries = []
    decoded_pins = []
    for index in range(FRAME_COUNT):
        image = guide_images.get(index, guide_images[0])
        payload = endpoint_payload if index in (0, 12, 24, 36, 48) else _png(image)
        filename = f"guide_{index:03d}.png"
        (directory / filename).write_bytes(payload)
        decoded_sha = hashlib.sha256(np.ascontiguousarray(image).tobytes()).hexdigest()
        decoded_pins.append(decoded_sha)
        entries.append(
            {
                "frame_index_int": index,
                "filename_string": filename,
                "sha256_string": _sha(payload),
                "bytes_int": len(payload),
                "decoded_rgb_sha256_string": decoded_sha,
                "source_anchor_byte_identical_bool": index in KEY_FRAMES,
            }
        )
    interval_payload = b"synthetic-lossless-v14-interval-video"
    (directory / "interval_guide.mkv").write_bytes(interval_payload)
    pose_payload = b'{"schema":"synthetic-v14-pose-contract.v1"}\n'
    (directory / "pose_contract.json").write_bytes(pose_payload)
    manifest = {
        "schema": "autorig-browser-ltx-interval-guide-bundle.v1",
        "resolution": [WIDTH, HEIGHT],
        "cycle_frame_count_int": FRAME_COUNT,
        "guide_count_int": 1,
        "browser_frame_count_int": FRAME_COUNT,
        "recovery_frame_indices_array": [12, 24, 36],
        "recovery_guides_byte_identical_endpoint_bool": None,
        "source_anchor_frame_indices_array": list(KEY_FRAMES),
        "source_anchors_byte_identical_bool": True,
        "endpoint_guide_sha256_string": endpoint_sha,
        "renderer_object": {
            "renderer_string": "browser_threejs",
            "scene_contract_string": "v14_unified_browser_interval_guide_v1",
            "all_guide_frames_browser_rendered_bool": True,
            "blender_used_bool": False,
            "shadows_enabled_bool": False,
            "deterministic_contact_cues_bool": True,
            "per_guide_contact_cue_visibility_bool": False,
            "per_frame_contact_cue_visibility_bool": True,
            "contact_cue_implementation_string": (
                "static_rest_hoof_radial_alpha_planes"
            ),
        },
        "deterministicRenderQa": {
            "status": "PASS",
            "frameCount": FRAME_COUNT,
            "byteIdenticalFrameCount": FRAME_COUNT,
            "mismatchFrameIndices": [],
        },
        "losslessVideoQa": {
            "status": "PASS",
            "frameCount": FRAME_COUNT,
            "codec": "png",
            "pixelFormat": "rgb24",
            "decodedRgbSha256MatchesBrowserFrames": True,
        },
        "contactCueQa": {
            "status": "PASS",
            "perFrameVisibility": True,
            "swingFramesHideExactlyOneCue": True,
            "barrierFramesShowAllFourCues": True,
            "frames": [{"frameIndex": index} for index in range(FRAME_COUNT)],
        },
        "frames_array": entries,
        "interval_guide_video_object": {
            "filename": "interval_guide.mkv",
            "bytes": len(interval_payload),
            "sha256": _sha(interval_payload),
            "container": "matroska",
            "codec": "png",
            "pixelFormat": "rgb24",
            "width": WIDTH,
            "height": HEIGHT,
            "frameCount": FRAME_COUNT,
            "audioStreamCount": 0,
            "decoded_rgb_sha256_array": decoded_pins,
            "exact_browser_frame_rgb_bool": True,
            "load_video_node_compatible_bool": True,
        },
        "poseContract": {
            "filename": "pose_contract.json",
            "bytes": len(pose_payload),
            "sha256": _sha(pose_payload),
        },
    }
    manifest_payload = (json.dumps(manifest, indent=2) + "\n").encode()
    (directory / "immutable_manifest.json").write_bytes(manifest_payload)
    return directory / "guide_000.png", endpoint_sha, len(endpoint_payload), _sha(manifest_payload)


@pytest.fixture(scope="module")
def contract(tmp_path_factory: pytest.TempPathFactory) -> dict:
    root = tmp_path_factory.mktemp("object-region-gate")
    endpoint = _endpoint()
    guide_images = {
        0: endpoint,
        6: _swing(endpoint, 475, -1),
        12: endpoint,
        18: _swing(endpoint, 300, -1),
        24: endpoint,
        30: _swing(endpoint, 430, 1),
        36: endpoint,
        42: _swing(endpoint, 350, 1),
        48: endpoint,
    }
    bundle = root / "guide_bundle"
    endpoint_path, endpoint_sha, endpoint_bytes, manifest_sha = _write_bundle(
        bundle, guide_images
    )
    frames = [endpoint.copy() for _ in range(FRAME_COUNT)]
    for index in (6, 18, 30, 42):
        frames[index] = guide_images[index].copy()
    candidate = root / "candidate_frames"
    candidate_sha, candidate_bytes = _write_candidate(candidate, frames)
    return {
        "root": root,
        "endpoint": endpoint,
        "guide_images": guide_images,
        "bundle": bundle,
        "endpoint_path": endpoint_path,
        "endpoint_sha": endpoint_sha,
        "endpoint_bytes": endpoint_bytes,
        "manifest_sha": manifest_sha,
        "candidate": candidate,
        "candidate_sha": candidate_sha,
        "candidate_bytes": candidate_bytes,
        "frames": frames,
    }


@pytest.fixture(autouse=True)
def authorize_synthetic_test_contract(
    contract: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        object_region_gate,
        "AUTHORITATIVE_GUIDE_MANIFEST_ENDPOINT_PINS",
        object_region_gate.AUTHORITATIVE_GUIDE_MANIFEST_ENDPOINT_PINS
        | frozenset({(contract["manifest_sha"], contract["endpoint_sha"])}),
    )


def _run(contract: dict, output: Path, **overrides) -> dict:
    arguments = {
        "candidate": contract["candidate"],
        "candidate_sha256": contract["candidate_sha"],
        "candidate_bytes": contract["candidate_bytes"],
        "endpoint_guide": contract["endpoint_path"],
        "endpoint_guide_sha256": contract["endpoint_sha"],
        "endpoint_guide_bytes": contract["endpoint_bytes"],
        "guide_bundle": contract["bundle"],
        "guide_manifest_sha256": contract["manifest_sha"],
        "output_dir": output,
    }
    arguments.update(overrides)
    return run_object_region_video_gate(**arguments)


def test_pinned_frame_directory_passes_all_object_region_gates(
    contract: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = contract["root"] / "pass-output"
    original_read_bytes = Path.read_bytes
    reads: dict[Path, int] = {}

    def counted(path: Path) -> bytes:
        resolved = path.resolve()
        reads[resolved] = reads.get(resolved, 0) + 1
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", counted)
    report = _run(contract, output)

    assert report["schema"] == SCHEMA
    assert report["verdict"] == "PASS"
    assert report["approved_for_fitting"] is True
    assert report["summary"] == {
        "recoveries_passed": 3,
        "recoveries_total": 3,
        "loop_endpoint_pass": True,
        "swings_passed": 4,
        "swings_total": 4,
    }
    expected_reads = {
        *(path.resolve() for path in contract["candidate"].glob("frame_*.png")),
        *(path.resolve() for path in contract["bundle"].glob("*.png")),
        (contract["bundle"] / "immutable_manifest.json").resolve(),
        contract["endpoint_path"].resolve(),
    }
    assert set(reads) == expected_reads
    assert all(count == 1 for count in reads.values())
    assert (output / "object_region_video_gate.json").is_file()
    evidence = cv2.imread(str(output / "object_region_video_gate.png"))
    assert evidence is not None and evidence.shape == (448, 1920, 3)


def test_recovery_endpoint_and_weak_swing_fail_closed(contract: dict) -> None:
    frames = [frame.copy() for frame in contract["frames"]]
    frames[12] = contract["guide_images"][6].copy()
    frames[30] = contract["endpoint"].copy()
    shifted = np.full_like(contract["endpoint"], (216, 210, 205))
    shifted[:, 6:] = contract["endpoint"][:, :-6]
    frames[48] = shifted
    candidate = contract["root"] / "failing-candidate"
    candidate_sha, candidate_bytes = _write_candidate(candidate, frames)
    output = contract["root"] / "failing-output"

    report = _run(
        contract,
        output,
        candidate=candidate,
        candidate_sha256=candidate_sha,
        candidate_bytes=candidate_bytes,
    )

    assert report["verdict"] == "FAIL"
    assert report["approved_for_fitting"] is False
    assert report["recovery_results"]["12"]["pass"] is False
    assert report["loop_endpoint_result"]["pass"] is False
    assert report["swing_results"]["30"]["pass"] is False


def test_all_frame_missing_hoof_cannot_self_normalize_baseline(contract: dict) -> None:
    frames = [_without_hoof(frame) for frame in contract["frames"]]
    candidate = contract["root"] / "all-frame-missing-hoof"
    candidate_sha, candidate_bytes = _write_candidate(candidate, frames)

    report = _run(
        contract,
        contract["root"] / "all-frame-missing-hoof-output",
        candidate=candidate,
        candidate_sha256=candidate_sha,
        candidate_bytes=candidate_bytes,
    )

    assert report["verdict"] == "FAIL"
    assert report["baseline_endpoint_result"]["pass"] is False
    distal = report["frames"]["0"]["distal_phase_results"]["6"]
    assert distal["checks"]["silhouette_recall"] is False
    assert distal["checks"]["boundary_p95"] is False
    assert distal["checks"]["object_psnr"] is False


def test_single_recovery_missing_hoof_fails_distal_phase_gate(contract: dict) -> None:
    frames = [frame.copy() for frame in contract["frames"]]
    frames[12] = _without_hoof(frames[12])
    candidate = contract["root"] / "single-recovery-missing-hoof"
    candidate_sha, candidate_bytes = _write_candidate(candidate, frames)

    report = _run(
        contract,
        contract["root"] / "single-recovery-missing-hoof-output",
        candidate=candidate,
        candidate_sha256=candidate_sha,
        candidate_bytes=candidate_bytes,
    )

    assert report["verdict"] == "FAIL"
    assert report["recovery_results"]["12"]["pass"] is False
    distal = report["recovery_results"]["12"]["distal_phase_results"]["6"]
    assert distal["checks"]["silhouette_recall"] is False
    assert distal["checks"]["boundary_p95"] is False
    assert distal["checks"]["object_psnr"] is False


def test_reversed_swing_direction_fails_direct_pinned_guide_agreement(
    contract: dict,
) -> None:
    frames = [frame.copy() for frame in contract["frames"]]
    frames[30] = _swing(contract["endpoint"], 430, -1)
    candidate = contract["root"] / "reversed-swing-direction"
    candidate_sha, candidate_bytes = _write_candidate(candidate, frames)

    report = _run(
        contract,
        contract["root"] / "reversed-swing-direction-output",
        candidate=candidate,
        candidate_sha256=candidate_sha,
        candidate_bytes=candidate_bytes,
    )

    result = report["swing_results"]["30"]
    assert report["verdict"] == "FAIL"
    assert result["checks"]["mae_ratio"] is True
    assert result["checks"]["changed_fraction_ratio"] is True
    assert result["checks"]["signed_motion_correlation"] is False
    assert result["checks"]["direct_silhouette_iou"] is False
    assert result["pass"] is False


def test_missing_optional_guide_bundle_emits_fail_not_partial_approval(contract: dict) -> None:
    output = contract["root"] / "no-guide-output"
    report = _run(
        contract,
        output,
        guide_bundle=None,
        guide_manifest_sha256=None,
    )
    assert report["verdict"] == "FAIL"
    assert report["swing_status"] == "not_evaluated_missing_pinned_guide_bundle"
    assert report["summary"]["swings_total"] == 4
    assert report["summary"]["swings_passed"] == 0


def test_pin_mismatch_and_existing_output_do_not_mutate(contract: dict) -> None:
    bad_output = contract["root"] / "bad-pin-output"
    with pytest.raises(ContractError, match="frame-set SHA-256 mismatch"):
        _run(contract, bad_output, candidate_sha256="0" * 64)
    assert not bad_output.exists()

    existing = contract["root"] / "existing-output"
    existing.mkdir()
    sentinel = existing / "sentinel.txt"
    sentinel.write_text("preserve", encoding="utf-8")
    with pytest.raises(ContractError, match="new non-existing directory"):
        _run(contract, existing)
    assert sentinel.read_text(encoding="utf-8") == "preserve"


def test_guide_bundle_rejects_path_escape(
    contract: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    evil = contract["root"] / "evil-bundle"
    shutil.copytree(contract["bundle"], evil)
    manifest_path = evil / "immutable_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["frames_array"][1]["filename_string"] = "../guide_006.png"
    payload = (json.dumps(manifest, indent=2) + "\n").encode()
    manifest_path.write_bytes(payload)
    monkeypatch.setattr(
        object_region_gate,
        "AUTHORITATIVE_GUIDE_MANIFEST_ENDPOINT_PINS",
        object_region_gate.AUTHORITATIVE_GUIDE_MANIFEST_ENDPOINT_PINS
        | frozenset({(_sha(payload), contract["endpoint_sha"])}),
    )
    output = contract["root"] / "evil-output"
    with pytest.raises(ContractError, match="escapes the guide bundle"):
        _run(
            contract,
            output,
            guide_bundle=evil,
            guide_manifest_sha256=_sha(payload),
        )
    assert not output.exists()


def test_guide_manifest_and_file_content_tamper_fail_before_output(contract: dict) -> None:
    manifest_tamper = contract["root"] / "manifest-tamper-bundle"
    shutil.copytree(contract["bundle"], manifest_tamper)
    manifest_path = manifest_tamper / "immutable_manifest.json"
    manifest_path.write_bytes(manifest_path.read_bytes() + b" ")
    manifest_output = contract["root"] / "manifest-tamper-output"
    with pytest.raises(ContractError, match="guide manifest SHA-256 mismatch"):
        _run(
            contract,
            manifest_output,
            guide_bundle=manifest_tamper,
        )
    assert not manifest_output.exists()

    file_tamper = contract["root"] / "file-tamper-bundle"
    shutil.copytree(contract["bundle"], file_tamper)
    guide_path = file_tamper / "guide_030.png"
    guide_path.write_bytes(guide_path.read_bytes() + b"tamper")
    file_output = contract["root"] / "file-tamper-output"
    with pytest.raises(ContractError, match="guide frame 30 byte-size mismatch"):
        _run(
            contract,
            file_output,
            guide_bundle=file_tamper,
        )
    assert not file_output.exists()


def test_code_owned_manifest_endpoint_allowlist_rejects_self_consistent_repin(
    contract: dict,
) -> None:
    authoritative_pair = (
        "7484b6fe3d7e190c118b01d5baec22e4a1021647eb4145c9c74ab0daeac29451",
        "d0714166ac91d38a6cfe0f0d2ee18bc18f221fc2ca6782d99a8a0cbb215576b3",
    )
    assert (
        authoritative_pair
        in object_region_gate.AUTHORITATIVE_GUIDE_MANIFEST_ENDPOINT_PINS
    )
    assert (
        (
            "a09418a8725984126071614b8921eeffaee7cd9a91ca9d4c4ae34b49d1f3a6cb",
            "d0714166ac91d38a6cfe0f0d2ee18bc18f221fc2ca6782d99a8a0cbb215576b3",
        )
        in object_region_gate.AUTHORITATIVE_GUIDE_MANIFEST_ENDPOINT_PINS
    )

    repinned = contract["root"] / "self-consistent-repin-bundle"
    shutil.copytree(contract["bundle"], repinned)
    manifest_path = repinned / "immutable_manifest.json"
    repinned_manifest = manifest_path.read_bytes() + b" "
    manifest_path.write_bytes(repinned_manifest)
    output = contract["root"] / "self-consistent-repin-output"

    with pytest.raises(ContractError, match="not a code-authorized immutable pair"):
        _run(
            contract,
            output,
            guide_bundle=repinned,
            guide_manifest_sha256=_sha(repinned_manifest),
        )
    assert not output.exists()


def test_frame_count_and_all_or_none_guide_contracts(contract: dict) -> None:
    incomplete = contract["root"] / "incomplete-candidate"
    shutil.copytree(contract["candidate"], incomplete)
    (incomplete / "frame_000048.png").unlink()
    output = contract["root"] / "incomplete-output"
    with pytest.raises(ContractError, match="exactly frame_000000.png"):
        _run(contract, output, candidate=incomplete)
    assert not output.exists()

    with pytest.raises(ContractError, match="provided together"):
        _run(
            contract,
            contract["root"] / "half-guide-output",
            guide_manifest_sha256=None,
        )


def test_cli_exit_codes_for_pass_and_fail(contract: dict, capsys: pytest.CaptureFixture) -> None:
    pass_output = contract["root"] / "cli-pass-output"
    arguments = [
        "--candidate",
        str(contract["candidate"]),
        "--candidate-sha256",
        contract["candidate_sha"],
        "--candidate-bytes",
        str(contract["candidate_bytes"]),
        "--endpoint-guide",
        str(contract["endpoint_path"]),
        "--endpoint-guide-sha256",
        contract["endpoint_sha"],
        "--endpoint-guide-bytes",
        str(contract["endpoint_bytes"]),
        "--guide-bundle",
        str(contract["bundle"]),
        "--guide-manifest-sha256",
        contract["manifest_sha"],
        "--output-dir",
        str(pass_output),
    ]
    assert main(arguments) == 0
    assert '"verdict": "PASS"' in capsys.readouterr().out

    fail_output = contract["root"] / "cli-fail-output"
    without_guide = arguments[: arguments.index("--guide-bundle")] + [
        "--output-dir",
        str(fail_output),
    ]
    assert main(without_guide) == 2
    assert '"verdict": "FAIL"' in capsys.readouterr().out

    contract_output = contract["root"] / "cli-contract-error-output"
    bad_pin = arguments.copy()
    bad_pin[bad_pin.index("--candidate-sha256") + 1] = "0" * 64
    bad_pin[bad_pin.index("--output-dir") + 1] = str(contract_output)
    assert main(bad_pin) == 1
    captured = capsys.readouterr()
    assert "OBJECT_REGION_GATE_CONTRACT_ERROR" in captured.err
    assert not contract_output.exists()


def test_exact_pinned_mp4_is_decoded_without_reopening_source(
    contract: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate = contract["root"] / "candidate.mp4"
    writer = cv2.VideoWriter(
        str(candidate),
        cv2.VideoWriter_fourcc(*"mp4v"),
        30.0,
        (WIDTH, HEIGHT),
    )
    if not writer.isOpened():
        pytest.skip("OpenCV MP4 writer is unavailable")
    try:
        for frame in contract["frames"]:
            writer.write(frame)
    finally:
        writer.release()
    payload = candidate.read_bytes()
    output = contract["root"] / "mp4-output"
    original_read_bytes = Path.read_bytes
    reads: dict[Path, int] = {}

    def counted(path: Path) -> bytes:
        resolved = path.resolve()
        reads[resolved] = reads.get(resolved, 0) + 1
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", counted)

    report = _run(
        contract,
        output,
        candidate=candidate,
        candidate_sha256=_sha(payload),
        candidate_bytes=len(payload),
    )

    assert report["inputs"]["candidate"]["kind"] == "mp4"
    assert report["inputs"]["candidate"]["sha256"] == _sha(payload)
    assert reads[candidate.resolve()] == 1
    assert (output / "object_region_video_gate.json").is_file()


def test_h264_savevideo_representative_exact_guide_sequence_passes(contract: dict) -> None:
    candidate = contract["root"] / "candidate-h264-savevideo-golden.mp4"
    payload = _write_h264_candidate(candidate, contract["frames"])
    output = contract["root"] / "candidate-h264-savevideo-golden-output"

    report = _run(
        contract,
        output,
        candidate=candidate,
        candidate_sha256=_sha(payload),
        candidate_bytes=len(payload),
    )

    assert report["verdict"] == "PASS"
    assert report["approved_for_fitting"] is True
    assert report["summary"] == {
        "recoveries_passed": 3,
        "recoveries_total": 3,
        "loop_endpoint_pass": True,
        "swings_passed": 4,
        "swings_total": 4,
    }
    assert report["frames"]["0"]["background"][
        "codec_tolerant_reference_segmentation"
    ] is True
    assert all(
        result["checks"]["signed_motion_correlation"]
        for result in report["swing_results"].values()
    )


def test_real_authoritative_v12_f2_h264_exact_guide_sequence_passes(
    tmp_path: Path,
) -> None:
    bundle = Path(
        r"R:\ComfyUI-data\autorig-fitting\canonical-candidates\experiments"
        r"\horse-walk-v12-browser-recovery-guides-f2"
    )
    manifest_path = bundle / "immutable_manifest.json"
    if not manifest_path.is_file():
        pytest.skip("external authoritative v12 f2 guide bundle is unavailable")
    manifest_payload = manifest_path.read_bytes()
    manifest_sha = _sha(manifest_payload)
    manifest = json.loads(manifest_payload)
    by_index = {
        entry["frame_index_int"]: cv2.imread(str(bundle / entry["filename_string"]))
        for entry in manifest["frames_array"]
    }
    assert manifest_sha == (
        "7484b6fe3d7e190c118b01d5baec22e4a1021647eb4145c9c74ab0daeac29451"
    )
    assert all(image is not None for image in by_index.values())
    frames = [by_index.get(index, by_index[0]) for index in range(FRAME_COUNT)]
    candidate = tmp_path / "authoritative-v12-f2-h264-golden.mp4"
    candidate_payload = _write_h264_candidate(candidate, frames)
    endpoint_path = bundle / "guide_000.png"
    endpoint_payload = endpoint_path.read_bytes()

    report = run_object_region_video_gate(
        candidate=candidate,
        candidate_sha256=_sha(candidate_payload),
        candidate_bytes=len(candidate_payload),
        endpoint_guide=endpoint_path,
        endpoint_guide_sha256=_sha(endpoint_payload),
        endpoint_guide_bytes=len(endpoint_payload),
        guide_bundle=bundle,
        guide_manifest_sha256=manifest_sha,
        output_dir=tmp_path / "authoritative-v12-f2-h264-golden-output",
    )

    assert report["verdict"] == "PASS"
    assert report["summary"] == {
        "recoveries_passed": 3,
        "recoveries_total": 3,
        "loop_endpoint_pass": True,
        "swings_passed": 4,
        "swings_total": 4,
    }


def test_v14_interval_bundle_loads_all_49_pins_and_passes_exact_sequence(
    contract: dict, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = tmp_path / "v14-interval-guide-bundle"
    endpoint_path, endpoint_sha, endpoint_bytes, manifest_sha = _write_interval_bundle(
        bundle, contract["guide_images"]
    )
    monkeypatch.setattr(
        object_region_gate,
        "AUTHORITATIVE_GUIDE_MANIFEST_ENDPOINT_PINS",
        object_region_gate.AUTHORITATIVE_GUIDE_MANIFEST_ENDPOINT_PINS
        | frozenset({(manifest_sha, endpoint_sha)}),
    )
    frames = [
        contract["guide_images"].get(index, contract["endpoint"]).copy()
        for index in range(FRAME_COUNT)
    ]
    candidate = tmp_path / "v14-interval-candidate.mp4"
    candidate_payload = _write_h264_candidate(candidate, frames)

    report = run_object_region_video_gate(
        candidate=candidate,
        candidate_sha256=_sha(candidate_payload),
        candidate_bytes=len(candidate_payload),
        endpoint_guide=endpoint_path,
        endpoint_guide_sha256=endpoint_sha,
        endpoint_guide_bytes=endpoint_bytes,
        guide_bundle=bundle,
        guide_manifest_sha256=manifest_sha,
        output_dir=tmp_path / "v14-interval-output",
    )

    assert report["verdict"] == "PASS"
    assert report["inputs"]["guide_bundle"]["schema"] == (
        "autorig-browser-ltx-interval-guide-bundle.v1"
    )
    assert len(report["inputs"]["guide_bundle"]["files"]) == 51
    assert report["contract"]["threshold_profile"] == "v14_interval_h264_golden_v1"


def test_v14_lossless_frame_directory_keeps_strict_threshold_profile(
    contract: dict, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = tmp_path / "v14-lossless-guide-bundle"
    endpoint_path, endpoint_sha, endpoint_bytes, manifest_sha = _write_interval_bundle(
        bundle, contract["guide_images"]
    )
    monkeypatch.setattr(
        object_region_gate,
        "AUTHORITATIVE_GUIDE_MANIFEST_ENDPOINT_PINS",
        object_region_gate.AUTHORITATIVE_GUIDE_MANIFEST_ENDPOINT_PINS
        | frozenset({(manifest_sha, endpoint_sha)}),
    )
    frames = [
        contract["guide_images"].get(index, contract["endpoint"]).copy()
        for index in range(FRAME_COUNT)
    ]
    candidate = tmp_path / "v14-lossless-candidate"
    candidate_sha, candidate_bytes = _write_candidate(candidate, frames)

    report = run_object_region_video_gate(
        candidate=candidate,
        candidate_sha256=candidate_sha,
        candidate_bytes=candidate_bytes,
        endpoint_guide=endpoint_path,
        endpoint_guide_sha256=endpoint_sha,
        endpoint_guide_bytes=endpoint_bytes,
        guide_bundle=bundle,
        guide_manifest_sha256=manifest_sha,
        output_dir=tmp_path / "v14-lossless-output",
    )

    assert report["verdict"] == "PASS"
    assert report["inputs"]["candidate"]["kind"] == "immutable_frame_directory"
    assert report["contract"]["threshold_profile"] == "lossless_frame_directory_v1"
    assert report["contract"]["loop_endpoint_thresholds"] == (
        object_region_gate.ENDPOINT_THRESHOLDS
    )
    assert report["contract"]["distal_phase_thresholds"] == (
        object_region_gate.DISTAL_PHASE_THRESHOLDS
    )


def test_v14_interval_non_key_frame_tamper_fails_before_output(
    contract: dict, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = tmp_path / "v14-interval-tamper-bundle"
    endpoint_path, endpoint_sha, endpoint_bytes, manifest_sha = _write_interval_bundle(
        bundle, contract["guide_images"]
    )
    monkeypatch.setattr(
        object_region_gate,
        "AUTHORITATIVE_GUIDE_MANIFEST_ENDPOINT_PINS",
        object_region_gate.AUTHORITATIVE_GUIDE_MANIFEST_ENDPOINT_PINS
        | frozenset({(manifest_sha, endpoint_sha)}),
    )
    (bundle / "guide_001.png").write_bytes(b"tampered-non-key-frame")
    output = tmp_path / "v14-interval-tamper-output"

    with pytest.raises(ContractError, match="guide frame 1 .* mismatch"):
        run_object_region_video_gate(
            candidate=contract["candidate"],
            candidate_sha256=contract["candidate_sha"],
            candidate_bytes=contract["candidate_bytes"],
            endpoint_guide=endpoint_path,
            endpoint_guide_sha256=endpoint_sha,
            endpoint_guide_bytes=endpoint_bytes,
            guide_bundle=bundle,
            guide_manifest_sha256=manifest_sha,
            output_dir=output,
        )
    assert not output.exists()


def test_real_authoritative_v14_f1_h264_exact_interval_sequence_passes(
    tmp_path: Path,
) -> None:
    bundle = Path(
        r"R:\ComfyUI-data\autorig-fitting\canonical-candidates\experiments"
        r"\horse-walk-v14-browser-interval-guide-f1"
    )
    manifest_path = bundle / "immutable_manifest.json"
    if not manifest_path.is_file():
        pytest.skip("external authoritative v14 f1 guide bundle is unavailable")
    manifest_payload = manifest_path.read_bytes()
    manifest_sha = _sha(manifest_payload)
    manifest = json.loads(manifest_payload)
    by_index = {
        entry["frame_index_int"]: cv2.imread(str(bundle / entry["filename_string"]))
        for entry in manifest["frames_array"]
    }
    assert manifest_sha == (
        "a09418a8725984126071614b8921eeffaee7cd9a91ca9d4c4ae34b49d1f3a6cb"
    )
    assert tuple(sorted(by_index)) == tuple(range(FRAME_COUNT))
    assert all(image is not None for image in by_index.values())
    frames = [by_index[index] for index in range(FRAME_COUNT)]
    candidate = tmp_path / "authoritative-v14-f1-h264-golden.mp4"
    candidate_payload = _write_h264_candidate(candidate, frames)
    endpoint_path = bundle / "guide_000.png"
    endpoint_payload = endpoint_path.read_bytes()

    report = run_object_region_video_gate(
        candidate=candidate,
        candidate_sha256=_sha(candidate_payload),
        candidate_bytes=len(candidate_payload),
        endpoint_guide=endpoint_path,
        endpoint_guide_sha256=_sha(endpoint_payload),
        endpoint_guide_bytes=len(endpoint_payload),
        guide_bundle=bundle,
        guide_manifest_sha256=manifest_sha,
        output_dir=tmp_path / "authoritative-v14-f1-h264-golden-output",
    )

    assert report["verdict"] == "PASS"
    assert report["inputs"]["guide_bundle"]["schema"] == (
        "autorig-browser-ltx-interval-guide-bundle.v1"
    )


def test_pinned_mp4_wrong_count_and_resolution_fail_before_output(contract: dict) -> None:
    short_candidate = contract["root"] / "candidate-48.mp4"
    short_writer = cv2.VideoWriter(
        str(short_candidate),
        cv2.VideoWriter_fourcc(*"mp4v"),
        30.0,
        (WIDTH, HEIGHT),
    )
    if not short_writer.isOpened():
        pytest.skip("OpenCV MP4 writer is unavailable")
    try:
        for frame in contract["frames"][:-1]:
            short_writer.write(frame)
    finally:
        short_writer.release()
    short_payload = short_candidate.read_bytes()
    short_output = contract["root"] / "candidate-48-output"
    with pytest.raises(ContractError, match="exactly 49 frames; got 48"):
        _run(
            contract,
            short_output,
            candidate=short_candidate,
            candidate_sha256=_sha(short_payload),
            candidate_bytes=len(short_payload),
        )
    assert not short_output.exists()

    wrong_size = (640, 360)
    small_candidate = contract["root"] / "candidate-small.mp4"
    small_writer = cv2.VideoWriter(
        str(small_candidate),
        cv2.VideoWriter_fourcc(*"mp4v"),
        30.0,
        wrong_size,
    )
    if not small_writer.isOpened():
        pytest.skip("OpenCV MP4 writer is unavailable")
    try:
        for frame in contract["frames"]:
            small_writer.write(cv2.resize(frame, wrong_size, interpolation=cv2.INTER_AREA))
    finally:
        small_writer.release()
    small_payload = small_candidate.read_bytes()
    small_output = contract["root"] / "candidate-small-output"
    with pytest.raises(ContractError, match="frames must be exactly 768x448"):
        _run(
            contract,
            small_output,
            candidate=small_candidate,
            candidate_sha256=_sha(small_payload),
            candidate_bytes=len(small_payload),
        )
    assert not small_output.exists()
