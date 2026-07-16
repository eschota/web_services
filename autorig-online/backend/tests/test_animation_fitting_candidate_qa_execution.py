from __future__ import annotations

from dataclasses import replace
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import shutil
import threading
from types import MappingProxyType
from typing import Any

import pytest

import animation_fitting_candidate_qa_execution as execution
from animal_animation_library import AnimationLibraryError
from animation_fitting_browser_qa_runner import (
    BrowserHorseQaResult,
    BrowserHorseQaRunnerConfig,
    REQUEST_RECEIPT_SCHEMA,
    RUN_RECEIPT_SCHEMA,
    V14_HORSE_QA_SCOPE,
    canonical_json_bytes,
)
from animation_fitting_candidate_ingest import BUNDLE_SCHEMA
from animation_fitting_candidate_review import UPLOAD_ARTIFACT_NAMES
from animation_fitting_candidate_selection import CANDIDATE_ADMISSION_SCHEMA


JOB_ID = "11111111-1111-4111-8111-111111111111"
SOURCE_MODEL_SHA = hashlib.sha256(b"source-model").hexdigest()
SKELETON_SHA = hashlib.sha256(b"skeleton").hexdigest()


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _canonical(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        + b"\n"
    )


def _pin(payload: bytes, filename: str) -> dict[str, Any]:
    return {"filename": filename, "bytes": len(payload), "sha256": _sha(payload)}


def _make_admitted_bundle(root: Path, *, candidate_index: int = 0) -> dict[str, Any]:
    files: dict[str, bytes] = {}
    for name in UPLOAD_ARTIFACT_NAMES:
        if name == "three-clip.json":
            payload = b'{"name":"Horse_Walk_BrowserFit"}\n'
        elif name.endswith(".json"):
            payload = b"{}\n"
        elif name.endswith(".png"):
            payload = b"\x89PNG\r\n\x1a\nfixture-" + name.encode()
        elif name.endswith(".mp4"):
            payload = b"\x00\x00\x00\x18ftypisomfixture-" + name.encode()
        else:
            payload = f"fixture-{name}".encode()
        files[name] = payload
    artifacts = {name: _pin(payload, name) for name, payload in files.items()}
    manifest_binding = {
        "schema": BUNDLE_SCHEMA,
        "library": {
            "version_id": 1,
            "revision": "horse-v1",
            "rig_type": "horse",
            "template_skeleton_sha256": SKELETON_SHA,
        },
        "fitting_job": {
            "id": JOB_ID,
            "semantic_id": "walk_forward",
            "workflow_name": "ltx-horse-v14",
            "workflow_fingerprint": "f" * 64,
        },
        "source_task": {
            "id": "22222222-2222-4222-8222-222222222222",
            "guid": "33333333-3333-4333-8333-333333333333",
        },
        "candidate": {
            "candidate_index": candidate_index,
            "seed": 1234 + candidate_index,
            "source_rig_type": "HORSE_2",
            "source_model_sha256": SOURCE_MODEL_SHA,
            "source_skeleton_sha256": SKELETON_SHA,
            "frame_count": 49,
            "fps": 30.0,
            "duration_seconds": 1.6,
            "review_state": "uploaded_pending_server_validation",
            "uploaded_qa_assertions_trusted": False,
            "server_validation": {
                "status": "pending",
                "required": [
                    "task_model_sha256_binding",
                    "task_skeleton_sha256_binding",
                    "media_decode_and_phase_extraction",
                    "deformation_recompute",
                    "visual_review",
                ],
            },
        },
        "controlled_generation": {"job_id": "a" * 64},
        "artifacts": artifacts,
    }
    identity = _sha(_canonical(manifest_binding)[:-1])
    manifest = {**manifest_binding, "identity_sha256": identity}
    manifest_bytes = _canonical(manifest)
    bundle = root / JOB_ID / "browser-candidates" / identity[:2] / identity
    bundle.mkdir(parents=True)
    for name, payload in files.items():
        (bundle / name).write_bytes(payload)
    (bundle / "candidate-manifest.json").write_bytes(manifest_bytes)

    admission_binding = {
        "schema": CANDIDATE_ADMISSION_SCHEMA,
        "job_id": JOB_ID,
        "candidate_index": candidate_index,
        "seed": 1234 + candidate_index,
        "candidate_identity_sha256": identity,
        "candidate_manifest": _pin(manifest_bytes, "candidate-manifest.json"),
        "lifecycle_identity_sha256": "b" * 64,
        "human_review_lifecycle_binding_sha256": "c" * 64,
    }
    admission_identity = _sha(_canonical(admission_binding)[:-1])
    admission = {**admission_binding, "identity_sha256": admission_identity}
    admission_path = (
        root
        / JOB_ID
        / "browser-candidate-selection"
        / "admissions"
        / f"{candidate_index:02d}"
        / "admission.json"
    )
    admission_path.parent.mkdir(parents=True)
    admission_path.write_bytes(_canonical(admission))
    return {
        "identity": identity,
        "bundle": bundle,
        "manifest": manifest,
        "admission_path": admission_path,
    }


def _config(tmp_path: Path) -> BrowserHorseQaRunnerConfig:
    placeholder = tmp_path / "placeholder"
    placeholder.mkdir(exist_ok=True)
    executable = placeholder / "node.exe"
    runner = placeholder / "runner.js"
    profile = placeholder / "qa.json"
    three = placeholder / "three.module.js"
    chrome = placeholder / "chrome.exe"
    ffmpeg = placeholder / "ffmpeg.exe"
    ffprobe = placeholder / "ffprobe.exe"
    bundle = placeholder / "bundle"
    bundle.mkdir(exist_ok=True)
    for path in (executable, runner, profile, three, chrome, ffmpeg, ffprobe):
        path.write_bytes(path.name.encode())
    return BrowserHorseQaRunnerConfig(
        input_root=placeholder,
        output_root=placeholder,
        bundle_directory=bundle,
        runner_executable=executable,
        expected_runner_executable_sha256="1" * 64,
        runner_script=runner,
        expected_runner_script_sha256="2" * 64,
        qa_profile_path=profile,
        expected_qa_profile_sha256="3" * 64,
        three_module=three,
        expected_three_module_sha256="4" * 64,
        chrome_executable=chrome,
        expected_chrome_executable_sha256="5" * 64,
        ffmpeg_executable=ffmpeg,
        expected_ffmpeg_executable_sha256="6" * 64,
        ffprobe_executable=ffprobe,
        expected_ffprobe_executable_sha256="7" * 64,
        expected_immutable_manifest_sha256="8" * 64,
        expected_fitting_bundle_sha256="9" * 64,
        expected_source_model_sha256=SOURCE_MODEL_SHA,
    )


def _fake_browser_runner(*, machine_passed: bool = True, wrong_candidate: bool = False):
    calls: list[Any] = []

    def run(config, request):
        calls.append(request)
        attempt = (
            config.output_root
            / "jobs"
            / request.job_id
            / "candidates"
            / request.candidate_id
            / request.attempt_id
        )
        qa_output = attempt / "qa-output"
        frames = qa_output / "frames"
        frames.mkdir(parents=True)
        frame_pins: dict[str, dict[str, Any]] = {}
        for index in range(49):
            relative = f"frames/frame_{index:04d}.png"
            payload = b"\x89PNG\r\n\x1a\n" + index.to_bytes(2, "big")
            (qa_output / relative).write_bytes(payload)
            frame_pins[relative] = {"bytes": len(payload), "sha256": _sha(payload)}
        camera = {"schema": "autorig.browser-horse-fixed-camera.v1"}
        camera_bytes = canonical_json_bytes(camera)
        (qa_output / "camera-settings.json").write_bytes(camera_bytes)
        deformation = {"schema": "autorig.browser-horse-target-deformation-qa.v1"}
        deformation_bytes = canonical_json_bytes(deformation)
        (qa_output / "deformation-report.json").write_bytes(deformation_bytes)
        video = b"\x00\x00\x00\x18ftypisom-browser-qa"
        (qa_output / "fixed-camera-preview.mp4").write_bytes(video)
        phase_rows = []
        for phase, index in zip(("start", "middle", "three_quarter"), (0, 24, 36)):
            phase_rows.append(
                {
                    "phase": phase,
                    "frame_index": index,
                    "evidence_url": None,
                    "sha256": frame_pins[f"frames/frame_{index:04d}.png"]["sha256"],
                }
            )
        evidence = {
            "schema": "autorig.browser-horse-visual-phase-evidence-envelope.v1",
            "visual_phase_gate": {
                "schema": "autorig.animation-visual-phase-qa.v1",
                "version": 1,
                "rig_type": "horse",
                "semantic_id": request.semantic_id,
                "fitted_clip_sha256": request.expected_three_clip_sha256,
                "decision": None,
                "camera": {
                    "static": True,
                    "projection": "perspective",
                    "view": "canonical_fitting_bundle",
                    "root_motion_locked": True,
                    "settings_sha256": _sha(camera_bytes),
                },
                "coincident_rest_vertex_separation": {
                    "measured": True,
                    "pass": machine_passed,
                    "threshold_m": 0.04,
                    "max_separation_m": 0.01 if machine_passed else 0.08,
                    "sample_count": 49,
                    "group_count": 1,
                    "report_url": None,
                    "report_sha256": _sha(deformation_bytes),
                },
                "required_phases": ["start", "middle", "three_quarter"],
                "frames": phase_rows,
                "reviewer": {"id": None, "reviewed_at": None},
            },
        }
        evidence_bytes = canonical_json_bytes(evidence)
        (qa_output / "visual-phase-qa.json").write_bytes(evidence_bytes)
        artifact_payloads = {
            "camera-settings.json": camera_bytes,
            "deformation-report.json": deformation_bytes,
            "fixed-camera-preview.mp4": video,
            "visual-phase-qa.json": evidence_bytes,
        }
        artifacts = dict(frame_pins)
        for relative, payload in artifact_payloads.items():
            pin = {"bytes": len(payload), "sha256": _sha(payload)}
            if relative.endswith(".json"):
                pin["canonical_json_sha256"] = _sha(payload)
            artifacts[relative] = pin

        clip = (
            config.input_root
            / "jobs"
            / request.job_id
            / "candidates"
            / request.candidate_id
            / "three-clip.json"
        ).read_bytes()
        run_request = {
            "schema": REQUEST_RECEIPT_SCHEMA,
            "job_id": request.job_id,
            "candidate_id": "0" * 64 if wrong_candidate else request.candidate_id,
            "attempt_id": request.attempt_id,
            "semantic_id": request.semantic_id,
            "execution_contract": {
                "browser_only": True,
                "blender_used": False,
                "adapter_scope": V14_HORSE_QA_SCOPE,
                "production_eligible": False,
            },
            "inputs": {
                "source_model_sha256": config.expected_source_model_sha256,
                "three_clip": {
                    "path": str(config.input_root / "three-clip.json"),
                    "bytes": len(clip),
                    "sha256": _sha(clip),
                },
                "runner_executable": {
                    "sha256": config.expected_runner_executable_sha256
                },
                "runner_script": {"sha256": config.expected_runner_script_sha256},
                "qa_profile": {"sha256": config.expected_qa_profile_sha256},
                "three_module": {"sha256": config.expected_three_module_sha256},
                "chrome_executable": {
                    "sha256": config.expected_chrome_executable_sha256
                },
                "ffmpeg_executable": {
                    "sha256": config.expected_ffmpeg_executable_sha256
                },
                "ffprobe_executable": {
                    "sha256": config.expected_ffprobe_executable_sha256
                },
            },
        }
        request_bytes = canonical_json_bytes(run_request)
        attempt.mkdir(parents=True, exist_ok=True)
        (attempt / "run-request.json").write_bytes(request_bytes)
        status = (
            "PASS_MACHINE_QA_AWAITING_HUMAN" if machine_passed else "FAIL_MACHINE_QA"
        )
        run_receipt = {
            "schema": RUN_RECEIPT_SCHEMA,
            "job_id": request.job_id,
            "candidate_id": request.candidate_id,
            "attempt_id": request.attempt_id,
            "semantic_id": request.semantic_id,
            "status": status,
            "execution": {
                "request_receipt": {
                    "bytes": len(request_bytes),
                    "sha256": _sha(request_bytes),
                }
            },
            "qa_profile": {
                "adapter_scope": V14_HORSE_QA_SCOPE,
                "production_eligible": False,
                "ranking_metrics_emitted": False,
            },
            "gates": {
                "machine_qa_passed": machine_passed,
                "machine": {"cameraStatic": machine_passed},
                "human_visual_decision": None,
                "ready_for_human_review": machine_passed,
                "approved_for_animation_library": False,
                "release_ready": False,
            },
            "artifacts": artifacts,
            "required_phase_artifacts": {
                phase: frame_pins[f"frames/frame_{index:04d}.png"]
                for phase, index in zip(
                    ("start", "middle", "three_quarter"), (0, 24, 36)
                )
            },
        }
        run_bytes = canonical_json_bytes(run_receipt)
        run_path = attempt / "run-receipt.json"
        run_path.write_bytes(run_bytes)
        return BrowserHorseQaResult(
            machine_qa_passed=machine_passed,
            ready_for_human_review=machine_passed,
            human_visual_decision=None,
            approved_for_animation_library=False,
            machine_gates=MappingProxyType({"cameraStatic": machine_passed}),
            attempt_directory=attempt,
            qa_output_directory=qa_output,
            evidence_path=qa_output / "visual-phase-qa.json",
            video_path=qa_output / "fixed-camera-preview.mp4",
            run_receipt_path=run_path,
            run_receipt_sha256=_sha(run_bytes),
            run_receipt_bytes=len(run_bytes),
        )

    return calls, run


@pytest.fixture
def admitted(tmp_path: Path):
    root = tmp_path / "jobs"
    root.mkdir()
    bundle = _make_admitted_bundle(root)
    return root, bundle, _config(tmp_path)


def _execute(root, bundle, config, *, attempt_id="attempt-1"):
    return execution.execute_admitted_candidate_browser_qa(
        job_id=JOB_ID,
        candidate_index=0,
        candidate_identity_sha256=bundle["identity"],
        attempt_id=attempt_id,
        browser_config=config,
        fitting_jobs_root=str(root),
    )


def test_config_fingerprint_covers_full_declared_config_and_validated_runtime(
    tmp_path, monkeypatch
):
    config = _config(tmp_path)
    runtime_state = {
        "pins": {
            "runner_executable": {
                "bytes": 10,
                "sha256": "a" * 64,
                "path": str(config.runner_executable.resolve()),
            },
            "runner_script": {
                "bytes": 11,
                "sha256": "b" * 64,
                "path": str(config.runner_script.resolve()),
            },
            "qa_profile": {
                "bytes": 12,
                "sha256": "c" * 64,
                "path": str(config.qa_profile_path.resolve()),
            },
            "immutable_manifest": {"bytes": 13, "sha256": "d" * 64},
            "fitting_bundle": {"bytes": 14, "sha256": "e" * 64},
            "three_module": {"bytes": 15, "sha256": "f" * 64},
            "chrome": {"bytes": 16, "sha256": "1" * 64},
            "ffmpeg": {"bytes": 17, "sha256": "2" * 64},
            "ffprobe": {"bytes": 18, "sha256": "3" * 64},
        }
    }
    monkeypatch.setattr(execution, "_validate_config", lambda value: runtime_state)
    first, _ = execution._browser_config_fingerprint(config)
    assert set(first["binding"]["declared"]) == set(config.__dataclass_fields__)
    assert first["binding"]["declared"]["timeout_seconds"] == 900.0
    assert first["binding"]["validated_pins"] == runtime_state["pins"]

    runtime_state["pins"]["runner_script"]["sha256"] = "9" * 64
    second, _ = execution._browser_config_fingerprint(config)
    assert second["identity_sha256"] != first["identity_sha256"]


def test_executes_admitted_candidate_and_publishes_rehashable_tree(
    admitted, monkeypatch
):
    root, bundle, config = admitted
    calls, fake = _fake_browser_runner()
    monkeypatch.setattr(execution, "run_browser_horse_qa", fake)
    monkeypatch.setattr(execution, "_validate_config", lambda value: {})

    result = _execute(root, bundle, config)

    assert result.created is True
    assert result.machine_qa_passed is True
    assert result.ready_for_human_review is True
    assert result.receipt["candidate"]["manifest"]["sha256"] == _sha(
        (bundle["bundle"] / "candidate-manifest.json").read_bytes()
    )
    assert result.receipt["execution_contract"] == {
        "browser_only": True,
        "blender_used": False,
        "adapter_scope": V14_HORSE_QA_SCOPE,
        "production_eligible": False,
        "human_review_required": True,
    }
    config_fingerprint = result.receipt["browser_config"]
    assert config_fingerprint["identity_sha256"] == _sha(
        _canonical(config_fingerprint["binding"])[:-1]
    )
    assert config_fingerprint["binding"]["declared"]["timeout_seconds"] == 900.0
    assert (
        config_fingerprint["binding"]["declared"]["expected_runner_script_sha256"]
        == "2" * 64
    )
    assert len(result.receipt["published_files"]) == 55
    assert not list((root / JOB_ID / "browser-candidate-qa-work").glob(".*"))
    trusted = execution.load_trusted_qa_evidence(result, fitting_jobs_root=str(root))
    assert trusted.runner_name == execution.QA_EXECUTION_RUNNER_NAME
    assert trusted.runner_revision == (
        f"{execution.QA_EXECUTION_SCHEMA}:{result.identity_sha256}"
    )
    assert trusted.metrics["visual_phase_gate"]["decision"] is None
    assert set(trusted.artifacts) == {
        "camera-settings.json",
        "deformation-report.json",
        "fixed-camera-preview.mp4",
        "phase-start.png",
        "phase-middle.png",
        "phase-three_quarter.png",
    }

    again = _execute(root, bundle, config)
    assert again.created is False
    assert again.identity_sha256 == result.identity_sha256
    assert len(calls) == 1


def test_existing_attempt_rejects_full_config_fingerprint_drift(admitted, monkeypatch):
    root, bundle, config = admitted
    calls, fake = _fake_browser_runner()
    monkeypatch.setattr(execution, "run_browser_horse_qa", fake)
    monkeypatch.setattr(execution, "_validate_config", lambda value: {})
    first = _execute(root, bundle, config)

    with pytest.raises(
        execution.CandidateQaExecutionError,
        match="attempt identity collision",
    ):
        _execute(root, bundle, replace(config, timeout_seconds=901.0))
    assert (
        first.receipt["browser_config"]["binding"]["declared"]["timeout_seconds"]
        == 900.0
    )
    assert len(calls) == 1


def test_final_appearing_during_browser_run_blocks_publication(admitted, monkeypatch):
    root, bundle, config = admitted
    calls, fake = _fake_browser_runner()

    def run_and_finalize(config_value, request):
        result = fake(config_value, request)
        final = root / JOB_ID / "browser-candidate-selection" / "final"
        final.mkdir(parents=True)
        return result

    monkeypatch.setattr(execution, "run_browser_horse_qa", run_and_finalize)
    monkeypatch.setattr(execution, "_validate_config", lambda value: {})

    with pytest.raises(
        execution.CandidateQaExecutionError,
        match="selection is FINAL",
    ):
        _execute(root, bundle, config)
    assert len(calls) == 1
    assert not execution._execution_target(
        root, JOB_ID, bundle["identity"], "attempt-1"
    ).exists()


def test_staging_is_reverified_inside_publication_lock(admitted, monkeypatch):
    root, bundle, config = admitted
    _, fake = _fake_browser_runner()
    monkeypatch.setattr(execution, "run_browser_horse_qa", fake)
    monkeypatch.setattr(execution, "_validate_config", lambda value: {})
    original_stage = execution._stage_execution_tree

    def stage_and_tamper(**kwargs):
        staging, expected = original_stage(**kwargs)
        frame = staging / "qa-output" / "frames" / "frame_0001.png"
        payload = frame.read_bytes()
        frame.write_bytes(b"X" + payload[1:])
        return staging, expected

    monkeypatch.setattr(execution, "_stage_execution_tree", stage_and_tamper)
    with pytest.raises(AnimationLibraryError, match="pin changed"):
        _execute(root, bundle, config)
    assert not execution._execution_target(
        root, JOB_ID, bundle["identity"], "attempt-1"
    ).exists()


def test_post_publish_loaded_receipt_must_match_precomputed_receipt(
    admitted, monkeypatch
):
    root, bundle, config = admitted
    _, fake = _fake_browser_runner()
    monkeypatch.setattr(execution, "run_browser_horse_qa", fake)
    monkeypatch.setattr(execution, "_validate_config", lambda value: {})
    original_load = execution._load_execution

    def load_with_wrong_identity(target, *, created, root=None):
        loaded = original_load(target, created=created, root=root)
        if created:
            return replace(loaded, identity_sha256="d" * 64)
        return loaded

    monkeypatch.setattr(execution, "_load_execution", load_with_wrong_identity)
    with pytest.raises(
        execution.CandidateQaExecutionError,
        match="differs from its staged receipt",
    ):
        _execute(root, bundle, config)


def test_same_attempt_concurrency_publishes_one_uncorrupted_tree(admitted, monkeypatch):
    root, bundle, config = admitted
    calls, fake = _fake_browser_runner()
    barrier = threading.Barrier(2)

    def synchronized_fake(config_value, request):
        result = fake(config_value, request)
        barrier.wait(timeout=5)
        return result

    monkeypatch.setattr(execution, "run_browser_horse_qa", synchronized_fake)
    monkeypatch.setattr(execution, "_validate_config", lambda value: {})

    def invoke():
        try:
            return _execute(root, bundle, config)
        except execution.CandidateQaExecutionError as exc:
            return exc

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: invoke(), range(2)))
    successes = [
        row
        for row in results
        if isinstance(row, execution.ImmutableCandidateQaExecution)
    ]
    failures = [
        row for row in results if isinstance(row, execution.CandidateQaExecutionError)
    ]
    assert len(successes) == 1
    assert len(failures) == 1
    assert "inventory" in str(failures[0]) or "pin changed" in str(failures[0])
    assert len(calls) == 2
    verified = execution._load_execution(
        successes[0].directory, created=False, root=root
    )
    assert verified.identity_sha256 == successes[0].identity_sha256


def test_trusted_evidence_loader_rejects_copy_outside_server_root(
    admitted, monkeypatch, tmp_path
):
    root, bundle, config = admitted
    _, fake = _fake_browser_runner()
    monkeypatch.setattr(execution, "run_browser_horse_qa", fake)
    monkeypatch.setattr(execution, "_validate_config", lambda value: {})
    result = _execute(root, bundle, config)
    outside = tmp_path / "attacker-controlled-copy"
    shutil.copytree(result.directory, outside)
    forged = replace(
        result,
        directory=outside,
        receipt_path=outside / "execution-receipt.json",
    )

    with pytest.raises(
        execution.CandidateQaExecutionError,
        match="escaped its server-owned root",
    ):
        execution.load_trusted_qa_evidence(forged, fitting_jobs_root=str(root))


def test_trusted_evidence_rehash_rejects_post_load_byte_tamper(admitted, monkeypatch):
    root, bundle, config = admitted
    _, fake = _fake_browser_runner()
    monkeypatch.setattr(execution, "run_browser_horse_qa", fake)
    monkeypatch.setattr(execution, "_validate_config", lambda value: {})
    result = _execute(root, bundle, config)
    original_load = execution._load_execution

    def load_then_tamper(target, *, created, root=None):
        loaded = original_load(target, created=created, root=root)
        video = loaded.directory / "qa-output" / "fixed-camera-preview.mp4"
        payload = video.read_bytes()
        video.write_bytes(b"X" + payload[1:])
        return loaded

    monkeypatch.setattr(execution, "_load_execution", load_then_tamper)
    with pytest.raises(
        execution.CandidateQaExecutionError,
        match="changed after execution receipt verification",
    ):
        execution.load_trusted_qa_evidence(result, fitting_jobs_root=str(root))


def test_rejects_unadmitted_candidate_before_browser_execution(tmp_path, monkeypatch):
    root = tmp_path / "jobs"
    root.mkdir()
    bundle = _make_admitted_bundle(root, candidate_index=1)
    calls, fake = _fake_browser_runner()
    monkeypatch.setattr(execution, "run_browser_horse_qa", fake)
    monkeypatch.setattr(execution, "_validate_config", lambda value: {})

    with pytest.raises(AnimationLibraryError, match="admission|missing"):
        execution.execute_admitted_candidate_browser_qa(
            job_id=JOB_ID,
            candidate_index=0,
            candidate_identity_sha256=bundle["identity"],
            attempt_id="attempt-1",
            browser_config=_config(tmp_path),
            fitting_jobs_root=str(root),
        )
    assert calls == []


def test_rejects_browser_result_bound_to_another_candidate(admitted, monkeypatch):
    root, bundle, config = admitted
    _, fake = _fake_browser_runner(wrong_candidate=True)
    monkeypatch.setattr(execution, "run_browser_horse_qa", fake)
    monkeypatch.setattr(execution, "_validate_config", lambda value: {})

    with pytest.raises(
        execution.CandidateQaExecutionError,
        match="request/result identity",
    ):
        _execute(root, bundle, config)
    assert not execution._execution_target(
        root, JOB_ID, bundle["identity"], "attempt-1"
    ).exists()


def test_rechecks_admission_after_run_before_atomic_publish(admitted, monkeypatch):
    root, bundle, config = admitted
    _, fake = _fake_browser_runner()
    monkeypatch.setattr(execution, "run_browser_horse_qa", fake)
    monkeypatch.setattr(execution, "_validate_config", lambda value: {})
    original_stage = execution._stage_execution_tree

    def stage_and_tamper(**kwargs):
        staged = original_stage(**kwargs)
        bundle["admission_path"].write_bytes(
            bundle["admission_path"].read_bytes() + b" "
        )
        return staged

    monkeypatch.setattr(execution, "_stage_execution_tree", stage_and_tamper)
    with pytest.raises(AnimationLibraryError):
        _execute(root, bundle, config)
    assert not execution._execution_target(
        root, JOB_ID, bundle["identity"], "attempt-1"
    ).exists()


def test_final_selection_blocks_execution_before_browser_run(admitted, monkeypatch):
    root, bundle, config = admitted
    final = root / JOB_ID / "browser-candidate-selection" / "final"
    final.mkdir(parents=True)
    calls, fake = _fake_browser_runner()
    monkeypatch.setattr(execution, "run_browser_horse_qa", fake)
    monkeypatch.setattr(execution, "_validate_config", lambda value: {})

    with pytest.raises(
        execution.CandidateQaExecutionError,
        match="selection is FINAL",
    ):
        _execute(root, bundle, config)
    assert calls == []


def test_machine_failure_is_published_but_cannot_enter_human_review(
    admitted, monkeypatch
):
    root, bundle, config = admitted
    _, fake = _fake_browser_runner(machine_passed=False)
    monkeypatch.setattr(execution, "run_browser_horse_qa", fake)
    monkeypatch.setattr(execution, "_validate_config", lambda value: {})

    result = _execute(root, bundle, config, attempt_id="machine-fail")

    assert result.machine_qa_passed is False
    assert result.receipt["result"]["release_ready"] is False
    with pytest.raises(
        execution.CandidateQaExecutionError,
        match="only an immutable machine PASS",
    ):
        execution.load_trusted_qa_evidence(result, fitting_jobs_root=str(root))
