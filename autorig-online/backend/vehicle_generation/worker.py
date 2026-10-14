"""Single-consumer durable vehicle worker. Run as its own service."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import multiprocessing
import os
import queue
import signal
import shutil
import threading
import time
from pathlib import Path
from typing import Any, Mapping

from .store import VehicleStore


class StageValidationError(RuntimeError):
    pass


class _CapabilityRun:
    def __init__(self, path: Path, run_id: str, base_url: str):
        self.dir, self.id, self.base_url = path, run_id, base_url.rstrip("/")
    def url(self, relative: str) -> str:
        return f"{self.base_url}/{relative.lstrip('/')}"


def _multiview_source(out: Path, document: Mapping[str, Any]) -> Path:
    manifest_path = out / "manifest.json"
    if not manifest_path.is_file(): raise StageValidationError("prepared source manifest is unavailable")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")); prepared = manifest.get("prepared_image_object") or {}
    relative, digest = str(prepared.get("path_string") or ""), str(prepared.get("sha256_string") or "")
    path = (out / relative).resolve()
    try: path.relative_to(out.resolve())
    except ValueError: raise StageValidationError("prepared source escapes the vehicle run") from None
    chain = document.get("source_sha_chain") or {}
    if (not relative or path.is_symlink() or not path.is_file() or len(digest) != 64 or
            digest != chain.get("prepared_sha256") or hashlib.sha256(path.read_bytes()).hexdigest() != digest):
        raise StageValidationError("prepared source hash chain is invalid")
    return path


def _prepare_multiview_workspace(out: Path, document: Mapping[str, Any], source: Path) -> Path:
    workspace = out / "multiview"; workspace.mkdir(parents=True, exist_ok=True); path = workspace / "manifest.json"
    chain = document.get("source_sha_chain") or {}; original, prepared = chain.get("original_sha256"), chain.get("prepared_sha256")
    if hashlib.sha256(source.read_bytes()).hexdigest() != prepared:
        raise StageValidationError("multiview prepared source bytes changed")
    if path.is_file():
        current = json.loads(path.read_text(encoding="utf-8"))
        if (current.get("schema_string") != "autorig.vehicle.multiview/1" or
                current.get("body_cleanup_contract_string") != "body-cleanup-v1" or
                current.get("original_source_sha256_string") != original or
                current.get("prepared_source_sha256_string") != prepared):
            raise StageValidationError("multiview workspace source identity changed")
        return workspace
    options_sha = hashlib.sha256(json.dumps(document.get("options") or {}, sort_keys=True,
                                            separators=(",", ":")).encode()).hexdigest()
    analysis = document.get("analysis") or {}; initial = {"schema_string": "autorig.vehicle.multiview/1",
        "pipeline_schema_string": "autorig.vehicle.multiview/1", "run_id_string": document["id"],
        "originating_vehicle_id_string": document["id"],
        "source_sha256_string": prepared, "original_source_sha256_string": original,
        "prepared_source_sha256_string": prepared, "options_sha256_string": options_sha,
        "body_cleanup_contract_string": "body-cleanup-v1",
        "analysis_object": analysis, "dimensions_m_object": analysis.get("estimated_dimensions_m_object") or {},
        "working_side_string": ({"left": "image_left", "right": "image_right"}.get(
            str((document.get("options") or {}).get("working_side") or "")) or analysis.get("working_side_string")),
        "canonical_working_side_string": ({"left": "negative_x", "right": "positive_x"}.get(
            str((document.get("options") or {}).get("working_side") or "")) or
            analysis.get("canonical_working_side_string")),
        "source_bindings_object": {"original_source_sha256_string": original,
                                   "prepared_source_sha256_string": prepared},
        "views_array": [], "provider_tasks_array": [], "artifacts_array": [], "status_string": "queued"}
    temporary = path.with_suffix(".json.tmp"); temporary.write_text(json.dumps(initial, ensure_ascii=False, indent=2), encoding="utf-8"); os.replace(temporary, path)
    return workspace


def _validated_wheel_doc(manifest: Mapping[str, Any]) -> dict[str, Any] | None:
    rig = manifest.get("validated_wheel_rig_object") or {}
    qa = rig.get("qa_object") or {}
    if rig.get("status_string") != "ready" or qa.get("ready_bool") is not True:
        return None
    if qa.get("supported_drive_profile_bool") is not True:
        return None
    if rig.get("coordinate_space_string") != "gltf_right_handed_y_up_forward_plus_z":
        raise StageValidationError("ready wheel rig has the wrong coordinate space")
    expected_source = str(manifest.get("prepared_source_sha256_string") or manifest.get("source_sha256_string") or "")
    if rig.get("source_sha256_string") != expected_source:
        raise StageValidationError("ready wheel rig source hash mismatch")
    wheels = []
    for row in rig.get("wheels_array") or []:
        part_id = str(row.get("part_id_string") or "")
        center, axis = row.get("center_xyz_array"), row.get("axis_xyz_array")
        radius, width = float(row.get("radius_m_float") or 0), float(row.get("width_m_float") or 0)
        if (not part_id or not isinstance(center, list) or len(center) != 3 or not isinstance(axis, list) or len(axis) != 3
                or radius <= 0 or width <= 0):
            raise StageValidationError("ready wheel rig contains invalid wheel geometry")
        side_name = str(row.get("side_string") or "").lower()
        if side_name not in {"left", "right"}: raise StageValidationError("ready wheel rig contains invalid wheel side")
        wheels.append({"name": part_id, "axle": int(row.get("axle_index_int") or 0),
            "side": 1 if side_name == "left" else -1, "center": [float(v) for v in center],
            "radius": radius, "width": width, "axis": [float(v) for v in axis],
            "steer": bool(row.get("steer_bool")), "mesh_group": str(row.get("mesh_group_id_string") or part_id)})
    expected_count = int(qa.get("expected_wheel_count_int") or 0)
    expected_axles = int(qa.get("expected_axle_count_int") or 0)
    expected_layout = qa.get("expected_axle_wheel_counts_array")
    if expected_count < 1 or expected_axles < 1 or not isinstance(expected_layout, list) or len(expected_layout) != expected_axles:
        raise StageValidationError("ready wheel rig lacks an exact expected axle layout")
    if len(wheels) != expected_count or len({wheel["name"] for wheel in wheels}) != len(wheels):
        raise StageValidationError("ready wheel rig count does not match its accepted receipt")
    actual_layout = []
    for axle in range(expected_axles):
        members = [wheel for wheel in wheels if wheel["axle"] == axle]
        actual_layout.append(len(members))
        if len({wheel["side"] for wheel in members}) != len(members):
            raise StageValidationError("ready wheel axle has duplicate sides")
        if len({wheel["mesh_group"] for wheel in members}) != 1:
            raise StageValidationError("ready wheel axle does not share one mesh group")
    if actual_layout != [int(value) for value in expected_layout]:
        raise StageValidationError("ready wheel axle layout does not match its accepted receipt")
    expected_steering = qa.get("steering_axle_indices_array")
    if not isinstance(expected_steering, list):
        raise StageValidationError("ready wheel rig lacks an expected steering axle receipt")
    actual_steering = []
    for axle in range(expected_axles):
        flags = {wheel["steer"] for wheel in wheels if wheel["axle"] == axle}
        if len(flags) != 1: raise StageValidationError("ready wheel axle has inconsistent steering flags")
        if True in flags: actual_steering.append(axle)
    if actual_steering != [int(value) for value in expected_steering]:
        raise StageValidationError("ready wheel steering layout does not match its accepted receipt")
    return {"schema": "autorig.motion-transfer.wheels/2", "wheels": wheels,
            "ground_y": min(wheel["center"][1] - wheel["radius"] for wheel in wheels),
            "symmetry": {"axis": [1.0, 0.0, 0.0], "offset": 0.0}, "texture": None,
            "source_receipt": rig}


def _preview_geometry_path(root: Path, manifest: Mapping[str, Any]) -> Path:
    if _validated_wheel_doc(manifest) is None:
        path = root / "voxels.glb"
        if not path.is_file(): raise StageValidationError("voxel preview GLB is missing")
        return path
    body = manifest.get("wheel_free_body_object") or {}
    relative = str(body.get("glb_path_string") or "")
    if body.get("excludes_wheels_bool") is not True or not relative:
        raise StageValidationError("ready wheel preview needs an explicit wheel-free body artifact")
    artifact = next((item for item in manifest.get("artifacts_array") or []
                     if str(item.get("path_string") or item.get("relative_path_string") or "") == relative), None)
    if artifact is None or not _artifact_ok(root, artifact):
        raise StageValidationError("wheel-free body artifact receipt is invalid")
    path = (root / relative).resolve()
    try: path.relative_to(root.resolve())
    except ValueError: raise StageValidationError("wheel-free body artifact escapes the run") from None
    return path


def _preview_import(manifest: Mapping[str, Any], glb_path: Path, allow_drive: bool = True,
                    wheels_override: Mapping[str, Any] | None = None) -> dict[str, Any]:
    try:
        from mt.vehicle_preview_import import import_preview
    except ImportError as exc: raise StageValidationError("MT vehicle preview importer is not installed") from exc
    digest = hashlib.sha256(glb_path.read_bytes()).hexdigest()
    analysis = manifest.get("analysis_object") or {}
    dimensions = analysis.get("estimated_dimensions_m_object") or {}
    author_views = []
    for view in manifest.get("fusion_views_array") or manifest.get("views_array") or []:
        reference, depth, camera = (view.get(key) or {} for key in
                                    ("reference_object", "depth_object", "camera_object"))
        author_views.append({"id": str(view.get("id_string") or view.get("id") or ""),
            "reference": {"path": reference.get("path_string"), "sha256": reference.get("sha256_string")},
            "depth": {"path": depth.get("path_string"), "sha256": depth.get("sha256_string")},
            "camera": {"path": camera.get("path_string"), "sha256": camera.get("sha256_string")}})
    fused = manifest.get("fused_voxels_object") or {}; removal = manifest.get("wheel_removal_object") or {}
    cleanup = manifest.get("body_cleanup_object") or {}
    primary = manifest.get("primary_reference_object") or {}; primary_ref = primary.get("reference_object") or {}
    primary_camera = primary.get("camera_object") or {}
    origin_id = str(manifest.get("originating_vehicle_id_string") or manifest.get("run_id_string") or "")
    receipt = {"category": "vehicle", "source_sha256": digest, "coordinate_space": "gltf_y_up_z_forward",
               "originating_vehicle_id": origin_id,
               "height_m": dimensions.get("height_float"), "parts": manifest.get("parts_array") or [],
               "authoring": {"artifact_root": str(glb_path.parent), "originating_vehicle_id": origin_id,
                   "primary_reference": {"reference": {"path": primary_ref.get("path_string"),
                                                          "sha256": primary_ref.get("sha256_string")},
                                         "camera": {"path": primary_camera.get("path_string"),
                                                    "sha256": primary_camera.get("sha256_string")}},
                   "fusion_views": author_views,
                   "fused_voxels": {"path": fused.get("glb_path_string"), "sha256": fused.get("glb_sha256_string"),
                       "count": fused.get("voxel_count_int"), "coordinate_space": fused.get("coordinate_space_string")},
                   "default_view": manifest.get("default_view_string"),
                   "shared_model_transform": manifest.get("shared_model_transform_4x4_array"),
                   "source_bindings": {"original_sha256": manifest.get("original_source_sha256_string"),
                       "prepared_sha256": manifest.get("prepared_source_sha256_string"),
                       "wheel_free_path": removal.get("path_string"),
                       "wheel_free_sha256": removal.get("output_sha256_string"),
                       "body_cleanup_path": cleanup.get("path_string"),
                       "body_cleanup_sha256": cleanup.get("output_sha256_string"),
                       "geometry_options_sha256": manifest.get("geometry_options_sha256_string") or
                                                  manifest.get("options_sha256_string")}}}
    if manifest.get("schema_string") != "autorig.vehicle.multiview/1":
        receipt.pop("authoring", None)
    runs_root = Path(os.getenv("AUTORIG_VEHICLE_PREVIEW_RUNS_DIR", "/srv/autorig/data/motion_transfer/runs"))
    wheels = dict(wheels_override) if wheels_override is not None else (_validated_wheel_doc(manifest) if allow_drive else None)
    result = import_preview(receipt, glb_path, wheels, runs_root,
                            title=str(analysis.get("title_string") or "Vehicle pipeline preview"))
    public_root = Path("/srv/autorig/data/motion_transfer/runs").resolve()
    published = runs_root.resolve() == public_root
    result["publicly_routable_bool"] = published
    if not published:
        result["unpublished_viewer_url"] = result.get("staged_viewer_url") or result.get("viewer_url")
        result["viewer_url"] = None; result["staged_viewer_url"] = None
    return result


def _mt_options(options: Mapping[str, Any], run_id: str, analysis: Mapping[str, Any] | None = None,
                require_geometry: bool = False) -> dict[str, Any]:
    # Reconstruct the wire object from the typed create-run fields. Never pass
    # arbitrary action keys into the MT client: it has trusted endpoint/path options.
    side = str(options.get("working_side") or "auto")
    resolution = int(options.get("resolution") or 64)
    value = {"run_id_string": run_id,
             "base_url_string": os.getenv("AUTORIG_VEHICLE_FARM_BASE_URL", "https://autorig.online").rstrip("/"),
             "voxel_resolution_int": resolution, "resolution_int": resolution,
             "max_voxel_points_int": 150000}
    resolved_side = {"left": "image_left", "right": "image_right"}.get(side)
    canonical_side = {"left": "negative_x", "right": "positive_x"}.get(side)
    if resolved_side is None and analysis:
        candidate = str(analysis.get("working_side_string") or "")
        if candidate in {"image_left", "image_right"}: resolved_side = candidate
        candidate = str(analysis.get("canonical_working_side_string") or "")
        if candidate in {"positive_x", "negative_x"}: canonical_side = candidate
    if resolved_side is not None: value["working_side_string"] = resolved_side
    elif require_geometry: raise StageValidationError("Vision did not resolve the vehicle working side")
    if canonical_side is not None: value["canonical_working_side_string"] = canonical_side
    elif require_geometry: raise StageValidationError("Vision did not resolve the canonical vehicle working side")
    dimensions = dict((analysis or {}).get("estimated_dimensions_m_object") or {})
    if require_geometry:
        try: numbers = {key: float(dimensions[key]) for key in ("length_float", "width_float", "height_float")}
        except (KeyError, TypeError, ValueError) as exc: raise StageValidationError("vehicle dimensions are unavailable") from exc
        if (not .5 <= numbers["length_float"] <= 30 or
                any(not .1 <= numbers[key] <= 30 for key in ("width_float", "height_float"))):
            raise StageValidationError("vehicle dimensions are outside the supported metric range")
        value["dimensions_m_object"] = numbers
    if options.get("length_m") is not None:
        value["target_length_m_float"] = float(options["length_m"])
    return value


def _model_options(request: Mapping[str, Any]) -> dict[str, Any]:
    raw = dict(request.get("options") or {})
    value = {key: raw[key] for key in ("require_exact_parts_bool", "registration_tolerance_m_float") if key in raw}
    selected = [str(item) for item in request.get("part_ids") or [] if str(item)]
    requested_mode = str(request.get("mode") or "body")
    value.update(mode_string="selected_parts" if requested_mode == "split" else "body_only",
                 selected_part_ids_array=selected, polygon_budget_int=int(request.get("polygon_budget") or 10000))
    return value


def _artifact_ok(root: Path, item: Mapping[str, Any]) -> bool:
    relative = str(item.get("relative_path_string") or item.get("path_string") or item.get("path") or "")
    if not relative: return False
    path = (root / relative).resolve()
    try: path.relative_to(root.resolve())
    except ValueError: return False
    if path.is_symlink() or not path.is_file() or path.stat().st_size <= 0: return False
    expected = str(item.get("sha256_string") or item.get("sha256") or "")
    return not expected or hashlib.sha256(path.read_bytes()).hexdigest() == expected


def _hashed_artifact(root: Path, artifacts: list[Mapping[str, Any]], relative: str, expected_sha: str) -> bool:
    if len(expected_sha) != 64: return False
    item = next((row for row in artifacts if str(row.get("path_string") or row.get("relative_path_string") or "") == relative), None)
    return bool(item and str(item.get("sha256_string") or item.get("sha256") or "") == expected_sha and
                _artifact_ok(root, item))


def _validate_multiview(root: Path, manifest: Mapping[str, Any], stage: str, expected_resolution: int | None) -> None:
    if manifest.get("schema_string") != "autorig.vehicle.multiview/1":
        raise StageValidationError("multiview stage returned a legacy single-view manifest")
    artifacts = list(manifest.get("artifacts_array") or [])
    stage_tasks = [task for task in manifest.get("provider_tasks_array") or []
                   if task.get("pipeline_stage_string") == stage]
    if not stage_tasks:
        raise StageValidationError("multiview stage has no provider task receipt")
    for task in stage_tasks:
        if (task.get("schema_string") != "autorig.vehicle.provider-task/1" or
                task.get("status_string") != "completed" or not task.get("service_string") or
                not task.get("task_id_string") or not task.get("node_string") or not task.get("worker_string") or
                len(str(task.get("input_sha256_string") or "")) != 64 or
                len(str(task.get("output_sha256_string") or "")) != 64):
            raise StageValidationError("multiview provider task receipt is incomplete")
    primary = manifest.get("primary_reference_object") or {}; primary_ref = primary.get("reference_object") or {}
    primary_camera = primary.get("camera_object") or {}
    if (primary.get("role_string") != "display_only" or primary.get("fusion_member_bool") is not False or
            primary_camera.get("schema_string") != "autorig.vehicle.primary-camera/1" or
            primary_camera.get("coordinate_space_string") != "model_local_gltf" or
            primary_camera.get("projection_string") != "orthographic" or
            not _hashed_artifact(root, artifacts, str(primary_ref.get("path_string") or ""),
                                 str(primary_ref.get("sha256_string") or "")) or
            not _hashed_artifact(root, artifacts, str(primary_camera.get("path_string") or ""),
                                 str(primary_camera.get("sha256_string") or ""))):
        raise StageValidationError("primary 45/45 display reference receipt is incomplete")
    cleanup = manifest.get("body_cleanup_object") or {}; counts = cleanup.get("semantic_counts_object") or {}
    cleanup_path, cleanup_sha = str(cleanup.get("path_string") or ""), str(cleanup.get("output_sha256_string") or "")
    if (cleanup.get("schema_string") != "autorig.vehicle.body-cleanup/1" or
            cleanup.get("contract_version_string") != "body-cleanup-v1" or
            cleanup.get("source_sha256_string") != manifest.get("prepared_source_sha256_string") or
            not _hashed_artifact(root, artifacts, cleanup_path, cleanup_sha) or
            primary_ref.get("sha256_string") != cleanup_sha):
        raise StageValidationError("vehicle body-cleanup receipt is incomplete")
    cleanup_ready = (cleanup.get("status_string") == "complete" and cleanup.get("wheels_removed_bool") is True and
        cleanup.get("equipment_removed_bool") is True and cleanup.get("cargo_removed_bool") is True and
        all(int(counts.get(key, -1)) == 0 for key in ("wheels_int", "equipment_int", "cargo_int")))
    removal = manifest.get("wheel_removal_object") or {}
    if stage == "remove_wheels":
        path, digest = str(removal.get("path_string") or ""), str(removal.get("output_sha256_string") or "")
        if not _hashed_artifact(root, artifacts, path, digest):
            raise StageValidationError("wheel removal receipt is incomplete")
        if manifest.get("status_string") == "complete" and (not cleanup_ready or removal.get("status_string") != "complete"):
            raise StageValidationError("complete remove stage lacks accepted body cleanup")
        return
    if not cleanup_ready:
        raise StageValidationError("downstream multiview stage lacks accepted body cleanup")
    views = manifest.get("views_array") or []
    if {str(view.get("id_string") or view.get("id") or "") for view in views} != {"top", "front", "left", "back"}:
        raise StageValidationError("multiview receipt needs exactly top/front/left/back views")
    for view in views:
        reference, camera = view.get("reference_object") or {}, view.get("camera_object") or {}
        path, digest = str(reference.get("path_string") or ""), str(reference.get("sha256_string") or "")
        if reference.get("wheel_free_bool") is not True or not _hashed_artifact(root, artifacts, path, digest):
            raise StageValidationError("multiview reference is not wheel-free and hash-bound")
        if reference.get("source_sha256_string") != cleanup_sha:
            raise StageValidationError("multiview reference is not derived from the accepted clean body")
        if camera.get("projection_string") != "orthographic" or camera.get("coordinate_space_string") != "model_local_gltf":
            raise StageValidationError("multiview camera contract is invalid")
        if not _hashed_artifact(root, artifacts, str(camera.get("path_string") or ""),
                                str(camera.get("sha256_string") or "")):
            raise StageValidationError("multiview camera artifact is incomplete")
        if stage in {"depth_views", "fuse_voxels"}:
            depth = view.get("depth_object") or {}; depth_path = str(depth.get("path_string") or "")
            if not _hashed_artifact(root, artifacts, depth_path, str(depth.get("sha256_string") or "")):
                raise StageValidationError("multiview depth receipt is incomplete")
            if depth.get("source_sha256_string") != digest:
                raise StageValidationError("multiview depth is not bound to its reference image")
            if (depth.get("convention_string") != "DepthAnythingV2-relative-near-white" or
                    depth.get("near_is_white_bool") is not True or depth.get("metric_bool") is not False):
                raise StageValidationError("multiview depth convention/polarity receipt is invalid")
    if stage != "fuse_voxels": return
    if str(manifest.get("default_view_string") or "") not in {"top", "front", "left", "back"}:
        raise StageValidationError("multiview default view is invalid")
    transform = manifest.get("shared_model_transform_4x4_array")
    if not isinstance(transform, list) or len(transform) != 16:
        raise StageValidationError("multiview shared model transform is missing")
    fused = manifest.get("fused_voxels_object") or {}; symmetry = fused.get("symmetry_object") or {}
    if manifest.get("status_string") == "needs_review" and not fused.get("glb_path_string"):
        if not str(fused.get("reason_string") or manifest.get("fusion_review_reason_string") or ""):
            raise StageValidationError("rejected fusion receipt has no reason")
        return
    resolution = int(fused.get("resolution_int") or 0); count = int(fused.get("voxel_count_int") or 0)
    if expected_resolution is not None and resolution != expected_resolution:
        raise StageValidationError("fused voxel resolution does not match the immutable run option")
    if count < 1 or count > 150000 or float(symmetry.get("pair_symmetry_error_m_float", -1)) != 0:
        raise StageValidationError("fused voxel count or X=0 symmetry receipt is invalid")
    if fused.get("coordinate_space_string") != "model_local_gltf":
        raise StageValidationError("fused voxel coordinate space is invalid")
    if (manifest.get("source_bindings_object") or {}).get("body_cleanup_source_sha256_string") != cleanup_sha:
        raise StageValidationError("fused voxel lineage does not name the accepted clean body")
    dimensions = manifest.get("dimensions_m_object") or {}
    canonical_side = str(manifest.get("canonical_working_side_string") or
                         fused.get("canonical_working_side_string") or "")
    try: dimension_values = [float(dimensions[key]) for key in ("length_float", "width_float", "height_float")]
    except (KeyError, TypeError, ValueError) as exc: raise StageValidationError("fused dimensions receipt is invalid") from exc
    if not .5 <= dimension_values[0] <= 30 or any(not .1 <= value <= 30 for value in dimension_values[1:]):
        raise StageValidationError("fused dimensions are outside the accepted range")
    quality = fused.get("quality_object") or {}; leave_one_out = quality.get("leave_one_view_out_object") or {}
    if (canonical_side not in {"positive_x", "negative_x"} or quality.get("fixed_resolution_bool") is not True or
            int(quality.get("requested_resolution_int") or 0) != resolution or
            quality.get("centered_x_lattice_bool") is not True or
            quality.get("provenance_mirrored_from_working_side_bool") is not True):
        raise StageValidationError("fused provenance/independent quality receipt is incomplete")
    if manifest.get("status_string") == "complete" and leave_one_out.get("accepted_bool") is not True:
        raise StageValidationError("complete fusion lacks accepted independent quality")
    for kind in ("npz", "json", "glb"):
        if not _hashed_artifact(root, artifacts, str(fused.get(f"{kind}_path_string") or ""),
                                str(fused.get(f"{kind}_sha256_string") or "")):
            raise StageValidationError(f"fused voxel {kind} receipt is incomplete")


def _validate_manifest(root: Path, manifest: Mapping[str, Any], source_sha: str, stage: str,
                       expected_resolution: int | None = None) -> dict[str, Any]:
    status = manifest.get("status_string")
    allowed = {"complete", "completed", "ready", "ok"}
    if stage in {"analyze", "prepare", "remove_wheels", "project_views", "depth_views", "fuse_voxels",
                 "cluster", "voxelize"}: allowed.add("needs_review")
    if status not in allowed: raise StageValidationError("vehicle pipeline did not complete")
    if manifest.get("stage_string") != stage: raise StageValidationError("vehicle manifest stage mismatch")
    original_sha = str(manifest.get("original_source_sha256_string") or manifest.get("source_sha256_string") or "")
    if original_sha != source_sha: raise StageValidationError("vehicle manifest original source hash mismatch")
    effective_sha = str(manifest.get("source_sha256_string") or "")
    prepared_sha = str((manifest.get("prepared_image_object") or {}).get("sha256_string") or
                       manifest.get("prepared_source_sha256_string") or "")
    if effective_sha != source_sha and (not prepared_sha or effective_sha != prepared_sha):
        raise StageValidationError("vehicle manifest prepared source hash mismatch")
    if stage in {"remove_wheels", "project_views", "depth_views", "fuse_voxels"}:
        _validate_multiview(root, manifest, stage, expected_resolution)
    if stage == "analyze":
        analysis = manifest.get("analysis_object") or {}
        if not analysis.get("is_wheeled_vehicle_bool"): raise StageValidationError("source is not a wheeled vehicle")
    if stage == "voxelize":
        coord = manifest.get("coordinate_system_object") or {}
        if str(coord.get("units_string") or coord.get("units") or "") != "meters": raise StageValidationError("vehicle common space must use meters")
    artifacts = manifest.get("artifacts_array") or []
    if not artifacts or not all(_artifact_ok(root, item) for item in artifacts): raise StageValidationError("vehicle artifact receipt is missing or invalid")
    if stage == "voxelize" and _validated_wheel_doc(manifest) is not None:
        paths = {str(item.get("path_string") or item.get("relative_path_string") or "") for item in artifacts}
        if "wheel_rig.json" not in paths: raise StageValidationError("ready wheel rig artifact is missing")
    value = dict(manifest)
    if stage == "prepare":
        camera = manifest.get("camera_object") or {}
        exact = (camera.get("normalization_status_string") == "accepted" and
                 camera.get("projection_string") == "orthographic" and
                 abs(float(camera.get("yaw_deg_float", -999)) - 45.0) <= .5 and
                 abs(float(camera.get("pitch_deg_float", -999)) - 45.0) <= .5 and bool(prepared_sha))
        if not exact:
            value["status_string"] = "needs_review"
            value["camera_blocker_string"] = "accepted orthographic yaw45/pitch45 preparation is required"
    return value


class _LeaseHeartbeat:
    def __init__(self, store: VehicleStore, job, every: float = 30):
        self.store, self.job, self.every, self.stop, self.lost = store, job, every, threading.Event(), threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True, name="vehicle-stage-lease")
    def _run(self):
        while not self.stop.wait(self.every):
            try: self.store.renew(self.job)
            except Exception: self.lost.set(); return
    def __enter__(self): self.thread.start(); return self
    def __exit__(self, exc_type, *_):
        self.stop.set(); self.thread.join(timeout=2)
        if exc_type is None and self.lost.is_set():
            raise StageValidationError("vehicle stage lease was lost before publication")


class _WorkerSingleton:
    def __init__(self, path: Path): self.path, self.handle = path, None
    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True); self.handle = self.path.open("a+b")
        try:
            if os.name == "nt":
                import msvcrt
                self.handle.seek(0); msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self.handle.close(); raise SystemExit("vehicle worker is already running") from exc
        return self
    def __exit__(self, *_):
        try:
            if os.name == "nt":
                import msvcrt
                self.handle.seek(0); msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        finally: self.handle.close()


def _mt_child(messages, source: str, out: str, stage: str, options: dict[str, Any]) -> None:
    if os.name != "nt": os.setsid()
    try:
        if stage in {"remove_wheels", "project_views", "depth_views", "fuse_voxels"}:
            from mt.vehicle_multiview_pipeline import run_pipeline
        else:
            from mt.vehicle_image_pipeline import run_pipeline
        def progress(phase, detail=None): messages.put(("progress", phase, dict(detail or {})))
        run_pipeline(source, out, stage=stage, options=options, phase_callback=progress)
        messages.put(("done",))
    except BaseException as exc:
        messages.put(("error", type(exc).__name__, str(exc)))
        raise


def _mt_stage(source: Path, out: Path, stage: str, options: Mapping[str, Any], phase_callback=None) -> dict[str, Any]:
    context = multiprocessing.get_context("spawn"); messages = context.Queue()
    process = context.Process(target=_mt_child, args=(messages, str(source), str(out), stage, dict(options)))
    process.start(); deadline = time.monotonic() + float(os.getenv("AUTORIG_VEHICLE_STAGE_TIMEOUT_SECONDS", "1800"))
    error = None
    while process.is_alive() and time.monotonic() < deadline:
        try: message = messages.get(timeout=.5)
        except queue.Empty: continue
        if message[0] == "progress" and phase_callback: phase_callback(message[1], **message[2])
        elif message[0] == "error": error = f"{message[1]}: {message[2]}"
    if process.is_alive():
        if os.name != "nt":
            try: os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError: pass
        else: process.terminate()
        process.join(timeout=10)
        if process.is_alive(): process.kill(); process.join(timeout=5)
        raise StageValidationError(f"vehicle {stage} stage exceeded its server timeout")
    process.join()
    while True:
        try: message = messages.get_nowait()
        except queue.Empty: break
        if message[0] == "progress" and phase_callback: phase_callback(message[1], **message[2])
        elif message[0] == "error": error = f"{message[1]}: {message[2]}"
    if process.exitcode != 0: raise StageValidationError(error or f"vehicle {stage} subprocess failed")
    manifest_path = out / "manifest.json"
    if not manifest_path.is_file(): raise StageValidationError("MT vehicle pipeline produced no manifest")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    return manifest


async def _model_stage(out: Path, manifest: Mapping[str, Any], request: Mapping[str, Any], run_id: str,
                       capability_base: str, revision: int) -> dict[str, Any]:
    if request.get("provider") != "blender":
        raise StageValidationError("vehicle farm model provider is unavailable")
    try:
        from mt.blender_vehicle import assemble
    except ImportError as exc: raise StageValidationError("farm Blender vehicle adapter is not installed") from exc
    result = assemble(_CapabilityRun(out, run_id, capability_base), manifest, options=_model_options(request))
    if hasattr(result, "__await__"): result = await result
    if not isinstance(result, Mapping): raise StageValidationError("farm Blender adapter returned no receipt")
    if result.get("status_string") == "needs_review":
        raise StageValidationError(str(result.get("reason_string") or "farm Blender assembly needs review"))
    selected = {str(value) for value in request.get("part_ids") or [] if str(value)}
    if request.get("mode") == "split":
        replaced = {str(value) for value in ((result.get("report_object") or {}).get("replacement_node_ids_array") or [])}
        if not selected or not selected.issubset(replaced):
            raise StageValidationError("Blender receipt does not contain every selected vehicle object")
    model_value = str(result.get("model_path_string") or result.get("model_path") or "")
    model = Path(model_value)
    if not model.is_absolute(): model = out / model
    if not model.is_file() or model.stat().st_size < 20 or model.read_bytes()[:4] != b"glTF":
        raise StageValidationError("farm Blender adapter produced no valid GLB")
    report_value = str(result.get("report_path_string") or result.get("report_path") or "")
    report = Path(report_value) if report_value else out / "vehicle-assembly.report.json"
    if not report.is_absolute(): report = out / report
    if not report.is_file(): raise StageValidationError("farm Blender adapter produced no report")
    public_model = out / "vehicle-assembly.glb"
    public_report = out / "vehicle-assembly.report.json"
    if model.resolve() != public_model.resolve():
        temporary = out / f".vehicle-model-{os.getpid()}.tmp"
        shutil.copyfile(model, temporary); os.replace(temporary, public_model)
    if report.resolve() != public_report.resolve():
        temporary = out / f".vehicle-report-{os.getpid()}.tmp"
        shutil.copyfile(report, temporary); os.replace(temporary, public_report)
    projection = out / "proj"; projection.mkdir(exist_ok=True)
    projection_model = projection / "model.glb"
    temporary = projection / f".vehicle-model-{os.getpid()}.tmp"
    shutil.copyfile(public_model, temporary); os.replace(temporary, projection_model)
    try:
        from mt.project import run as render_projections
    except ImportError as exc: raise StageValidationError("MT projection renderer is not installed") from exc
    projection_receipt = await asyncio.to_thread(render_projections, public_model.read_bytes(), projection, size=512,
                                                 views=("left", "top"), passes=("depth16", "mask", "lit"), video=False)
    required = ("cameras.json", "left_depth16.png", "left_lit.png", "top_mask.png")
    if any(not (projection / name).is_file() for name in required):
        raise StageValidationError("vehicle wheel projections are incomplete")
    value = json.loads(json.dumps(dict(result), default=str))
    value.update(model_path_string=str(public_model), report_path_string=str(public_report),
                 wheel_projection_receipt_object=projection_receipt)
    request_hash = hashlib.sha256(json.dumps(request, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:12]
    version_relative = f"models/r{revision}-{request_hash}"
    version_dir = out / version_relative; version_dir.mkdir(parents=True, exist_ok=True)
    for source, name in ((public_model, "vehicle-assembly.glb"), (public_report, "vehicle-assembly.report.json")):
        temporary = version_dir / f".{name}.{os.getpid()}.tmp"
        shutil.copyfile(source, temporary); os.replace(temporary, version_dir / name)
    value.update(version_relative_path_string=version_relative,
                 version_model_path_string=str(version_dir / "vehicle-assembly.glb"),
                 version_report_path_string=str(version_dir / "vehicle-assembly.report.json"))
    return value


def _record_model_artifact(document: dict[str, Any], request: Mapping[str, Any], receipt: Mapping[str, Any],
                           capability_base: str) -> None:
    relative = str(receipt["version_relative_path_string"]).strip("/")
    entry = {"model_url": f"{capability_base}/{relative}/vehicle-assembly.glb",
             "report_url": f"{capability_base}/{relative}/vehicle-assembly.report.json",
             "revision": document["revision"], "polygon_budget": request.get("polygon_budget"),
             "provider": request.get("provider"), "receipt": receipt.get("report_object") or {}}
    artifacts = document.setdefault("artifacts", {})
    if request.get("mode") == "split":
        part_versions = artifacts.setdefault("model_parts", {})
        for part_id in request.get("part_ids") or []:
            part_versions[str(part_id)] = dict(entry, part_id=str(part_id))
    else:
        artifacts["model_body"] = entry
        artifacts["model_url"] = entry["model_url"]
    artifacts.pop("wheel_rig_url", None)
    document["rig_invalidated_bool"] = True


def _manifest_with_part_reviews(manifest: Mapping[str, Any], document: Mapping[str, Any],
                                request: Mapping[str, Any]) -> dict[str, Any]:
    value = json.loads(json.dumps(dict(manifest)))
    selected = {str(item) for item in request.get("part_ids") or []}
    reviews = document.get("accepted_parts") or {}
    receipts = []
    for part in value.get("parts_array") or []:
        part_id = str(part.get("part_id_string") or part.get("id") or "")
        if part_id not in selected: continue
        review = reviews.get(part_id)
        if not review: raise StageValidationError(f"selected vehicle part lacks owner review: {part_id}")
        part["shape_gate_object"] = {"accepted_bool": True, "reason_string": "owner accepted exact hash-bound mask",
                                     "review_id_string": review.get("review_id")}
        receipts.append({key: review.get(key) for key in ("review_id", "part_id", "run_revision",
                                                           "original_sha256", "prepared_sha256", "mask_sha256")})
    if selected != {str(item.get("part_id")) for item in receipts}:
        raise StageValidationError("selected vehicle part review set is incomplete")
    value["selected_part_reviews_array"] = receipts
    return value


def _rig_stage(out: Path) -> dict[str, Any]:
    try:
        from mt.wheels import run as build_wheels
    except ImportError as exc: raise StageValidationError("MT vehicle wheel rig is not installed") from exc
    result = build_wheels(out)
    path = out / "wheels" / "rig.json"
    if not path.is_file() or not isinstance(result, Mapping) or not result.get("wheels"):
        raise StageValidationError("MT wheel rig produced no wheels")
    public = out / "wheels.json"; temporary = out / f".wheels-{os.getpid()}.tmp"
    shutil.copyfile(path, temporary); os.replace(temporary, public)
    return dict(result)


def _configure_stage(out: Path, options: Mapping[str, Any]) -> dict[str, Any]:
    path = out / "wheels" / "rig.json"
    if not path.is_file(): raise StageValidationError("vehicle must be wheel-rigged before configuration")
    try:
        from mt.vehicle_wheel_contract import update_file
    except ImportError as exc: raise StageValidationError("MT wheel configuration contract is not installed") from exc
    before_bytes = path.read_bytes(); updates = options.get("wheel_updates")
    if isinstance(updates, Mapping):
        rows = [{"name": name, **dict(value)} for name, value in updates.items() if isinstance(value, Mapping)]
    else:
        rows = options.get("wheels")
    if not isinstance(rows, list) or not rows:
        raise StageValidationError("configure needs options.wheel_updates or options.wheels")
    patch = {"wheels": rows}
    if options.get("expected_wheel_revision") is not None:
        patch["expected_revision"] = options["expected_wheel_revision"]
    doc = update_file(path, patch); encoded = path.read_bytes()
    public = out / "wheels.json"; temporary = out / f".wheels-{os.getpid()}.tmp"
    temporary.write_bytes(encoded); os.replace(temporary, public)
    return {**doc, "before_sha256": hashlib.sha256(before_bytes).hexdigest(),
            "after_sha256": hashlib.sha256(encoded).hexdigest()}


def _persist_wheel_rig(out: Path, wheel_doc: Mapping[str, Any]) -> dict[str, Any]:
    try:
        from mt.vehicle_wheel_contract import revision
        from mt.wheels import upgrade_contract
    except ImportError as exc: raise StageValidationError("MT wheel contract is not installed") from exc
    doc = upgrade_contract(dict(wheel_doc)); doc["revision"] = revision(doc)
    wheel_dir = out / "wheels"; wheel_dir.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(doc, ensure_ascii=False, indent=1).encode("utf-8")
    for path in (wheel_dir / "rig.json", out / "wheels.json"):
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp"); temporary.write_bytes(encoded); os.replace(temporary, path)
    return doc


async def _farm_fuse_stage(store: VehicleStore, job, request: dict[str, Any], out: Path,
                           run_id: str, capability_base: str) -> dict[str, Any]:
    try:
        from mt.blender import submit_vehicle_fuse, vehicle_fuse_outcome, wait_vehicle_fuse
    except ImportError as exc: raise StageValidationError("vehicle fusion farm dispatcher is not installed") from exc
    manifest_path = out / "manifest.json"
    if not manifest_path.is_file(): raise StageValidationError("depth-view manifest is unavailable")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_string") != "autorig.vehicle.multiview/1" or manifest.get("stage_string") != "depth_views":
        raise StageValidationError("fusion requires a completed multiview depth manifest")
    input_sha = hashlib.sha256(json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    task_id = str(request.get("provider_task_id_string") or "")
    if not task_id:
        run = _CapabilityRun(out, run_id, capability_base)
        handle = await asyncio.to_thread(submit_vehicle_fuse, run, manifest)
        task_id = str(handle.get("job_id_string") or "")
        if not task_id: raise StageValidationError("fusion farm returned no persisted task id")
        request = store.checkpoint_job(job, {"provider_task_id_string": task_id,
                                             "provider_input_sha256_string": input_sha})
        store.progress(job, "farm_submitted", 2, {"provider_task_id_string": task_id})
    elif request.get("provider_input_sha256_string") != input_sha:
        raise StageValidationError("persisted fusion task input hash changed")
    files = await wait_vehicle_fuse(task_id, timeout=330.0, wait_worker=45.0)
    run = _CapabilityRun(out, run_id, capability_base)
    installed = await asyncio.to_thread(vehicle_fuse_outcome, run, task_id, files)
    if installed.get("schema_string") != "autorig.vehicle-fuse-install/1" or installed.get("status_string") != "complete":
        if installed.get("schema_string") != "autorig.vehicle-fuse-install/1" or installed.get("status_string") != "needs_review":
            raise StageValidationError("fusion farm outcome receipt is invalid")
    fused = dict(installed.get("fused_voxels_object") or {}); new_artifacts = list(installed.get("artifacts_array") or [])
    if new_artifacts:
        for kind in ("npz", "json", "glb"):
            relative = str(fused.get(f"{kind}_path_string") or "")
            artifact = next((row for row in new_artifacts if row.get("path_string") == relative), None)
            if artifact is None: raise StageValidationError("fusion install artifact mapping is incomplete")
            fused[f"{kind}_sha256_string"] = artifact.get("sha256_string")
    output_sha = hashlib.sha256(json.dumps(fused, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    provider = {"schema_string": "autorig.vehicle.provider-task/1", "pipeline_stage_string": "fuse_voxels",
        "service_string": "vehicle_fuse", "task_id_string": task_id, "status_string": "completed",
        "node_string": str(receipt.get("worker_string") or "farm"), "worker_string": str(receipt.get("worker_string") or "farm"),
        "input_sha256_string": input_sha, "output_sha256_string": output_sha}
    manifest.update(stage_string="fuse_voxels", status_string=installed["status_string"], fused_voxels_object=fused)
    if installed.get("reason_string"): manifest["fusion_review_reason_string"] = installed["reason_string"]
    manifest["provider_tasks_array"] = list(manifest.get("provider_tasks_array") or []) + [provider]
    old = [row for row in manifest.get("artifacts_array") or [] if not str(row.get("path_string") or "").startswith("fused/")]
    manifest["artifacts_array"] = old + new_artifacts
    temporary = manifest_path.with_suffix(".json.tmp"); temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"); os.replace(temporary, manifest_path)
    return manifest


def _public_from_manifest(document: dict[str, Any], manifest: Mapping[str, Any], capability_base: str,
                          artifact_root: Path) -> None:
    document["analysis"] = manifest.get("analysis_object") or document.get("analysis")
    parts = manifest.get("parts_array") or document.get("parts") or []
    names = {str((item.get("relative_path_string") or item.get("path_string") or "")): item for item in manifest.get("artifacts_array") or []}
    def url(name: str) -> str | None: return f"{capability_base}/{name}" if name in names or artifact_root.joinpath(name).is_file() else None
    enriched = []
    for source in parts:
        part = dict(source); mask = str(part.get("source_mask_path_string") or part.get("mask_path_string") or part.get("mask_path") or "")
        if mask:
            part["mask_url"] = f"{capability_base}/{mask}"
            artifact = names.get(mask) or {}
            part["mask_sha256"] = artifact.get("sha256_string") or artifact.get("sha256")
        role = str(part.get("image_role") or part.get("image_role_string") or "")
        if role: part["image_role"] = role
        review = (document.get("accepted_parts") or {}).get(str(part.get("part_id_string") or part.get("id") or "")) or {}
        if review and review.get("mask_sha256") == part.get("mask_sha256"):
            part.update(accepted_bool=True, review_id=review.get("review_id"),
                        review_revision=review.get("run_revision"))
        enriched.append(part)
    document["parts"] = enriched
    artifacts = dict(document.get("artifacts") or {})
    for key, name in (("overlay_url", "semantic_observation.png"), ("voxel_url", "voxels.glb"), ("voxel_preview_url", "relative_depth.png")):
        found = url(name)
        if found: artifacts[key] = found
    document["artifacts"] = artifacts
    prepared = manifest.get("prepared_image_object") or {}
    prepared_path = str(prepared.get("path_string") or manifest.get("prepared_image_path_string") or "")
    prepared_sha = str(prepared.get("sha256_string") or manifest.get("prepared_image_sha256_string") or
                       manifest.get("prepared_source_sha256_string") or
                       (document.get("source_sha_chain") or {}).get("prepared_sha256") or "")
    if prepared_path:
        document["prepared_image_url"] = f"{capability_base}/{prepared_path}"
    document["original_url"] = document.get("original_url") or document.get("source_url")
    document["source_sha_chain"] = {"original_sha256": document.get("source_sha256"),
                                    "prepared_sha256": prepared_sha or None,
                                    "pipeline_version": document.get("pipeline_version")}
    if manifest.get("camera_object") is not None:
        document["camera"] = manifest.get("camera_object")
    if manifest.get("schema_string") == "autorig.vehicle.multiview/1":
        removal = dict(manifest.get("wheel_removal_object") or {})
        if removal.get("path_string"): removal["image_url"] = f"{capability_base}/{removal['path_string']}"
        document["wheel_removal_object"] = removal or None
        cleanup = dict(manifest.get("body_cleanup_object") or {})
        if cleanup.get("path_string"): cleanup["image_url"] = f"{capability_base}/{cleanup['path_string']}"
        document["body_cleanup_object"] = cleanup or None
        views_object = {}
        for source in manifest.get("views_array") or []:
            view = dict(source); view_id = str(view.get("id_string") or view.get("id") or "")
            reference, depth = dict(view.get("reference_object") or {}), dict(view.get("depth_object") or {})
            if reference.get("path_string"): reference["image_url"] = f"{capability_base}/{reference['path_string']}"
            if depth.get("path_string"): depth["depth_url"] = f"{capability_base}/{depth['path_string']}"
            view["reference_object"], view["depth_object"] = reference, depth
            views_object[view_id] = view
        document["views_object"] = views_object
        fused = dict(manifest.get("fused_voxels_object") or {})
        if fused.get("glb_path_string"): fused["voxel_url"] = f"{capability_base}/{fused['glb_path_string']}"
        if fused.get("json_path_string"): fused["preview_url"] = f"{capability_base}/{fused['json_path_string']}"
        document["fusion_object"] = fused or None
        document["multiview_object"] = {"schema_string": manifest.get("schema_string"),
            "stage_string": manifest.get("stage_string"), "status_string": manifest.get("status_string"),
            "provider_tasks_array": manifest.get("provider_tasks_array") or [],
            "default_view_string": manifest.get("default_view_string"),
            "shared_model_transform_4x4_array": manifest.get("shared_model_transform_4x4_array")}


async def process_one(store: VehicleStore) -> bool:
    job = store.claim()
    if job is None: return False
    try:
        document = store.get(job["run_id"]); request = json.loads(job["request_json"]); run_root = store.run_dir(job["run_id"])
        out = run_root / "pipeline"
        stage = str(job["action"]); receipt: dict[str, Any]
        with _LeaseHeartbeat(store, job):
          if stage in {"analyze", "prepare", "remove_wheels", "project_views", "depth_views",
                       "fuse_voxels", "cluster", "voxelize"}:
            def progress(*args, **kwargs):
                if args and isinstance(args[0], Mapping):
                    kwargs = {**dict(args[0]), **kwargs}; args = args[1:]
                phase = str(kwargs.get("phase") or kwargs.get("stage") or (args[0] if args else stage))
                value = kwargs.get("progress", kwargs.get("percent", args[1] if len(args) > 1 else 0))
                try:
                    numeric = float(value); numeric = numeric * 100 if 0 < numeric <= 1 else numeric
                    store.progress(job, phase, numeric, kwargs)
                except (TypeError, ValueError): store.progress(job, phase, 0, kwargs)
            # Geometry identity is fixed at create time. Stage action options are for the
            # selected provider/tool only and must never silently change cached geometry.
            multiview = stage in {"remove_wheels", "project_views", "depth_views", "fuse_voxels"}
            options = dict(document["options"])
            mt_options = _mt_options(options, document["id"], document.get("analysis") or {}, multiview)
            if stage == "prepare": mt_options["camera_attempt_int"] = int(request.get("camera_attempt_int", 0))
            if stage == "remove_wheels": mt_options["remove_attempt_int"] = int(request.get("remove_attempt_int", 0))
            if stage == "project_views": mt_options["view_attempts_object"] = dict(request.get("view_attempts_object") or {})
            stage_source = _multiview_source(out, document) if multiview else Path(document["source"]["path"])
            stage_out = _prepare_multiview_workspace(out, document, stage_source) if multiview else out
            if stage == "fuse_voxels":
                raw = await _farm_fuse_stage(store, job, request, stage_out, document["id"],
                                             document["source_url"].rsplit("/", 1)[0] + "/multiview")
            else:
                raw = await asyncio.to_thread(_mt_stage, stage_source, stage_out, stage, mt_options, progress)
            receipt = _validate_manifest(stage_out, raw, document["source_sha256"], stage,
                                         int(document["options"].get("resolution") or 192))
            if stage in {"voxelize", "fuse_voxels"}:
                relative = str((receipt.get("fused_voxels_object") or {}).get("glb_path_string") or "")
                preview_path = _preview_geometry_path(out, receipt) if stage == "voxelize" else stage_out / relative
                if stage == "voxelize" or relative:
                    wheel_doc = _validated_wheel_doc(receipt) if receipt.get("status_string") == "complete" else None
                    if wheel_doc is not None: wheel_doc = _persist_wheel_rig(out, wheel_doc)
                    preview = await asyncio.to_thread(_preview_import, receipt, preview_path,
                                                      receipt.get("status_string") == "complete", wheel_doc)
                    receipt["preview_import_object"] = preview
                    if wheel_doc is not None: receipt["persisted_wheel_rig_object"] = wheel_doc
          elif stage == "model":
            manifest = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
            if request.get("mode") == "split":
                manifest = _manifest_with_part_reviews(manifest, document, request)
            receipt = await _model_stage(out, manifest, request, document["id"],
                                         document["source_url"].rsplit("/", 1)[0], int(document["revision"]))
          elif stage == "rig": receipt = await asyncio.to_thread(_rig_stage, out)
          elif stage == "configure":
            receipt = await asyncio.to_thread(_configure_stage, out, request.get("options") or {})
            multiview_manifest = json.loads((out / "multiview" / "manifest.json").read_text(encoding="utf-8"))
            fused_path = out / "multiview" / str((multiview_manifest.get("fused_voxels_object") or {}).get("glb_path_string") or "")
            receipt["preview_import_object"] = await asyncio.to_thread(
                _preview_import, multiview_manifest, fused_path, True, receipt)
          else: raise StageValidationError(f"unsupported vehicle stage: {stage}")
        capability = document["source_url"].rsplit("/", 1)[0]
        public_base = f"{capability}/multiview" if stage in {"remove_wheels", "project_views", "depth_views", "fuse_voxels"} else capability
        artifact_root = out / "multiview" if stage in {"remove_wheels", "project_views", "depth_views", "fuse_voxels"} else out
        _public_from_manifest(document, receipt if stage in {"analyze", "prepare", "remove_wheels", "project_views",
            "depth_views", "fuse_voxels", "cluster", "voxelize"} else {}, public_base, artifact_root)
        outcome = "needs_review" if receipt.get("status_string") == "needs_review" else "completed"
        document.update(status=outcome, stage=stage, progress=100, error=None, revision=int(document["revision"]) + 1)
        if document.get("stages"): document["stages"][-1].update(status="completed", receipt={"stage": stage, "pipeline_version": document["pipeline_version"]})
        if stage == "model": _record_model_artifact(document, request, receipt, capability)
        if stage in {"rig", "configure"}: document["artifacts"]["wheel_rig_url"] = f"{capability}/wheels.json"
        if stage in {"voxelize", "fuse_voxels"}:
            preview = receipt.get("preview_import_object") or {}
            document["viewer_url"] = preview.get("staged_viewer_url") or preview.get("viewer_url")
            document["preview_viewer_url"] = document["viewer_url"]
            document["preview_run_id"] = preview.get("run_id")
            rig = receipt.get("validated_wheel_rig_object") or {}; qa = rig.get("qa_object") or {}
            if receipt.get("status_string") == "complete" and preview.get("drive_ready") is True and _validated_wheel_doc(receipt) is not None:
                document["artifacts"]["wheel_rig_url"] = f"{public_base}/wheel_rig.json"
                document["drive_blocker_string"] = None
            else:
                document["drive_blocker_string"] = str(qa.get("blocker_string") or
                    "validated canonical wheel geometry is pending")
        wheel_rig = receipt.get("persisted_wheel_rig_object") or (receipt if stage == "configure" else None)
        if wheel_rig and wheel_rig.get("revision"):
            document["wheel_rig"] = wheel_rig
            document["wheel_rig_revision"] = wheel_rig["revision"]
            document["artifacts"]["wheel_rig_url"] = f"{capability}/wheels.json"
        if stage == "configure":
            preview = receipt.get("preview_import_object") or {}
            document["viewer_url"] = preview.get("staged_viewer_url") or preview.get("viewer_url")
            document["preview_viewer_url"], document["preview_run_id"] = document["viewer_url"], preview.get("run_id")
        store.finish(job, document, receipt); return True
    except Exception as exc:
        try: store.fail(job, f"{type(exc).__name__}: {exc}")
        except Exception: pass
        return True


async def serve(store: VehicleStore, poll_seconds: float) -> None:
    while True:
        if not await process_one(store): await asyncio.sleep(poll_seconds)


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--once", action="store_true"); parser.add_argument("--poll", type=float, default=2.0); parser.add_argument("--root", type=Path)
    args = parser.parse_args(); store = VehicleStore(args.root) if args.root else VehicleStore()
    with _WorkerSingleton(store.root / ".worker.lock"):
        if args.once: asyncio.run(process_one(store))
        else: asyncio.run(serve(store, max(.2, args.poll)))


if __name__ == "__main__": main()
