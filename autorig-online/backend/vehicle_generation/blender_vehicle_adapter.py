"""Launch the deterministic Blender vehicle assembly builder.

The adapter consumes the calibrated receipt produced by the vehicle vision
pipeline.  It never estimates placement from a provider GLB: every part is
mounted with the receipt's common-car-space transform.  Blender writes a new
assembly, leaving all source assets untouched.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import pathlib
import subprocess
import sys
from typing import Any


BLENDER_CANDIDATES = (
    pathlib.Path(r"C:\Program Files\Blender Foundation\Blender 5.2\blender.exe"),
    pathlib.Path(r"C:\Program Files\Blender Foundation\Blender 5.1\blender.exe"),
    pathlib.Path(r"C:\Program Files\Blender Foundation\Blender 4.3\blender.exe"),
)


class VehicleReceiptError(ValueError):
    """The upstream receipt cannot place a deterministic assembly."""


def _vec(value: Any, size: int, field: str) -> list[float]:
    if not isinstance(value, (list, tuple)) or len(value) != size:
        raise VehicleReceiptError(f"{field} must contain {size} numbers")
    try:
        result = [float(v) for v in value]
    except (TypeError, ValueError) as exc:
        raise VehicleReceiptError(f"{field} must contain {size} numbers") from exc
    if not all(math.isfinite(v) for v in result):
        raise VehicleReceiptError(f"{field} must contain only finite numbers")
    return result


def normalized_receipt(raw: dict[str, Any]) -> dict[str, Any]:
    """Validate and reduce the vision receipt to the Blender-owned contract."""
    if not isinstance(raw, dict):
        raise VehicleReceiptError("receipt must be an object")
    coord = raw.get("coordinate_system_object") or raw.get("coordinate_system")
    if not isinstance(coord, dict):
        raise VehicleReceiptError("an explicit calibrated coordinate system is required")
    matrix = coord.get("source_to_common_matrix_4x4_array") or coord.get("source_to_common_matrix")
    if matrix is None:
        raise VehicleReceiptError("source_to_common_matrix is required")
    if isinstance(matrix, (list, tuple)) and len(matrix) == 4 and all(isinstance(row, (list, tuple)) for row in matrix):
        matrix = [value for row in matrix for value in row]
    matrix = _vec(matrix, 16, "source_to_common_matrix")
    if str(coord.get("units_string") or coord.get("units") or "") != "meters":
        raise VehicleReceiptError("common-car-space units must be meters")
    published_gltf = str(coord.get("up_axis_string") or "").lstrip("+") == "Y" and \
        str(coord.get("forward_axis_string") or "") == "+Z"
    if published_gltf:
        # Published glTF [x lateral, y up, +z forward] -> Blender
        # [x lateral, -y forward, z up].  Pre-multiply the source matrix.
        matrix = matrix[0:4] + [-value for value in matrix[8:12]] + matrix[4:8] + matrix[12:16]
    elif not (str(coord.get("up_axis_string") or "").lstrip("+") == "Z" and
              str(coord.get("forward_axis_string") or "") == "-Y"):
        raise VehicleReceiptError("coordinate system must be glTF Y-up/+Z or Blender Z-up/-Y")
    source_sha = str(raw.get("source_sha256_string") or raw.get("source_sha256") or "").lower()
    if len(source_sha) != 64 or any(ch not in "0123456789abcdef" for ch in source_sha):
        raise VehicleReceiptError("source_sha256_string must bind the receipt to its source image")

    body = raw.get("body_object") or raw.get("body") or {}
    bmin, bmax = body.get("bounds_min_xyz_array"), body.get("bounds_max_xyz_array")
    bounds_min = _vec(bmin, 3, "body.bounds_min") if bmin is not None else None
    bounds_max = _vec(bmax, 3, "body.bounds_max") if bmax is not None else None
    if published_gltf and bounds_min and bounds_max:
        bounds_min, bounds_max = ([bounds_min[0], -bounds_max[2], bounds_min[1]],
                                  [bounds_max[0], -bounds_min[2], bounds_max[1]])
    parts = []
    skipped_unregistered = []
    source_parts = raw.get("parts_array") or raw.get("parts") or []
    bbox_by_mask = {
        str(p.get("source_mask_path_string")): p.get("source_bbox_px_array")
        for p in source_parts if isinstance(p, dict) and p.get("source_mask_path_string") and p.get("source_bbox_px_array")
    }
    canonical_wheels = any(
        isinstance(p, dict) and str(p.get("semantic_string") or p.get("semantic") or "").lower() == "wheel"
        and (p.get("mesh_group_id_string") or p.get("mesh_group_id"))
        for p in source_parts
    )
    seen: set[str] = set()
    for index, source in enumerate(source_parts):
        if not isinstance(source, dict):
            raise VehicleReceiptError(f"parts[{index}] must be an object")
        part_id = str(source.get("part_id_string") or source.get("id") or "").strip()
        if not part_id or part_id in seen:
            raise VehicleReceiptError(f"parts[{index}] needs a unique part id")
        seen.add(part_id)
        semantic = str(source.get("semantic_string") or source.get("semantic") or "part").lower()
        if semantic == "wheel" and canonical_wheels and not (source.get("mesh_group_id_string") or source.get("mesh_group_id")):
            skipped_unregistered.append(part_id)
            continue
        transform = source.get("transform_object") or source.get("transform") or {}
        position = source.get("pivot_xyz_array") or transform.get("position_xyz_array") or transform.get("position")
        dimensions = source.get("dimensions_xyz_array") or source.get("dimensions")
        derived_from_bbox = False
        path_points = source.get("path_xyz_array") or source.get("path_points_xyz_array")
        if semantic in {"pipe", "exhaust", "intake"} and path_points:
            if source.get("source_bound_bool") is not True:
                raise VehicleReceiptError(f"{part_id} pipe path is not source-bound")
            path_points = [_vec(point, 3, f"{part_id}.path") for point in path_points]
            if len(path_points) < 2:
                raise VehicleReceiptError(f"{part_id} pipe path needs at least two points")
            radius = float(source.get("cross_section_radius_m_float") or 0)
            if not math.isfinite(radius) or not .002 <= radius <= .5:
                raise VehicleReceiptError(f"{part_id} pipe radius must be 0.002-0.5 m")
            position = position or [0, 0, 0]
            dimensions = dimensions or [radius * 2, radius * 2, radius * 2]
        bbox = source.get("source_bbox_px_array") or bbox_by_mask.get(str(source.get("source_mask_path_string") or ""))
        if semantic == "wheel" and (position is None or dimensions is None) and bbox and bounds_min and bounds_max:
            x0, y0, x1, y1 = _vec(bbox, 4, f"{part_id}.source_bbox")
            m = matrix
            cx, cy = (x0 + x1) * .5, (y0 + y1) * .5
            def project(px, py):
                point = (px, 127.5, py, 1.0)
                return [sum(m[row * 4 + column] * point[column] for column in range(4)) for row in range(3)]
            centre = project(cx, cy)
            horizontal = project(x1, cy)
            vertical = project(cx, y1)
            diameter = max(math.dist(centre[1:], horizontal[1:]), math.dist(centre[1:], vertical[1:]), .05) * 2
            side = str(source.get("side_string") or source.get("side") or "center").lower()
            common_x = bounds_min[0] if side == "left" else bounds_max[0] if side == "right" else 0.0
            position = [common_x, centre[1], centre[2]]
            dimensions = [min(bounds_max[0] - bounds_min[0], diameter * .45), diameter, diameter]
            derived_from_bbox = True
        if position is None or dimensions is None:
            if source.get("asset_path_string") or source.get("asset_path") or source.get("modeled_bool"):
                raise VehicleReceiptError(f"{part_id} needs common-space pivot and dimensions")
            skipped_unregistered.append(part_id)
            continue
        position = _vec(position, 3, f"{part_id}.pivot")
        dimensions = _vec(dimensions, 3, f"{part_id}.dimensions")
        if min(dimensions) <= 0:
            raise VehicleReceiptError(f"{part_id}.dimensions must be positive")
        rotation = source.get("orientation_xyzw_array") or transform.get("rotation_xyzw_array") or [0, 0, 0, 1]
        rotation = _vec(rotation, 4, f"{part_id}.orientation")
        if published_gltf and not derived_from_bbox:
            if any(abs(rotation[i]) > 1e-7 for i in range(3)) or abs(rotation[3] - 1) > 1e-7:
                raise VehicleReceiptError(f"{part_id} non-identity glTF quaternion needs an accepted orientation adapter")
            position = [position[0], -position[2], position[1]]
            dimensions = [dimensions[0], dimensions[2], dimensions[1]]
            path_points = [[point[0], -point[2], point[1]] for point in (path_points or [])]
        mesh_group = str(source.get("mesh_group_id_string") or source.get("mesh_group_id") or "").strip()
        axle = source.get("axle_index_int", source.get("axle_index"))
        if semantic == "wheel":
            if axle is None:
                raise VehicleReceiptError(f"{part_id} needs axle_index")
            axle = int(axle)
            mesh_group = mesh_group or f"wheel.axle_{axle}"
        parts.append({
            "id": part_id, "semantic": semantic, "position": position,
            "dimensions": dimensions, "rotation": rotation,
            "mesh_group": mesh_group or part_id, "axle": axle,
            "side": str(source.get("side_string") or source.get("side") or "center").lower(),
            "asset_path": str(source.get("asset_path_string") or source.get("asset_path") or ""),
            "material_ids": list(source.get("material_id_strings_array") or source.get("material_ids") or []),
            "path": path_points or [], "radius": float(source.get("cross_section_radius_m_float") or 0),
            "shape_gate": dict(source.get("shape_gate_object") or {}),
            "asset_registration_mode": str(source.get("asset_registration_mode_string") or
                ("absolute_common_space" if (source.get("shape_gate_object") or {}).get("shared_lattice_bool") else
                 "local_part_space")),
            "acceptance_receipt": dict(source.get("acceptance_receipt_object") or {}),
        })

    wheels = [p for p in parts if p["semantic"] == "wheel"]
    if wheels:
        by_axle: dict[int, list[dict[str, Any]]] = {}
        for wheel in wheels:
            by_axle.setdefault(int(wheel["axle"]), []).append(wheel)
        for axle, pair in by_axle.items():
            if len(pair) < 2:
                raise VehicleReceiptError(f"axle {axle} needs at least two registered wheels")
            base = pair[0]["dimensions"]
            for wheel in pair[1:]:
                if any(abs(a - b) > max(0.002, abs(a) * 0.01) for a, b in zip(base, wheel["dimensions"])):
                    raise VehicleReceiptError(f"axle {axle} wheel dimensions disagree")
                wheel["mesh_group"] = pair[0]["mesh_group"]

    body_dims = body.get("dimensions_xyz_array") or body.get("dimensions")
    if body_dims is None:
        bmin, bmax = body.get("bounds_min_xyz_array"), body.get("bounds_max_xyz_array")
        if bmin is not None and bmax is not None:
            lo, hi = _vec(bmin, 3, "body.bounds_min"), _vec(bmax, 3, "body.bounds_max")
            body_dims = [hi[i] - lo[i] for i in range(3)]
    body_dims = _vec(body_dims or [4.2, 1.8, 1.4], 3, "body.dimensions")
    if min(body_dims) <= 0:
        raise VehicleReceiptError("body dimensions must be positive")
    mode_raw = str(raw.get("assembly_mode_string") or raw.get("mode_string") or raw.get("mode") or "full_vehicle")
    mode = {"body": "body_only", "split": "selected_parts"}.get(mode_raw, mode_raw)
    if mode not in {"full_vehicle", "body_only", "selected_parts"}:
        raise VehicleReceiptError("unsupported vehicle assembly mode")
    selected_ids = [str(value) for value in (raw.get("selected_part_ids_array") or raw.get("part_ids") or [])]
    if mode == "selected_parts" and not selected_ids:
        raise VehicleReceiptError("selected_parts mode needs part ids")
    return {
        "schema": "autorig.vehicle.blender-assembly/1",
        "run_id": str(raw.get("run_id_string") or raw.get("run_id") or "vehicle"),
        "source_sha256": source_sha,
        "prepared_source_sha256": str(raw.get("prepared_source_sha256_string") or
                                      (raw.get("prepared_source_object") or {}).get("sha256_string") or ""),
        "camera": dict(raw.get("camera_object") or raw.get("canonical_camera_object") or {}),
        "source_to_common_matrix": matrix,
        "source_coordinate_system": dict(coord),
        "body": {
            "dimensions": body_dims,
            "asset_path": str(body.get("asset_path_string") or body.get("asset_path") or
                              ((raw.get("voxels_object") or {}).get("wheel_free_body_object") or {}).get("glb_path_string") or
                              (raw.get("voxels_object") or {}).get("glb_path_string") or ""),
        },
        "parts": parts,
        "materials": raw.get("materials_array") or raw.get("materials") or [],
        "quality": raw.get("quality_object") or {},
        "registration_tolerance_m": float(raw.get("registration_tolerance_m_float") or .02),
        "skipped_unregistered_parts": skipped_unregistered,
        "assembly_mode": mode,
        "selected_part_ids": selected_ids,
        "polygon_budget": int(raw.get("polygon_budget_int") or raw.get("polygon_budget") or 0),
    }


def blender_executable(explicit: str = "") -> pathlib.Path:
    candidates = ([pathlib.Path(explicit)] if explicit else []) + ([pathlib.Path(os.environ["BLENDER_EXE"])] if os.environ.get("BLENDER_EXE") else []) + list(BLENDER_CANDIDATES)
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError("Blender 4.3+ executable was not found")


def build(receipt_path: pathlib.Path, output_dir: pathlib.Path, blender: str = "") -> dict[str, Any]:
    receipt_path = receipt_path.resolve()
    output_dir = output_dir.resolve()
    if not receipt_path.is_file():
        raise FileNotFoundError(receipt_path)
    normalized = normalized_receipt(json.loads(receipt_path.read_text(encoding="utf-8")))
    output_dir.mkdir(parents=True, exist_ok=True)
    normalized_path = output_dir / "vehicle-assembly.input.json"
    normalized_path.write_text(json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8")
    script = pathlib.Path(__file__).with_name("blender_vehicle_build.py")
    command = [str(blender_executable(blender)), "--background", "--factory-startup", "--python", str(script), "--", str(normalized_path), str(output_dir)]
    completed = subprocess.run(command, capture_output=True, text=True, encoding="utf-8", errors="replace")
    report_path = output_dir / "vehicle-assembly.report.json"
    model_path = output_dir / "vehicle-assembly.glb"
    # Blender can exit zero after a Python exception. Artifacts are the contract.
    if not report_path.is_file() or not model_path.is_file() or model_path.stat().st_size < 1024:
        tail = "\n".join((completed.stdout + "\n" + completed.stderr).splitlines()[-25:])
        raise RuntimeError(f"Blender vehicle assembly failed (exit {completed.returncode}):\n{tail}")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if not report.get("ok_bool") and report.get("status_string") != "needs_review":
        raise RuntimeError(str(report.get("error_string") or "Blender vehicle assembly failed"))
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("receipt", type=pathlib.Path)
    parser.add_argument("output_dir", type=pathlib.Path)
    parser.add_argument("--blender", default="")
    args = parser.parse_args(argv)
    report = build(args.receipt, args.output_dir, args.blender)
    print(json.dumps({"ok_bool": True, "model_path_string": report["model_path_string"], "report_path_string": report["report_path_string"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

