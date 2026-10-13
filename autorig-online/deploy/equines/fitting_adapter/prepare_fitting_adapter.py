"""Prepare and bind a fail-closed MT quadruped fitting job.

This helper never invents joint limits.  It validates the real MT rig, records
source hashes, and later selects semantic track anchors from the surface
vertices emitted by animation_fitting/render_actionless_bundle.py.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path


SCHEMA = "autorig.equine-fitting-adapter/1"
ANCHOR_SCHEMA = "autorig.equine-fitting-anchor-map/1"
LEGS = ("LeftFront", "RightFront", "LeftHind", "RightHind")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def read_json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temp, path)


def finite3(value, label: str) -> tuple[float, float, float]:
    if not isinstance(value, list) or len(value) != 3:
        raise ValueError(f"{label} must contain three coordinates")
    out = tuple(float(item) for item in value)
    if not all(math.isfinite(item) for item in out):
        raise ValueError(f"{label} contains a non-finite coordinate")
    return out


def validate_rig(rig: dict) -> dict[str, dict]:
    if rig.get("body_plan") != "quadruped":
        raise ValueError("rig.json body_plan must be quadruped")
    rows = rig.get("bones")
    if not isinstance(rows, list) or not rows:
        raise ValueError("rig.json has no bones")
    bones = {}
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("name"), str):
            raise ValueError("rig.json contains a malformed bone")
        name = row["name"]
        if name in bones:
            raise ValueError(f"duplicate bone {name}")
        finite3(row.get("head"), f"{name}.head")
        finite3(row.get("tail"), f"{name}.tail")
        bones[name] = row
    required = {"Hips", "Head", "Tail"}
    for leg in LEGS:
        required.update((f"{leg}UpLeg", f"{leg}Leg", f"{leg}Foot", f"{leg}Toe"))
    missing = sorted(required.difference(bones))
    if missing:
        raise ValueError(f"rig lacks required quadruped bones: {missing}")
    for leg in LEGS:
        expected = {
            f"{leg}Leg": f"{leg}UpLeg",
            f"{leg}Foot": f"{leg}Leg",
            f"{leg}Toe": f"{leg}Foot",
        }
        for child, parent in expected.items():
            if bones[child].get("parent") != parent:
                raise ValueError(f"unexpected hierarchy: {child}.parent != {parent}")
    return bones


def prepare(args) -> None:
    source, rig_path = Path(args.source).resolve(), Path(args.rig_json).resolve()
    if not source.is_file() or source.suffix.lower() != ".glb":
        raise ValueError("source must be an existing GLB")
    if not rig_path.is_file():
        raise ValueError("rig-json is missing")
    rig = read_json(rig_path)
    bones = validate_rig(rig)
    doc = {
        "schema": SCHEMA,
        "state": "needs_actionless_bundle",
        "species": args.species,
        "task_id": args.task_id,
        "run_id": args.run_id,
        "source": {"path": str(source), "sha256": sha256(source), "bytes": source.stat().st_size},
        "rig_json": {"path": str(rig_path), "sha256": sha256(rig_path)},
        "rig_contract": {
            "body_plan": "quadruped",
            "forward_axis": rig.get("forward_axis"),
            "model_height_units": rig.get("model_height_units"),
            "bone_count": len(bones),
            "required_semantics": ["fore_left.hoof", "fore_right.hoof", "hind_left.hoof",
                                   "hind_right.hoof", "head", "tail"],
            "joint_limits": "required_from_source_constraints; none_invented_by_adapter",
        },
        "actionless_bundle_job": {
            "blender": "4.3.x_required",
            "script": "animation_fitting/render_actionless_bundle.py",
            "argv": ["--input", str(source), "--output-dir", "<bundle-dir>", "--species", args.species,
                     "--source-task-id", args.task_id],
            "note": "Run on a farm host with the pinned animation_fitting tree. The exporter must produce "
                    "immutable_manifest.json; Blender 5.1 is not accepted for this adapter.",
        },
        "limits": {
            "relative_depth": "VDA depth is diagnostic until calibrated to bundle camera_z",
            "single_view": "turns require calibrated depth or synchronized multiview evidence",
        },
    }
    atomic_json(Path(args.output), doc)


def verify_manifest(bundle: Path) -> dict:
    manifest_path = bundle / "immutable_manifest.json"
    manifest = read_json(manifest_path)
    rows = manifest.get("files")
    if not isinstance(rows, list) or not rows:
        raise ValueError("immutable_manifest.json has no files")
    covered = set()
    for row in rows:
        name = row.get("filename")
        if not isinstance(name, str) or Path(name).name != name:
            raise ValueError("immutable manifest contains an unsafe filename")
        path = bundle / name
        if not path.is_file() or path.stat().st_size != int(row.get("bytes", -1)) or sha256(path) != row.get("sha256"):
            raise ValueError(f"immutable bundle member changed: {name}")
        covered.add(name)
    for required in ("fitting_bundle.json", "skeleton.json", "surface_anchors.json", "reference_camera_z.npy"):
        if required not in covered:
            raise ValueError(f"immutable manifest does not cover {required}")
    return manifest


def anchor_rows(bundle: Path) -> dict[str, list[dict]]:
    raw = read_json(bundle / "surface_anchors.json").get("bones")
    if not isinstance(raw, list):
        raise ValueError("surface_anchors.json has no bone groups")
    out = {}
    for group in raw:
        bone, points = group.get("bone"), group.get("points")
        if not isinstance(bone, str) or not isinstance(points, list):
            raise ValueError("malformed surface anchor group")
        clean = []
        for point in points:
            vertex = point.get("vertex_id")
            if not isinstance(vertex, int) or vertex < 0:
                raise ValueError(f"invalid vertex id in {bone}")
            clean.append({**point, "world": finite3(point.get("world"), f"{bone}:{vertex}.world")})
        if clean:
            out[bone] = clean
    return out


def dot3(left, right) -> float:
    return sum(float(left[i]) * float(right[i]) for i in range(3))


def choose_lowest(groups: dict[str, list[dict]], names: tuple[str, ...], up) -> tuple[str, dict]:
    candidates = [(name, point) for name in names for point in groups.get(name, [])]
    if not candidates:
        raise ValueError(f"no surface anchors for {names}")
    return min(candidates, key=lambda item: (dot3(item[1]["world"], up), -float(item[1].get("weight", 0))))


def choose_farthest(groups: dict[str, list[dict]], names: tuple[str, ...], origin) -> tuple[str, dict]:
    candidates = [(name, point) for name in names for point in groups.get(name, [])]
    if not candidates:
        raise ValueError(f"no surface anchors for {names}")
    return max(candidates, key=lambda item: sum((item[1]["world"][axis] - origin[axis]) ** 2 for axis in range(3)))


def bundle_frame(meta: dict) -> dict:
    ground = meta.get("ground_plane") or {}
    up = finite3(ground.get("normal"), "ground_plane.normal")
    length = math.sqrt(dot3(up, up))
    if length <= 1e-9:
        raise ValueError("ground_plane.normal is zero")
    up = tuple(value / length for value in up)
    ground_height = float(ground.get("height"))
    if not math.isfinite(ground_height):
        raise ValueError("ground_plane.height is not finite")
    bounds = meta.get("bounds") or {}
    minimum = finite3(bounds.get("minimum"), "bounds.minimum")
    maximum = finite3(bounds.get("maximum"), "bounds.maximum")
    if any(maximum[i] <= minimum[i] for i in range(3)):
        raise ValueError("bundle bounds are empty or reversed")
    corners = [(x, y, z) for x in (minimum[0], maximum[0]) for y in (minimum[1], maximum[1])
               for z in (minimum[2], maximum[2])]
    projected = [dot3(corner, up) for corner in corners]
    height = max(projected) - min(projected)
    if height <= 0:
        raise ValueError("bundle has no height along its ground normal")
    return {"coordinate_space": "blender_world", "up": up, "ground_height": ground_height,
            "bounds_minimum": minimum, "bounds_maximum": maximum, "height": height,
            "centre": tuple((minimum[i] + maximum[i]) * 0.5 for i in range(3))}


def anchor_id(bone: str, point: dict) -> str:
    return str(point.get("id") or f"{bone}:{point['vertex_id']}")


def bind(args) -> None:
    adapter = read_json(Path(args.adapter))
    if adapter.get("schema") != SCHEMA:
        raise ValueError("unsupported adapter manifest")
    source = Path(adapter["source"]["path"])
    rig_path = Path(adapter["rig_json"]["path"])
    if sha256(source) != adapter["source"]["sha256"] or sha256(rig_path) != adapter["rig_json"]["sha256"]:
        raise ValueError("source GLB or rig.json changed after adapter preparation")
    rig = read_json(rig_path)
    bones = validate_rig(rig)
    bundle = Path(args.bundle).resolve()
    verify_manifest(bundle)
    meta = read_json(bundle / "fitting_bundle.json")
    if (meta.get("source") or {}).get("sha256") != adapter["source"]["sha256"]:
        raise ValueError("actionless bundle source SHA does not match the MT GLB")
    groups = anchor_rows(bundle)
    frame = bundle_frame(meta)
    selected = {}
    semantic_legs = {"fore_left": "LeftFront", "fore_right": "RightFront",
                     "hind_left": "LeftHind", "hind_right": "RightHind"}
    for semantic, leg in semantic_legs.items():
        bone, point = choose_lowest(groups, (f"{leg}Toe", f"{leg}Foot"), frame["up"])
        selected[f"{semantic}.hoof"] = {"track_id": f"{semantic}.hoof", "anchor_id": anchor_id(bone, point),
                                         "bone": bone, "vertex_id": point["vertex_id"], "world": list(point["world"]),
                                         "ground_distance": dot3(point["world"], frame["up"]) - frame["ground_height"]}
    head_candidates = [(name, point) for name in ("Head",) for point in groups.get(name, [])]
    if not head_candidates:
        raise ValueError("no Head surface anchors")
    head_bone, head = max(head_candidates, key=lambda item: dot3(item[1]["world"], frame["up"]))
    tail_names = tuple(name for name in groups if name == "Tail" or name.startswith("Tail"))
    tail_bone, tail = choose_farthest(groups, tail_names, frame["centre"])
    selected["head"] = {"track_id": "head", "anchor_id": anchor_id(head_bone, head), "bone": head_bone,
                        "vertex_id": head["vertex_id"], "world": list(head["world"])}
    selected["tail"] = {"track_id": "tail", "anchor_id": anchor_id(tail_bone, tail), "bone": tail_bone,
                        "vertex_id": tail["vertex_id"], "world": list(tail["world"])}
    ground_distances = [row["ground_distance"] for row in selected.values() if row["track_id"].endswith(".hoof")]
    if max(ground_distances) - min(ground_distances) > 0.08 * frame["height"]:
        raise ValueError("selected hoof anchors do not share a plausible ground band")
    result = {"schema": ANCHOR_SCHEMA, "adapter_sha256": sha256(Path(args.adapter)),
              "bundle_manifest_sha256": sha256(bundle / "immutable_manifest.json"),
              "source_sha256": adapter["source"]["sha256"], "tracks": list(selected.values()),
              "coordinates": {"source_rig_json": "glTF model space, +Y up by contract",
                              "surface_anchors": "Blender matrix_world space",
                              "source_to_bundle_transform": "not declared and not used for anchor selection",
                              "bundle_up": list(frame["up"]), "bundle_ground_height": frame["ground_height"],
                              "bundle_height": frame["height"], "bundle_bounds": {
                                  "minimum": list(frame["bounds_minimum"]), "maximum": list(frame["bounds_maximum"])}},
              "depth_contract": {"camera_z": "bundle_metric_reference", "video_depth": "relative_until_calibrated"}}
    atomic_json(Path(args.output), result)


def parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="command", required=True)
    prep = sub.add_parser("prepare")
    prep.add_argument("--source", required=True)
    prep.add_argument("--rig-json", required=True)
    prep.add_argument("--species", required=True)
    prep.add_argument("--task-id", required=True)
    prep.add_argument("--run-id", required=True)
    prep.add_argument("--output", required=True)
    bindp = sub.add_parser("bind-anchors")
    bindp.add_argument("--adapter", required=True)
    bindp.add_argument("--bundle", required=True)
    bindp.add_argument("--output", required=True)
    return ap


def main() -> int:
    args = parser().parse_args()
    (prepare if args.command == "prepare" else bind)(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
