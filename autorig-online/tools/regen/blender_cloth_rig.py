"""AutoRig Regen: add hair/cloth bone chains to a character the converter rigged.

Runs inside Blender::

    blender --background --factory-startup --python-exit-code 1 \
        --python blender_cloth_rig.py -- \
        --model rigged.fbx --decomposition DECOMP_DIR --out OUT_DIR \
        [--weights-module backend/regen/weights.py] [--stem NAME]

or in-process wherever the ``bpy`` module is importable::

    blender_cloth_rig.main(["--model", ..., "--decomposition", ..., "--out", ...])

Steps: import the rigged model into a clean scene; map humanoid bones; align
the decomposition (dressed Hunyuan GLB space) onto the rigged mesh with a
similarity ICP; transfer part labels to the rigged vertices; build one bone
chain per decomposition chain under the mapped attach bone; reweight the
moving-part vertices with ``compute_chain_weights``; measure body colliders;
write ``<stem>.autorig-cloth.json``, ``<stem>_cloth.fbx``, ``<stem>_cloth.glb``
and ``cloth_rig_report.json``; re-import both exports and verify them.

Blender exits 0 even when a ``--python`` script raises, so on ANY failure this
script writes ``error.json`` next to the outputs and calls ``sys.exit(1)``;
callers must still verify the artifacts (``cloth_rig_runner.py`` does).
Helper modules are loaded by file path (never through a package).
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import bpy  # noqa: F401  (Blender only)
import numpy as np
from mathutils import Vector
from mathutils.kdtree import KDTree

HERE = Path(__file__).resolve().parent
DEFAULT_WEIGHTS_MODULE = HERE.parents[1] / "backend" / "regen" / "weights.py"
LOG_PREFIX = "[cloth-rig]"


def _load_sibling(module_name: str, filename: str):
    """Load a helper next to this file by path; register before exec (dataclasses)."""
    path = (HERE / filename).resolve()
    existing = sys.modules.get(module_name)
    if existing is not None and Path(getattr(existing, "__file__", "") or "").resolve() == path:
        return existing
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


core = _load_sibling("autorig_regen_cloth_rig_core", "cloth_rig_core.py")
manifest_lib = _load_sibling("autorig_regen_cloth_manifest", "cloth_manifest.py")
ClothRigFailure = core.ClothRigFailure

UP = np.array([0.0, 0.0, 1.0])
TORSO_ORDER = ("hips", "spine", "chest", "upper_chest", "neck", "head")
HEAD_TOP_HELPER = "cloth_head_top"
# Fallback collider radii as a fraction of character height.
DEFAULT_RADIUS_REL = {
    "head": 0.065, "hips": 0.085, "spine": 0.08, "chest": 0.085, "upper_chest": 0.085,
    "upper_leg": 0.045, "lower_leg": 0.035, "upper_arm": 0.03, "lower_arm": 0.025,
}


def log(message: str) -> None:
    print(f"{LOG_PREFIX} {message}", flush=True)


# --------------------------------------------------------------------------- arguments


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    if argv is None:
        argv = sys.argv[sys.argv.index("--") + 1 :] if "--" in sys.argv else sys.argv[1:]
    parser = argparse.ArgumentParser(
        prog="blender_cloth_rig.py", description="Add hair/cloth bone chains to a rigged character."
    )
    parser.add_argument("--model", required=True, help="rigged model (.fbx, .glb, .gltf or .blend)")
    parser.add_argument("--decomposition", required=True, help="directory with decomposition.json")
    parser.add_argument("--out", required=True, help="output directory")
    parser.add_argument("--weights-module", default=None,
                        help="path of weights.py (default: env REGEN_WEIGHTS_MODULE or backend/regen/weights.py)")
    parser.add_argument("--stem", default=None, help="artifact stem (default: model file stem)")
    parser.add_argument("--max-residual", type=float, default=0.02,
                        help="alignment residual limit as a fraction of character height (default 0.02)")
    parser.add_argument("--max-rotation", type=float, default=20.0,
                        help="largest rotation (degrees) the alignment may apply beyond its initial yaw (default 20)")
    parser.add_argument("--no-self-check", action="store_true", help="skip the re-import verification")

    def fail(message: str) -> None:
        raise ClothRigFailure("arguments", f"invalid arguments: {message}")

    parser.error = fail  # type: ignore[method-assign]
    return parser.parse_args(argv)


# --------------------------------------------------------------------------- blender helpers


def _call_operator(operator, **kwargs) -> None:
    """Call an operator with only the properties this Blender version knows."""
    known = set(operator.get_rna_type().properties.keys())
    accepted = {k: v for k, v in kwargs.items() if k in known}
    result = operator(**accepted)
    if "FINISHED" not in result:
        raise ClothRigFailure("blender", f"{operator.idname_py()} returned {sorted(result)}")


def _ensure_io_addons() -> None:
    import addon_utils

    for module in ("io_scene_fbx", "io_scene_gltf2"):
        try:
            addon_utils.enable(module, default_set=False)
        except Exception:  # pragma: no cover - reported by the import itself
            pass


def reset_scene() -> None:
    if bpy.context.object is not None and bpy.context.object.mode != "OBJECT":
        bpy.ops.object.mode_set(mode="OBJECT")
    bpy.ops.wm.read_factory_settings(use_empty=True)
    _ensure_io_addons()


def import_model(path: Path) -> None:
    ext = path.suffix.lower()
    if ext == ".blend":
        bpy.ops.wm.open_mainfile(filepath=str(path), load_ui=False)
        _ensure_io_addons()
    else:
        reset_scene()
        if ext == ".fbx":
            _call_operator(bpy.ops.import_scene.fbx, filepath=str(path), use_anim=True,
                           ignore_leaf_bones=False, automatic_bone_orientation=False)
        elif ext in (".glb", ".gltf"):
            _call_operator(bpy.ops.import_scene.gltf, filepath=str(path), disable_bone_shape=True)
        else:
            raise ClothRigFailure("import", f"unsupported model format {ext!r} (use .fbx, .glb, .gltf or .blend)")
    active = bpy.context.view_layer.objects.active
    if active is not None and active.mode != "OBJECT":
        bpy.ops.object.mode_set(mode="OBJECT")


def _is_skinned_by(obj, arm) -> bool:
    if obj.type != "MESH":
        return False
    if any(m.type == "ARMATURE" and m.object == arm for m in obj.modifiers):
        return True
    return obj.parent == arm and obj.parent_type == "ARMATURE"


def _descends_from(obj, arm) -> bool:
    cursor = obj.parent
    while cursor is not None:
        if cursor == arm:
            return True
        cursor = cursor.parent
    return False


def _foreach(collection, attr: str, width: int, integer: bool = False) -> np.ndarray:
    """Bulk read through the buffer protocol in Blender's native types
    (float32/int32; other dtypes silently take the slow per-item path),
    widened to float64/int64."""
    buf = np.empty(len(collection) * width, dtype=np.int32 if integer else np.float32)
    if len(collection):
        collection.foreach_get(attr, buf)
    buf = buf.astype(np.int64 if integer else np.float64)
    return buf.reshape(-1, width) if width > 1 else buf


def _matrix_np(matrix) -> np.ndarray:
    return np.array([list(row) for row in matrix], dtype=np.float64)


def _transform(matrix: np.ndarray, points: np.ndarray) -> np.ndarray:
    return points @ matrix[:3, :3].T + matrix[:3, 3]


def kd_nearest_factory(points: np.ndarray):
    """Nearest-neighbour queries over ``points`` with ``mathutils.kdtree``."""
    pts = np.ascontiguousarray(points, dtype=np.float64)
    tree = KDTree(len(pts))
    for index, co in enumerate(pts.tolist()):
        tree.insert(co, index)
    tree.balance()

    def query(q: np.ndarray):
        q = np.asarray(q, dtype=np.float64)
        idx = np.fromiter((tree.find(co)[1] for co in q.tolist()), dtype=np.int64, count=len(q))
        nearest = pts[idx]
        return nearest, np.linalg.norm(nearest - q, axis=1)

    query.indices = lambda q: np.fromiter(  # type: ignore[attr-defined]
        (tree.find(co)[1] for co in np.asarray(q, dtype=np.float64).tolist()), dtype=np.int64, count=len(q)
    )
    return query


def surface_samples(world: np.ndarray, tris: np.ndarray, count: int, rng: np.random.Generator) -> np.ndarray:
    if count <= 0 or len(tris) == 0:
        return np.empty((0, 3))
    a, b, c = world[tris[:, 0]], world[tris[:, 1]], world[tris[:, 2]]
    area = 0.5 * np.linalg.norm(np.cross(b - a, c - a), axis=1)
    total = float(area.sum())
    if total <= 0:
        return np.empty((0, 3))
    pick = rng.choice(len(tris), size=count, p=area / total)
    r1 = np.sqrt(rng.random(count))[:, None]
    r2 = rng.random(count)[:, None]
    return (1 - r1) * a[pick] + r1 * (1 - r2) * b[pick] + r1 * r2 * c[pick]


@dataclass
class MeshData:
    obj: Any
    name: str
    skinned: bool
    world: np.ndarray
    tris: np.ndarray
    edges: np.ndarray
    dominant: np.ndarray  # bone name index per vertex into ``bone_names``, -1 = none
    labels: np.ndarray | None = None
    parts: np.ndarray | None = None
    transfer_distance: np.ndarray | None = None


@dataclass
class GroupPlan:
    source_name: str
    name: str
    kind: str
    part_index: int
    part_name: str
    attach_semantic: str
    attach_bone: str
    attach_slot: str | None
    connection: str
    preset: str
    chains_world: list
    bone_names: list
    order: list
    warnings: list = field(default_factory=list)
    weighted_vertices: int = 0


# --------------------------------------------------------------------------- the step


class ClothRig:
    def __init__(self, args: argparse.Namespace, paths: dict, report: dict):
        self.args = args
        self.paths = paths
        self.report = report
        self.warnings: list[str] = report["warnings"]
        self.timings: dict[str, float] = report["timings"]
        self.rng = np.random.default_rng(0)

    # ----------------------------------------------------------------- driver
    def run(self) -> None:
        self._stage("load_inputs", self.load_inputs)
        self._stage("import", self.import_rig)
        self._stage("alignment", self.align)
        self._stage("bone_mapping", self.map_bones)
        self._stage("label_transfer", self.transfer_labels)
        self._stage("plan_groups", self.plan_groups)
        self._stage("colliders", self.measure_colliders)
        self._stage("chain_bones", self.create_bones)
        self._stage("weights", self.write_weights)
        self._stage("manifest", self.write_manifest)
        self._stage("export", self.export)
        if self.args.no_self_check:
            self.report["self_check"] = {"skipped": True}
        else:
            self._stage("self_check", self.self_check)

    def _stage(self, name: str, func) -> None:
        log(f"{name} ...")
        started = time.perf_counter()
        try:
            func()
        except ClothRigFailure:
            raise
        except Exception as exc:
            raise ClothRigFailure(name, f"{name} failed: {exc.__class__.__name__}: {exc}") from exc
        finally:
            self.timings[name] = round(time.perf_counter() - started, 3)

    # ----------------------------------------------------------------- inputs
    def load_inputs(self) -> None:
        args = self.args
        self.model_path = Path(args.model).resolve()
        if not self.model_path.is_file():
            raise ClothRigFailure("load_inputs", f"model not found: {self.model_path}")
        self.decomp = core.load_decomposition(args.decomposition)
        self.warnings.extend(self.decomp.warnings)
        weights_path = args.weights_module or os.environ.get("REGEN_WEIGHTS_MODULE") or str(DEFAULT_WEIGHTS_MODULE)
        self.weights_path = Path(weights_path).resolve()
        if not self.weights_path.is_file():
            raise ClothRigFailure(
                "load_inputs",
                f"weights module not found: {self.weights_path} (pass --weights-module or set REGEN_WEIGHTS_MODULE)",
            )
        self.weights_module = core.load_weights_module(self.weights_path)
        values = self.decomp.values
        self.outer_label = values.index("outer") if "outer" in values else None
        self.report["inputs"] = {
            "model": str(self.model_path),
            "decomposition": str(Path(args.decomposition).resolve()),
            "weights_module": str(self.weights_path),
            "weights_api_version": getattr(self.weights_module, "WEIGHTS_API_VERSION", None),
            "labels": {"values": values, "points": int(len(self.decomp.positions))},
            "parts": len(self.decomp.parts),
            "groups": len(self.decomp.groups),
        }

    # ----------------------------------------------------------------- import
    def import_rig(self) -> None:
        import_model(self.model_path)
        scene = bpy.context.scene
        armatures = [o for o in scene.objects if o.type == "ARMATURE"]
        if not armatures:
            raise ClothRigFailure("import", f"no armature in {self.model_path.name}")
        best = None
        for arm in armatures:
            skinned = [o for o in scene.objects if _is_skinned_by(o, arm)]
            score = (sum(len(o.data.vertices) for o in skinned), len(arm.data.bones))
            if best is None or score > best[0]:
                best = (score, arm, skinned)
        _, arm, skinned = best
        if not skinned:
            raise ClothRigFailure("import", f"armature {arm.name!r} has no skinned mesh")
        if len(armatures) > 1:
            self.warnings.append(
                f"{len(armatures)} armatures found; using {arm.name!r} and ignoring the others"
            )
        attached = [o for o in scene.objects if o.type == "MESH" and o not in skinned and _descends_from(o, arm)]
        ignored = [o.name for o in scene.objects if o.type == "MESH" and o not in skinned and o not in attached]
        if ignored:
            self.warnings.append(f"meshes not attached to the armature are not exported: {ignored[:10]}")
        self.arm = arm
        self.bone_names = [b.name for b in arm.data.bones]
        self.bone_index = {n: i for i, n in enumerate(self.bone_names)}
        self.has_animation = len(bpy.data.actions) > 0
        self.action_names = [a.name for a in bpy.data.actions]

        self.original_pose_position = arm.data.pose_position
        arm.data.pose_position = "REST"
        bpy.context.view_layer.update()
        depsgraph = bpy.context.evaluated_depsgraph_get()
        self.meshes: list[MeshData] = []
        weight_counts = np.zeros(len(self.bone_names), dtype=np.int64)
        for obj in skinned + attached:
            mesh = obj.data
            base = _foreach(mesh.vertices, "co", 3)
            evaluated = obj.evaluated_get(depsgraph)
            co = base
            try:
                temp = evaluated.to_mesh()
                if len(temp.vertices) == len(mesh.vertices):
                    co = _foreach(temp.vertices, "co", 3)
                else:
                    self.warnings.append(f"{obj.name}: modifiers change the vertex count; using base coordinates")
            finally:
                evaluated.to_mesh_clear()
            world = _transform(_matrix_np(obj.matrix_world), co)
            mesh.calc_loop_triangles()
            tris = _foreach(mesh.loop_triangles, "vertices", 3, integer=True)
            edges = _foreach(mesh.edges, "vertices", 2, integer=True)
            dominant = np.full(len(mesh.vertices), -1, dtype=np.int64)
            skinned_flag = obj in skinned
            if skinned_flag:
                group_to_bone = {g.index: self.bone_index.get(g.name, -1) for g in obj.vertex_groups}
                best_w = np.zeros(len(mesh.vertices))
                for vertex in mesh.vertices:
                    for g in vertex.groups:
                        bone = group_to_bone.get(g.group, -1)
                        if bone < 0 or g.weight <= 0.0:
                            continue
                        weight_counts[bone] += 1
                        if g.weight > best_w[vertex.index]:
                            best_w[vertex.index] = g.weight
                            dominant[vertex.index] = bone
            self.meshes.append(MeshData(obj=obj, name=obj.name, skinned=skinned_flag, world=world,
                                        tris=tris, edges=edges, dominant=dominant))
        self.weight_counts = weight_counts
        all_world = np.concatenate([m.world for m in self.meshes])
        self.height = float(np.ptp(all_world[:, 2]))
        if not self.height > 0:
            raise ClothRigFailure("import", "rigged meshes have zero height")
        matrix = _matrix_np(arm.matrix_world)
        self.bone_head = {}
        self.bone_tail = {}
        for bone in arm.data.bones:
            self.bone_head[bone.name] = _transform(matrix, np.array([list(bone.head_local)]))[0]
            self.bone_tail[bone.name] = _transform(matrix, np.array([list(bone.tail_local)]))[0]
        self.bone_infos = [
            core.BoneInfo(
                name=bone.name, parent=bone.parent.name if bone.parent else None,
                head=self.bone_head[bone.name], tail=self.bone_tail[bone.name], use_deform=bool(bone.use_deform),
                weight_count=int(weight_counts[self.bone_index[bone.name]]),
            )
            for bone in arm.data.bones
        ]
        self.report["armature"] = {
            "name": arm.name,
            "bones": len(self.bone_names),
            "skinned_meshes": [o.name for o in skinned],
            "attached_meshes": [o.name for o in attached],
            "vertices": int(sum(len(m.world) for m in self.meshes)),
            "height": self.height,
            "actions": self.action_names[:50],
            "blender_version": bpy.app.version_string,
        }

    # ----------------------------------------------------------------- alignment
    def align(self) -> None:
        dst_parts = []
        samples = []
        total_area = []
        for mesh in self.meshes:
            dst_parts.append(mesh.world)
            if len(mesh.tris):
                a, b, c = (mesh.world[mesh.tris[:, i]] for i in range(3))
                total_area.append(float(0.5 * np.linalg.norm(np.cross(b - a, c - a), axis=1).sum()))
            else:
                total_area.append(0.0)
        area_sum = sum(total_area) or 1.0
        for mesh, area in zip(self.meshes, total_area):
            count = int(round(40000 * area / area_sum))
            samples.append(surface_samples(mesh.world, mesh.tris, count, self.rng))
        vertices = core.subsample(np.concatenate(dst_parts), 60000, self.rng)
        dst = np.concatenate([vertices] + samples)
        src = core.gltf_to_blender(self.decomp.positions)
        # Front/back check: the rig's left from its named L/R bones, the
        # decomposition's from its shoulder landmarks (when it trusts its facing).
        named = core.map_humanoid(self.bone_infos, up=UP)
        rig_left = named.left_axis if named.left_axis_source == "names" else None
        decomp_left = self._landmark_left()
        left_check = (decomp_left, rig_left) if rig_left is not None and decomp_left is not None else None
        result = core.align_similarity(
            src, dst, nearest_factory=kd_nearest_factory, height=self.height,
            max_residual_rel=self.args.max_residual, max_rotation_deg=self.args.max_rotation,
            left_check=left_check,
        )
        self.alignment = result
        self.warnings.extend(result.warnings)
        self.report["alignment"] = result.to_report()
        log(
            f"alignment scale={result.icp.scale:.4f} rot={result.icp.rotation_angle_deg():.2f}deg "
            f"residual={100 * result.residual_rel:.3f}% of height"
        )

    def to_world(self, points_gltf) -> np.ndarray:
        icp = self.alignment.icp
        return core.apply_similarity(icp.scale, icp.rotation, icp.translation, core.gltf_to_blender(points_gltf))

    def _landmark_left(self) -> np.ndarray | None:
        """The character's left in the decomposition (Blender axes), from its shoulder landmarks."""
        landmarks = self.decomp.landmarks
        if landmarks.get("facing_confident") is False:
            return None
        left, right = landmarks.get("shoulder_l"), landmarks.get("shoulder_r")
        if not (core._finite_vec3(left) and core._finite_vec3(right)):
            return None
        vec = core.gltf_to_blender(np.asarray(left, float) - np.asarray(right, float))
        return vec if np.linalg.norm(vec) > 1e-9 else None

    # ----------------------------------------------------------------- bone mapping
    def map_bones(self) -> None:
        hint = self._landmark_left()
        if hint is not None:
            hint = self.alignment.icp.rotation @ hint
        self.mapping = core.map_humanoid(self.bone_infos, up=UP, left_hint=hint)
        self.warnings.extend(self.mapping.warnings)
        self.left_axis = self.mapping.left_axis
        self.forward_axis = np.cross(self.left_axis, UP)
        self.report["bone_mapping"] = self.mapping.to_report()
        found = self.mapping.to_report()["slots"]
        log("bone mapping: " + ", ".join(f"{k}={v['bone']}({v['method'][0]})" for k, v in found.items()))
        if "hips" not in self.mapping.slots:
            self.warnings.append("no hips bone found; groups attach to the nearest bone instead")

    # ----------------------------------------------------------------- labels
    def transfer_labels(self) -> None:
        dressed_world = self.to_world(self.decomp.positions)
        nearest = kd_nearest_factory(dressed_world)
        gate = 0.03 * self.height
        counts = np.zeros(len(self.decomp.values), dtype=np.int64)
        far_total = 0
        all_dist = []
        for mesh in self.meshes:
            idx = nearest.indices(mesh.world)
            dist = np.linalg.norm(dressed_world[idx] - mesh.world, axis=1)
            labels = self.decomp.labels[idx].copy()
            parts = self.decomp.part_index[idx].copy()
            far = dist > gate
            parts[far] = -1
            far_total += int(np.count_nonzero(far))
            mesh.labels, mesh.parts, mesh.transfer_distance = labels, parts, dist
            counts += np.bincount(labels, minlength=len(self.decomp.values))
            all_dist.append(dist)
        dist = np.concatenate(all_dist)
        total = len(dist)
        if far_total > 0.01 * total:
            self.warnings.append(
                f"{far_total} of {total} rigged vertices are farther than 3% of height from any "
                "decomposition point; they keep their weights"
            )
        part_counts = {}
        for index, part in enumerate(self.decomp.parts):
            part_counts[str(part.get("name", index))] = int(sum(
                np.count_nonzero(m.parts == index) for m in self.meshes if m.skinned
            ))
        self.report["label_transfer"] = {
            "method": "nearest neighbour (mathutils.kdtree) in the aligned space",
            "vertices": int(total),
            "label_counts": {v: int(c) for v, c in zip(self.decomp.values, counts)},
            "part_vertex_counts": part_counts,
            "median_distance_rel": float(np.median(dist) / self.height),
            "p95_distance_rel": float(np.percentile(dist, 95) / self.height),
            "beyond_gate": far_total,
            "gate_rel": 0.03,
        }

    # ----------------------------------------------------------------- groups
    def part_attach_origin(self, part_index: int) -> tuple[np.ndarray | None, str, int]:
        """Centroid of the part's vertices that border the body (non-moving vertices)."""
        for strict in (True, False):
            if strict and self.outer_label is None:
                continue
            points = []
            for mesh in self.meshes:
                if not mesh.skinned or len(mesh.edges) == 0:
                    continue
                body = mesh.parts < 0
                if strict:
                    body &= mesh.labels == self.outer_label
                a, b = mesh.edges[:, 0], mesh.edges[:, 1]
                inside = mesh.parts == part_index
                border = np.unique(np.concatenate([a[inside[a] & body[b]], b[inside[b] & body[a]]]))
                if len(border):
                    points.append(mesh.world[border])
            if points and sum(len(p) for p in points) >= 3:
                pts = np.concatenate(points)
                return pts.mean(axis=0), "edge_border_outer" if strict else "edge_border_body", len(pts)
        # No shared edge with the body (a separate shell): use the decomposition's
        # own attach origin, measured on the dressed mesh, when it has one.
        part = self.decomp.parts[part_index]
        candidates = [part.get("attach_origin")] + [
            g.get("attach_origin") for g in self.decomp.groups if g.get("part") == part.get("name")
        ]
        for origin in candidates:
            if core._finite_vec3(origin) and np.any(np.asarray(origin, dtype=float) != 0):
                return self.to_world(np.asarray(origin, dtype=float)[None, :])[0], "decomposition", 0
        part_pts = [m.world[m.parts == part_index] for m in self.meshes if m.skinned]
        body_pts = [m.world[m.parts < 0] for m in self.meshes if m.skinned]
        part_pts = np.concatenate(part_pts) if part_pts else np.empty((0, 3))
        body_pts = np.concatenate(body_pts) if body_pts else np.empty((0, 3))
        if len(part_pts) == 0 or len(body_pts) == 0:
            return None, "none", 0
        body_sample = core.subsample(body_pts, 50000, self.rng)
        _, dist = kd_nearest_factory(body_sample)(part_pts)
        cutoff = np.percentile(dist, 5)
        near = part_pts[dist <= cutoff]
        if len(near) < 3:
            near = part_pts[np.argsort(dist)[:3]]
        return near.mean(axis=0), "proximity", len(near)

    def _sanitize_joints(self, joints: np.ndarray) -> tuple[np.ndarray, bool]:
        min_len = 1e-3 * self.height
        out = [joints[0].copy()]
        fixed = False
        direction = -UP
        for point in joints[1:]:
            seg = point - out[-1]
            length = float(np.linalg.norm(seg))
            if length < min_len:
                point = out[-1] + direction * min_len
                fixed = True
            else:
                direction = seg / length
            out.append(np.asarray(point, dtype=np.float64))
        return np.array(out), fixed

    def _nearest_bone(self, point: np.ndarray) -> str:
        candidates = [n for n in self.bone_names if self.arm.data.bones[n].use_deform] or self.bone_names
        return min(candidates, key=lambda n: float(np.linalg.norm(self.bone_head[n] - point)))

    def plan_groups(self) -> None:
        decomp = self.decomp
        self.part_origins: dict[int, dict] = {}
        pending = []
        skipped = []
        for g_index, group in enumerate(decomp.groups):
            label = str(group.get("name") or f"group{g_index}")
            part_ref = group.get("part")
            found = decomp.part_by_name(part_ref) if part_ref is not None else None
            if found is None:
                skipped.append({"group": label, "reason": f"unknown part {part_ref!r}"})
                continue
            part_index, part = found
            if part.get("rigid"):
                skipped.append({"group": label, "reason": "part is rigid"})
                continue
            raw_chains = [c.get("joints") or [] for c in group.get("chains") or []]
            chains = [np.asarray(j, dtype=np.float64) for j in raw_chains if len(j) >= 2]
            if len(chains) < len(raw_chains):
                self.warnings.append(f"group {label!r}: {len(raw_chains) - len(chains)} chain(s) with < 2 joints skipped")
            if not chains:
                skipped.append({"group": label, "reason": "no chains"})
                continue
            vertex_total = sum(int(np.count_nonzero(m.parts == part_index)) for m in self.meshes if m.skinned)
            if vertex_total == 0:
                skipped.append({"group": label, "reason": f"no rigged vertex carries part {part.get('name')!r}"})
                continue
            kind = str(group.get("kind") or part.get("kind") or "cloth").lower()
            if kind not in manifest_lib.KINDS:
                kind = {"clothing": "cloth", "garment": "cloth"}.get(kind, "accessory")
            connection = str(group.get("connection") or "none").lower()
            if connection not in manifest_lib.CONNECTIONS:
                self.warnings.append(f"group {label!r}: connection {connection!r} unknown, using 'none'")
                connection = "none"
            attach_semantic = str(group.get("attach") or part.get("attach") or "hips")
            chains_world = []
            for chain in chains:
                fixed_chain, fixed = self._sanitize_joints(self.to_world(chain))
                if fixed:
                    self.warnings.append(f"group {label!r}: degenerate chain segment lengthened")
                chains_world.append(fixed_chain)
            attach_bone, attach_slot = core.resolve_attach(attach_semantic, self.mapping)
            group_warnings = []
            if connection in ("open", "loop") and len({len(c) for c in chains_world}) > 1:
                group_warnings.append("chains differ in length; runtimes link them only up to the shortest")
            roots = np.array([c[0] for c in chains_world])
            if attach_bone is None:
                attach_bone = self._nearest_bone(roots.mean(axis=0))
                group_warnings.append(f"attach {attach_semantic!r} unmapped; using nearest bone {attach_bone!r}")
            elif attach_slot != attach_semantic:
                group_warnings.append(f"attach {attach_semantic!r} unmapped; using {attach_slot!r}")
            if part_index not in self.part_origins:
                origin, method, count = self.part_attach_origin(part_index)
                self.part_origins[part_index] = {"origin": origin, "method": method, "count": count}
            order = list(range(len(chains_world)))
            if connection in ("open", "loop") and len(chains_world) > 1:
                if attach_slot in (None, "hips", "spine", "chest", "upper_chest", "neck", "head"):
                    axis = UP
                else:  # a limb: order around the limb's own direction
                    head = self.bone_head[attach_bone]
                    tip = self._slot_segment_end(attach_slot)
                    axis = tip - head if np.linalg.norm(tip - head) > 1e-9 else UP
                centre = roots.mean(axis=0) if connection == "loop" else self.bone_head[attach_bone]
                order = core.order_chains_angular(roots, connection, centre, axis, self.forward_axis)
                chains_world = [chains_world[i] for i in order]
            preset = str(group.get("preset") or manifest_lib.default_preset_name(kind, connection))
            pending.append(dict(
                source_name=label, kind=kind, part_index=part_index, part_name=str(part.get("name")),
                attach_semantic=attach_semantic, attach_bone=attach_bone, attach_slot=attach_slot,
                connection=connection, preset=preset, chains_world=chains_world, order=order,
                warnings=group_warnings,
            ))
        existing = list(self.bone_names) + [HEAD_TOP_HELPER]
        plans = core.plan_group_names(
            [p["source_name"] for p in pending],
            [[len(c) for c in p["chains_world"]] for p in pending],
            existing,
        )
        self.groups: list[GroupPlan] = []
        for data, plan in zip(pending, plans):
            self.groups.append(GroupPlan(name=plan["name"], bone_names=plan["bones"], **data))
            self.warnings.extend(f"group {plan['name']!r}: {w}" for w in data["warnings"])
        self.report["skipped_groups"] = skipped
        for item in skipped:
            self.warnings.append(f"group {item['group']!r} skipped: {item['reason']}")
        if not self.groups:
            self.warnings.append("no simulated group: the manifest lists colliders only")

    # ----------------------------------------------------------------- colliders
    def _slot_segment_end(self, slot: str) -> np.ndarray:
        slots = self.mapping.slots
        name = slots[slot]
        if slot in TORSO_ORDER:
            later = [s for s in TORSO_ORDER[TORSO_ORDER.index(slot) + 1 :] if s in slots]
            if later:
                return self.bone_head[slots[later[0]]]
        nxt = {"upper_leg": "lower_leg", "lower_leg": "foot", "foot": "toe", "shoulder": "upper_arm",
               "upper_arm": "lower_arm", "lower_arm": "hand"}
        if slot[-2:] in ("_l", "_r"):
            kind, side = slot[:-2], slot[-1]
            follow = nxt.get(kind)
            if follow and f"{follow}_{side}" in slots:
                return self.bone_head[slots[f"{follow}_{side}"]]
        return self.bone_tail[name]

    def measure_colliders(self) -> None:
        slots = self.mapping.slots
        slot_of_bone = {}
        for slot, name in slots.items():
            slot_of_bone.setdefault(name, slot)
        segments = {slot: (self.bone_head[slots[slot]], self._slot_segment_end(slot)) for slot in slots}
        owner = np.full(len(self.bone_names), -1, dtype=np.int64)
        slot_list = list(segments)
        for index, name in enumerate(self.bone_names):
            if name in slot_of_bone:
                owner[index] = slot_list.index(slot_of_bone[name])
            elif slot_list:
                mid = (self.bone_head[name] + self.bone_tail[name]) / 2.0
                dists = [core.point_segment_distance(mid[None, :], *segments[s])[0][0] for s in slot_list]
                owner[index] = int(np.argmin(dists))
        owned: dict[str, list] = {s: [] for s in slot_list}
        for mesh in self.meshes:
            if not mesh.skinned:
                continue
            keep = (mesh.parts < 0) & (mesh.dominant >= 0)
            slot_idx = owner[mesh.dominant[keep]]
            pts = mesh.world[keep]
            for s_index, slot in enumerate(slot_list):
                sel = slot_idx == s_index
                if np.any(sel):
                    owned[slot].append(pts[sel])
        owned_pts = {s: (np.concatenate(v) if v else np.empty((0, 3))) for s, v in owned.items()}
        h = self.height
        min_r, max_r = 0.005 * h, 0.25 * h

        def default_radius(slot: str) -> float:
            key = slot[:-2] if slot[-2:] in ("_l", "_r") else slot
            return DEFAULT_RADIUS_REL.get(key, 0.04) * h

        def torso_next(slot: str) -> str | None:
            later = [s for s in TORSO_ORDER[TORSO_ORDER.index(slot) + 1 :] if s in slots]
            return later[0] if later else None

        specs = []
        for slot in ("hips", "spine", "chest", "upper_chest"):
            if slot in slots and torso_next(slot):
                specs.append((slot, slot, torso_next(slot)))
        for side in ("l", "r"):
            for name, a, b in (("thigh", "upper_leg", "lower_leg"), ("calf", "lower_leg", "foot"),
                               ("upperarm", "upper_arm", "lower_arm"), ("forearm", "lower_arm", "hand")):
                if f"{a}_{side}" in slots and f"{b}_{side}" in slots:
                    specs.append((f"{name}_{side}", f"{a}_{side}", f"{b}_{side}"))
        colliders = []
        measured = {}
        for name, slot_a, slot_b in specs:
            a = self.bone_head[slots[slot_a]]
            b = self.bone_head[slots[slot_b]]
            pts = owned_pts.get(slot_a, np.empty((0, 3)))
            if len(pts) >= 8:
                r_a, r_b, count = core.capsule_radii(pts, a, b, percentile=60.0)
                method = "percentile60"
            else:
                r_a = r_b = default_radius(slot_a)
                count = len(pts)
                method = "default"
                self.warnings.append(f"collider {name}: only {count} vertices dominated by {slots[slot_a]!r}; default radius")
            r_a = float(np.clip(r_a, min_r, max_r))
            r_b = float(np.clip(r_b, min_r, max_r))
            colliders.append({
                "name": name, "tag": manifest_lib.BODY_TAG, "shape": "capsule",
                "bone": slots[slot_a], "to_bone": slots[slot_b], "t": 0.0,
                "radius": r_a, "radius_to": 0.0 if abs(r_a - r_b) < 1e-6 else r_b,
            })
            measured[name] = {"vertices": int(count), "method": method}
        self.head_helper = None
        if "head" in slots:
            head_bone = slots["head"]
            head = self.bone_head[head_bone]
            pts = owned_pts.get("head", np.empty((0, 3)))
            if len(pts) >= 8:
                centre = pts.mean(axis=0)
                radius = core.robust_radius(np.linalg.norm(pts - centre, axis=1), 60.0)
                top_extent = float(np.max((pts - head) @ UP))
                method = "percentile60"
            else:
                radius = default_radius("head")
                centre = head + UP * radius
                top_extent = 2.0 * radius
                method = "default"
                self.warnings.append("collider head: too few head vertices; default radius")
            radius = float(np.clip(radius, 0.02 * h, 0.2 * h))
            top = None  # an existing child straight above the head (HeadTop_End), not an eye or jaw bone
            for bone in self.arm.data.bones[head_bone].children:
                rel = self.bone_head[bone.name] - head
                offset = float(rel @ UP)
                upright = offset > 0.85 * float(np.linalg.norm(rel))
                if offset > 0.03 * h and upright and (top is None or offset > top[1]):
                    top = (bone.name, offset)
            if top is None:
                top_extent = max(top_extent, 1e-3 * h)
                self.head_helper = {"name": HEAD_TOP_HELPER, "parent": head_bone,
                                    "head": head + UP * top_extent, "tail": head + UP * (top_extent + 0.05 * h)}
                to_bone, end = HEAD_TOP_HELPER, self.head_helper["head"]
            else:
                to_bone, end = top[0], self.bone_head[top[0]]
            seg = end - head
            t = float(np.clip((centre - head) @ seg / max(float(seg @ seg), 1e-12), 0.0, 1.0))
            colliders.insert(0, {
                "name": "head", "tag": manifest_lib.BODY_TAG, "shape": "sphere", "bone": head_bone,
                "to_bone": to_bone, "t": t, "radius": radius, "radius_to": 0.0,
            })
            measured["head"] = {"vertices": int(len(pts)), "method": method,
                                "helper_bone": self.head_helper is not None}
        self.colliders = colliders
        self.report["colliders"] = {"count": len(colliders), "measured": measured}

    # ----------------------------------------------------------------- bones
    def create_bones(self) -> None:
        arm = self.arm
        for obj in bpy.context.view_layer.objects:
            obj.select_set(False)
        arm.hide_set(False)
        arm.hide_viewport = False
        bpy.context.view_layer.objects.active = arm
        arm.select_set(True)
        bpy.ops.object.mode_set(mode="EDIT")
        created: list[str] = []
        try:
            ebs = arm.data.edit_bones
            inv = np.linalg.inv(_matrix_np(arm.matrix_world))
            to_arm = lambda p: Vector(_transform(inv, np.asarray(p)[None, :])[0])  # noqa: E731
            to_arm_dir = lambda d: Vector(inv[:3, :3] @ np.asarray(d))  # noqa: E731
            collection = None
            try:
                collection = arm.data.collections.get("AutoRig Cloth") or arm.data.collections.new("AutoRig Cloth")
            except AttributeError:  # Blender < 4.0 has no bone collections
                collection = None

            def new_bone(name: str, head_w, tail_w, parent, connect: bool, deform: bool, roll_hint=None):
                bone = ebs.new(name)
                if bone.name != name:
                    raise ClothRigFailure("chain_bones", f"bone name {name!r} is taken (got {bone.name!r})")
                bone.head = to_arm(head_w)
                bone.tail = to_arm(tail_w)
                bone.parent = parent
                bone.use_connect = connect
                bone.use_deform = deform
                if roll_hint is not None:
                    axis = np.asarray(tail_w) - np.asarray(head_w)
                    axis = axis / max(np.linalg.norm(axis), 1e-12)
                    hint = np.asarray(roll_hint) - axis * float(np.asarray(roll_hint) @ axis)
                    if np.linalg.norm(hint) > 1e-6:
                        bone.align_roll(to_arm_dir(hint))
                if collection is not None:
                    collection.assign(bone)
                created.append(name)
                return bone

            for group in self.groups:
                parent = ebs.get(group.attach_bone)
                if parent is None:
                    raise ClothRigFailure("chain_bones", f"attach bone {group.attach_bone!r} vanished")
                attach_head = self.bone_head[group.attach_bone]
                for joints, names in zip(group.chains_world, group.bone_names):
                    outward = joints[0] - attach_head
                    previous = None
                    for k in range(len(joints) - 1):
                        previous = new_bone(names[k], joints[k], joints[k + 1], previous or parent,
                                            previous is not None, True, outward)
                    direction = joints[-1] - joints[-2]
                    length = float(np.linalg.norm(direction))
                    end_len = max(0.25 * length, 2e-3 * self.height)
                    tail = joints[-1] + direction / max(length, 1e-12) * end_len
                    new_bone(names[-1], joints[-1], tail, previous, True, False, outward)
            if self.head_helper is not None:
                helper = self.head_helper
                parent = ebs.get(helper["parent"])
                new_bone(helper["name"], helper["head"], helper["tail"], parent, False, False)
        finally:
            bpy.ops.object.mode_set(mode="OBJECT")
        missing = [n for n in created if n not in arm.data.bones]
        if missing:
            raise ClothRigFailure("chain_bones", f"bones lost after leaving edit mode: {missing[:10]}")
        self.created_bones = created
        self.bone_set = set(arm.data.bones.keys())
        self.report["chain_bones_created"] = len(created)

    # ----------------------------------------------------------------- weights
    def write_weights(self) -> None:
        part_ids = sorted({g.part_index for g in self.groups})
        bone_set = self.bone_set
        self.chain_weight_counts: dict[str, int] = {}
        parts_report = []
        for part_index in part_ids:
            groups = [g for g in self.groups if g.part_index == part_index]
            part = self.decomp.parts[part_index]
            attach_bone, _slot = core.resolve_attach(str(part.get("attach") or groups[0].attach_semantic), self.mapping)
            if attach_bone is None:
                attach_bone = groups[0].attach_bone
            chains, id_names = [], [attach_bone]
            group_of_id = [None]
            for group in groups:
                for joints, names in zip(group.chains_world, group.bone_names):
                    chains.append(joints)
                    id_names.extend(names[:-1])
                    group_of_id.extend([group.name] * (len(names) - 1))
            if not self.arm.data.bones[attach_bone].use_deform:
                self.warnings.append(f"attach bone {attach_bone!r} does not deform; part {part.get('name')!r} root weights are inert")
            selections = []
            for mesh in self.meshes:
                if not mesh.skinned:
                    continue
                idx = np.nonzero(mesh.parts == part_index)[0]
                if len(idx):
                    selections.append((mesh, idx))
            rigid_hits = sum(int(np.count_nonzero(m.parts == part_index)) for m in self.meshes if not m.skinned)
            if rigid_hits:
                self.warnings.append(f"part {part.get('name')!r}: {rigid_hits} vertices on non-skinned meshes left as they are")
            if not selections:
                continue
            points = np.concatenate([mesh.world[idx] for mesh, idx in selections])
            origin_info = self.part_origins.get(part_index) or {"origin": None, "method": "none", "count": 0}
            bone_ids, weights, stats = core.compute_part_weights(
                self.weights_module, points, chains, attach_origin=origin_info["origin"],
                attach_blend=0.25, max_influences=4,
            )
            bone_ids, weights = _merge_rows(bone_ids, weights)
            offset = 0
            per_group = {g.name: 0 for g in groups}
            for mesh, idx in selections:
                rows = slice(offset, offset + len(idx))
                offset += len(idx)
                self._write_mesh_weights(mesh.obj, idx, bone_ids[rows], weights[rows], id_names, bone_set)
                for row_ids, row_w in zip(bone_ids[rows], weights[rows]):
                    touched = {group_of_id[i] for i, w in zip(row_ids, row_w) if w > 0 and i > 0}
                    for name in touched:
                        per_group[name] += 1
            for i, name in enumerate(id_names):
                if i == 0:
                    continue
                self.chain_weight_counts[name] = int(np.count_nonzero((bone_ids == i) & (weights > 0)))
            for group in groups:
                group.weighted_vertices = per_group[group.name]
            parts_report.append({
                "part": str(part.get("name")), "attach_bone": attach_bone,
                "attach_origin": None if origin_info["origin"] is None else [float(v) for v in origin_info["origin"]],
                "attach_origin_method": origin_info["method"], "attach_origin_vertices": origin_info["count"],
                "reweighted_vertices": int(len(points)), "weights": stats,
            })
        self.report["parts"] = parts_report

    def _write_mesh_weights(self, obj, idx, bone_ids, weights, id_names, bone_set) -> None:
        index_list = [int(i) for i in idx]
        for vgroup in obj.vertex_groups:
            if vgroup.name in bone_set:
                vgroup.remove(index_list)
        cache = {}
        for vertex, row_ids, row_w in zip(index_list, bone_ids, weights):
            for bone_id, weight in zip(row_ids, row_w):
                if weight <= 0.0:
                    continue
                name = id_names[int(bone_id)]
                vgroup = cache.get(name)
                if vgroup is None:
                    vgroup = obj.vertex_groups.get(name) or obj.vertex_groups.new(name=name)
                    cache[name] = vgroup
                vgroup.add([vertex], float(weight), "REPLACE")

    # ----------------------------------------------------------------- manifest
    def write_manifest(self) -> None:
        slots = self.mapping.slots
        warnings = self.warnings
        if "hips" in slots and "head" in slots and slots["hips"] != slots["head"]:
            bone_a, bone_b = slots["hips"], slots["head"]
        else:
            chain_bones = set(self.created_bones)
            pool = [n for n in self.bone_names if n not in chain_bones]
            if "hips" in slots:
                bone_a = slots["hips"]
            else:  # the root bone with the largest hierarchy
                roots = [n for n in pool if self.arm.data.bones[n].parent is None]
                bone_a = max(roots, key=lambda n: len(self.arm.data.bones[n].children_recursive))
            bone_b = max(pool, key=lambda n: float(np.linalg.norm(self.bone_head[n] - self.bone_head[bone_a])))
            warnings.append(f"calibration uses {bone_a!r} -> {bone_b!r} (hips/head not both mapped)")
        distance = float(np.linalg.norm(self.bone_head[bone_a] - self.bone_head[bone_b]))
        if not distance > 0:
            raise ClothRigFailure("manifest", "calibration bones coincide")
        length_scale = manifest_lib.length_scale_for_height(self.height)
        presets, seen = [], set()
        for group in self.groups:
            if group.preset in seen:
                continue
            seen.add(group.preset)
            base = group.preset if group.preset in manifest_lib.BUILTIN_PRESETS else \
                manifest_lib.default_preset_name(group.kind, group.connection)
            presets.append(manifest_lib.preset_entry(group.preset, base=base, length_scale=length_scale))
        groups = [{
            "name": g.name, "kind": g.kind, "attach_bone": g.attach_bone, "preset": g.preset,
            "connection": g.connection, "collider_tags": [manifest_lib.BODY_TAG],
            "chains": [{"bones": list(names)} for names in g.bone_names],
        } for g in self.groups]
        doc = manifest_lib.build_manifest(
            calibration={"bone_a": bone_a, "bone_b": bone_b, "distance": distance},
            presets=presets, groups=groups, colliders=self.colliders,
        )
        problems = manifest_lib.validate_manifest(doc)
        missing = sorted(n for n in manifest_lib.manifest_bone_names(doc) if n not in self.bone_set)
        if missing:
            problems.append(f"manifest names bones the armature lacks: {missing[:10]}")
        if problems:
            raise ClothRigFailure("manifest", "manifest failed validation: " + "; ".join(problems[:10]))
        self.manifest = doc
        self.report["calibration"] = doc["calibration"]
        self.report["length_scale"] = length_scale
        self.report["groups"] = [{
            "name": g.name, "source_group": g.source_name, "kind": g.kind, "part": g.part_name,
            "attach": g.attach_semantic, "attach_slot": g.attach_slot, "attach_bone": g.attach_bone,
            "connection": g.connection, "preset": g.preset, "chains": len(g.bone_names),
            "bones": sum(len(n) for n in g.bone_names),
            "deforming_bones": sum(len(n) - 1 for n in g.bone_names),
            "chain_order_from_decomposition": g.order,
            "weighted_vertices": g.weighted_vertices,
        } for g in self.groups]

    # ----------------------------------------------------------------- export
    def export(self) -> None:
        arm = self.arm
        arm.data.pose_position = self.original_pose_position or "POSE"
        objects = [arm] + [m.obj for m in self.meshes]
        for obj in bpy.context.view_layer.objects:
            obj.select_set(False)
        for obj in objects:
            obj.hide_set(False)
            obj.hide_viewport = False
            obj.select_set(True)
        bpy.context.view_layer.objects.active = arm
        bpy.context.view_layer.update()
        has_anim = self.has_animation
        self.report["renamed_actions"] = self._restore_fbx_take_names() if has_anim else {}
        _call_operator(
            bpy.ops.export_scene.fbx, filepath=str(self.paths["fbx"]), use_selection=True,
            object_types={"ARMATURE", "MESH"}, use_mesh_modifiers=False, add_leaf_bones=False,
            primary_bone_axis="Y", secondary_bone_axis="X", use_armature_deform_only=False,
            armature_nodetype="NULL", bake_anim=has_anim, bake_anim_use_all_actions=has_anim,
            bake_anim_use_nla_strips=has_anim, path_mode="COPY", embed_textures=True,
            axis_forward="-Z", axis_up="Y",
        )
        _call_operator(
            bpy.ops.export_scene.gltf, filepath=str(self.paths["glb"]), export_format="GLB",
            use_selection=True, export_skins=True, export_def_bones=False, export_leaf_bone=False,
            export_animations=has_anim, export_yup=True, export_apply=False,
            export_rest_position_armature=True, export_all_influences=False,
            export_materials="EXPORT", check_existing=False,
        )
        text = json.dumps(self.manifest, indent=2, ensure_ascii=True) + "\n"
        for key in ("manifest", "manifest_alias"):
            self.paths[key].write_text(text, encoding="utf-8")
        self.report["outputs"] = {k: str(self.paths[k]) for k in ("fbx", "glb", "manifest", "manifest_alias", "report")}
        self.report["exported_actions"] = self.action_names[:50] if has_anim else []
        for key in ("fbx", "glb"):
            path = self.paths[key]
            if not path.is_file() or path.stat().st_size < 1024:
                raise ClothRigFailure("export", f"export produced no usable {key.upper()} at {path}")

    def _restore_fbx_take_names(self) -> dict:
        """Keep FBX clip names stable through this import/export round trip.

        Blender's FBX importer names an action ``<armature>|<take>`` and its
        exporter writes the take ``<armature>|<action>``, so every round trip
        would grow the clip name by one prefix (``root|root|Animation``). Strip
        the importer's prefix, plus the one the exporter will add back.
        """
        if self.model_path.suffix.lower() != ".fbx":
            return {}
        prefix = f"{self.arm.name}|"
        renamed = {}
        for action in list(bpy.data.actions):
            name = action.name
            if not name.startswith(prefix):
                continue
            take = name[len(prefix):]
            target = take[len(prefix):] if take.startswith(prefix) else take
            if target and target not in bpy.data.actions:
                action.name = target
                renamed[name] = action.name
        return renamed

    # ----------------------------------------------------------------- self check
    def self_check(self) -> None:
        results = {}
        failures = []
        expected_weights = {n: c for n, c in self.chain_weight_counts.items() if c > 0}
        manifest_bones = manifest_lib.manifest_bone_names(self.manifest)
        expected_parent = {}
        for group in self.groups:
            for names in group.bone_names:
                expected_parent[names[0]] = group.attach_bone
                for a, b in zip(names[:-1], names[1:]):
                    expected_parent[b] = a
        if self.head_helper is not None:
            expected_parent[self.head_helper["name"]] = self.head_helper["parent"]
        for key in ("fbx", "glb"):
            path = self.paths[key]
            import_model(path)
            scene = bpy.context.scene
            arms = [o for o in scene.objects if o.type == "ARMATURE"]
            info = {"file": str(path), "bytes": path.stat().st_size}
            if not arms:
                info.update(ok=False, error="no armature after re-import")
                results[key] = info
                failures.append(f"{key}: no armature after re-import")
                continue
            arm = max(arms, key=lambda o: len(o.data.bones))
            bones = arm.data.bones
            missing = sorted(n for n in manifest_bones if n not in bones)
            wrong_parent = sorted(
                n for n, parent in expected_parent.items()
                if n in bones and (bones[n].parent is None or bones[n].parent.name != parent)
            )
            counts: dict[str, int] = {}
            for obj in scene.objects:
                if not _is_skinned_by(obj, arm):
                    continue
                names = {g.index: g.name for g in obj.vertex_groups if g.name in expected_weights}
                if not names:
                    continue
                for vertex in obj.data.vertices:
                    for g in vertex.groups:
                        name = names.get(g.group)
                        if name is not None and g.weight > 1e-4:
                            counts[name] = counts.get(name, 0) + 1
            lost_weights = sorted(n for n in expected_weights if counts.get(n, 0) == 0)
            ok = not missing and not wrong_parent and not lost_weights
            info.update(
                ok=ok, bones=len(bones), missing_bones=missing[:50], wrong_parents=wrong_parent[:50],
                chain_bones_expected_weighted=len(expected_weights),
                chain_bones_weighted=sum(1 for n in expected_weights if counts.get(n, 0) > 0),
                lost_vertex_groups=lost_weights[:50],
                animations=len(bpy.data.actions),
            )
            results[key] = info
            if missing:
                failures.append(f"{key}: missing bones {missing[:5]}")
            if wrong_parent:
                failures.append(f"{key}: wrong parents for {wrong_parent[:5]}")
            if lost_weights:
                failures.append(f"{key}: chain vertex groups lost {lost_weights[:5]}")
            if self.has_animation and not bpy.data.actions:
                self.warnings.append(f"{key}: the input had animation but the re-import has none")
        self.report["self_check"] = results
        if failures:
            raise ClothRigFailure("self_check", "exported files failed verification: " + "; ".join(failures),
                                  {"self_check": results})


def _merge_rows(bone_ids: np.ndarray, weights: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Merge duplicate ids per row, drop weights below 1e-4 and renormalise."""
    ids = bone_ids.copy()
    w = weights.copy()
    width = ids.shape[1]
    for a in range(width):
        for b in range(a + 1, width):
            same = (ids[:, a] == ids[:, b]) & (w[:, b] > 0)
            w[same, a] += w[same, b]
            w[same, b] = 0.0
    w[w < 1e-4] = 0.0
    sums = w.sum(axis=1)
    empty = sums <= 0
    if np.any(empty):
        order = np.argmax(weights, axis=1)
        w[empty] = 0.0
        w[empty, 0] = 1.0
        ids[empty, 0] = bone_ids[empty, order[empty]]
        sums = w.sum(axis=1)
    w /= sums[:, None]
    ids[w <= 0] = 0
    return ids, w


# --------------------------------------------------------------------------- entry point


def _new_report(paths: dict) -> dict:
    return {
        "format": "autorig.regen.cloth_rig_report",
        "version": 1,
        "generator": manifest_lib.GENERATOR,
        "ok": False,
        "blender_version": bpy.app.version_string,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "outputs": {},
        "warnings": [],
        "timings": {},
    }


def _write_json(path: Path, data: dict) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=True, default=_json_default) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _json_default(value):
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, Path):
        return str(value)
    return str(value)


def _guess_out_dir(argv: list[str] | None) -> Path | None:
    items = argv if argv is not None else (sys.argv[sys.argv.index("--") + 1 :] if "--" in sys.argv else sys.argv[1:])
    for i, item in enumerate(items):
        if item == "--out" and i + 1 < len(items):
            return Path(items[i + 1])
        if item.startswith("--out="):
            return Path(item.split("=", 1)[1])
    return None


def main(argv: list[str] | None = None) -> int:
    """Run the step. Returns 0 on success; on failure writes error.json and calls sys.exit(1)."""
    started = time.perf_counter()
    paths = None
    report = None
    try:
        args = _parse_args(argv)
        out_dir = Path(args.out).resolve()
        out_dir.mkdir(parents=True, exist_ok=True)
        stem = manifest_lib.resolve_stem(args.model, args.stem)
        paths = manifest_lib.artifact_paths(out_dir, stem)
        for path in paths.values():  # a previous run's artifacts must not read as this run's
            for stale in (path, path.with_name(path.name + ".failed")):
                if stale.exists():
                    stale.unlink()
        report = _new_report(paths)
        report["stem"] = stem
        log(f"blender {bpy.app.version_string}; model={args.model}; out={out_dir}")
        ClothRig(args, paths, report).run()
        report["ok"] = True
        report["duration_s"] = round(time.perf_counter() - started, 3)
        _write_json(paths["report"], report)
        log(f"OK: {paths['manifest'].name}, {paths['fbx'].name}, {paths['glb'].name} "
            f"({report['duration_s']}s, {len(report['warnings'])} warnings)")
        return 0
    except Exception as exc:  # noqa: BLE001 - every failure must reach error.json
        stage = getattr(exc, "stage", "internal")
        message = str(exc) or exc.__class__.__name__
        error = {
            "ok": False,
            "stage": stage,
            "error": message,
            "type": exc.__class__.__name__,
            "details": getattr(exc, "details", {}),
            "traceback": traceback.format_exc(),
            "generator": manifest_lib.GENERATOR,
            "blender_version": bpy.app.version_string,
        }
        if paths is None:
            out_dir = _guess_out_dir(argv)
            if out_dir is not None:
                out_dir.mkdir(parents=True, exist_ok=True)
                paths = {"error": out_dir / "error.json", "report": out_dir / "cloth_rig_report.json"}
        if paths is not None:
            for key in ("manifest", "manifest_alias", "fbx", "glb"):
                path = paths.get(key)
                if path is not None and path.exists():
                    os.replace(path, path.with_name(path.name + ".failed"))
            _write_json(paths["error"], error)
            if report is not None:
                report["ok"] = False
                report["error"] = {k: error[k] for k in ("stage", "error", "type")}
                report["duration_s"] = round(time.perf_counter() - started, 3)
                _write_json(paths["report"], report)
        traceback.print_exc()
        log(f"FAILED at {stage}: {message}")
        sys.stdout.flush()
        sys.exit(1)


if __name__ == "__main__":
    main()
