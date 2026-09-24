"""AutoRig Regen cloth rig step (``autorig-online/tools/regen``).

Two layers:

* numpy-only tests of the core (alignment, bone mapping, naming, ordering,
  weights adapter, manifest) and of the runner's exit-code trap; they run in
  the normal backend suite;
* end-to-end tests that build a synthetic rigged character with ``bpy`` (a
  Mixamo-named T-pose mannequin with a skirt and a ponytail), export it as FBX
  and GLB, run ``blender_cloth_rig.main`` in-process and inspect the exported
  files. They are skipped where the ``bpy`` module is not installed.

Run the Blender part with the ``bpy`` 4.3 wheel (CPython 3.11, numpy<2)::

    python -m pytest autorig-online/backend/tests/test_regen_cloth_rig.py -q
"""

from __future__ import annotations

import importlib.util
import json
import math
import stat
import sys
import textwrap
from pathlib import Path

import numpy as np
import pytest

REPO = Path(__file__).resolve().parents[3]
TOOLS = REPO / "autorig-online" / "tools" / "regen"
SCHEMA_PATH = REPO / "autorig-cloth" / "spec" / "cloth-manifest.v1.schema.json"
BUILTIN_PRESETS_PATH = REPO / "autorig-cloth" / "spec" / "builtin-presets.v1.json"
REAL_WEIGHTS = REPO / "autorig-online" / "backend" / "regen" / "weights.py"


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


core = _load("autorig_regen_cloth_rig_core", TOOLS / "cloth_rig_core.py")
manifest_lib = _load("autorig_regen_cloth_manifest", TOOLS / "cloth_manifest.py")
runner = _load("autorig_regen_cloth_rig_runner", TOOLS / "cloth_rig_runner.py")

# A tiny stand-in for backend/regen/weights.py with the pinned API, used when
# the real module is absent and by the adapter unit tests.
WEIGHTS_DOUBLE_SOURCE = textwrap.dedent(
    '''
    import numpy as np

    WEIGHTS_API_VERSION = 1


    def compute_chain_weights(points, chains, *, attach_origin=None, attach_blend=0.25, max_influences=4):
        pts = np.asarray(points, dtype=np.float64).reshape(-1, 3)
        segs, ids = [], []
        offset = 0
        for joints in chains:
            joints = np.asarray(joints, dtype=np.float64)
            for j in range(len(joints) - 1):
                segs.append((joints[j], joints[j + 1]))
                ids.append(1 + offset + j)
            offset += len(joints) - 1
        dist = np.empty((len(pts), len(segs)))
        for s, (a, b) in enumerate(segs):
            ab = b - a
            t = np.clip((pts - a) @ ab / max(float(ab @ ab), 1e-12), 0.0, 1.0)
            dist[:, s] = np.linalg.norm(pts - (a + t[:, None] * ab), axis=1)
        k = min(2, len(segs))
        near = np.argsort(dist, axis=1)[:, :k]
        inv = 1.0 / (np.take_along_axis(dist, near, axis=1) + 1e-9) ** 2
        w = inv / inv.sum(axis=1, keepdims=True)
        bone_ids = np.zeros((len(pts), max_influences), dtype=np.int32)
        weights = np.zeros((len(pts), max_influences), dtype=np.float32)
        bone_ids[:, :k] = np.asarray(ids)[near]
        weights[:, :k] = w
        return bone_ids, weights
    '''
)


def _write_double(directory: Path) -> Path:
    path = directory / "weights_double.py"
    path.write_text(WEIGHTS_DOUBLE_SOURCE, encoding="utf-8")
    return path


# =========================================================================== numpy-only


def _random_body(n: int = 3000, seed: int = 1) -> np.ndarray:
    """An asymmetric humanoid-ish point cloud in Blender Z-up (meters)."""
    rng = np.random.default_rng(seed)
    parts = []
    t = rng.random((n, 2))
    parts.append(np.c_[0.15 * np.cos(2 * np.pi * t[:, 0]), 0.1 * np.sin(2 * np.pi * t[:, 0]), 0.9 + 0.6 * t[:, 1]])
    t = rng.random((n // 2, 2))
    parts.append(np.c_[0.1 * np.cos(2 * np.pi * t[:, 0]), 0.1 * np.sin(2 * np.pi * t[:, 0]) + 0.02, 1.55 + 0.2 * t[:, 1]])
    for side in (1, -1):
        t = rng.random((n // 2, 2))
        parts.append(np.c_[side * 0.09 + 0.06 * np.cos(2 * np.pi * t[:, 0]), 0.06 * np.sin(2 * np.pi * t[:, 0]), 0.9 * t[:, 1]])
        t = rng.random((n // 3, 2))
        parts.append(np.c_[side * (0.18 + 0.55 * t[:, 1]), 0.04 * np.cos(2 * np.pi * t[:, 0]), 1.42 + 0.04 * np.sin(2 * np.pi * t[:, 0])])
    t = rng.random((n // 3, 2))  # a ponytail at the back breaks the front/back symmetry
    parts.append(np.c_[0.03 * np.cos(2 * np.pi * t[:, 0]), 0.16 + 0.03 * np.sin(2 * np.pi * t[:, 0]), 1.1 + 0.55 * t[:, 1]])
    return np.concatenate(parts)


def test_gltf_axis_conversion_matches_blender_importer():
    p = np.array([[1.0, 2.0, 3.0]])
    assert core.gltf_to_blender(p).tolist() == [[1.0, -3.0, 2.0]]
    assert np.allclose(core.blender_to_gltf(core.gltf_to_blender(p)), p)


def test_umeyama_recovers_similarity():
    rng = np.random.default_rng(0)
    src = rng.normal(size=(200, 3))
    angle = math.radians(20)
    rot = np.array([[math.cos(angle), -math.sin(angle), 0], [math.sin(angle), math.cos(angle), 0], [0, 0, 1]])
    dst = 1.7 * src @ rot.T + np.array([0.3, -2.0, 5.0])
    scale, rotation, translation = core.umeyama(src, dst)
    assert scale == pytest.approx(1.7, rel=1e-9)
    assert np.allclose(rotation, rot)
    assert np.allclose(translation, [0.3, -2.0, 5.0])


def test_similarity_icp_recovers_known_transform():
    rigged = _random_body(n=1500)
    rng = np.random.default_rng(5)
    dressed = 0.9 * rigged[rng.permutation(len(rigged))[:4000]] + np.array([0.2, -0.1, 0.3])
    dressed += rng.normal(scale=0.002, size=dressed.shape)
    debris = np.array([0.2, -0.1, 0.3]) + rng.normal(scale=0.02, size=(6, 3)) + np.array([0.0, 0.0, 2.5])
    dressed = np.concatenate([dressed, debris])  # a floating fragment far above the head
    result = core.align_similarity(
        dressed, rigged, nearest_factory=core.brute_force_nearest, n_forward=1500, n_reverse=1500,
        n_source_tree=4000,
    )
    icp = result.icp
    assert icp.yaw_init_deg == 0.0
    assert icp.scale == pytest.approx(1 / 0.9, rel=0.01)
    assert icp.rotation_angle_deg() < 1.0
    mapped = core.apply_similarity(icp.scale, icp.rotation, icp.translation, np.array([[0.2, -0.1, 0.3]]))
    assert np.allclose(mapped, 0.0, atol=0.01)
    assert result.residual_rel < 0.01


def test_left_right_landmarks_catch_a_turned_rig():
    """A body is nearly front/back symmetric: without the L/R check a rig turned
    by 180 degrees can pass as unturned. With it, the 180 degree candidate wins."""
    body = _random_body(n=1200)
    turned = body @ core.rotation_z(180.0).T  # the rig faces the other way
    rng = np.random.default_rng(3)
    dressed = 0.9 * body[rng.permutation(len(body))[:2000]] + np.array([0.2, -0.1, 0.3])
    kwargs = dict(nearest_factory=core.brute_force_nearest, n_forward=800, n_reverse=800, n_source_tree=2000)
    result = core.align_similarity(dressed, turned, left_check=((1.0, 0, 0), (-1.0, 0, 0)), **kwargs)
    assert result.icp.yaw_init_deg == 180.0
    assert result.icp.rotation_angle_deg() == pytest.approx(180.0, abs=2.0)
    assert result.warnings and "180 degree turn" in result.warnings[0]
    assert result.to_report()["left_right_check"] is True
    with pytest.raises(core.AlignmentError, match="left side"):
        core.align_similarity(dressed, turned, left_check=((1.0, 0, 0), (-1.0, 0, 0)),
                              yaw_candidates=(0.0,), **kwargs)


def test_similarity_icp_rejects_a_wrong_space():
    rigged = _random_body(n=1500)
    lying = rigged[:4000] @ np.array([[1, 0, 0], [0, 0, -1], [0, 1, 0.0]]).T  # rotated 90 deg about X
    with pytest.raises(core.AlignmentError, match="cannot align"):
        core.align_similarity(lying, rigged, nearest_factory=core.brute_force_nearest,
                              n_forward=1000, n_reverse=1000, n_source_tree=3000)


def _bones(spec):
    return [core.BoneInfo(name=n, parent=p, head=np.asarray(h, float), tail=np.asarray(t, float),
                          use_deform=True, weight_count=w) for n, p, h, t, w in spec]


def _humanoid_spec(names: dict, extra=()):
    """Skeleton in Blender space; ``names`` maps a semantic key to the bone name."""
    n = names.get
    spec = [
        (n("hips"), n("root"), (0, 0, 0.95), (0, 0, 1.05), 10),
        (n("spine"), n("hips"), (0, 0, 1.05), (0, 0, 1.18), 10),
        (n("chest"), n("spine"), (0, 0, 1.18), (0, 0, 1.30), 10),
        (n("upper_chest"), n("chest"), (0, 0, 1.30), (0, 0, 1.46), 10),
        (n("neck"), n("upper_chest"), (0, 0, 1.46), (0, 0, 1.56), 10),
        (n("head"), n("neck"), (0, 0, 1.56), (0, 0, 1.76), 10),
    ]
    for side, sx in (("l", 1), ("r", -1)):
        spec += [
            (n(f"upper_leg_{side}"), n("hips"), (sx * 0.09, 0, 0.92), (sx * 0.09, 0, 0.5), 10),
            (n(f"lower_leg_{side}"), n(f"upper_leg_{side}"), (sx * 0.09, 0, 0.5), (sx * 0.09, 0, 0.08), 10),
            (n(f"foot_{side}"), n(f"lower_leg_{side}"), (sx * 0.09, 0, 0.08), (sx * 0.09, -0.12, 0.03), 10),
            (n(f"shoulder_{side}"), n("upper_chest"), (sx * 0.03, 0, 1.4), (sx * 0.17, 0, 1.42), 10),
            (n(f"upper_arm_{side}"), n(f"shoulder_{side}"), (sx * 0.17, 0, 1.42), (sx * 0.45, 0, 1.42), 10),
            (n(f"lower_arm_{side}"), n(f"upper_arm_{side}"), (sx * 0.45, 0, 1.42), (sx * 0.7, 0, 1.42), 10),
            (n(f"hand_{side}"), n(f"lower_arm_{side}"), (sx * 0.7, 0, 1.42), (sx * 0.8, 0, 1.42), 10),
        ]
    if n("root"):
        spec.insert(0, (n("root"), None, (0, 0, 0), (0, 0, 0.1), 0))
    spec += list(extra)
    return _bones(spec)


def _named(prefix, table):
    return {k: (prefix + v if v else None) for k, v in table.items()}


MIXAMO = {
    "hips": "Hips", "spine": "Spine", "chest": "Spine1", "upper_chest": "Spine2", "neck": "Neck", "head": "Head",
    "upper_leg_l": "LeftUpLeg", "lower_leg_l": "LeftLeg", "foot_l": "LeftFoot", "shoulder_l": "LeftShoulder",
    "upper_arm_l": "LeftArm", "lower_arm_l": "LeftForeArm", "hand_l": "LeftHand",
    "upper_leg_r": "RightUpLeg", "lower_leg_r": "RightLeg", "foot_r": "RightFoot", "shoulder_r": "RightShoulder",
    "upper_arm_r": "RightArm", "lower_arm_r": "RightForeArm", "hand_r": "RightHand",
}
UNREAL = {
    "root": "root", "hips": "pelvis", "spine": "spine_01", "chest": "spine_02", "upper_chest": "spine_03",
    "neck": "neck_01", "head": "head",
    "upper_leg_l": "thigh_l", "lower_leg_l": "calf_l", "foot_l": "foot_l", "shoulder_l": "clavicle_l",
    "upper_arm_l": "upperarm_l", "lower_arm_l": "lowerarm_l", "hand_l": "hand_l",
    "upper_leg_r": "thigh_r", "lower_leg_r": "calf_r", "foot_r": "foot_r", "shoulder_r": "clavicle_r",
    "upper_arm_r": "upperarm_r", "lower_arm_r": "lowerarm_r", "hand_r": "hand_r",
}
AUTO_RIG_PRO = {
    "hips": "root.x", "spine": "spine_01.x", "chest": "spine_02.x", "upper_chest": "spine_03.x",
    "neck": "neck.x", "head": "head.x",
    "upper_leg_l": "thigh_stretch.l", "lower_leg_l": "leg_stretch.l", "foot_l": "foot.l", "shoulder_l": "shoulder.l",
    "upper_arm_l": "arm_stretch.l", "lower_arm_l": "forearm_stretch.l", "hand_l": "hand.l",
    "upper_leg_r": "thigh_stretch.r", "lower_leg_r": "leg_stretch.r", "foot_r": "foot.r", "shoulder_r": "shoulder.r",
    "upper_arm_r": "arm_stretch.r", "lower_arm_r": "forearm_stretch.r", "hand_r": "hand.r",
}
RIGIFY = {
    "hips": "DEF-spine", "spine": "DEF-spine.001", "chest": "DEF-spine.002", "upper_chest": "DEF-spine.003",
    "neck": "DEF-spine.004", "head": "DEF-spine.006",
    "upper_leg_l": "DEF-thigh.L", "lower_leg_l": "DEF-shin.L", "foot_l": "DEF-foot.L", "shoulder_l": "DEF-shoulder.L",
    "upper_arm_l": "DEF-upper_arm.L", "lower_arm_l": "DEF-forearm.L", "hand_l": "DEF-hand.L",
    "upper_leg_r": "DEF-thigh.R", "lower_leg_r": "DEF-shin.R", "foot_r": "DEF-foot.R", "shoulder_r": "DEF-shoulder.R",
    "upper_arm_r": "DEF-upper_arm.R", "lower_arm_r": "DEF-forearm.R", "hand_r": "DEF-hand.R",
}
GENERIC = {
    "hips": "Hips", "spine": "Spine", "chest": "Chest", "upper_chest": "UpperChest", "neck": "Neck", "head": "Head",
    "upper_leg_l": "UpperLeg.L", "lower_leg_l": "LowerLeg.L", "foot_l": "Foot.L", "shoulder_l": "Shoulder.L",
    "upper_arm_l": "UpperArm.L", "lower_arm_l": "LowerArm.L", "hand_l": "Hand.L",
    "upper_leg_r": "UpperLeg.R", "lower_leg_r": "LowerLeg.R", "foot_r": "Foot.R", "shoulder_r": "Shoulder.R",
    "upper_arm_r": "UpperArm.R", "lower_arm_r": "LowerArm.R", "hand_r": "Hand.R",
}


@pytest.mark.parametrize(
    "table,prefix",
    [(MIXAMO, "mixamorig:"), (MIXAMO, ""), (UNREAL, ""), (AUTO_RIG_PRO, ""), (RIGIFY, ""), (GENERIC, "")],
    ids=["mixamo-prefixed", "mixamo", "unreal", "auto-rig-pro", "rigify", "generic"],
)
def test_bone_mapping_by_name(table, prefix):
    names = _named(prefix, table)
    extra = []
    if table is RIGIFY:  # neck upper segment and a twist segment must not confuse the mapping
        extra = [("DEF-spine.005", "DEF-spine.004", (0, 0, 1.5), (0, 0, 1.56), 3),
                 ("DEF-thigh.L.001", "DEF-thigh.L", (0.09, 0, 0.7), (0.09, 0, 0.5), 5)]
    if table is AUTO_RIG_PRO:
        extra = [("thigh_twist.l", "thigh_stretch.l", (0.09, 0, 0.8), (0.09, 0, 0.6), 5),
                 ("c_root.x", None, (0, 0, 0.95), (0, 0, 1.0), 0)]
    mapping = core.map_humanoid(_humanoid_spec(names, extra))
    for slot in ("hips", "spine", "chest", "upper_chest", "neck", "head", "upper_leg_l", "lower_leg_r",
                 "foot_l", "upper_arm_r", "lower_arm_l", "hand_r", "shoulder_l"):
        assert mapping.slots.get(slot) == names[slot], (slot, mapping.to_report())
    assert mapping.left_axis_source == "names"
    assert mapping.left_axis[0] > 0.99


def test_bone_mapping_falls_back_to_geometry():
    names = {k: f"bone{i:02d}" for i, k in enumerate(MIXAMO)}
    mapping = core.map_humanoid(_humanoid_spec(names), left_hint=(1.0, 0.0, 0.0))
    report = mapping.to_report()
    for slot in ("hips", "head", "neck", "upper_leg_l", "lower_leg_l", "foot_l", "upper_leg_r",
                 "upper_arm_l", "lower_arm_l", "hand_l", "upper_arm_r", "hand_r"):
        assert mapping.slots.get(slot) == names[slot], (slot, report)
        assert mapping.methods[slot] in ("geometry", "hierarchy")
    # Without a hint the Blender convention (character faces -Y, left is +X) applies.
    assert core.map_humanoid(_humanoid_spec(names)).slots["hand_l"] == names["hand_l"]


def test_group_names_are_unique_ascii_and_short():
    plans = core.plan_group_names(
        ["Jupe évasée!!", "hair", "hair", "x" * 90],
        [[5] * 8, [5] * 3, [4] * 2, [6] * 120],
        existing_bones=["mixamorig:Hips", "hair_00_0"],
    )
    names = [p["name"] for p in plans]
    assert names[0] == "Jupe_evasee"
    assert len(set(n.lower() for n in names)) == 4
    assert "hair_00_0" not in [b for p in plans for c in p["bones"] for b in c]
    for plan in plans:
        for chain in plan["bones"]:
            assert chain[-1].endswith("_end")
            for bone in chain:
                assert bone.isascii() and len(bone) <= core.MAX_BONE_NAME
    assert plans[0]["bones"][0] == ["Jupe_evasee_00_0", "Jupe_evasee_00_1", "Jupe_evasee_00_2",
                                    "Jupe_evasee_00_3", "Jupe_evasee_00_end"]
    assert plans[3]["chain_width"] == 3


def test_loop_and_open_chain_ordering():
    angles = np.radians([200, 10, 90, 300, 150, 45, 250, 330])
    roots = np.c_[np.cos(angles), np.sin(angles), np.zeros(8)]
    order = core.order_chains_angular(roots, "loop", (0, 0, 0), (0, 0, 1), (1, 0, 0))
    assert [round(math.degrees(angles[i])) for i in order] == [10, 45, 90, 150, 200, 250, 300, 330]
    angles = np.radians([90, 359.9, 180, 270])  # the front chain sits just below the seam
    roots = np.c_[np.cos(angles), np.sin(angles), np.zeros(4)]
    order = core.order_chains_angular(roots, "loop", (0, 0, 0), (0, 0, 1), (1, 0, 0))
    assert [round(math.degrees(angles[i]), 1) for i in order] == [359.9, 90, 180, 270]
    arc = np.radians([100, 160, 130, 190])  # an open arc behind the body
    roots = np.c_[np.cos(arc), np.sin(arc), np.zeros(4)]
    order = core.order_chains_angular(roots, "open", (0, 0, 0), (0, 0, 1), (1, 0, 0))
    assert [round(math.degrees(arc[i])) for i in order] == [100, 130, 160, 190]
    assert core.order_chains_angular(roots, "none", (0, 0, 0), (0, 0, 1), (1, 0, 0)) == [0, 1, 2, 3]


def test_weights_adapter_validates_and_normalises(tmp_path):
    module = core.load_weights_module(_write_double(tmp_path))
    chains = [np.array([[0, 0, 1.0], [0, 0, 0.5], [0, 0, 0.0]]), np.array([[1, 0, 1.0], [1, 0, 0.0]])]
    points = np.array([[0.1, 0, 0.8], [0.9, 0, 0.3], [0.5, 0, 0.5]])
    ids, weights, stats = core.compute_part_weights(module, points, chains, attach_origin=None)
    assert ids.shape == (3, 4) and np.allclose(weights.sum(axis=1), 1.0)
    assert ids.max() < 1 + 2 + 1 and stats["bones"] == 4
    assert core.chain_id_layout([3, 2]) == [(-1, -1), (0, 0), (0, 1), (1, 0)]

    class Broken:
        WEIGHTS_API_VERSION = 1

        @staticmethod
        def compute_chain_weights(points, chains, **_):
            return np.full((len(points), 4), 99), np.full((len(points), 4), 0.25)

    with pytest.raises(core.ClothRigFailure, match="outside"):
        core.compute_part_weights(Broken, points, chains, attach_origin=None)


def test_weights_module_version_is_checked(tmp_path):
    path = tmp_path / "weights_v2.py"
    path.write_text("WEIGHTS_API_VERSION = 2\ndef compute_chain_weights(*a, **k):\n    pass\n")
    with pytest.raises(core.ClothRigFailure, match="WEIGHTS_API_VERSION"):
        core.load_weights_module(path)


@pytest.mark.skipif(not REAL_WEIGHTS.is_file(), reason="backend/regen/weights.py not present")
def test_real_weights_module_loads_by_path_through_the_adapter():
    module = core.load_weights_module(REAL_WEIGHTS)
    chains = [np.array([[0, 0, 1.0], [0, 0, 0.6], [0, 0, 0.2], [0, 0, 0.0]])]
    points = np.array([[0.05, 0, 0.95], [0.05, 0, 0.5], [0.05, 0, 0.1]])
    ids, weights, _ = core.compute_part_weights(module, points, chains, attach_origin=np.array([0, 0, 1.1]))
    assert np.allclose(weights.sum(axis=1), 1.0)
    assert (ids <= 3).all()  # end joint (id 4 would be joint 3) never weighted


def _spec_json(path: Path) -> dict:
    """A file of ``autorig-cloth/spec``. The VPS deploy ships ``tools/regen`` without it."""
    if not (REPO / "autorig-cloth").is_dir():
        pytest.skip("autorig-cloth/ is not part of this checkout")
    return json.loads(path.read_text(encoding="utf-8"))


def _manifest_example(presets: list | None = None) -> dict:
    return manifest_lib.build_manifest(
        calibration={"bone_a": "Hips", "bone_b": "Head", "distance": 0.62},
        presets=presets or [manifest_lib.preset_entry("skirt", length_scale=1.0)],
        groups=[{"name": "skirt", "kind": "cloth", "attach_bone": "Hips", "preset": "skirt", "connection": "loop",
                 "collider_tags": ["body"], "chains": [{"bones": ["skirt_00_0", "skirt_00_end"]},
                                                       {"bones": ["skirt_01_0", "skirt_01_end"]}]}],
        colliders=[{"name": "head", "shape": "sphere", "bone": "Head", "to_bone": "", "t": 0.3, "radius": 0.1},
                   {"name": "thigh_l", "shape": "capsule", "bone": "LeftUpLeg", "to_bone": "LeftLeg",
                    "radius": 0.07, "radius_to": 0.05}],
    )


def test_manifest_builder_and_validator():
    doc = _manifest_example()
    assert manifest_lib.validate_manifest(doc) == []
    assert list(doc["presets"][0]) == ["name", *manifest_lib.PRESET_FIELDS]
    bad = json.loads(json.dumps(doc))
    bad["colliders"][1]["to_bone"] = ""
    bad["groups"][0]["chains"][1]["bones"] = ["skirt_00_0", "x"]
    bad["presets"][0]["damping"] = 3
    problems = manifest_lib.validate_manifest(bad)
    assert any("capsule needs to_bone" in p for p in problems)
    assert any("already used" in p for p in problems)
    assert any("damping" in p for p in problems)
    jsonschema = pytest.importorskip("jsonschema")
    jsonschema.validate(doc, json.loads(SCHEMA_PATH.read_text()))


def test_builtin_presets_equal_the_canonical_file():
    """The inlined values are autorig-cloth/spec/builtin-presets.v1.json, which the runtime matches too."""
    canonical = _spec_json(BUILTIN_PRESETS_PATH)
    assert (canonical["format"], canonical["version"]) == ("autorig.cloth.builtin-presets", 1)
    for preset in [*canonical["presets"], canonical["default"]]:
        assert list(preset) == ["name", *manifest_lib.PRESET_FIELDS]
    expected = {p["name"]: {f: p[f] for f in manifest_lib.PRESET_FIELDS} for p in canonical["presets"]}
    assert manifest_lib.BUILTIN_PRESETS == expected
    assert list(manifest_lib.BUILTIN_PRESETS) == list(expected)
    assert all(tuple(values) == manifest_lib.PRESET_FIELDS for values in manifest_lib.BUILTIN_PRESETS.values())
    # The producer picks cloth presets by connection; for a loop it agrees with the runtime's kind fallback.
    for entry in canonical["kind_fallback"]:
        assert manifest_lib.default_preset_name(entry["kind"], "loop") == entry["preset"]


def test_validator_and_builder_require_every_preset_field():
    doc = _manifest_example()
    for field in manifest_lib.PRESET_FIELDS:
        partial = json.loads(json.dumps(doc))
        del partial["presets"][0][field]
        assert manifest_lib.validate_manifest(partial) == [
            f"presets[0].{field} is missing; every preset field is required"]
    nameless = json.loads(json.dumps(doc))
    del nameless["presets"][0]["name"]
    assert manifest_lib.validate_manifest(nameless) == ["presets[0].name must be a non-empty string"]
    with pytest.raises(ValueError, match=r"lacks \['damping'.*every preset field is required"):
        _manifest_example([{"name": "skirt", "gravity": 1.0}])


@pytest.mark.parametrize("height", [1.7, 1.0, 0.3, 2.4, 9.0])
def test_produced_presets_are_complete_and_schema_valid(height):
    scale = manifest_lib.length_scale_for_height(height)
    presets = [manifest_lib.preset_entry(name, length_scale=scale) for name in manifest_lib.BUILTIN_PRESETS]
    presets.append(manifest_lib.preset_entry("skirt_hero", base="skirt", length_scale=scale))
    doc = _manifest_example(presets)
    assert manifest_lib.validate_manifest(doc) == []
    for entry in doc["presets"]:
        assert list(entry) == ["name", *manifest_lib.PRESET_FIELDS]
        base = manifest_lib.BUILTIN_PRESETS["skirt" if entry["name"] == "skirt_hero" else entry["name"]]
        for field in manifest_lib.PRESET_FIELDS:  # only lengths follow the character's height
            factor = scale if field in manifest_lib.PRESET_LENGTH_FIELDS else 1.0
            assert entry[field] == pytest.approx(base[field] * factor, abs=1e-6), (entry["name"], field)
            if height == manifest_lib.REFERENCE_HEIGHT_M:
                assert entry[field] == base[field]  # the canonical values, exactly
    schema = _spec_json(SCHEMA_PATH)
    assert schema["$defs"]["preset"]["required"] == ["name", *manifest_lib.PRESET_FIELDS]
    jsonschema = pytest.importorskip("jsonschema")
    jsonschema.validate(doc, schema)


def test_bone_resolution_mirrors_the_runtime():
    depth_first = ["Armature|Hips", "mixamorig:Hips", "Spine", "Armature|"]
    assert manifest_lib.resolve_bone("mixamorig:Hips", depth_first) == "mixamorig:Hips"  # exact name first
    assert manifest_lib.resolve_bone("Hips", depth_first) == "Armature|Hips"  # then stripped: first in order
    assert manifest_lib.resolve_bone("rig:Spine", depth_first) == "Spine"
    assert manifest_lib.resolve_bone("mixamorig:", depth_first) is None  # an empty stripped name never matches
    assert manifest_lib.resolve_bone("Head", depth_first) is None


def test_stem_and_artifact_names():
    assert manifest_lib.resolve_stem("/x/hero model.fbx") == "hero_model"
    paths = manifest_lib.artifact_paths("/out", "hero")
    assert paths["manifest"].name == "hero.autorig-cloth.json"
    assert paths["fbx"].name == "hero_cloth.fbx" and paths["glb"].name == "hero_cloth.glb"
    assert paths["manifest_alias"].name == "hero_cloth.autorig-cloth.json"


def _fake_blender(directory: Path, body: str) -> Path:
    path = directory / "fake-blender"
    path.write_text(f"#!{sys.executable}\n" + textwrap.dedent(body), encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return path


def _minimal_inputs(directory: Path) -> tuple[Path, Path]:
    model = directory / "hero.fbx"
    model.write_bytes(b"Kaydara FBX Binary  \x00" + b"\x00" * 64)
    decomposition = directory / "decomp"
    decomposition.mkdir()
    (decomposition / "decomposition.json").write_text("{}")
    return model, decomposition


def test_runner_raises_when_blender_exits_zero_without_artifacts(tmp_path):
    model, decomposition = _minimal_inputs(tmp_path)
    fake = _fake_blender(tmp_path, """
        import sys
        print("Blender 4.3.2 (fake)")
        print("Traceback (most recent call last):")
        print("VertexPBRError: something inside the script blew up")
        sys.exit(0)
    """)
    out = tmp_path / "out"
    out.mkdir()
    (out / "hero.autorig-cloth.json").write_text("{}")  # a stale manifest must not count
    with pytest.raises(runner.ClothRigError) as info:
        runner.run_cloth_rig(model, decomposition, out, blender_bin=fake, timeout=60)
    assert info.value.returncode == 0
    assert "cloth_rig_report.json" in info.value.reason
    assert "something inside the script blew up" in str(info.value)
    assert "something inside the script blew up" in info.value.stdout_tail
    assert not (out / "hero.autorig-cloth.json").exists()


def test_runner_reports_error_json_and_timeouts(tmp_path):
    model, decomposition = _minimal_inputs(tmp_path)
    fake = _fake_blender(tmp_path, """
        import json, sys
        out = sys.argv[sys.argv.index("--out") + 1]
        json.dump({"ok": False, "stage": "alignment", "error": "residual 9.1% too high"},
                  open(out + "/error.json", "w"))
        print("[cloth-rig] FAILED at alignment")
        sys.exit(1)
    """)
    with pytest.raises(runner.ClothRigError, match="alignment: residual 9.1% too high") as info:
        runner.run_cloth_rig(model, decomposition, tmp_path / "out", blender_bin=fake, timeout=60)
    assert info.value.error["stage"] == "alignment" and info.value.returncode == 1
    slow = _fake_blender(tmp_path, """
        import time
        print("working", flush=True)
        time.sleep(30)
    """)
    with pytest.raises(runner.ClothRigError, match="timed out"):
        runner.run_cloth_rig(model, decomposition, tmp_path / "out2", blender_bin=slow, timeout=1)


def test_runner_command_and_binary_resolution(tmp_path, monkeypatch):
    command = runner.build_command("/opt/blender/blender", "m.glb", "d", "o", stem="hero", weights_module="w.py")
    assert command[:6] == ["/opt/blender/blender", "--background", "--factory-startup", "-noaudio",
                           "--python-exit-code", "1"]
    assert command[command.index("--python") + 1].endswith("blender_cloth_rig.py")
    assert command[command.index("--") + 1 :][:2] == ["--model", str(Path("m.glb").resolve())]
    assert "--stem" in command and "--weights-module" in command
    monkeypatch.setenv(runner.ENV_BLENDER, str(tmp_path / "missing-blender"))
    with pytest.raises(runner.ClothRigError, match="not found"):
        runner.resolve_blender_bin()


# =========================================================================== bpy end to end

SKIRT_TOP_Z = 1.00
SKIRT_Z = [1.00, 0.94, 0.86, 0.78, 0.70, 0.62, 0.54]
SKIRT_CHAIN_Z = [1.00, 0.88, 0.76, 0.64, 0.52]
PONYTAIL_PATH = [(0.0, 0.09, 1.70), (0.0, 0.15, 1.62), (0.0, 0.17, 1.50), (0.0, 0.17, 1.35),
                 (0.0, 0.16, 1.20), (0.0, 0.15, 1.08)]
PONYTAIL_R = [0.04, 0.045, 0.04, 0.035, 0.03, 0.02]
PONYTAIL_CHAIN = [(0.10, 1.69), (0.16, 1.58), (0.17, 1.42), (0.16, 1.25), (0.15, 1.09)]
DRESSED_SCALE = 0.9
DRESSED_OFFSET = np.array([0.12, -0.07, 0.25])
BONES = [  # name, parent, head, tail, connected
    ("Hips", None, (0, 0, 0.95), (0, 0, 1.05), False),
    ("Spine", "Hips", (0, 0, 1.05), (0, 0, 1.18), True),
    ("Spine1", "Spine", (0, 0, 1.18), (0, 0, 1.30), True),
    ("Spine2", "Spine1", (0, 0, 1.30), (0, 0, 1.46), True),
    ("Neck", "Spine2", (0, 0, 1.46), (0, 0, 1.56), True),
    ("Head", "Neck", (0, 0, 1.56), (0, 0, 1.76), True),
]
for _side, _sx in (("Left", 1), ("Right", -1)):
    BONES += [
        (f"{_side}UpLeg", "Hips", (_sx * 0.09, 0, 0.92), (_sx * 0.09, 0, 0.50), False),
        (f"{_side}Leg", f"{_side}UpLeg", (_sx * 0.09, 0, 0.50), (_sx * 0.09, 0, 0.08), True),
        (f"{_side}Foot", f"{_side}Leg", (_sx * 0.09, 0, 0.08), (_sx * 0.09, -0.13, 0.03), True),
        (f"{_side}Arm", "Spine2", (_sx * 0.17, 0, 1.42), (_sx * 0.45, 0, 1.42), False),
        (f"{_side}ForeArm", f"{_side}Arm", (_sx * 0.45, 0, 1.42), (_sx * 0.70, 0, 1.42), True),
        (f"{_side}Hand", f"{_side}ForeArm", (_sx * 0.70, 0, 1.42), (_sx * 0.80, 0, 1.42), True),
    ]
PREFIX = "mixamorig:"
# What the AutoRig converter really exports (Auto-Rig Pro), including twist bones.
ARP_NAMES = {"Hips": "root.x", "Spine": "spine_01.x", "Spine1": "spine_02.x", "Spine2": "spine_03.x",
             "Neck": "neck.x", "Head": "head.x"}
for _side, _s in (("Left", "l"), ("Right", "r")):
    ARP_NAMES.update({f"{_side}UpLeg": f"thigh_stretch.{_s}", f"{_side}Leg": f"leg_stretch.{_s}",
                      f"{_side}Foot": f"foot.{_s}", f"{_side}Arm": f"arm_stretch.{_s}",
                      f"{_side}ForeArm": f"forearm_stretch.{_s}", f"{_side}Hand": f"hand.{_s}"})


def _skeleton(naming: str) -> list:
    """(name, parent, head, tail, connected) per bone for a naming convention."""
    if naming == "mixamo":  # Mixamo rigs end the head in an unweighted "HeadTop_End" leaf
        return [(PREFIX + n, PREFIX + p if p else None, h, t, c) for n, p, h, t, c in BONES] + [
            (PREFIX + "HeadTop_End", PREFIX + "Head", (0, 0, 1.76), (0, 0, 1.80), True)]
    bones = [(ARP_NAMES[n], ARP_NAMES[p] if p else None, h, t, c) for n, p, h, t, c in BONES]
    for s, sx in (("l", 1), ("r", -1)):  # twist bones along the lower half of each limb bone
        bones += [
            (f"thigh_twist.{s}", f"thigh_stretch.{s}", (sx * 0.09, 0, 0.71), (sx * 0.09, 0, 0.52), False),
            (f"leg_twist.{s}", f"leg_stretch.{s}", (sx * 0.09, 0, 0.29), (sx * 0.09, 0, 0.10), False),
            (f"arm_twist.{s}", f"arm_stretch.{s}", (sx * 0.31, 0, 1.42), (sx * 0.44, 0, 1.42), False),
            (f"forearm_twist.{s}", f"forearm_stretch.{s}", (sx * 0.58, 0, 1.42), (sx * 0.69, 0, 1.42), False),
        ]
    return bones


class _MeshBuilder:
    """Rings lofted into quads; each face carries a class: body / skirt / ponytail."""

    def __init__(self):
        self.verts: list = []
        self.vclass: list = []
        self.faces: list = []
        self.fclass: list = []

    def ring(self, centre, u, v, ru, rv, n, cls):
        start = len(self.verts)
        for i in range(n):
            a = 2 * math.pi * i / n
            self.verts.append(np.asarray(centre) + ru * math.cos(a) * np.asarray(u) + rv * math.sin(a) * np.asarray(v))
            self.vclass.append(cls)
        return list(range(start, start + n))

    def loft(self, rings, cls):
        for r0, r1 in zip(rings[:-1], rings[1:]):
            n = len(r0)
            for i in range(n):
                self.faces.append((r0[i], r0[(i + 1) % n], r1[(i + 1) % n], r1[i]))
                self.fclass.append(cls)

    def tube(self, centres, radii, n, u, v, cls, squash=1.0):
        rings = [self.ring(c, u, v, r, r * squash, n, cls) for c, r in zip(centres, radii)]
        self.loft(rings, cls)
        return rings


def _build_mesh() -> _MeshBuilder:
    m = _MeshBuilder()
    X, Y, Z = (1, 0, 0), (0, 1, 0), (0, 0, 1)
    # Rings sit inside bone segments (not on joints), so every spine bone owns vertices.
    torso_z = [0.86, 0.92, SKIRT_TOP_Z, 1.08, 1.13, 1.24, 1.36, 1.44]
    torso_r = [0.15, 0.16, 0.145, 0.13, 0.14, 0.16, 0.17, 0.12]
    torso = m.tube([(0, 0, z) for z in torso_z], torso_r, 32, X, Y, "body", squash=0.72)
    m.tube([(0, 0, z) for z in (1.44, 1.52, 1.60)], [0.055] * 3, 16, X, Y, "body")
    head_rings = []
    for lat in np.linspace(-80, 80, 9):
        a = math.radians(lat)
        head_rings.append(m.ring((0, 0, 1.66 + 0.11 * math.sin(a)), X, Y, 0.11 * math.cos(a), 0.11 * math.cos(a), 24, "body"))
    m.loft(head_rings, "body")
    for sx in (1, -1):
        m.tube([(sx * 0.09, 0, z) for z in (0.92, 0.80, 0.68, 0.56, 0.48, 0.40, 0.28, 0.16, 0.08)],
               [0.08, 0.075, 0.065, 0.055, 0.05, 0.05, 0.045, 0.04, 0.04], 16, X, Y, "body")
        m.tube([(sx * 0.09, y, 0.045) for y in (0.02, -0.04, -0.10, -0.14)], [0.04, 0.042, 0.04, 0.03],
               12, X, Z, "body")
        m.tube([(sx * x, 0, 1.42) for x in (0.16, 0.26, 0.36, 0.45, 0.55, 0.65, 0.72, 0.78)],
               [0.055, 0.05, 0.045, 0.042, 0.038, 0.032, 0.03, 0.025], 12, Y, Z, "body")
    # Skirt: shares the torso's waist ring (so it borders the body by edges), then flares out.
    waist = torso[torso_z.index(SKIRT_TOP_Z)]
    rings = [waist]
    for k, z in enumerate(SKIRT_Z[1:], start=1):
        f = k / (len(SKIRT_Z) - 1)
        rx, ry = 0.145 + f * (0.30 - 0.145), 0.72 * 0.145 + f * (0.30 - 0.72 * 0.145)
        rings.append(m.ring((0, 0, z), X, Y, rx, ry, 32, "skirt"))
    m.loft(rings, "skirt")
    # Ponytail: a separate shell from the back of the head down the back.
    path = np.array(PONYTAIL_PATH, dtype=float)
    rings = []
    for k, (c, r) in enumerate(zip(path, PONYTAIL_R)):
        tangent = path[min(k + 1, len(path) - 1)] - path[max(k - 1, 0)]
        v = np.cross(tangent, X)
        rings.append(m.ring(c, X, v / np.linalg.norm(v), r, r, 12, "ponytail"))
    m.loft(rings, "ponytail")
    return m


def _skirt_radius(z: float) -> tuple[float, float]:
    f = (SKIRT_TOP_Z - z) / (SKIRT_TOP_Z - SKIRT_Z[-1])
    return 0.145 + f * (0.30 - 0.145), 0.72 * 0.145 + f * (0.30 - 0.72 * 0.145)


def _segment_distance(p, a, b):
    ab = b - a
    t = np.clip((p - a) @ ab / (ab @ ab), 0, 1)
    return np.linalg.norm(p - (a + t[:, None] * ab), axis=1)


def _nearest_bone_weights(points: np.ndarray, skeleton: list):
    dists = np.stack([_segment_distance(points, np.array(h, float), np.array(t, float)) for _, _, h, t, _ in skeleton], 1)
    near = np.argsort(dists, axis=1, kind="stable")[:, :2]
    inv = 1.0 / (np.take_along_axis(dists, near, axis=1) + 1e-4) ** 2
    return near, inv / inv.sum(axis=1, keepdims=True)


def _to_centimetre_rig(arm, body, verts) -> None:
    """Rebuild the rig like a centimetre FBX: armature object scaled 0.01 and
    turned 10 degrees about Z, bones and vertices stored x100, world unchanged."""
    import bpy
    from mathutils import Matrix, Vector

    matrix = Matrix.Rotation(math.radians(10), 4, "Z") @ Matrix.Scale(0.01, 4)
    inverse = matrix.inverted()
    bpy.context.view_layer.objects.active = arm
    bpy.ops.object.mode_set(mode="EDIT")
    bones = arm.data.edit_bones
    snapshot = {b.name: (b.head.copy(), b.tail.copy(), b.use_connect) for b in bones}
    for b in bones:  # connected children follow their parent's tail; detach while moving
        b.use_connect = False
    for b in bones:
        head, tail, _ = snapshot[b.name]
        b.head, b.tail = inverse @ head, inverse @ tail
    for b in bones:
        b.use_connect = snapshot[b.name][2]
    bpy.ops.object.mode_set(mode="OBJECT")
    arm.matrix_world = matrix
    bpy.context.view_layer.update()
    to_local = body.matrix_world.inverted()
    for vertex, position in zip(body.data.vertices, verts):
        vertex.co = to_local @ Vector(position)
    bpy.context.view_layer.update()


def _build_character(directory: Path, *, animate: bool = False, centimetre: bool = False,
                     naming: str = "mixamo") -> dict:
    import bpy

    skeleton = _skeleton(naming)

    bpy.ops.wm.read_factory_settings(use_empty=True)
    arm_data = bpy.data.armatures.new("Armature")
    arm = bpy.data.objects.new("Armature", arm_data)
    bpy.context.scene.collection.objects.link(arm)
    bpy.context.view_layer.objects.active = arm
    bpy.ops.object.mode_set(mode="EDIT")
    for name, parent, head, tail, connected in skeleton:
        eb = arm_data.edit_bones.new(name)
        eb.head, eb.tail = head, tail
        if parent:
            eb.parent = arm_data.edit_bones[parent]
            eb.use_connect = connected
    bpy.ops.object.mode_set(mode="OBJECT")

    mb = _build_mesh()
    verts = np.array(mb.verts)
    mesh = bpy.data.meshes.new("Body")
    mesh.from_pydata(verts.tolist(), [], mb.faces)
    mesh.update()
    obj = bpy.data.objects.new("Body", mesh)
    bpy.context.scene.collection.objects.link(obj)
    obj.parent = arm
    obj.modifiers.new("Armature", "ARMATURE").object = arm
    weighted = [b for b in skeleton if not b[0].endswith("_End")]
    groups = [obj.vertex_groups.new(name=b[0]) for b in weighted]
    near, weights = _nearest_bone_weights(verts, weighted)
    for v in range(len(verts)):
        for k in range(2):
            groups[near[v, k]].add([v], float(weights[v, k]), "REPLACE")

    if animate:  # a short clip bending the chest
        arm.animation_data_create()
        arm.animation_data.action = bpy.data.actions.new("Wave")
        bone = arm.pose.bones[skeleton[2][0]]  # Spine1 / spine_02.x
        bone.rotation_mode = "XYZ"
        for frame, angle in ((1, 0.0), (10, 0.4), (20, 0.0)):
            bone.rotation_euler = (0.0, angle, 0.0)
            bone.keyframe_insert("rotation_euler", frame=frame)
        bpy.context.scene.frame_start, bpy.context.scene.frame_end = 1, 20
    if centimetre:
        _to_centimetre_rig(arm, obj, verts)
    for o in (arm, obj):
        o.select_set(True)
    fbx = directory / "hero.fbx"
    glb = directory / "hero_glb.glb"
    bpy.ops.export_scene.fbx(filepath=str(fbx), use_selection=True, add_leaf_bones=False, bake_anim=animate)
    bpy.ops.export_scene.gltf(filepath=str(glb), export_format="GLB", use_selection=True, export_animations=animate)
    # A vertex touching faces of two classes (the waist ring the skirt shares) is
    # "boundary": nearest-neighbour label transfer may put it on either side.
    touching = [set() for _ in range(len(verts))]
    for face, cls in zip(mb.faces, mb.fclass):
        for v in face:
            touching[v].add(cls)
    vclass = np.array([next(iter(s)) if len(s) == 1 else "boundary" for s in touching])
    original = {}
    for v in range(len(verts)):
        original[v] = {weighted[near[v, k]][0]: float(weights[v, k]) for k in range(2)}
    return {"fbx": fbx, "glb": glb, "verts": verts, "vclass": vclass, "faces": mb.faces,
            "fclass": mb.fclass, "original_weights": original}


def _dressed_from_blender(points) -> np.ndarray:
    return core.blender_to_gltf(DRESSED_SCALE * np.asarray(points, float) + DRESSED_OFFSET)


def _write_decomposition(directory: Path, character: dict, *, wrong: bool = False) -> Path:
    """A decomposition of the synthetic character in glTF Y-up, dressed space = 0.9 * rig + offset."""
    directory.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(7)
    verts = character["verts"]
    tris, tcls = [], []
    for face, cls in zip(character["faces"], character["fclass"]):
        tris += [(face[0], face[1], face[2]), (face[0], face[2], face[3])]
        tcls += [cls, cls]
    tris = np.array(tris)
    a, b, c = verts[tris[:, 0]], verts[tris[:, 1]], verts[tris[:, 2]]
    area = 0.5 * np.linalg.norm(np.cross(b - a, c - a), axis=1)
    pick = rng.choice(len(tris), size=30000, p=area / area.sum())
    r1 = np.sqrt(rng.random(len(pick)))[:, None]
    r2 = rng.random(len(pick))[:, None]
    samples = (1 - r1) * a[pick] + r1 * (1 - r2) * b[pick] + r1 * r2 * c[pick]
    cls = np.array(tcls)[pick]
    labels = np.select([cls == "skirt", cls == "ponytail"], [2, 1], 0).astype(np.uint8)
    part_index = np.select([cls == "skirt", cls == "ponytail"], [0, 1], -1).astype(np.int16)
    positions = _dressed_from_blender(samples)
    if wrong:  # the character lying on its back: no similarity maps it onto the rig
        positions = positions @ np.array([[1, 0, 0], [0, 0, -1], [0, 1, 0.0]]).T
    np.savez_compressed(directory / "labels.npz", positions=positions.astype(np.float32),
                        labels=labels, part_index=part_index)

    angles = np.radians(np.arange(8) * 45.0)
    skirt_chains = []
    for angle in angles:
        joints = []
        for z in SKIRT_CHAIN_Z:
            rx, ry = _skirt_radius(z)
            joints.append((0.95 * rx * math.cos(angle), 0.95 * ry * math.sin(angle), z))
        skirt_chains.append({"joints": _dressed_from_blender(joints).tolist()})
    shuffled = [skirt_chains[i] for i in (5, 2, 7, 0, 3, 6, 1, 4)]
    pony_chains = [{"joints": _dressed_from_blender([(x, y, z) for y, z in PONYTAIL_CHAIN]).tolist()}
                   for x in (-0.012, 0.0, 0.012)]

    def bbox(mask):
        pts = positions[mask]
        return pts.min(axis=0).tolist(), pts.max(axis=0).tolist()

    s_min, s_max = bbox(part_index == 0)
    p_min, p_max = bbox(part_index == 1)
    lm = lambda p: _dressed_from_blender([p])[0].tolist()  # noqa: E731
    doc = {
        "format": "autorig.regen.decomposition", "version": 1, "space": "dressed_glb",
        "height": float(np.ptp(positions[:, 1])),
        "body_transform": np.eye(4).reshape(-1).tolist(),
        "labels": {"values": ["outer", "hair", "cloth"], "file": "labels.npz"},
        "parts": [
            {"name": "skirt", "kind": "cloth", "vertex_count": int(np.sum(part_index == 0)),
             "bbox_min": s_min, "bbox_max": s_max, "attach": "hips", "rigid": False},
            {"name": "ponytail", "kind": "hair", "vertex_count": int(np.sum(part_index == 1)),
             "bbox_min": p_min, "bbox_max": p_max, "attach": "head", "rigid": False},
        ],
        "groups": [
            {"name": "skirt", "kind": "cloth", "part": "skirt", "attach": "hips", "connection": "loop",
             "preset": "skirt", "chains": shuffled},
            {"name": "ponytail", "kind": "hair", "part": "ponytail", "attach": "head", "connection": "none",
             "preset": "hair", "chains": pony_chains},
        ],
        "landmarks": {
            "ground_y": float(positions[:, 1].min()), "height": float(np.ptp(positions[:, 1])),
            "neck": lm((0, 0, 1.5)), "head_center": lm((0, 0, 1.66)), "head_radius": 0.11 * DRESSED_SCALE,
            "hips": lm((0, 0, 0.95)), "shoulder_l": lm((0.17, 0, 1.42)), "shoulder_r": lm((-0.17, 0, 1.42)),
        },
        "diagnostics": {},
    }
    (directory / "decomposition.json").write_text(json.dumps(doc, indent=1))
    return directory


def _weights_module_path(directory: Path) -> Path:
    return REAL_WEIGHTS if REAL_WEIGHTS.is_file() else _write_double(directory)


@pytest.fixture(scope="module")
def bpy_mod():
    return pytest.importorskip("bpy")


@pytest.fixture(scope="module")
def rig_script(bpy_mod):
    return _load("autorig_regen_blender_cloth_rig", TOOLS / "blender_cloth_rig.py")


@pytest.fixture(scope="module")
def character(bpy_mod, tmp_path_factory):
    root = tmp_path_factory.mktemp("regen_character")
    data = _build_character(root)
    data["decomposition"] = _write_decomposition(root / "decomposition", data)
    data["weights"] = _weights_module_path(root)
    return data


def _run(rig_script, character, model_key: str, out: Path, *extra: str) -> dict:
    code = rig_script.main([
        "--model", str(character[model_key]), "--decomposition", str(character["decomposition"]),
        "--out", str(out), "--weights-module", str(character["weights"]), "--stem", "hero", *extra,
    ])
    assert code == 0
    return {
        "out": out,
        "report": json.loads((out / "cloth_rig_report.json").read_text()),
        "manifest": json.loads((out / "hero.autorig-cloth.json").read_text()),
    }


@pytest.fixture(scope="module")
def fbx_run(rig_script, character, tmp_path_factory):
    return _run(rig_script, character, "fbx", tmp_path_factory.mktemp("regen_out_fbx"))


@pytest.fixture(scope="module")
def glb_run(rig_script, character, tmp_path_factory):
    return _run(rig_script, character, "glb", tmp_path_factory.mktemp("regen_out_glb"))


def _import(path: Path) -> dict:
    """Import a model into an empty scene; return bones and per-vertex weights of its skinned meshes."""
    import bpy

    bpy.ops.wm.read_factory_settings(use_empty=True)
    if path.suffix == ".fbx":
        bpy.ops.import_scene.fbx(filepath=str(path))
    else:
        bpy.ops.import_scene.gltf(filepath=str(path))
    arm = next(o for o in bpy.context.scene.objects if o.type == "ARMATURE")
    bones = {b.name: {"parent": b.parent.name if b.parent else None,
                      "head": np.array(arm.matrix_world @ b.head_local)} for b in arm.data.bones}
    positions, weights = [], []
    for obj in bpy.context.scene.objects:
        if obj.type != "MESH" or not any(m.type == "ARMATURE" for m in obj.modifiers):
            continue
        names = {g.index: g.name for g in obj.vertex_groups}
        for v in obj.data.vertices:
            positions.append(np.array(obj.matrix_world @ v.co))
            weights.append({names[g.group]: g.weight for g in v.groups if g.weight > 0})
    return {"bones": bones, "positions": np.array(positions), "weights": weights,
            "actions": sorted(a.name for a in bpy.data.actions),
            "animated_bones": sorted({fc.data_path.split('"')[1] for a in bpy.data.actions
                                      for fc in a.fcurves if fc.data_path.startswith("pose.bones")})}


def _classify(character: dict, positions: np.ndarray) -> np.ndarray:
    """Index of the source vertex each imported vertex sits on (exports keep positions)."""
    ref = character["verts"]
    ref_sq = (ref ** 2).sum(axis=1)
    index = np.empty(len(positions), dtype=np.int64)
    for start in range(0, len(positions), 512):
        q = positions[start : start + 512]
        d2 = (q ** 2).sum(axis=1)[:, None] + ref_sq[None, :] - 2.0 * q @ ref.T
        index[start : start + 512] = np.argmin(d2, axis=1)
    assert np.linalg.norm(ref[index] - positions, axis=1).max() < 1e-4
    return index


def _chain_bone_names(manifest: dict) -> list[str]:
    return manifest_lib.chain_bone_names(manifest)


def test_manifest_validates_against_schema(fbx_run):
    manifest = fbx_run["manifest"]
    assert manifest_lib.validate_manifest(manifest) == []
    assert manifest["generator"] == "autorig-regen/0.1.0"
    assert manifest["calibration"]["bone_a"] == PREFIX + "Hips"
    assert manifest["calibration"]["bone_b"] == PREFIX + "Head"
    assert manifest["calibration"]["distance"] == pytest.approx(0.61, abs=1e-3)
    groups = {g["name"]: g for g in manifest["groups"]}
    assert set(groups) == {"skirt", "ponytail"}
    assert groups["skirt"]["connection"] == "loop" and groups["skirt"]["attach_bone"] == PREFIX + "Hips"
    assert groups["ponytail"]["attach_bone"] == PREFIX + "Head"
    assert len(groups["skirt"]["chains"]) == 8 and len(groups["ponytail"]["chains"]) == 3
    assert groups["skirt"]["chains"][0]["bones"] == ["skirt_00_0", "skirt_00_1", "skirt_00_2", "skirt_00_3", "skirt_00_end"]
    assert {p["name"] for p in manifest["presets"]} == {"skirt", "hair"}
    scale = fbx_run["report"]["length_scale"]
    for preset in manifest["presets"]:  # the built-in values, radii scaled to the character's height
        assert list(preset) == ["name", *manifest_lib.PRESET_FIELDS]
        base = manifest_lib.BUILTIN_PRESETS[preset["name"]]
        for field in manifest_lib.PRESET_FIELDS:
            factor = scale if field in manifest_lib.PRESET_LENGTH_FIELDS else 1.0
            assert preset[field] == pytest.approx(base[field] * factor, abs=1e-6), (preset["name"], field)
    colliders = {c["name"]: c for c in manifest["colliders"]}
    assert {"head", "hips", "spine", "chest", "thigh_l", "thigh_r", "calf_l", "calf_r",
            "upperarm_l", "upperarm_r", "forearm_l", "forearm_r"} <= set(colliders)
    assert colliders["head"]["shape"] == "sphere"
    assert colliders["head"]["radius"] == pytest.approx(0.11, rel=0.25)
    # Anchored on the rig's own head-top leaf; the centre is mid-skull (z ~ 1.66).
    assert colliders["head"]["bone"] == PREFIX + "Head" and colliders["head"]["to_bone"] == PREFIX + "HeadTop_End"
    assert 1.56 + colliders["head"]["t"] * 0.20 == pytest.approx(1.66, abs=0.03)
    assert colliders["thigh_l"]["radius"] == pytest.approx(0.07, rel=0.35)
    assert all(c["tag"] == "body" for c in manifest["colliders"])
    alias = json.loads((fbx_run["out"] / "hero_cloth.autorig-cloth.json").read_text())
    assert alias == manifest
    jsonschema = pytest.importorskip("jsonschema")
    jsonschema.validate(manifest, json.loads(SCHEMA_PATH.read_text()))


def test_report_records_mapping_alignment_and_counts(fbx_run):
    report = fbx_run["report"]
    assert report["ok"] is True
    slots = report["bone_mapping"]["slots"]
    for slot, bone in (("hips", "Hips"), ("spine", "Spine"), ("chest", "Spine1"), ("upper_chest", "Spine2"),
                       ("neck", "Neck"), ("head", "Head"), ("upper_leg_l", "LeftUpLeg"),
                       ("lower_leg_r", "RightLeg"), ("upper_arm_l", "LeftArm"), ("hand_r", "RightHand")):
        assert slots[slot]["bone"] == PREFIX + bone
    assert slots["hips"]["method"] == "name"
    alignment = report["alignment"]
    assert alignment["scale"] == pytest.approx(1 / DRESSED_SCALE, rel=0.01)
    assert alignment["rotation_deg"] < 1.0
    assert alignment["residual_rel"] < alignment["threshold_rel"]
    groups = {g["name"]: g for g in report["groups"]}
    assert groups["skirt"]["chains"] == 8 and groups["skirt"]["bones"] == 40
    assert groups["skirt"]["deforming_bones"] == 32 and groups["skirt"]["weighted_vertices"] > 100
    assert groups["ponytail"]["chains"] == 3 and groups["ponytail"]["weighted_vertices"] > 50
    parts = {p["part"]: p for p in report["parts"]}
    assert parts["skirt"]["attach_origin_method"] == "edge_border_outer"  # the skirt shares the waist ring
    assert np.allclose(parts["skirt"]["attach_origin"], (0, 0, 0.97), atol=0.03)
    # The ponytail root ring sits inside the head, so it is labelled outer and the
    # next ring borders it: the origin is the back of the head.
    assert np.allclose(parts["ponytail"]["attach_origin"], (0, 0.11, 1.68), atol=0.04)
    counts = report["label_transfer"]["part_vertex_counts"]
    assert 6 * 32 <= counts["skirt"] <= 7 * 32  # skirt rings, plus some of the shared waist ring
    assert 5 * 12 <= counts["ponytail"] <= 6 * 12  # the root ring inside the head may read as outer
    assert report["self_check"]["fbx"]["ok"] and report["self_check"]["glb"]["ok"]
    assert report["warnings"] == []


@pytest.mark.parametrize("kind", ["fbx", "glb"])
def test_exported_bones_parents_and_names(bpy_mod, fbx_run, kind):
    manifest = fbx_run["manifest"]
    scene = _import(fbx_run["out"] / f"hero_cloth.{kind}")
    bones = scene["bones"]
    for name in manifest_lib.manifest_bone_names(manifest):
        assert name in bones, name
    for group in manifest["groups"]:
        for chain in group["chains"]:
            names = chain["bones"]
            assert bones[names[0]]["parent"] == group["attach_bone"]
            for parent, child in zip(names[:-1], names[1:]):
                assert bones[child]["parent"] == parent
            for name in names:
                assert name.isascii() and len(name) <= 60


def test_loop_chains_are_in_angular_order(bpy_mod, fbx_run):
    manifest = fbx_run["manifest"]
    scene = _import(fbx_run["out"] / "hero_cloth.glb")
    skirt = next(g for g in manifest["groups"] if g["name"] == "skirt")
    roots = np.array([scene["bones"][c["bones"][0]]["head"] for c in skirt["chains"]])
    centre = roots.mean(axis=0)
    angles = np.degrees(np.arctan2(roots[:, 1] - centre[1], roots[:, 0] - centre[0]))
    steps = np.mod(np.diff(np.concatenate([angles, angles[:1]])), 360.0)
    # One turn in one direction, every neighbour one chain on (the waist is an
    # ellipse, so the steps alternate around 45 degrees).
    assert np.all((steps > 20.0) & (steps < 70.0)), angles
    assert steps.sum() == pytest.approx(360.0, abs=1e-6)
    # The decomposition listed them shuffled; the tool re-sorted them, starting at the front (-Y).
    assert _report_order_changed(fbx_run["report"])
    assert np.argmin(roots[:, 1]) == 0


def _report_order_changed(report: dict) -> bool:
    skirt = next(g for g in report["groups"] if g["name"] == "skirt")
    return skirt["chain_order_from_decomposition"] != list(range(8))


def _check_weights(character: dict, scene: dict, manifest: dict, exact_body: bool):
    source = _classify(character, scene["positions"])
    vclass = character["vclass"][source]
    chain_bones = set(_chain_bone_names(manifest))
    skirt_bones = {b for b in chain_bones if b.startswith("skirt_")}
    pony_bones = {b for b in chain_bones if b.startswith("ponytail_")}
    end_bones = {b for b in chain_bones if b.endswith("_end")}
    moving = np.isin(vclass, ["skirt", "ponytail"])
    for weights in (scene["weights"][i] for i in np.nonzero(moving)[0]):
        nonzero = {k: w for k, w in weights.items() if w > 1e-6}
        assert 1 <= len(nonzero) <= 4
        assert sum(nonzero.values()) == pytest.approx(1.0, abs=2e-3)
        assert not (set(nonzero) & end_bones)
    # Dominated by the part's own chain bones. Near the root the weights module
    # fades into the attach bone by design, so the strict rule starts below the
    # first chain joint (skirt z < 0.86, ponytail z < 1.58).
    positions = scene["positions"]
    for cls, bones, deep_z in (("skirt", skirt_bones, 0.86), ("ponytail", pony_bones, 1.58)):
        rows = np.nonzero(vclass == cls)[0]
        top = [max(scene["weights"][i], key=scene["weights"][i].get) for i in rows]
        dominated = sum(t in bones for t in top)
        assert dominated >= 0.8 * len(rows), (cls, dominated, len(rows))
        for i, t in zip(rows, top):
            if positions[i][2] < deep_z:
                assert t in bones, (cls, positions[i], scene["weights"][i])
    # Body vertices keep their weights exactly. The synthetic body continues
    # under the skirt (a real dressed/retopo mesh is one outer shell), so hidden
    # body vertices within 3 cm of a part surface may legitimately read as cloth.
    part_verts = character["verts"][np.isin(character["vclass"], ["skirt", "ponytail"])]
    body_rows = np.nonzero(vclass == "body")[0]
    changed_far, changed_near, near_total = 0, 0, 0
    for i in body_rows:
        near = np.min(np.linalg.norm(part_verts - positions[i], axis=1)) < 0.03
        near_total += int(near)
        original = character["original_weights"][source[i]]
        weights = {k: w for k, w in scene["weights"][i].items() if w > 1e-6}
        tol = 1e-5 if exact_body else 2e-3
        same = set(weights) <= set(original) and all(
            abs(weights.get(bone, 0.0) - w) <= tol for bone, w in original.items()
        )
        if not same:
            if near:
                changed_near += 1
            else:
                changed_far += 1
                assert same, (positions[i], weights, original)
    assert changed_far == 0
    assert changed_near <= max(2, 0.05 * near_total), (changed_near, near_total)
    assert len(body_rows) - near_total > 800  # the strict rule covers most of the body


def test_fbx_output_weights(bpy_mod, character, fbx_run):
    scene = _import(fbx_run["out"] / "hero_cloth.fbx")
    _check_weights(character, scene, fbx_run["manifest"], exact_body=True)


def test_glb_input_works_and_glb_output_weights(bpy_mod, character, glb_run):
    report, manifest = glb_run["report"], glb_run["manifest"]
    assert report["ok"] and report["alignment"]["scale"] == pytest.approx(1 / DRESSED_SCALE, rel=0.01)
    assert report["warnings"] == []
    assert report["bone_mapping"]["slots"]["head"]["bone"] == PREFIX + "Head"
    assert {g["name"] for g in manifest["groups"]} == {"skirt", "ponytail"}
    scene = _import(glb_run["out"] / "hero_cloth.glb")
    for name in manifest_lib.manifest_bone_names(manifest):
        assert name in scene["bones"]
    _check_weights(character, scene, manifest, exact_body=False)


def test_animated_input_keeps_its_actions(rig_script, tmp_path):
    data = _build_character(tmp_path, animate=True)
    decomposition = _write_decomposition(tmp_path / "decomposition", data)
    out = tmp_path / "out"
    weights = _weights_module_path(tmp_path)
    for model in ("fbx", "glb"):
        assert rig_script.main(["--model", str(data[model]), "--decomposition", str(decomposition),
                                "--out", str(out / model), "--weights-module", str(weights), "--stem", "hero"]) == 0
    source_fbx, source_glb = _import(data["fbx"]), _import(data["glb"])
    assert len(source_fbx["actions"]) == 1 and len(source_glb["actions"]) == 1
    for model, source in (("fbx", source_fbx), ("glb", source_glb)):
        report = json.loads((out / model / "cloth_rig_report.json").read_text())
        assert len(report["exported_actions"]) == 1
        for kind in ("fbx", "glb"):
            result = _import(out / model / f"hero_cloth.{kind}")
            assert len(result["actions"]) == 1, (model, kind, result["actions"])
            assert PREFIX + "Spine1" in result["animated_bones"]
            assert set(source["animated_bones"]) <= set(result["animated_bones"])
            assert "skirt_00_0" in result["bones"]
    # FBX in, FBX out: the clip keeps its name instead of growing an "Armature|" prefix.
    assert _import(out / "fbx" / "hero_cloth.fbx")["actions"] == source_fbx["actions"]


def test_centimetre_armature_with_object_transform(rig_script, tmp_path):
    """Converter FBX files often carry a 0.01-scaled (and turned) armature object."""
    data = _build_character(tmp_path, centimetre=True)
    decomposition = _write_decomposition(tmp_path / "decomposition", data)
    out = tmp_path / "out"
    assert rig_script.main(["--model", str(data["fbx"]), "--decomposition", str(decomposition), "--out", str(out),
                            "--weights-module", str(_weights_module_path(tmp_path)), "--stem", "hero"]) == 0
    manifest = json.loads((out / "hero.autorig-cloth.json").read_text())
    report = json.loads((out / "cloth_rig_report.json").read_text())
    assert report["warnings"] == []
    assert manifest["calibration"]["distance"] == pytest.approx(0.61, abs=1e-3)  # world meters, not armature units
    head = next(c for c in manifest["colliders"] if c["name"] == "head")
    assert head["radius"] == pytest.approx(0.11, rel=0.25)
    scene = _import(out / "hero_cloth.fbx")
    skirt = next(g for g in manifest["groups"] if g["name"] == "skirt")
    roots = np.array([scene["bones"][c["bones"][0]]["head"] for c in skirt["chains"]])
    assert np.allclose(roots[:, 2], SKIRT_CHAIN_Z[0], atol=0.01)
    _check_weights(data, scene, manifest, exact_body=True)


def test_auto_rig_pro_names_with_twist_bones(rig_script, tmp_path):
    """The converter's real naming (root.x, thigh_stretch.l, ...) with twist bones, GLB in."""
    data = _build_character(tmp_path, naming="arp")
    decomposition = _write_decomposition(tmp_path / "decomposition", data)
    out = tmp_path / "out"
    assert rig_script.main(["--model", str(data["glb"]), "--decomposition", str(decomposition), "--out", str(out),
                            "--weights-module", str(_weights_module_path(tmp_path)), "--stem", "hero"]) == 0
    manifest = json.loads((out / "hero.autorig-cloth.json").read_text())
    report = json.loads((out / "cloth_rig_report.json").read_text())
    assert report["warnings"] == []
    mapping = report["bone_mapping"]
    assert mapping["flavour"] == "auto-rig-pro"
    expected = {"hips": "root.x", "spine": "spine_01.x", "chest": "spine_02.x", "upper_chest": "spine_03.x",
                "neck": "neck.x", "head": "head.x", "upper_leg_l": "thigh_stretch.l", "lower_leg_r": "leg_stretch.r",
                "upper_arm_l": "arm_stretch.l", "lower_arm_r": "forearm_stretch.r", "hand_l": "hand.l"}
    assert {k: mapping["slots"][k]["bone"] for k in expected} == expected
    assert manifest["calibration"]["bone_a"] == "root.x" and manifest["calibration"]["bone_b"] == "head.x"
    attach = {g["name"]: g["attach_bone"] for g in manifest["groups"]}
    assert attach == {"skirt": "root.x", "ponytail": "head.x"}
    colliders = {c["name"]: c for c in manifest["colliders"]}
    assert (colliders["thigh_l"]["bone"], colliders["thigh_l"]["to_bone"]) == ("thigh_stretch.l", "leg_stretch.l")
    # head.x has no child above it, so a non-deforming head-top leaf is added for the sphere.
    assert (colliders["head"]["bone"], colliders["head"]["to_bone"]) == ("head.x", "cloth_head_top")
    assert report["colliders"]["measured"]["head"]["helper_bone"] is True
    # The twist bones' vertices count toward the limb they lie on.
    assert report["colliders"]["measured"]["thigh_l"]["method"] == "percentile60"
    assert colliders["thigh_l"]["radius"] == pytest.approx(0.07, rel=0.35)
    scene = _import(out / "hero_cloth.glb")
    _check_weights(data, scene, manifest, exact_body=False)


def test_wrong_transform_fails_with_clear_error(rig_script, character, tmp_path):
    decomposition = _write_decomposition(tmp_path / "wrong", character, wrong=True)
    out = tmp_path / "out"
    with pytest.raises(SystemExit) as info:
        rig_script.main(["--model", str(character["fbx"]), "--decomposition", str(decomposition),
                         "--out", str(out), "--weights-module", str(character["weights"]), "--stem", "hero"])
    assert info.value.code == 1
    error = json.loads((out / "error.json").read_text())
    assert error["stage"] == "alignment"
    assert "cannot align the decomposition" in error["error"] and "% of character height" in error["error"]
    assert not (out / "hero.autorig-cloth.json").exists()
    assert json.loads((out / "cloth_rig_report.json").read_text())["ok"] is False


def test_runner_end_to_end_with_a_blender_shim(bpy_mod, character, tmp_path):
    """The runner around a 'blender' that executes the script with this interpreter's bpy."""
    shim = _fake_blender(tmp_path, """
        import runpy, sys
        args = sys.argv[1:]
        script = args[args.index("--python") + 1]
        sys.argv = [script] + args[args.index("--"):]
        runpy.run_path(script, run_name="__main__")
    """)
    result = runner.run_cloth_rig(character["glb"], character["decomposition"], tmp_path / "out",
                                  blender_bin=shim, weights_module=character["weights"], stem="hero", timeout=600)
    assert result.returncode == 0
    assert result.manifest["groups"] and result.fbx.is_file() and result.glb.is_file()
    assert result.report["ok"] is True
    assert "[cloth-rig] OK" in result.stdout_tail
