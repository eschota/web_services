"""Synthetic T-pose mannequins for tests and smoke checks.

``make_pair()`` builds, in "dressed space" (height 1, feet on y=0, facing +Z):

* the base body B: lathe torso, cylinder neck, sphere head, capsule arms and
  legs, box feet (pointing forward);
* the dressed character A: B's skin with a tight shirt (torso offset by 1% of
  the height) flowing into a flared, open-bottom skirt from the hips to the
  knees (one connected surface, like a Hunyuan dress), hair (the scalp region
  of the head pushed out 3% of the height) with a ponytail hanging down the
  back, and optionally a cape (a partial cylindrical sheet on the back).

B is then moved by a random similarity (scale 0.8-1.25, translation, <= 3 deg
rotation), mimicking Hunyuan normalising each reconstruction independently.
Every dressed vertex carries a ground-truth tag.

    python -m regen.cli synth --out /tmp/regen-synth [--cape]
"""

from __future__ import annotations

import dataclasses
import math
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np

TAGS: Tuple[str, ...] = ("skin", "shirt", "skirt", "hair_cap", "ponytail", "cape", "boot")

# (y, half-width X, half-depth Z) of the torso, bottom to top
_TORSO = np.array(
    [
        (0.435, 0.05, 0.035),
        (0.445, 0.10, 0.065),
        (0.46, 0.125, 0.078),
        (0.50, 0.14, 0.085),
        (0.56, 0.135, 0.080),
        (0.62, 0.13, 0.078),
        (0.68, 0.135, 0.082),
        (0.72, 0.14, 0.085),
        (0.76, 0.13, 0.080),
        (0.785, 0.10, 0.065),
        (0.80, 0.05, 0.042),
    ]
)

HEAD_CENTER = np.array([0.0, 0.9, 0.0])
HEAD_RADIUS = 0.1
HAIR_THICKNESS = 0.03
SHIRT_OFFSET = 0.01
SKIRT_TOP = 0.54
SKIRT_HEM = 0.28


@dataclasses.dataclass
class SyntheticPair:
    body: "object"  # trimesh.Trimesh in dressed space (ground truth pose)
    body_moved: "object"  # trimesh.Trimesh after the random similarity (what B.glb holds)
    dressed: "object"  # trimesh.Trimesh
    dressed_tags: np.ndarray  # (V,) int index into TAGS
    body_to_dressed: np.ndarray  # 4x4 true transform: B.glb coords -> dressed coords
    scale: float  # scale applied to B (body_moved = scale * R @ body + t)
    rotation_deg: float
    height: float = 1.0

    def tag_mask(self, *names: str) -> np.ndarray:
        ids = [TAGS.index(n) for n in names]
        return np.isin(self.dressed_tags, ids)


# ------------------------------------------------------------------ builders


def _torso_profile(y: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    return np.interp(y, _TORSO[:, 0], _TORSO[:, 1]), np.interp(y, _TORSO[:, 0], _TORSO[:, 2])


def lathe(
    ys: Sequence[float],
    half_x: Sequence[float],
    half_z: Sequence[float],
    n_theta: int,
    *,
    center_xz: Tuple[float, float] = (0.0, 0.0),
    theta_range: Optional[Tuple[float, float]] = None,
    cap_bottom=False,
    cap_top=False,
) -> Tuple[np.ndarray, np.ndarray]:
    """Surface of revolution with elliptic cross-sections, outward normals.

    ``ys`` must increase. ``theta_range`` (radians, 0 = +Z, pi/2 = +X) makes
    an open sheet instead of a closed tube. ``cap_bottom`` / ``cap_top``
    close the ends with a pole: True puts it at the end ring's height, a
    number at that height.
    """
    ys = np.asarray(ys, dtype=np.float64)
    ax = np.asarray(half_x, dtype=np.float64)
    bz = np.asarray(half_z, dtype=np.float64)
    rows = len(ys)
    closed = theta_range is None
    if closed:
        theta = np.arange(n_theta) / n_theta * 2.0 * math.pi
    else:
        theta = np.linspace(theta_range[0], theta_range[1], n_theta)
    cols = len(theta)
    x = center_xz[0] + ax[:, None] * np.sin(theta)[None, :]
    z = center_xz[1] + bz[:, None] * np.cos(theta)[None, :]
    y = np.repeat(ys[:, None], cols, axis=1)
    vertices = np.stack([x, y, z], axis=-1).reshape(-1, 3)
    idx = np.arange(rows * cols).reshape(rows, cols)
    j_next = (np.arange(cols) + 1) % cols if closed else np.arange(1, cols)
    j_cur = np.arange(cols) if closed else np.arange(cols - 1)
    a = idx[:-1][:, j_cur]
    b = idx[:-1][:, j_next]
    c = idx[1:][:, j_next]
    d = idx[1:][:, j_cur]
    faces = np.concatenate(
        [np.stack([a, b, c], axis=-1).reshape(-1, 3), np.stack([a, c, d], axis=-1).reshape(-1, 3)]
    )
    extra = []
    if cap_bottom is not False and closed:
        pole = len(vertices) + len(extra)
        extra.append([center_xz[0], ys[0] if cap_bottom is True else float(cap_bottom), center_xz[1]])
        faces = np.concatenate([faces, np.stack([np.full(cols, pole), idx[0][j_next], idx[0][j_cur]], axis=-1)])
    if cap_top is not False and closed:
        pole = len(vertices) + len(extra)
        extra.append([center_xz[0], ys[-1] if cap_top is True else float(cap_top), center_xz[1]])
        faces = np.concatenate([faces, np.stack([np.full(cols, pole), idx[-1][j_cur], idx[-1][j_next]], axis=-1)])
    if extra:
        vertices = np.concatenate([vertices, np.asarray(extra)])
    return vertices, faces.astype(np.int64)


def _capsule(p0, p1, radius: float, n_theta: int):
    """Capsule between two points built as a lathe along its axis, with rings
    spaced like its circumference (evenly sized triangles, as in a Hunyuan
    mesh; trimesh's own capsule has no vertices along its cylinder)."""
    import trimesh

    p0 = np.asarray(p0, dtype=np.float64)
    p1 = np.asarray(p1, dtype=np.float64)
    axis = p1 - p0
    length = float(np.linalg.norm(axis))
    step = 2.0 * math.pi * radius / n_theta
    n_cap = max(3, n_theta // 4)
    phi = np.linspace(0.0, 0.5 * math.pi, n_cap + 1)[1:]  # from the pole towards the equator
    cyl = np.linspace(-0.5 * length, 0.5 * length, max(2, int(math.ceil(length / step))) + 1)
    ys = np.concatenate([-0.5 * length - radius * np.cos(phi), cyl[1:-1], (0.5 * length + radius * np.cos(phi))[::-1]])
    rs = np.concatenate([radius * np.sin(phi), np.full(len(cyl) - 2, radius), (radius * np.sin(phi))[::-1]])
    vertices, faces = lathe(
        ys, rs, rs, n_theta, cap_bottom=-0.5 * length - radius, cap_top=0.5 * length + radius
    )
    mesh = trimesh.Trimesh(vertices, faces, process=False)
    transform = trimesh.geometry.align_vectors([0.0, 1.0, 0.0], axis / length)
    transform[:3, 3] = 0.5 * (p0 + p1)
    mesh.apply_transform(transform)
    return mesh


def _trimesh(vertices, faces):
    import trimesh

    return trimesh.Trimesh(vertices=vertices, faces=faces, process=False)


def _concat(meshes: List[Tuple[object, int]]):
    """Concatenate (mesh, tag) pairs into one mesh plus per-vertex tags."""
    import trimesh

    vertices = []
    faces = []
    tags = []
    offset = 0
    for mesh, tag in meshes:
        v = np.asarray(mesh.vertices)
        f = np.asarray(mesh.faces)
        vertices.append(v)
        faces.append(f + offset)
        tags.append(np.asarray(tag if np.ndim(tag) else np.full(len(v), tag), dtype=np.int64))
        offset += len(v)
    mesh = trimesh.Trimesh(np.concatenate(vertices), np.concatenate(faces), process=False)
    return mesh, np.concatenate(tags)


def _keep_faces(mesh, keep: np.ndarray):
    import trimesh

    faces = np.asarray(mesh.faces)[keep]
    used, local = np.unique(faces.reshape(-1), return_inverse=True)
    return trimesh.Trimesh(np.asarray(mesh.vertices)[used], local.reshape(-1, 3), process=False), used


def _body_parts(res: float, arm_angle_deg: float = 0.0) -> Dict[str, object]:
    import trimesh

    n_theta = max(24, int(96 * res))
    ys = np.linspace(_TORSO[0, 0], _TORSO[-1, 0], max(12, int(48 * res)))
    ax, bz = _torso_profile(ys)
    torso = _trimesh(*lathe(ys, ax, bz, n_theta, cap_bottom=True, cap_top=True))
    neck_ys = np.linspace(0.775, 0.845, max(4, int(10 * res)))
    neck = _trimesh(*lathe(neck_ys, np.full(len(neck_ys), 0.042), np.full(len(neck_ys), 0.042), max(16, int(48 * res)), cap_bottom=True, cap_top=True))
    subdiv = 3 if res < 0.8 else (4 if res < 1.6 else 5)
    head = trimesh.creation.icosphere(subdivisions=subdiv, radius=HEAD_RADIUS)
    head.apply_translation(HEAD_CENTER)
    around = max(16, int(48 * res))
    drop = math.radians(arm_angle_deg)  # 0 = T-pose; ~45 = A-pose
    arms = [
        _capsule(
            (s * 0.145, 0.745, 0.0),
            (s * (0.145 + 0.35 * math.cos(drop)), 0.745 - 0.35 * math.sin(drop), 0.0),
            0.035,
            around,
        )
        for s in (-1.0, 1.0)
    ]
    legs = [_capsule((s * 0.075, 0.08, 0.0), (s * 0.075, 0.44, 0.0), 0.056, around) for s in (-1.0, 1.0)]
    feet = []
    for s in (-1.0, 1.0):
        foot = trimesh.creation.box(extents=(0.085, 0.045, 0.16))
        foot = foot.subdivide().subdivide()
        foot.apply_translation((s * 0.075, 0.0225, 0.045))
        feet.append(foot)
    return {"torso": torso, "neck": neck, "head": head, "arms": arms, "legs": legs, "feet": feet}


def make_body(resolution: float = 1.0, *, arm_angle_deg: float = 0.0):
    """The base body B in dressed space (a single concatenated mesh).
    ``arm_angle_deg`` lowers the arms from the T-pose (45 is an A-pose)."""
    parts = _body_parts(resolution, arm_angle_deg)
    items = [(parts["torso"], 0), (parts["neck"], 0), (parts["head"], 0)]
    items += [(m, 0) for m in parts["arms"] + parts["legs"] + parts["feet"]]
    mesh, _ = _concat(items)
    return mesh


def make_dressed(resolution: float = 1.0, *, cape: bool = False, ponytail: bool = True, boots: bool = False):
    """The dressed character A and its per-vertex ground-truth tags.

    ``boots``: loose boot shafts around the shins, from the floor up (they
    must stay rigid: footwear does not swing)."""
    import trimesh

    res = resolution
    parts = _body_parts(res)
    skin, shirt, skirt, hair_cap, pony, cape_tag, boot_tag = (TAGS.index(t) for t in TAGS)
    items: List[Tuple[object, object]] = []

    # dress: flared skirt (bottom rows) flowing into a tight shirt (top rows)
    n_theta = max(32, int(128 * res))
    skirt_ys = np.linspace(SKIRT_HEM, SKIRT_TOP, max(8, int(36 * res)))
    t = (SKIRT_TOP - skirt_ys) / (SKIRT_TOP - SKIRT_HEM)
    skirt_ax = 0.19 + t * (0.26 - 0.19)
    skirt_bz = 0.135 + t * (0.22 - 0.135)
    shirt_ys = np.linspace(0.555, 0.795, max(8, int(32 * res)))
    shirt_ax, shirt_bz = _torso_profile(shirt_ys)
    ys = np.concatenate([skirt_ys, shirt_ys])
    ax = np.concatenate([skirt_ax, shirt_ax + SHIRT_OFFSET])
    bz = np.concatenate([skirt_bz, shirt_bz + SHIRT_OFFSET])
    dv, df = lathe(ys, ax, bz, n_theta)
    dress_tags = np.where(dv[:, 1] <= SKIRT_TOP + 1e-9, skirt, shirt)
    items.append((_trimesh(dv, df), dress_tags))

    # head: scalp pushed out into a hair volume above a hairline that is high
    # at the forehead and low at the nape
    head = parts["head"].copy()
    rel = np.asarray(head.vertices) - HEAD_CENTER
    direction = rel / np.linalg.norm(rel, axis=1, keepdims=True)
    theta = np.arctan2(rel[:, 0], rel[:, 2])
    hairline = 0.84 + 0.09 * (1.0 + np.cos(theta)) * 0.5
    is_hair = np.asarray(head.vertices)[:, 1] > hairline
    new = HEAD_CENTER + direction * np.where(is_hair, HEAD_RADIUS + HAIR_THICKNESS, HEAD_RADIUS)[:, None]
    head = trimesh.Trimesh(new, np.asarray(head.faces), process=False)
    items.append((head, np.where(is_hair, hair_cap, skin)))

    if ponytail:
        around = max(12, int(32 * res))
        if cape:
            tail = _capsule((0.0, 0.87, -0.15), (0.0, 0.56, -0.32), 0.03, around)
        else:
            tail = _capsule((0.0, 0.87, -0.14), (0.0, 0.52, -0.19), 0.03, around)
        items.append((tail, pony))

    if cape:
        cape_ys = np.linspace(0.35, 0.77, max(8, int(36 * res)))
        drop = 0.77 - cape_ys
        cv, cf = lathe(
            cape_ys,
            0.17 + drop * 0.31,
            0.13 + drop * 0.31,
            max(12, int(48 * res)),
            theta_range=(math.radians(105.0), math.radians(255.0)),
        )
        items.append((_trimesh(cv, cf), cape_tag))

    if boots:
        boot_ys = np.linspace(0.0, 0.22, max(6, int(24 * res)))
        for side in (-1.0, 1.0):
            bv, bf = lathe(
                boot_ys,
                np.full(len(boot_ys), 0.101),
                np.full(len(boot_ys), 0.101),
                max(16, int(48 * res)),
                center_xz=(side * 0.075, 0.02),
            )
            items.append((_trimesh(bv, bf), boot_tag))

    items.append((parts["neck"], skin))
    items += [(m, skin) for m in parts["arms"] + parts["feet"]]
    for leg in parts["legs"]:  # only the shins show below the skirt
        keep = np.all(np.asarray(leg.vertices)[np.asarray(leg.faces)][:, :, 1] < 0.30, axis=1)
        shin, _ = _keep_faces(leg, keep)
        items.append((shin, skin))
    return _concat(items)


def random_similarity(rng: np.random.Generator, max_rotation_deg: float = 3.0) -> Tuple[float, np.ndarray, np.ndarray, float]:
    """(scale, rotation, translation, rotation angle in degrees)."""
    from .geometry import axis_angle_matrix

    scale = float(rng.uniform(0.8, 1.25))
    axis = rng.normal(size=3)
    angle = math.radians(float(rng.uniform(0.5, max_rotation_deg)))
    rotation = axis_angle_matrix(axis, angle)
    translation = rng.uniform(-0.3, 0.3, size=3)
    return scale, rotation, translation, math.degrees(angle)


def make_pair(
    seed: int = 0,
    resolution: float = 1.0,
    *,
    cape: bool = False,
    ponytail: bool = True,
    boots: bool = False,
    move_body: bool = True,
) -> SyntheticPair:
    import trimesh

    rng = np.random.default_rng(seed)
    body = make_body(resolution)
    dressed, tags = make_dressed(resolution, cape=cape, ponytail=ponytail, boots=boots)
    if move_body:
        scale, rotation, translation, angle = random_similarity(rng)
    else:
        scale, rotation, translation, angle = 1.0, np.eye(3), np.zeros(3), 0.0
    moved_vertices = scale * np.asarray(body.vertices) @ rotation.T + translation
    body_moved = trimesh.Trimesh(moved_vertices, np.asarray(body.faces), process=False)
    inverse = np.eye(4)
    inverse[:3, :3] = rotation.T / scale
    inverse[:3, 3] = -(rotation.T @ translation) / scale
    return SyntheticPair(
        body=body,
        body_moved=body_moved,
        dressed=dressed,
        dressed_tags=tags,
        body_to_dressed=inverse,
        scale=scale,
        rotation_deg=angle,
    )


def textured_copy(mesh, seed: int = 0, size: int = 64):
    """The mesh with planar-projected UVs and a small random base-colour
    texture, like a (tiny) Hunyuan PBR output."""
    import trimesh
    from PIL import Image
    from trimesh.visual import TextureVisuals
    from trimesh.visual.material import PBRMaterial

    rng = np.random.default_rng(seed)
    vertices = np.asarray(mesh.vertices)
    lo = vertices.min(axis=0)
    span = np.maximum(vertices.max(axis=0) - lo, 1e-9)
    uv = (vertices[:, [0, 1]] - lo[[0, 1]]) / span[[0, 1]]
    image = Image.fromarray(rng.integers(0, 255, size=(size, size, 3), dtype=np.uint8))
    material = PBRMaterial(name="synthetic", baseColorTexture=image, metallicFactor=0.0, roughnessFactor=0.9)
    return trimesh.Trimesh(
        vertices, np.asarray(mesh.faces), visual=TextureVisuals(uv=uv, material=material), process=False
    )


def write_pair(pair: SyntheticPair, out_dir, *, textured: bool = False) -> Tuple[Path, Path]:
    """Write ``dressed.glb`` and ``body.glb`` (one mesh each); returns their paths.

    ``textured`` gives both meshes UVs and a base-colour texture so material
    round trips can be checked."""
    from .mesh_io import save_trimesh_glb

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    dressed_path = out / "dressed.glb"
    body_path = out / "body.glb"
    dressed = textured_copy(pair.dressed, 1) if textured else pair.dressed
    body = textured_copy(pair.body_moved, 2) if textured else pair.body_moved
    save_trimesh_glb(dressed_path, [("dressed", dressed)])
    save_trimesh_glb(body_path, [("body", body)])
    return dressed_path, body_path
