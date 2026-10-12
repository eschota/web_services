"""Monster rig · V3: the monster's own procedural clip set (Idle breathing, Prowl, crouched Walk, Attack lunge) on the
pinned chain with the crouch as the rest pose, and rig_first.animate using it (option clip_set own|mixamo|both).
python patch_monster_clips.py <mt dir>"""
import pathlib
import sys

root = pathlib.Path(sys.argv[1])

CLIPS = '''

# ------------------------------------------------------------------------------------------------ the monster's own clips
# Owner 2026-10-13: a crouched / digitigrade monster gets its own clip set, built on the pinned chain with the crouch as
# the rest pose (every rotation is a delta from rest, so the knees stay bent and the arms keep hanging), no Mixamo
# shoulder motion. Procedural: the fallback behind video-MT; `clips(doc, opts)` -> append_clips dicts.
CLIP_SPEC = {
    "clip_set": {"values": ("own", "mixamo", "both"), "default": "own",
                 "doc": "a monster's clips: its own procedural set, the Mixamo library (bind-relative), or both"},
    "fps": {"min": 10, "max": 60, "default": 30},
    "walk_period_s": {"min": 0.4, "max": 3.0, "default": 1.3, "doc": "crouched walk cycle"},
    "prowl_period_s": {"min": 0.6, "max": 4.0, "default": 2.2, "doc": "prowl (slow stalking walk) cycle"},
    "stride_deg": {"min": 0.0, "max": 60.0, "default": 16.0, "doc": "thigh swing forward / back (walk)"},
    "knee_deg": {"min": 0.0, "max": 60.0, "default": 14.0, "doc": "extra knee flexion in the swing (walk)"},
    "arm_swing_deg": {"min": 0.0, "max": 45.0, "default": 10.0, "doc": "arm counter-swing (walk)"},
    "bob_pct_H": {"min": 0.0, "max": 6.0, "default": 1.2, "doc": "hips bob (walk)"},
    "breath_deg": {"min": 0.0, "max": 10.0, "default": 2.5, "doc": "idle: spine breathing"},
    "head_sway_deg": {"min": 0.0, "max": 30.0, "default": 8.0, "doc": "idle / prowl: head sway"},
    "lunge_deg": {"min": 0.0, "max": 60.0, "default": 22.0, "doc": "attack: the trunk's forward lunge"},
    "lunge_arm_deg": {"min": 0.0, "max": 90.0, "default": 45.0, "doc": "attack: the arms swing forward"},
    "lunge_pct_H": {"min": 0.0, "max": 20.0, "default": 6.0, "doc": "attack: hips travel forward"},
}


def clip_options(opts=None) -> dict:
    o, given = {}, dict(opts or {})
    for k, s in CLIP_SPEC.items():
        v = given.get(k, s["default"])
        if "values" in s:
            v = str(v)
            if v not in s["values"]:
                raise ValueError(f"monster clip option {k}: {v!r} not in {s['values']}")
        else:
            v = float(v)
            if not (s["min"] <= v <= s["max"]):
                raise ValueError(f"monster clip option {k}: {v} outside [{s['min']}, {s['max']}]")
        o[k] = v
    return o


def clip_options_for(run_dir, opts=None) -> dict:
    doc = _read(pathlib.Path(run_dir) / "monster_options.json") or {}
    env = {k: os.environ[f"MONSTER_{k.upper()}"] for k in CLIP_SPEC if f"MONSTER_{k.upper()}" in os.environ}
    return clip_options({**env, **(doc.get("clip_options") or {}), **(opts or {})})


def _qaxis(axis, deg):
    a = np.asarray(axis, np.float64)
    a = a / max(np.linalg.norm(a), 1e-12)
    h = np.radians(np.asarray(deg, np.float64)) / 2
    return np.stack([a[0] * np.sin(h), a[1] * np.sin(h), a[2] * np.sin(h), np.cos(h)], -1)


def _qmul(a, b):
    ax, ay, az, aw = a[..., 0], a[..., 1], a[..., 2], a[..., 3]
    bx, by, bz, bw = b[..., 0], b[..., 1], b[..., 2], b[..., 3]
    return np.stack([aw * bx + ax * bw + ay * bz - az * by, aw * by - ax * bz + ay * bw + az * bx,
                     aw * bz + ax * by - ay * bx + az * bw, aw * bw - ax * bx - ay * by - az * bz], -1)


def _clip(doc, name, seconds, o, fn, loop=True):
    """fn(t, ph, keys, trans, X, Y, Z, H) fills keys[bone] = [(axis, deg array)]; -> append_clips dict."""
    from . import fastrig as F
    fps = int(o["fps"])
    n = int(round(seconds * fps))
    t = np.arange(n + 1) / fps
    ph = 2 * np.pi * t / seconds
    Rc = F.yaw_matrix(F.FORWARD_YAW.get(doc.get("forward_axis") or "+z", 0.0))
    X, Y, Z = (Rc.T @ np.eye(3)[i] for i in range(3))          # canonical axes in the model frame
    H = float(doc.get("model_height_units") or 1.0)
    names = {b["name"] for b in doc.get("bones") or []}
    keys, trans = {}, {}
    fn(t, ph, keys, trans, X, Y, Z, H)
    rot = {}
    for bone, parts in keys.items():
        if bone not in names:
            continue
        q = np.tile([0.0, 0.0, 0.0, 1.0], (len(t), 1))
        for axis, deg in parts:
            q = _qmul(q, _qaxis(axis, np.broadcast_to(np.asarray(deg, np.float64), (len(t),))))
        rot[bone] = q
    out = {"name": name, "duration": float(t[-1]), "times": t.astype(np.float32), "rotations": rot, "loop": loop}
    if "Hips" in trans and "Hips" in names:
        hips = next(b for b in doc["bones"] if b["name"] == "Hips")
        parent = next((b for b in doc["bones"] if b["name"] == hips.get("parent")), None)
        rest = np.asarray(hips["head"], np.float64) - (np.asarray(parent["head"], np.float64) if parent else 0.0)
        out["hips_translation"] = rest[None] + trans["Hips"]
    return out


def clips(doc, opts=None, run_dir=None) -> list:
    """The monster's own set: Monster Idle (breathing), Monster Prowl, Monster Walk (crouched), Monster Attack (a
    lunge, one-shot). Rotations are deltas from the pinned rest pose: X = the lateral axis (flex / extend in the
    side plane), Y = yaw, in the model frame."""
    o = clip_options_for(run_dir, opts) if run_dir else clip_options(opts)
    out = []

    def lift(ph):
        return 0.5 - 0.5 * np.cos(ph)

    def idle(t, ph, keys, trans, X, Y, Z, H):
        b = o["breath_deg"]
        keys["Spine"] = [(X, -b * np.sin(ph))]
        keys["Spine1"] = [(X, -b * 0.6 * np.sin(ph))]
        keys["Neck"] = [(X, b * 0.8 * np.sin(ph))]
        keys["Head"] = [(Y, o["head_sway_deg"] * np.sin(ph * 0.5)), (X, 3.0 * np.sin(ph + 1.0))]
        for s, side in ((1, "Left"), (-1, "Right")):
            keys[f"{side}Arm"] = [(X, 2.0 * np.sin(ph + s * 0.4))]
            keys[f"{side}ForeArm"] = [(X, 3.0 * np.sin(ph + s * 0.9))]
            keys[f"{side}UpLeg"] = [(X, 1.5 * np.sin(ph))]
            keys[f"{side}Leg"] = [(X, -2.0 * np.sin(ph))]
        trans["Hips"] = np.outer(0.004 * H * np.sin(ph), Y)

    def walk(stride, knee, arms, bob, head):
        def fn(t, ph, keys, trans, X, Y, Z, H):
            for s, side in ((1, "Left"), (-1, "Right")):
                p = ph + (0.0 if s > 0 else np.pi)
                keys[f"{side}UpLeg"] = [(X, -stride * np.sin(p))]                    # thigh forward / back
                keys[f"{side}Leg"] = [(X, knee * lift(p + 0.6 * np.pi))]             # the knee flexes in the swing
                keys[f"{side}Foot"] = [(X, -0.5 * knee * lift(p + 0.6 * np.pi))]      # the foot stays level
                keys[f"{side}Arm"] = [(X, arms * np.sin(p + np.pi))]                  # arms counter-swing
                keys[f"{side}ForeArm"] = [(X, 0.5 * arms * lift(p + np.pi))]
            keys["Spine"] = [(Y, 3.0 * np.sin(ph)), (X, 2.0 * np.sin(2 * ph))]
            keys["Spine1"] = [(Y, 2.0 * np.sin(ph))]
            keys["Head"] = [(Y, head * np.sin(ph + 0.5)), (X, -2.0 * np.sin(2 * ph))]
            trans["Hips"] = np.outer(-bob / 100 * H * lift(2 * ph), Y)
        return fn

    def attack(t, ph, keys, trans, X, Y, Z, H):
        T = float(t[-1])
        u = np.clip(t / (0.45 * T), 0, 1)
        back = np.clip((t - 0.45 * T) / (0.55 * T), 0, 1)
        env = np.where(t < 0.45 * T, np.sin(np.pi / 2 * u) ** 2, (1 - back) ** 2)
        keys["Spine"] = [(X, o["lunge_deg"] * env)]
        keys["Spine1"] = [(X, 0.4 * o["lunge_deg"] * env)]
        keys["Head"] = [(X, -0.6 * o["lunge_deg"] * env)]
        for side in ("Left", "Right"):
            keys[f"{side}Arm"] = [(X, -o["lunge_arm_deg"] * env)]
            keys[f"{side}ForeArm"] = [(X, -0.5 * o["lunge_arm_deg"] * env)]
            keys[f"{side}UpLeg"] = [(X, -6.0 * env)]
            keys[f"{side}Leg"] = [(X, 10.0 * env)]
        trans["Hips"] = np.outer(o["lunge_pct_H"] / 100 * H * env, Z) + np.outer(-0.01 * H * env, Y)

    out.append(_clip(doc, "Monster Idle", 4.0, o, idle))
    out.append(_clip(doc, "Monster Prowl", o["prowl_period_s"], o,
                     walk(0.7 * o["stride_deg"], 0.8 * o["knee_deg"], 0.6 * o["arm_swing_deg"], 0.6 * o["bob_pct_H"],
                          o["head_sway_deg"])))
    out.append(_clip(doc, "Monster Walk", o["walk_period_s"], o,
                     walk(o["stride_deg"], o["knee_deg"], o["arm_swing_deg"], o["bob_pct_H"], 0.5 * o["head_sway_deg"])))
    out.append(_clip(doc, "Monster Attack", 1.4, o, attack, loop=False))
    return out
'''

p = root / "monster.py"
s = p.read_text(encoding="utf-8")
if "CLIP_SPEC" not in s:
    s = s.rstrip("\n") + "\n" + CLIPS
    p.write_text(s, encoding="utf-8", newline="\n")
    print("monster: clips added")
else:
    print("monster: clips already")

p = root / "rig_first.py"
s = p.read_text(encoding="utf-8")
old = '''    if (doc.get("monster") or {}).get("use") and (doc.get("monster") or {}).get("policy"):
        doc["retarget_reference_policy"] = doc["monster"]["policy"]   # Monster rig · V3: the crouch stays under the motion
    if str(doc.get("body_plan") or "").lower().startswith(("biped", "human")):
        for name in RT.MAIN:'''
new = '''    _mn_set = "mixamo"
    if (doc.get("monster") or {}).get("use") and (doc.get("monster") or {}).get("policy"):
        doc["retarget_reference_policy"] = doc["monster"]["policy"]   # Monster rig · V3: the crouch stays under the motion
        try:                                                     # the monster's own clip set (owner 2026-10-13)
            from . import monster as MN
            _mn_set = MN.clip_options_for(run)["clip_set"]
            if _mn_set in ("own", "both"):
                clips += MN.clips(doc, run_dir=run)
            doc["monster"]["clip_set"] = _mn_set
        except Exception as exc:                                 # noqa: BLE001
            doc["monster"]["clip_set_error"] = f"{type(exc).__name__}: {exc}"[:120]
            _mn_set = "mixamo"
    if str(doc.get("body_plan") or "").lower().startswith(("biped", "human")) and _mn_set != "own":
        for name in RT.MAIN:'''
if "_mn_set" in s:
    print("rig_first: already hooked")
else:
    assert s.count(old) == 1, s.count(old)
    s = s.replace(old, new)
    p.write_text(s, encoding="utf-8", newline="\n")
    print("rig_first: monster clip set hooked")
