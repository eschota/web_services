"""Gait gate hooks (Fable · equines, 2026-10-13): mt/gait_gate.py judges every generated reference clip against real
gait timing before it enters the library (animal_video.run_path: a failing clip is saved as quality "suspect" with the
gate's reasons, so library_clips never attaches it); the "look" clip (look around: the neck and head turn, the legs
stay) joins the generated set and replaces the procedural Look Around (animpick).
python patch_gait_gate.py <mt dir>  (copies gait_gate.py beside it; idempotent per file)"""
import pathlib
import shutil
import sys

d = pathlib.Path(sys.argv[1])
here = pathlib.Path(__file__).resolve().parent
if (here / "gait_gate.py").is_file():
    shutil.copy(here / "gait_gate.py", d / "gait_gate.py")
    print("gait_gate.py copied")


def patch_animal_video():
    p = d / "animal_video.py"
    s = p.read_text(encoding="utf-8")
    if "gait_gate" in s:
        print("animal_video: already patched (gait gate)")
        return
    old = '''    "clip": {"values": ("walk", "trot", "canter", "gallop", "rear", "idle", "run", "prowl", "attack"), "default": "walk"},'''
    new = '''    "clip": {"values": ("walk", "trot", "canter", "gallop", "rear", "idle", "run", "prowl", "attack", "look"), "default": "walk"},'''
    assert s.count(old) == 1, "clip values"
    s = s.replace(old, new)
    old = '''ONE_SHOT = ("rear", "attack")'''
    new = '''ONE_SHOT = ("rear", "attack", "look")'''
    assert s.count(old) == 1, "one shot"
    s = s.replace(old, new)
    old = '''    "trot": "trots in place as on an invisible treadmill: a full natural trot cycle, diagonal legs moving together",'''
    new = '''    "trot": "trots in place as on an invisible treadmill: a full natural trot cycle, diagonal legs moving together",
    "look": "stands still on all four legs and looks around once: turns its head and neck to the left, then to the "
            "right, then back to the front; the legs and the body do not move",'''
    assert s.count(old) == 1, "action trot"
    s = s.replace(old, new)
    old = '''    quality = "suspect" if (trunk_ptp > o["trunk_max_deg"] or limb_ptp > o["limb_max_deg"] or at_floor) else "ok"
    entry = {"schema": SCHEMA, "species": species, "what": what, "clip": o["clip"], "period_s": round(period, 4),
             "plan": plan, "loop": loop, "quality": quality,
             "quality_why": {"trunk_ptp_deg": round(trunk_ptp, 1), "limb_ptp_deg": round(limb_ptp, 1), "at_floor": at_floor},'''
    new = '''    quality = "suspect" if (trunk_ptp > o["trunk_max_deg"] or limb_ptp > o["limb_max_deg"] or at_floor) else "ok"
    gate = None
    if plan == "quadruped":                                     # Gait gate · V3 (mt/gait_gate.py): real gait timing
        try:
            from . import gait_gate as _GG
            gate = _GG.judge({"clip": o["clip"], "period_s": period, "loop": loop, "curves": cyc})
            if not gate["ok"]:
                quality = "suspect"
        except Exception as exc:                                 # noqa: BLE001 - the gate never breaks the path
            gate = {"ok": None, "why": [f"{type(exc).__name__}: {exc}"[:120]]}
    entry = {"schema": SCHEMA, "species": species, "what": what, "clip": o["clip"], "period_s": round(period, 4),
             "plan": plan, "loop": loop, "quality": quality,
             "quality_why": {"trunk_ptp_deg": round(trunk_ptp, 1), "limb_ptp_deg": round(limb_ptp, 1), "at_floor": at_floor,
                             "gait": gate},'''
    assert s.count(old) == 1, "quality anchor"
    s = s.replace(old, new)
    p.write_bytes(s.replace("\r\n", "\n").encode("utf-8"))
    assert p.read_bytes().count(bytes([8])) == 0
    print("animal_video: gait gate + look clip")


def patch_animpick():
    q = d / "animpick.py"
    t = q.read_text(encoding="utf-8")
    if '"look": "Look Around"' in t:
        print("animpick: already patched (look)")
        return
    old = 'VIDEO_CLIP_NAMES = {"walk": "Walk", "trot": "Trot", "canter": "Canter", "gallop": "Gallop", "rear": "Rear", "idle": "Idle"}'
    new = old[:-1] + ',' + chr(10) + '                    "look": "Look Around"}   # a generated look-around replaces the procedural one'
    assert t.count(old) == 1, "animpick anchor"
    t = t.replace(old, new)
    q.write_bytes(t.replace("\r\n", "\n").encode("utf-8"))
    print("animpick: look video clip")


patch_animal_video()
patch_animpick()
