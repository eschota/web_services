#!/usr/bin/env python3
"""Generate one canonical-horse clip on local ComfyUI (LTX-2 19B + static LoRA).

Usage:
  python run_ltx_clip.py --image <reference png> --action walk_forward \
      --out-name horse_walk_v3 [--seed 42] [--loop-strength 0.85] [--tiles 2]
"""
import argparse
import json
import os
import shutil
import sys
import time
import uuid
import urllib.error
import urllib.request

sys.path.insert(0, r"R:\autorig_rig_page\autorig-online\tools\animation_fitting\workflows")
from build_ltx2_graph import build  # noqa: E402

BASE = "http://127.0.0.1:8188"
SPECS = r"R:\autorig_rig_page\autorig-online\backend\animation_fitting\specs\action_prompts.v1.json"
OUT_DIR = r"R:\ComfyUI_windows_portable\ComfyUI\output"
CANDIDATES = r"R:\ComfyUI-data\autorig-fitting\candidates"

# Gait-specific reinforcement learned from the walk v2 rejection: the model
# animated only the near leg pair and pinned the hooves. Spell out the far
# legs, the four-beat order and the treadmill-style backward hoof drift.
ACTION_OVERRIDES = {
    "walk_forward": (
        "The horse starts walking immediately and keeps walking forward on an "
        "invisible treadmill for the entire clip without ever stopping: the "
        "body stays centered in frame while every hoof that touches the "
        "ground slides backward at a constant speed and swings forward "
        "through the air. All four legs step continuously, both near-side "
        "legs and both far-side legs, in the natural four-beat lateral "
        "walking sequence: left hind, left fore, right hind, right fore, "
        "evenly spaced in time, with a gentle head nod on every stride. "
        "Brisk natural real-time pace, about one full stride per second, "
        "not slow motion."
    ),
}

# Game-controller frame budgets at 30 fps (all 8n+1). The taxonomy's legacy
# {33,49,65,97} profile is wrong for locomotion: a horse walk cycle is ~1.1 s
# (33 frames), trot ~0.8 s (25), run ~0.7 s (21), sprint ~0.55 s (17). Gaits
# are therefore generated long and free-running (gen_frames) and ONE period is
# extracted and retimed to target_frames by the gait analyzer; static loops
# and one-shots are generated at their final length directly.
FRAME_BUDGET = {
    # action:            (gen_frames, target_frames, kind)
    "walk_forward":      (97, 33, "gait"),
    "walk_backward":     (97, 33, "gait"),
    "trot_jog":          (97, 25, "gait"),
    "run":               (97, 21, "gait"),
    "sprint":            (97, 17, "gait"),
    "jump_air":          (49, 25, "static_loop"),
    "idle_neutral":      (97, 97, "static_loop"),
    "idle_alert":        (97, 97, "static_loop"),
    "idle_relaxed":      (97, 97, "static_loop"),
    "idle_look_around":  (97, 97, "static_loop"),
    "idle_fidget":       (97, 97, "static_loop"),
    "eat_interact":      (129, 129, "static_loop"),
    "sleep_rest":        (97, 97, "static_loop"),
    "turn_left_90":      (33, 33, "one_shot"),
    "turn_right_90":     (33, 33, "one_shot"),
    "turn_around_180":   (49, 49, "one_shot"),
    "stop_brake":        (33, 33, "one_shot"),
    "jump_start":        (17, 17, "one_shot"),
    "jump_land":         (25, 25, "one_shot"),
    "jump_full":         (49, 49, "one_shot"),
    "fall":              (49, 49, "one_shot"),
    "attack_primary":    (49, 49, "one_shot"),
    "attack_secondary":  (65, 65, "one_shot"),
    "attack_heavy":      (49, 49, "one_shot"),
    "hit_front":         (25, 25, "one_shot"),
    "hit_left":          (25, 25, "one_shot"),
    "hit_right":         (25, 25, "one_shot"),
    "death":             (81, 81, "one_shot"),
    "get_up":            (65, 65, "one_shot"),
    "vocalize_emote":    (33, 33, "one_shot"),
}
GAME_FPS = 30

# Gaits are periodic: the seamless loop comes from extracting one cycle out of
# the steady-state walk, not from forcing the clip back to the rest pose.
FREE_CYCLE_TAIL = (
    "Continuous steady-state locomotion. Never pause, never return to a "
    "standing pose, never change speed."
)


def compose_prompt(action_id, species="horse", free_cycle=False):
    d = json.load(open(SPECS, encoding="utf-8"))
    action = next(a for a in d["actions_array"] if a["action_id_string"] == action_id)
    motion = ACTION_OVERRIDES.get(action_id) or action["motion_prompt_string"]
    mode = action["generation_mode_string"]
    if free_cycle:
        tail = FREE_CYCLE_TAIL
    else:
        tail = d["loop_instruction_string"] if mode == "loop" else d["one_shot_instruction_string"]
    prompt = " ".join([
        d["common_positive_prefix_string"], motion, tail,
    ]).replace("{{species}}", species)
    return prompt, d["common_negative_prompt_string"], int(action["frame_count_int"]), mode


def upload_image(path):
    boundary = uuid.uuid4().hex
    name = os.path.basename(path)
    body = (
        (f"--{boundary}\r\nContent-Disposition: form-data; name=\"image\"; "
         f"filename=\"{name}\"\r\nContent-Type: image/png\r\n\r\n").encode()
        + open(path, "rb").read()
        + (f"\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"overwrite\"\r\n\r\ntrue"
           f"\r\n--{boundary}--\r\n").encode()
    )
    req = urllib.request.Request(f"{BASE}/upload/image", data=body,
                                 headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
    return json.load(urllib.request.urlopen(req, timeout=60)).get("name") or name


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--image", required=True)
    ap.add_argument("--action", required=True)
    ap.add_argument("--out-name", required=True)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--loop-strength", type=float, default=0.85)
    ap.add_argument("--tiles", type=int, default=2)
    ap.add_argument("--frames", type=int, default=0, help="override taxonomy frame count")
    ap.add_argument("--timeout-min", type=int, default=120)
    ap.add_argument("--free-cycle", action="store_true",
                    help="gait mode: continuous locomotion, no end-frame guide, "
                         "loop is extracted later from the steady state")
    args = ap.parse_args()

    gen_frames, target_frames, kind = FRAME_BUDGET.get(args.action, (None, None, None))
    # Gaits are always free-running (no end guide); the loop is cut later.
    free_cycle = args.free_cycle or kind == "gait"
    prompt, negative, frames, mode = compose_prompt(args.action, free_cycle=free_cycle)
    if gen_frames:
        frames = gen_frames
    if args.frames:
        frames = args.frames
    loop_strength = 0.0 if free_cycle else (args.loop_strength if mode == "loop" else 0.0)
    # LTXVAddGuide on the last frame appends one latent block (8 frames) to
    # the video, so ask for frames - 8 to get exactly the budgeted count back,
    # with the final frame pinned to the canonical pose.
    latent_frames = frames - 8 if loop_strength > 0 else frames
    image_name = upload_image(args.image)
    graph = build(prompt, image_name, args.out_name, latent_frames,
                  frame_rate=GAME_FPS, fps=float(GAME_FPS),
                  seed=args.seed, loop_guide_strength=loop_strength,
                  spatial_tiles=args.tiles)
    print(json.dumps({"budget": {"gen_frames": frames, "target_frames": target_frames,
                                 "kind": kind, "fps": GAME_FPS}}))
    graph["negative_prompt"]["inputs"]["text"] = negative
    payload = json.dumps({"prompt": graph, "client_id": "horse-pilot"}).encode()
    req = urllib.request.Request(f"{BASE}/prompt", data=payload,
                                 headers={"Content-Type": "application/json"})
    try:
        resp = json.load(urllib.request.urlopen(req, timeout=60))
    except urllib.error.HTTPError as e:
        print("QUEUE ERROR", e.code, e.read().decode("utf-8", "replace")[:1500])
        return 2
    pid = resp.get("prompt_id")
    print(json.dumps({"prompt_id": pid, "frames": frames, "mode": mode,
                      "loop_strength": loop_strength, "seed": args.seed,
                      "node_errors": resp.get("node_errors") or {}}, ensure_ascii=False))
    if not pid:
        return 2
    t0 = time.time()
    while time.time() - t0 < args.timeout_min * 60:
        time.sleep(15)
        try:
            h = json.load(urllib.request.urlopen(f"{BASE}/history/{pid}", timeout=30))
        except Exception:
            continue
        rec = h.get(pid)
        if not rec:
            continue
        st = rec.get("status", {})
        if st.get("status_str") == "error":
            print("RENDER ERROR:", json.dumps(st.get("messages", [])[-3:], ensure_ascii=False)[:1200])
            return 3
        if st.get("completed"):
            for o in (rec.get("outputs") or {}).values():
                for item in o.get("images", []) + o.get("videos", []) + o.get("gifs", []):
                    fn = item.get("filename", "")
                    if fn.endswith(".mp4"):
                        src = os.path.join(OUT_DIR, item.get("subfolder", ""), fn)
                        os.makedirs(CANDIDATES, exist_ok=True)
                        dst = os.path.join(CANDIDATES, args.out_name + ".mp4")
                        shutil.copyfile(src, dst)
                        print("DONE", json.dumps({"mp4": dst, "seconds": int(time.time() - t0)}))
                        return 0
            print("completed but no mp4 output found")
            return 4
    print("TIMEOUT")
    return 5


if __name__ == "__main__":
    sys.exit(main())
