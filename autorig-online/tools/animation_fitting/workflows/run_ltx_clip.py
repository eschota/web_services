#!/usr/bin/env python3
"""Generate one morphology-checked horse candidate on ComfyUI.

Requires --bundle, its --image, --action, --out-name and a new --output-dir.
Use --dry-run to validate/persist a graph without submitting GPU work.
LTX output remains pending gait QA and is never an approved skeletal clip.
"""
import argparse
import json
import os
import shutil
import sys
import time
import uuid
import hashlib
import re
import subprocess
import urllib.parse
import urllib.error
import urllib.request

from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from animation_fitting.game_timing import FRAME_BUDGET, GAME_FPS, timing
from animation_fitting.workflows.build_ltx2_graph import build  # noqa: E402

BASE = "http://127.0.0.1:8188"
SPECS = Path(__file__).resolve().parents[3] / "backend/animation_fitting/specs/action_prompts.v1.json"

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

# Gaits are periodic: the seamless loop comes from extracting one cycle out of
# the steady-state walk, not from forcing the clip back to the rest pose.
FREE_CYCLE_TAIL = (
    "Continuous steady-state locomotion. Never pause, never return to a "
    "standing pose, never change speed."
)


# Identity anchor (owner's call): the reference is a MANNEQUIN, not an animal.
# Without this LTX "repairs" the low-poly proxy into a photoreal horse within
# ~1.5 s and the color-coded legs dissolve.
IDENTITY_PREFIX_CLAY = (
    "The subject is a matte grey clay 3D model of a {{species}}: a smooth "
    "untextured grey plastic figure with solid brightly colored legs (each "
    "leg a single flat color). It is a 3D dummy in a plain grey studio, not "
    "a living animal. Keep it exactly this untextured grey clay model in "
    "every frame - same shape, same flat grey plastic, same solid leg colors "
    "- while it moves."
)
IDENTITY_PREFIX = (
    "The subject is a stylized low-poly faceted 3D mannequin of a {{species}}: "
    "a flat-shaded matte grey plastic figure with hard polygonal edges and "
    "solid brightly colored legs (each leg a single flat color). It is a 3D "
    "dummy in a plain grey studio, not a living animal. Keep it exactly this "
    "untextured faceted mannequin in every frame — same polygons, same flat "
    "grey plastic, same solid leg colors — while it moves."
)
IDENTITY_NEGATIVE = (
    ", realistic horse, photorealistic animal, fur, hair, skin texture, "
    "muscles, veins, smooth organic surface, subsurface scattering, mane "
    "strands, tail hair, added detail, changing leg colors, color bleeding "
    "between legs"
)


def compose_prompt(action_id, species="horse", free_cycle=False, identity="lowpoly"):
    d = json.load(open(SPECS, encoding="utf-8"))
    action = next(a for a in d["actions_array"] if a["action_id_string"] == action_id)
    motion = ACTION_OVERRIDES.get(action_id) or action["motion_prompt_string"]
    mode = action["generation_mode_string"]
    if free_cycle:
        tail = FREE_CYCLE_TAIL
    else:
        tail = d["loop_instruction_string"] if mode == "loop" else d["one_shot_instruction_string"]
    identity_prefix = IDENTITY_PREFIX_CLAY if identity == "clay" else IDENTITY_PREFIX
    prompt = " ".join([
        identity_prefix, d["common_positive_prefix_string"], motion, tail,
    ]).replace("{{species}}", species)
    negative = d["common_negative_prompt_string"] + IDENTITY_NEGATIVE
    return prompt, negative, int(action["frame_count_int"]), mode


def upload_image(path):
    boundary = uuid.uuid4().hex
    name = uuid.uuid4().hex + Path(path).suffix.lower()
    body = (
        (f"--{boundary}\r\nContent-Disposition: form-data; name=\"image\"; "
         f"filename=\"{name}\"\r\nContent-Type: image/png\r\n\r\n").encode()
        + open(path, "rb").read()
        + (f"\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"overwrite\"\r\n\r\nfalse"
           f"\r\n--{boundary}--\r\n").encode()
    )
    req = urllib.request.Request(f"{BASE}/upload/image", data=body,
                                 headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
    return json.load(urllib.request.urlopen(req, timeout=60)).get("name") or name


def validate_generation(frames, loop_strength, free_cycle, kind):
    if isinstance(frames, bool) or not isinstance(frames, int) or not 9 <= frames <= 377 or (frames - 1) % 8:
        raise ValueError("LTX generation requires 9..377 samples in 8n+1; skeletal targets do not")
    if not 0 <= loop_strength <= 1:
        raise ValueError("loop strength must be in [0,1]")
    if free_cycle and kind != "gait":
        raise ValueError("free-cycle applies only to locomotion")
    latent_frames = frames - 8 if loop_strength > 0 else frames
    if latent_frames < 9:
        raise ValueError("end guide needs at least 17 output samples")
    return latent_frames


def validate_submission(response):
    if response.get("node_errors") or not isinstance(response.get("prompt_id"), str) or not response["prompt_id"]:
        raise ValueError("Comfy rejected graph: " + json.dumps(response)[:1500])
    return response["prompt_id"]


def validate_video(path, expected_frames, ffprobe):
    result = subprocess.run([ffprobe, '-v', 'error', '-count_frames', '-select_streams', 'v:0',
        '-show_entries', 'stream=nb_read_frames,avg_frame_rate,width,height', '-of', 'json', str(path)],
        capture_output=True, text=True, timeout=60, check=True)
    streams = json.loads(result.stdout).get('streams', [])
    if len(streams) != 1:
        raise ValueError('output must contain exactly one readable video stream')
    stream = streams[0]
    num, den = map(int, stream['avg_frame_rate'].split('/'))
    if int(stream['nb_read_frames']) != expected_frames or den == 0 or num / den != GAME_FPS:
        raise ValueError('rendered timing disagrees with requested 30 FPS frame budget: ' + str(stream))
    if (stream['width'], stream['height']) != (768, 448):
        raise ValueError('rendered resolution disagrees with graph')
    return stream


def write_json_atomic(path, payload):
    path = Path(path)
    temporary = path.with_name('.' + path.name + '.tmp')
    temporary.write_text(json.dumps(payload, indent=2, allow_nan=False) + '\n', encoding='utf-8')
    os.replace(temporary, path)


def main():
    global BASE
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--image", required=True, type=Path)
    ap.add_argument("--bundle", required=True, type=Path)
    ap.add_argument("--action", required=True, choices=FRAME_BUDGET)
    ap.add_argument("--out-name", required=True)
    ap.add_argument("--output-dir", type=Path, required=True)
    ap.add_argument("--gpu-lock", type=Path, help="shared project lock path; tracking must use the same lease")
    ap.add_argument("--comfy-url", default=BASE)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--loop-strength", type=float, default=0.85)
    ap.add_argument("--tiles", type=int, choices=(2, 4, 8), default=4)
    ap.add_argument("--frames", type=int)
    ap.add_argument("--timeout-min", type=float, default=30)
    ap.add_argument("--ffprobe", default="ffprobe")
    ap.add_argument("--identity", choices=("lowpoly", "clay"), default="lowpoly")
    ap.add_argument("--free-cycle", action="store_true")
    ap.add_argument("--dry-run", action="store_true", help="write pinned graph/timing without contacting Comfy or GPU")
    args = ap.parse_args()
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,80}", args.out_name):
        ap.error("out-name must be a simple identifier")
    if args.timeout_min <= 0:
        ap.error("timeout must be positive")
    BASE = args.comfy_url.rstrip('/')
    contract = timing(args.action)
    frames = args.frames if args.frames is not None else contract['generation_samples']
    loop_strength = 0.0 if args.free_cycle or not contract['loop'] else args.loop_strength
    latent_frames = validate_generation(frames, loop_strength, args.free_cycle, contract['kind'])
    image_bytes = args.image.read_bytes()
    from animation_fitting.audit_horse_rest_rig import audit_bundle
    from animation_fitting.rig import load_rig_bundle
    rig_audit = audit_bundle(args.bundle)
    if not rig_audit['passed']:
        raise ValueError('Source rig failed morphology QA: ' + json.dumps(rig_audit['blocking_reasons']))
    rig = load_rig_bundle(args.bundle)
    expected_image = rig.artifacts.get('ltx_semantic', rig.artifacts['rgb'])
    if hashlib.sha256(image_bytes).digest() != hashlib.sha256(expected_image.read_bytes()).digest():
        raise ValueError('Reference image must match the immutable source bundle')
    prompt, negative, _, mode = compose_prompt(args.action, free_cycle=args.free_cycle, identity=args.identity)
    graph = build(prompt, args.image.name, args.out_name, latent_frames,
                  frame_rate=GAME_FPS, fps=float(GAME_FPS), seed=args.seed,
                  loop_guide_strength=loop_strength, spatial_tiles=args.tiles)
    graph['negative_prompt']['inputs']['text'] = negative
    args.output_dir.mkdir(parents=True, exist_ok=False)
    record = {'schema': 'autorig-ltx-clip-run.v1', 'state': 'prepared',
              'comfy_url': BASE,
              'action': args.action, 'seed': args.seed, 'timing': contract,
              'requested_generation_samples': frames, 'latent_samples': latent_frames,
              'source_image_sha256': hashlib.sha256(image_bytes).hexdigest(),
              'prompts_sha256': hashlib.sha256(SPECS.read_bytes()).hexdigest(),
              'quality_approved': False}
    record['rig_audit'] = rig_audit
    def persist():
        write_json_atomic(args.output_dir / 'workflow.json', graph)
        write_json_atomic(args.output_dir / 'run.json', record)
    persist()
    if args.dry_run:
        print(json.dumps(record))
        return 0
    ffprobe = shutil.which(args.ffprobe)
    if not ffprobe:
        raise ValueError('ffprobe must be available before GPU submission')
    from animation_fitting.gpu_lease import gpu_lease, DEFAULT_GPU_LOCK
    lock = args.gpu_lock or DEFAULT_GPU_LOCK
    try:
        with gpu_lease(lock, 'ltx:' + args.action):
            queue = json.load(urllib.request.urlopen(BASE + '/queue', timeout=15))
            if not isinstance(queue.get('queue_running'), list) or not isinstance(queue.get('queue_pending'), list):
                raise ValueError('Comfy queue response missing expected lists')
            if queue['queue_running'] or queue['queue_pending']:
                raise ValueError('Comfy is busy; no job submitted, no existing work interrupted')
            graph['ref_image']['inputs']['image'] = upload_image(args.image)
            payload = json.dumps({'prompt': graph, 'client_id': 'animal-pipeline'}).encode()
            req = urllib.request.Request(BASE + '/prompt', data=payload, headers={'Content-Type':'application/json'})
            response = json.load(urllib.request.urlopen(req, timeout=60))
            record['submission_response'] = response
            persist()
            pid = validate_submission(response)
            record.update(state='submitted', prompt_id=pid)
            persist()
            deadline = time.monotonic() + args.timeout_min * 60
            while time.monotonic() < deadline:
                try:
                    history = json.load(urllib.request.urlopen(BASE + '/history/' + urllib.parse.quote(pid, safe=''), timeout=30))
                except (OSError, ValueError):
                    time.sleep(5)
                    continue
                rec = history.get(pid)
                if rec:
                    status = rec.get('status', {})
                    if status.get('status_str') == 'error':
                        raise ValueError('Comfy execution error: ' + json.dumps(status.get('messages', []))[-2000:])
                    if status.get('completed'):
                        videos = [item for output in (rec.get('outputs') or {}).values()
                                  for key in ('images', 'videos', 'gifs') for item in output.get(key, [])
                                  if str(item.get('filename', '')).lower().endswith('.mp4')]
                        if len(videos) != 1:
                            raise ValueError('expected exactly one completed MP4')
                        item = videos[0]
                        url = BASE + '/view?' + urllib.parse.urlencode({k:item[k] for k in ('filename','subfolder','type') if k in item})
                        dst = args.output_dir / (args.out_name + '.mp4')
                        with urllib.request.urlopen(url, timeout=120) as response, dst.open('xb') as output:
                            shutil.copyfileobj(response, output)
                        record['video_probe'] = validate_video(dst, frames, ffprobe)
                        record.update(state='rendered_pending_gait_qa', video_sha256=hashlib.sha256(dst.read_bytes()).hexdigest())
                        persist()
                        print(json.dumps(record))
                        return 0
                time.sleep(5)
            record['state'] = 'pending_after_timeout'
            persist()
            print(json.dumps(record))
            return 5
    except Exception as exc:
        record.update(state='failed', error=str(exc))
        persist()
        print(json.dumps(record))
        return 2


if __name__ == "__main__":
    sys.exit(main())
