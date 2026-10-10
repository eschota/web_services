"""Render GET /api/fleet as a PNG card and (optionally) post it to the DEV channel.

    python3 fleet_card.py --out /tmp/fleet.png
    python3 fleet_card.py --out /tmp/fleet.png --send --caption "Fleet: /api/fleet live"

Runs on the VPS (Linux, UTF-8): DEV captions sent from Windows curl arrive as
mojibake, so the card is always posted from here with an explicit UTF-8 body.
"""

from __future__ import annotations

import argparse
import json
import mimetypes
import urllib.request
import uuid
from typing import Any, Dict, List, Tuple

from PIL import Image, ImageDraw, ImageFont

API = "http://127.0.0.1:8255/api/fleet"
DEV_SEND = "https://autorig.online/dev/api/send"
FONT = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
FONT_BOLD = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
FONT_MONO = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"

BG = (17, 20, 26)
PANEL = (26, 30, 38)
LINE = (44, 50, 62)
TEXT = (226, 230, 238)
MUTED = (140, 150, 166)
STATE_COLORS = {
    "idle": (46, 160, 90),
    "busy": (52, 120, 230),
    "degraded": (214, 150, 40),
    "blocked": (214, 64, 64),
    "offline": (110, 116, 128),
    "out_of_fleet": (70, 74, 84),
    "unknown": (110, 116, 128),
}


def fetch() -> Dict[str, Any]:
    with urllib.request.urlopen(API, timeout=15) as response:
        return json.loads(response.read().decode("utf-8"))


def _gpu(box: Dict[str, Any]) -> str:
    gpu = box.get("gpu_object") or {}
    name = str(gpu.get("name") or "?").replace("NVIDIA GeForce ", "")
    if gpu.get("error"):
        return f"{name}: {gpu['error']}"
    total = gpu.get("vram_total_mb")
    free = gpu.get("vram_free_mb")
    if total:
        return f"{name}  {round((free or 0) / 1024, 1)}/{round(total / 1024, 1)} GB free"
    return name


def _disks(box: Dict[str, Any]) -> List[Tuple[str, Tuple[int, int, int]]]:
    out = []
    for disk in box.get("disks_array") or []:
        if disk.get("work_drive") is False:
            continue
        free = disk.get("free_gb")
        if free is None:
            continue
        color = TEXT
        limit = 10 if str(disk.get("drive", "")).upper().startswith("C") else 15
        if float(free) <= 0.05:
            color = STATE_COLORS["blocked"]
        elif float(free) < limit:
            color = STATE_COLORS["degraded"]
        out.append((f"{disk.get('drive')}{round(float(free))}G", color))
    return out


def _busy(box: Dict[str, Any]) -> str:
    items = []
    for item in box.get("busy_with_array") or []:
        what = str(item.get("what") or "")
        what = what.replace("gen_video_minimax_h3_by_url.json", "H3 video").replace(
            "gen_animation_by_url.json", "video").replace("autorig_interactive", "rig").replace(".json", "")
        items.append(f"{item.get('service')}: {what}")
    text = "; ".join(items)
    if box.get("queue_depth_int"):
        text = (text + "  " if text else "") + f"queue {box['queue_depth_int']}"
    return text or "-"


def _issue(box: Dict[str, Any]) -> str:
    for key in ("blockers_array", "warnings_array"):
        for item in box.get(key) or []:
            return str(item)
    return box.get("note_string") or ""


def render(snapshot: Dict[str, Any], path: str) -> None:
    font = ImageFont.truetype(FONT, 19)
    small = ImageFont.truetype(FONT, 16)
    bold = ImageFont.truetype(FONT_BOLD, 20)
    title = ImageFont.truetype(FONT_BOLD, 30)
    mono = ImageFont.truetype(FONT_MONO, 16)
    boxes = snapshot.get("boxes_array") or []
    cols = [("Box", 150), ("State", 150), ("Roles", 250), ("Busy / queue", 330), ("GPU", 330),
            ("Disk", 210), ("Build", 140), ("V3", 70), ("Blocker / note", 760)]
    width = sum(w for _, w in cols) + 60
    row_h = 46
    height = 150 + row_h * (len(boxes) + 1) + 120
    image = Image.new("RGB", (width, height), BG)
    draw = ImageDraw.Draw(image)
    draw.text((30, 24), "AutoRig fleet", font=title, fill=TEXT)
    draw.text((270, 33), f"{snapshot.get('generated_at_utc')}   GET https://autorig.online/api/fleet",
              font=small, fill=MUTED)
    summary = snapshot.get("summary_object") or {}
    queues = snapshot.get("queues_object") or {}
    at = queues.get("autorig_tasks_object") or {}
    rf = queues.get("renderfin_object") or {}
    line = (f"AutoRig converters taking work: {', '.join(summary.get('autorig_capable_array') or []) or 'none'}"
            f"    Hunyuan: {', '.join(summary.get('hunyuan_ready_array') or []) or 'none'}"
            f"    LLM: {', '.join(summary.get('ai_ready_array') or []) or 'none'}"
            f"    V3 ready: {', '.join(summary.get('v3_ready_array') or []) or 'none'}")
    draw.text((30, 74), line, font=font, fill=TEXT)
    draw.text((30, 104), f"Queues: AutoRig created {at.get('created_int')} / processing {at.get('processing_int')}"
                         f"   renderfin pending {rf.get('pending_int')} / rendering {rf.get('rendering_int')}",
              font=small, fill=MUTED)
    y = 140
    x = 30
    draw.rectangle((20, y - 6, width - 20, y + row_h - 10), fill=PANEL)
    for name, w in cols:
        draw.text((x, y + 4), name, font=bold, fill=MUTED)
        x += w
    y += row_h
    for box in boxes:
        state = str(box.get("state_string") or "unknown")
        color = STATE_COLORS.get(state, STATE_COLORS["unknown"])
        draw.line((20, y - 6, width - 20, y - 6), fill=LINE)
        x = 30
        draw.text((x, y + 4), str(box.get("id")), font=bold, fill=TEXT)
        x += cols[0][1]
        chip = state.replace("_", " ")
        chip_w = int(draw.textlength(chip, font=small)) + 22
        draw.rounded_rectangle((x, y + 2, x + chip_w, y + 30), radius=13, fill=color)
        draw.text((x + 11, y + 6), chip, font=small, fill=(255, 255, 255))
        x += cols[1][1]
        draw.text((x, y + 6), ", ".join(box.get("roles_array") or []) or "-", font=small, fill=TEXT)
        x += cols[2][1]
        draw.text((x, y + 6), _busy(box)[:38], font=small, fill=TEXT)
        x += cols[3][1]
        gpu_text = _gpu(box)
        gpu_color = STATE_COLORS["blocked"] if (box.get("gpu_object") or {}).get("error") else TEXT
        draw.text((x, y + 6), gpu_text[:36], font=small, fill=gpu_color)
        x += cols[4][1]
        dx = x
        for text, dcolor in _disks(box)[:4]:
            draw.text((dx, y + 6), text, font=mono, fill=dcolor)
            dx += int(draw.textlength(text, font=mono)) + 12
        x += cols[5][1]
        build = box.get("build_object") or {}
        draw.text((x, y + 6), str(build.get("build_id") or build.get("server_version") or "-")[:10],
                  font=mono, fill=MUTED)
        x += cols[6][1]
        v3 = box.get("v3_object") or {}
        v3_text = "yes" if v3.get("ready") else ("no" if v3.get("target_bool") else "n/a")
        draw.text((x, y + 6), v3_text, font=small,
                  fill=STATE_COLORS["idle"] if v3.get("ready") else MUTED)
        x += cols[7][1]
        issue = _issue(box)
        issue_color = STATE_COLORS["blocked"] if box.get("blockers_array") else (
            STATE_COLORS["degraded"] if box.get("warnings_array") else MUTED)
        draw.text((x, y + 6), issue[:86], font=small, fill=issue_color)
        y += row_h
    vps = snapshot.get("vps_object") or {}
    disk = vps.get("disk_object") or {}
    draw.line((20, y - 6, width - 20, y - 6), fill=LINE)
    draw.text((30, y + 14), f"VPS: {disk.get('free_gb')} GB free ({disk.get('used_percent')}% used),"
                            f" new tasks paused: {vps.get('new_tasks_paused_bool')},"
                            f" release {vps.get('release_string')}", font=small, fill=MUTED)
    draw.text((30, y + 44), "One call for every agent: GET https://autorig.online/api/fleet  (?format=text,"
                            " /api/fleet/box/<id>)", font=small, fill=MUTED)
    image.save(path, "PNG", optimize=True)


def send(path: str, caption: str, agent: str, project: str, hint: str) -> Dict[str, Any]:
    boundary = uuid.uuid4().hex
    parts: List[bytes] = []
    for name, value in (("agent", agent), ("project", project), ("caption", caption),
                        ("concept_hint", hint)):
        parts.append((f"--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n"
                      f"Content-Type: text/plain; charset=utf-8\r\n\r\n").encode("utf-8")
                     + value.encode("utf-8") + b"\r\n")
    with open(path, "rb") as handle:
        data = handle.read()
    ctype = mimetypes.guess_type(path)[0] or "application/octet-stream"
    parts.append((f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; "
                  f"filename=\"{path.rsplit('/', 1)[-1]}\"\r\nContent-Type: {ctype}\r\n\r\n").encode("utf-8")
                 + data + b"\r\n")
    parts.append(f"--{boundary}--\r\n".encode("utf-8"))
    body = b"".join(parts)
    request = urllib.request.Request(DEV_SEND, data=body, method="POST",
                                     headers={"Content-Type": f"multipart/form-data; boundary={boundary}",
                                              "User-Agent": "autorig-fleet-card/1"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="/tmp/fleet-card.png")
    parser.add_argument("--send", action="store_true")
    parser.add_argument("--caption", default="")
    parser.add_argument("--agent", default="Fleet · V3")
    parser.add_argument("--project", default="AutoRig V3")
    args = parser.parse_args()
    snapshot = fetch()
    render(snapshot, args.out)
    print("rendered", args.out)
    if args.send:
        result = send(args.out, args.caption, args.agent, args.project,
                      "a status table of the AutoRig render and converter fleet")
        print(json.dumps(result, ensure_ascii=False)[:400])


if __name__ == "__main__":
    main()
