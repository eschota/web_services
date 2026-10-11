"""Render the V3 converter rollout per node as a PNG and post it to DEV (run on the VPS).

    /srv/autorig/venv/bin/python3 v3_rollout_card.py --out /tmp/v3.png [--send --caption "..."]

Rows come from /srv/autorig/data/var/fleet/v3_target.json (per-node rollout
records) joined with the live GET /api/fleet answer.
"""

from __future__ import annotations

import argparse
import json
import urllib.request

from PIL import Image, ImageDraw, ImageFont

import fleet_card

TARGET = "/srv/autorig/data/var/fleet/v3_target.json"
API = "http://127.0.0.1:8255/api/fleet"
GREEN, RED, AMBER, GREY = (46, 160, 90), (214, 64, 64), (214, 150, 40), (110, 116, 128)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="/tmp/v3-rollout.png")
    parser.add_argument("--send", action="store_true")
    parser.add_argument("--caption", default="")
    args = parser.parse_args()
    target = json.load(open(TARGET, encoding="utf-8"))
    nodes = target.get("nodes") or {}
    fleet = json.loads(urllib.request.urlopen(API, timeout=15).read().decode("utf-8"))
    boxes = {b["id"]: b for b in fleet.get("boxes_array") or []}
    commit = str(target.get("commit") or "")
    rows = []
    for bid in ("f1", "f2", "f5", "f7", "f11", "f13"):
        box = boxes.get(bid) or {}
        v3 = box.get("v3_object") or {}
        rec = nodes.get(bid) or {}
        canary = str(rec.get("canary") or "")
        rows.append({
            "box": bid,
            "ready": bool(v3.get("ready")),
            "serving": (v3.get("deploy_commit") or "-")[:8],
            "base": str(rec.get("base_commit") or "-")[:8],
            "artifact": str(rec.get("artifact_sha256") or "-")[:12],
            "v3": "7/7" if "7/7" in canary else ("-" if not canary else canary[:12]),
            "rig": (canary.split("only_rig ")[1].split(",")[0] if "only_rig " in canary else "-"),
            "dispatch": "on" if (box.get("dispatch_object") or {}).get("autorig_endpoint_enabled") else "off",
            "note": "; ".join((box.get("blockers_array") or [])[:1])[:70] or (", ".join(v3.get("blocked_by") or [])[:70]),
        })
    font = ImageFont.truetype(fleet_card.FONT, 18)
    bold = ImageFont.truetype(fleet_card.FONT_BOLD, 19)
    title = ImageFont.truetype(fleet_card.FONT_BOLD, 28)
    mono = ImageFont.truetype(fleet_card.FONT_MONO, 17)
    cols = [("Box", 90), ("V3", 110), ("Serving", 130), ("Was", 120), ("Delta artifact", 190),
            ("V3 canary", 140), ("only_rig", 130), ("AutoRig", 110), ("Note", 720)]
    width = sum(w for _, w in cols) + 60
    height = 170 + 50 * len(rows) + 70
    image = Image.new("RGB", (width, height), fleet_card.BG)
    draw = ImageDraw.Draw(image)
    draw.text((30, 22), "AutoRig converter V3 rollout", font=title, fill=fleet_card.TEXT)
    draw.text((30, 66), f"accepted commit {commit[:12]} (eschota/autorig.online main)   "
                        f"{fleet.get('generated_at_utc')}", font=font, fill=fleet_card.MUTED)
    ready = [r["box"] for r in rows if r["ready"]]
    draw.text((30, 96), f"V3 ready: {', '.join(ready) or 'none'}   (identity = deploy_commit + boot_build_id, "
                        f"feature_flags.normalized_source)", font=font, fill=fleet_card.TEXT)
    y = 140
    draw.rectangle((20, y - 6, width - 20, y + 36), fill=fleet_card.PANEL)
    x = 30
    for name, w in cols:
        draw.text((x, y + 2), name, font=bold, fill=fleet_card.MUTED)
        x += w
    y += 50
    for row in rows:
        draw.line((20, y - 8, width - 20, y - 8), fill=fleet_card.LINE)
        x = 30
        draw.text((x, y), row["box"], font=bold, fill=fleet_card.TEXT)
        x += cols[0][1]
        chip = "ready" if row["ready"] else "no"
        color = GREEN if row["ready"] else (RED if row["box"] == "f11" else AMBER)
        cw = int(draw.textlength(chip, font=font)) + 22
        draw.rounded_rectangle((x, y - 2, x + cw, y + 26), radius=13, fill=color)
        draw.text((x + 11, y + 1), chip, font=font, fill=(255, 255, 255))
        x += cols[1][1]
        for key, idx, f in (("serving", 2, mono), ("base", 3, mono), ("artifact", 4, mono),
                            ("v3", 5, font), ("rig", 6, font), ("dispatch", 7, font)):
            fill = fleet_card.TEXT
            if key == "dispatch":
                fill = GREEN if row[key] == "on" else GREY
            if key == "v3" and row[key] == "7/7":
                fill = GREEN
            draw.text((x, y + 1), str(row[key]), font=f, fill=fill)
            x += cols[idx][1]
        draw.text((x, y + 1), row["note"], font=font, fill=RED if row["box"] in ("f11",) or not row["ready"] else GREY)
        y += 50
    draw.text((30, y + 10), "Every node: drained first, deployed with deploy_farm.bat HEAD (per-base delta), "
                            "V3 canary 7/7 byte-verified + only_rig canary, then put back.",
              font=font, fill=fleet_card.MUTED)
    image.save(args.out, "PNG", optimize=True)
    print("rendered", args.out)
    if args.send:
        print(json.dumps(fleet_card.send(args.out, args.caption, "Fleet · V3", "AutoRig V3",
                                         "a status table of the converter fleet rollout"), ensure_ascii=False)[:300])


if __name__ == "__main__":
    main()
