"""Sparse held-out comparison for native closeup masks, before triangle fusion."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
from PIL import Image

from check_semantic_anchors import require_hash, sha


def run(args):
    if args.output.exists():
        raise ValueError("Refusing to overwrite a reviewer result")
    packet = json.loads(args.packet.read_text(encoding="utf-8"))
    annotation = json.loads(args.annotations.read_text(encoding="utf-8"))
    closeup = json.loads((args.closeup / "receipt.json").read_text(encoding="utf-8"))
    masks_receipt = json.loads((args.masks / "receipt.json").read_text(encoding="utf-8"))
    require_hash(args.source, packet["source"]["sha256"])
    require_hash(args.packet.parent / annotation["sheet"], annotation["sheet_sha256"])
    if annotation["packet_sha256"] != packet["packet_canonical_sha256"] or closeup["packet_sha256"] != annotation["packet_sha256"]:
        raise ValueError("Packet/annotation/closeup mismatch")
    if closeup["source_sha256"] != packet["source"]["sha256"] or masks_receipt["source_sha256"] != closeup["source_sha256"]:
        raise ValueError("Mask source mismatch")
    require_hash(args.closeup / "receipt.json", masks_receipt["closeup_receipt_sha256"])
    masks = {}
    for view in ("front", "left", "back", "right"):
        name = f"mask_{view}.png"
        require_hash(args.masks / name, masks_receipt["files"][name])
        masks[view] = np.array(Image.open(args.masks / name).convert("L")) > 0
    rows = []
    for point in annotation["points"]:
        view = point["view"]
        original = packet["registered_projections"]["crop_transforms"][view]
        x0, y0, x1, y1 = original["source_box_xyxy"]
        cw, ch = original["cell_size"]
        old_x = x0 + point["u"] * (x1 - x0) / cw
        old_y = y0 + point["v"] * (y1 - y0) / ch
        new = closeup["cameras"][view]
        x = int((old_x - new["xstart_old_pixels"]) * new["magnification"])
        y = int((old_y - new["ystart_old_pixels"]) * new["magnification"])
        if not (0 <= y < masks[view].shape[0] and 0 <= x < masks[view].shape[1]):
            raise ValueError("Anchor outside closeup")
        selected = bool(masks[view][y, x])
        rows.append({**point, "native_pixel": [x, y], "selected": selected,
                     "match": selected == point["hair"]})
    positives = [row for row in rows if row["hair"]]
    negatives = [row for row in rows if not row["hair"]]
    report = {"schema": "autorig.hair-lab.dense-semantic-anchors/1",
        "status": "sparse_review_only_not_acceptance", "source_sha256": sha(args.source),
        "annotation_sha256": sha(args.annotations), "mask_receipt_sha256": sha(args.masks / "receipt.json"),
        "hair_hits": sum(row["match"] for row in positives), "hair_anchors": len(positives),
        "nonhair_correct": sum(row["match"] for row in negatives), "nonhair_anchors": len(negatives),
        "points": rows, "limits": ["Sparse manual reviewer labels, not dense semantic ground truth",
            "Original80px crop introduces positional uncertainty; near-boundary labels need review",
            "Dense mask evidence alone never authorizes replacing geometry"]}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({key: report[key] for key in
        ("status", "hair_hits", "hair_anchors", "nonhair_correct", "nonhair_anchors")}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("source", "packet", "annotations", "closeup", "masks", "output"):
        parser.add_argument("--" + key, required=True, type=Path)
    run(parser.parse_args())
