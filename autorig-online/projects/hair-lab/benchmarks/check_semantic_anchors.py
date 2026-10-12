"""Independent sparse semantic checks on exact registered source-triangle masks.

Annotations must be made against the original lit sheet, not the algorithm mask.
This is sparse reviewer evidence, not dense ground truth or automatic acceptance.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
from PIL import Image


def sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def require_hash(path: Path, expected: str) -> str:
    actual = sha(path)
    if actual != expected:
        raise ValueError(f"Hash mismatch: {path.name}")
    return actual


def check(source: Path, packet_path: Path, projections: Path,
          annotation_path: Path, candidate: Path) -> dict:
    packet = json.loads(packet_path.read_text(encoding="utf-8"))
    annotation = json.loads(annotation_path.read_text(encoding="utf-8"))
    canonical = dict(packet)
    stored = canonical.pop("packet_canonical_sha256")
    calculated = hashlib.sha256(json.dumps(canonical, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    if stored != calculated or annotation["packet_sha256"] != stored:
        raise ValueError("Packet identity mismatch")
    require_hash(source, packet["source"]["sha256"])
    sheet = packet_path.parent / annotation["sheet"]
    require_hash(sheet, annotation["sheet_sha256"])
    require_hash(projections / "cameras.json",
                 packet["registered_projections"]["cameras_sha256"])
    with np.load(candidate, allow_pickle=False) as data:
        ids = data["face_ids"]
    count = packet["source"]["triangles"]
    if ids.ndim != 1 or not np.issubdtype(ids.dtype, np.integer):
        raise ValueError("Candidate must have integer face IDs")
    if np.any(ids < 0) or np.any(ids >= count) or len(np.unique(ids)) != len(ids):
        raise ValueError("Candidate face IDs invalid or repeated")
    chosen = set(map(int, ids))
    rows, maps, used_ids = [], {}, set()
    for point in annotation["points"]:
        name, view, u, v = point["id"], point["view"], point["u"], point["v"]
        if name in used_ids or type(point["hair"]) is not bool:
            raise ValueError("Annotation identity/label invalid")
        used_ids.add(name)
        crop = packet["registered_projections"]["crop_transforms"][view]
        cw, ch = crop["cell_size"]
        if not (0 <= u < cw and 0 <= v < ch):
            raise ValueError("Annotation outside cell")
        x0, y0, x1, y1 = crop["source_box_xyxy"]
        x = int(x0 + u * (x1 - x0) / cw)
        y = int(y0 + v * (y1 - y0) / ch)
        if view not in maps:
            name_tri = view + "_tri.png"
            require_hash(projections / name_tri,
                packet["registered_projections"]["files"][name_tri]["sha256"])
            rgb = np.array(Image.open(projections / name_tri).convert("RGB"), dtype=np.int64)
            if list(rgb.shape[1::-1]) != crop["source_size"]:
                raise ValueError("Registered triangle-map shape mismatch")
            maps[view] = (rgb[..., 0] << 16 | rgb[..., 1] << 8 | rgb[..., 2]) - 1
        face_id = int(maps[view][y, x])
        if face_id >= count:
            raise ValueError("Registered triangle ID outside source")
        selected = face_id in chosen
        rows.append({**point, "source_pixel": [x, y], "source_face_id": face_id,
                     "selected": selected, "match": selected == point["hair"]})
    positive = [row for row in rows if row["hair"]]
    negative = [row for row in rows if not row["hair"]]
    return {"schema": "autorig.hair-lab.semantic-anchors/1",
            "status": "sparse_review_only_not_acceptance",
            "packet_sha256": stored, "source_sha256": sha(source),
            "annotations_sha256": sha(annotation_path), "candidate_sha256": sha(candidate),
            "anchors": len(rows), "hair_hits": sum(row["match"] for row in positive),
            "hair_anchors": len(positive), "nonhair_correct": sum(row["match"] for row in negative),
            "nonhair_anchors": len(negative), "points": rows,
            "limits": ["Reviewer-labelled sparse pixels, not exhaustive or owner-approved truth",
                       "Low-resolution source crops can alias; no hidden hair is annotated",
                       "Source geometry must remain unchanged; this report never enables Apply"]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("source", "packet", "projections", "annotations", "candidate", "output"):
        parser.add_argument("--" + key, required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit("Refusing to overwrite evidence; choose a new revision")
    report = check(args.source, args.packet, args.projections, args.annotations, args.candidate)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, allow_nan=False), encoding="utf-8")
    print(json.dumps({key: report[key] for key in
        ("status", "anchors", "hair_hits", "hair_anchors", "nonhair_correct", "nonhair_anchors")}))


if __name__ == "__main__":
    main()
