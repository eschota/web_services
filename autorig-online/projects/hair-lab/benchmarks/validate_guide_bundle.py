"""Read-only preflight of a real exported Hair Lab guide/head bundle.

Does not repair geometry, infer segmentation, or replace the C#/Unity gate.
The report binds exact input JSON bytes and catches cross-language contract
mistakes before starting a Unity player. Customer GLBs are never changed.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path


def sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def vector(value):
    return isinstance(value, list) and len(value) == 3 and all(
        isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)
        for x in value
    )


def validate(directory):
    paths = [directory / name for name in ("guides.json", "head-neutral.json")]
    raw = [path.read_bytes() for path in paths]
    guides, head = [json.loads(data) for data in raw]
    errors = []
    counts = {"guides": 0, "samples": 0, "hairpins": 0, "nonpositive_width_samples": 0}

    def problem(code, guide=None, sample=None):
        errors.append({"code": code, "guide": guide, "sample": sample})

    if guides.get("schema") != "autorig.hair.guides/1" or head.get("schema") != "autorig.hair.head-neutral/1":
        problem("schema_mismatch")
    source_hash = guides.get("source", {}).get("sha256", "")
    if len(source_hash) != 64 or any(c not in "0123456789abcdefABCDEF" for c in source_hash):
        problem("invalid_source_sha256")
    if source_hash != head.get("source", {}).get("sha256"):
        problem("different_source_identity")
    coordinates = guides.get("coordinates", {})
    if coordinates != head.get("coordinates"):
        problem("different_coordinate_metadata")
    scale = coordinates.get("metersPerNormalizedUnit")
    if not isinstance(scale, (int, float)) or not math.isfinite(scale) or not 1e-6 <= scale <= 1e6:
        problem("missing_or_invalid_physical_scale")
        scale = 1.0
    if not coordinates.get("scaleProvenance"):
        problem("missing_scale_provenance")
    rects = guides.get("atlas", {}).get("rects", [])
    rect_ids = {r.get("id") for r in rects}
    if len(rect_ids) != len(rects) or not rects or any(not isinstance(i, str) for i in rect_ids):
        problem("atlas_ids_require_unique_strings")
    seen_ids = set()
    all_guides = guides.get("guides", [])
    if not isinstance(all_guides, list) or not 1 <= len(all_guides) <= 200000:
        problem("guide_count_out_of_range")
        all_guides = []
    for guide in all_guides:
        name = guide.get("id")
        counts["guides"] += 1
        if not isinstance(name, str) or not name or name in seen_ids:
            problem("invalid_or_duplicate_guide_id", name)
        seen_ids.add(name)
        root = guide.get("root")
        samples = guide.get("samples", [])
        width = guide.get("width")
        if not isinstance(width, (int, float)) or not math.isfinite(width) or not 0 < width * scale <= 1000:
            problem("invalid_guide_width", name)
        if not isinstance(guide.get("atlasRectId"), str) or guide.get("atlasRectId") not in rect_ids:
            problem("invalid_atlas_reference", name)
        if not vector(root) or not isinstance(samples, list) or len(samples) < 2:
            problem("invalid_root_or_samples", name)
            continue
        previous_direction = None
        previous = None
        for i, sample in enumerate(samples):
            counts["samples"] += 1
            point = sample.get("position")
            sample_width = sample.get("widthScale")
            if not isinstance(sample_width, (int, float)) or not math.isfinite(sample_width) or sample_width <= 0:
                problem("nonpositive_sample_width", name, i)
                counts["nonpositive_width_samples"] += 1
            if not vector(point):
                problem("invalid_sample_position", name, i)
                previous = None
                continue
            if i == 0 and math.dist(point, root) * scale > 1e-6:
                problem("root_sample_mismatch", name, i)
            if previous is not None:
                delta = [b - a for a, b in zip(previous, point)]
                length = math.sqrt(sum(d * d for d in delta))
                if length * scale <= 1e-6:
                    problem("duplicate_or_too_short_segment", name, i)
                else:
                    direction = [d / length for d in delta]
                    if previous_direction is not None and sum(a * b for a, b in zip(previous_direction, direction)) <= -0.95:
                        problem("irreducible_hairpin", name, i)
                        counts["hairpins"] += 1
                    previous_direction = direction
            previous = point
    return {
        "schema": "autorig.hair.bundle-preflight/1", "result": "PASS" if not errors else "FAIL",
        "source_sha256": source_hash,
        "input_hashes": {p.name: sha256(b) for p, b in zip(paths, raw)},
        "counts": counts, "errors": errors,
        "scope": "Neutral bundle structure/guide preflight only; no segmentation, hairstyle, dynamics or Unity acceptance.",
        "scale_provenance": coordinates.get("scaleProvenance"),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = validate(args.directory)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: report[k] for k in ("result", "source_sha256", "counts")}))
    raise SystemExit(0 if report["result"] == "PASS" else 1)
