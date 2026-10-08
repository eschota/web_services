"""Normalize the public Motion Transfer run API for the V3 viewer.

This module is deliberately independent of the private Motion Transfer toolkit.
It consumes only the documented GET run payload plus optional JSON documents
already fetched from that run.  It never starts work or guesses missing stages.
"""
from __future__ import annotations

import argparse
import json
import math
import mimetypes
import re
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


SCHEMA = "autorig.mt.viewer-manifest/1"
RUN_ID = re.compile(r"^[0-9a-f]{20}$")
TASK_ID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
VIEWS = {"front", "back", "left", "right", "top"}
RUN_STATUSES = {"queued", "running", "done", "partial", "failed"}
STAGE_STATUSES = {"waiting", "queued", "running", "done", "partial", "failed", "skipped"}
MAX_FILES, MAX_STAGES, MAX_TEXT = 1024, 512, 1000
PUBLIC = "https://autorig.online/api/mt"


class ManifestError(ValueError):
    pass


def _text(value, limit=MAX_TEXT):
    return str(value or "")[:limit]


def _file_url(run_id: str, rel: str, value: str) -> str:
    if (not isinstance(rel, str) or not re.fullmatch(r"[A-Za-z0-9._/-]{1,512}", rel)
            or any(part in {"", ".", ".."} for part in rel.split("/"))):
        raise ManifestError("invalid artifact relative path")
    parsed = urlsplit(str(value))
    expected = f"/api/mt/files/{run_id}/{rel}"
    if parsed.scheme != "https" or parsed.netloc != "autorig.online":
        raise ManifestError("artifact URL must be same-origin HTTPS")
    if parsed.path != expected or parsed.query or parsed.fragment:
        raise ManifestError("artifact URL/path does not match its declared run-relative path")
    return f"https://autorig.online{expected}"


def _all_files(run):
    rows = []
    outputs = run.get("outputs") or {}
    if not isinstance(outputs, dict):
        raise ManifestError("outputs must be an object")
    for group, entries in outputs.items():
        if not isinstance(entries, dict):
            raise ManifestError("output group must be an object")
        for rel, url in entries.items():
            rows.append({"group": _text(group, 80), "path": str(rel), "url": _file_url(run["run_id"], str(rel), str(url))})
    if len(rows) > MAX_FILES:
        raise ManifestError("too many output files")
    return sorted(rows, key=lambda item: item["path"])


def _metadata(rel, file_meta):
    raw = (file_meta or {}).get(rel) or {}
    result = {}
    if isinstance(raw.get("bytes"), int) and raw["bytes"] >= 0:
        result["bytes"] = raw["bytes"]
    digest = str(raw.get("sha256") or "").lower()
    if digest:
        if not SHA256.fullmatch(digest):
            raise ManifestError("invalid file sha256")
        result["sha256"] = digest
    return result


def _stage_rows(run):
    stages = run.get("stages") or {}
    if not isinstance(stages, dict) or len(stages) > MAX_STAGES:
        raise ManifestError("invalid stages")
    rows = []
    for name, raw in stages.items():
        if not isinstance(raw, dict):
            raise ManifestError("stage entry must be an object")
        status = str(raw.get("status") or "waiting")
        if status not in STAGE_STATUSES:
            status = "unknown"
        duration = raw.get("seconds")
        duration_valid = (isinstance(duration, (int, float)) and not isinstance(duration, bool)
                          and math.isfinite(duration) and duration >= 0)
        rows.append({"name": _text(name, 120), "status": status,
                     "duration_ms": round(float(duration) * 1000, 3) if duration_valid else None,
                     "error": _text(raw.get("error"), 500)})
    return rows


def _projection(rel, row):
    match = re.fullmatch(r"proj/(front|back|left|right|top)_([a-z0-9_]+)\.png", rel)
    if not match:
        return None
    view, render_pass = match.groups()
    return {"id": rel.replace("/", ":"), "view": view, "pass": render_pass,
            "label": f"{view} · {render_pass}", "url": row["url"], "ready": True,
            **{k:v for k,v in row.items() if k in {"bytes","sha256"}}}


def _sheet(rel, row):
    match = re.fullmatch(r"proj/sheet_([a-z0-9_]+)\.png", rel)
    if not match:
        return None
    return {"id": rel.replace("/", ":"), "pass": match.group(1), "label": f"sheet · {match.group(1)}",
            "layout": [["front", "left"], ["back", "right"]], "url": row["url"], "ready": True,
            **{k:v for k,v in row.items() if k in {"bytes","sha256"}}}


def _download(row):
    media, _ = mimetypes.guess_type(row["path"])
    return {"name": row["path"], "type": media or "application/octet-stream", "url": row["url"],
            **{k:v for k,v in row.items() if k in {"bytes","sha256"}}}


def _legend(value):
    if value is None:
        return None
    if not isinstance(value, list) or len(value) > 64:
        raise ManifestError("label legend is not a bounded list")
    rows = []
    for item in value:
        if not isinstance(item, dict) or not isinstance(item.get("index"), int):
            raise ManifestError("invalid label legend entry")
        rgb = item.get("rgb")
        if not isinstance(rgb, list) or len(rgb) != 3 or any(not isinstance(v, int) or isinstance(v, bool) or not 0 <= v <= 255 for v in rgb):
            raise ManifestError("invalid label legend color")
        rows.append({"index": item["index"], "name": _text(item.get("name"), 120), "rgb": rgb})
    return rows


def normalize_run(run: dict, *, documents: dict | None = None, file_meta: dict | None = None) -> dict:
    if not isinstance(run, dict) or not RUN_ID.fullmatch(str(run.get("run_id") or "")):
        raise ManifestError("run_id must be exactly 20 lowercase hex characters")
    run_id = run["run_id"]
    status = str(run.get("status") or "failed")
    if status not in RUN_STATUSES:
        raise ManifestError("invalid run status")
    request = run.get("request") or {}
    if not isinstance(request, dict):
        raise ManifestError("request must be an object")
    task_id = str(request.get("task_id") or "")
    if task_id and not TASK_ID.fullmatch(task_id):
        raise ManifestError("invalid task_id")
    docs = documents or {}
    if not isinstance(docs, dict):
        raise ManifestError("documents must be an object keyed by run-relative path")
    if any(not isinstance(path, str) or not isinstance(value, dict) for path, value in docs.items()):
        raise ManifestError("each optional document must be an object keyed by relative path")
    if file_meta is not None and not isinstance(file_meta, dict):
        raise ManifestError("file metadata must be an object")
    rows = []
    for row in _all_files(run):
        rows.append({**row, **_metadata(row["path"], file_meta)})
    by_path = {row["path"]: row for row in rows}

    projection_doc = docs.get("proj/manifest.json") or {}
    if projection_doc and projection_doc.get("schema") != "autorig.motion-transfer.projections/1":
        raise ManifestError("projection manifest schema mismatch")
    source_sha = str(projection_doc.get("glb_sha256") or "").lower()
    if source_sha and not SHA256.fullmatch(source_sha):
        raise ManifestError("projection manifest has invalid glb_sha256")
    model_meta = (file_meta or {}).get("proj/model.glb") or {}
    model_hash = str(model_meta.get("sha256") or "").lower()
    provenance_status = "verified" if source_sha and model_hash == source_sha else ("declared" if source_sha else "missing")
    provenance = {"status": provenance_status, "source_sha256": source_sha or None,
                  "projection_schema": projection_doc.get("schema"), "projection_task_id": projection_doc.get("task_id")}
    if task_id and projection_doc and projection_doc.get("task_id") != task_id:
        raise ManifestError("projection task_id missing or mismatched")
    if source_sha and model_hash and model_hash != source_sha:
        raise ManifestError("projection source hash does not match proj/model.glb metadata")

    projections = [item for row in rows if (item := _projection(row["path"], row))]
    sheets = [item for row in rows if (item := _sheet(row["path"], row))]
    models, videos, tracks = [], [], []
    label_docs = {path: value for path, value in docs.items() if re.fullmatch(r"labels/[^/]+/labels\.json", path)}
    for row in rows:
        rel = row["path"]
        if rel.endswith(".glb"):
            if rel == "proj/model.glb": kind, label = "source_model", "Prepared source model"
            elif re.fullmatch(r"labels/[^/]+/labels\.glb", rel):
                kind, label = "vertex_labels", rel.split("/")[1]
            elif re.fullmatch(r"(?:objects|vehicle|rig)/.*\.glb", rel):
                kind, label = "separated_model", Path(rel).stem
            else:
                kind, label = "model", Path(rel).stem
            legend_path = rel.rsplit("/", 1)[0] + "/labels.json"
            label_document = label_docs.get(legend_path) or {}
            legend = _legend(label_document.get("legend"))
            iou = label_document.get("view_iou") or {}
            confidence = (sum(float(value) for value in iou.values()) / len(iou)
                          if isinstance(iou, dict) and iou
                          and all(isinstance(value,(int,float)) and not isinstance(value,bool) and math.isfinite(value) for value in iou.values()) else None)
            models.append({"id": rel.replace("/", ":"), "label": label, "url": row["url"], "type": kind,
                           "ready": True, **({"legend": legend} if legend is not None else {}),
                           **({"confidence": confidence} if confidence is not None else {}),
                           **({"source_sha256": source_sha} if source_sha else {}),
                           **{k:v for k,v in row.items() if k in {"bytes","sha256"}}})
        if rel.endswith(".mp4"):
            videos.append({"id": rel.replace("/", ":"), "label": Path(rel).stem, "url": row["url"],
                           "type": "video/mp4", "ready": True,
                           **{k:v for k,v in row.items() if k in {"bytes","sha256"}}})
        if re.fullmatch(r"track/[^/]+/joints3d\.json", rel):
            document = docs.get(rel) or {}
            valid_doc = document.get("schema") == "autorig.motion-transfer.joints3d/1"
            track_source = str(document.get("source_sha256") or "").lower()
            coordinate = document.get("coordinate_space")
            matrix = document.get("matrix_to_model")
            positions = document.get("positions")
            frames = document.get("frames")
            joints = document.get("joints")
            valid_mask = document.get("valid")
            bones = document.get("bones")
            fps = document.get("fps")
            frame_declared = document.get("frame") == "glTF model frame (+Y up), same units as the model"
            if coordinate is None and frame_declared:
                coordinate = "model_local_gltf"
            if matrix is None and frame_declared:
                matrix = [1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1]
            shape_valid = (isinstance(frames, int) and not isinstance(frames, bool) and 1 <= frames <= 10000
                           and isinstance(fps,(int,float)) and not isinstance(fps,bool) and math.isfinite(fps) and 0 < fps <= 240
                           and isinstance(joints, list) and 1 <= len(joints) <= 2048
                           and all(isinstance(point,list) and len(point)==3
                                   and all(isinstance(value,(int,float)) and not isinstance(value,bool) and math.isfinite(value) for value in point)
                                   for point in joints)
                           and frames * len(joints) <= 2_000_000
                           and isinstance(positions, list) and len(positions) == frames
                           and all(isinstance(frame,list) and len(frame)==len(joints)
                                   and all(isinstance(point,list) and len(point)==3
                                           and all(isinstance(value,(int,float)) and not isinstance(value,bool) and math.isfinite(value) for value in point)
                                           for point in frame) for frame in positions)
                           and isinstance(valid_mask,list) and len(valid_mask)==frames
                           and all(isinstance(frame,list) and len(frame)==len(joints)
                                   and all(isinstance(value,bool) for value in frame) for frame in valid_mask)
                           and isinstance(bones,list) and len(bones) <= 4096
                           and all(isinstance(edge,list) and len(edge)==2 and all(isinstance(value,int) and not isinstance(value,bool) for value in edge)
                                   and edge[0] != edge[1] and all(0 <= value < len(joints) for value in edge) for edge in bones))
            provenance_valid = bool(source_sha and track_source == source_sha and coordinate == "model_local_gltf"
                                    and isinstance(matrix, list) and len(matrix) == 16
                                    and all(isinstance(value,(int,float)) and not isinstance(value,bool) and math.isfinite(value) for value in matrix))
            validated = bool(valid_doc and shape_valid and provenance_valid)
            tracks.append({"id": rel.replace("/", ":"), "label": rel.split("/")[1], "url": row["url"],
                           "ready": validated, "status": "validated" if validated else "available_unvalidated",
                           "reason": "" if validated else "track lacks bounded shape, coordinate, or exact source binding",
                           "schema": document.get("schema"), "coordinate_space": coordinate if provenance_valid else None,
                           "matrix_to_model": matrix if provenance_valid else None,
                           "source_sha256": track_source if provenance_valid else None,
                           "fps": fps if isinstance(fps, (int,float)) and not isinstance(fps,bool) and math.isfinite(fps) else None,
                           "frame_count": frames if isinstance(frames, int) and not isinstance(frames,bool) else None,
                           "joint_count": len(joints) if isinstance(joints, list) else None,
                           "bone_count": len(document.get("bones") or []) if isinstance(document.get("bones"), list) else None})

    phases_doc = docs.get("phases.json") or {}
    phases = None
    if "phases.json" in by_path:
        valid = phases_doc.get("schema") == "autorig.mt.phases/1" and phases_doc.get("run_id") == run_id
        phases = {"url": by_path["phases.json"]["url"], "ready": bool(valid), "schema": phases_doc.get("schema"),
                  "status": phases_doc.get("status"), "phase_count": len(phases_doc.get("phases") or []) if valid else None}

    terminal = status in {"done", "partial", "failed"}
    wall = run.get("seconds")
    wall_valid = (isinstance(wall,(int,float)) and not isinstance(wall,bool)
                  and math.isfinite(wall) and wall >= 0)
    lifecycle = {"queue_status": "queued" if status == "queued" else "left_queue",
                 "processing_status": "waiting" if status == "queued" else ("running" if status == "running" else "terminal"),
                 "created_at": run.get("created_at"), "finished_at": run.get("finished_at"),
                 "queue_duration_ms": None,
                 "processing_duration_ms": None,
                 "wall_duration_ms": round(float(wall)*1000,3) if terminal and wall_valid else None}
    pose = [row["url"] for row in rows if re.fullmatch(r"maps/pose_(?:best|1|2)\.png", row["path"])]
    vehicle = [row["url"] for row in rows if row["path"].startswith(("vehicle/", "rig/"))]
    diorama_images = [row["url"] for row in rows if re.fullmatch(r"(?:diorama|maps)/diorama[^/]*\.(?:png|jpg|jpeg|webp)", row["path"], re.I)]
    final_videos = [row["url"] for row in rows if re.fullmatch(r"(?:diorama|final|motion)/final[^/]*\.mp4", row["path"], re.I)]
    file_paths={row["path"] for row in rows}
    raw_stages=run.get("stages") or {}
    def aggregate(prefix, available):
        values=[str(item.get("status") or "waiting") for name,item in raw_stages.items()
                if name==prefix or name.startswith(prefix+":")]
        if any(value=="running" for value in values): return "running"
        if values and all(value=="done" for value in values): return "done"
        if any(value=="failed" for value in values): return "partial" if available else "failed"
        return "available" if available else "missing"
    dag=[
        {"id":"source","status":"done" if "proj/model.glb" in file_paths else "missing","depends_on":[],"branch":"fast_geometry"},
        {"id":"projections","status":aggregate("projections",bool(projections)),"depends_on":["source"],"branch":"fast_geometry"},
        {"id":"semantic_maps","status":aggregate("map",any(path.startswith("maps/") for path in file_paths)),"depends_on":["projections"],"branch":"fast_geometry"},
        {"id":"vertex_label_fusion","status":aggregate("labels",any(path.startswith("labels/") for path in file_paths)),"depends_on":["semantic_maps"],"branch":"fast_geometry"},
        {"id":"motion","status":aggregate("video",any(path.startswith("motion/") for path in file_paths)),"depends_on":["projections"],"branch":"fast_geometry"},
        {"id":"tracks","status":aggregate("track",bool(tracks)),"depends_on":["motion","projections"],"branch":"fast_geometry"},
        {"id":"stabilized_pose","status":"done" if any("pose_best" in url for url in pose) else "planned","depends_on":["projections"],"branch":"presentation"},
        {"id":"diorama_image","status":"done" if diorama_images else "planned","depends_on":["stabilized_pose"],"branch":"presentation","artifact_type":"image_2d","requested_radius_m":3.0},
        {"id":"final_video","status":"done" if final_videos else "planned","depends_on":["diorama_image"],"branch":"presentation","artifact_type":"video"},
    ]
    available=[item["id"] for item in dag if item["status"] not in {"missing","failed","planned"}]
    return {"schema": SCHEMA, "run_id": run_id, "kind": _text(run.get("kind"), 40), "task_id": task_id or None,
            "source_sha256": source_sha or None, "status": status, "ready": status in {"done","partial"},
            "complete": status == "done",
            "error": _text(run.get("error"), 500), "lifecycle": lifecycle, "provenance": provenance,
            "scope": {"available": available,
                      "missing": ["anatomical_weights", "fitted_bones", "skin", "validated_deformation"],
                      "note": "Motion Transfer evidence is analysis input; it is not a finished rig."},
            "dag": dag, "stages": _stage_rows(run), "projections": projections, "sheets": sheets, "models": models,
            "videos": videos, "tracks3d": tracks,
            "outputs": {"pose": pose, "vehicle": vehicle, "diorama_image": diorama_images,
                        "final_video": final_videos, "phases": phases},
            "downloads": [_download(row) for row in rows]}


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ManifestError("redirects are not allowed by the bounded MT client")


def _get_json(url: str, *, limit: int = 4 * 1024 * 1024, run_id: str | None = None) -> dict:
    parsed=urlsplit(url)
    allowed = f"/api/mt/files/{run_id}/" if run_id else "/api/mt/runs/"
    if parsed.scheme!="https" or parsed.netloc!="autorig.online" or not parsed.path.startswith(allowed) or parsed.query or parsed.fragment:
        raise ManifestError("untrusted MT JSON URL")
    request = Request(url, headers={"User-Agent": "AutoRigAudit/1.0", "Accept": "application/json"})
    with build_opener(_NoRedirect).open(request, timeout=60) as response:
        payload = response.read(limit + 1)
    if len(payload) > limit:
        raise ManifestError("JSON response exceeds bounded client limit")
    value = json.loads(payload)
    if not isinstance(value, dict):
        raise ManifestError("JSON response root must be an object")
    return value


def fetch_existing_run(run_id: str) -> tuple[dict, dict]:
    if not RUN_ID.fullmatch(run_id):
        raise ManifestError("run_id must be exactly 20 lowercase hex characters")
    run = _get_json(f"{PUBLIC}/runs/{run_id}")
    documents = {}
    wanted = {"proj/manifest.json", "phases.json"}
    for group in (run.get("outputs") or {}).values():
        if isinstance(group, dict):
            for rel in group:
                if (re.fullmatch(r"labels/[^/]+/labels\.json", rel)
                        or re.fullmatch(r"track/[^/]+/joints3d\.json", rel)
                        or rel in {"vehicle/rig.json", "rig/rig.json", "objects/objects.json", "context.json"}):
                    wanted.add(rel)
    declared = {row["path"]: row["url"] for row in _all_files(run)}
    for rel in sorted(wanted & declared.keys()):
        documents[rel] = _get_json(declared[rel], run_id=run_id)
    return run, documents


def submit_run(task_id: str, key_file: Path, options: dict | None = None) -> tuple[dict, dict]:
    if not TASK_ID.fullmatch(task_id):
        raise ManifestError("submit task_id must be a UUID")
    key = key_file.read_text(encoding="utf-8").strip()
    if not key:
        raise ManifestError("empty API key file")
    if options is not None and not isinstance(options, dict):
        raise ManifestError("submit options must be an object")
    if options and "task_id" in options:
        raise ManifestError("submit options must not override task_id")
    body = {**(options or {}), "task_id": task_id}
    request = Request(f"{PUBLIC}/kit", data=json.dumps(body).encode(), method="POST",
                      headers={"User-Agent": "AutoRigAudit/1.0", "Accept": "application/json",
                               "Content-Type": "application/json", "Authorization": f"Bearer {key}"})
    with build_opener(_NoRedirect).open(request, timeout=60) as response:
        receipt = json.loads(response.read(1024 * 1024))
    run_id = str(receipt.get("run_id") or "")
    return fetch_existing_run(run_id)


def main(argv=None):
    parser = argparse.ArgumentParser()
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--run-json", type=Path)
    source.add_argument("--run-id")
    source.add_argument("--submit-task-id")
    parser.add_argument("--documents-json", type=Path)
    parser.add_argument("--file-meta-json", type=Path)
    parser.add_argument("--api-key-file", type=Path)
    parser.add_argument("--submit-options-json", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    output = args.output.resolve()
    project = Path(__file__).resolve().parents[2]
    if not output.is_relative_to(project) or output.exists():
        raise SystemExit("output must be a fresh path inside autorig-online")
    load = lambda path: json.loads(path.read_text(encoding="utf-8")) if path else None
    if args.submit_task_id:
        if not args.api_key_file:
            raise SystemExit("--submit-task-id requires explicit --api-key-file")
        run, documents = submit_run(args.submit_task_id, args.api_key_file, load(args.submit_options_json))
    elif args.run_id:
        run, documents = fetch_existing_run(args.run_id)
    else:
        run, documents = load(args.run_json), load(args.documents_json)
    manifest = normalize_run(run, documents=documents, file_meta=load(args.file_meta_json))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(manifest, indent=2, sort_keys=True, allow_nan=False), encoding="utf-8")
    print(json.dumps({"status": manifest["status"], "ready": manifest["ready"], "output": str(output)}))


if __name__ == "__main__":
    main()
