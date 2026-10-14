"""SQLite state and leases for the restart-safe vehicle pipeline."""
from __future__ import annotations

import hashlib
import json
import os
import secrets
import sqlite3
import time
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit


DEFAULT_ROOT = Path(os.getenv("AUTORIG_VEHICLE_PIPELINE_DIR", "/srv/autorig/data/var/vehicle-pipeline"))
PIPELINE_VERSION = os.getenv("AUTORIG_VEHICLE_PIPELINE_VERSION", "vehicle-v3-2026-10-15.5-body-cleanup")
TERMINAL = {"completed", "failed"}


def feature_identity(run_id: str, original_sha: str, prepared_sha: str | None,
                     geometry_sha: str, voxel_sha: str) -> str:
    return hashlib.sha256(json.dumps({"run_id": run_id, "original": original_sha, "prepared": prepared_sha,
        "geometry": geometry_sha, "voxel": voxel_sha}, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def owner_key(owner: Any) -> str:
    owner_type = getattr(owner, "owner_type", None)
    owner_id = getattr(owner, "owner_id", None)
    if owner_type not in {"user", "anon"} or not owner_id:
        raise ValueError("vehicle owner is not resolved")
    return f"{owner_type}:{owner_id}"


class RevisionConflict(RuntimeError):
    pass


class _ClosingConnection(sqlite3.Connection):
    def __exit__(self, exc_type, exc, traceback):
        try:
            return super().__exit__(exc_type, exc, traceback)
        finally:
            self.close()


class VehicleStore:
    def __init__(self, root: Path | str = DEFAULT_ROOT) -> None:
        self.root = Path(root)
        self.runs_dir = self.root / "runs"
        self.root.mkdir(parents=True, exist_ok=True)
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.root / "vehicle.sqlite3"
        self._init()

    def connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.db_path, timeout=15, isolation_level=None, factory=_ClosingConnection)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA busy_timeout=15000")
        return db

    def _init(self) -> None:
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS vehicle_runs(
                  id TEXT PRIMARY KEY, owner_key TEXT NOT NULL, source_sha256 TEXT NOT NULL,
                  capability TEXT NOT NULL, revision INTEGER NOT NULL, status TEXT NOT NULL,
                  stage TEXT NOT NULL, active_job_id INTEGER, document_json TEXT NOT NULL,
                  created_at REAL NOT NULL, updated_at REAL NOT NULL);
                CREATE INDEX IF NOT EXISTS vehicle_runs_owner ON vehicle_runs(owner_key, updated_at);
                CREATE TABLE IF NOT EXISTS vehicle_jobs(
                  id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL, action TEXT NOT NULL,
                  idempotency_key TEXT NOT NULL UNIQUE, request_json TEXT NOT NULL,
                  status TEXT NOT NULL DEFAULT 'queued', attempts INTEGER NOT NULL DEFAULT 0,
                  lease_token TEXT, lease_until REAL, receipt_json TEXT, error TEXT,
                  created_at REAL NOT NULL, updated_at REAL NOT NULL,
                  FOREIGN KEY(run_id) REFERENCES vehicle_runs(id));
                CREATE INDEX IF NOT EXISTS vehicle_jobs_claim
                  ON vehicle_jobs(status, lease_until, created_at);
                CREATE TABLE IF NOT EXISTS vehicle_analysis(
                  source_sha256 TEXT NOT NULL, pipeline_version TEXT NOT NULL,
                  manifest_json TEXT NOT NULL, updated_at REAL NOT NULL,
                  PRIMARY KEY(source_sha256, pipeline_version));
                CREATE TABLE IF NOT EXISTS vehicle_stage_receipts(
                  source_sha256 TEXT NOT NULL, pipeline_version TEXT NOT NULL, stage TEXT NOT NULL,
                  receipt_json TEXT NOT NULL, updated_at REAL NOT NULL,
                  PRIMARY KEY(source_sha256, pipeline_version, stage));
                CREATE TABLE IF NOT EXISTS vehicle_prepare_candidates(
                  run_id TEXT NOT NULL, camera_attempt INTEGER NOT NULL, receipt_json TEXT NOT NULL,
                  updated_at REAL NOT NULL, PRIMARY KEY(run_id, camera_attempt),
                  FOREIGN KEY(run_id) REFERENCES vehicle_runs(id));
                CREATE TABLE IF NOT EXISTS vehicle_features(
                  source_sha256 TEXT NOT NULL, pipeline_version TEXT NOT NULL,
                  category TEXT, title TEXT, dimensions_json TEXT NOT NULL,
                  materials_json TEXT NOT NULL, objects_json TEXT NOT NULL,
                  voxel_embedding_json TEXT NOT NULL, updated_at REAL NOT NULL,
                  PRIMARY KEY(source_sha256, pipeline_version));
                CREATE TABLE IF NOT EXISTS vehicle_source_chain(
                  run_id TEXT PRIMARY KEY, original_sha256 TEXT NOT NULL, prepared_sha256 TEXT,
                  prepared_path TEXT, pipeline_version TEXT NOT NULL, updated_at REAL NOT NULL,
                  FOREIGN KEY(run_id) REFERENCES vehicle_runs(id));
                CREATE TABLE IF NOT EXISTS vehicle_part_reviews(
                  review_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, part_id TEXT NOT NULL,
                  owner_key TEXT NOT NULL, run_revision INTEGER NOT NULL,
                  original_sha256 TEXT NOT NULL, prepared_sha256 TEXT NOT NULL, mask_sha256 TEXT NOT NULL,
                  receipt_json TEXT NOT NULL, created_at REAL NOT NULL,
                  FOREIGN KEY(run_id) REFERENCES vehicle_runs(id));
                CREATE TABLE IF NOT EXISTS vehicle_reference_reviews(
                  review_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, owner_key TEXT NOT NULL,
                  run_revision INTEGER NOT NULL, original_sha256 TEXT NOT NULL, prepared_sha256 TEXT NOT NULL,
                  camera_receipt_json TEXT NOT NULL, receipt_json TEXT NOT NULL, created_at REAL NOT NULL,
                  FOREIGN KEY(run_id) REFERENCES vehicle_runs(id));
                CREATE TABLE IF NOT EXISTS vehicle_feature_versions(
                  identity_sha256 TEXT PRIMARY KEY, run_id TEXT NOT NULL, owner_key TEXT NOT NULL,
                  original_sha256 TEXT NOT NULL, prepared_sha256 TEXT,
                  geometry_options_sha256 TEXT NOT NULL, voxel_artifact_sha256 TEXT NOT NULL,
                  feature_json TEXT NOT NULL, created_at REAL NOT NULL,
                  FOREIGN KEY(run_id) REFERENCES vehicle_runs(id));
            """)

    @staticmethod
    def run_id() -> str:
        return "veh_" + secrets.token_hex(16)

    def run_dir(self, run_id: str) -> Path:
        if not run_id.startswith("veh_") or len(run_id) != 36:
            raise ValueError("invalid vehicle run id")
        path = self.runs_dir / run_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def create(self, owner: Any, document: Mapping[str, Any], source_sha256: str) -> dict[str, Any]:
        now = time.time()
        value = dict(document)
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "INSERT INTO vehicle_runs VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (value["id"], owner_key(owner), source_sha256, value.pop("_capability"),
                 value["revision"], value["status"], value["stage"], None,
                 json.dumps(value, ensure_ascii=False, separators=(",", ":")), now, now),
            )
            job_id = self._enqueue_db(db, value["id"], "analyze", {"action": "analyze", "options": value["options"]}, value["revision"])
            value["stages"] = [{"stage": "analyze", "status": "queued", "job_id": job_id}]
            db.execute("UPDATE vehicle_runs SET active_job_id=?,document_json=? WHERE id=?",
                       (job_id, json.dumps(value, ensure_ascii=False, separators=(",", ":")), value["id"]))
            row = db.execute("SELECT document_json FROM vehicle_runs WHERE id=?", (value["id"],)).fetchone()
            db.execute("COMMIT")
        return json.loads(row[0])

    def get(self, run_id: str, owner: Any | None = None) -> dict[str, Any]:
        with self.connect() as db:
            row = db.execute("SELECT * FROM vehicle_runs WHERE id=?", (run_id,)).fetchone()
        if row is None or (owner is not None and row["owner_key"] != owner_key(owner)):
            raise KeyError(run_id)
        return json.loads(row["document_json"])

    def capability_path(self, run_id: str, token: str, name: str) -> Path:
        with self.connect() as db:
            row = db.execute("SELECT capability FROM vehicle_runs WHERE id=?", (run_id,)).fetchone()
        if row is None or not secrets.compare_digest(str(row[0]), token):
            raise KeyError(run_id)
        allowed = {"source.png", "source.jpg", "manifest.json", "inventory.json", "relative_depth.png",
                   "semantic_observation.png", "voxels.npz", "voxels.glb", "vehicle-assembly.glb",
                   "vehicle-assembly.report.json", "wheels.json"}
        run_root = self.run_dir(run_id)
        manifest_path = run_root / "pipeline" / "manifest.json"
        if manifest_path.is_file() and not manifest_path.is_symlink():
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                allowed.update(str(item.get("relative_path_string") or item.get("path_string") or "")
                               for item in manifest.get("artifacts_array") or [] if isinstance(item, Mapping))
            except (OSError, json.JSONDecodeError):
                pass
        with self.connect() as db:
            run_row = db.execute("SELECT document_json FROM vehicle_runs WHERE id=?", (run_id,)).fetchone()
        def strings(value):
            if isinstance(value, str): yield value
            elif isinstance(value, Mapping):
                for child in value.values(): yield from strings(child)
            elif isinstance(value, list):
                for child in value: yield from strings(child)
        if run_row:
            document = json.loads(run_row[0])
            prefix = f"/api/vehicle/assets/{run_id}/{token}/"
            for value in strings(document.get("artifacts") or {}):
                path_value = urlsplit(value).path
                if path_value.startswith(prefix): allowed.add(path_value[len(prefix):])
        if name not in allowed:
            raise KeyError(name)
        path = run_root / name
        if not path.exists(): path = run_root / "pipeline" / name
        run_root = run_root.resolve()
        try: path.resolve().relative_to(run_root)
        except ValueError: raise KeyError(name) from None
        if path.is_symlink() or not path.is_file():
            raise KeyError(name)
        return path

    def enqueue(self, run_id: str, owner: Any, request: Mapping[str, Any], expected_revision: int | None) -> dict[str, Any]:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM vehicle_runs WHERE id=?", (run_id,)).fetchone()
            if row is None or row["owner_key"] != owner_key(owner):
                db.execute("ROLLBACK")
                raise KeyError(run_id)
            if expected_revision is not None and int(expected_revision) != int(row["revision"]):
                db.execute("ROLLBACK")
                raise RevisionConflict(f"expected revision {expected_revision}, current {row['revision']}")
            if row["active_job_id"] is not None:
                db.execute("ROLLBACK")
                raise RevisionConflict("vehicle run already has an active stage")
            document = json.loads(row["document_json"])
            action = str(request["action"])
            requested_retry = action == "retry"
            if document.get("status") == "failed" and not requested_retry:
                db.execute("ROLLBACK")
                raise ValueError("failed vehicle stage must be retried before another action")
            if action == "retry":
                if document.get("status") == "needs_review" and document.get("stage") in {
                        "prepare", "remove_wheels", "project_views"}:
                    action = str(document["stage"])
                else:
                    action = str(document.get("failed_stage") or "")
                if not action:
                    db.execute("ROLLBACK")
                    raise ValueError("run has no failed stage to retry")
            else:
                allowed_next = {"analyze": {"prepare"}, "prepare": {"remove_wheels"},
                                "remove_wheels": {"project_views"}, "project_views": {"depth_views"},
                                "depth_views": {"fuse_voxels"}, "fuse_voxels": {"cluster", "configure"},
                                "voxelize": {"cluster"}, "cluster": {"model"},
                                "model": {"model", "rig"}, "rig": {"model", "configure"},
                                "configure": {"cluster", "model", "configure"}}
                current_stage = str(document.get("stage") or "analyze")
                if action not in allowed_next.get(current_stage, set()):
                    db.execute("ROLLBACK")
                    if current_stage == "analyze" and action == "voxelize":
                        raise ValueError("needs_camera_preparation: prepare must follow analyze")
                    expected = ", ".join(sorted(allowed_next.get(current_stage, set()))) or "none"
                    raise ValueError(f"{action} cannot follow {current_stage}; expected {expected}")
                if current_stage == "prepare" and action in {"remove_wheels", "voxelize"}:
                    camera = document.get("camera") or {}
                    measured = (camera.get("normalization_status_string") == "accepted" and
                             camera.get("projection_string") == "orthographic" and
                             abs(float(camera.get("yaw_deg_float", -999)) - 45.0) <= .5 and
                             abs(float(camera.get("pitch_deg_float", -999)) - 45.0) <= .5 and
                             bool(document.get("prepared_image_url")))
                    chain = document.get("source_sha_chain") or {}; review = document.get("reference_review") or {}
                    owner_reviewed = (bool(document.get("prepared_image_url")) and
                                      review.get("original_sha256") == chain.get("original_sha256") and
                                      review.get("prepared_sha256") == chain.get("prepared_sha256"))
                    if not (measured or owner_reviewed):
                        db.execute("ROLLBACK")
                        raise ValueError("needs_camera_preparation: accepted orthographic yaw45/pitch45 image is required")
                geometry = dict(request.get("options") or {})
                base = dict(document.get("options") or {})
                aliases = {"resolution": "resolution", "voxel_resolution_int": "resolution",
                           "working_side": "working_side", "working_side_string": "working_side",
                           "length_m": "length_m", "target_length_m_float": "length_m"}
                for supplied, canonical in aliases.items():
                    if supplied not in geometry: continue
                    wanted, fixed = geometry[supplied], base.get(canonical)
                    if wanted in (0, 0.0, "0", "0.0", ""): wanted = None
                    if wanted != fixed:
                        db.execute("ROLLBACK")
                        raise ValueError(f"{supplied} is fixed when the run is created; create a new vehicle run")
                if action == "model" and request.get("mode") == "split":
                    selected = {str(item) for item in request.get("part_ids") or []}
                    accepted = document.get("accepted_parts") or {}
                    current_parts = {str(part.get("part_id_string") or part.get("id") or ""): part
                                     for part in document.get("parts") or []}
                    missing = []
                    for part_id in selected:
                        part, review = current_parts.get(part_id) or {}, accepted.get(part_id) or {}
                        if (not review or review.get("mask_sha256") != part.get("mask_sha256") or
                                review.get("original_sha256") != (document.get("source_sha_chain") or {}).get("original_sha256") or
                                review.get("prepared_sha256") != (document.get("source_sha_chain") or {}).get("prepared_sha256")):
                            missing.append(part_id)
                    if not selected or missing:
                        db.execute("ROLLBACK")
                        raise ValueError("selected parts need current owner review: " + ", ".join(sorted(missing or selected)))
                if action == "configure" and not document.get("wheel_rig_revision"):
                    db.execute("ROLLBACK"); raise ValueError("validated vehicle wheel rig is unavailable")
            clean = dict(request); clean["action"] = action
            if action == "prepare":
                previous_attempt = int(document.get("camera_attempt_int", -1))
                camera_attempt = previous_attempt + 1
                if requested_retry and camera_attempt > 3:
                    db.execute("ROLLBACK")
                    raise ValueError("camera preparation retry limit reached (3 corrections)")
                if not requested_retry: camera_attempt = 0
                clean["camera_attempt_int"] = camera_attempt
                document["camera_attempt_int"] = camera_attempt
                document["prepare_retry_available_bool"] = camera_attempt < 3
            if action == "remove_wheels":
                attempt = int(document.get("remove_attempt_int", -1)) + 1
                if not requested_retry: attempt = 0
                if requested_retry and attempt > 3:
                    db.execute("ROLLBACK"); raise ValueError("body cleanup retry limit reached (3 corrections)")
                clean["remove_attempt_int"] = attempt; document["remove_attempt_int"] = attempt
                document["body_cleanup_retry_available_bool"] = attempt < 3
            if action == "project_views":
                attempts = dict(document.get("view_attempts_object") or {})
                if not requested_retry: attempts = {view: 0 for view in ("top", "front", "left", "back")}
                else:
                    views = document.get("views_object") or {}; failed = [view for view in ("top", "front", "left", "back")
                              if (views.get(view) or {}).get("status_string") != "complete"]
                    if not failed: db.execute("ROLLBACK"); raise ValueError("no projected view needs retry")
                    for view in failed:
                        attempts[view] = int(attempts.get(view, 0)) + 1
                        if attempts[view] > 3:
                            db.execute("ROLLBACK"); raise ValueError(f"{view} projection retry limit reached")
                clean["view_attempts_object"] = attempts; document["view_attempts_object"] = attempts
                document["project_views_retry_available_bool"] = any(int(value) < 3 for value in attempts.values())
            job_id = self._enqueue_db(db, run_id, action, clean, row["revision"])
            document.update(status="queued", stage=action, progress=0, error=None,
                            revision=int(row["revision"]) + 1)
            document.setdefault("stages", []).append({"stage": action, "status": "queued", "job_id": job_id})
            if action == "prepare": document["stages"][-1]["camera_attempt_int"] = clean["camera_attempt_int"]
            if action == "remove_wheels": document["stages"][-1]["remove_attempt_int"] = clean["remove_attempt_int"]
            if action == "project_views": document["stages"][-1]["view_attempts_object"] = clean["view_attempts_object"]
            db.execute("UPDATE vehicle_runs SET revision=?,status='queued',stage=?,active_job_id=?,document_json=?,updated_at=? WHERE id=?",
                       (document["revision"], action, job_id, json.dumps(document, ensure_ascii=False, separators=(",", ":")), time.time(), run_id))
            db.execute("COMMIT")
        return document

    def accept_reference(self, run_id: str, owner: Any, request: Mapping[str, Any], expected_revision: int | None) -> dict[str, Any]:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM vehicle_runs WHERE id=?", (run_id,)).fetchone()
            if row is None or row["owner_key"] != owner_key(owner):
                db.execute("ROLLBACK"); raise KeyError(run_id)
            if expected_revision is None or int(expected_revision) != int(row["revision"]):
                db.execute("ROLLBACK"); raise RevisionConflict(f"expected revision {expected_revision}, current {row['revision']}")
            if row["active_job_id"] is not None:
                db.execute("ROLLBACK"); raise RevisionConflict("vehicle run already has an active stage")
            document = json.loads(row["document_json"])
            if document.get("stage") != "prepare" or not document.get("prepared_image_url") or not document.get("camera"):
                db.execute("ROLLBACK"); raise ValueError("prepared vehicle reference is unavailable")
            proof = request.get("reference_evidence") or {}; chain = document.get("source_sha_chain") or {}
            supplied = (proof.get("original_sha256"), proof.get("prepared_sha256"))
            expected = (chain.get("original_sha256"), chain.get("prepared_sha256"))
            if not all(expected) or supplied != expected:
                db.execute("ROLLBACK"); raise ValueError("reference evidence is stale or forged")
            now = time.time(); receipt = {"run_id": run_id, "run_revision": int(row["revision"]),
                "original_sha256": supplied[0], "prepared_sha256": supplied[1], "accepted_at": now,
                "acceptance_kind": "owner_reference_review"}
            review_id = hashlib.sha256(json.dumps({**receipt, "owner_key": row["owner_key"]}, sort_keys=True).encode()).hexdigest()
            db.execute("INSERT OR IGNORE INTO vehicle_reference_reviews VALUES(?,?,?,?,?,?,?,?,?)",
                       (review_id, run_id, row["owner_key"], row["revision"], supplied[0], supplied[1],
                        json.dumps(document["camera"], ensure_ascii=False), json.dumps(receipt, ensure_ascii=False), now))
            document.update(reference_accepted_bool=True, reference_review_id=review_id,
                            reference_review_revision=int(row["revision"]), reference_review={**receipt, "review_id": review_id},
                            prepare_retry_available_bool=False, status="completed", revision=int(row["revision"]) + 1)
            document.setdefault("stages", []).append({"stage": "accept_reference", "status": "completed",
                "review_id": review_id, "revision": document["revision"]})
            db.execute("UPDATE vehicle_runs SET revision=?,status='completed',document_json=?,updated_at=? WHERE id=? AND revision=?",
                       (document["revision"], json.dumps(document, ensure_ascii=False, separators=(",", ":")), now,
                        run_id, row["revision"]))
            db.execute("COMMIT")
        return document

    def accept_parts(self, run_id: str, owner: Any, request: Mapping[str, Any], expected_revision: int | None) -> dict[str, Any]:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM vehicle_runs WHERE id=?", (run_id,)).fetchone()
            if row is None or row["owner_key"] != owner_key(owner):
                db.execute("ROLLBACK"); raise KeyError(run_id)
            if expected_revision is None or int(expected_revision) != int(row["revision"]):
                db.execute("ROLLBACK"); raise RevisionConflict(f"expected revision {expected_revision}, current {row['revision']}")
            if row["active_job_id"] is not None:
                db.execute("ROLLBACK"); raise RevisionConflict("vehicle run already has an active stage")
            document = json.loads(row["document_json"]); camera = document.get("camera") or {}
            measured_camera = (camera.get("normalization_status_string") == "accepted" and
                            camera.get("projection_string") == "orthographic" and
                            abs(float(camera.get("yaw_deg_float", -999)) - 45.0) <= .5 and
                            abs(float(camera.get("pitch_deg_float", -999)) - 45.0) <= .5)
            chain = document.get("source_sha_chain") or {}; reference = document.get("reference_review") or {}
            owner_camera = (reference.get("original_sha256") == chain.get("original_sha256") and
                            reference.get("prepared_sha256") == chain.get("prepared_sha256"))
            if not (measured_camera or owner_camera):
                db.execute("ROLLBACK"); raise ValueError("canonical camera is not accepted")
            parts = {
                str(part.get("part_id_string") or part.get("id") or ""): part for part in document.get("parts") or []}
            evidence = {str(item.get("part_id") or ""): item for item in request.get("part_evidence") or []}
            selected = [str(item) for item in request.get("part_ids") or []]
            if len(set(selected)) != len(selected) or set(evidence) != set(selected):
                db.execute("ROLLBACK"); raise ValueError("part evidence ids must exactly match selected part ids")
            accepted = dict(document.get("accepted_parts") or {}); now = time.time()
            for part_id in selected:
                part, proof = parts.get(part_id), evidence[part_id]
                bbox = (part or {}).get("source_bbox_px_array") or (part or {}).get("bbox_px_array") or []
                if (not part or len(bbox) != 4 or float(bbox[2]) <= float(bbox[0]) or float(bbox[3]) <= float(bbox[1])
                        or not part.get("mask_url") or not part.get("mask_sha256")):
                    db.execute("ROLLBACK"); raise ValueError(f"part {part_id} has no valid source-bound mask geometry")
                shape_gate = part.get("shape_gate_object") or {}
                if shape_gate.get("accepted_bool") is False:
                    db.execute("ROLLBACK"); raise ValueError(f"part {part_id} failed its geometry gate")
                expected = (chain.get("original_sha256"), chain.get("prepared_sha256"), part.get("mask_sha256"))
                supplied = (proof.get("original_sha256"), proof.get("prepared_sha256"), proof.get("mask_sha256"))
                if supplied != expected:
                    db.execute("ROLLBACK"); raise ValueError(f"part {part_id} evidence is stale or forged")
                receipt = {"part_id": part_id, "owner_key": row["owner_key"], "run_revision": int(row["revision"]),
                           "original_sha256": supplied[0], "prepared_sha256": supplied[1], "mask_sha256": supplied[2],
                           "accepted_at": now}
                review_id = hashlib.sha256(json.dumps(receipt, sort_keys=True).encode()).hexdigest()
                db.execute("INSERT OR IGNORE INTO vehicle_part_reviews VALUES(?,?,?,?,?,?,?,?,?,?)",
                           (review_id, run_id, part_id, row["owner_key"], row["revision"], supplied[0], supplied[1], supplied[2],
                            json.dumps(receipt, ensure_ascii=False), now))
                accepted[part_id] = {**receipt, "review_id": review_id}
            document["accepted_parts"] = accepted; document["revision"] = int(row["revision"]) + 1
            for part in document.get("parts") or []:
                part_id = str(part.get("part_id_string") or part.get("id") or "")
                review = accepted.get(part_id)
                if review and review.get("mask_sha256") == part.get("mask_sha256"):
                    part.update(accepted_bool=True, review_id=review["review_id"],
                                review_revision=review["run_revision"])
            document.setdefault("stages", []).append({"stage": "accept_parts", "status": "completed",
                                                        "part_ids": selected, "revision": document["revision"]})
            db.execute("UPDATE vehicle_runs SET revision=?,document_json=?,updated_at=? WHERE id=? AND revision=?",
                       (document["revision"], json.dumps(document, ensure_ascii=False, separators=(",", ":")), now,
                        run_id, row["revision"]))
            db.execute("COMMIT")
        return document

    def _enqueue_db(self, db: sqlite3.Connection, run_id: str, action: str, request: Mapping[str, Any], revision: int) -> int:
        canonical = json.dumps(request, sort_keys=True, separators=(",", ":"))
        key = hashlib.sha256(f"{run_id}:{revision}:{action}:{canonical}".encode()).hexdigest()
        now = time.time()
        cursor = db.execute("INSERT INTO vehicle_jobs(run_id,action,idempotency_key,request_json,created_at,updated_at) VALUES(?,?,?,?,?,?)",
                            (run_id, action, key, canonical, now, now))
        return int(cursor.lastrowid)

    def claim(self, lease_seconds: float = 300) -> sqlite3.Row | None:
        now = time.time(); token = secrets.token_hex(16)
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM vehicle_jobs WHERE status IN ('queued','running') AND (lease_until IS NULL OR lease_until<?) ORDER BY created_at,id LIMIT 1", (now,)).fetchone()
            if row is None:
                db.execute("COMMIT"); return None
            changed = db.execute("UPDATE vehicle_jobs SET status='running',attempts=attempts+1,lease_token=?,lease_until=?,updated_at=? WHERE id=? AND (lease_until IS NULL OR lease_until<?)",
                                 (token, now + lease_seconds, now, row["id"], now)).rowcount
            if changed != 1:
                db.execute("ROLLBACK"); return None
            db.execute("UPDATE vehicle_runs SET status='running',updated_at=? WHERE id=?", (now, row["run_id"]))
            db.execute("COMMIT")
        with self.connect() as db:
            return db.execute("SELECT * FROM vehicle_jobs WHERE id=? AND lease_token=?", (row["id"], token)).fetchone()

    def finish(self, job: sqlite3.Row, document: Mapping[str, Any], receipt: Mapping[str, Any]) -> None:
        now = time.time(); value = dict(document)
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            changed = db.execute("UPDATE vehicle_jobs SET status='completed',receipt_json=?,lease_token=NULL,lease_until=NULL,updated_at=? WHERE id=? AND lease_token=?",
                                 (json.dumps(receipt, ensure_ascii=False), now, job["id"], job["lease_token"])).rowcount
            if changed != 1: db.execute("ROLLBACK"); raise RevisionConflict("vehicle stage lease was lost")
            db.execute("UPDATE vehicle_runs SET revision=?,status=?,stage=?,active_job_id=NULL,document_json=?,updated_at=? WHERE id=? AND active_job_id=?",
                       (value["revision"], value["status"], value["stage"], json.dumps(value, ensure_ascii=False, separators=(",", ":")), now, job["run_id"], job["id"]))
            if job["action"] in {"analyze", "prepare", "remove_wheels", "project_views", "depth_views",
                                 "fuse_voxels", "cluster", "voxelize"}:
                db.execute("INSERT INTO vehicle_analysis VALUES(?,?,?,?) ON CONFLICT(source_sha256,pipeline_version) DO UPDATE SET manifest_json=excluded.manifest_json,updated_at=excluded.updated_at",
                           (value["source_sha256"], PIPELINE_VERSION, json.dumps(receipt, ensure_ascii=False), now))
                db.execute("INSERT INTO vehicle_stage_receipts VALUES(?,?,?,?,?) ON CONFLICT(source_sha256,pipeline_version,stage) DO UPDATE SET receipt_json=excluded.receipt_json,updated_at=excluded.updated_at",
                           (value["source_sha256"], PIPELINE_VERSION, job["action"], json.dumps(receipt, ensure_ascii=False), now))
                if job["action"] == "prepare":
                    db.execute("INSERT INTO vehicle_prepare_candidates VALUES(?,?,?,?) ON CONFLICT(run_id,camera_attempt) DO UPDATE SET receipt_json=excluded.receipt_json,updated_at=excluded.updated_at",
                               (job["run_id"], int(value.get("camera_attempt_int", 0)),
                                json.dumps(receipt, ensure_ascii=False), now))
                prepared = receipt.get("prepared_image_object") or {}
                prepared_sha = str(prepared.get("sha256_string") or receipt.get("prepared_image_sha256_string") or "") or None
                prepared_path = str(prepared.get("path_string") or receipt.get("prepared_image_path_string") or "") or None
                db.execute("INSERT INTO vehicle_source_chain VALUES(?,?,?,?,?,?) ON CONFLICT(run_id) DO UPDATE SET prepared_sha256=COALESCE(excluded.prepared_sha256,vehicle_source_chain.prepared_sha256),prepared_path=COALESCE(excluded.prepared_path,vehicle_source_chain.prepared_path),pipeline_version=excluded.pipeline_version,updated_at=excluded.updated_at",
                           (job["run_id"], value["source_sha256"], prepared_sha, prepared_path, PIPELINE_VERSION, now))
                analysis = receipt.get("analysis_object") if isinstance(receipt, Mapping) else {}
                analysis = analysis if isinstance(analysis, Mapping) else {}
                dimensions = analysis.get("estimated_dimensions_m_object") or analysis.get("dimensions_object") or receipt.get("dimensions_object") or {}
                materials = analysis.get("materials_array") or receipt.get("materials_array") or []
                objects = receipt.get("parts_array") or analysis.get("objects_array") or []
                voxels = receipt.get("fused_voxels_object") or receipt.get("voxels_object") or {}
                embedding = (voxels.get("shape_embedding_descriptor_object") or voxels.get("embedding_object")
                             or receipt.get("embedding_object") or {})
                existing = db.execute("SELECT * FROM vehicle_features WHERE source_sha256=? AND pipeline_version=?",
                                      (value["source_sha256"], PIPELINE_VERSION)).fetchone()
                def previous(column: str, fallback):
                    if existing is None: return fallback
                    try: return json.loads(existing[column])
                    except (TypeError, json.JSONDecodeError): return fallback
                dimensions = dimensions or previous("dimensions_json", {})
                materials = materials or previous("materials_json", [])
                objects = objects or previous("objects_json", [])
                embedding = embedding or previous("voxel_embedding_json", {})
                category = str(analysis.get("category_string") or (existing["category"] if existing else "") or "vehicle")
                title = str(analysis.get("title_string") or analysis.get("name_string") or (existing["title"] if existing else "") or "vehicle")
                db.execute("INSERT INTO vehicle_features VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(source_sha256,pipeline_version) DO UPDATE SET category=excluded.category,title=excluded.title,dimensions_json=excluded.dimensions_json,materials_json=excluded.materials_json,objects_json=excluded.objects_json,voxel_embedding_json=excluded.voxel_embedding_json,updated_at=excluded.updated_at",
                           (value["source_sha256"], PIPELINE_VERSION, category, title,
                            json.dumps(dimensions, ensure_ascii=False), json.dumps(materials, ensure_ascii=False),
                            json.dumps(objects, ensure_ascii=False), json.dumps(embedding, ensure_ascii=False), now))
                if job["action"] in {"voxelize", "fuse_voxels"}:
                    geometry_sha = str(receipt.get("geometry_options_sha256_string") or
                                       receipt.get("options_sha256_string") or "")
                    voxel_relative = str(voxels.get("glb_path_string") or "voxels.glb")
                    voxel_sha = next((str(item.get("sha256_string") or item.get("sha256") or "")
                                      for item in receipt.get("artifacts_array") or []
                                      if str(item.get("path_string") or item.get("relative_path_string") or "") == voxel_relative), "")
                    if len(geometry_sha) != 64 or len(voxel_sha) != 64:
                        if job["action"] == "fuse_voxels" and receipt.get("status_string") == "needs_review" and not voxel_sha:
                            geometry_sha = voxel_sha = ""
                        else:
                            db.execute("ROLLBACK"); raise ValueError("voxel feature identity is incomplete")
                    if not voxel_sha:
                        continue_feature = False
                    else:
                        continue_feature = True
                    if continue_feature:
                        chain = value.get("source_sha_chain") or {}
                        feature = {"category": category, "title": title, "dimensions": dimensions,
                                   "materials": materials, "objects": objects, "voxel_embedding": embedding}
                        identity = feature_identity(job["run_id"], value["source_sha256"], chain.get("prepared_sha256"),
                                                    geometry_sha, voxel_sha)
                        run_owner = db.execute("SELECT owner_key FROM vehicle_runs WHERE id=?", (job["run_id"],)).fetchone()[0]
                        db.execute("INSERT INTO vehicle_feature_versions VALUES(?,?,?,?,?,?,?,?,?)",
                                   (identity, job["run_id"], run_owner, value["source_sha256"], chain.get("prepared_sha256"),
                                    geometry_sha, voxel_sha, json.dumps(feature, ensure_ascii=False), now))
            db.execute("COMMIT")

    def progress(self, job: sqlite3.Row, stage: str, percent: float, detail: Mapping[str, Any] | None = None) -> None:
        percent = max(0, min(99, int(percent)))
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT document_json FROM vehicle_runs WHERE id=? AND active_job_id=?", (job["run_id"], job["id"])).fetchone()
            if row is None: db.execute("ROLLBACK"); raise RevisionConflict("vehicle stage is no longer active")
            value = json.loads(row[0]); value.update(status="running", stage=str(job["action"]), progress=percent)
            if value.get("stages"):
                value["stages"][-1].update(status="running", phase=stage, progress=percent)
                if detail: value["stages"][-1]["detail"] = dict(detail)
            db.execute("UPDATE vehicle_runs SET status='running',stage=?,document_json=?,updated_at=? WHERE id=? AND active_job_id=?",
                       (job["action"], json.dumps(value, ensure_ascii=False, separators=(",", ":")), time.time(), job["run_id"], job["id"]))
            db.execute("UPDATE vehicle_jobs SET lease_until=?,updated_at=? WHERE id=? AND lease_token=?",
                       (time.time() + 300, time.time(), job["id"], job["lease_token"]))
            db.execute("COMMIT")

    def renew(self, job: sqlite3.Row, lease_seconds: float = 300) -> None:
        with self.connect() as db:
            changed = db.execute("UPDATE vehicle_jobs SET lease_until=?,updated_at=? WHERE id=? AND lease_token=? AND status='running'",
                                 (time.time() + lease_seconds, time.time(), job["id"], job["lease_token"])).rowcount
        if changed != 1: raise RevisionConflict("vehicle stage lease was lost")

    def checkpoint_job(self, job: sqlite3.Row, patch: Mapping[str, Any]) -> dict[str, Any]:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT request_json FROM vehicle_jobs WHERE id=? AND lease_token=? AND status='running'",
                             (job["id"], job["lease_token"])).fetchone()
            if row is None: db.execute("ROLLBACK"); raise RevisionConflict("vehicle stage lease was lost")
            request = json.loads(row[0]); request.update(dict(patch))
            db.execute("UPDATE vehicle_jobs SET request_json=?,lease_until=?,updated_at=? WHERE id=? AND lease_token=?",
                       (json.dumps(request, sort_keys=True, separators=(",", ":")), time.time() + 300, time.time(),
                        job["id"], job["lease_token"]))
            db.execute("COMMIT")
        return request

    def fail(self, job: sqlite3.Row, message: str) -> None:
        now = time.time()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT document_json,revision FROM vehicle_runs WHERE id=?", (job["run_id"],)).fetchone()
            value = json.loads(row["document_json"]); value.update(status="failed", stage=job["action"], progress=0,
                error=message[:1000], failed_stage=job["action"], revision=int(row["revision"]) + 1)
            if value.get("stages"): value["stages"][-1].update(status="failed", error=message[:1000])
            changed = db.execute("UPDATE vehicle_jobs SET status='failed',error=?,lease_token=NULL,lease_until=NULL,updated_at=? WHERE id=? AND lease_token=? AND status='running'",
                                 (message[:1000], now, job["id"], job["lease_token"])).rowcount
            if changed != 1:
                db.execute("ROLLBACK")
                raise RevisionConflict("vehicle stage lease was lost")
            changed = db.execute("UPDATE vehicle_runs SET revision=?,status='failed',active_job_id=NULL,document_json=?,updated_at=? WHERE id=? AND active_job_id=?",
                                 (value["revision"], json.dumps(value, ensure_ascii=False, separators=(",", ":")), now, job["run_id"], job["id"])).rowcount
            if changed != 1:
                db.execute("ROLLBACK")
                raise RevisionConflict("vehicle run is owned by another stage")
            db.execute("COMMIT")
