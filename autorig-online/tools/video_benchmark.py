#!/usr/bin/env python3
"""Bounded, resumable video-to-video benchmark harness for AutoRig.

The harness deliberately separates rendering from review.  It records what was
submitted and produces comparison media, but a human must fill the review
fields before a pipeline can be called a candidate, accepted, or rejected.

Manifest v1 (``sources_array`` may also be named ``sources``)::

  {
    "schema_version_int": 1,
    "sources_array": [{
      "id_string": "meeting",
      "url_string": "https://.../meeting.mp4",
      "sha256_string": "optional",
      "segment_start_seconds_float": 0,
      "segment_duration_seconds_float": 4,
      "prompt_string": "Two colleagues greet each other..."
    }],
    "pipelines_array": [{
      "id_string": "ltx23-base",
      "enabled_bool": true,
      "request_object": {"frame_count": 97, "width": 960, "height": 540,
                         "seed": 9221001, "steps": 8}
    }]
  }

An optional ``cases_array`` can name explicit source/pipeline pairs and override
``prompt_string`` or ``request_object``.  Without it, enabled pipelines are run
against every source.  Source media are never modified or removed.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional


SCHEMA_VERSION = 1
TERMINAL_STATES = {"completed", "failed", "cancelled"}
ACTIVE_STATES = {"accepted", "queued", "pending", "rendering"}
REVIEW_FIELDS = (
    "faces_score_int",
    "anatomy_score_int",
    "identity_score_int",
    "action_score_int",
    "people_count_score_int",
    "camera_score_int",
    "temporal_score_int",
)


class BenchmarkError(RuntimeError):
    pass


def _json_object(path: Path) -> Dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BenchmarkError(f"Cannot read valid JSON from {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise BenchmarkError(f"Expected a JSON object in {path}")
    return value


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_name, path)
    finally:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass


def _slug(value: object) -> str:
    text = str(value or "").strip()
    cleaned = "".join(ch if ch.isalnum() or ch in "-_" else "-" for ch in text)
    cleaned = cleaned.strip("-")
    if not cleaned or len(cleaned) > 80:
        raise BenchmarkError(f"Invalid or overlong id: {text!r}")
    return cleaned


def _api_endpoint(pipeline: Mapping[str, Any]) -> str:
    endpoint = str(pipeline.get("endpoint_string") or "/api/video").strip()
    if not endpoint.startswith("/api/") or "://" in endpoint:
        raise BenchmarkError(f"Unsafe API endpoint: {endpoint!r}")
    return endpoint


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _run(command: List[str]) -> None:
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode:
        raise BenchmarkError(f"Command failed ({result.returncode}): {' '.join(command)}\n{result.stderr[-3000:]}")


def _ffprobe(ffprobe: str, path: Path) -> Dict[str, Any]:
    result = subprocess.run(
        [ffprobe, "-v", "error", "-show_format", "-show_streams", "-of", "json", str(path)],
        capture_output=True, text=True, check=False,
    )
    if result.returncode:
        raise BenchmarkError(f"ffprobe failed for {path}: {result.stderr[-3000:]}")
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise BenchmarkError(f"ffprobe returned invalid JSON for {path}: {exc}") from exc
    if not isinstance(value, dict) or not isinstance(value.get("streams"), list):
        raise BenchmarkError(f"ffprobe returned an incomplete object for {path}")
    if not any(row.get("codec_type") == "video" for row in value["streams"] if isinstance(row, dict)):
        raise BenchmarkError(f"No video stream in {path}")
    return value


def _download(url: str, destination: Path, timeout: float, max_bytes: int,
              transport: str = "urllib") -> None:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise BenchmarkError(f"Only declared HTTP(S) source URLs are accepted: {url!r}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_suffix(destination.suffix + ".part")
    if transport == "curl":
        command = [
            "curl.exe" if os.name == "nt" else "curl",
            "--silent", "--show-error", "--location",
            "--max-time", str(timeout), "--max-filesize", str(max_bytes),
            "--user-agent", "AutoRigVideoBenchmark/1",
            "--output", str(partial), "--write-out", "%{http_code}", url,
        ]
        try:
            result = subprocess.run(command, capture_output=True, text=True, check=False)
        except OSError as exc:
            raise BenchmarkError(f"Source download failed for {url}: {exc}") from exc
        try:
            status = int(result.stdout.strip())
        except ValueError as exc:
            partial.unlink(missing_ok=True)
            raise BenchmarkError(f"Source download returned no HTTP status for {url}") from exc
        if result.returncode or not 200 <= status < 300:
            partial.unlink(missing_ok=True)
            raise BenchmarkError(
                f"Source download failed for {url}: curl {result.returncode}, HTTP {status}: "
                f"{result.stderr[-1000:]}"
            )
        if partial.stat().st_size > max_bytes:
            partial.unlink(missing_ok=True)
            raise BenchmarkError(f"Download exceeds {max_bytes} bytes: {url}")
        os.replace(partial, destination)
        return
    if transport != "urllib":
        raise BenchmarkError(f"Unsupported HTTP transport: {transport!r}")
    request = urllib.request.Request(url, headers={"User-Agent": "AutoRigVideoBenchmark/1"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response, partial.open("wb") as output:
            declared = response.headers.get("Content-Length")
            if declared and int(declared) > max_bytes:
                raise BenchmarkError(f"Download exceeds {max_bytes} bytes: {url}")
            written = 0
            while True:
                chunk = response.read(min(1024 * 1024, max_bytes + 1 - written))
                if not chunk:
                    break
                written += len(chunk)
                if written > max_bytes:
                    raise BenchmarkError(f"Download exceeds {max_bytes} bytes: {url}")
                output.write(chunk)
    except (OSError, urllib.error.URLError) as exc:
        raise BenchmarkError(f"Source download failed for {url}: {exc}") from exc
    os.replace(partial, destination)


def _request_json(method: str, url: str, payload: Optional[Mapping[str, Any]], timeout: float,
                  token: str = "", transport: str = "urllib") -> tuple[int, Dict[str, Any]]:
    headers = {"Accept": "application/json", "User-Agent": "AutoRigVideoBenchmark/1"}
    data = None
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = "Bearer " + token
    if transport == "curl":
        command = [
            "curl.exe" if os.name == "nt" else "curl",
            "--silent", "--show-error", "--max-time", str(timeout),
            "--request", method, "--header", "Accept: application/json",
            "--header", "User-Agent: AutoRigVideoBenchmark/1",
        ]
        if payload is not None:
            command += ["--header", "Content-Type: application/json", "--data-binary", "@-"]
        header_path: Optional[Path] = None
        if token:
            temp_root = Path(__file__).resolve().parents[2] / ".codex_tmp" / "video-benchmark-http"
            temp_root.mkdir(parents=True, exist_ok=True)
            fd, name = tempfile.mkstemp(prefix="headers-", suffix=".txt", dir=temp_root)
            header_path = Path(name)
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
                stream.write("Authorization: Bearer " + token + "\n")
            command += ["--header", "@" + str(header_path)]
        command += ["--write-out", "\n%{http_code}", url]
        try:
            result = subprocess.run(command, input=data, capture_output=True, check=False)
        except OSError as exc:
            raise BenchmarkError(f"HTTP request failed for {url}: {exc}") from exc
        finally:
            if header_path is not None:
                header_path.unlink(missing_ok=True)
        if result.returncode:
            detail = result.stderr.decode("utf-8", errors="replace")[-1000:]
            raise BenchmarkError(f"HTTP request failed for {url}: curl {result.returncode}: {detail}")
        try:
            raw, status_raw = result.stdout.rsplit(b"\n", 1)
            status = int(status_raw)
        except (ValueError, TypeError) as exc:
            raise BenchmarkError(f"HTTP request returned no status suffix from {url}") from exc
    elif transport == "urllib":
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read()
                status = int(response.status)
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            status = int(exc.code)
    else:
        raise BenchmarkError(f"Unsupported HTTP transport: {transport!r}")
    try:
        value = json.loads(raw.decode("utf-8")) if raw else {}
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BenchmarkError(f"HTTP {status} returned invalid JSON from {url}: {exc}") from exc
    if not isinstance(value, dict):
        raise BenchmarkError(f"HTTP {status} returned non-object JSON from {url}")
    return status, value


def _array(manifest: Mapping[str, Any], preferred: str, fallback: str) -> List[Dict[str, Any]]:
    value = manifest.get(preferred, manifest.get(fallback, []))
    if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
        raise BenchmarkError(f"{preferred} must be an array of objects")
    return list(value)


def _manifest_cases(manifest: Mapping[str, Any]) -> List[Dict[str, Any]]:
    sources = {_slug(row.get("id_string", row.get("id"))): row
               for row in _array(manifest, "sources_array", "sources")}
    pipelines = {_slug(row.get("id_string", row.get("id"))): row
                 for row in _array(manifest, "pipelines_array", "pipelines")}
    if not sources or not pipelines:
        raise BenchmarkError("Manifest needs at least one source and one pipeline")
    explicit = _array(manifest, "cases_array", "cases")
    pairs: Iterable[Mapping[str, Any]]
    if explicit:
        pairs = explicit
    else:
        pairs = ({"source_id_string": source_id, "pipeline_id_string": pipeline_id}
                 for source_id in sources for pipeline_id, pipeline in pipelines.items()
                 if pipeline.get("enabled_bool", True))
    result: List[Dict[str, Any]] = []
    seen = set()
    for pair in pairs:
        source_id = _slug(pair.get("source_id_string", pair.get("source_id")))
        pipeline_id = _slug(pair.get("pipeline_id_string", pair.get("pipeline_id")))
        if source_id not in sources or pipeline_id not in pipelines:
            raise BenchmarkError(f"Unknown source/pipeline pair: {source_id}/{pipeline_id}")
        case_id = _slug(pair.get("id_string", pair.get("id", f"{source_id}--{pipeline_id}")))
        if case_id in seen:
            raise BenchmarkError(f"Duplicate case id: {case_id}")
        seen.add(case_id)
        source = sources[source_id]
        pipeline = pipelines[pipeline_id]
        request = dict(pipeline.get("request_object") or {})
        request.update(pair.get("request_object") or {})
        prompt = pair.get("prompt_string", pipeline.get("prompt_string", source.get("prompt_string", "")))
        if prompt:
            request["prompt"] = str(prompt)
        seed = request.get("seed")
        if not isinstance(seed, int) or seed <= 0:
            raise BenchmarkError(f"Case {case_id} requires a fixed positive integer seed")
        result.append({"id": case_id, "source_id": source_id, "pipeline_id": pipeline_id,
                       "source": source, "pipeline": pipeline, "request": request})
    return result


class Benchmark:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.manifest_path = args.manifest.resolve()
        self.output = args.output_dir.resolve()
        repository = Path(__file__).resolve().parents[2]
        try:
            self.output.relative_to(repository)
        except ValueError as exc:
            raise BenchmarkError(f"Output directory must stay inside repository {repository}") from exc
        self.manifest = _json_object(self.manifest_path)
        if int(self.manifest.get("schema_version_int", 1)) != SCHEMA_VERSION:
            raise BenchmarkError(f"Unsupported manifest schema: {self.manifest.get('schema_version_int')}")
        all_cases = _manifest_cases(self.manifest)
        requested_ids = list(dict.fromkeys(args.case_id or []))
        known_ids = {str(case["id"]) for case in all_cases}
        unknown_ids = set(requested_ids) - known_ids
        if unknown_ids:
            raise BenchmarkError(f"Unknown case id: {sorted(unknown_ids)[0]}")
        self.cases = [
            case for case in all_cases
            if not requested_ids or case["id"] in requested_ids
        ]
        if not self.cases:
            raise BenchmarkError("Case selection is empty")
        if args.prompt_overrides:
            overrides = _json_object(args.prompt_overrides.resolve())
            unselected = set(overrides) - {str(case["id"]) for case in self.cases}
            if unselected:
                raise BenchmarkError(
                    f"Prompt override does not name a selected case: {sorted(unselected)[0]}"
                )
            for case in self.cases:
                if case["id"] not in overrides:
                    continue
                prompt = overrides[case["id"]]
                if not isinstance(prompt, str) or not prompt.strip() or len(prompt) > 12000:
                    raise BenchmarkError(f"Invalid prompt override for {case['id']}")
                case["request"]["prompt"] = prompt.strip()
        self.state_path = self.output / "state.json"
        self.state = self._load_state()

    def _load_state(self) -> Dict[str, Any]:
        if self.state_path.exists():
            state = _json_object(self.state_path)
            if state.get("manifest_sha256_string") != _sha256(self.manifest_path):
                raise BenchmarkError("Manifest changed since this benchmark state was created; use a new output directory")
            return state
        return {"schema_version_int": SCHEMA_VERSION,
                "manifest_path_string": str(self.manifest_path),
                "manifest_sha256_string": _sha256(self.manifest_path),
                "created_at_unix_float": time.time(), "cases_object": {}}

    def save(self) -> None:
        self.state["updated_at_unix_float"] = time.time()
        _atomic_json(self.state_path, self.state)

    def _source_paths(self, case: Mapping[str, Any]) -> Dict[str, Path]:
        source_id = str(case["source_id"])
        return {"original": self.output / "sources" / source_id / "original.mp4",
                "trimmed": self.output / "sources" / source_id / "segment.mp4",
                "first": self.output / "sources" / source_id / "first.png",
                "end": self.output / "sources" / source_id / "end.png",
                "probe": self.output / "sources" / source_id / "ffprobe.json"}

    def prepare(self) -> None:
        unique = {case["source_id"]: case for case in self.cases}
        for case in unique.values():
            source = case["source"]
            paths = self._source_paths(case)
            url = str(source.get("url_string", source.get("url", ""))).strip()
            if not paths["original"].exists():
                _download(url, paths["original"], self.args.http_timeout, self.args.max_source_bytes,
                          getattr(self.args, "http_transport", "urllib"))
            expected = str(source.get("sha256_string") or "").lower().strip()
            actual = _sha256(paths["original"])
            if expected and expected != actual:
                raise BenchmarkError(f"SHA-256 mismatch for {case['source_id']}: {actual}")
            probe = _ffprobe(self.args.ffprobe, paths["original"])
            _atomic_json(paths["probe"], probe)
            start = float(source.get("segment_start_seconds_float", 0))
            duration = float(source.get("segment_duration_seconds_float", 4))
            if start < 0 or not (0.25 <= duration <= self.args.max_segment_seconds):
                raise BenchmarkError(f"Unsafe segment bounds for {case['source_id']}: {start}, {duration}")
            paths["trimmed"].parent.mkdir(parents=True, exist_ok=True)
            _run([self.args.ffmpeg, "-y", "-ss", str(start), "-i", str(paths["original"]),
                  "-t", str(duration), "-map", "0:v:0", "-an", "-c:v", "libx264",
                  "-pix_fmt", "yuv420p", str(paths["trimmed"])])
            _run([self.args.ffmpeg, "-y", "-i", str(paths["trimmed"]), "-frames:v", "1", str(paths["first"])])
            _run([self.args.ffmpeg, "-y", "-sseof", "-0.05", "-i", str(paths["trimmed"]),
                  "-frames:v", "1", str(paths["end"])])
        self.save()

    def _case_state(self, case: Mapping[str, Any]) -> MutableMapping[str, Any]:
        states = self.state.setdefault("cases_object", {})
        return states.setdefault(case["id"], {
            "source_id_string": case["source_id"], "pipeline_id_string": case["pipeline_id"],
            "status_string": "prepared", "attempt_count_int": 0,
        })

    def _submit(self, case: Mapping[str, Any]) -> None:
        row = self._case_state(case)
        paths = self._source_paths(case)
        request = dict(case["request"])
        request["image_base64"] = base64.b64encode(paths["first"].read_bytes()).decode("ascii")
        if bool(case["pipeline"].get("use_end_frame_bool", False)):
            request["image_base64_end"] = base64.b64encode(paths["end"].read_bytes()).decode("ascii")
        endpoint = _api_endpoint(case["pipeline"])
        row.update({"status_string": "submitting", "submitted_request_object": {
            key: value for key, value in request.items() if key not in {"image_base64", "image_base64_end"}},
            "submit_endpoint_string": endpoint,
            "attempt_count_int": int(row.get("attempt_count_int", 0)) + 1,
            "submit_started_at_unix_float": time.time()})
        self.save()
        try:
            status, response = _request_json("POST", self.args.base_url.rstrip("/") + endpoint,
                                             request, self.args.http_timeout, self.args.token,
                                             getattr(self.args, "http_transport", "urllib"))
        except (BenchmarkError, TimeoutError, OSError) as exc:
            # Submission may have reached the server.  Never spend GPU twice by
            # guessing; reconciliation or an explicit state edit is required.
            row.update({"status_string": "submit_uncertain", "last_error_string": str(exc)})
            self.save()
            return
        task_id = str(response.get("task_id_string") or "").strip()
        if status not in (200, 202) or not task_id:
            row.update({"status_string": "submit_rejected", "http_status_int": status,
                        "response_object": response})
            self.save()
            return
        row.update({"status_string": str(response.get("status_string") or "accepted").lower(),
                    "task_id_string": task_id, "accepted_response_object": response,
                    "accepted_at_unix_float": time.time()})
        self.save()  # Durable before the first status request.

    def poll_once(self) -> None:
        for case in self.cases:
            row = self._case_state(case)
            task_id = str(row.get("task_id_string") or "")
            if not task_id or str(row.get("status_string")) in TERMINAL_STATES:
                continue
            url = self.args.base_url.rstrip("/") + "/api/ai/render-status/" + urllib.parse.quote(task_id)
            try:
                status, response = _request_json(
                    "GET", url, None, self.args.http_timeout, self.args.token,
                    getattr(self.args, "http_transport", "urllib"),
                )
                if status != 200:
                    raise BenchmarkError(f"Status HTTP {status}: {response}")
                remote = str(response.get("status_string") or "").lower()
                if remote not in ACTIVE_STATES | TERMINAL_STATES:
                    raise BenchmarkError(f"Unrecognised status payload: {response}")
            except (BenchmarkError, TimeoutError, OSError) as exc:
                row["last_poll_error_string"] = str(exc)
                row["last_poll_error_at_unix_float"] = time.time()
                self.save()
                continue
            row.update({"status_string": remote, "last_status_object": response,
                        "last_polled_at_unix_float": time.time()})
            row.pop("last_poll_error_string", None)
            if remote == "completed":
                row["output_url_string"] = str(response.get("output_url_string") or
                                                row.get("accepted_response_object", {}).get("video_url_string") or "")
            self.save()

    def run(self) -> None:
        self.prepare()
        deadline = time.monotonic() + self.args.max_wait_seconds
        submissions = 0
        while True:
            self.poll_once()
            active = sum(1 for row in self.state["cases_object"].values()
                         if row.get("task_id_string") and row.get("status_string") not in TERMINAL_STATES)
            for case in self.cases:
                if submissions >= self.args.submit_limit or active >= self.args.max_inflight:
                    break
                row = self._case_state(case)
                if row.get("status_string") != "prepared" or row.get("task_id_string"):
                    continue
                self._submit(case)
                submissions += 1
                if self._case_state(case).get("task_id_string"):
                    active += 1
            unfinished = [row for row in self.state["cases_object"].values()
                          if row.get("task_id_string") and row.get("status_string") not in TERMINAL_STATES]
            if not unfinished or time.monotonic() >= deadline:
                break
            time.sleep(self.args.poll_interval_seconds)

    def artifacts(self) -> None:
        self.prepare()
        for case in self.cases:
            row = self._case_state(case)
            if row.get("status_string") != "completed":
                continue
            case_dir = self.output / "cases" / str(case["id"])
            case_dir.mkdir(parents=True, exist_ok=True)
            generated = case_dir / "generated.mp4"
            url = str(row.get("output_url_string") or "")
            if not url:
                row["artifact_error_string"] = "Completed task has no output URL"
                self.save()
                continue
            if not generated.exists():
                _download(url, generated, self.args.http_timeout, self.args.max_source_bytes,
                          getattr(self.args, "http_transport", "urllib"))
            generated_probe = _ffprobe(self.args.ffprobe, generated)
            _atomic_json(case_dir / "generated.ffprobe.json", generated_probe)
            source = self._source_paths(case)["trimmed"]
            comparison = case_dir / "source-generated-side-by-side.mp4"
            _run([self.args.ffmpeg, "-y", "-i", str(source), "-i", str(generated),
                  "-filter_complex",
                  "[0:v]scale=480:270:force_original_aspect_ratio=decrease,pad=480:270:(ow-iw)/2:(oh-ih)/2[left];"
                  "[1:v]scale=480:270:force_original_aspect_ratio=decrease,pad=480:270:(ow-iw)/2:(oh-ih)/2[right];"
                  "[left][right]hstack=inputs=2[out]", "-map", "[out]", "-an", "-shortest",
                  "-c:v", "libx264", "-pix_fmt", "yuv420p", str(comparison)])
            sheet = case_dir / "contact-sheet.jpg"
            _run([self.args.ffmpeg, "-y", "-i", str(comparison), "-vf",
                  "fps=1,scale=640:-2,tile=2x3", "-frames:v", "1", str(sheet)])
            review_path = case_dir / "manual-review.json"
            if not review_path.exists():
                review: Dict[str, Any] = {
                    "schema_version_int": SCHEMA_VERSION, "case_id_string": case["id"],
                    "pipeline_disposition_string": None,
                    "allowed_dispositions_array": ["candidate", "accepted", "rejected"],
                    "reviewer_string": None, "notes_string": "",
                    "defects_array": [], "reviewed_at_string": None,
                }
                review.update({field: None for field in REVIEW_FIELDS})
                _atomic_json(review_path, review)
            row["artifacts_object"] = {
                "generated_video_path_string": str(generated),
                "comparison_video_path_string": str(comparison),
                "contact_sheet_path_string": str(sheet),
                "manual_review_path_string": str(review_path),
            }
            self.save()

    def report(self) -> None:
        summary = {"schema_version_int": SCHEMA_VERSION,
                   "generated_at_unix_float": time.time(), "cases_array": []}
        for case in self.cases:
            row = dict(self._case_state(case))
            review_path = self.output / "cases" / str(case["id"]) / "manual-review.json"
            review = _json_object(review_path) if review_path.exists() else None
            summary["cases_array"].append({"case_id_string": case["id"],
                                            "source_id_string": case["source_id"],
                                            "pipeline_id_string": case["pipeline_id"],
                                            "execution_object": row,
                                            "manual_review_object": review})
        _atomic_json(self.output / "report.json", summary)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "run", "status", "artifacts", "report"))
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path,
                        help="Explicit repository-local evidence directory")
    parser.add_argument("--base-url", default="https://autorig.online")
    parser.add_argument("--token", default=os.environ.get("AUTORIG_API_TOKEN", ""))
    parser.add_argument("--ffmpeg", default="ffmpeg")
    parser.add_argument("--ffprobe", default="ffprobe")
    parser.add_argument("--max-inflight", type=int, default=1, choices=range(1, 5))
    parser.add_argument("--submit-limit", type=int, default=1,
                        help="Maximum new GPU jobs this invocation may submit")
    parser.add_argument("--max-wait-seconds", type=float, default=900)
    parser.add_argument("--poll-interval-seconds", type=float, default=10)
    parser.add_argument("--http-timeout", type=float, default=60)
    parser.add_argument("--http-transport", choices=("urllib", "curl"), default="urllib",
                        help="HTTP client; curl avoids Windows urllib TLS reset failures")
    parser.add_argument("--max-segment-seconds", type=float, default=8)
    parser.add_argument("--max-source-bytes", type=int, default=500 * 1024 * 1024)
    parser.add_argument("--case-id", action="append", default=[],
                        help="Run only this exact manifest case; repeat for a bounded batch")
    parser.add_argument("--prompt-overrides", type=Path,
                        help="JSON object mapping selected case ids to reviewed prompts")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.submit_limit < 0 or args.max_wait_seconds < 0 or args.poll_interval_seconds <= 0:
            raise BenchmarkError("Limits and intervals must be non-negative")
        benchmark = Benchmark(args)
        if args.command == "prepare":
            benchmark.prepare()
        elif args.command == "run":
            benchmark.run()
        elif args.command == "status":
            benchmark.poll_once()
        elif args.command == "artifacts":
            benchmark.artifacts()
        else:
            benchmark.report()
        print(json.dumps({"ok": True, "state": str(benchmark.state_path)}, ensure_ascii=False))
        return 0
    except BenchmarkError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
