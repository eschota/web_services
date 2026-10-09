"""Fail-closed client contract for the future AutoRig V3 pipeline.

The live Motion Transfer service currently exposes the interactive ``/kit``
API.  It does not yet advertise this dispatch protocol, so discovery raises
``UnsupportedServerRevision`` before a submission is attempted.  This makes
the adapter safe to wire into a shadow/migration path without accidentally
claiming that today's viewer API is a durable production worker API.
"""
from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping
from urllib.parse import urljoin, urlsplit

import httpx


PROTOCOL_SCHEMA = "autorig.v3.dispatch/1"
CAPABILITY_PATH = "/api/mt"
REQUIRED_RIG_ARTIFACTS = frozenset({"rigged_glb", "rig_json", "rig_qa"})
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class V3ProtocolError(ValueError):
    """A remote response violates the V3 dispatch contract."""


class UnsupportedServerRevision(V3ProtocolError):
    """The server has not advertised the exact protocol this client speaks."""


class V3State(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    NEEDS_REVIEW = "needs_review"
    DONE = "done"
    FAILED = "failed"


def _sha256(value: Any, field: str) -> str:
    result = str(value or "").strip().lower()
    if not _SHA256.fullmatch(result):
        raise V3ProtocolError(f"{field} must be lowercase 64-hex")
    return result


def _task_uuid(value: Any) -> str:
    try:
        result = str(uuid.UUID(str(value)))
    except (ValueError, AttributeError, TypeError) as exc:
        raise V3ProtocolError("task_id must be a canonical UUID") from exc
    if str(value) != result:
        raise V3ProtocolError("task_id must be a canonical UUID")
    return result


def _canonical_json(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def idempotency_key(task_id: str, source_sha256: str, attempt: int) -> str:
    """Return the stable key for one logical attempt, including retries."""
    task = _task_uuid(task_id)
    source = _sha256(source_sha256, "source_sha256")
    if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
        raise V3ProtocolError("attempt must be a positive integer")
    raw = f"{PROTOCOL_SCHEMA}\0{task}\0{source}\0{attempt}".encode("ascii")
    return "v3-" + hashlib.sha256(raw).hexdigest()


@dataclass(frozen=True)
class V3Submission:
    task_id: str
    source_sha256: str
    attempt: int
    idempotency_key: str

    @classmethod
    def create(cls, task_id: str, source_sha256: str, attempt: int) -> "V3Submission":
        task = _task_uuid(task_id)
        source = _sha256(source_sha256, "source_sha256")
        return cls(task, source, attempt, idempotency_key(task, source, attempt))

    def __post_init__(self) -> None:
        task = _task_uuid(self.task_id)
        source = _sha256(self.source_sha256, "source_sha256")
        expected = idempotency_key(task, source, self.attempt)
        if self.idempotency_key != expected:
            raise V3ProtocolError("idempotency_key is not bound to task/source/attempt")

    def payload(self) -> dict[str, Any]:
        return {
            "schema": PROTOCOL_SCHEMA,
            "task_id": self.task_id,
            "source_sha256": self.source_sha256,
            "attempt": self.attempt,
            "idempotency_key": self.idempotency_key,
        }


@dataclass(frozen=True)
class V3Capabilities:
    submit_path: str
    status_template: str


@dataclass(frozen=True)
class V3Artifact:
    role: str
    path: str
    url: str
    sha256: str
    bytes: int
    source_sha256: str


@dataclass(frozen=True)
class V3Status:
    state: V3State
    task_id: str
    source_sha256: str
    attempt: int
    idempotency_key: str
    run_id: str | None = None
    retry_after_seconds: float | None = None
    artifacts: tuple[V3Artifact, ...] = ()
    manifest_sha256: str | None = None
    qa_status: str | None = None
    reason: str | None = None


def _relative_endpoint(value: Any, field: str, *, template: bool = False) -> str:
    path = str(value or "").strip()
    if not path.startswith("/") or "\\" in path or "?" in path or "#" in path:
        raise UnsupportedServerRevision(f"{field} must be a relative absolute path")
    if "//" in path or any(part in {".", ".."} for part in path.split("/")):
        raise UnsupportedServerRevision(f"{field} is not normalized")
    marker_count = path.count("{run_id}")
    if template and marker_count != 1:
        raise UnsupportedServerRevision(f"{field} must contain one {{run_id}} marker")
    if not template and "{" in path:
        raise UnsupportedServerRevision(f"{field} must not be a template")
    return path


def parse_capabilities(response: httpx.Response) -> V3Capabilities:
    content_type = response.headers.get("content-type", "").split(";", 1)[0].lower()
    if response.status_code != 200 or content_type != "application/json":
        raise UnsupportedServerRevision(
            "Motion Transfer does not advertise autorig.v3.dispatch/1"
        )
    try:
        payload = response.json()
    except (ValueError, json.JSONDecodeError) as exc:
        raise UnsupportedServerRevision("V3 capability document is not JSON") from exc
    if not isinstance(payload, dict) or payload.get("schema") != PROTOCOL_SCHEMA:
        revision = payload.get("schema") if isinstance(payload, dict) else None
        raise UnsupportedServerRevision(f"unsupported V3 server revision: {revision!r}")
    endpoints = payload.get("endpoints")
    if not isinstance(endpoints, dict):
        raise UnsupportedServerRevision("V3 capability endpoints are missing")
    return V3Capabilities(
        submit_path=_relative_endpoint(endpoints.get("submit"), "submit endpoint"),
        status_template=_relative_endpoint(
            endpoints.get("status"), "status endpoint", template=True
        ),
    )


def _validate_manifest(
    manifest: Any,
    manifest_sha256: Any,
    submission: V3Submission,
    required_roles: frozenset[str],
) -> tuple[tuple[V3Artifact, ...], str]:
    if not isinstance(manifest, dict):
        raise V3ProtocolError("done response requires an artifact_manifest object")
    supplied_digest = _sha256(manifest_sha256, "artifact_manifest_sha256")
    actual_digest = hashlib.sha256(_canonical_json(manifest)).hexdigest()
    if supplied_digest != actual_digest:
        raise V3ProtocolError("artifact manifest hash mismatch")
    if manifest.get("schema") != "autorig.v3.artifacts/1":
        raise V3ProtocolError("unsupported artifact manifest schema")
    if _task_uuid(manifest.get("task_id")) != submission.task_id:
        raise V3ProtocolError("artifact manifest task_id mismatch")
    if _sha256(manifest.get("source_sha256"), "manifest source_sha256") != submission.source_sha256:
        raise V3ProtocolError("artifact manifest source_sha256 mismatch")
    rows = manifest.get("artifacts")
    if not isinstance(rows, list):
        raise V3ProtocolError("artifact manifest artifacts must be a list")
    artifacts: list[V3Artifact] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise V3ProtocolError("artifact manifest entry must be an object")
        role = str(row.get("role") or "").strip()
        if not role or role in seen:
            raise V3ProtocolError("artifact roles must be non-empty and unique")
        seen.add(role)
        path = str(row.get("path") or "").strip()
        url = str(row.get("url") or "").strip()
        if not path or path.startswith(("/", "\\")) or ".." in path.replace("\\", "/").split("/"):
            raise V3ProtocolError(f"artifact {role} has an unsafe path")
        expected_suffix = {"rigged_glb": ".glb", "rig_json": ".json", "rig_qa": ".json"}.get(role)
        if expected_suffix and not path.lower().endswith(expected_suffix):
            raise V3ProtocolError(f"artifact {role} must end with {expected_suffix}")
        parsed_url = urlsplit(url)
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.hostname:
            raise V3ProtocolError(f"artifact {role} has no absolute HTTP URL")
        size = row.get("bytes")
        if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
            raise V3ProtocolError(f"artifact {role} bytes must be positive")
        source = _sha256(row.get("source_sha256"), f"artifact {role} source_sha256")
        if source != submission.source_sha256:
            raise V3ProtocolError(f"artifact {role} source_sha256 mismatch")
        artifacts.append(
            V3Artifact(role, path, url, _sha256(row.get("sha256"), f"artifact {role} sha256"), size, source)
        )
    missing = required_roles - seen
    if missing:
        raise V3ProtocolError(f"required rig artifacts missing: {', '.join(sorted(missing))}")
    return tuple(artifacts), supplied_digest


def parse_status(
    payload: Any,
    submission: V3Submission,
    *,
    required_roles: frozenset[str] = REQUIRED_RIG_ARTIFACTS,
    artifact_bytes: Mapping[str, bytes] | None = None,
    _allow_unverified: bool = False,
) -> V3Status:
    if not isinstance(payload, dict) or payload.get("schema") != PROTOCOL_SCHEMA:
        raise UnsupportedServerRevision("status has no supported V3 protocol revision")
    if _task_uuid(payload.get("task_id")) != submission.task_id:
        raise V3ProtocolError("status task_id mismatch")
    if _sha256(payload.get("source_sha256"), "status source_sha256") != submission.source_sha256:
        raise V3ProtocolError("status source_sha256 mismatch")
    if payload.get("attempt") != submission.attempt:
        raise V3ProtocolError("status attempt mismatch")
    if payload.get("idempotency_key") != submission.idempotency_key:
        raise V3ProtocolError("status idempotency_key mismatch")
    raw_state = str(payload.get("status") or "").strip().lower()
    if raw_state == "partial":
        raw_state = V3State.NEEDS_REVIEW.value
    try:
        state = V3State(raw_state)
    except ValueError as exc:
        raise V3ProtocolError(f"unknown V3 status: {raw_state!r}") from exc
    qa = payload.get("qa")
    qa_status = str(qa.get("status") or "").strip().lower() if isinstance(qa, dict) else None
    artifacts: tuple[V3Artifact, ...] = ()
    digest: str | None = None
    if state is V3State.DONE:
        if qa_status != "accepted":
            raise V3ProtocolError("done requires qa.status=accepted")
        artifacts, digest = _validate_manifest(
            payload.get("artifact_manifest"),
            payload.get("artifact_manifest_sha256"),
            submission,
            required_roles,
        )
        if artifact_bytes is None and not _allow_unverified:
            raise V3ProtocolError("done requires byte verification of rig artifacts")
        if artifact_bytes is not None:
            for artifact in artifacts:
                data = artifact_bytes.get(artifact.role)
                if not isinstance(data, bytes):
                    raise V3ProtocolError(f"artifact {artifact.role} bytes were not fetched")
                if len(data) != artifact.bytes:
                    raise V3ProtocolError(f"artifact {artifact.role} byte count mismatch")
                if hashlib.sha256(data).hexdigest() != artifact.sha256:
                    raise V3ProtocolError(f"artifact {artifact.role} content hash mismatch")
    return V3Status(
        state=state,
        task_id=submission.task_id,
        source_sha256=submission.source_sha256,
        attempt=submission.attempt,
        idempotency_key=submission.idempotency_key,
        run_id=str(payload.get("run_id")) if payload.get("run_id") else None,
        artifacts=artifacts,
        manifest_sha256=digest,
        qa_status=qa_status,
        reason=str(payload.get("reason")) if payload.get("reason") else None,
    )


class V3PipelineClient:
    """HTTP client whose server paths come only from revision discovery."""

    def __init__(self, base_url: str, token: str, client: httpx.AsyncClient) -> None:
        parsed = urlsplit(base_url)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
        ):
            raise V3ProtocolError("base_url must be an absolute HTTP origin")
        self.base_url = base_url.rstrip("/") + "/"
        self.token = str(token or "").strip()
        self.client = client
        if not self.token:
            raise V3ProtocolError("token is required")

    async def capabilities(self) -> V3Capabilities:
        response = await self.client.get(urljoin(self.base_url, CAPABILITY_PATH.lstrip("/")))
        return parse_capabilities(response)

    async def submit(
        self,
        submission: V3Submission,
        *,
        timeout_retries: int = 1,
    ) -> V3Status:
        caps = await self.capabilities()
        if isinstance(timeout_retries, bool) or timeout_retries < 0:
            raise V3ProtocolError("timeout_retries must be non-negative")
        url = urljoin(self.base_url, caps.submit_path.lstrip("/"))
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Idempotency-Key": submission.idempotency_key,
        }
        response: httpx.Response | None = None
        for retry in range(timeout_retries + 1):
            try:
                response = await self.client.post(url, json=submission.payload(), headers=headers)
                break
            except httpx.TimeoutException:
                if retry == timeout_retries:
                    raise
        assert response is not None
        if response.status_code == 429:
            retry_after = response.headers.get("retry-after")
            try:
                delay = max(0.0, float(retry_after)) if retry_after is not None else None
            except ValueError:
                delay = None
            return V3Status(
                V3State.QUEUED,
                submission.task_id,
                submission.source_sha256,
                submission.attempt,
                submission.idempotency_key,
                retry_after_seconds=delay,
                reason="capacity",
            )
        response.raise_for_status()
        return await self._verified_status(response.json(), submission)

    async def status(self, submission: V3Submission, run_id: str) -> V3Status:
        caps = await self.capabilities()
        if not re.fullmatch(r"[A-Za-z0-9_-]{8,128}", str(run_id or "")):
            raise V3ProtocolError("run_id has an invalid format")
        path = caps.status_template.replace("{run_id}", str(run_id))
        response = await self.client.get(
            urljoin(self.base_url, path.lstrip("/")),
            headers={"Authorization": f"Bearer {self.token}"},
        )
        response.raise_for_status()
        return await self._verified_status(response.json(), submission)

    async def _verified_status(
        self, payload: Any, submission: V3Submission
    ) -> V3Status:
        preliminary = parse_status(payload, submission, _allow_unverified=True)
        if preliminary.state is not V3State.DONE:
            return preliminary
        expected_origin = urlsplit(self.base_url)
        fetched: dict[str, bytes] = {}
        for artifact in preliminary.artifacts:
            parsed = urlsplit(artifact.url)
            if (
                parsed.scheme.lower() != expected_origin.scheme.lower()
                or parsed.hostname != expected_origin.hostname
                or parsed.port != expected_origin.port
            ):
                raise V3ProtocolError(f"artifact {artifact.role} URL is outside the V3 origin")
            response = await self.client.get(artifact.url)
            response.raise_for_status()
            fetched[artifact.role] = response.content
        return parse_status(payload, submission, artifact_bytes=fetched)
