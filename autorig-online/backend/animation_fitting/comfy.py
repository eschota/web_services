from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import mimetypes
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Mapping, Optional
from urllib.parse import quote, urlencode, urlparse

import httpx

from .specs import WorkflowBinding, WorkflowProfile


VIDEO_EXTENSIONS = (".mp4", ".webm", ".mov", ".mkv")
DEFAULT_RENDER_TIMEOUT_SECONDS = 7200.0


class ComfyContractError(RuntimeError):
    """Raised when a worker or workflow violates the pinned API contract."""


class ComfyTaskError(RuntimeError):
    """Raised when a submitted Comfy task fails or completes without video."""


def _render_timeout_seconds(value: Optional[float]) -> float:
    raw: object = value
    if raw is None:
        raw = os.getenv("AUTORIG_LTX_RENDER_TIMEOUT_SECONDS", str(DEFAULT_RENDER_TIMEOUT_SECONDS))
    try:
        timeout = float(raw)
    except (TypeError, ValueError) as exc:
        raise ComfyContractError("AUTORIG_LTX_RENDER_TIMEOUT_SECONDS must be numeric") from exc
    if timeout <= 0:
        raise ComfyContractError("AUTORIG_LTX_RENDER_TIMEOUT_SECONDS must be positive")
    return timeout


@dataclass(frozen=True)
class ComfyWorker:
    worker_id: str
    base_url: str
    workflow_name: str
    expected_workflow_fingerprint: str
    username: str = ""
    password: str = field(default="", repr=False)

    def __post_init__(self) -> None:
        worker_id = _safe_token(self.worker_id, "worker_id")
        parsed = urlparse(self.base_url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ComfyContractError("Comfy worker base_url must be an absolute HTTP(S) URL")
        if parsed.scheme == "http" and parsed.hostname not in ("127.0.0.1", "localhost", "::1"):
            raise ComfyContractError("Plain HTTP is allowed only for a loopback Comfy worker")
        if not self.workflow_name.endswith(".json") or "/" in self.workflow_name or "\\" in self.workflow_name:
            raise ComfyContractError("workflow_name must be a simple .json filename")
        fingerprint = self.expected_workflow_fingerprint.strip().lower()
        if not re.fullmatch(r"[a-f0-9]{64}", fingerprint):
            raise ComfyContractError("expected_workflow_fingerprint must be a SHA-256 hex digest")
        object.__setattr__(self, "worker_id", worker_id)
        object.__setattr__(self, "base_url", self.base_url.rstrip("/"))
        object.__setattr__(self, "expected_workflow_fingerprint", fingerprint)


@dataclass(frozen=True)
class ComfyOutputFile:
    filename: str
    subfolder: str = ""
    file_type: str = "output"


@dataclass(frozen=True)
class ComfySubmission:
    prompt_id: str
    client_id: str
    resumed_existing_bool: bool


def canonical_workflow_bytes(workflow: Mapping[str, Any]) -> bytes:
    return json.dumps(
        workflow,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def workflow_fingerprint(workflow: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_workflow_bytes(workflow)).hexdigest()


def deterministic_prompt_id(idempotency_key: str) -> str:
    if not idempotency_key:
        raise ComfyContractError("idempotency_key is required")
    raw = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()[:32]
    return f"{raw[:8]}-{raw[8:12]}-4{raw[13:16]}-8{raw[17:20]}-{raw[20:32]}"


def apply_workflow_bindings(
    api_prompt: Mapping[str, Any],
    workflow: WorkflowProfile,
    *,
    uploaded_start_image: str,
    positive_prompt: str,
    negative_prompt: str,
    frame_count: int,
    seed: int,
    output_prefix: str,
) -> Dict[str, Any]:
    if not isinstance(api_prompt, Mapping) or not api_prompt:
        raise ComfyContractError("Comfy API prompt must be a non-empty object")
    if (int(frame_count) - 1) % 8 != 0:
        raise ComfyContractError("frame_count must satisfy 8n+1")
    if not uploaded_start_image or not positive_prompt or not negative_prompt or not output_prefix:
        raise ComfyContractError("Workflow binding values must be non-empty")
    result = copy.deepcopy(dict(api_prompt))
    nodes_by_title: Dict[str, tuple[str, Dict[str, Any]]] = {}
    for node_id, raw_node in result.items():
        if not isinstance(raw_node, dict):
            raise ComfyContractError(f"Comfy API prompt node {node_id} must be an object")
        inputs = raw_node.get("inputs")
        if not isinstance(inputs, dict) or not isinstance(raw_node.get("class_type"), str):
            raise ComfyContractError(f"Comfy API prompt node {node_id} lacks class_type or inputs")
        meta = raw_node.get("_meta")
        title = str(meta.get("title") or "").strip() if isinstance(meta, dict) else ""
        if title:
            if title in nodes_by_title:
                raise ComfyContractError(f"Workflow node title must be unique: {title}")
            nodes_by_title[title] = (str(node_id), raw_node)

    values: Dict[str, object] = {
        "start_image": uploaded_start_image,
        "positive_prompt": positive_prompt,
        "negative_prompt": negative_prompt,
        "frame_count": int(frame_count),
        "fps": int(workflow.input_fps),
        "output_fps": int(workflow.output_fps),
        "seed": int(seed),
        "output": output_prefix,
    }
    if workflow.generation_mode == "loop":
        values["end_image"] = uploaded_start_image
        loop_guides = [
            node
            for node in result.values()
            if node.get("class_type") == "LTXVAddGuide"
            and node.get("inputs", {}).get("frame_idx") == -1
        ]
        if len(loop_guides) != 1:
            raise ComfyContractError("Loop workflow must contain one N-1 LTXVAddGuide")
    else:
        end_guides = [
            node
            for node in result.values()
            if node.get("class_type") == "LTXVAddGuide"
            and int(node.get("inputs", {}).get("frame_idx", 0)) < 0
        ]
        if "AUTORIG_END_FRAME" in nodes_by_title or end_guides:
            raise ComfyContractError("One-shot workflow must not include end-frame conditioning")

    if workflow.post_sampling_guide_crop_required:
        crop_nodes = [
            node for node in result.values() if node.get("class_type") == "LTXVCropGuides"
        ]
        if len(crop_nodes) != 1:
            raise ComfyContractError(
                "Workflow must contain one post-sampling LTXVCropGuides node"
            )

    for role, value in values.items():
        binding = workflow.bindings.get(role)
        if not isinstance(binding, WorkflowBinding):
            raise ComfyContractError(f"Workflow binding is missing: {role}")
        for target in binding.targets:
            found = nodes_by_title.get(target.node_title)
            if not found:
                raise ComfyContractError(f"Workflow node title is missing: {target.node_title}")
            _, node = found
            if target.input_name not in node["inputs"]:
                raise ComfyContractError(
                    f"Workflow node input is missing: {target.node_title}.{target.input_name}"
                )
            node["inputs"][target.input_name] = value
    return result


class ComfyAnimationClient:
    def __init__(
        self,
        worker: ComfyWorker,
        *,
        client: Optional[httpx.AsyncClient] = None,
        request_timeout_seconds: float = 30.0,
        render_timeout_seconds: Optional[float] = None,
        poll_interval_seconds: float = 3.0,
    ) -> None:
        self.worker = worker
        self.request_timeout_seconds = float(request_timeout_seconds)
        self.render_timeout_seconds = _render_timeout_seconds(render_timeout_seconds)
        self.poll_interval_seconds = float(poll_interval_seconds)
        self._owns_client = client is None
        auth = None
        if worker.username and worker.password:
            auth = httpx.BasicAuth(worker.username, worker.password)
        self._client = client or httpx.AsyncClient(
            auth=auth,
            timeout=httpx.Timeout(self.request_timeout_seconds),
            follow_redirects=True,
        )

    async def __aenter__(self) -> "ComfyAnimationClient":
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.close()

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    def _url(self, path: str) -> str:
        return f"{self.worker.base_url}{path}"

    async def _get_json(self, path: str) -> Dict[str, Any]:
        response = await self._client.get(self._url(path), timeout=self.request_timeout_seconds)
        response.raise_for_status()
        try:
            parsed = json.loads(response.text.lstrip("\ufeff"))
        except json.JSONDecodeError as exc:
            raise ComfyContractError(f"Expected JSON from Comfy {path}") from exc
        if not isinstance(parsed, dict):
            raise ComfyContractError(f"Expected a JSON object from Comfy {path}")
        return parsed

    async def fetch_api_workflow(self) -> tuple[Dict[str, Any], str]:
        encoded = quote(f"workflows/{self.worker.workflow_name}", safe="")
        workflow = await self._get_json(f"/api/userdata/{encoded}")
        if isinstance(workflow.get("nodes"), list):
            raise ComfyContractError(
                f"{self.worker.workflow_name} is a UI workflow; an API-prompt workflow is required"
            )
        fingerprint = workflow_fingerprint(workflow)
        if fingerprint != self.worker.expected_workflow_fingerprint:
            raise ComfyContractError(
                f"Workflow fingerprint mismatch for {self.worker.worker_id}: "
                f"expected {self.worker.expected_workflow_fingerprint}, got {fingerprint}"
            )
        return workflow, fingerprint

    async def health(self) -> Dict[str, Any]:
        system, queue = await asyncio.gather(
            self._get_json("/system_stats"),
            self._get_json("/queue"),
        )
        _, fingerprint = await self.fetch_api_workflow()
        return {
            "ok_bool": True,
            "worker_id_string": self.worker.worker_id,
            "base_url_string": self.worker.base_url,
            "workflow_name_string": self.worker.workflow_name,
            "workflow_fingerprint_string": fingerprint,
            "queue_load_int": _queue_load(queue),
            "system_object": system,
        }

    async def upload_reference_image(self, image_path: Path) -> str:
        path = Path(image_path).resolve()
        data = await asyncio.to_thread(path.read_bytes)
        if not data:
            raise ComfyContractError(f"Reference image is empty: {path}")
        digest = hashlib.sha256(data).hexdigest()
        suffix = path.suffix.lower() if path.suffix.lower() in (".png", ".jpg", ".jpeg", ".webp") else ".png"
        filename = f"autorig_{digest[:32]}{suffix}"
        content_type = mimetypes.types_map.get(suffix, "application/octet-stream")
        response = await self._client.post(
            self._url("/upload/image"),
            data={"type": "input", "subfolder": "autorig_animation_fitting", "overwrite": "true"},
            files={"image": (filename, data, content_type)},
            timeout=max(self.request_timeout_seconds, 120.0),
        )
        response.raise_for_status()
        parsed = response.json()
        if not isinstance(parsed, dict):
            raise ComfyContractError("Comfy upload_image returned a non-object response")
        returned_name = str(parsed.get("name") or filename).strip()
        subfolder = str(parsed.get("subfolder") or "autorig_animation_fitting").strip().replace("\\", "/")
        if not returned_name:
            raise ComfyContractError("Comfy upload_image did not return a filename")
        return f"{subfolder}/{returned_name}".strip("/")

    async def queue_load(self) -> int:
        return _queue_load(await self._get_json("/queue"))

    async def prompt_exists(self, prompt_id: str) -> bool:
        history, queue = await asyncio.gather(
            self._get_json(f"/history/{quote(prompt_id, safe='')}"),
            self._get_json("/queue"),
        )
        return prompt_id in history or _queue_contains(queue, prompt_id)

    async def submit(self, prompt: Mapping[str, Any], idempotency_key: str) -> ComfySubmission:
        prompt_id = deterministic_prompt_id(idempotency_key)
        client_id = str(uuid.uuid4())
        if await self.prompt_exists(prompt_id):
            return ComfySubmission(prompt_id=prompt_id, client_id=client_id, resumed_existing_bool=True)
        response = await self._client.post(
            self._url("/prompt"),
            json={"prompt": dict(prompt), "client_id": client_id, "prompt_id": prompt_id},
            timeout=self.request_timeout_seconds,
        )
        if response.is_error:
            raise ComfyContractError(
                f"Comfy /prompt rejected the workflow ({response.status_code}): "
                f"{response.text[:6000]}"
            )
        parsed = response.json()
        returned = str(parsed.get("prompt_id") or "").strip() if isinstance(parsed, dict) else ""
        if not returned:
            raise ComfyContractError("Comfy /prompt did not return prompt_id")
        if returned != prompt_id:
            raise ComfyContractError(f"Comfy changed deterministic prompt_id {prompt_id} to {returned}")
        return ComfySubmission(prompt_id=prompt_id, client_id=client_id, resumed_existing_bool=False)

    async def wait_for_output(self, prompt_id: str) -> tuple[Dict[str, Any], ComfyOutputFile]:
        deadline = time.monotonic() + self.render_timeout_seconds
        missing_polls = 0
        while time.monotonic() < deadline:
            history_envelope = await self._get_json(f"/history/{quote(prompt_id, safe='')}")
            history = history_envelope.get(prompt_id)
            if isinstance(history, dict):
                error = _history_error(history)
                if error:
                    raise ComfyTaskError(f"Comfy task {prompt_id} failed: {error}")
                output = _find_video_output(history.get("outputs"))
                if output:
                    return history, output
                if _history_completed(history):
                    raise ComfyTaskError(f"Comfy task {prompt_id} completed without a video output")
            queue = await self._get_json("/queue")
            if _queue_contains(queue, prompt_id):
                missing_polls = 0
            else:
                missing_polls += 1
                if missing_polls >= 3:
                    raise ComfyTaskError(f"Comfy task {prompt_id} disappeared from history and queue")
            await asyncio.sleep(self.poll_interval_seconds)
        raise ComfyTaskError(f"Comfy task {prompt_id} timed out")

    async def download_output(self, output: ComfyOutputFile) -> bytes:
        query = urlencode(
            {"filename": output.filename, "subfolder": output.subfolder, "type": output.file_type}
        )
        response = await self._client.get(
            self._url(f"/view?{query}"),
            timeout=max(self.request_timeout_seconds, 120.0),
        )
        response.raise_for_status()
        data = response.content
        if len(data) < 32:
            raise ComfyTaskError(f"Comfy returned an unexpectedly small video ({len(data)} bytes)")
        return data


def _safe_token(value: str, label: str) -> str:
    token = str(value or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,120}", token):
        raise ComfyContractError(f"{label} contains unsupported characters")
    return token


def _queue_load(queue: Mapping[str, Any]) -> int:
    return sum(len(queue.get(key) or []) for key in ("queue_running", "queue_pending") if isinstance(queue.get(key), list))


def _queue_contains(queue: Mapping[str, Any], prompt_id: str) -> bool:
    for key in ("queue_running", "queue_pending"):
        rows = queue.get(key)
        if isinstance(rows, list) and prompt_id in json.dumps(rows, ensure_ascii=False):
            return True
    return False


def _history_completed(history: Mapping[str, Any]) -> bool:
    status = history.get("status")
    return isinstance(status, dict) and (
        status.get("completed") is True or status.get("status_str") == "success"
    )


def _history_error(history: Mapping[str, Any]) -> str:
    status = history.get("status")
    messages = status.get("messages") if isinstance(status, dict) else None
    if not isinstance(messages, list):
        return ""
    for message in messages:
        if isinstance(message, list) and len(message) >= 2 and message[0] == "execution_error":
            return json.dumps(message[1], ensure_ascii=False)[:3000]
    return ""


def _find_video_output(value: object) -> Optional[ComfyOutputFile]:
    if isinstance(value, list):
        for item in value:
            found = _find_video_output(item)
            if found:
                return found
        return None
    if not isinstance(value, dict):
        return None
    filename = str(value.get("filename") or "")
    if filename.lower().endswith(VIDEO_EXTENSIONS):
        return ComfyOutputFile(
            filename=filename,
            subfolder=str(value.get("subfolder") or ""),
            file_type=str(value.get("type") or "output"),
        )
    for nested in value.values():
        found = _find_video_output(nested)
        if found:
            return found
    return None


def worker_from_environment(generation_mode: str) -> ComfyWorker:
    mode = str(generation_mode or "").strip().lower()
    if mode not in ("loop", "one_shot"):
        raise ComfyContractError(f"Unsupported generation mode: {generation_mode}")
    from .specs import load_animation_fitting_specs

    profile = load_animation_fitting_specs().workflows[mode]
    suffix = "LOOP" if mode == "loop" else "ONE_SHOT"
    workflow_name = os.getenv(f"AUTORIG_LTX_{suffix}_WORKFLOW", profile.workflow_name)
    fingerprint = os.getenv(
        f"AUTORIG_LTX_{suffix}_WORKFLOW_FINGERPRINT",
        profile.workflow_fingerprint,
    ).strip().lower()
    if workflow_name != profile.workflow_name or fingerprint != profile.workflow_fingerprint:
        raise ComfyContractError(
            f"{mode} worker must use pinned workflow {profile.workflow_name} "
            f"with fingerprint {profile.workflow_fingerprint}"
        )
    return ComfyWorker(
        worker_id=os.getenv("AUTORIG_LTX_COMFY_WORKER_ID", "local-4090"),
        base_url=os.getenv("AUTORIG_LTX_COMFY_URL", "http://127.0.0.1:8188"),
        workflow_name=workflow_name,
        expected_workflow_fingerprint=fingerprint,
        username=os.getenv("AUTORIG_LTX_COMFY_USERNAME", ""),
        password=os.getenv("AUTORIG_LTX_COMFY_PASSWORD", ""),
    )
