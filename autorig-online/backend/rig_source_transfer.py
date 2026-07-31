"""Immutable connected-rig-source to textured-appearance transfer contract."""
from __future__ import annotations

import copy
import re
from typing import Any, Dict
from urllib.parse import urlparse


RIG_SOURCE_TRANSFER_SCHEMA = "autorig.rig-source-transfer.v1"
WORKER_TRANSPORT_KEY = "_autorig_rig_source_transfer"
SERVER_AUTHORIZATION_KEY = "_autorig_rig_source_transfer_authorization"
SERVER_AUTHORIZATION_VALUE = "server_validated.v1"
SERVER_VALIDATION_KEY = "_autorig_rig_source_transfer_validation"
PROTECTED_VIEWER_SETTINGS_KEYS = frozenset(
    {"rig_source_transfer", SERVER_AUTHORIZATION_KEY, SERVER_VALIDATION_KEY}
)
MAX_POSITION_DELTA_M = 1e-6
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class RigSourceTransferContractError(ValueError):
    """Raised when the paired immutable rig-source contract is unsafe."""


def _artifact(value: Any, field: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise RigSourceTransferContractError(f"{field} must be an object")
    url = str(value.get("url") or "").strip()
    parsed = urlparse(url)
    if parsed.scheme != "https" or not parsed.netloc:
        raise RigSourceTransferContractError(f"{field}.url must be an absolute HTTPS URL")
    sha256 = str(value.get("sha256") or "").strip()
    if not _SHA256_RE.fullmatch(sha256):
        raise RigSourceTransferContractError(
            f"{field}.sha256 must be lowercase 64-hex"
        )
    size = value.get("bytes")
    if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
        raise RigSourceTransferContractError(f"{field}.bytes must be a positive integer")
    role = str(value.get("role") or "").strip()
    expected_role = {
        "connected_source": "connected_pretexture_mesh",
        "appearance_target": "textured_pbr_uv_split_mesh",
    }[field]
    if role != expected_role:
        raise RigSourceTransferContractError(
            f"{field}.role must be {expected_role!r}"
        )
    return {"url": url, "sha256": sha256, "bytes": size, "role": role}


def normalize_rig_source_transfer(value: Any) -> Dict[str, Any]:
    """Validate and return the canonical worker-safe paired-source contract."""
    if not isinstance(value, dict):
        raise RigSourceTransferContractError("rig_source_transfer must be an object")
    if value.get("schema") != RIG_SOURCE_TRANSFER_SCHEMA:
        raise RigSourceTransferContractError(
            f"rig_source_transfer.schema must be {RIG_SOURCE_TRANSFER_SCHEMA!r}"
        )
    connected_source = _artifact(value.get("connected_source"), "connected_source")
    appearance_target = _artifact(value.get("appearance_target"), "appearance_target")
    if connected_source["sha256"] == appearance_target["sha256"]:
        raise RigSourceTransferContractError(
            "connected source and appearance target must be distinct immutable artifacts"
        )

    mapping = value.get("mapping")
    if not isinstance(mapping, dict):
        raise RigSourceTransferContractError("mapping must be an object")
    if mapping.get("method") != "exact_position_and_face_topology_v1":
        raise RigSourceTransferContractError(
            "mapping.method must be 'exact_position_and_face_topology_v1'"
        )
    delta = mapping.get("max_position_delta_m")
    if isinstance(delta, bool) or not isinstance(delta, (int, float)):
        raise RigSourceTransferContractError(
            "mapping.max_position_delta_m must be numeric"
        )
    delta = float(delta)
    if not 0.0 < delta <= MAX_POSITION_DELTA_M:
        raise RigSourceTransferContractError(
            f"mapping.max_position_delta_m must be inside (0, {MAX_POSITION_DELTA_M}]"
        )
    required_true = (
        "require_each_source_component_watertight",
        "require_full_vertex_coverage",
        "require_source_vertex_coverage",
        "require_face_topology_identity",
        "require_duplicate_weight_identity",
        "preserve_appearance_static_signatures",
    )
    for key in required_true:
        if mapping.get(key) is not True:
            raise RigSourceTransferContractError(f"mapping.{key} must be true")
    if mapping.get("source_component_policy") != "one_or_more_watertight_components":
        raise RigSourceTransferContractError(
            "mapping.source_component_policy must be "
            "'one_or_more_watertight_components'"
        )
    if value.get("output_revision_policy") != "new_task_immutable":
        raise RigSourceTransferContractError(
            "output_revision_policy must be 'new_task_immutable'"
        )
    return {
        "schema": RIG_SOURCE_TRANSFER_SCHEMA,
        "connected_source": connected_source,
        "appearance_target": appearance_target,
        "mapping": {
            "method": "exact_position_and_face_topology_v1",
            "max_position_delta_m": delta,
            "source_component_policy": "one_or_more_watertight_components",
            **{key: True for key in required_true},
        },
        "output_revision_policy": "new_task_immutable",
    }


def build_rig_source_transfer(
    *,
    connected_source_url: str,
    connected_source_sha256: str,
    connected_source_bytes: int,
    appearance_target_url: str,
    appearance_target_sha256: str,
    appearance_target_bytes: int,
) -> Dict[str, Any]:
    """Build the exact fail-closed contract used by production task dispatch."""
    return normalize_rig_source_transfer(
        {
            "schema": RIG_SOURCE_TRANSFER_SCHEMA,
            "connected_source": {
                "url": connected_source_url,
                "sha256": connected_source_sha256,
                "bytes": connected_source_bytes,
                "role": "connected_pretexture_mesh",
            },
            "appearance_target": {
                "url": appearance_target_url,
                "sha256": appearance_target_sha256,
                "bytes": appearance_target_bytes,
                "role": "textured_pbr_uv_split_mesh",
            },
            "mapping": {
                "method": "exact_position_and_face_topology_v1",
                "max_position_delta_m": MAX_POSITION_DELTA_M,
                "source_component_policy": "one_or_more_watertight_components",
                "require_each_source_component_watertight": True,
                "require_full_vertex_coverage": True,
                "require_source_vertex_coverage": True,
                "require_face_topology_identity": True,
                "require_duplicate_weight_identity": True,
                "preserve_appearance_static_signatures": True,
            },
            "output_revision_policy": "new_task_immutable",
        }
    )


def copy_for_worker_transport(value: Any) -> Dict[str, Any]:
    """Return an isolated validated copy suitable for a worker request body."""
    return copy.deepcopy(normalize_rig_source_transfer(value))


def authorized_transfer_from_settings(value: Any) -> Dict[str, Any] | None:
    """Return a transfer only when it carries the server-only authorization marker."""
    if not isinstance(value, dict):
        return None
    if value.get(SERVER_AUTHORIZATION_KEY) != SERVER_AUTHORIZATION_VALUE:
        return None
    return copy_for_worker_transport(value.get("rig_source_transfer"))


def public_viewer_settings_copy(value: Any) -> Dict[str, Any]:
    """Remove server-only paired-transfer state from a public viewer response."""
    if not isinstance(value, dict):
        return {}
    return {
        key: copy.deepcopy(item)
        for key, item in value.items()
        if key not in PROTECTED_VIEWER_SETTINGS_KEYS
    }


def merge_public_viewer_settings(
    existing: Any,
    replacement: Any,
) -> Dict[str, Any]:
    """Reject client injection and preserve an existing authorized transfer atomically."""
    if not isinstance(replacement, dict):
        raise RigSourceTransferContractError("viewer settings must be an object")
    forbidden = sorted(PROTECTED_VIEWER_SETTINGS_KEYS.intersection(replacement))
    if forbidden:
        raise RigSourceTransferContractError(
            f"viewer settings contain server-only keys: {forbidden!r}"
        )
    merged = copy.deepcopy(replacement)
    if isinstance(existing, dict):
        for key in ("rig_v2_animal_detection", "viewer_theme_selection"):
            if isinstance(existing.get(key), dict) and key not in merged:
                merged[key] = copy.deepcopy(existing[key])
        if existing.get(SERVER_AUTHORIZATION_KEY) == SERVER_AUTHORIZATION_VALUE:
            # Validate before preserving.  A corrupt server marker must fail
            # closed instead of silently downgrading to direct textured rigging.
            merged["rig_source_transfer"] = copy_for_worker_transport(
                existing.get("rig_source_transfer")
            )
            merged[SERVER_AUTHORIZATION_KEY] = SERVER_AUTHORIZATION_VALUE
            validation = existing.get(SERVER_VALIDATION_KEY)
            if not isinstance(validation, dict):
                raise RigSourceTransferContractError(
                    "authorized rig source transfer is missing validation evidence"
                )
            merged[SERVER_VALIDATION_KEY] = copy.deepcopy(validation)
    return merged


def safe_pair_upload_basename(value: Any, *, default: str) -> str:
    """Return a cross-platform basename for a paired multipart upload."""
    name = str(value or default).replace("\\", "/").rsplit("/", 1)[-1].strip()
    if not name or name in {".", ".."} or "\x00" in name:
        raise RigSourceTransferContractError("paired upload filename is invalid")
    return name
