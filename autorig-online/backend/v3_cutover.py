"""Explicit V3-only cutover contract; importing this module never changes state.

The controller must validate the *serving* build on each target before enabling
creation. A web viewer build is not evidence of a converter's rig capability.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


SCHEMA = "autorig.v3.cutover/1"
TASK_ROUTES = frozenset({"website", "telegram", "api", "generation", "retry", "convert"})
REQUIRED_STAGES = frozenset({"source", "analysis", "rig", "retarget", "qa", "publish"})
REQUIRED_EXPORTS = frozenset({"glb", "fbx", "unity_package"})


class CutoverNotReady(ValueError):
    pass


@dataclass(frozen=True)
class WorkerReadiness:
    name: str
    serving_artifact_sha256: str
    protocol: str
    stages: frozenset[str]
    exports: frozenset[str]
    durable_queue: bool
    viewer_and_agent: bool


def require_v3_only_ready(
    workers: Mapping[str, WorkerReadiness],
    *,
    required_workers: frozenset[str],
    connected_routes: frozenset[str],
    callback_verified: bool,
    source_binding_verified: bool,
    subscription_export_gate_verified: bool,
) -> None:
    """Fail closed before a global switch, listing all missing integrations.

    This is readiness, NOT an anatomical quality certificate. Result QA must
    still distinguish needs_review from done for every individual task.
    """
    missing = []
    if not required_workers:
        missing.append("fleet targets missing")
    for name in sorted(required_workers):
        worker = workers.get(name)
        if worker is None:
            missing.append(f"{name}: no serving-build receipt")
            continue
        digest = worker.serving_artifact_sha256
        if worker.name != name or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            missing.append(f"{name}: serving-build identity invalid")
        if worker.protocol != "autorig.v3.dispatch/2":
            missing.append(f"{name}: V3 dispatch unavailable")
        for stage in sorted(REQUIRED_STAGES - worker.stages):
            missing.append(f"{name}: stage {stage} unavailable")
        for export in sorted(REQUIRED_EXPORTS - worker.exports):
            missing.append(f"{name}: export {export} unavailable")
        if worker.durable_queue is not True:
            missing.append(f"{name}: queue durability unverified")
        if worker.viewer_and_agent is not True:
            missing.append(f"{name}: task viewer/agent binding unverified")
    missing.extend(f"route {route} not connected" for route in sorted(TASK_ROUTES - connected_routes))
    for label, verified in (
        ("callback", callback_verified),
        ("source binding", source_binding_verified),
        ("subscription export gate", subscription_export_gate_verified),
    ):
        if verified is not True:
            missing.append(f"{label} unverified")
    if missing:
        raise CutoverNotReady("; ".join(missing))


def new_task_pipeline(*, cutover_enabled: bool, intent: str) -> tuple[str, str]:
    """Keep user intent separate from algorithm version, with no legacy fallback.

    Callers may use this only AFTER require_v3_only_ready. Existing task records
    are not rewritten: their result URLs and original execution binding survive.
    """
    if cutover_enabled is not True:
        raise CutoverNotReady("V3-only creation has not been activated")
    if intent not in {"rig", "convert", "generate", "accessory"}:
        raise ValueError("unsupported task intent")
    return "v3", intent
