"""Pure helpers for converter worker request payloads."""
from typing import Any, Dict, List, Optional


def _normalize_task_type(value: Optional[str]) -> str:
    s = (value or "").strip()
    return s if s else "t_pose"


def build_worker_task_payload(
    input_url: str,
    task_type: str = "t_pose",
    transform_params: Optional[dict] = None,
    *,
    pipeline_kind: str = "rig",
    animal_type: Optional[str] = None,
    mode: Optional[str] = None,
    animal_semantic_markers: Optional[Dict[str, List[float]]] = None,
    viewer_environment: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build the JSON body sent to the converter worker."""
    task_type = _normalize_task_type(task_type)
    pk = (pipeline_kind or "rig").strip().lower()
    if pk not in ("rig", "convert"):
        pk = "rig"

    if pk == "convert":
        return {
            "input_url": input_url,
            "type": task_type,
        }

    payload: Dict[str, Any] = {
        "input_url": input_url,
        "type": task_type,
        "mode": mode or "only_rig",
    }
    if animal_type:
        payload["animal_type"] = animal_type
    if animal_semantic_markers:
        payload["animal_semantic_markers"] = animal_semantic_markers
    if transform_params:
        if transform_params.get("local_position"):
            payload["local_position"] = transform_params["local_position"]
        if transform_params.get("local_rotation"):
            payload["local_rotation"] = transform_params["local_rotation"]
        if transform_params.get("local_scale"):
            payload["local_scale"] = transform_params["local_scale"]
    if isinstance(viewer_environment, dict) and viewer_environment:
        payload["viewer_environment"] = viewer_environment
    return payload
