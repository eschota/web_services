"""Deterministic, side-effect-free edits for AI node graphs.

This module is intentionally only a validator and patch applicator.  It never
runs a node, calls a model, saves a graph, or evaluates text as code.  A caller
may propose a small whitelist of operations; the complete result is returned
only after the same graph validation used by the saved-graph API succeeds.
"""

from __future__ import annotations

import copy
import json
import math
import time
from typing import Any, Dict, List, Mapping, MutableMapping, Set

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

import ai_graph
import ai_model_catalogue
import ai_model_defaults
import ai_services


router = APIRouter()

MAX_OPERATIONS = 200
DISPLAY_PARAM_KEYS = {"_label", "_display_mode"}
CONTROL_INPUTS = {"control_pose": "pose", "control_depth": "depth", "control_canny": "canny"}


class GraphEditRequest(BaseModel):
    graph: ai_graph.Graph
    operations: List[Dict[str, Any]] = Field(default_factory=list)


def _reject(code: str, message: str, operation_index: int | None = None) -> None:
    detail: Dict[str, Any] = {"error_string": code, "message_string": message}
    if operation_index is not None:
        detail["operation_index_int"] = operation_index
    raise HTTPException(status_code=400, detail=detail)


def _node_map(graph: ai_graph.Graph) -> Dict[str, ai_graph.GraphNode]:
    return {node.id: node for node in graph.nodes}


def _node(graph: ai_graph.Graph, node_id: object, operation_index: int) -> ai_graph.GraphNode:
    wanted = str(node_id or "").strip()
    found = _node_map(graph).get(wanted)
    if not found:
        _reject("unknown_node", f"The graph has no node called '{wanted}'", operation_index)
    return found


def _descendants(graph: ai_graph.Graph, roots: Set[str]) -> Set[str]:
    edges: Dict[str, List[str]] = {}
    for link in graph.links:
        edges.setdefault(link.from_node, []).append(link.to_node)
    seen = set(roots)
    pending = list(roots)
    while pending:
        current = pending.pop()
        for target in edges.get(current, []):
            if target not in seen:
                seen.add(target)
                pending.append(target)
    return seen


def _link_tuple(value: Mapping[str, Any], operation_index: int) -> tuple[str, str, str, str]:
    fields = tuple(str(value.get(key, "")).strip() for key in ("from", "output", "to", "input"))
    if not all(fields):
        _reject("bad_link", "A link requires from, output, to, and input", operation_index)
    return fields  # type: ignore[return-value]


def _declared_param_names(service_id: str) -> Set[str]:
    return {str(item.get("name")) for item in ai_services.params_for(service_id)} | DISPLAY_PARAM_KEYS


def _param_declaration(service_id: str, name: str) -> Mapping[str, Any] | None:
    return next((item for item in ai_services.params_for(service_id)
                 if str(item.get("name")) == name), None)


def _finite_number(name: str, value: Any) -> int | float:
    if isinstance(value, bool):
        _reject("bad_parameter_value", f"Parameter '{name}' must be a finite number")
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            _reject("bad_parameter_value", f"Parameter '{name}' must be a finite number")
        try:
            value = float(stripped)
        except ValueError:
            _reject("bad_parameter_value", f"Parameter '{name}' must be a finite number")
    if not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        _reject("bad_parameter_value", f"Parameter '{name}' must be a finite number")
    return int(value) if float(value).is_integer() else float(value)


def _normalize_param_value(service_id: str, name: str, value: Any) -> Any:
    """Coerce browser/model numeric strings without weakening declarations."""
    if name in DISPLAY_PARAM_KEYS:
        return value
    declaration = _param_declaration(service_id, name)
    if not declaration:
        return value
    kind = str(declaration.get("type") or "")
    if kind in {"number", "range"} or name in {"width", "height"}:
        return _finite_number(name, value)
    if kind == "select" and isinstance(value, str):
        options = declaration.get("options") or []
        allowed = [item.get("value") for item in options if isinstance(item, Mapping)]
        numeric = [item for item in allowed
                   if isinstance(item, (int, float)) and not isinstance(item, bool)]
        if allowed and len(numeric) == len(allowed):
            parsed = _finite_number(name, value)
            for option in numeric:
                if float(option) == float(parsed):
                    return option
    return value


def _normalize_node_params(node: ai_graph.GraphNode) -> None:
    if node.kind != ai_graph.NODE_SERVICE:
        return
    service_id = str(node.service or "")
    node.params = {
        str(name): _normalize_param_value(service_id, str(name), value)
        for name, value in node.params.items()
    }


def _validate_param_value(service_id: str, name: str, value: Any) -> None:
    if name in DISPLAY_PARAM_KEYS:
        if not isinstance(value, str):
            _reject("bad_parameter_value", f"Parameter '{name}' must be text")
        return
    declaration = _param_declaration(service_id, name)
    if not declaration:
        _reject("unknown_parameter", f"Service '{service_id}' does not declare parameter '{name}'")
    kind = str(declaration.get("type") or "")
    if kind in {"number", "range"} or (name in {"width", "height"} and isinstance(value, (int, float))):
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
            _reject("bad_parameter_value", f"Parameter '{name}' must be a finite number")
        minimum = declaration.get("min")
        maximum = declaration.get("max")
        # Image dimensions are customisable even though their UI control also
        # lists common presets.
        if name in {"width", "height"}:
            minimum, maximum = 256, 2048
        if minimum is not None and float(value) < float(minimum):
            _reject("bad_parameter_value", f"Parameter '{name}' is below its minimum")
        if maximum is not None and float(value) > float(maximum):
            _reject("bad_parameter_value", f"Parameter '{name}' is above its maximum")
        return
    if kind in {"text", "textarea", "model"}:
        if not isinstance(value, str):
            _reject("bad_parameter_value", f"Parameter '{name}' must be text")
        return
    if kind == "select":
        options = declaration.get("options") or []
        allowed = {item.get("value") for item in options if isinstance(item, Mapping)}
        if allowed and value not in allowed:
            _reject("bad_parameter_value", f"Parameter '{name}' is not one of the declared choices")
        if not allowed and not isinstance(value, (str, int, float)):
            _reject("bad_parameter_value", f"Parameter '{name}' has an unsupported value")
        return
    if not isinstance(value, (str, int, float, bool)) and value is not None:
        _reject("bad_parameter_value", f"Parameter '{name}' must be a scalar JSON value")


def _validate_params(graph: ai_graph.Graph) -> None:
    for node in graph.nodes:
        if node.kind == ai_graph.NODE_SERVICE:
            allowed = _declared_param_names(str(node.service or ""))
        else:
            allowed = DISPLAY_PARAM_KEYS
        unknown = set(node.params) - allowed
        if unknown:
            _reject("unknown_parameter",
                    f"Node '{node.id}' does not declare parameter '{sorted(unknown)[0]}'")
        for name, value in node.params.items():
            _validate_param_value(str(node.service or ""), str(name), value)


def _catalogue_entry(name: object, kind: str, service_id: str) -> Mapping[str, Any] | None:
    filename = str(name or "").strip()
    if not filename:
        return None
    entry = ai_model_catalogue.known_file(filename, kind)
    if not entry:
        _reject("unknown_model", f"No {kind} called '{filename}' is in the live catalogue")
    if not entry.get("usable") or service_id not in (entry.get("services") or []):
        _reject("incompatible_model", f"'{filename}' is not usable by the {service_id} service")
    return entry


def _families_compatible(checkpoint: Mapping[str, Any], lora: Mapping[str, Any]) -> bool:
    checkpoint_family = str(checkpoint.get("family") or "").lower()
    lora_family = str(lora.get("family") or "").lower()
    if checkpoint_family == lora_family:
        return True
    checkpoint_base = str(checkpoint.get("base") or "").lower()
    lora_base = str(lora.get("base") or "").lower()
    return bool(checkpoint_base and lora_base and
                (checkpoint_base in lora_base or lora_base in checkpoint_base))


def _validate_catalogue_and_controls(graph: ai_graph.Graph) -> None:
    by_id = _node_map(graph)
    selected_checkpoints: Dict[str, Mapping[str, Any]] = {}
    for node in graph.nodes:
        if node.kind != ai_graph.NODE_SERVICE:
            continue
        service_id = str(node.service or "")
        checkpoint = _catalogue_entry(node.params.get("checkpoint"), "checkpoint", service_id)
        lora = _catalogue_entry(node.params.get("lora"), "lora", service_id)
        if checkpoint:
            selected_checkpoints[node.id] = checkpoint
        if checkpoint and lora and not _families_compatible(checkpoint, lora):
            _reject("incompatible_model_family",
                    f"Checkpoint and LoRA on '{node.id}' belong to incompatible families")
        if checkpoint:
            explicit = {
                key: value for key, value in node.params.items()
                if key in {"steps", "cfg", "sampler", "scheduler", "lora_strength"}
                and value not in (None, "", 0, "0")
            }
            try:
                ai_model_defaults.resolve(checkpoint, lora, explicit)
            except (TypeError, ValueError) as exc:
                _reject("invalid_sampling_settings",
                        f"Sampling settings on '{node.id}' are not supported: {exc}")

    occupied_inputs: Set[tuple[str, str]] = set()
    control_links: Dict[str, List[ai_graph.GraphLink]] = {}
    for link in graph.links:
        socket = (link.to_node, link.input)
        if socket in occupied_inputs:
            _reject("input_already_connected",
                    f"'{link.to_node}.{link.input}' has more than one incoming link")
        occupied_inputs.add(socket)
        if link.input in CONTROL_INPUTS:
            control_links.setdefault(link.to_node, []).append(link)

    for target_id, links in control_links.items():
        target = by_id.get(target_id)
        if not target or target.service != "image":
            _reject("unsupported_control_target", "Control maps may only connect to an image node")
        if len(links) > 1:
            _reject("multiple_control_channels",
                    f"Image node '{target_id}' may use only one ControlNet channel")
        link = links[0]
        source = by_id[link.from_node]
        channel = CONTROL_INPUTS[link.input]
        if source.service != "control_" + channel:
            _reject("control_channel_mismatch",
                    f"'{source.id}' is not a {channel} ControlNet extractor")
        checkpoint = selected_checkpoints.get(target_id)
        if not checkpoint:
            continue  # The service's deployed default workflow owns compatibility.
        explicit_channels = {str(value) for value in (checkpoint.get("control_channels") or [])}
        source_service = ai_services.service(str(source.service or "")) or {}
        compatible_families = {str(value).lower()
                               for value in (source_service.get("compatible_image_families") or [])}
        family = str(checkpoint.get("family") or "").lower()
        if channel not in explicit_channels and family not in compatible_families:
            _reject("unsupported_control_family",
                    f"Checkpoint '{checkpoint.get('file')}' does not support {channel} control")


def _validate_final(graph: ai_graph.Graph) -> None:
    ai_graph.validate(graph)
    _validate_params(graph)
    _validate_catalogue_and_controls(graph)
    try:
        payload = json.dumps(graph.model_dump(by_alias=True), ensure_ascii=False,
                             sort_keys=True, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        _reject("invalid_graph_value", f"The graph contains a non-JSON value: {exc}")
    if len(payload) > ai_graph.MAX_GRAPH_BYTES:
        _reject("graph_too_large", "The edited graph is larger than the store accepts")


def apply_operations(original: ai_graph.Graph,
                     operations: List[Mapping[str, Any]]) -> tuple[ai_graph.Graph, Dict[str, Any], List[str]]:
    """Return a fully validated copy or raise without mutating ``original``."""
    if len(operations) > MAX_OPERATIONS:
        _reject("too_many_operations", f"At most {MAX_OPERATIONS} operations are accepted")
    graph = ai_graph.Graph.model_validate(copy.deepcopy(original.model_dump(by_alias=True)))
    for existing in graph.nodes:
        _normalize_node_params(existing)
    invalidated: Set[str] = set()
    summary: Dict[str, Any] = {
        "operation_count_int": len(operations),
        "added_node_ids_array": [],
        "removed_node_ids_array": [],
        "updated_node_ids_array": [],
        "connections_added_int": 0,
        "connections_removed_int": 0,
        "renamed_bool": False,
    }

    for index, raw in enumerate(operations):
        if not isinstance(raw, Mapping):
            _reject("bad_operation", "Every operation must be an object", index)
        op = str(raw.get("op") or "").strip()
        if op == "add_node":
            try:
                added = ai_graph.GraphNode.model_validate(raw.get("node"))
            except Exception as exc:
                _reject("bad_node", f"The new node is invalid: {exc}", index)
            if not added.id.strip() or added.id in _node_map(graph):
                _reject("duplicate_node_id", f"Node id '{added.id}' is empty or already used", index)
            try:
                _normalize_node_params(added)
                for name, value in added.params.items():
                    _validate_param_value(str(added.service or ""), str(name), value)
            except HTTPException as exc:
                if isinstance(exc.detail, dict):
                    exc.detail.setdefault("operation_index_int", index)
                raise
            graph.nodes.append(added)
            invalidated.add(added.id)
            summary["added_node_ids_array"].append(added.id)
        elif op == "remove_node":
            removed = _node(graph, raw.get("id"), index)
            if graph.comparison_anchor_id == removed.id:
                graph.comparison_anchor_id = ""
            invalidated.update(_descendants(graph, {removed.id}))
            graph.nodes = [node for node in graph.nodes if node.id != removed.id]
            graph.links = [link for link in graph.links
                           if link.from_node != removed.id and link.to_node != removed.id]
            graph.results.pop(removed.id, None)
            summary["removed_node_ids_array"].append(removed.id)
        elif op == "update_params":
            target = _node(graph, raw.get("id"), index)
            values = raw.get("values")
            if not isinstance(values, Mapping):
                _reject("bad_parameter_update", "update_params values must be an object", index)
            allowed = (_declared_param_names(str(target.service or ""))
                       if target.kind == ai_graph.NODE_SERVICE else DISPLAY_PARAM_KEYS)
            unknown = set(map(str, values)) - allowed
            if unknown:
                _reject("unknown_parameter",
                        f"Node '{target.id}' does not declare parameter '{sorted(unknown)[0]}'", index)
            normalized_values = {
                str(name): _normalize_param_value(str(target.service or ""), str(name), value)
                for name, value in values.items()
            }
            for name, value in normalized_values.items():
                try:
                    _validate_param_value(str(target.service or ""), str(name), value)
                except HTTPException as exc:
                    if isinstance(exc.detail, dict):
                        exc.detail.setdefault("operation_index_int", index)
                    raise
            target.params.update(copy.deepcopy(normalized_values))
            invalidated.update(_descendants(graph, {target.id}))
            summary["updated_node_ids_array"].append(target.id)
        elif op == "set_input":
            target = _node(graph, raw.get("id"), index)
            if target.kind != ai_graph.NODE_INPUT:
                _reject("not_an_input_node", f"Node '{target.id}' is not an input node", index)
            value = raw.get("value")
            if value is not None and not isinstance(value, str):
                _reject("bad_input_value", "An input value must be text or null", index)
            target.value = value
            invalidated.update(_descendants(graph, {target.id}))
            summary["updated_node_ids_array"].append(target.id)
        elif op in {"connect", "disconnect"}:
            source_id, output, target_id, input_name = _link_tuple(raw, index)
            _node(graph, source_id, index)
            _node(graph, target_id, index)
            wanted = (source_id, output, target_id, input_name)
            existing = [(link.from_node, link.output, link.to_node, link.input)
                        for link in graph.links]
            if op == "connect":
                if wanted in existing:
                    _reject("duplicate_link", "That connection already exists", index)
                graph.links.append(ai_graph.GraphLink(**{
                    "from": source_id, "output": output, "to": target_id, "input": input_name}))
                summary["connections_added_int"] += 1
            else:
                if wanted not in existing:
                    _reject("unknown_link", "That connection is not in the graph", index)
                graph.links = [link for link in graph.links
                               if (link.from_node, link.output, link.to_node, link.input) != wanted]
                summary["connections_removed_int"] += 1
            invalidated.update(_descendants(graph, {target_id}))
        elif op == "move_node":
            target = _node(graph, raw.get("id"), index)
            try:
                x, y = float(raw.get("x")), float(raw.get("y"))
            except (TypeError, ValueError):
                _reject("bad_position", "move_node requires numeric x and y", index)
            if not math.isfinite(x) or not math.isfinite(y):
                _reject("bad_position", "Node coordinates must be finite", index)
            target.x, target.y = x, y
        elif op == "rename_graph":
            name = raw.get("name")
            if not isinstance(name, str) or not name.strip() or len(name.strip()) > 200:
                _reject("bad_graph_name", "A graph name must be 1-200 visible characters", index)
            graph.name = name.strip()
            summary["renamed_bool"] = True
        else:
            _reject("unknown_operation", f"Operation '{op}' is not allowed", index)

    _validate_final(graph)
    for node_id in invalidated:
        graph.results.pop(node_id, None)
    summary["added_node_ids_array"] = sorted(set(summary["added_node_ids_array"]))
    summary["removed_node_ids_array"] = sorted(set(summary["removed_node_ids_array"]))
    summary["updated_node_ids_array"] = sorted(set(summary["updated_node_ids_array"]))
    return graph, summary, sorted(invalidated)


@router.post("/api/ai/graph-edits/validate")
async def api_validate_graph_edits(body: GraphEditRequest):
    graph, summary, invalidated = apply_operations(body.graph, body.operations)
    return {
        "success_bool": True,
        "graph_object": graph.model_dump(by_alias=True),
        "summary": summary,
        "invalidated_node_ids_array": invalidated,
        "server_time_unix_int": int(time.time()),
    }


@router.get("/api/ai/graph-edits/schema")
async def api_graph_edits_schema():
    services = []
    for entry in ai_services.SERVICES:
        service_id = str(entry["id"])
        services.append({
            "id": service_id,
            "title": entry.get("title"),
            "status": entry.get("status"),
            "inputs": entry.get("inputs") or [],
            "outputs": entry.get("outputs") or [],
            "params": ai_services.params_for(service_id),
        })
    models = [{key: entry.get(key) for key in (
        "file", "title", "kind", "family", "base", "services", "usable",
        "control_channels", "sampling_policy", "unusable_reason")}
              for entry in ai_model_catalogue.entries() if entry.get("usable")]
    return {
        "success_bool": True,
        "side_effects_string": "validation only; does not save, render, delete, or execute text",
        "max_operations_int": MAX_OPERATIONS,
        "max_nodes_int": ai_graph.MAX_NODES,
        "operations_array": [
            {"op": "add_node", "fields": ["node"]},
            {"op": "remove_node", "fields": ["id"]},
            {"op": "update_params", "fields": ["id", "values"]},
            {"op": "set_input", "fields": ["id", "value"]},
            {"op": "connect", "fields": ["from", "output", "to", "input"]},
            {"op": "disconnect", "fields": ["from", "output", "to", "input"]},
            {"op": "move_node", "fields": ["id", "x", "y"]},
            {"op": "rename_graph", "fields": ["name"]},
        ],
        "services_array": services,
        "models_array": models,
        "server_time_unix_int": int(time.time()),
    }
