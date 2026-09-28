"""Which graph node a render submit belongs to (owner rule, 2026-09-28).

The node editor sends three headers with every submit it makes on behalf of a
node: ``X-Graph-Id`` (the saved graph the canvas is), ``X-Node-Id`` (the node's
stored id) and ``X-Submit-Session`` (one value per Render press per tab). The
task-owner middleware puts them in a context variable before the route runs;
every renderfin submit site merges :func:`fields` into its payload, so the farm
task carries ``graph_id`` / ``node_id`` / ``node_signature`` / ``submit_session``
and the queue can stand down what a graph no longer needs.

The signature is computed here, on the server, from the *stored* graph, never
taken from the browser: Render autosaves first, so the stored node is what the
tab is rendering, and the same function judges it again at the next save.
"""
from __future__ import annotations

import contextvars
import logging
import re
from typing import Dict

logger = logging.getLogger(__name__)

_SAFE = re.compile(r"^[A-Za-z0-9_.:-]{1,80}$")

_context: contextvars.ContextVar[Dict[str, str]] = contextvars.ContextVar(
    "ai_graph_context", default={})


def set_from_headers(headers: Dict[str, str]) -> None:
    """Remember the graph headers of the request being handled (middleware)."""
    values: Dict[str, str] = {}
    for header, key in (("x-graph-id", "graph_id"), ("x-node-id", "node_id"),
                        ("x-submit-session", "submit_session")):
        value = str(headers.get(header) or "").strip()
        if value and _SAFE.match(value):
            values[key] = value
    _context.set(values)


def current() -> Dict[str, str]:
    return dict(_context.get() or {})


def fields() -> Dict[str, str]:
    """What a renderfin payload gets: ids plus the node's current signature.

    Empty when the request did not come from a node (an agent calling the
    API directly, an avatar page, a smoke test): such a task belongs to no
    graph and no save can ever cancel it.
    """
    values = current()
    graph_id = values.get("graph_id") or ""
    node_id = values.get("node_id") or ""
    if not graph_id or not node_id:
        return {}
    out = {"graph_id": graph_id, "node_id": node_id,
           "submit_session": values.get("submit_session") or ""}
    try:
        import ai_graph
        out["node_signature"] = ai_graph.stored_node_signature(graph_id, node_id)
        out["node_identity"] = ai_graph.stored_node_identity(graph_id, node_id)
    except Exception:
        logger.exception("node signature for %s/%s not computed", graph_id, node_id)
        out["node_signature"] = ""
    return out
