"""Internal transport routing for logical worker URLs.

Worker identities and all persisted/public URLs remain logical.  Only the URL
used for an outbound server-to-server HTTP request is rewritten here.
"""
from __future__ import annotations

import json
import os
from typing import Any, Dict
from urllib.parse import SplitResult, urlsplit, urlunsplit

import httpx


ENV_NAME = "AUTORIG_WORKER_TRANSPORTS"


class WorkerTransportConfigError(ValueError):
    """The explicit worker transport mapping is unsafe or malformed."""


def _origin(value: str, *, field: str) -> str:
    raw = str(value or "").strip()
    parsed = urlsplit(raw)
    if parsed.scheme.lower() not in {"http", "https"}:
        raise WorkerTransportConfigError(f"{field} must use http or https")
    if not parsed.hostname or parsed.username is not None or parsed.password is not None:
        raise WorkerTransportConfigError(f"{field} must be an absolute origin without credentials")
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise WorkerTransportConfigError(f"{field} must not contain a path, query, or fragment")
    try:
        port = parsed.port
    except ValueError as exc:
        raise WorkerTransportConfigError(f"{field} has an invalid port") from exc
    host = parsed.hostname.lower()
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if (parsed.scheme.lower(), port) in {("http", 80), ("https", 443)}:
        port = None
    netloc = f"{host}:{port}" if port is not None else host
    return f"{parsed.scheme.lower()}://{netloc}"


def worker_transport_map(raw: str | None = None) -> Dict[str, str]:
    """Parse and validate the explicit logical-origin to transport-origin map."""
    value = os.getenv(ENV_NAME, "") if raw is None else raw
    if not str(value or "").strip():
        return {}
    try:
        payload = json.loads(value)
    except (TypeError, json.JSONDecodeError) as exc:
        raise WorkerTransportConfigError(f"{ENV_NAME} must be a JSON object") from exc
    if not isinstance(payload, dict):
        raise WorkerTransportConfigError(f"{ENV_NAME} must be a JSON object")
    result: Dict[str, str] = {}
    for logical, transport in payload.items():
        if not isinstance(logical, str) or not isinstance(transport, str):
            raise WorkerTransportConfigError(f"{ENV_NAME} keys and values must be strings")
        source = _origin(logical, field="logical worker origin")
        destination = _origin(transport, field="transport worker origin")
        if source in result and result[source] != destination:
            raise WorkerTransportConfigError(f"duplicate logical worker origin: {source}")
        result[source] = destination
    return result


def worker_transport_url(url: str) -> str:
    """Rewrite only an exact configured origin, preserving path/query/fragment."""
    raw = str(url or "").strip()
    if not raw:
        return raw
    parsed = urlsplit(raw)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
        raise WorkerTransportConfigError("worker request URL must be absolute http(s)")
    if parsed.username is not None or parsed.password is not None:
        raise WorkerTransportConfigError("worker request URL must not contain credentials")
    source = _origin(
        urlunsplit(SplitResult(parsed.scheme, parsed.netloc, "", "", "")),
        field="worker request origin",
    )
    destination = worker_transport_map().get(source)
    if not destination:
        return raw
    target = urlsplit(destination)
    return urlunsplit((target.scheme, target.netloc, parsed.path, parsed.query, parsed.fragment))


def worker_http_client(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
    """Create an AsyncClient that routes configured worker origins at send time."""
    if not worker_transport_map():
        return httpx.AsyncClient(*args, **kwargs)

    async def _route_request(request: httpx.Request) -> None:
        routed = worker_transport_url(str(request.url))
        if routed == str(request.url):
            return
        request.url = httpx.URL(routed)
        netloc = request.url.netloc
        request.headers["Host"] = (
            netloc.decode("ascii") if isinstance(netloc, bytes) else str(netloc)
        )

    supplied_hooks = kwargs.pop("event_hooks", None) or {}
    event_hooks = {
        key: list(value)
        for key, value in supplied_hooks.items()
    }
    event_hooks["request"] = [_route_request, *event_hooks.get("request", [])]
    kwargs["event_hooks"] = event_hooks
    return httpx.AsyncClient(*args, **kwargs)
