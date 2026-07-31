"""Worker registry: one JSON file per GPU server (port of C# server registry)."""
from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Dict, List, Optional

from . import config
from .models import RenderServer

_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9_.-]+")


class ServerRegistry:
    def __init__(self, servers_dir: Optional[Path] = None):
        self.servers_dir = Path(servers_dir or config.SERVERS_DIR)
        self._servers: Dict[str, RenderServer] = {}
        self.load()

    def load(self) -> None:
        self._servers = {}
        if not self.servers_dir.is_dir():
            return
        for path in sorted(self.servers_dir.glob("*.json")):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                server = RenderServer(**data)
                if server.render_server_name:
                    self._servers[server.render_server_name] = server
            except Exception as exc:
                print(f"[Renderfin][Registry] skip {path.name}: {exc}")

    def _path_for(self, name: str) -> Path:
        safe = _SAFE_NAME_RE.sub("_", name).strip("._") or "server"
        return self.servers_dir / f"{safe}.json"

    def save(self, server: RenderServer) -> None:
        self.servers_dir.mkdir(parents=True, exist_ok=True)
        payload = server.model_dump()
        payload["date_update"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        self._path_for(server.render_server_name).write_text(
            json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        self._servers[server.render_server_name] = server

    def delete(self, name: str) -> bool:
        path = self._path_for(name)
        existed = name in self._servers
        self._servers.pop(name, None)
        if path.is_file():
            path.unlink()
            existed = True
        return existed

    def get(self, name: str) -> Optional[RenderServer]:
        return self._servers.get(name)

    def all(self) -> List[RenderServer]:
        return list(self._servers.values())

    def handle_operation(self, body: RenderServer) -> Dict[str, object]:
        """Dispatch a RenderServer-shaped POST body (registration protocol)."""
        op = (body.render_operation or "").strip().lower()
        if op == "delete_server":
            ok = self.delete(body.render_server_name)
            return {"ok": ok, "operation": op}
        if op in ("add_server", "info", "set_status", ""):
            existing = self.get(body.render_server_name)
            if existing and op in ("info", "set_status"):
                existing.status = body.status or existing.status
                existing.queue_size = body.queue_size
                if body.available_workflows:
                    existing.available_workflows = body.available_workflows
                if body.workflow_overrides:
                    existing.workflow_overrides = body.workflow_overrides
                self.save(existing)
                return {"ok": True, "operation": op or "info"}
            body.render_operation = None
            self.save(body)
            return {"ok": True, "operation": op or "add_server"}
        return {"ok": False, "error": f"unknown render_operation {op!r}"}
