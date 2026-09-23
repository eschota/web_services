"""Image-generation gate for a GTX 1080 Ti converter box.

These boxes earn money converting and rigging models, and some also serve the
LLM (llama-server) for Text/Vision. Image generation only borrows the card
when all of that is idle, and hands it back the moment it is not:

* Renderfin reaches ComfyUI only through this proxy (127.0.0.1:8388 on the
  box, over an SSH tunnel from the VPS). ComfyUI itself listens on
  127.0.0.1:8389.
* The gate is OPEN only after the local converter has reported, continuously
  for SETTLE seconds, no pending or processing task, no Hunyuan work and no
  resident language model, and the box has enough free RAM.
* CLOSED answers GET /queue, POST /prompt and POST /upload/image with
  HTTP 423 {"error": "gpu_leased", "retryable": true}. Renderfin already reads
  that as "shared GPU temporarily leased": the box drops to offline for new
  dispatch and nothing is charged against a task.
* If the converter becomes busy while ComfyUI holds a prompt, the prompt is
  interrupted, the queue cleared, models unloaded, and the prompt's history
  entry deleted. The guard then answers /history/<id> with {} and hides the
  id from /queue, which is exactly how Renderfin recognises a prompt the
  worker forgot: the task goes back to Pending and runs elsewhere, without
  spending an attempt.
* Everything else (/history, /view, /object_info, /system_stats, websockets
  are not used by Renderfin) is proxied unchanged, so artifacts of a finished
  prompt stay downloadable while the gate is closed.

Standard library + aiohttp only (aiohttp ships with ComfyUI's requirements).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
from typing import Any, Dict, Optional, Set

from aiohttp import ClientSession, ClientTimeout, web

LOG = logging.getLogger("comfy1080_guard")
HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
       "te", "trailers", "transfer-encoding", "upgrade", "content-length",
       "content-encoding", "host"}
LEASED = {"error": "gpu_leased", "retryable": True,
          "reason": "converter box is busy with conversion or LLM work"}


def converter_idle(status: Dict[str, Any]) -> tuple[bool, str]:
    """Is the converter provably idle? Anything unreadable counts as busy."""
    if not isinstance(status, dict) or not status:
        return False, "no converter status"
    summary = status.get("tasks_summary") or {}
    for key in ("pending", "processing", "queue_size", "stuck"):
        try:
            if int(summary.get(key) or 0) > 0:
                return False, f"converter {key}={summary.get(key)}"
        except (TypeError, ValueError):
            return False, f"converter {key} unreadable"
    for key in ("pending_tasks", "processing_tasks"):
        value = status.get(key)
        if isinstance(value, list) and value:
            return False, f"converter {key}"
        if isinstance(value, int) and value > 0:
            return False, f"converter {key}={value}"
    hunyuan = status.get("hunyuan") or {}
    if hunyuan.get("active_task"):
        return False, "hunyuan active"
    try:
        if int(hunyuan.get("queue_size") or 0) > 0:
            return False, "hunyuan queued"
    except (TypeError, ValueError):
        return False, "hunyuan queue unreadable"
    ai = status.get("ai_models") or {}
    if ai.get("running") or ai.get("loaded_model"):
        return False, f"llm resident ({ai.get('loaded_model') or 'starting'})"
    control = status.get("workload_control") or {}
    if control.get("active"):
        return False, "workload active"
    if status.get("maintenance"):
        return False, "converter maintenance"
    return True, "idle"


def free_ram_gb() -> float:
    try:
        import ctypes

        class MEMSTAT(ctypes.Structure):
            _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                        ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                        ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong),
                        ("ullTotalVirtual", ctypes.c_ulonglong), ("ullAvailVirtual", ctypes.c_ulonglong),
                        ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]
        stat = MEMSTAT()
        stat.dwLength = ctypes.sizeof(MEMSTAT)
        ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat))
        return stat.ullAvailPhys / 1024 ** 3
    except Exception:
        return 999.0


class Guard:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.backend = f"http://127.0.0.1:{args.comfy_port}"
        self.status_url = (f"http://127.0.0.1:{args.converter_port}"
                           "/api-converter-glb/server-status")
        self.open = False
        self.reason = "starting"
        self.idle_since: Optional[float] = None
        self.preempted: Dict[str, float] = {}
        self.last_preempt_at = 0.0
        self.comfy_busy = False
        self.session: Optional[ClientSession] = None
        self.state_file = args.state_file

    # ---- state ------------------------------------------------------------
    def _hidden(self) -> Set[str]:
        now = time.time()
        self.preempted = {k: v for k, v in self.preempted.items() if now - v < 1800}
        return set(self.preempted)

    async def _comfy_queue(self) -> Optional[dict]:
        try:
            async with self.session.get(f"{self.backend}/queue",
                                        timeout=ClientTimeout(total=5)) as r:
                if r.status == 200:
                    return await r.json()
        except Exception:
            return None
        return None

    async def _post(self, path: str, body: dict) -> None:
        try:
            async with self.session.post(f"{self.backend}{path}", json=body,
                                         timeout=ClientTimeout(total=15)) as r:
                await r.read()
        except Exception as exc:
            LOG.warning("POST %s failed: %s", path, exc)

    async def preempt(self, why: str) -> None:
        queue = await self._comfy_queue() or {}
        ids = []
        for bucket in ("queue_running", "queue_pending"):
            for entry in queue.get(bucket) or []:
                if isinstance(entry, (list, tuple)) and len(entry) > 1:
                    ids.append(str(entry[1]))
        if not ids:
            return
        now = time.time()
        self.last_preempt_at = now
        for pid in ids:
            self.preempted[pid] = now
        LOG.warning("preempting %s for %s", ids, why)
        await self._post("/queue", {"clear": True})
        await self._post("/interrupt", {})
        # Wait for the worker to drop the running prompt before freeing VRAM.
        for _ in range(30):
            q = await self._comfy_queue()
            if q is not None and not q.get("queue_running"):
                break
            await asyncio.sleep(0.5)
        await self._post("/free", {"unload_models": True, "free_memory": True})
        await self._post("/history", {"delete": ids})

    async def _read_status(self) -> Dict[str, Any]:
        try:
            async with self.session.get(self.status_url,
                                        timeout=ClientTimeout(total=8)) as r:
                if r.status == 200:
                    return await r.json(content_type=None)
        except Exception:
            return {}
        return {}

    async def watch(self) -> None:
        was_busy = False
        while True:
            status = await self._read_status()
            idle, reason = converter_idle(status)
            if idle:
                ram = free_ram_gb()
                if ram < self.args.min_free_ram_gb and not self.comfy_busy:
                    idle, reason = False, f"free RAM {ram:.1f} GB"
            now = time.time()
            if idle:
                if self.idle_since is None:
                    self.idle_since = now
                should_open = now - self.idle_since >= self.args.settle
                if not should_open:
                    reason = f"settling {int(now - self.idle_since)}/{self.args.settle}s"
            else:
                self.idle_since = None
                should_open = False
            if should_open != self.open:
                LOG.info("gate %s (%s)", "OPEN" if should_open else "CLOSED", reason)
            self.open = should_open
            self.reason = reason
            # Hand the card back the moment the converter wants it.
            queue = await self._comfy_queue()
            busy_now = bool(queue and (queue.get("queue_running") or queue.get("queue_pending")))
            if not idle and busy_now:
                await self.preempt(reason)
                busy_now = False
            if was_busy and not busy_now:
                await self._post("/free", {"unload_models": True, "free_memory": False})
            was_busy = busy_now
            self.comfy_busy = busy_now
            self._write_state()
            await asyncio.sleep(self.args.poll)

    def _write_state(self) -> None:
        if not self.state_file:
            return
        try:
            with open(self.state_file, "w", encoding="ascii") as fh:
                json.dump({"open": self.open, "reason": self.reason,
                           "comfy_busy": self.comfy_busy,
                           "preempted": sorted(self._hidden()),
                           "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}, fh)
        except Exception:
            pass

    # ---- proxy ------------------------------------------------------------
    async def handle(self, request: web.Request) -> web.StreamResponse:
        path = request.path
        method = request.method
        hidden = self._hidden()
        if path == "/guard/status":
            return web.json_response({"open": self.open, "reason": self.reason,
                                      "comfy_busy": self.comfy_busy,
                                      "preempted": sorted(hidden)})
        gated = ((method == "GET" and path == "/queue")
                 or (method == "POST" and path in ("/prompt", "/api/prompt",
                                                   "/upload/image", "/api/upload/image")))
        if gated and not self.open:
            # Right after a preemption Renderfin must be able to read the
            # (filtered) queue to learn its prompt is gone; otherwise it
            # assumes the prompt is still there and holds the task.
            recent = time.time() - self.last_preempt_at < 180
            if not (method == "GET" and path == "/queue" and recent):
                return web.json_response(dict(LEASED, detail=self.reason), status=423)
        if method == "GET" and path.startswith("/history/"):
            pid = path.rsplit("/", 1)[-1]
            if pid in hidden:
                return web.json_response({})
        upstream = await self._forward(request)
        if method == "GET" and path == "/queue" and hidden and isinstance(upstream, dict):
            for bucket in ("queue_running", "queue_pending"):
                upstream[bucket] = [e for e in upstream.get(bucket) or []
                                    if not (isinstance(e, list) and len(e) > 1 and str(e[1]) in hidden)]
            return web.json_response(upstream)
        return upstream

    async def _forward(self, request: web.Request):
        url = f"{self.backend}{request.rel_url}"
        headers = {k: v for k, v in request.headers.items() if k.lower() not in HOP}
        body = await request.read()
        try:
            async with self.session.request(request.method, url, headers=headers, data=body or None,
                                            timeout=ClientTimeout(total=600),
                                            allow_redirects=False) as r:
                if request.method == "GET" and request.path == "/queue" and r.status == 200 and self._hidden():
                    return await r.json(content_type=None)
                resp = web.StreamResponse(status=r.status, headers={
                    k: v for k, v in r.headers.items() if k.lower() not in HOP})
                await resp.prepare(request)
                async for chunk in r.content.iter_chunked(1 << 20):
                    await resp.write(chunk)
                await resp.write_eof()
                return resp
        except Exception as exc:
            return web.json_response({"error": "comfy_backend_unavailable", "retryable": True,
                                      "detail": str(exc)[:200]}, status=503)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8388)
    parser.add_argument("--comfy-port", type=int, default=8389)
    parser.add_argument("--converter-port", type=int, required=True)
    parser.add_argument("--settle", type=int, default=60)
    parser.add_argument("--poll", type=float, default=2.0)
    parser.add_argument("--min-free-ram-gb", type=float, default=10.0)
    parser.add_argument("--state-file", default="")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    guard = Guard(args)
    guard.session = ClientSession()
    app = web.Application(client_max_size=256 * 1024 * 1024)
    app.router.add_route("*", "/{tail:.*}", guard.handle)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, args.listen, args.port).start()
    LOG.info("guard on %s:%s -> comfy %s, converter %s", args.listen, args.port,
             args.comfy_port, args.converter_port)
    await guard.watch()


if __name__ == "__main__":
    asyncio.run(main())
