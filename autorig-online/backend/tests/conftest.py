"""Tests must not depend on production config, secrets or live services.

Several modules read their config at import time from `os.getenv(NAME, "/srv/autorig/...")`. On the
production host those defaults point to real secrets and live switches, so a test run there used to
read a real worker token and send a real job into the live gen3d queue. Two layers keep that out:

1. Before any test module is imported (this file is loaded first) the env vars are pointed at paths
   that do not exist, so constants evaluated at import time cannot see the real files.
2. An autouse fixture re-points the already-evaluated module constants (a module might have been
   imported earlier by another plugin, or the environment may be changed afterwards) and blocks
   TCP connections to the local production service ports.

A test that needs a file or a service builds its own under a temp dir and patches the constant.
"""

from __future__ import annotations

import os
import pathlib
import socket
import sys
import tempfile

import pytest

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

# A directory that is guaranteed to stay empty: nothing is ever created under it.
_VOID = pathlib.Path(tempfile.gettempdir()) / "autorig-tests-void-do-not-create"

# env name -> module constant that caches it (module, attribute, is_pathlib).
_FILE_ENVS = {
    "AUTORIG_EXTRA_3D_WORKERS_FILE": [("ai_vision_api", "EXTRA_3D_WORKERS_FILE", False)],
    "RENDERFIN_HUNYUAN_WORKERS_FILE": [("ai_vision_api", "WORKERS_FILE", False)],
    "AUTORIG_GEN3D_POLICY": [("ai_vision_api", "GEN3D_POLICY_FILE", True)],
    "AUTORIG_V3_DISPATCH_TOKEN_FILE": [
        ("ai_vision_api", "GEN3D_TOKEN_FILE", True),
        ("generation_fast", "TOKEN_FILE", True),
    ],
    "AUTORIG_TRIPO_ENV": [("generation_tripo", "TRIPO_ENV", True)],
    "ASTRA_ADMIN_KEY_FILE": [("astra_owner_access", "KEY_FILE", True)],
    "AUTORIG_V3_EXPORT_WORKER_KEYS": [],
    "AUTORIG_PUBLIC_CHAT_KEY_FILE": [],
    "AUTORIG_VISION_CONFIG": [],
}

# Local production services. Tests that need an HTTP peer must mock the client, not dial these.
_LIVE_PORTS = {8200, 8210, 8283}

for _name in _FILE_ENVS:
    os.environ[_name] = str(_VOID / _name.lower())
os.environ["AUTORIG_GEN3D_QUEUE_URL"] = "http://127.0.0.1:9"  # discard port, and blocked below anyway


def _is_live_port(address) -> bool:
    try:
        host, port = address[0], int(address[1])
    except Exception:
        return False
    return port in _LIVE_PORTS and host in ("127.0.0.1", "::1", "localhost", "0.0.0.0")


def _is_prod_path(value) -> bool:
    """True for an unset value or a production default; a path a test chose itself is left alone."""
    text = str(value or "").replace("\\", "/")
    return not text or text.startswith(("/srv/", "/etc/", "/root/", "/home/"))


@pytest.fixture(autouse=True)
def _isolate_from_production(monkeypatch):
    for env_name, targets in _FILE_ENVS.items():
        path = str(_VOID / env_name.lower())
        if _is_prod_path(os.environ.get(env_name)):
            monkeypatch.setenv(env_name, path)
        for module_name, attr, is_path in targets:
            module = sys.modules.get(module_name)
            if module is not None and hasattr(module, attr) and _is_prod_path(getattr(module, attr)):
                monkeypatch.setattr(module, attr, pathlib.Path(path) if is_path else path)
    module = sys.modules.get("ai_vision_api")
    if module is not None and hasattr(module, "GEN3D_QUEUE_URL"):
        monkeypatch.setattr(module, "GEN3D_QUEUE_URL", "http://127.0.0.1:9")

    real_connect = socket.socket.connect
    real_connect_ex = socket.socket.connect_ex

    def guarded_connect(self, address):
        if _is_live_port(address):
            raise ConnectionRefusedError("tests must not talk to a live AutoRig service: %r" % (address,))
        return real_connect(self, address)

    def guarded_connect_ex(self, address):
        if _is_live_port(address):
            return 111  # ECONNREFUSED
        return real_connect_ex(self, address)

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)
    monkeypatch.setattr(socket.socket, "connect_ex", guarded_connect_ex)
    yield
