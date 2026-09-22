"""Clearing the farm queue: who may ask, and what it is allowed to stop.

Deliberately without `from __future__ import annotations`: FastAPI resolves a
dependency's annotations through the callable's own `__globals__`, which a
class instance does not have, and a stringised `Request` then looks to it like
a query parameter.
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import ai_queue_admin  # noqa: E402
from fastapi import Depends, FastAPI, HTTPException, Request  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402


class _Admin:
    """Stand-in for main.py's own admin dependency, with the same answers."""

    def __init__(self, admins=("owner@example.com",)):
        self.admins = set(admins)
        self.calls = 0

    async def __call__(self, request: Request):
        self.calls += 1
        email = request.cookies.get("session")
        if not email:
            raise HTTPException(status_code=401, detail="Authentication required")
        if email not in self.admins:
            raise HTTPException(status_code=403, detail="Admin access required")
        return email


def _client(admin: _Admin) -> TestClient:
    app = FastAPI()
    app.include_router(ai_queue_admin.build_queue_admin_router(admin))
    return TestClient(app, raise_server_exceptions=False)


class AdminGateTests(unittest.TestCase):
    def test_a_stranger_cannot_clear_the_queue(self):
        admin = _Admin()
        with patch.object(ai_queue_admin, "clear_pending_renders") as cleared:
            response = _client(admin).post("/api/ai/queue/clear")
        self.assertEqual(response.status_code, 401)
        cleared.assert_not_called()

    def test_a_signed_in_non_admin_cannot_clear_the_queue(self):
        admin = _Admin()
        with patch.object(ai_queue_admin, "clear_pending_renders") as cleared:
            response = _client(admin).post(
                "/api/ai/queue/clear", cookies={"session": "someone@example.com"})
        self.assertEqual(response.status_code, 403)
        cleared.assert_not_called()

    def test_an_admin_clears_the_queue_and_is_told_what_was_left_alone(self):
        admin = _Admin()

        async def fake_clear(base_url):
            self.assertTrue(base_url)
            return {"cancelled_int": 7, "running_untouched_int": 2, "pending_seen_int": 7}

        with patch.object(ai_queue_admin, "clear_pending_renders", new=fake_clear):
            response = _client(admin).post(
                "/api/ai/queue/clear", cookies={"session": "owner@example.com"})
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body["success_bool"])
        self.assertEqual(body["cancelled_int"], 7)
        self.assertEqual(body["running_untouched_int"], 2)

    def test_an_unreachable_render_queue_is_a_bad_gateway_not_a_crash(self):
        admin = _Admin()

        async def broken(base_url):
            raise RuntimeError("connection refused")

        with patch.object(ai_queue_admin, "clear_pending_renders", new=broken):
            response = _client(admin).post(
                "/api/ai/queue/clear", cookies={"session": "owner@example.com"})
        self.assertEqual(response.status_code, 502)


class ViewerAdminFlagTests(unittest.TestCase):
    """The soft probe only ever decides whether a button is drawn."""

    def test_no_session_cookie_is_not_an_admin_and_asks_the_database_nothing(self):
        app = FastAPI()

        @app.get("/probe")
        async def probe(request: Request):
            return {"admin_bool": await ai_queue_admin.viewer_is_admin(request)}

        with patch.dict(sys.modules, {}, clear=False):
            response = TestClient(app).get("/probe")
        self.assertEqual(response.json(), {"admin_bool": False})

    def test_a_broken_lookup_reports_not_admin_rather_than_failing(self):
        app = FastAPI()

        @app.get("/probe")
        async def probe(request: Request):
            return {"admin_bool": await ai_queue_admin.viewer_is_admin(request)}

        client = TestClient(app)
        client.cookies.set("session", "token")
        with patch.dict(sys.modules, {"database": None}):
            response = client.get("/probe")
        self.assertEqual(response.json(), {"admin_bool": False})


class ClearPendingRendersTests(unittest.TestCase):
    """The proxy asks renderfin for the one operation that spares running work."""

    def test_it_calls_the_cancel_pending_endpoint_and_reports_its_counts(self):
        import asyncio

        seen = {}

        class _Response:
            status_code = 200

            def raise_for_status(self):
                return None

            def json(self):
                return {"cancelled_int": 3, "running_untouched_int": 1,
                        "pending_seen_int": 3, "extra": "ignored"}

        class _Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def post(self, url, timeout=None):
                seen["url"] = url
                return _Response()

        with patch.object(ai_queue_admin.httpx, "AsyncClient", _Client):
            result = asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
                ai_queue_admin.clear_pending_renders("http://127.0.0.1:8210/renderfin/"))
        self.assertEqual(seen["url"], "http://127.0.0.1:8210/renderfin/api-render/cancel-pending")
        self.assertEqual(result, {"cancelled_int": 3, "running_untouched_int": 1,
                                  "pending_seen_int": 3})


if __name__ == "__main__":
    unittest.main()
