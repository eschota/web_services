"""Astra's owner access (astra_owner_access.py): only local, only signed, only once, only the bound method + path,
only user id 2. Run from autorig-online/backend:
    python -m unittest discover -s tests -p test_astra_owner_access.py     (pytest works too)"""
import asyncio
import importlib
import os
import pathlib
import sys
import tempfile
import time
import unittest
from types import SimpleNamespace

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

KEY = b"k" * 48
TURN = "0123456789ab"


def scope(method="GET", path="/api/admin/users", query=b""):
    return {"method": method, "path": path, "raw_path": path.encode(), "query_string": query}


def headers(token, **extra):
    h = {"x-astra-owner": token}
    h.update({k.replace("_", "-"): v for k, v in extra.items()})
    return h


class FakeDB:
    def __init__(self, user):
        self.user = user
        self.asked = []

    async def get(self, model, ident):
        self.asked.append(ident)
        return self.user


def fake_request(tok, host="127.0.0.1"):
    return SimpleNamespace(headers=headers(tok) if tok else {}, scope=scope(), method="GET",
                           client=SimpleNamespace(host=host), state=SimpleNamespace())


class AstraOwnerAccess(unittest.TestCase):
    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())
        (self.tmp / "astra-admin.key").write_bytes(KEY + b"\n")
        os.environ["ASTRA_ADMIN_KEY_FILE"] = str(self.tmp / "astra-admin.key")
        os.environ["ASTRA_ADMIN_AUDIT_FILE"] = str(self.tmp / "audit.jsonl")
        import astra_owner_access
        self.a = importlib.reload(astra_owner_access)
        self.a.PROCESS_STARTED = time.time() - 60

    def tearDown(self):
        os.environ.pop("ASTRA_ADMIN_KEY_FILE", None)
        os.environ.pop("ASTRA_ADMIN_AUDIT_FILE", None)

    def test_valid_token_once(self):
        a = self.a
        tok = a.make_token(KEY, "GET", "/api/admin/users?page=2", TURN)
        self.assertEqual(a.verify(scope(query=b"page=2"), headers(tok), "127.0.0.1"), TURN)
        with self.assertRaisesRegex(a.Rejected, "replayed"):
            a.verify(scope(query=b"page=2"), headers(tok), "127.0.0.1")

    def test_through_nginx_is_rejected(self):
        a = self.a
        for extra in ({"x_forwarded_for": "1.2.3.4"}, {"x_real_ip": "1.2.3.4"}, {"forwarded": "for=1.2.3.4"}):
            tok = a.make_token(KEY, "GET", "/api/admin/users", TURN)
            with self.assertRaisesRegex(a.Rejected, "proxy"):
                a.verify(scope(), headers(tok, **extra), "127.0.0.1")
        tok = a.make_token(KEY, "GET", "/api/admin/users", TURN)
        with self.assertRaisesRegex(a.Rejected, "local"):
            a.verify(scope(), headers(tok), "203.0.113.9")

    def test_expired_and_future_tokens(self):
        a = self.a
        old = a.make_token(KEY, "GET", "/api/admin/users", TURN, ts=int(time.time()) - a.TTL_SECONDS - 5)
        with self.assertRaisesRegex(a.Rejected, "expired"):
            a.verify(scope(), headers(old), "127.0.0.1")
        future = a.make_token(KEY, "GET", "/api/admin/users", TURN, ts=int(time.time()) + 600)
        with self.assertRaisesRegex(a.Rejected, "expired"):
            a.verify(scope(), headers(future), "127.0.0.1")

    def test_token_from_before_the_process_started(self):
        a = self.a
        a.PROCESS_STARTED = time.time()
        tok = a.make_token(KEY, "GET", "/api/admin/users", TURN, ts=int(time.time()) - 30)
        with self.assertRaisesRegex(a.Rejected, "expired"):
            a.verify(scope(), headers(tok), "127.0.0.1")

    def test_method_and_path_binding(self):
        a = self.a
        tok = a.make_token(KEY, "GET", "/api/admin/users", TURN)
        with self.assertRaisesRegex(a.Rejected, "signature"):
            a.verify(scope(path="/api/admin/settings"), headers(tok), "127.0.0.1")
        with self.assertRaisesRegex(a.Rejected, "signature"):
            a.verify(scope(method="POST"), headers(tok), "127.0.0.1")
        with self.assertRaisesRegex(a.Rejected, "signature"):
            a.verify(scope(query=b"user=1"), headers(tok), "127.0.0.1")
        other = a.make_token(b"x" * 48, "GET", "/api/admin/users", TURN)
        with self.assertRaisesRegex(a.Rejected, "signature"):
            a.verify(scope(), headers(other), "127.0.0.1")

    def test_forged_and_unconfigured(self):
        a = self.a
        with self.assertRaisesRegex(a.Rejected, "malformed"):
            a.verify(scope(), headers("v1.123.abc"), "127.0.0.1")
        a.KEY_FILE = self.tmp / "missing.key"
        tok = a.make_token(KEY, "GET", "/api/admin/users", TURN)
        with self.assertRaisesRegex(a.Rejected, "not configured"):
            a.verify(scope(), headers(tok), "127.0.0.1")

    def test_audit_has_no_query_values(self):
        a = self.a
        tok = a.make_token(KEY, "GET", "/api/language/resolve?email=someone@example.com", TURN)
        a.verify(scope(path="/api/language/resolve", query=b"email=someone@example.com"), headers(tok), "127.0.0.1")
        text = a.AUDIT_FILE.read_text()
        self.assertNotIn("someone@example.com", text)
        self.assertIn('"email"', text)
        self.assertIn(TURN, text)

    def test_resolve_owner_maps_to_user_2_only(self):
        from fastapi import HTTPException
        a = self.a
        owner = SimpleNamespace(id=2, email="owner@example.com")
        db = FakeDB(owner)
        self.assertIsNone(asyncio.run(a.resolve_owner(fake_request(""), db, object, lambda e: True)))
        req = fake_request(a.make_token(KEY, "GET", "/api/admin/users", TURN))
        self.assertIs(asyncio.run(a.resolve_owner(req, db, object, lambda e: True)), owner)
        self.assertIs(asyncio.run(a.resolve_owner(req, db, object, lambda e: True)), owner)  # cached: nonce once
        self.assertEqual(db.asked, [2])
        bad = fake_request(a.make_token(KEY, "GET", "/api/admin/users", TURN))
        with self.assertRaises(HTTPException) as exc:
            asyncio.run(a.resolve_owner(bad, FakeDB(owner), object, lambda e: False))   # not an admin: refused
        self.assertEqual(exc.exception.status_code, 401)
        with self.assertRaises(HTTPException):
            asyncio.run(a.resolve_owner(fake_request("v1.1.2.3.4"), db, object, lambda e: True))


if __name__ == "__main__":
    unittest.main()
