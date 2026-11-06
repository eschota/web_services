"""Site modes and the adult-domain gate (site_mode.py, geo_country.py), 2026-10-11.

Run from backend/: python3 -m unittest tests.test_site_mode
"""
import asyncio
import ipaddress
import json
import os
import struct
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import geo_country  # noqa: E402
import site_mode  # noqa: E402


# ----------------------------------------------------------------- a tiny MMDB writer
class _Enc:
    def __init__(self):
        self.buf = bytearray()
        self.strings = {}

    def _ctrl(self, typ, size):
        out = bytearray()
        first_type = typ if typ <= 7 else 0
        if size < 29:
            out.append((first_type << 5) | size)
            ext = b""
        elif size < 285:
            out.append((first_type << 5) | 29)
            ext = bytes([size - 29])
        else:
            out.append((first_type << 5) | 30)
            ext = struct.pack(">H", size - 285)
        if typ > 7:
            out.append(typ - 7)
        out += ext
        return bytes(out)

    def enc(self, v):
        if isinstance(v, str):
            if v in self.strings and self.strings[v] < 2048:     # exercise the pointer path
                off = self.strings[v]
                self.buf += bytes([(1 << 5) | (off >> 8), off & 0xFF])
                return
            self.strings[v] = len(self.buf)
            raw = v.encode()
            self.buf += self._ctrl(2, len(raw)) + raw
        elif isinstance(v, bool):
            self.buf += self._ctrl(14, int(v))
        elif isinstance(v, int):
            raw = v.to_bytes(8, "big").lstrip(b"\x00")
            self.buf += self._ctrl(9 if v >= 2 ** 32 else 6, len(raw)) + raw
        elif isinstance(v, dict):
            self.buf += self._ctrl(7, len(v))
            for k, val in v.items():
                self.enc(k)
                self.enc(val)
        elif isinstance(v, list):
            self.buf += self._ctrl(11, len(v))
            for val in v:
                self.enc(val)
        else:
            raise TypeError(v)


def build_mmdb(networks, record_size=24):
    nodes = [[None, None]]
    leaves = {}
    data = _Enc()
    for cidr, rec in networks:
        net = ipaddress.ip_network(cidr)
        key = json.dumps(rec, sort_keys=True)
        if key not in leaves:
            leaves[key] = len(data.buf)
            data.enc(rec)
        bits = int(net.network_address)
        node = 0
        for depth in range(net.prefixlen):
            bit = (bits >> (31 - depth)) & 1
            if depth == net.prefixlen - 1:
                nodes[node][bit] = ("data", leaves[key])
            else:
                nxt = nodes[node][bit]
                if not (isinstance(nxt, tuple) and nxt[0] == "node"):
                    nodes.append([None, None])
                    nodes[node][bit] = ("node", len(nodes) - 1)
                node = nodes[node][bit][1]
    count = len(nodes)
    tree = bytearray()
    for left, right in nodes:
        vals = []
        for r in (left, right):
            if r is None:
                vals.append(count)
            elif r[0] == "node":
                vals.append(r[1])
            else:
                vals.append(count + 16 + r[1])
        if record_size == 24:
            tree += vals[0].to_bytes(3, "big") + vals[1].to_bytes(3, "big")
        elif record_size == 28:
            tree += (vals[0] & 0xFFFFFF).to_bytes(3, "big")
            tree.append(((vals[0] >> 24) << 4) | (vals[1] >> 24))
            tree += (vals[1] & 0xFFFFFF).to_bytes(3, "big")
        else:
            tree += struct.pack(">II", vals[0], vals[1])
    meta = _Enc()
    meta.enc({"node_count": count, "record_size": record_size, "ip_version": 4,
              "database_type": "Test-City", "binary_format_major_version": 2,
              "binary_format_minor_version": 0, "build_epoch": 1760000000, "languages": ["en"]})
    return bytes(tree) + b"\x00" * 16 + bytes(data.buf) + b"\xab\xcd\xefMaxMind.com" + bytes(meta.buf)


NETS = [
    ("81.2.69.0/24", {"country": {"iso_code": "GB"}}),
    ("8.8.8.0/24", {"country": {"iso_code": "US"}, "subdivisions": [{"names": {"en": "California"}}]}),
    ("9.9.9.0/24", {"country": {"iso_code": "US"}, "subdivisions": [{"names": {"en": "Texas"}}]}),
    ("10.0.0.0/8", {"country": {"iso_code": "US"}}),
    ("5.6.7.0/24", {"country": {"iso_code": "NL"}, "subdivisions": [{"iso_code": "NH"}]}),
]


class GeoReaderTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    def _db(self, record_size):
        path = os.path.join(self.tmp.name, f"t{record_size}.mmdb")
        Path(path).write_bytes(build_mmdb(NETS, record_size))
        return path

    def test_lookup_every_record_size(self):
        for size in (24, 28, 32):
            path = self._db(size)
            self.assertEqual(geo_country.locate("81.2.69.160", path), ("GB", None), size)
            self.assertEqual(geo_country.locate("8.8.8.8", path), ("US", "CA"), size)
            self.assertEqual(geo_country.locate("9.9.9.9", path), ("US", "TX"), size)
            self.assertEqual(geo_country.locate("10.1.2.3", path), ("US", None), size)
            self.assertEqual(geo_country.locate("5.6.7.8", path), ("NL", "NH"), size)
            self.assertEqual(geo_country.locate("1.1.1.1", path), (None, None), size)
            self.assertEqual(geo_country.locate("::ffff:8.8.8.8", path), ("US", "CA"), size)
            self.assertEqual(geo_country.locate("not-an-ip", path), (None, None), size)

    def test_missing_database_is_unknown(self):
        self.assertEqual(geo_country.locate("8.8.8.8", os.path.join(self.tmp.name, "nope.mmdb")), (None, None))

    def test_metadata(self):
        info = geo_country.database_info(self._db(24))
        self.assertTrue(info["loaded_bool"])
        self.assertEqual(info["database_type_string"], "Test-City")


class _Cfg:
    """Point site_mode at a temporary config file and geo database."""

    def __init__(self, over=None, nets=NETS):
        self.tmp = tempfile.TemporaryDirectory()
        self.cfg = os.path.join(self.tmp.name, "site-modes.json")
        self.db = os.path.join(self.tmp.name, "geo.mmdb")
        Path(self.db).write_bytes(build_mmdb(nets))
        Path(self.cfg).write_text(json.dumps(over or {}), encoding="utf-8")

    def __enter__(self):
        self.old = (os.environ.get("AUTORIG_SITE_MODES_FILE"), os.environ.get("AUTORIG_GEOIP_MMDB"))
        os.environ["AUTORIG_SITE_MODES_FILE"] = self.cfg
        os.environ["AUTORIG_GEOIP_MMDB"] = self.db
        site_mode._CFG.update(data=None, mtime=None, path=None, checked=0.0)
        return self

    def __exit__(self, *exc):
        for key, val in zip(("AUTORIG_SITE_MODES_FILE", "AUTORIG_GEOIP_MMDB"), self.old):
            if val is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = val
        site_mode._CFG.update(data=None, mtime=None, path=None, checked=0.0)
        self.tmp.cleanup()


class GeoPolicyTest(unittest.TestCase):
    def test_blocked_and_allowed(self):
        with _Cfg():
            self.assertEqual(site_mode.geo_verdict("81.2.69.1")["reason"], "country_GB")
            self.assertEqual(site_mode.geo_verdict("9.9.9.9")["reason"], "us_state_TX")
            self.assertFalse(site_mode.geo_verdict("8.8.8.8")["blocked"])        # California: no AV law
            self.assertEqual(site_mode.geo_verdict("10.0.0.1")["reason"], "us_state_unknown")
            self.assertFalse(site_mode.geo_verdict("5.6.7.8")["blocked"])        # NL
            self.assertEqual(site_mode.geo_verdict("1.1.1.1")["reason"], "country_unknown")

    def test_unknown_can_be_allowed(self):
        with _Cfg({"geo": {"unknown_country": "allow", "us_unknown_state": "allow"}}):
            self.assertFalse(site_mode.geo_verdict("1.1.1.1")["blocked"])
            self.assertFalse(site_mode.geo_verdict("10.0.0.1")["blocked"])

    def test_defaults_cover_the_named_laws(self):
        geo = site_mode.DEFAULTS["geo"]
        for code in ("GB", "FR", "IT", "DE", "AU", "BR"):
            self.assertIn(code, geo["blocked_countries"])
        for state in ("TX", "UT", "LA", "VA", "FL", "MO", "AZ"):
            self.assertIn(state, geo["blocked_us_states"])
        self.assertNotIn("US", geo["blocked_countries"])


class PathsTest(unittest.TestCase):
    def test_section(self):
        for path in ("/nodes", "/nodes/my-graph", "/workflows", "/queue", "/lora", "/system_prompts"):
            self.assertEqual(site_mode.in_section(path), "page", path)
        for path in ("/api/ai/graphs", "/api/ai/graphs/abcdef123456", "/api/ai/graph/templates",
                     "/api/ai/civitai/post", "/api/ai/queue/items", "/api/ai/loras", "/api/ai/prompts"):
            self.assertEqual(site_mode.in_section(path), "api", path)
        for path in ("/", "/gallery", "/task", "/api/ai/loras/sync/manifest", "/api/ai/graph-edits/schema",
                     "/api/ai/status/x", "/image", "/nodesx", "/api/ai/queue/clear"):
            self.assertIsNone(site_mode.in_section(path), path)

    def test_task_ids(self):
        tid = "66ba97ba-1111-2222-3333-444455556666"
        self.assertEqual(site_mode.task_id_of("/task", "id=" + tid), tid)
        self.assertEqual(site_mode.task_id_of("/task", "task_id=" + tid), tid)
        self.assertEqual(site_mode.task_id_of("/api/task/" + tid, ""), tid)
        self.assertEqual(site_mode.task_id_of("/api/task/" + tid + "/card", ""), tid)
        self.assertEqual(site_mode.task_id_of("/thumb/" + tid, ""), tid)
        self.assertEqual(site_mode.task_id_of("/api/video/" + tid, ""), tid)
        self.assertIsNone(site_mode.task_id_of("/gallery", ""))
        self.assertIsNone(site_mode.task_id_of("/api/task/create", ""))

    def test_internal(self):
        local = {"type": "http", "client": ("127.0.0.1", 5000)}
        self.assertTrue(site_mode.is_internal(local, {"host": "127.0.0.1:8200"}))
        self.assertFalse(site_mode.is_internal(local, {"host": "autorig.online", "x-real-ip": "1.2.3.4"}))
        self.assertFalse(site_mode.is_internal(local, {"host": "127.0.0.1:8200", "x-forwarded-for": "1.2.3.4"}))
        self.assertFalse(site_mode.is_internal(local, {"host": "autorig.red"}))
        self.assertFalse(site_mode.is_internal({"client": ("8.8.8.8", 1)}, {"host": "127.0.0.1:8200"}))

    def test_safe_next(self):
        self.assertEqual(site_mode._safe_next("/nodes?g=1"), "/nodes?g=1")
        for bad in ("https://evil.example/", "//evil.example", "/\\evil", "/age-gate?next=/x", "", None):
            self.assertEqual(site_mode._safe_next(bad), "/", bad)

    def test_oauth_redirect(self):
        from starlette.requests import Request

        def req(host):
            return Request({"type": "http", "headers": [(b"host", host.encode())], "query_string": b""})

        with _Cfg({"nsfw_hosts": ["autorig.red"]}):
            self.assertIsNone(site_mode.oauth_redirect_uri(req("autorig.online"), None))
            self.assertEqual(site_mode.oauth_redirect_uri(req("autorig.red"), None),
                             "https://autorig.red/auth/callback")


# ----------------------------------------------------------------- middleware end to end
def _app():
    from fastapi import FastAPI

    app = FastAPI()

    @app.get("/")
    async def home():
        return {"ok": True}

    @app.get("/nodes")
    async def nodes():
        return {"page": "nodes"}

    @app.get("/api/ai/graphs")
    async def graphs():
        return {"graphs_array": ["secret"]}

    @app.get("/task")
    async def task(id: str = ""):
        return {"task": id}

    @app.get("/thumb/{task_id}")
    async def thumb(task_id: str):
        return {"thumb": task_id}

    app.include_router(site_mode.router)
    app.add_middleware(site_mode.SiteModeMiddleware)

    async def behind_nginx(scope, receive, send):
        # uvicorn sees nginx (and the local services) as 127.0.0.1
        if scope["type"] == "http":
            scope = dict(scope, client=("127.0.0.1", 40000))
        await app(scope, receive, send)

    return behind_nginx


class MiddlewareTest(unittest.TestCase):
    def setUp(self):
        from fastapi.testclient import TestClient

        self.saved = (site_mode.session_identity, site_mode.identity, site_mode.current_consent,
                      site_mode.task_rating)
        self.ident = None
        self.consent = None

        async def fake_session(token):
            return self.ident if token else None

        async def fake_identity(scope, headers):
            return self.ident if site_mode._cookie(headers, "session") else None

        async def fake_consent(user_id, version):
            return self.consent

        async def fake_rating(task_id):
            return "adult" if task_id.startswith("a") else "safe"

        site_mode.session_identity = fake_session
        site_mode.identity = fake_identity
        site_mode.current_consent = fake_consent
        site_mode.task_rating = fake_rating
        self.client = TestClient(_app(), base_url="https://autorig.online")

    def tearDown(self):
        (site_mode.session_identity, site_mode.identity, site_mode.current_consent,
         site_mode.task_rating) = self.saved

    def get(self, path, host="autorig.online", cookies=None, **kw):
        headers = {"host": host, "x-real-ip": "8.8.8.8", "accept": "text/html"}
        headers.update(kw.pop("headers", {}))
        if cookies:
            headers["cookie"] = "; ".join(f"{k}={v}" for k, v in cookies.items())
        return self.client.get(path, headers=headers, follow_redirects=False, **kw)

    def test_main_host_unchanged_by_default(self):
        with _Cfg():
            self.assertEqual(self.get("/nodes").json(), {"page": "nodes"})
            self.assertEqual(self.get("/api/ai/graphs").json(), {"graphs_array": ["secret"]})
            self.assertEqual(self.get("/task?id=a1234567").json(), {"task": "a1234567"})

    def test_main_split(self):
        with _Cfg({"main_split_section": True, "main_split_items": True, "nsfw_hosts": ["autorig.red"]}):
            r = self.get("/nodes")
            self.assertEqual(r.status_code, 200)
            self.assertIn("autorig.red", r.text)
            self.assertNotIn("secret", r.text)
            self.assertIn("noindex", r.headers.get("x-robots-tag", ""))
            r = self.get("/api/ai/graphs")
            self.assertEqual(r.status_code, 403)
            self.assertEqual(r.json()["detail"]["error_string"], "nsfw_domain_only")
            self.assertNotIn("secret", r.text)
            self.assertNotIn('"task"', self.get("/task?id=a1234567").text)       # adult task: neutral page
            self.assertEqual(self.get("/task?id=b1234567").json(), {"task": "b1234567"})
            self.assertEqual(self.get("/thumb/a1234567").status_code, 404)
            # internal callers (autorig-mt, surabot) keep working
            r = self.client.get("/api/ai/graphs", headers={"host": "127.0.0.1:8200"})
            self.assertEqual(r.json(), {"graphs_array": ["secret"]})

    def test_admin_staging_cookie(self):
        with _Cfg():
            self.assertEqual(self.get("/nodes", cookies={"autorig_site_mode": "main-split"}).json(),
                             {"page": "nodes"})                                   # nobody signed in: ignored
            self.ident = {"user_id": 1, "is_admin": False, "via": "session"}
            self.assertEqual(self.get("/nodes", cookies={"autorig_site_mode": "main-split", "session": "x"}).json(),
                             {"page": "nodes"})                                   # not an admin: ignored
            self.ident = {"user_id": 2, "is_admin": True, "via": "session"}
            r = self.get("/nodes", cookies={"autorig_site_mode": "main-split", "session": "x"})
            self.assertNotIn("nodes\"", r.text)
            r = self.get("/nodes", cookies={"autorig_site_mode": "nsfw", "session": "x"})
            self.assertEqual(r.status_code, 302)
            self.assertTrue(r.headers["location"].startswith("/age-gate"))

    def test_adult_host_gate(self):
        with _Cfg({"nsfw_hosts": ["autorig.red"]}):
            r = self.get("/nodes", host="autorig.red")
            self.assertEqual(r.status_code, 302)
            self.assertEqual(r.headers["location"], "/age-gate?next=%2Fnodes")
            r = self.get("/api/ai/graphs", host="autorig.red", headers={"accept": "application/json"})
            self.assertEqual(r.status_code, 401)
            self.assertNotIn("secret", r.text)
            r = self.get("/age-gate", host="autorig.red")
            self.assertEqual(r.status_code, 200)
            self.assertIn("/auth/login", r.text)                                  # sign in first
            self.assertEqual(r.headers.get("rating"), site_mode.RTA_LABEL)
            self.ident = {"user_id": 7, "is_admin": False, "via": "session"}
            r = self.get("/age-gate", host="autorig.red", cookies={"session": "x"})
            self.assertIn('name="age_confirmed"', r.text)                         # then the 18+ consent
            r = self.get("/api/ai/graphs", host="autorig.red", cookies={"session": "x"},
                         headers={"accept": "application/json"})
            self.assertEqual(r.status_code, 403)
            self.consent = {"consent_version": "2026-10-11"}
            r = self.get("/api/ai/graphs", host="autorig.red", cookies={"session": "x"})
            self.assertEqual(r.json(), {"graphs_array": ["secret"]})
            self.assertEqual(r.headers.get("rating"), site_mode.RTA_LABEL)
            self.assertEqual(self.get("/robots.txt", host="autorig.red").text, "User-agent: *\nDisallow: /\n")
            self.assertEqual(self.get("/api/age-gate/check", host="autorig.red", cookies={"session": "x"}).status_code, 204)

    def test_adult_host_geo(self):
        with _Cfg({"nsfw_hosts": ["autorig.red"]}):
            self.ident = {"user_id": 7, "is_admin": False, "via": "session"}
            self.consent = {"consent_version": "2026-10-11"}
            r = self.get("/nodes", host="autorig.red", cookies={"session": "x"}, headers={"x-real-ip": "81.2.69.5"})
            self.assertEqual(r.status_code, 451)
            self.assertNotIn("nodes\"", r.text)
            r = self.get("/api/age-gate/check", host="autorig.red", cookies={"session": "x"},
                         headers={"x-real-ip": "9.9.9.9"})
            self.assertEqual(r.status_code, 403)
            self.ident = {"user_id": 2, "is_admin": True, "via": "session"}        # the owner, abroad
            r = self.get("/nodes", host="autorig.red", cookies={"session": "x"}, headers={"x-real-ip": "81.2.69.5"})
            self.assertEqual(r.json(), {"page": "nodes"})

    def test_listing_conditions(self):
        from sqlalchemy import Column, Integer, String
        from sqlalchemy.orm import declarative_base

        Base = declarative_base()

        class T(Base):
            __tablename__ = "t"
            id = Column(Integer, primary_key=True)
            content_rating = Column(String)

        with _Cfg():
            conds = site_mode.listing_conditions(None, T)
            self.assertEqual(len(conds), 1)
            self.assertIn("content_rating", str(conds[0]))
        with _Cfg({"main_list_filter": False}):
            self.assertEqual(site_mode.listing_conditions(None, T), [])


class ConsentStoreTest(unittest.TestCase):
    def test_record_and_withdraw(self):
        tmp = tempfile.TemporaryDirectory()
        try:
            from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
            from sqlalchemy.orm import sessionmaker
        except ImportError:
            self.skipTest("sqlalchemy async not available")
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp.name}/c.db")
        local = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        import types
        fake_db = types.ModuleType("database")
        fake_db.AsyncSessionLocal = local
        saved = sys.modules.get("database")
        sys.modules["database"] = fake_db
        site_mode._TABLE_READY = False
        site_mode._CONSENT_CACHE.clear()
        try:
            async def run():
                self.assertIsNone(await site_mode.current_consent(5, "v1"))
                rec = await site_mode.record_consent(5, "v1", "NL", "autorig.red")
                self.assertEqual(rec["ip_country"], "NL")
                got = await site_mode.current_consent(5, "v1")
                self.assertEqual(got["ip_country"], "NL")
                self.assertIsNone(await site_mode.current_consent(5, "v2"))      # a new terms version asks again
                self.assertEqual(await site_mode.withdraw_consent(5), 1)
                self.assertIsNone(await site_mode.current_consent(5, "v1"))
                await engine.dispose()
            asyncio.run(run())
        finally:
            if saved is not None:
                sys.modules["database"] = saved
            else:
                sys.modules.pop("database", None)
            site_mode._TABLE_READY = False
            tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
