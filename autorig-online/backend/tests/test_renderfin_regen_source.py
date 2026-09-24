"""Where a regen's source picture comes from (renderfin/regen_source.py).

No network: every HTTP answer comes from an httpx.MockTransport, and every
main-app cache root is a temp directory.
"""
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from PIL import Image, ImageDraw

from renderfin import config, regen_source

from test_renderfin_character_gen import run

TASK_ID = "c8691854-bdd2-4503-9281-fdc8cafdb0d7"
INTERNAL = "http://main.invalid"
PUBLIC = "https://site.invalid"


def _glb(body: bytes = b"\x00" * 64) -> bytes:
    return b"glTF" + (2).to_bytes(4, "little") + (12 + len(body)).to_bytes(4, "little") + body


def _picture(width=720, height=360, fmt="JPEG") -> bytes:
    image = Image.new("RGB", (width, height), (30, 60, 90))
    ImageDraw.Draw(image).rectangle((width // 3, 10, 2 * width // 3, height - 10), fill=(200, 150, 100))
    buffer = io.BytesIO()
    image.save(buffer, fmt)
    return buffer.getvalue()


class _Roots:
    def __init__(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.glb_cache = root / "glb_cache"
        self.artifacts = root / "artifact-cache"
        self.preflight = root / "preflight"
        self.scratch = root / "tmp"
        for path in (self.glb_cache, self.artifacts, self.preflight, self.scratch):
            path.mkdir(parents=True)
        self.patches = [
            patch.object(config, "MAIN_GLB_CACHE_DIR", self.glb_cache),
            patch.object(config, "MAIN_ARTIFACT_CACHE_DIR", self.artifacts),
            patch.object(config, "PREFLIGHT_RENDER_DIR", self.preflight),
            patch.object(config, "MAIN_APP_INTERNAL_URL", INTERNAL),
            patch.object(config, "MAIN_APP_PUBLIC_URL", PUBLIC),
        ]

    def __enter__(self):
        for p in self.patches:
            p.start()
        return self

    def __exit__(self, *exc):
        for p in self.patches:
            p.stop()
        self.tmp.cleanup()


def _client(handler):
    return httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=True)


def _refuse(request):
    raise AssertionError(f"unexpected HTTP request to {request.url}")


class NormalizeTests(unittest.TestCase):
    def test_task_ids_are_canonical_uuids_or_refused(self):
        self.assertEqual(regen_source.normalize_task_id(TASK_ID.upper()), TASK_ID)
        self.assertEqual(regen_source.normalize_task_id(f"  {TASK_ID} "), TASK_ID)
        for bad in ("", None, "../../etc/passwd", "c8691854", f"{TASK_ID}/x", "rgx:" + TASK_ID):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    regen_source.normalize_task_id(bad)

    def test_views(self):
        self.assertEqual(regen_source.normalize_view(""), "front")
        self.assertEqual(regen_source.normalize_view(" Back "), "back")
        with self.assertRaises(ValueError):
            regen_source.normalize_view("top_side_45")

    def test_candidates_are_built_from_config_and_the_id_only(self):
        with _Roots() as roots:
            glbs = regen_source.glb_candidates(TASK_ID)
            self.assertEqual(glbs[0].path, roots.glb_cache / f"{TASK_ID}_prepared.glb")
            self.assertEqual(
                [c.url for c in glbs[1:]],
                [
                    f"{INTERNAL}/api/task/{TASK_ID}/prepared.glb",
                    f"{INTERNAL}/api/task/{TASK_ID}/model.glb",
                    f"{PUBLIC}/api/task/{TASK_ID}/prepared.glb",
                    f"{PUBLIC}/api/task/{TASK_ID}/model.glb",
                ],
            )
            posters = regen_source.poster_candidates(TASK_ID)
            self.assertEqual(
                [c.url or c.path for c in posters],
                [
                    f"{INTERNAL}/api/thumb/{TASK_ID}",
                    f"{PUBLIC}/api/thumb/{TASK_ID}",
                    roots.preflight / f"{TASK_ID}.jpg",
                ],
            )


class FetchGlbTests(unittest.TestCase):
    def test_the_local_cache_is_used_before_any_http(self):
        async def scenario():
            with _Roots() as roots:
                cached = roots.glb_cache / f"{TASK_ID}_prepared.glb"
                cached.write_bytes(_glb())
                async with _client(_refuse) as client:
                    path, origin = await regen_source.fetch_glb(
                        client, TASK_ID, roots.scratch / "m.glb"
                    )
                self.assertEqual((path, origin), (cached, "glb_cache prepared"))

        run(scenario())

    def test_a_corrupt_cache_entry_is_skipped(self):
        async def scenario():
            with _Roots() as roots:
                (roots.glb_cache / f"{TASK_ID}_prepared.glb").write_bytes(b"glTF-truncated")

                def handler(request):
                    if request.url.path.endswith("/prepared.glb") and request.url.host == "main.invalid":
                        return httpx.Response(200, content=_glb(b"\x01" * 32))
                    return httpx.Response(404)

                dest = roots.scratch / "m.glb"
                async with _client(handler) as client:
                    path, origin = await regen_source.fetch_glb(client, TASK_ID, dest)
                self.assertEqual((path, origin), (dest, "main app prepared.glb"))
                self.assertEqual(dest.read_bytes(), _glb(b"\x01" * 32))
                self.assertEqual(list(roots.scratch.glob(".*part")), [])

        run(scenario())

    def test_an_x_accel_answer_is_read_from_the_cache_it_names(self):
        """A cached file comes back from 127.0.0.1 as an EMPTY 200 for nginx
        to fill in; taken at face value it is a zero-byte model."""

        async def scenario():
            with _Roots() as roots:
                viewer = roots.glb_cache / f"{TASK_ID}_prepared_viewer.glb"
                viewer.write_bytes(_glb())
                member = roots.artifacts / TASK_ID / "files" / "guid.glb"
                member.parent.mkdir(parents=True)
                member.write_bytes(_glb(b"\x02" * 16))

                def handler(request):
                    if request.url.path.endswith("/prepared.glb"):
                        return httpx.Response(
                            200, headers={"X-Accel-Redirect": f"/_autorig_glb_cache/{viewer.name}"}
                        )
                    return httpx.Response(404)

                async with _client(handler) as client:
                    path, origin = await regen_source.fetch_glb(
                        client, TASK_ID, roots.scratch / "m.glb"
                    )
                self.assertEqual((path, origin), (viewer, "main app prepared.glb"))

                def artifact_handler(request):
                    if request.url.path.endswith("/model.glb"):
                        return httpx.Response(
                            200,
                            headers={
                                "X-Accel-Redirect": f"/_autorig_artifacts/{TASK_ID}/files/guid.glb"
                            },
                        )
                    return httpx.Response(404)

                async with _client(artifact_handler) as client:
                    path, origin = await regen_source.fetch_glb(
                        client, TASK_ID, roots.scratch / "m.glb"
                    )
                self.assertEqual((path, origin), (member, "main app model.glb"))

        run(scenario())

    def test_an_x_accel_uri_cannot_leave_its_root(self):
        with _Roots() as roots:
            outside = roots.glb_cache.parent / "secret.glb"
            outside.write_bytes(_glb())
            for uri in (
                "/_autorig_glb_cache/../secret.glb",
                "/_autorig_glb_cache/%2e%2e/secret.glb",
                "/_autorig_artifacts/../secret.glb",
                "/_autorig_task_cache/secret.glb",
                "/etc/passwd",
            ):
                with self.subTest(uri=uri):
                    self.assertIsNone(regen_source.accel_local_path(uri))

    def test_an_html_page_or_an_oversized_body_is_not_a_model(self):
        async def scenario():
            with _Roots() as roots:
                def handler(request):
                    if request.url.host == "main.invalid":
                        return httpx.Response(200, text="<html>viewer</html>")
                    return httpx.Response(200, content=_glb(b"\x00" * 4096))

                dest = roots.scratch / "m.glb"
                with patch.object(config, "REGEN_MAX_GLB_BYTES", 1024):
                    async with _client(handler) as client:
                        with self.assertRaises(regen_source.RegenSourceError) as raised:
                            await regen_source.fetch_glb(client, TASK_ID, dest)
                self.assertNotIsInstance(raised.exception, regen_source.RegenSourceUnavailable)
                self.assertFalse(dest.exists())
                self.assertEqual(list(roots.scratch.iterdir()), [])

        run(scenario())

    def test_not_found_everywhere_means_no_model(self):
        async def scenario():
            with _Roots() as roots:
                async with _client(lambda request: httpx.Response(404)) as client:
                    with self.assertRaises(regen_source.RegenSourceError) as raised:
                        await regen_source.fetch_glb(client, TASK_ID, roots.scratch / "m.glb")
                self.assertNotIsInstance(raised.exception, regen_source.RegenSourceUnavailable)
                self.assertIn("no model GLB", str(raised.exception))

        run(scenario())

    def test_an_unreachable_main_app_is_transient_and_worded_neutrally(self):
        async def scenario():
            with _Roots() as roots:
                def handler(request):
                    if request.url.host == "main.invalid":
                        raise httpx.ConnectError("connection refused", request=request)
                    return httpx.Response(502, text="Bad Gateway")

                async with _client(handler) as client:
                    with self.assertRaises(regen_source.RegenSourceUnavailable) as raised:
                        await regen_source.fetch_glb(client, TASK_ID, roots.scratch / "m.glb")
                message = str(raised.exception)
                self.assertIn("main app prepared.glb: ConnectError", message)
                self.assertIn("site prepared.glb: status 502", message)
                # the retry loop revives jobs whose error reads like a stale
                # farm token; a regen source outage must never look like one
                self.assertNotIn("http ", message.lower())

        run(scenario())


class FetchPosterTests(unittest.TestCase):
    def test_the_gallery_poster_comes_first(self):
        async def scenario():
            with _Roots() as roots:
                (roots.preflight / f"{TASK_ID}.jpg").write_bytes(_picture(300, 300))

                def handler(request):
                    if request.url.host == "main.invalid":
                        return httpx.Response(200, content=_picture())
                    return httpx.Response(404)

                async with _client(handler) as client:
                    data, origin = await regen_source.fetch_poster(client, TASK_ID)
                self.assertEqual(origin, "main app thumb")
                self.assertEqual(data, _picture())

        run(scenario())

    def test_the_preflight_render_stands_in_when_no_poster_is_served(self):
        async def scenario():
            with _Roots() as roots:
                (roots.preflight / f"{TASK_ID}.jpg").write_bytes(_picture(300, 300))
                async with _client(lambda request: httpx.Response(404)) as client:
                    data, origin = await regen_source.fetch_poster(client, TASK_ID)
                self.assertEqual(origin, "preflight render")

        run(scenario())

    def test_a_cached_poster_is_read_from_the_artifact_cache(self):
        async def scenario():
            with _Roots() as roots:
                poster = roots.artifacts / TASK_ID / "poster.jpg"
                poster.parent.mkdir(parents=True)
                poster.write_bytes(_picture())

                def handler(request):
                    return httpx.Response(
                        200, headers={"X-Accel-Redirect": f"/_autorig_artifacts/{TASK_ID}/poster.jpg"}
                    )

                async with _client(handler) as client:
                    data, origin = await regen_source.fetch_poster(client, TASK_ID)
                self.assertEqual((data, origin), (_picture(), "main app thumb"))

        run(scenario())

    def test_a_tiny_or_broken_poster_is_passed_over(self):
        async def scenario():
            with _Roots():
                def handler(request):
                    if request.url.host == "main.invalid":
                        return httpx.Response(200, content=_picture(64, 64))
                    return httpx.Response(200, content=b"not an image")

                async with _client(handler) as client:
                    with self.assertRaises(regen_source.RegenSourceError) as raised:
                        await regen_source.fetch_poster(client, TASK_ID)
                self.assertNotIsInstance(raised.exception, regen_source.RegenSourceUnavailable)
                self.assertIn("neither a model nor a poster", str(raised.exception))

        run(scenario())

    def test_an_unreachable_poster_is_transient(self):
        async def scenario():
            with _Roots():
                def handler(request):
                    raise httpx.ReadTimeout("slow", request=request)

                async with _client(handler) as client:
                    with self.assertRaises(regen_source.RegenSourceUnavailable):
                        await regen_source.fetch_poster(client, TASK_ID)

        run(scenario())


class PictureTests(unittest.TestCase):
    def test_a_poster_is_letterboxed_onto_the_still_backdrop(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "src.png"
            regen_source.poster_to_png(_picture(720, 360), out, size=1024)
            with Image.open(out) as still:
                self.assertEqual((still.format, still.size), ("PNG", (1024, 1024)))
                rgb = still.convert("RGB")
                self.assertEqual(rgb.getpixel((512, 5)), (127, 127, 127))  # bar
                self.assertNotEqual(rgb.getpixel((512, 512)), (127, 127, 127))  # poster
            self.assertEqual([p.name for p in Path(tmp).iterdir()], ["src.png"])

    def test_a_small_poster_is_scaled_up_to_fill_the_frame(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "src.png"
            regen_source.poster_to_png(_picture(200, 400, fmt="PNG"), out, size=1024)
            with Image.open(out) as still:
                rgb = still.convert("RGB")
                # 200x400 fits as 512x1024: the poster reaches the top edge
                self.assertNotEqual(rgb.getpixel((512, 0)), (127, 127, 127))
                self.assertEqual(rgb.getpixel((100, 512)), (127, 127, 127))

    def test_check_still(self):
        with tempfile.TemporaryDirectory() as tmp:
            good = Path(tmp) / "good.png"
            Image.open(io.BytesIO(_picture(512, 512, fmt="PNG"))).save(good)
            regen_source.check_still(good)

            blank = Path(tmp) / "blank.png"
            Image.new("RGB", (1024, 1024), (127, 127, 127)).save(blank)
            small = Path(tmp) / "small.png"
            Image.open(io.BytesIO(_picture(128, 128, fmt="PNG"))).save(small)
            broken = Path(tmp) / "broken.png"
            broken.write_bytes(b"\x89PNG not really")
            for path, reason in ((blank, "blank"), (small, "too small"), (broken, "readable")):
                with self.subTest(path=path.name):
                    with self.assertRaises(regen_source.RegenSourceError) as raised:
                        regen_source.check_still(path)
                    self.assertIn(reason, str(raised.exception))
                    self.assertNotIsInstance(
                        raised.exception, regen_source.RegenSourceUnavailable
                    )


if __name__ == "__main__":
    unittest.main()
