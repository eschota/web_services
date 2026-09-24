"""Still-image mode of the GLB renderer (tools/renderfin/glb_turntable.mjs).

The wrapper tests drive renderfin.turntable against a stand-in renderer script,
so they need no browser. RendererArgumentTests run the real script's argument
parser with node (it fails before Chrome starts). RealRenderTests need node
and Chrome or chrome-headless-shell and are skipped without them; point
RENDERFIN_TURNTABLE_CHROME or CHROME_PATH at a browser to run them.
"""
from __future__ import annotations

import asyncio
import glob
import json
import os
import shutil
import struct
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from PIL import Image, ImageChops, ImageDraw

from renderfin import config
from renderfin.turntable import (
    STILL_VIEWS,
    TurntableError,
    render_still,
    render_turntable,
    render_views,
)

APP_DIR = Path(__file__).resolve().parents[2]
RENDERER_SCRIPT = APP_DIR / "tools" / "renderfin" / "glb_turntable.mjs"
DEFAULT_T_POSE_GLB = APP_DIR / "static" / "glb" / "default_t_pose.glb"
GREY = (127, 127, 127)


def _run(coro):
    return asyncio.run(coro)


def _find_node() -> str:
    return shutil.which(config.TURNTABLE_NODE) or shutil.which("node") or ""


def _find_chrome() -> str:
    # Same order the script uses, then the Playwright browser caches.
    candidates = [
        config.TURNTABLE_CHROME,
        os.environ.get("CHROME_PATH", ""),
        os.environ.get("CHROME_HEADLESS_SHELL", ""),
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
        "/usr/bin/chrome-headless-shell",
        "/opt/chrome-headless-shell/chrome-headless-shell",
    ]
    for root in ("/opt/pw-browsers", os.path.expanduser("~/.cache/ms-playwright")):
        candidates += sorted(glob.glob(f"{root}/chromium_headless_shell-*/chrome-linux/headless_shell"))
        candidates += sorted(glob.glob(f"{root}/chromium-*/chrome-linux/chrome"))
    return next((path for path in candidates if path and os.path.isfile(path)), "")


NODE = _find_node()
CHROME = _find_chrome()


FAKE_RENDERER = textwrap.dedent(
    '''
    """Stand-in for glb_turntable.mjs: records its argv, then (mis)behaves on request."""
    import json
    import os
    import subprocess
    import sys
    import time
    from pathlib import Path

    args = sys.argv[1:]
    Path(os.environ["FAKE_RENDER_ARGV"]).write_text(json.dumps(args))
    options = dict(zip(args[0::2], args[1::2]))
    mode = os.environ.get("FAKE_RENDER_MODE", "ok")
    if mode == "exit":
        print("[glb_turntable] harness failed: chrome crashed while rendering", flush=True)
        sys.exit(3)
    if mode == "hang":
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
        Path(os.environ["FAKE_RENDER_PIDS"]).write_text(json.dumps([os.getpid(), child.pid]))
        time.sleep(60)
    if mode == "silent":
        sys.exit(0)

    output = Path(options["--output"])
    if "--views" in options:
        targets = [output.with_name(f"{output.stem}_{view}.png") for view in options["--views"].split(",")]
    elif "--still" in options:
        targets = [output]
    else:
        output.write_bytes(os.urandom(4096))  # turntable: any non-trivial file
        sys.exit(0)

    from PIL import Image, ImageDraw

    size = int(options["--size"])
    transparent = options.get("--background") == "transparent"

    def picture(side, *, alpha=transparent, blank=False):
        image = Image.new("RGBA" if alpha else "RGB", (side, side), (0, 0, 0, 0) if alpha else (127, 127, 127))
        if not blank:
            box = (side // 4, side // 8, side * 3 // 4, side * 7 // 8)
            ImageDraw.Draw(image).rectangle(box, fill=(200, 60, 60, 255) if alpha else (200, 60, 60))
        return image

    def noise(side):
        return Image.frombytes("RGB", (side, side), os.urandom(side * side * 3))

    for number, target in enumerate(targets):
        if mode == "ok" or (mode == "partial" and number == 0):
            picture(size).save(target, "PNG")
        elif mode == "tiny":
            target.write_bytes(b"\\x89PNG\\r\\n\\x1a\\n")
        elif mode == "jpeg":
            noise(size).save(target, "JPEG")
        elif mode == "wrong_size":
            picture(size // 2).save(target, "PNG")
        elif mode == "truncated":
            noise(size).save(target, "PNG")
            data = target.read_bytes()
            target.write_bytes(data[: len(data) * 3 // 5])
        elif mode == "blank":
            picture(size, blank=True).save(target, "PNG")
        elif mode == "no_alpha":
            picture(size, alpha=False).save(target, "PNG")
        print(f"[glb_turntable] wrote {target}", flush=True)
    '''
)


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    try:  # killed but not yet reaped: a zombie is as dead as it gets
        with open(f"/proc/{pid}/stat", encoding="ascii", errors="replace") as handle:
            return handle.read().rsplit(")", 1)[1].split()[0] != "Z"
    except (OSError, IndexError):
        return True


class StillRenderWrapperTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = Path(tmp.name)
        self.script = self.tmp / "fake_renderer.py"
        self.script.write_text(FAKE_RENDERER, encoding="utf-8")
        self.glb = self.tmp / "hero.glb"
        self.glb.write_bytes(b"glTF: the fake renderer never parses this")
        self.argv_file = self.tmp / "argv.json"
        self.pids_file = self.tmp / "pids.json"
        patchers = (
            patch.multiple(
                config,
                TURNTABLE_NODE=sys.executable,
                TURNTABLE_SCRIPT=str(self.script),
                TURNTABLE_CHROME="",
                TURNTABLE_FFMPEG="ffmpeg",
                TURNTABLE_SECONDS=6.0,
                TURNTABLE_TIMEOUT_SECONDS=60.0,
            ),
            patch.dict(
                os.environ,
                {
                    "FAKE_RENDER_ARGV": str(self.argv_file),
                    "FAKE_RENDER_PIDS": str(self.pids_file),
                    "FAKE_RENDER_MODE": "ok",
                },
            ),
        )
        for patcher in patchers:
            patcher.start()
            self.addCleanup(patcher.stop)

    def set_mode(self, mode: str) -> None:
        os.environ["FAKE_RENDER_MODE"] = mode

    def argv(self) -> list[str]:
        return json.loads(self.argv_file.read_text())

    def test_render_still_defaults_to_a_grey_orthographic_1024_front_view(self):
        out = self.tmp / "renders" / "front.png"

        self.assertEqual(_run(render_still(self.glb, out)), out)

        self.assertEqual(
            self.argv(),
            [
                "--glb", str(self.glb),
                "--output", str(out),
                "--still", "front",
                "--size", "1024",
                "--background", "#7f7f7f",
                "--ortho", "1",
                "--margin", "0.08",
            ],
        )
        with Image.open(out) as image:
            self.assertEqual((image.format, image.size), ("PNG", (1024, 1024)))

    def test_render_still_passes_its_options_and_the_configured_chrome(self):
        out = self.tmp / "angle.png"
        with patch.object(config, "TURNTABLE_CHROME", "/opt/chrome/chrome"):
            _run(
                render_still(
                    self.glb,
                    out,
                    view="top_side_45",
                    size=512,
                    background=" #A0B1C2 ",
                    ortho=False,
                    margin=0.125,
                )
            )

        self.assertEqual(
            self.argv()[4:],
            [
                "--still", "top_side_45",
                "--size", "512",
                "--background", "#a0b1c2",
                "--ortho", "0",
                "--margin", "0.125",
                "--chrome", "/opt/chrome/chrome",
            ],
        )

    def test_render_views_names_each_file_after_the_glb_stem(self):
        out_dir = self.tmp / "views"

        result = _run(render_views(self.glb, out_dir, views=("front", "left")))

        self.assertEqual(
            result,
            {"front": out_dir / "hero_front.png", "left": out_dir / "hero_left.png"},
        )
        self.assertEqual(list(result), ["front", "left"])
        self.assertEqual(
            self.argv()[:6],
            ["--glb", str(self.glb), "--output", str(out_dir / "hero.png"), "--views", "front,left"],
        )
        self.assertTrue(all(path.is_file() for path in result.values()))
        self.assertFalse((out_dir / "hero.png").exists())

    def test_render_views_defaults_and_transparent_background(self):
        result = _run(render_views(self.glb, self.tmp, background="transparent"))

        self.assertEqual(list(result), ["front", "back", "left", "right"])
        argv = self.argv()
        self.assertEqual(argv[argv.index("--views") + 1], "front,back,left,right")
        self.assertEqual(argv[argv.index("--background") + 1], "transparent")
        with Image.open(result["back"]) as image:
            self.assertEqual(image.mode, "RGBA")

    def test_render_views_accepts_a_comma_separated_string(self):
        result = _run(render_views(self.glb, self.tmp, views="back, top_side_45"))

        self.assertEqual(list(result), ["back", "top_side_45"])

    def test_turntable_command_is_unchanged(self):
        out = self.tmp / "hero_turntable.mp4"

        self.assertEqual(_run(render_turntable(self.glb, out)), out)

        self.assertEqual(
            self.argv(),
            ["--glb", str(self.glb), "--output", str(out), "--seconds", "6.0", "--ffmpeg", "ffmpeg"],
        )

    def test_invalid_arguments_fail_before_anything_is_spawned(self):
        out = self.tmp / "x.png"
        calls = {
            "unknown view": lambda: render_still(self.glb, out, view="side"),
            "browser-only view": lambda: render_views(self.glb, self.tmp, views=("front", "bottom")),
            "repeated view": lambda: render_views(self.glb, self.tmp, views=("front", "front")),
            "no views": lambda: render_views(self.glb, self.tmp, views=()),
            "fractional size": lambda: render_still(self.glb, out, size=1000.5),
            "boolean size": lambda: render_still(self.glb, out, size=True),
            "small size": lambda: render_still(self.glb, out, size=32),
            "large size": lambda: render_still(self.glb, out, size=4096),
            "named colour": lambda: render_still(self.glb, out, background="white"),
            "short hex": lambda: render_still(self.glb, out, background="#fff"),
            "wide margin": lambda: render_still(self.glb, out, margin=0.5),
            "negative margin": lambda: render_still(self.glb, out, margin=-0.01),
            "nan margin": lambda: render_still(self.glb, out, margin=float("nan")),
            "text margin": lambda: render_still(self.glb, out, margin="wide"),
        }
        for name, call in calls.items():
            with self.subTest(name):
                with self.assertRaises(TurntableError):
                    _run(call())
        self.assertFalse(self.argv_file.exists())

    def test_missing_glb_or_script_is_reported(self):
        with self.assertRaisesRegex(TurntableError, "glb not found"):
            _run(render_still(self.tmp / "missing.glb", self.tmp / "x.png"))
        with patch.object(config, "TURNTABLE_SCRIPT", str(self.tmp / "missing.mjs")):
            with self.assertRaisesRegex(TurntableError, "script not found"):
                _run(render_views(self.glb, self.tmp))
        self.assertFalse(self.argv_file.exists())

    def test_non_zero_exit_carries_the_renderer_output(self):
        self.set_mode("exit")

        with self.assertRaises(TurntableError) as caught:
            _run(render_still(self.glb, self.tmp / "x.png"))

        self.assertIn("still render exited 3", str(caught.exception))
        self.assertIn("chrome crashed while rendering", str(caught.exception))

    def test_a_png_left_by_an_earlier_run_does_not_pass_for_this_one(self):
        out = self.tmp / "front.png"
        stale = Image.new("RGB", (1024, 1024), GREY)
        ImageDraw.Draw(stale).rectangle((200, 100, 800, 900), fill=(10, 200, 10))
        stale.save(out)
        self.set_mode("silent")  # exit 0, writes nothing

        with self.assertRaisesRegex(TurntableError, "produced no output"):
            _run(render_still(self.glb, out))
        self.assertFalse(out.exists())

    def test_the_exit_code_alone_is_never_trusted(self):
        expected = {
            "tiny": "produced no output",
            "jpeg": "is JPEG, not PNG",
            "wrong_size": "is 512x512, expected 1024x1024",
            "truncated": "unreadable image",
            "blank": "is blank",
        }
        for mode, message in expected.items():
            with self.subTest(mode):
                self.set_mode(mode)
                with self.assertRaises(TurntableError) as caught:
                    _run(render_still(self.glb, self.tmp / f"{mode}.png"))
                self.assertIn(message, str(caught.exception))
                self.assertIn("[glb_turntable] wrote", str(caught.exception))

    def test_a_transparent_render_must_have_visible_alpha(self):
        for mode, message in {"no_alpha": "has no alpha channel", "blank": "is blank"}.items():
            with self.subTest(mode):
                self.set_mode(mode)
                with self.assertRaisesRegex(TurntableError, message):
                    _run(render_still(self.glb, self.tmp / "alpha.png", background="transparent"))

    def test_render_views_checks_every_view(self):
        self.set_mode("partial")  # only the first view is written

        with self.assertRaisesRegex(TurntableError, "hero_back.png"):
            _run(render_views(self.glb, self.tmp / "views", views=("front", "back")))

    @unittest.skipUnless(os.name == "posix", "process groups are POSIX-only")
    def test_timeout_kills_the_renderer_and_everything_it_started(self):
        self.set_mode("hang")  # stands in for node with Chrome under it

        started = time.monotonic()
        with patch.object(config, "TURNTABLE_TIMEOUT_SECONDS", 2.0):
            with self.assertRaisesRegex(TurntableError, "still render timed out"):
                _run(render_still(self.glb, self.tmp / "x.png"))

        self.assertLess(time.monotonic() - started, 20)
        pids = json.loads(self.pids_file.read_text())
        deadline = time.monotonic() + 10
        while any(_alive(pid) for pid in pids) and time.monotonic() < deadline:
            time.sleep(0.05)
        self.assertEqual([pid for pid in pids if _alive(pid)], [])


@unittest.skipUnless(NODE, "node is not installed")
class RendererArgumentTests(unittest.TestCase):
    """The real script's parser; every case exits before Chrome is looked for."""

    def fails(self, *args: str) -> str:
        completed = subprocess.run(
            [NODE, str(RENDERER_SCRIPT), *args],
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(completed.returncode, 1, completed.stdout + completed.stderr)
        return completed.stderr

    def test_unknown_arguments_still_fail(self):
        for output, mode in (("o.mp4", ()), ("o.png", ("--still", "front"))):
            with self.subTest(output):
                stderr = self.fails("--glb", "m.glb", "--output", output, *mode, "--bogus", "1")
                self.assertIn("[glb_turntable] unknown argument --bogus", stderr)

    def test_still_arguments_are_validated(self):
        cases = [
            (["--still", "front", "--views", "back"], "--still and --views are mutually exclusive"),
            (["--still", "side"], "unknown view 'side'"),
            (["--views", "front,front"], "--views must not repeat a view"),
            (["--views", "front,"], "unknown view ''"),
            (["--still", "front", "--seconds", "3"], "turntable-only argument(s) with --still/--views: --seconds"),
            (["--views", "front", "--ffmpeg", "ffmpeg"], "turntable-only argument(s) with --still/--views: --ffmpeg"),
            (["--still", "front", "--size", "4096"], "--size must be an integer in [64, 2048]"),
            (["--still", "front", "--ortho", "yes"], "--ortho must be 1 or 0"),
            (["--still", "front", "--margin", "0.5"], "--margin must be in [0, 0.4]"),
            (["--still", "front", "--background", "white"], "--background must be '#rrggbb' or 'transparent'"),
            (["--ortho", "1"], "still-only argument(s) without --still/--views: --ortho"),
            (["--margin", "0.1", "--background", "#000000"], "--margin, --background"),
        ]
        for extra, message in cases:
            with self.subTest(args=extra):
                self.assertIn(message, self.fails("--glb", "m.glb", "--output", "o.png", *extra))

    def test_turntable_argument_errors_are_unchanged(self):
        self.assertIn(
            "[glb_turntable] usage: --glb <model.glb> --output <out.mp4>",
            self.fails("--glb", "m.glb"),
        )
        self.assertIn(
            "--size must be an even integer in [64, 2048]",
            self.fails("--glb", "m.glb", "--output", "o.mp4", "--size", "777"),
        )
        self.assertIn(
            "missing value for --fps",
            self.fails("--glb", "m.glb", "--output", "o.mp4", "--fps"),
        )


def _max_channel(image: Image.Image) -> Image.Image:
    red, green, blue = image.convert("RGB").split()
    return ImageChops.lighter(ImageChops.lighter(red, green), blue)


def _mask(image: Image.Image, threshold: int) -> Image.Image:
    return _max_channel(image).point(lambda value: 255 if value > threshold else 0)


def _foreground(image: Image.Image, background=GREY) -> Image.Image:
    return _mask(ImageChops.difference(image.convert("RGB"), Image.new("RGB", image.size, background)), 12)


def _count(mask: Image.Image) -> int:
    return mask.histogram()[255]


def _colour_name(red: int, green: int, blue: int) -> str:
    if red > 1.6 * green and red > 1.6 * blue:
        return "red"
    if green > 1.6 * red and green > 1.6 * blue:
        return "green"
    if blue > 1.6 * red and blue > 1.6 * green:
        return "blue"
    if red > 1.6 * blue and green > 1.6 * blue:
        return "yellow"
    if max(red, green, blue) - min(red, green, blue) < 24:
        return "grey"
    return f"other{(red, green, blue)}"


def _marked_cube_glb() -> bytes:
    """A unit cube with one colour per face, each face its own primitive.

    The -Z face gets its colour from COLOR_0, the mesh is skinned to one joint
    and an animation turns that joint 180 degrees: if the renderer ever played
    it, "front" would show the back face.
    """
    faces = [  # (axis, sign, linear colour, colour via COLOR_0)
        (2, 1, (0.80, 0.04, 0.04), False),   # +Z red: front
        (2, -1, (0.04, 0.08, 0.80), True),   # -Z blue: back
        (0, 1, (0.04, 0.70, 0.04), False),   # +X green: right
        (0, -1, (0.80, 0.65, 0.04), False),  # -X yellow: left
        (1, 1, (0.80, 0.80, 0.80), False),   # +Y light grey: top
        (1, -1, (0.10, 0.10, 0.10), False),  # -Y dark: bottom
    ]
    blob = bytearray()
    views, accessors = [], []

    def accessor(values, fmt, count, component, kind, target=None, bounds=None):
        blob.extend(b"\0" * (-len(blob) % 4))
        start = len(blob)
        blob.extend(struct.pack(f"<{len(values)}{fmt}", *values))
        view = {"buffer": 0, "byteOffset": start, "byteLength": len(blob) - start}
        if target:
            view["target"] = target
        views.append(view)
        entry = {"bufferView": len(views) - 1, "componentType": component, "count": count, "type": kind}
        if bounds:
            entry["min"], entry["max"] = bounds
        accessors.append(entry)
        return len(accessors) - 1

    primitives, materials = [], []
    for axis, sign, colour, via_vertex in faces:
        u, v = [index for index in range(3) if index != axis]
        corners = []
        for a, b in ((-0.5, -0.5), (0.5, -0.5), (0.5, 0.5), (-0.5, 0.5)):
            point = [0.0, 0.0, 0.0]
            point[axis], point[u], point[v] = 0.5 * sign, a, b
            corners.append(point)
        normal = [0.0, 0.0, 0.0]
        normal[axis] = float(sign)
        # counter-clockwise seen from outside: (u x v) points along +axis
        clockwise = sign * (1 if (axis, u, v) in ((0, 1, 2), (1, 2, 0), (2, 0, 1)) else -1) < 0
        indices = [0, 2, 1, 0, 3, 2] if clockwise else [0, 1, 2, 0, 2, 3]
        attributes = {
            "POSITION": accessor(
                [c for point in corners for c in point], "f", 4, 5126, "VEC3", 34962,
                ([min(p[i] for p in corners) for i in range(3)], [max(p[i] for p in corners) for i in range(3)]),
            ),
            "NORMAL": accessor(normal * 4, "f", 4, 5126, "VEC3", 34962),
            "JOINTS_0": accessor([0, 0, 0, 0] * 4, "B", 4, 5121, "VEC4", 34962),
            "WEIGHTS_0": accessor([1.0, 0.0, 0.0, 0.0] * 4, "f", 4, 5126, "VEC4", 34962),
        }
        base = [*colour, 1.0]
        if via_vertex:
            attributes["COLOR_0"] = accessor([*colour, 1.0] * 4, "f", 4, 5126, "VEC4", 34962)
            base = [1.0, 1.0, 1.0, 1.0]
        materials.append(
            {"pbrMetallicRoughness": {"baseColorFactor": base, "metallicFactor": 0.0, "roughnessFactor": 0.8}}
        )
        primitives.append(
            {
                "attributes": attributes,
                "indices": accessor(indices, "H", 6, 5123, "SCALAR", 34963),
                "material": len(materials) - 1,
            }
        )
    identity = [1.0, 0, 0, 0, 0, 1.0, 0, 0, 0, 0, 1.0, 0, 0, 0, 0, 1.0]
    inverse_bind = accessor(identity, "f", 1, 5126, "MAT4")
    times = accessor([0.0, 1.0], "f", 2, 5126, "SCALAR", None, ([0.0], [1.0]))
    half_turn = accessor([0.0, 1.0, 0.0, 0.0] * 2, "f", 2, 5126, "VEC4")
    root = {
        "asset": {"version": "2.0"},
        "scene": 0,
        "scenes": [{"nodes": [0]}],
        "nodes": [{"children": [1, 2]}, {"name": "joint"}, {"mesh": 0, "skin": 0}],
        "skins": [{"joints": [1], "inverseBindMatrices": inverse_bind}],
        "animations": [
            {
                "channels": [{"sampler": 0, "target": {"node": 1, "path": "rotation"}}],
                "samplers": [{"input": times, "output": half_turn}],
            }
        ],
        "meshes": [{"primitives": primitives}],
        "materials": materials,
        "accessors": accessors,
        "bufferViews": views,
        "buffers": [{"byteLength": len(blob)}],
    }
    payload = json.dumps(root, separators=(",", ":")).encode()
    payload += b" " * (-len(payload) % 4)
    blob.extend(b"\0" * (-len(blob) % 4))
    body = (
        struct.pack("<II", len(payload), 0x4E4F534A) + payload
        + struct.pack("<II", len(blob), 0x004E4942) + bytes(blob)
    )
    return struct.pack("<4sII", b"glTF", 2, 12 + len(body)) + body


@unittest.skipUnless(
    NODE and CHROME,
    "needs node and Chrome/chrome-headless-shell (set RENDERFIN_TURNTABLE_CHROME or CHROME_PATH)",
)
class RealRenderTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = Path(tmp.name)
        patcher = patch.multiple(
            config,
            TURNTABLE_NODE=NODE,
            TURNTABLE_SCRIPT=str(RENDERER_SCRIPT),
            TURNTABLE_CHROME=CHROME,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_default_t_pose_front_is_centred_inside_the_margin_and_differs_from_back(self):
        size, margin = 1024, 0.08

        renders = _run(render_views(DEFAULT_T_POSE_GLB, self.tmp, views=("front", "back")))

        images = {}
        for view, path in renders.items():
            with Image.open(path) as image:
                images[view] = image.convert("RGB")
        front, back = images["front"], images["back"]
        for corner in ((0, 0), (size - 1, 0), (0, size - 1), (size - 1, size - 1)):
            self.assertEqual(front.getpixel(corner), GREY)
        left, top, right, bottom = _foreground(front).getbbox()  # right/bottom exclusive
        self.assertGreaterEqual(min(left, top), margin * size - 1)
        self.assertLessEqual(max(right, bottom), (1 - margin) * size + 1)
        # fitted, not merely inside: the longer side spans the whole margin box
        self.assertGreaterEqual(max(right - left, bottom - top), (1 - 2 * margin) * size - 4)
        self.assertAlmostEqual((left + right) / 2, size / 2, delta=0.01 * size)
        self.assertAlmostEqual((top + bottom) / 2, size / 2, delta=0.01 * size)
        # The figure is nearly symmetric and translucent, so compare where it is.
        figure = ImageChops.lighter(_foreground(front), _foreground(back))
        changed = ImageChops.multiply(_mask(ImageChops.difference(front, back), 16), figure)
        self.assertGreater(_count(changed) / _count(figure), 0.05)

    def test_views_follow_the_browser_convention_and_keep_the_rest_pose(self):
        glb = self.tmp / "marked_cube.glb"
        glb.write_bytes(_marked_cube_glb())

        renders = _run(render_views(glb, self.tmp, views=STILL_VIEWS, size=256, background="transparent"))

        faces = {"front": "red", "back": "blue", "left": "yellow", "right": "green"}
        for view, colour in faces.items():
            with self.subTest(view), Image.open(renders[view]) as image:
                self.assertEqual(image.mode, "RGBA")
                self.assertEqual(image.getpixel((0, 0))[3], 0)
                *rgb, alpha = image.getpixel((128, 128))
                self.assertEqual(alpha, 255)
                self.assertEqual(_colour_name(*rgb), colour)
        # From (+X, +Y, +Z): top face above, +Z face lower left, +X face lower right.
        with Image.open(renders["top_side_45"]) as image:
            image = image.convert("RGB")
            self.assertEqual(_colour_name(*image.getpixel((128, 74))), "grey")
            self.assertEqual(_colour_name(*image.getpixel((81, 155))), "red")
            self.assertEqual(_colour_name(*image.getpixel((175, 155))), "green")

    def test_turntable_still_encodes_an_mp4(self):
        ffmpeg = shutil.which(config.TURNTABLE_FFMPEG)
        encoders = ""
        if ffmpeg:
            encoders = subprocess.run(
                [ffmpeg, "-hide_banner", "-encoders"], capture_output=True, text=True, timeout=60
            ).stdout
        if "libx264" not in encoders:
            self.skipTest("needs an ffmpeg with libx264 (RENDERFIN_TURNTABLE_FFMPEG)")
        out = self.tmp / "default_t_pose_turntable.mp4"

        _run(render_turntable(DEFAULT_T_POSE_GLB, out, seconds=0.2))

        self.assertEqual(out.read_bytes()[4:8], b"ftyp")


if __name__ == "__main__":
    unittest.main()
