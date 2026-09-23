"""Streaming video decode: graph rewrite, RAM guard, and the node's window math.

Pinned here:
  * every LTX-2.5 template's decode chain (VAEDecodeTiled -> delivery trim /
    resize -> CreateVideo -> SaveVideo) folds into one AutorigStreamVideoSave
    that keeps the latent, VAE, audio, fps, output prefix, tiling, delivery size
    and control-video trim,
  * graphs that are not a plain decode chain are left alone,
  * the RAM guard refuses only clips that exceed 20% of the box's RAM,
  * the node's temporal windows and feathering reproduce
    comfy.utils.tiled_scale_multidim along time, frame for frame.
"""
from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from renderfin import stream_decode  # noqa: E402
from renderfin.config import WORKFLOWS_DIR  # noqa: E402
from renderfin.runtime_settings import apply_runtime_settings  # noqa: E402
from renderfin.templating import render_workflow_text  # noqa: E402

NODE_MATH = BACKEND.parent / "deploy" / "comfy-nodes" / "autorig_stream_decode" / "stream_math.py"

LTX25 = ("gen_animation_ltx25_by_url.json", "gen_animation_ltx25_hq_by_url.json",
         "gen_animation_by_url.json", "gen_animation_hq_by_url.json")
CONTROL = ("gen_video_ltx23_pose_by_url.json", "gen_video_ltx23_depth_by_url.json",
           "gen_video_ltx23_control_by_url.json")


def _graph(name, width=960, height=540, frames=97, control=False):
    text = (WORKFLOWS_DIR / name).read_text(encoding="utf-8")
    wf = render_workflow_text(text, width=width, height=height, prompt="a dancer", negative_prompt="",
                              image_filename="in.png", control_video_filename="drive.mp4" if control else "",
                              output_prefix="task-1", frames=frames, randomize_seeds=False)
    prompt = SimpleNamespace(type="", lora="", lora_strength=None, frame_count=frames, control_strength=0.8,
                             steps=0, creativity=0, cfg=None, sampler="", scheduler="", clip_skip=None)
    return apply_runtime_settings(wf, prompt, width, height)


def _only(wf, kind):
    found = [n for n in wf.values() if n.get("class_type") == kind]
    assert len(found) == 1, (kind, len(found))
    return found[0]


class GraphRewriteTests(unittest.TestCase):
    def test_ltx25_chain_folds_into_the_stream_node(self):
        for name in LTX25:
            wf = _graph(name)
            decode = _only(wf, "VAEDecodeTiled")
            audio = _only(wf, "CreateVideo")["inputs"]["audio"]
            self.assertTrue(stream_decode.to_streaming(wf), name)
            for gone in ("VAEDecodeTiled", "CreateVideo", "SaveVideo"):
                self.assertFalse(any(n["class_type"] == gone for n in wf.values()), (name, gone))
            node = wf["save"]
            self.assertEqual(node["class_type"], stream_decode.STREAM_NODE)
            inputs = node["inputs"]
            self.assertEqual(inputs["samples"], decode["inputs"]["samples"], name)
            self.assertEqual(inputs["vae"], ["video_vae", 0])
            self.assertEqual(inputs["audio"], audio)
            self.assertEqual(inputs["filename_prefix"], "task-1")
            self.assertEqual(inputs["fps"], 24.0)
            self.assertEqual((inputs["tile_size"], inputs["overlap"], inputs["temporal_size"],
                              inputs["temporal_overlap"]), (512, 64, 64, 16))
            self.assertEqual((inputs["width"], inputs["height"], inputs["max_frames"]), (960, 540, 0), name)
            self.assertEqual(node["_meta"]["title"], "AUTORIG_OUTPUT")
            # nothing dangles: every link points at a node that still exists
            for n in wf.values():
                for v in n["inputs"].values():
                    if isinstance(v, list) and v and isinstance(v[0], str):
                        self.assertIn(v[0], wf, (name, v))

    def test_model_size_delivery_needs_no_resize(self):
        wf = _graph("gen_animation_ltx25_by_url.json", 1152, 2048, 193)
        self.assertTrue(stream_decode.to_streaming(wf))
        self.assertEqual((wf["save"]["inputs"]["width"], wf["save"]["inputs"]["height"]), (0, 0))

    def test_control_video_keeps_its_trim_and_latent_crop(self):
        for name in CONTROL:
            wf = _graph(name, control=True)
            self.assertTrue(stream_decode.to_streaming(wf), name)
            node = next(n for n in wf.values() if n["class_type"] == stream_decode.STREAM_NODE)
            self.assertEqual(node["inputs"]["max_frames"], 97, name)
            source = wf[node["inputs"]["samples"][0]]
            self.assertEqual(source["class_type"], "LTXVSelectLatents", name)

    def test_non_chain_graphs_are_left_alone(self):
        shared = {
            "d": {"class_type": "VAEDecodeTiled", "inputs": {"samples": ["l", 0], "vae": ["v", 0]}},
            "cv": {"class_type": "CreateVideo", "inputs": {"images": ["d", 0], "fps": 24}},
            "s": {"class_type": "SaveVideo", "inputs": {"video": ["cv", 0], "filename_prefix": "x"}},
            "preview": {"class_type": "PreviewImage", "inputs": {"images": ["d", 0]}},
        }
        self.assertFalse(stream_decode.to_streaming(shared))
        self.assertIn("preview", shared)
        plain = {
            "d": {"class_type": "VAEDecode", "inputs": {"samples": ["l", 0], "vae": ["v", 0]}},
            "cv": {"class_type": "CreateVideo", "inputs": {"images": ["d", 0], "fps": 24}},
            "s": {"class_type": "SaveVideo", "inputs": {"video": ["cv", 0], "filename_prefix": "x"}},
        }
        self.assertFalse(stream_decode.has_video_decode_chain(plain))
        self.assertFalse(stream_decode.to_streaming(plain))

    def test_minimax_h3_is_not_rewritten(self):
        wf = _graph("gen_video_minimax_h3_by_url.json")
        self.assertFalse(stream_decode.has_video_decode_chain(wf))


class RamGuardTests(unittest.TestCase):
    GB = 2 ** 30

    def test_long_clips_fit_a_32_gb_box(self):
        ram = 32 * self.GB
        self.assertEqual(stream_decode.ram_guard_error(608, 960, 393, ram), "")
        self.assertEqual(stream_decode.ram_guard_error(1152, 2048, 193, ram), "")
        self.assertEqual(stream_decode.ram_guard_error(1152, 1536, 249, ram), "")

    def test_oversized_clip_is_refused_with_its_numbers(self):
        why = stream_decode.ram_guard_error(1152, 2048, 393, 32 * self.GB)
        self.assertIn("393 frames", why)
        self.assertIn("20%", why)

    def test_unknown_ram_does_not_block(self):
        self.assertEqual(stream_decode.ram_guard_error(2048, 2048, 400, 0), "")


class DecodePolicyTests(unittest.TestCase):
    GB = 2 ** 30

    def test_twelve_gb_boxes_get_128_frame_windows(self):
        wf = _graph("gen_animation_ltx25_by_url.json")
        stream_decode.to_streaming(wf)
        self.assertTrue(stream_decode.apply_decode_policy(wf, 12 * self.GB))
        inputs = wf["save"]["inputs"]
        self.assertEqual((inputs["tile_size"], inputs["temporal_size"], inputs["temporal_overlap"]), (512, 128, 32))

    def test_eight_gb_boxes_get_smaller_tiles(self):
        wf = _graph("gen_animation_ltx25_hq_by_url.json")
        self.assertTrue(stream_decode.apply_decode_policy(wf, 8 * self.GB))  # in-RAM chain too
        decode = _only(wf, "VAEDecodeTiled")["inputs"]
        self.assertEqual((decode["tile_size"], decode["temporal_size"], decode["temporal_overlap"]), (384, 96, 32))

    def test_unknown_vram_keeps_the_template(self):
        wf = _graph("gen_animation_ltx25_by_url.json")
        self.assertFalse(stream_decode.apply_decode_policy(wf, 0))
        self.assertEqual(_only(wf, "VAEDecodeTiled")["inputs"]["temporal_size"], 64)

    def test_image_tiled_decoders_are_untouched(self):
        wf = {"d": {"class_type": "VAEDecodeTiled", "inputs": {"tile_size": 1024}}}
        self.assertFalse(stream_decode.apply_decode_policy(wf, 12 * self.GB))


class ProbeTests(unittest.TestCase):
    def test_probe_reads_node_ram_and_vram_and_caches(self):
        import asyncio
        import httpx
        calls = []

        def handler(request):
            calls.append(request.url.path)
            if request.url.path.startswith("/object_info/"):
                return httpx.Response(200, json={stream_decode.STREAM_NODE: {}})
            return httpx.Response(200, json={"system": {"ram_total": 34 * 2 ** 30},
                                             "devices": [{"vram_total": 12 * 2 ** 30}]})

        async def run():
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                first = await stream_decode.probe(client, "http://probe-test:1")
                second = await stream_decode.probe(client, "http://probe-test:1")
                return first, second

        stream_decode._probe_cache.pop("http://probe-test:1", None)
        first, second = asyncio.run(run())
        self.assertEqual(first, {"stream": 1, "ram_total": 34 * 2 ** 30, "vram_total": 12 * 2 ** 30})
        self.assertEqual(first, second)
        self.assertEqual(len(calls), 2)

    def test_unreachable_box_reads_as_no_node(self):
        import asyncio
        import httpx

        def handler(request):
            raise httpx.ConnectError("down")

        async def run():
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                return await stream_decode.probe(client, "http://probe-down:1")

        stream_decode._probe_cache.pop("http://probe-down:1", None)
        self.assertEqual(asyncio.run(run()), {"stream": 0, "ram_total": 0, "vram_total": 0})


def _load_math():
    spec = importlib.util.spec_from_file_location("autorig_stream_math", NODE_MATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _comfy_temporal_reference(latent_frames, tile, overlap, decode):
    """comfy.utils.tiled_scale_multidim reduced to the time axis (0.37)."""
    import torch
    up = lambda n: max(0, n * 8 - 7)  # noqa: E731  LTX 2.4 VAE upscale_ratio[0]
    total = up(latent_frames)
    out = torch.zeros(total)
    div = torch.zeros(total)
    positions = range(0, latent_frames - overlap, tile - overlap) if latent_frames > tile else [0]
    for it in positions:
        pos = max(0, min(latent_frames - overlap, it))
        l = min(tile, latent_frames - pos)
        ps = decode(pos, l)
        mask = torch.ones(ps.shape[0])
        feather = round(up(overlap))
        if feather < mask.shape[0]:
            for t in range(feather):
                a = (t + 1) / feather
                mask[t] *= a
                mask[mask.shape[0] - 1 - t] *= a
        start = 8 * pos
        out[start:start + ps.shape[0]] += ps * mask
        div[start:start + ps.shape[0]] += mask
    return out / div


try:
    import torch  # noqa: F401  the node's math runs on torch tensors
    HAVE_TORCH = True
except ImportError:  # the production venv has no torch; ComfyUI boxes do
    HAVE_TORCH = False


@unittest.skipUnless(HAVE_TORCH, "torch not installed")
class NodeWindowMathTests(unittest.TestCase):
    def test_streamed_windows_equal_comfys_tiled_blend(self):
        import torch
        m = _load_math()
        up = lambda n: max(0, n * 8 - 7)  # noqa: E731

        def decode(pos, length):
            # distinct content per window so a wrong blend cannot hide
            return torch.arange(up(length), dtype=torch.float32) + 1000.0 * pos

        for latent_frames in (13, 25, 49, 50, 1, 8, 9):
            ref = _comfy_temporal_reference(latent_frames, 8, 2, decode)
            plan, feather = m.temporal_plan(latent_frames, 8, 2, up, 8)
            blender = m.TemporalBlender()
            got = []
            for i, (pos, length, out_start, out_len) in enumerate(plan):
                frames = decode(pos, length).view(-1, 1, 1, 1)
                mask = m.temporal_mask(out_len, feather) if len(plan) > 1 else torch.ones(out_len)
                blender.add(frames, out_start, mask)
                nxt = plan[i + 1][2] if i + 1 < len(plan) else None
                part = blender.release(nxt)
                if part is not None:
                    got.append(part.view(-1))
            got = torch.cat(got)
            self.assertEqual(got.shape, ref.shape, latent_frames)
            self.assertTrue(torch.allclose(got, ref, atol=1e-3), latent_frames)


if __name__ == "__main__":
    unittest.main()
