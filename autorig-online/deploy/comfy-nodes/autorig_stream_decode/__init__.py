"""AutoRig streaming video decode + save for ComfyUI 0.37.

`AutorigStreamVideoSave` replaces the VAEDecodeTiled -> (ImageScale) ->
CreateVideo -> SaveVideo chain of a video graph. It decodes the latent in
temporal windows with exactly VAEDecodeTiled's tiling and blending, and hands
each finished frame straight to the H.264 encoder, so system RAM holds about
one temporal window instead of the whole clip as float32 (5.5 GB per copy at
193 frames of 1152x2048; the old chain held three or four such copies).

The file lands in the output directory under the same name SaveVideo would
give it (<prefix>_00001_.mp4), and the history entry carries it the same way,
so renderfin collects it unchanged. /interrupt is honoured between windows.

No model files; pure torch + PyAV, both shipped with ComfyUI.
"""
from __future__ import annotations

import logging
import math
import os
from fractions import Fraction

import av
import torch

import comfy.model_management
import comfy.utils
import folder_paths

from .stream_math import TemporalBlender, temporal_mask, temporal_plan

log = logging.getLogger("autorig_stream_decode")


def _color(target):
    try:
        from av.video.reformatter import ColorPrimaries, ColorRange, ColorTrc
        target.color_primaries = ColorPrimaries.BT709
        target.color_trc = ColorTrc.BT709
        target.colorspace = 1  # BT709 non-constant luminance, as SaveVideo
        target.color_range = ColorRange.MPEG
    except Exception:  # older PyAV: SaveVideo's defaults without tags
        pass


class AutorigStreamVideoSave:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "vae": ("VAE",),
                "samples": ("LATENT",),
                "fps": ("FLOAT", {"default": 24.0, "min": 1.0, "max": 120.0, "step": 1.0}),
                "filename_prefix": ("STRING", {"default": "video/ComfyUI"}),
                "tile_size": ("INT", {"default": 512, "min": 64, "max": 4096, "step": 32}),
                "overlap": ("INT", {"default": 64, "min": 0, "max": 4096, "step": 32}),
                "temporal_size": ("INT", {"default": 64, "min": 8, "max": 4096, "step": 4}),
                "temporal_overlap": ("INT", {"default": 16, "min": 4, "max": 4096, "step": 4}),
                "width": ("INT", {"default": 0, "min": 0, "max": 8192, "tooltip": "Delivery width; 0 keeps the decoded size."}),
                "height": ("INT", {"default": 0, "min": 0, "max": 8192, "tooltip": "Delivery height; 0 keeps the decoded size."}),
                "max_frames": ("INT", {"default": 0, "min": 0, "max": 100000, "tooltip": "Keep only the first N frames; 0 keeps all."}),
            },
            "optional": {"audio": ("AUDIO",)},
            "hidden": {"prompt": "PROMPT", "extra_pnginfo": "EXTRA_PNGINFO"},
        }

    RETURN_TYPES = ()
    FUNCTION = "run"
    OUTPUT_NODE = True
    CATEGORY = "autorig/video"

    def run(self, vae, samples, fps, filename_prefix, tile_size, overlap, temporal_size,
            temporal_overlap, width, height, max_frames, audio=None, prompt=None, extra_pnginfo=None):
        latent = samples["samples"]
        if getattr(latent, "is_nested", False):
            latent = latent.unbind()[0]
        if latent.ndim != 5:
            raise ValueError("AutorigStreamVideoSave needs a video latent [B, C, T, H, W]")
        # VAEDecodeTiled's parameter conversion, verbatim.
        if tile_size < overlap * 4:
            overlap = tile_size // 4
        if temporal_size < temporal_overlap * 2:
            temporal_overlap = temporal_overlap // 2
        tc = vae.temporal_compression_decode()
        if tc is None:
            raise ValueError("AutorigStreamVideoSave needs a video VAE")
        tile_t = max(2, temporal_size // tc)
        overlap_t = max(1, min(tile_t // 2, temporal_overlap // tc))
        comp = vae.spacial_compression_decode()
        tile_xy, overlap_xy = tile_size // comp, overlap // comp

        up = vae.upscale_ratio[0]
        idx = (vae.upscale_index_formula or vae.upscale_ratio)[0]
        upscale = up if callable(up) else (lambda n, k=up: k * n)
        index = idx if not callable(idx) else 1  # all shipped video VAEs use a plain factor
        plan, feather = temporal_plan(latent.shape[2], tile_t, overlap_t, upscale, index)

        out_h, out_w = latent.shape[3] * comp, latent.shape[4] * comp
        dst_w, dst_h = (width or out_w), (height or out_h)
        dst_w -= dst_w % 2
        dst_h -= dst_h % 2
        full_folder, filename, counter, subfolder, _ = folder_paths.get_save_image_path(
            filename_prefix, folder_paths.get_output_directory(), dst_w, dst_h)
        file = f"{filename}_{counter:05}_.mp4"
        path = os.path.join(full_folder, file)
        rate = Fraction(round(float(fps) * 1000), 1000)
        pbar = comfy.utils.ProgressBar(len(plan))
        written = 0
        limit = int(max_frames) if max_frames else None
        log.info("stream decode %s: %d latent frames in %d windows (tile %d/%d latent, spatial %d/%d) -> %dx%d",
                 file, latent.shape[2], len(plan), tile_t, overlap_t, tile_xy, overlap_xy, dst_w, dst_h)
        try:
            with av.open(path, mode="w", format="mp4", options={"movflags": "use_metadata_tags+faststart"}) as out:
                stream = out.add_stream("libx264", rate=rate)
                stream.width, stream.height, stream.pix_fmt = dst_w, dst_h, "yuv420p"
                _color(stream.codec_context)
                # Every stream must exist before the first packet is muxed.
                astream = wave = layout = None
                if audio is not None:
                    sr = int(audio["sample_rate"])
                    wave = audio["waveform"][0]
                    layout = {1: "mono", 2: "stereo", 6: "5.1"}.get(wave.shape[0], "stereo")
                    astream = out.add_stream("aac", rate=sr, layout=layout)

                def emit(frames):
                    nonlocal written
                    if frames is None:
                        return
                    if limit is not None:
                        frames = frames[:max(0, limit - written)]
                    if frames.shape[0] == 0:
                        return
                    if (frames.shape[2], frames.shape[1]) != (dst_w, dst_h):
                        frames = comfy.utils.common_upscale(
                            frames.movedim(-1, 1), dst_w, dst_h, "lanczos", "center").movedim(1, -1)
                    frames = (frames * 255).clamp(0, 255).to(torch.uint8).cpu().numpy()
                    for img in frames:
                        vf = av.VideoFrame.from_ndarray(img, format="rgb24").reformat(format="yuv420p", dst_colorspace=1)
                        _color(vf)
                        out.mux(stream.encode(vf))
                    written += frames.shape[0]

                blender = TemporalBlender()
                for i, (pos, length, out_start, out_len) in enumerate(plan):
                    comfy.model_management.throw_exception_if_processing_interrupted()
                    window = latent[:, :, pos:pos + length]
                    images = vae.decode_tiled(window, tile_x=tile_xy, tile_y=tile_xy, overlap=overlap_xy,
                                              tile_t=length, overlap_t=1)
                    images = images.reshape(-1, images.shape[-3], images.shape[-2], images.shape[-1])
                    if images.shape[0] != out_len:
                        raise RuntimeError(f"window {i}: decoded {images.shape[0]} frames, expected {out_len}")
                    mask = temporal_mask(out_len, feather) if len(plan) > 1 else torch.ones(out_len)
                    blender.add(images, out_start, mask)
                    del images
                    nxt = plan[i + 1][2] if i + 1 < len(plan) else None
                    emit(blender.release(nxt))
                    pbar.update(1)
                    if limit is not None and written >= limit:
                        break
                out.mux(stream.encode(None))

                if astream is not None:
                    wave = wave[:, :math.ceil(sr / rate * written)]
                    af = av.AudioFrame.from_ndarray(wave.float().cpu().contiguous().numpy(), format="fltp", layout=layout)
                    af.sample_rate = sr
                    af.pts = 0
                    out.mux(astream.encode(af))
                    out.mux(astream.encode(None))
        except BaseException:
            try:
                os.remove(path)
            except OSError:
                pass
            raise
        log.info("stream decode %s: wrote %d frames", file, written)
        return {"ui": {"images": [{"filename": file, "subfolder": subfolder, "type": "output"}], "animated": (True,)}}


NODE_CLASS_MAPPINGS = {"AutorigStreamVideoSave": AutorigStreamVideoSave}
NODE_DISPLAY_NAME_MAPPINGS = {"AutorigStreamVideoSave": "AutoRig Stream Video Decode + Save"}
