"""Stream video decode to disk on boxes that carry AutorigStreamVideoSave.

The templates decode with core VAEDecodeTiled and hand a whole float32 clip
through ImageScale / CreateVideo / SaveVideo, so system RAM grows with frames
x pixels (5.5 GB per copy at 193 frames of 1152x2048, several copies alive at
once). Our custom node (autorig-online/deploy/comfy-nodes/autorig_stream_decode)
decodes the same temporal windows and writes frames straight into the H.264
encoder, so RAM stays at about one window whatever the length.

`to_streaming` rewrites the rendered graph's decode chain into that node. It is
applied per box at submit, only after the box has shown the node in its
/object_info, so the templates stay valid everywhere while the node rolls out.
Boxes without it keep the in-RAM chain behind a RAM guard.
"""
from __future__ import annotations

import math
import time
from typing import Any, Dict, Optional, Tuple

import httpx

STREAM_NODE = "AutorigStreamVideoSave"
# Share of a box's physical RAM one decoded float32 clip may take on the in-RAM
# path (the chain holds three or four copies of it at its peak).
RAM_GUARD_SHARE = 0.20
_PROBE_TTL = 300.0
_probe_cache: Dict[str, Tuple[float, Dict[str, int]]] = {}


def _node(workflow: Dict[str, Any], ref: Any) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    if isinstance(ref, list) and ref and isinstance(ref[0], str) and ref[0] in workflow:
        return ref[0], workflow[ref[0]]
    return None, None


def _consumers(workflow: Dict[str, Any], node_id: str) -> int:
    count = 0
    for node in workflow.values():
        for value in (node.get("inputs") or {}).values():
            if isinstance(value, list) and value and value[0] == node_id:
                count += 1
    return count


def _model_sizes(width: int, height: int):
    """The padded model sizes a delivery size decodes at (/32 plain, /64 IC control):
    a resize to one of them before the delivery resize is an identity pass."""
    return {tuple(math.ceil(v / g) * g for v in (width, height)) for g in (32, 64)}


def to_streaming(workflow: Dict[str, Any]) -> bool:
    """Replace VAEDecodeTiled -> [ImageFromBatch] -> [ImageScale] -> CreateVideo
    -> SaveVideo with one AutorigStreamVideoSave. Returns True when rewritten."""
    changed = False
    for save_id, save in list(workflow.items()):
        if save.get("class_type") != "SaveVideo":
            continue
        cv_id, cv = _node(workflow, (save.get("inputs") or {}).get("video"))
        if not cv or cv.get("class_type") != "CreateVideo":
            continue
        chain = []
        width = height = max_frames = 0
        ref = (cv.get("inputs") or {}).get("images")
        node_id, node = _node(workflow, ref)
        while node is not None and node.get("class_type") in {"ImageScale", "ImageFromBatch"}:
            inputs = node.get("inputs") or {}
            if node.get("class_type") == "ImageScale":
                if inputs.get("crop") != "center" or inputs.get("upscale_method") != "lanczos":
                    break
                size = int(inputs.get("width") or 0), int(inputs.get("height") or 0)
                if width and size not in _model_sizes(width, height):
                    break  # a second, real resize: not ours to fold
                if not width:
                    width, height = size
                ref = inputs.get("image")
            else:
                if max_frames or int(inputs.get("batch_index") or 0) != 0:
                    break  # only a trim that keeps the timeline from frame 0
                max_frames = int(inputs.get("length") or 0)
                ref = inputs.get("image")
            chain.append(node_id)
            node_id, node = _node(workflow, ref)
        if node is None or node.get("class_type") != "VAEDecodeTiled":
            continue
        # every node we fold away must feed only the next link of this chain
        if any(_consumers(workflow, nid) != 1 for nid in chain + [node_id, cv_id]):
            continue
        dec = node.get("inputs") or {}
        cv_inputs = cv.get("inputs") or {}
        stream_inputs = {
            "vae": dec.get("vae"),
            "samples": dec.get("samples"),
            "fps": float(cv_inputs.get("fps") or 24),
            "filename_prefix": (save.get("inputs") or {}).get("filename_prefix", "video/ComfyUI"),
            "tile_size": int(dec.get("tile_size", 512)),
            "overlap": int(dec.get("overlap", 64)),
            "temporal_size": int(dec.get("temporal_size", 64)),
            "temporal_overlap": int(dec.get("temporal_overlap", 16)),
            "width": width,
            "height": height,
            "max_frames": max_frames,
        }
        if cv_inputs.get("audio") is not None:
            stream_inputs["audio"] = cv_inputs["audio"]
        for nid in chain + [node_id, cv_id]:
            workflow.pop(nid, None)
        workflow[save_id] = {"class_type": STREAM_NODE, "inputs": stream_inputs,
                             "_meta": dict(save.get("_meta") or {})}
        changed = True
    return changed


def decoded_clip_bytes(width: int, height: int, frames: int) -> int:
    """float32 RGB bytes of the decoded clip at the /32-padded model size."""
    w = math.ceil(max(1, int(width or 0)) / 32) * 32
    h = math.ceil(max(1, int(height or 0)) / 32) * 32
    return int(frames) * w * h * 3 * 4


def ram_guard_error(width: int, height: int, frames: int, ram_total: int) -> str:
    """Why the in-RAM decode chain must not run this clip on this box ('' = fine)."""
    if not ram_total:
        return ""
    need = decoded_clip_bytes(width, height, frames)
    if need <= RAM_GUARD_SHARE * ram_total:
        return ""
    return (f"decoded clip {need / 2**30:.1f} GB ({frames} frames at {width}x{height}) exceeds "
            f"{int(RAM_GUARD_SHARE * 100)}% of this box's {ram_total / 2**30:.0f} GB RAM "
            f"and the box has no {STREAM_NODE}")


async def probe(client: httpx.AsyncClient, base_url: str, auth=None) -> Dict[str, int]:
    """{"stream": 0/1, "ram_total": bytes, "vram_total": bytes} for one ComfyUI; cached 5 min.

    Any failure reads as "no node, sizes unknown": the graph keeps its
    in-RAM chain and the template's own decode settings.
    """
    now = time.time()
    hit = _probe_cache.get(base_url)
    if hit and now - hit[0] < _PROBE_TTL:
        return dict(hit[1])
    facts = {"stream": 0, "ram_total": 0, "vram_total": 0}
    try:
        resp = await client.get(f"{base_url}/object_info/{STREAM_NODE}", timeout=10.0, auth=auth)
        facts["stream"] = int(resp.status_code == 200 and STREAM_NODE in (resp.json() or {}))
    except Exception:
        pass
    try:
        resp = await client.get(f"{base_url}/system_stats", timeout=10.0, auth=auth)
        if resp.status_code == 200:
            stats = resp.json() or {}
            facts["ram_total"] = int((stats.get("system") or {}).get("ram_total") or 0)
            devices = stats.get("devices") or [{}]
            facts["vram_total"] = int(devices[0].get("vram_total") or 0)
    except Exception:
        pass
    _probe_cache[base_url] = (now, dict(facts))
    return facts


# Temporal/spatial decode tiles by VRAM class, measured 2026-09-23 on the
# LTX-2.5 diffusion VAE (docs/video-decode-optimization-20260923.md):
#   * 12 GB (3080 Ti): 512 px tiles x 128 frames fit with ~6 GB to spare;
#   * 8 GB (3070 Ti): 512 x 96 runs out of memory, 512 x 128 spills into
#     shared memory (194 s for 97 frames); 384 x 96 decodes 97 frames in 58 s.
# A 32-frame temporal overlap (4 latent frames) removes the motion hitch the
# 16-frame overlap leaves at every window seam; 64-frame windows with that
# overlap average so many windows that motion visibly damps, hence 96+.
DECODE_POLICY = (
    # (min VRAM bytes, tile_size, overlap, temporal_size, temporal_overlap)
    (10 * 2 ** 30, 512, 64, 128, 32),
    (0, 384, 64, 96, 32),
)
_DECODE_CLASSES = {"VAEDecodeTiled", STREAM_NODE}


def decode_settings(vram_total: int) -> Optional[Dict[str, int]]:
    if not vram_total:
        return None
    for floor, tile, overlap, temporal, t_overlap in DECODE_POLICY:
        if vram_total >= floor:
            return {"tile_size": tile, "overlap": overlap, "temporal_size": temporal,
                    "temporal_overlap": t_overlap}
    return None


def apply_decode_policy(workflow: Dict[str, Any], vram_total: int) -> bool:
    """Set the video decoder's tiles for this box's VRAM class (unknown VRAM: keep)."""
    settings = decode_settings(vram_total)
    if not settings:
        return False
    changed = False
    for node in workflow.values():
        if node.get("class_type") in _DECODE_CLASSES and "temporal_size" in (node.get("inputs") or {}):
            node["inputs"].update(settings)
            changed = True
    return changed


def has_video_decode_chain(workflow: Dict[str, Any]) -> bool:
    return any(n.get("class_type") == "VAEDecodeTiled" for n in workflow.values()) and any(
        n.get("class_type") == "SaveVideo" for n in workflow.values())
