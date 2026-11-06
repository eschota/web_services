"""Music: Stable Audio 3 through ComfyUI's native nodes (2026-09-26).

A text prompt in, a stereo 44.1 kHz MP3 out. The typed request carries the
clip length in ``audio_seconds``; everything else (steps, cfg, sampler) goes
through the ordinary runtime settings, which already touch KSampler.

Scheduling uses its own token, ``gen_music_sa3.json``: only a box whose
registration advertises it (the 12 GB cards holding the medium checkpoint)
gets the work. The small-music template is optional for the 8 GB boxes and is
scheduled on ``gen_music_sa3_small.json``.
"""
from __future__ import annotations

from typing import Any, Dict

MUSIC_WORKFLOWS = {
    "music_sa3": "gen_music_sa3.json",
    "music_sa3_small": "gen_music_sa3_small.json",
}
MUSIC_TYPES = frozenset(MUSIC_WORKFLOWS)
MUSIC_EXT = ".mp3"
MIN_SECONDS = 1.0
# The medium model is trained to 384 s (its seconds_total embedder's range).
MAX_SECONDS = 380.0
DEFAULT_SECONDS = 30.0


def is_music(prompt: Any) -> bool:
    return str(getattr(prompt, "type", "") or "").strip().lower() in MUSIC_TYPES


def clamp_seconds(value: Any) -> float:
    try:
        seconds = float(value or 0)
    except (TypeError, ValueError):
        seconds = 0.0
    if seconds <= 0:
        seconds = DEFAULT_SECONDS
    return round(max(MIN_SECONDS, min(MAX_SECONDS, seconds)), 2)


# Stable Audio 3 writes a whole piece for the length it is given: asked for 8 s
# it plays a short phrase and fades from the first second (measured 26.09:
# -8 dBFS falling to -32 dBFS over 8 s). Asked for 20 s it holds its level
# until the last ~4 s. So anything shorter is rendered at this length and cut
# to the requested one, which keeps a short clip's music at full level.
MIN_RENDER_SECONDS = 20.0


def apply_music_settings(workflow: Dict[str, Any], prompt: Any) -> None:
    """Set the render length on the empty latent; cut short requests to size."""
    seconds = clamp_seconds(getattr(prompt, "audio_seconds", 0))
    render_seconds = max(seconds, MIN_RENDER_SECONDS)
    for node in workflow.values():
        if isinstance(node, dict) and node.get("class_type") == "EmptyLatentAudio":
            node.setdefault("inputs", {})["seconds"] = render_seconds
    if render_seconds <= seconds:
        return
    for node_id, node in list(workflow.items()):
        if not isinstance(node, dict) or not str(node.get("class_type", "")).startswith("SaveAudio"):
            continue
        inputs = node.setdefault("inputs", {})
        source = inputs.get("audio")
        trim_id = "music_trim_" + str(node_id)
        workflow[trim_id] = {"class_type": "TrimAudioDuration",
                             "inputs": {"audio": source, "start_index": 0.0, "duration": seconds}}
        inputs["audio"] = [trim_id, 0]
