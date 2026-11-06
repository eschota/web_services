"""How big a video job a render box can hold, and how to read a GPU failure.

Owner, 2026-09-29: "such errors must be auto-retried, the box's ComfyUI
restarted, and the hang found and fixed". What the ledger and the boxes showed
that day (pixel-frames = width x height x frames at the model grid):

* H3 Eros Max on the 8 GB RTX 3070 Ti boxes (f5, f15): 155-163M finish in
  11-14 min; 182-186M finish only after 25-38 min of spilling into shared
  memory; from ~197M up they either die with torch.OutOfMemoryError in
  SamplerCustomAdvanced (f15: 217M after 885 s, 262M five times after
  50-90 s) or reset the GPU driver (nvlddmkm event 153: f15 2026-09-28 09:44
  at 197M, ComfyUI crashed and came back; f5 2026-09-29 14:19 at 217M,
  ComfyUI was left a one-thread process that held its port and never
  answered again).
* H3 Eros Max on 12 GB (Raptor) did 217M in 763-770 s, on 24 GB
  (worker-4090) in 355-365 s.
* LTX on 8 GB did 178M (LTX-2.5; 10Eros so far 82M in ~250 s), 226M on
  12 GB, 341M on 24 GB (10Eros 241M in 179 s). The IC-LoRA control graphs
  carry a guide clip as well; on 24 GB 158M (64-px grid) finished 182 times
  and ran out of memory 18 times on 2026-09-26 while the owner's own work
  shared the card, so that edge is transient rather than a size limit.

Nothing routed on size, so a 217M H3 job went to whichever box was idle
first. The tables below are the routing budget (a job above a box's budget is
never sent there), a rough duration model (a prompt running far longer than
it should is treated as hung), and the error classifier. The budgets can be
overridden without a code change:

  RENDERFIN_VRAM_BUDGETS_JSON='{"h3": {"8": 165, "12": 300, "24": 520}}'
  RENDERFIN_BOX_VRAM_GB='f5=8,f15=8,Raptor=12,f12=12,worker-4090=24'

Values are millions of pixel-frames per VRAM class (GiB). A box reports its
own VRAM in /system_stats; the static table is only the fallback.
"""
from __future__ import annotations

import json
import math
import os
from typing import Any, Dict, Iterable, List, Optional, Tuple

FAMILY_H3 = "h3"
FAMILY_LTX = "ltx"
FAMILY_LTX_CONTROL = "ltx_control"

# Graphs that also feed a guide clip through the model (IC-LoRA control and
# CrossView): about twice the tokens of a plain clip of the same size.
_LTX_CONTROL_WORKFLOWS = frozenset({
    "gen_video_ltx23_control_by_url.json",
    "gen_video_ltx23_pose_by_url.json",
    "gen_video_ltx23_depth_by_url.json",
    "gen_video_ltx25_crossview_by_url.json",
})
_LTX_CANONICAL = frozenset({
    "autorig_animal_loop_v1",
    "autorig_animal_one_shot_v1",
    "autorig_animal_loop_ltx2_19b_v1",
    "autorig_animal_oneshot_ltx2_19b_v1",
})

# Millions of pixel-frames a box of this VRAM class may be given.
DEFAULT_BUDGETS_MPXF: Dict[str, Dict[int, float]] = {
    FAMILY_H3: {8: 165.0, 12: 300.0, 24: 520.0},
    FAMILY_LTX: {8: 150.0, 12: 240.0, 24: 450.0},
    FAMILY_LTX_CONTROL: {8: 60.0, 12: 100.0, 24: 200.0},
}
# (fixed seconds, seconds per million pixel-frames), from the same ledger.
DEFAULT_SPEED: Dict[str, Dict[int, Tuple[float, float]]] = {
    FAMILY_H3: {8: (150.0, 6.0), 12: (60.0, 3.6), 24: (40.0, 1.8)},
    FAMILY_LTX: {8: (230.0, 3.2), 12: (60.0, 1.6), 24: (30.0, 0.8)},
    FAMILY_LTX_CONTROL: {8: (230.0, 6.4), 12: (60.0, 3.2), 24: (40.0, 1.5)},
}
DEFAULT_BOX_VRAM_GB = "f5=8,f15=8,Raptor=12,f12=12,worker-4090=24"
# A card reports a little under its nominal size (8589279232 bytes on a
# 3070 Ti, 25.7e9 on a 4090); it still belongs to its nominal class.
_CLASS_TOLERANCE_GIB = 0.75

# Out of memory: the job was too big for the card (or the card was already
# full). Includes comfy-aimdo's own allocator ("VRAM grow failed", f5 x3 on
# 2026-09-28; "VBAR OOM").
_OOM_MARKERS = (
    "outofmemory",
    "out of memory",
    "allocation on device",
    "not enough memory",
    "cudamalloc",
    "cublas_status_alloc_failed",
    "vram grow failed",
    "vbar oom",
)
# The GPU or its CUDA context failed under the job: the box is suspect, the
# job is not. f15 2026-09-29 15:06: a 544x960 Qwen edit died in comfy-aimdo's
# vbar_fault with "Fault failed: 2" (VBAR_FAULT_ERROR, not VBAR_FAULT_OOM)
# after seven out-of-memory errors in the same process.
_GPU_FAULT_MARKERS = (
    "fault failed",
    "hostbuf_file_reader_read failed",
    "cuda error",
    "cudaerror",
    "cuda driver",
    "acceleratorerror",
    "unspecified launch failure",
    "illegal memory access",
    "illegal instruction",
    "misaligned address",
    "launch timed out",
    "device-side assert",
    "cublas_status_execution_failed",
    "cublas_status_internal_error",
    "cudnn_status_execution_failed",
    "cudnn_status_internal_error",
)


def family(workflow_file: str) -> Optional[str]:
    """Which budget table a workflow file belongs to (None = not budgeted)."""
    name = str(workflow_file or "").strip().lower()
    if not name:
        return None
    if name.startswith("gen_video_minimax_h3"):
        return FAMILY_H3
    if name in _LTX_CONTROL_WORKFLOWS:
        return FAMILY_LTX_CONTROL
    if name.startswith("gen_animation") or name.startswith("gen_video_ltx") or name in _LTX_CANONICAL:
        return FAMILY_LTX
    return None


def task_family(task: Any) -> Optional[str]:
    return family(getattr(task, "workflow_file", "") or getattr(task, "workflow", ""))


def model_frames(frame_count: int) -> int:
    """The frame count the samplers run (8k+1), as runtime_settings sets it."""
    return max(1, round((int(frame_count or 0) - 1) / 8)) * 8 + 1


def _video_dims(width: int, height: int) -> Tuple[int, int]:
    def one(value: int, default: int) -> int:
        value = int(value or 0)
        if value <= 0:
            value = default
        value = max(64, min(2048, value))
        return value if value % 2 == 0 else min(2048, value + 1)

    return one(width, 960), one(height, 540)


def job_cost(prompt: Any, fam: Optional[str]) -> int:
    """Pixel-frames the sampler holds at once for this request (0 = unknown)."""
    if not fam:
        return 0
    width, height = _video_dims(
        getattr(prompt, "main_size_width", 0), getattr(prompt, "main_size_height", 0)
    )
    grid = 64 if fam == FAMILY_LTX_CONTROL else 32
    width = math.ceil(width / grid) * grid
    height = math.ceil(height / grid) * grid
    return int(width * height * model_frames(getattr(prompt, "frame_count", 0)))


def task_cost(task: Any) -> int:
    return job_cost(getattr(task, "prompt", None), task_family(task))


def describe_job(task: Any) -> str:
    prompt = getattr(task, "prompt", None)
    fam = task_family(task)
    width, height = _video_dims(
        getattr(prompt, "main_size_width", 0), getattr(prompt, "main_size_height", 0)
    )
    frames = model_frames(getattr(prompt, "frame_count", 0))
    label = {FAMILY_H3: "H3", FAMILY_LTX: "LTX", FAMILY_LTX_CONTROL: "LTX control"}.get(fam or "", "video")
    return f"{label} {width}x{height}x{frames} ({task_cost(task) / 1e6:.0f}M px-frames)"


_BUDGET_CACHE: Dict[str, Dict[str, Dict[int, float]]] = {}


def _parse_budgets() -> Dict[str, Dict[int, float]]:
    raw = os.getenv("RENDERFIN_VRAM_BUDGETS_JSON", "").strip()
    cached = _BUDGET_CACHE.get(raw)
    if cached is not None:
        return cached
    budgets = {fam: dict(table) for fam, table in DEFAULT_BUDGETS_MPXF.items()}
    if raw:
        try:
            data = json.loads(raw)
            for fam, table in (data or {}).items():
                if not isinstance(table, dict):
                    continue
                parsed = {int(float(k)): float(v) for k, v in table.items() if float(v) > 0}
                if parsed:
                    budgets[str(fam)] = parsed
        except Exception as exc:  # a broken override must not take routing down
            print(f"[Renderfin][Budget] ignoring RENDERFIN_VRAM_BUDGETS_JSON: {exc}")
    _BUDGET_CACHE.clear()
    _BUDGET_CACHE[raw] = budgets
    return budgets


def static_vram_gb() -> Dict[str, float]:
    table: Dict[str, float] = {}
    raw = os.getenv("RENDERFIN_BOX_VRAM_GB", DEFAULT_BOX_VRAM_GB)
    for item in str(raw or "").split(","):
        name, _, value = item.partition("=")
        try:
            if name.strip() and float(value) > 0:
                table[name.strip().lower()] = float(value)
        except ValueError:
            continue
    return table


def _class_value(table: Dict[int, Any], vram_gib: float) -> Tuple[Optional[int], Any]:
    classes = sorted(table)
    if not classes:
        return None, None
    chosen = None
    for cls in classes:
        if vram_gib >= cls - _CLASS_TOLERANCE_GIB:
            chosen = cls
    if chosen is None:
        return None, None
    return chosen, table[chosen]


def budget_pxf(fam: Optional[str], vram_bytes: int) -> Optional[int]:
    """Largest job (pixel-frames) this family may send to a card this size.

    None means "no rule" (not a budgeted family, or unknown VRAM): routing
    stays exactly as it was. A card below the smallest class gets a share of
    the smallest class's budget in proportion to its memory.
    """
    if not fam or not vram_bytes or vram_bytes <= 0:
        return None
    table = _parse_budgets().get(fam)
    if not table:
        return None
    gib = vram_bytes / 2 ** 30
    cls, value = _class_value(table, gib)
    if cls is None:
        smallest = min(table)
        return int(table[smallest] * 1e6 * max(0.0, gib) / smallest)
    return int(float(value) * 1e6)


def expected_seconds(fam: Optional[str], vram_bytes: int, cost: int) -> Optional[float]:
    if not fam or not vram_bytes or not cost:
        return None
    table = DEFAULT_SPEED.get(fam)
    if not table:
        return None
    cls, value = _class_value(table, vram_bytes / 2 ** 30)
    if cls is None:
        value = table[min(table)]
    fixed, per_million = value
    return float(fixed) + float(per_million) * cost / 1e6


def hang_after_seconds(
    fam: Optional[str],
    vram_bytes: int,
    cost: int,
    *,
    factor: float,
    floor: float,
    ceiling: float,
) -> Optional[float]:
    """How long a prompt may run before it counts as hung (None = no rule)."""
    expected = expected_seconds(fam, vram_bytes, cost)
    if expected is None:
        return None
    return float(min(ceiling, max(floor, factor * expected)))


def vram_from_stats(stats: Any) -> Tuple[int, int]:
    """(vram_total, vram_free) bytes from a ComfyUI /system_stats answer."""
    try:
        device = (stats.get("devices") or [{}])[0]
        return int(device.get("vram_total") or 0), int(device.get("vram_free") or 0)
    except Exception:
        return 0, 0


def _messages(entry: Any) -> List[Any]:
    try:
        return list(((entry or {}).get("status") or {}).get("messages") or [])
    except Exception:
        return []


def execution_error(entry: Any) -> Dict[str, Any]:
    info: Dict[str, Any] = {}
    for message in _messages(entry):
        if isinstance(message, (list, tuple)) and len(message) > 1 and message[0] == "execution_error":
            info = message[1] if isinstance(message[1], dict) else {}
    return info


def _error_text(entry: Any) -> str:
    info = execution_error(entry)
    return " ".join(
        str(info.get(key) or "") for key in ("exception_type", "exception_message")
    ).lower()


def is_oom(entry: Any) -> bool:
    """Did this ComfyUI history entry die of GPU (or host) memory?"""
    text = _error_text(entry)
    return any(marker in text for marker in _OOM_MARKERS)


def is_gpu_fault(entry: Any) -> bool:
    """Did the GPU / CUDA context fail under the job (not a memory shortage)?"""
    if is_oom(entry):
        return False
    text = _error_text(entry)
    return any(marker in text for marker in _GPU_FAULT_MARKERS)


def was_interrupted(entry: Any) -> bool:
    """Stopped by /interrupt from outside renderfin (renderfin's own cancel
    marks the task Error before the poller can see the history entry)."""
    return any(
        isinstance(message, (list, tuple)) and message and message[0] == "execution_interrupted"
        for message in _messages(entry)
    )


def classify_failure(entry: Any) -> Optional[str]:
    """'oom', 'gpu_fault', 'interrupted' or None (a failure of the request,
    which every box would repeat: not retried)."""
    if is_oom(entry):
        return "oom"
    if is_gpu_fault(entry):
        return "gpu_fault"
    if was_interrupted(entry) and not execution_error(entry):
        return "interrupted"
    return None


def best_budget(budgets: Iterable[Tuple[str, Optional[int]]]) -> Tuple[str, Optional[int]]:
    """(box, budget) of the most generous box; budget None means unlimited."""
    best_name, best = "", 0
    for name, budget in budgets:
        if budget is None:
            return name, None
        if budget > best:
            best_name, best = name, budget
    return best_name, best
