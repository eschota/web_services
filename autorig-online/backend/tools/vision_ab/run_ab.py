#!/usr/bin/env python3
"""A/B of the farm Vision models on real M+ keywording requests.

Every request is the body M+ sends today (MicrostockPlus
src/libs/mpcore/renderfinvisionapi.ts + trends_ai_meta.ts): the exact M+
prompt (mplus_prompt.json, rebuilt by extract_mplus_prompt.js), the preview
resized like M+ does and sent as image_base64, structured=true,
max_output_tokens from configs.json (512 per the A/B plan; M+ itself sends
640 since 4f70345 - use --max-output-tokens 640 to match), wait_seconds 120. A per-request "[ref ...]" salt (the
same trick M+ uses) keeps the farm's request cache out of the measurement.

    python run_ab.py --configs A,B,C --repeats 3 --out results/run1.jsonl
    python run_ab.py --configs A,C --limit 2 --repeats 1 --out results/smoke.jsonl

Etiquette (render-fleet skill): the whole client stays at <= 1 request/s
(one shared gate for every HTTP call); a POST holds up to wait_seconds, then
the task is polled every 5 s. The output is JSONL, one line per request, and a
rerun with the same --out skips what is already there (resume).

Needs Python 3.9+ and Pillow (resize). No other dependencies.
"""
from __future__ import annotations

import argparse
import base64
import io
import json
import os
import random
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from metadata import score_answer  # noqa: E402

DEFAULT_BASE = "https://autorig.online"
USER_AGENT = "AutoRig-VisionAB/1 (+backend/tools/vision_ab)"
WAIT_SECONDS = 120
POLL_SECONDS = 5.0
TASK_DEADLINE_SECONDS = 420.0
JPEG_QUALITY = 85
MAX_IMAGE_BYTES = int(1.5 * 1024 * 1024)


class Gate:
    """At most one HTTP call per `gap` seconds for the whole process."""

    def __init__(self, gap: float) -> None:
        self.gap = gap
        self.lock = threading.Lock()
        self.last = 0.0

    def wait(self) -> None:
        with self.lock:
            now = time.monotonic()
            delay = self.last + self.gap - now
            if delay > 0:
                time.sleep(delay)
            self.last = time.monotonic()


def http_json(gate: Gate, method: str, url: str, body: Optional[Dict[str, Any]] = None,
              timeout: float = 60.0) -> Tuple[int, Dict[str, Any], str]:
    gate.wait()
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "User-Agent": USER_AGENT, "Accept": "application/json",
        **({"Content-Type": "application/json"} if data is not None else {})})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            text = resp.read().decode("utf-8", "replace")
            status = resp.status
    except urllib.error.HTTPError as exc:
        text = exc.read().decode("utf-8", "replace")
        status = exc.code
    except Exception as exc:  # network error, timeout
        return 0, {}, "%s: %s" % (type(exc).__name__, exc)
    try:
        parsed = json.loads(text)
    except ValueError:
        parsed = {}
    return status, parsed if isinstance(parsed, dict) else {}, text[:500]


def fetch_bytes(gate: Gate, url: str) -> bytes:
    gate.wait()
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read()


def standardize(raw: bytes, long_side: int) -> Tuple[bytes, int, int]:
    """renderfinimage.ts StandardizeImageBytes: long side <= N (never upscale),
    JPEG ~q85, no metadata, <= 1.5 MB (quality then size steps down)."""
    from PIL import Image  # imported lazily so --help works without Pillow
    img = Image.open(io.BytesIO(raw))
    img = img.convert("RGB")
    for shrink in (1.0, 0.75, 0.5):
        side = max(64, round(long_side * shrink))
        w, h = img.size
        scale = min(1.0, side / float(max(w, h)))
        out = img if scale >= 1.0 else img.resize((max(1, round(w * scale)), max(1, round(h * scale))),
                                                  Image.LANCZOS)
        for quality in (JPEG_QUALITY, 75, 60, 45, 30):
            buf = io.BytesIO()
            out.save(buf, format="JPEG", quality=quality, optimize=True)
            if buf.tell() <= MAX_IMAGE_BYTES:
                return buf.getvalue(), out.size[0], out.size[1]
    raise ValueError("image_over_size_cap")


def load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def done_keys(out_path: str) -> set:
    keys = set()
    if os.path.exists(out_path):
        with open(out_path, "r", encoding="utf-8") as fh:
            for line in fh:
                try:
                    row = json.loads(line)
                except ValueError:
                    continue
                if row.get("status") in ("completed", "failed", "wrong_model"):
                    keys.add((row["config"], row["image_id"], row["repeat"]))
    return keys


def run_one(gate: Gate, base: str, cfg: Dict[str, Any], image: Dict[str, Any], repeat: int,
            prepared: Dict[str, Any], prompts: Dict[str, str], run_id: str,
            exact_keywords: int, max_tokens: int = 0) -> Dict[str, Any]:
    salt = "ab-%s-%s-%s-r%d" % (run_id, cfg["name"], image["id"], repeat)
    salt = "".join(ch for ch in salt if ch.isalnum() or ch in "_-")[:64]
    prompt = prompts.get(image.get("kind") or "photo", prompts["photo"]) + "\n\n[ref " + salt + "]"
    body = {
        "prompt": prompt,
        "model": cfg["model"],
        "max_output_tokens": int(max_tokens or cfg.get("max_output_tokens") or 512),
        "wait_seconds": WAIT_SECONDS,
        "structured": True,
        "image_base64": prepared["b64"],
    }
    row: Dict[str, Any] = {
        "run_id": run_id, "config": cfg["name"], "model_requested": cfg["model"],
        "image_id": image["id"], "kind": image.get("kind", "photo"), "repeat": repeat,
        "long_side_px": cfg.get("long_side_px"), "img_w": prepared["w"], "img_h": prepared["h"],
        "img_bytes": prepared["bytes"], "prompt_chars": len(prompt), "salt": salt,
        "max_output_tokens": body["max_output_tokens"],
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    t0 = time.monotonic()
    status, data, text = http_json(gate, "POST", base + "/api/vision", body, timeout=WAIT_SECONDS + 60)
    polls = 0
    if status < 200 or status >= 300:
        detail = data.get("detail") if isinstance(data.get("detail"), dict) else {}
        row.update(status="failed", http_status=status,
                   error=str(detail.get("error_string") or data.get("error_string") or text)[:400],
                   wall_seconds=round(time.monotonic() - t0, 2))
        return row
    while not data.get("finished_bool") and data.get("task_id_string"):
        if time.monotonic() - t0 > TASK_DEADLINE_SECONDS:
            break
        time.sleep(POLL_SECONDS)
        polls += 1
        st, polled, _ = http_json(gate, "GET", base + "/api/ai/status/" + str(data["task_id_string"]))
        if 200 <= st < 300 and polled:
            data = polled
    wall = time.monotonic() - t0
    answer = str(data.get("answer_string") or "")
    finished = bool(data.get("finished_bool"))
    st_text = str(data.get("status_string") or "").lower()
    row.update({
        "status": "completed" if finished and st_text == "completed" else ("failed" if finished else "timeout"),
        "http_status": status,
        "task_id": data.get("task_id_string"),
        "node": data.get("node_string") or str(data.get("task_id_string") or "").split(".")[0],
        "served_model": data.get("served_model_string"),
        "cache_hit": bool(data.get("cache_hit_bool")),
        "server_elapsed_seconds": data.get("elapsed_seconds_float"),
        "wall_seconds": round(wall, 2),
        "polls": polls,
        "error": str(data.get("error_string") or "")[:400],
        "structured_answer": data.get("structured_answer_bool"),
        "answer": answer,
        "raw_answer": str(data.get("raw_answer_string") or "")[:6000],
        "reasoning_chars": len(str(data.get("reasoning_string") or "")),
    })
    # With AI_MODEL_SINGLE the backend serves every request with its one model,
    # whatever was asked: such a row is not a measurement of the asked model.
    served = str(row.get("served_model") or "")
    row["model_mismatch"] = bool(served) and served != cfg["model"]
    if row["model_mismatch"] and row["status"] == "completed":
        row["status"] = "wrong_model"
    row.update(score_answer(answer, exact_keywords))
    row["has_think_tag"] = row["has_think_tag"] or "<think>" in row["raw_answer"].lower()
    return row


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base", default=os.environ.get("VISION_AB_BASE", DEFAULT_BASE))
    ap.add_argument("--images", default=os.path.join(HERE, "images.json"))
    ap.add_argument("--configs-file", default=os.path.join(HERE, "configs.json"))
    ap.add_argument("--prompt-file", default=os.path.join(HERE, "mplus_prompt.json"))
    ap.add_argument("--configs", default="A,B,C", help="comma list of config names")
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--limit", type=int, default=0, help="only the first N images (smoke test)")
    ap.add_argument("--concurrency", type=int, default=2,
                    help="tasks in flight; HTTP calls stay <= 1/s regardless")
    ap.add_argument("--gap", type=float, default=1.1, help="seconds between any two HTTP calls")
    ap.add_argument("--seed", type=int, default=20261106, help="order shuffle seed")
    ap.add_argument("--out", default=os.path.join(HERE, "results", "results.jsonl"))
    ap.add_argument("--max-output-tokens", type=int, default=0, help="override configs.json for every arm")
    ap.add_argument("--skip-preflight", action="store_true")
    args = ap.parse_args(argv)

    configs = {c["name"]: c for c in load_json(args.configs_file)["configs"]}
    wanted = [n.strip() for n in args.configs.split(",") if n.strip()]
    missing = [n for n in wanted if n not in configs]
    if missing:
        ap.error("unknown config(s): %s" % ", ".join(missing))
    prompt_doc = load_json(args.prompt_file)
    prompts = {k: prompt_doc[k] for k in ("photo", "ai", "video") if k in prompt_doc}
    exact = int(prompt_doc.get("exact_keywords_count") or 50)
    images = load_json(args.images)["images"]
    if args.limit:
        images = images[:args.limit]
    gate = Gate(args.gap)

    if not args.skip_preflight:
        st, models, text = http_json(gate, "GET", args.base + "/api/ai/models")
        listed = {str(m.get("id")) for m in (models.get("models_array") or []) if isinstance(m, dict)}
        print("models listed by %s: %s (default %s)" % (args.base, ", ".join(sorted(listed)) or text,
                                                        models.get("default_model_string")))
        for name in wanted:
            if configs[name]["model"] not in listed:
                print("WARNING: config %s asks for %r, which /api/ai/models does not list - "
                      "its requests will fail (unknown_model / model_unavailable) until the model "
                      "is registered and installed on a node." % (name, configs[name]["model"]))

    os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
    skip = done_keys(args.out)
    run_id = uuid.uuid4().hex[:8]
    print("run %s -> %s (%d already done)" % (run_id, args.out, len(skip)))

    # Prepare every (image, size) once, the way M+ does it.
    raw_cache: Dict[str, bytes] = {}
    prepared: Dict[Tuple[str, int], Dict[str, Any]] = {}
    sizes = sorted({int(configs[n].get("long_side_px") or 1024) for n in wanted})
    for img in images:
        try:
            raw_cache[img["id"]] = fetch_bytes(gate, img["url"])
        except Exception as exc:
            print("skip image %s: download failed (%s)" % (img["id"], exc))
            continue
        for side in sizes:
            jpeg, w, h = standardize(raw_cache[img["id"]], side)
            prepared[(img["id"], side)] = {"b64": base64.b64encode(jpeg).decode("ascii"),
                                           "w": w, "h": h, "bytes": len(jpeg)}
    images = [i for i in images if i["id"] in raw_cache]

    # Interleave configs per (image, repeat) so time-of-day load hits all arms alike.
    jobs = []
    for rep in range(1, args.repeats + 1):
        for img in images:
            arms = list(wanted)
            random.Random("%s-%s-%d" % (args.seed, img["id"], rep)).shuffle(arms)
            for name in arms:
                if (name, img["id"], rep) not in skip:
                    jobs.append((configs[name], img, rep))
    print("%d requests to send (%d images x %d configs x %d repeats)" % (
        len(jobs), len(images), len(wanted), args.repeats))

    write_lock = threading.Lock()
    counter = {"n": 0}

    def work(job):
        cfg, img, rep = job
        side = int(cfg.get("long_side_px") or 1024)
        try:
            row = run_one(gate, args.base, cfg, img, rep, prepared[(img["id"], side)], prompts, run_id, exact,
                          args.max_output_tokens)
        except Exception as exc:  # keep the run going; the row says why
            row = {"run_id": run_id, "config": cfg["name"], "model_requested": cfg["model"],
                   "image_id": img["id"], "repeat": rep, "status": "error",
                   "error": "%s: %s" % (type(exc).__name__, exc)}
        with write_lock:
            with open(args.out, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
            counter["n"] += 1
            print("[%d/%d] %s %s r%d -> %s %s s node=%s kw=%s json=%s %s" % (
                counter["n"], len(jobs), row["config"], row["image_id"], row["repeat"], row.get("status"),
                row.get("server_elapsed_seconds"), row.get("node"), row.get("kw_saved_count"),
                row.get("json_valid"), (row.get("error") or "")[:80]), flush=True)

    with ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as pool:
        list(pool.map(work, jobs))
    print("done; summarize with: python summarize.py %s" % args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
