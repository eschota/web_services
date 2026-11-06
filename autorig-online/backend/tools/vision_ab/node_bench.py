#!/usr/bin/env python3
"""Before/after timing of one ai-node, run ON the node box itself (stdlib only).

Sends N identical vision requests straight to the local ai_node
(127.0.0.1:<listen_port>/api-converter-glb/ai-vision), one at a time, each
with a fresh "[ref …]" line at the START of the prompt so llama-server's
prompt cache cannot reuse the previous request's image prefill. Records the
node's own elapsed_seconds plus llama-server's print_timing lines (prompt
eval / eval tokens and tok/s) appended to llama_log_file during the run.

    python node_bench.py --config C:\\ProgramData\\AutoRig\\ai-node\\config.json \
        --prompt-file mplus_prompt.json --image-url https://pvs3.microstock.plus/..._p.jpg \
        --n 5 --warmup 1 --label before --out bench_before.json

The token is read from the node's token file and never printed.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import sys
import time
import urllib.error
import urllib.request
import uuid

TIMING_RE = re.compile(r"(prompt eval time|       eval time) =\s+([\d.]+) ms /\s+(\d+) tokens.*?([\d.]+) tokens per second")


def call(base, token, method, path, body=None, timeout=60):
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method, headers={
        "Authorization": "Bearer " + token, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8") or "{}")
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, json.loads(exc.read().decode("utf-8") or "{}")
        except ValueError:
            return exc.code, {}


def _degenerate(text):
    """Same test as backend ai_vision_api._degenerate_answer: nearly all one repeated non-word character."""
    compact = "".join(text.split())
    if len(compact) < 24:
        return False
    char = max(set(compact), key=compact.count)
    return not char.isalnum() and compact.count(char) >= 0.9 * len(compact)


def log_size(path):
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


def read_timings(path, start):
    try:
        with open(path, "rb") as fh:
            fh.seek(start if log_size(path) >= start else 0)
            text = fh.read().decode("utf-8", "replace")
    except OSError:
        return []
    out, cur = [], {}
    for m in TIMING_RE.finditer(text):
        kind = "prompt" if m.group(1).startswith("prompt") else "eval"
        cur[kind] = {"ms": float(m.group(2)), "tokens": int(m.group(3)), "tps": float(m.group(4))}
        if kind == "eval":
            out.append(cur)
            cur = {}
    return out


def run_concurrent(args, base, token, llama_log, model, prompt, run_id, result):
    """Warm-up (sequential), then N requests submitted back to back (1/s): files per minute."""
    for i in range(args.warmup):
        body = {"prompt": "[ref nbw-%s-%s-%d]\n\n%s" % (args.label, run_id, i, prompt), "image_url": args.image_url,
                "max_output_tokens": args.max_output_tokens, "model": model}
        code, accepted = call(base, token, "POST", "/ai-vision", body)
        while code == 202:
            time.sleep(1.0)
            _, st = call(base, token, "GET", "/ai-vision/status/" + accepted["task_id"])
            if st.get("status") in ("Completed", "Failed"):
                result["warmup"] = {"status": st.get("status"), "node_elapsed": st.get("elapsed_seconds")}
                break
    mark = log_size(llama_log) if llama_log else 0
    t0 = time.monotonic()
    tasks = []
    for i in range(args.n):
        body = {"prompt": "[ref nbc-%s-%s-%d]\n\n%s" % (args.label, run_id, i, prompt), "image_url": args.image_url,
                "max_output_tokens": args.max_output_tokens, "model": model}
        code, accepted = call(base, token, "POST", "/ai-vision", body)
        tasks.append({"i": i, "submit_http": code, "submitted": round(time.monotonic() - t0, 2),
                      "task_id": accepted.get("task_id"), "error": None if code == 202 else accepted})
        time.sleep(1.0)
    pending = [t for t in tasks if t["task_id"]]
    while pending and time.monotonic() - t0 < 900:
        time.sleep(2.0)
        for t in list(pending):
            _, st = call(base, token, "GET", "/ai-vision/status/" + t["task_id"])
            if st.get("status") in ("Completed", "Failed"):
                t.update(status=st.get("status"), error=st.get("error") or "", done=round(time.monotonic() - t0, 2),
                         node_elapsed=st.get("elapsed_seconds"), completion_tokens=(st.get("model_usage") or {}).get("completion_tokens"))
                pending.remove(t)
    for t in tasks:
        t.pop("task_id", None)
    done = [t for t in tasks if t.get("status") == "Completed"]
    span = max((t["done"] for t in done), default=0.0)
    timings = read_timings(llama_log, mark) if llama_log else []
    result["runs"] = tasks
    result["llama_timings"] = timings
    result["summary"] = {
        "mode": "concurrent", "completed": len(done), "of": args.n,
        "span_s": span, "files_per_min": round(60.0 * len(done) / span, 2) if span else None,
        "latency_median_s": round(statistics.median([t["done"] - t["submitted"] for t in done]), 2) if done else None,
        "eval_tps_median": round(statistics.median([x["eval"]["tps"] for x in timings]), 2) if timings else None,
        "completion_tokens_total": sum(int(t.get("completion_tokens") or 0) for t in done),
    }
    if span:
        result["summary"]["output_tokens_per_s"] = round(result["summary"]["completion_tokens_total"] / span, 1)
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(result, fh, indent=1)
    for t in tasks:
        print(json.dumps(t), flush=True)
    print(json.dumps(result["summary"]), flush=True)
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--prompt-file", required=True)
    ap.add_argument("--image-url", required=True,
                    help="one URL, or several separated by commas (sequential mode cycles them)")
    ap.add_argument("--n", type=int, default=5)
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--max-output-tokens", type=int, default=640)
    ap.add_argument("--concurrent", action="store_true",
                    help="submit all N at once (1/s) and report files/min instead of one-at-a-time latency")
    ap.add_argument("--model", default="", help="model id (default: the config's model_id)")
    ap.add_argument("--label", default="run")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    cfg_dir = os.path.dirname(os.path.abspath(args.config))
    with open(args.config, encoding="utf-8-sig") as fh:
        cfg = json.load(fh)
    token_path = cfg.get("token_file") or "token.txt"
    token_path = token_path if os.path.isabs(token_path) else os.path.join(cfg_dir, token_path)
    with open(token_path, encoding="utf-8-sig") as fh:
        token = fh.read().strip()
    base = "http://127.0.0.1:%d/api-converter-glb" % int(cfg.get("listen_port") or 5480)
    llama_log = cfg.get("llama_log_file") or ""
    llama_log = llama_log if (not llama_log or os.path.isabs(llama_log)) else os.path.join(cfg_dir, llama_log)
    model = args.model or cfg.get("model_id")
    with open(args.prompt_file, encoding="utf-8") as fh:
        prompt = json.load(fh)["photo"]

    status, server = call(base, token, "GET", "/server-status")
    result = {"label": args.label, "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "model": model, "image_url": args.image_url, "extra_llama_args": cfg.get("extra_llama_args"),
              "server_version": server.get("server_version"), "loaded_model_before": (server.get("ai_models") or {}).get("loaded_model"),
              "runs": []}
    run_id = uuid.uuid4().hex[:6]
    if args.concurrent:
        return run_concurrent(args, base, token, llama_log, model, prompt, run_id, result)
    for i in range(args.warmup + args.n):
        warm = i < args.warmup
        mark = log_size(llama_log) if llama_log else 0
        urls = [u for u in args.image_url.split(",") if u]  # several URLs: request i uses urls[i % len]
        body = {"prompt": "[ref nb-%s-%s-%d]\n\n%s" % (args.label, run_id, i, prompt), "image_url": urls[i % len(urls)],
                "max_output_tokens": args.max_output_tokens, "model": model}
        t0 = time.monotonic()
        code, accepted = call(base, token, "POST", "/ai-vision", body)
        row = {"i": i, "warmup": warm, "submit_http": code}
        if code != 202:
            row["error"] = accepted.get("error") or accepted
            result["runs"].append(row)
            print(json.dumps(row), flush=True)
            time.sleep(5)
            continue
        task_id = accepted["task_id"]
        st = {}
        while time.monotonic() - t0 < 600:
            time.sleep(1.0)
            _, st = call(base, token, "GET", "/ai-vision/status/" + task_id)
            if st.get("status") in ("Completed", "Failed"):
                break
        row.update(status=st.get("status"), error=st.get("error") or "", node_elapsed=st.get("elapsed_seconds"),
                   wall=round(time.monotonic() - t0, 2), answer_chars=len(st.get("answer") or ""),
                   answer_head=str(st.get("answer") or "")[:60],
                   degenerate=_degenerate(str(st.get("answer") or "")),
                   reasoning_chars=len(st.get("reasoning") or ""), usage=st.get("model_usage"))
        timings = read_timings(llama_log, mark) if llama_log else []
        # Other callers may share the node: our request is the one with the largest prompt in this window.
        if timings:
            row["llama"] = max(timings, key=lambda t: t.get("prompt", {}).get("tokens", 0))
            row["llama_lines_in_window"] = len(timings)
        result["runs"].append(row)
        print(json.dumps(row), flush=True)

    done = [r for r in result["runs"] if not r["warmup"] and r.get("status") == "Completed"]
    def med(key):
        vals = [key(r) for r in done]
        vals = [v for v in vals if v is not None]
        return round(statistics.median(vals), 2) if vals else None
    result["summary"] = {
        "completed": len(done), "of": args.n,
        "node_elapsed_median_s": med(lambda r: r.get("node_elapsed")),
        "prompt_tokens_median": med(lambda r: r.get("llama", {}).get("prompt", {}).get("tokens")),
        "prompt_ms_median": med(lambda r: r.get("llama", {}).get("prompt", {}).get("ms")),
        "prompt_tps_median": med(lambda r: r.get("llama", {}).get("prompt", {}).get("tps")),
        "eval_tokens_median": med(lambda r: r.get("llama", {}).get("eval", {}).get("tokens")),
        "eval_tps_median": med(lambda r: r.get("llama", {}).get("eval", {}).get("tps")),
    }
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(result, fh, indent=1)
    print(json.dumps(result["summary"]), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
