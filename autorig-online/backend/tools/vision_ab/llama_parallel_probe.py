#!/usr/bin/env python3
"""How much does llama-server gain from 2-3 requests at once on this card? Run ON the node box.

Talks to the node's already running llama-server (127.0.0.1:<llama_port>, it has -np auto = 4 slots) directly,
changes nothing: 1 request alone, then 2 and 3 at once, M+ prompt + one image, fresh prefix each (no prompt
cache reuse). Prints aggregate output tokens/s per level. If the model is not loaded it says so and exits.

    python llama_parallel_probe.py --config C:\\ProgramData\\AutoRig\\ai-node\\config.json \
        --prompt-file mplus_prompt.json --image-url https://.../x_nwp.jpg
"""
import argparse
import base64
import json
import sys
import threading
import time
import urllib.request
import uuid


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--prompt-file", required=True)
    ap.add_argument("--image-url", required=True)
    ap.add_argument("--max-tokens", type=int, default=400)
    ap.add_argument("--levels", default="1,2,3")
    args = ap.parse_args()
    cfg = json.load(open(args.config, encoding="utf-8-sig"))
    base = "http://127.0.0.1:%d" % int(cfg.get("llama_port") or 8092)
    try:
        urllib.request.urlopen(base + "/health", timeout=3).read()
    except Exception as exc:
        print(json.dumps({"error": "llama-server not running: %s" % exc}))
        return 2
    prompt = json.load(open(args.prompt_file, encoding="utf-8"))["photo"]
    req = urllib.request.Request(args.image_url, headers={"User-Agent": "AutoRig-AIVision/1"})
    image = base64.b64encode(urllib.request.urlopen(req, timeout=30).read()).decode("ascii")
    run = uuid.uuid4().hex[:6]

    def one(tag, out):
        body = {"model": cfg.get("model_id"), "max_tokens": args.max_tokens, "temperature": 0.3,
                "messages": [{"role": "user", "content": [
                    {"type": "text", "text": "[ref lp-%s-%s]\n\n%s" % (run, tag, prompt)},
                    {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64," + image}}]}]}
        r = urllib.request.Request(base + "/v1/chat/completions", data=json.dumps(body).encode(),
                                   headers={"Content-Type": "application/json"})
        t0 = time.monotonic()
        data = json.loads(urllib.request.urlopen(r, timeout=600).read())
        t = data.get("timings") or {}
        out.append({"wall": time.monotonic() - t0, "predicted": t.get("predicted_n"),
                    "eval_tps": t.get("predicted_per_second"), "prompt_n": t.get("prompt_n"),
                    "prompt_ms": t.get("prompt_ms")})

    results = {}
    for level in [int(x) for x in args.levels.split(",")]:
        out, threads = [], []
        t0 = time.monotonic()
        for i in range(level):
            th = threading.Thread(target=one, args=("%d-%d" % (level, i), out))
            th.start()
            threads.append(th)
            time.sleep(0.2)
        for th in threads:
            th.join()
        span = time.monotonic() - t0
        tokens = sum(int(o.get("predicted") or 0) for o in out)
        results[level] = {"span_s": round(span, 2), "output_tokens": tokens,
                          "aggregate_tok_s": round(tokens / span, 1) if span else None,
                          "per_request_eval_tps": [round(o["eval_tps"] or 0, 1) for o in out],
                          "prompt_tokens": [o["prompt_n"] for o in out]}
        print(json.dumps({"level": level, **results[level]}), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
