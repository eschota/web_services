#!/usr/bin/env python3
"""Blind pairwise judging of keywording quality.

For every image and every pair (baseline vs candidate, e.g. A-B and A-C) the
judge sees the image and two metadata sets labelled only "1" and "2" (sides
shuffled with a fixed seed, config names never shown) and returns a verdict.

    python judge.py results/run1.jsonl --pairs A-B,A-C --out results/run1.judge.jsonl

Backends (--backend):
  anthropic        Claude via the official SDK (pip install anthropic). The key
                   comes from ANTHROPIC_API_KEY, or from --env-file (a
                   KEY=value file; the first non-empty ANTHROPIC_API_KEY* line
                   is used, e.g. way-fr /srv/autorig/secrets/anthropic.env).
                   The key is never printed or written anywhere.
  command:<argv>   Any other judge: the command gets one JSON request on stdin
                   ({image_url, prompt, candidate_1, candidate_2}) and prints one
                   JSON verdict (same fields as the schema below).
  export           No model call: writes the blind packets to --out for a human
                   or another agent to fill in (verdict fields left empty).
"""
from __future__ import annotations

import argparse
import itertools
import json
import os
import random
import shlex
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_MODEL = "claude-opus-5-5"

RUBRIC = """You are a senior microstock reviewer (Adobe Stock, Shutterstock). Two keywording results
for the SAME image are shown, labelled 1 and 2. They come from two anonymous systems; judge only
what you can verify in the image.

Score each result 1-5 on overall commercial usefulness, weighing:
- title and description: accurate, specific, describe what is visible, natural English;
- keywords: relevant to visible content, most important first, specific (not generic filler),
  buyer search value; no invented objects, places, brands, people names or non-English words;
- count each keyword that is wrong for this image (hallucinated or irrelevant) as an error.

Then pick the better one ("1", "2", or "tie" when they are practically equal).
Do not reward length for its own sake. Answer only with the JSON object."""

VERDICT_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["winner", "score_1", "score_2", "wrong_keywords_1", "wrong_keywords_2", "reason"],
    "properties": {
        "winner": {"type": "string", "enum": ["1", "2", "tie"]},
        "score_1": {"type": "integer"},
        "score_2": {"type": "integer"},
        "wrong_keywords_1": {"type": "integer"},
        "wrong_keywords_2": {"type": "integer"},
        "reason": {"type": "string"},
    },
}


def read_key(env_file: Optional[str]) -> str:
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if key or not env_file:
        return key
    with open(env_file, "r", encoding="utf-8") as fh:
        for line in fh:
            name, sep, value = line.strip().partition("=")
            if sep and name.strip().startswith("ANTHROPIC_API_KEY"):
                value = value.strip().strip('"').strip("'")
                if value:
                    return value
    return ""


def candidate(row: Dict[str, Any]) -> Dict[str, Any]:
    return {"title": row.get("title", ""), "description": row.get("description", ""),
            "keywords": row.get("keywords") or []}


def judge_anthropic(client, model: str, request: Dict[str, Any]) -> Dict[str, Any]:
    text = (RUBRIC + "\n\nResult 1:\n" + json.dumps(request["candidate_1"], ensure_ascii=False, indent=1)
            + "\n\nResult 2:\n" + json.dumps(request["candidate_2"], ensure_ascii=False, indent=1))
    response = client.beta.messages.create(
        model=model,
        max_tokens=4000,
        output_config={"effort": "medium",
                       "format": {"type": "json_schema", "schema": VERDICT_SCHEMA}},
        # Refusal fallbacks (on by default for this model family): a declined
        # request is re-run on a fallback model inside the same call.
        betas=["server-side-fallback-2026-07-01"],
        fallbacks="default",
        messages=[{"role": "user", "content": [
            {"type": "image", "source": {"type": "url", "url": request["image_url"]}},
            {"type": "text", "text": text},
        ]}],
    )
    if response.stop_reason == "refusal":
        return {"error": "refusal"}
    body = "".join(block.text for block in response.content if block.type == "text")
    verdict = json.loads(body)
    verdict["judge_model"] = response.model
    return verdict


def judge_command(argv: List[str], request: Dict[str, Any]) -> Dict[str, Any]:
    proc = subprocess.run(argv, input=json.dumps(request), capture_output=True, text=True, timeout=600)
    if proc.returncode != 0:
        return {"error": "judge command exit %d: %s" % (proc.returncode, proc.stderr[-300:])}
    return json.loads(proc.stdout)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("results", help="JSONL written by run_ab.py")
    ap.add_argument("--pairs", default="A-B,A-C", help="baseline-candidate pairs, comma separated")
    ap.add_argument("--images", default=os.path.join(HERE, "images.json"))
    ap.add_argument("--repeats", default="1", help="which repeats to judge: 1 | all")
    ap.add_argument("--backend", default="anthropic")
    ap.add_argument("--model", default=os.environ.get("VISION_AB_JUDGE_MODEL", DEFAULT_MODEL))
    ap.add_argument("--env-file", default=None, help="file with ANTHROPIC_API_KEY=... (not printed)")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--out", default=None)
    args = ap.parse_args(argv)

    urls = {i["id"]: i["url"] for i in json.load(open(args.images, encoding="utf-8"))["images"]}
    rows: Dict[tuple, Dict[str, Any]] = {}
    with open(args.results, encoding="utf-8") as fh:
        for line in fh:
            row = json.loads(line)
            if row.get("status") == "completed" and row.get("mplus_ok"):
                rows[(row["config"], row["image_id"], row["repeat"])] = row
    pairs = [tuple(p.split("-", 1)) for p in args.pairs.split(",") if "-" in p]
    out = args.out or os.path.splitext(args.results)[0] + ".judge.jsonl"

    client = None
    if args.backend == "anthropic":
        key = read_key(args.env_file)
        if not key:
            print("No ANTHROPIC_API_KEY (env or --env-file). Use --backend export or command:<argv>.")
            return 2
        import anthropic  # pip install anthropic
        client = anthropic.Anthropic(api_key=key)
        del key

    rng = random.Random(args.seed)
    repeats = sorted({k[2] for k in rows}) if args.repeats == "all" else [int(args.repeats)]
    n = 0
    with open(out, "a", encoding="utf-8") as fh:
        for (base, cand), image_id, rep in itertools.product(pairs, sorted(urls), repeats):
            left, right = rows.get((base, image_id, rep)), rows.get((cand, image_id, rep))
            if not left or not right:
                continue
            swap = rng.random() < 0.5
            first, second = (right, left) if swap else (left, right)
            request = {"image_url": urls[image_id], "prompt": RUBRIC,
                       "candidate_1": candidate(first), "candidate_2": candidate(second)}
            record: Dict[str, Any] = {"pair": "%s-%s" % (base, cand), "image_id": image_id, "repeat": rep,
                                      "side_1": first["config"], "side_2": second["config"],
                                      "judged_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
            if args.backend == "export":
                record.update(request=request, winner=None)
            else:
                try:
                    verdict = (judge_anthropic(client, args.model, request) if client
                               else judge_command(shlex.split(args.backend.split(":", 1)[1]), request))
                except Exception as exc:
                    verdict = {"error": "%s: %s" % (type(exc).__name__, str(exc)[:300])}
                record.update(verdict)
                # Map the blind labels back to configs.
                w = verdict.get("winner")
                record["winner_config"] = {"1": first["config"], "2": second["config"]}.get(w, "tie" if w == "tie" else None)
                for side, row in (("1", first), ("2", second)):
                    record["score_" + row["config"]] = verdict.get("score_" + side)
                    record["wrong_keywords_" + row["config"]] = verdict.get("wrong_keywords_" + side)
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            fh.flush()
            n += 1
            print("%s %s r%d -> %s" % (record["pair"], image_id, rep, record.get("winner_config") or record.get("error") or "exported"))
    print("%d judgements -> %s" % (n, out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
