#!/usr/bin/env python3
"""Tables for report.md from run_ab.py results (+ optional judge.py verdicts).

    python summarize.py results/run1.jsonl [--judge results/run1.judge.jsonl] [--report results/run1.report.md]

Prints Markdown: per config p50/p90 of the server time and the wall time,
JSON validity, M+ acceptance, keyword counts, per-node split (configs are only
comparable on the same node / GPU), and the judge's win rate and mean score.
With --report it writes the full report (REPORT_TEMPLATE below, in Russian
for the owner) with these tables filled in; the rest is filled by hand.
"""
from __future__ import annotations

import argparse
import contextlib
import io
import json
import statistics
from collections import defaultdict
from typing import Any, Dict, List, Optional

REPORT_TEMPLATE = """# A/B ключевания M+: Qwen3.5-4B против qwen35-9b-uncensored — отчёт

- Дата прогона: `{date}`, файл результатов: `{results}`
- Какая нода отвечала на B (4B): `____`; на A/C: `____` (GPU каждой — `/api/ai/fleet`)
- llama.cpp на нодах: `b10709` (f13/f7/f11, CUDA 12, GTX 1080 Ti) — сверить; флаги 4B: `____`; флаги 9B: `____`

## Конфигурации

| Имя | Модель | Картинка | max_output_tokens |
|---|---|---|---|
| A | qwen35-9b-uncensored (как сейчас) | 1024 px по длинной стороне, JPEG ~85, base64 (как M+) | 512 |
| B | qwen35-4b (Qwen3.5-4B Q4_K_M + mmproj F16, unsloth) | 1024 px | 512 |
| C | qwen35-9b-uncensored | 768 px | 512 |

Набор: `images.json` — 20 реальных превью M+ из аудита 06.11 + 1 видео (постер вместо среднего кадра).
Промпт: точный промпт M+ (`mplus_prompt.json`, пересобирается `extract_mplus_prompt.js`), `structured: true`,
соль `[ref …]` на каждый запрос (кэш фермы не участвует), ≤1 запрос/с.
Сравнивать скорость только на одной ноде/GPU: f12 (3080 Ti) и f13 (1080 Ti) отличаются в разы.

{tables}

## Критерии принятия (из плана ресёрча)

| Критерий | Порог | B (4B) | C (768 px) |
|---|---|---|---|
| Время p50 (на одной ноде с A) | ≤ 12 с | | |
| Время p90 | ≤ 20 с | | |
| Оценка судьи относительно A | ≥ 95 % | | |
| Валидный JSON | ≥ 99 % | | |
| M+ сохраняет 50 ключей | не хуже A | | |
| `<think>` в ответе | 0 | | |

## Вывод

- Решение: `____` (переводить M+ на 4B / оставить 9B / 9B с 768 px)
- Замечания по ответам (бренды, места, не-английские слова, повторы): `____`
"""


def pct(values: List[float], q: float) -> Optional[float]:
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * q
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def fmt(v: Optional[float], unit: str = "") -> str:
    return "—" if v is None else ("%.1f%s" % (v, unit))


def share(n: int, d: int) -> str:
    return "—" if not d else "%d/%d (%.0f%%)" % (n, d, 100.0 * n / d)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("results")
    ap.add_argument("--judge", default=None)
    ap.add_argument("--report", default=None, help="write the filled report template here")
    args = ap.parse_args()
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        tables(args)
    print(buf.getvalue())
    if args.report:
        import time
        with open(args.report, "w", encoding="utf-8") as fh:
            fh.write(REPORT_TEMPLATE.format(date=time.strftime("%Y-%m-%d"), results=args.results,
                                            tables=buf.getvalue().strip()))
        print("report ->", args.report)
    return 0


def tables(args) -> None:

    rows = [json.loads(line) for line in open(args.results, encoding="utf-8") if line.strip()]
    by_cfg: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_cfg[r["config"]].append(r)

    print("## Speed and validity\n")
    print("| Config | Model | px | requests | completed | server p50 | server p90 | wall p50 | wall p90 "
          "| JSON valid | M+ accepts | kw saved (mean) | 50/50 kw | <think> | cache hits |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for name in sorted(by_cfg):
        rs = by_cfg[name]
        done = [r for r in rs if r.get("status") == "completed"]
        srv = [float(r["server_elapsed_seconds"]) for r in done if r.get("server_elapsed_seconds")]
        wall = [float(r["wall_seconds"]) for r in done if r.get("wall_seconds")]
        kw = [r.get("kw_saved_count") or 0 for r in done]
        print("| %s | %s | %s | %d | %s | %s | %s | %s | %s | %s | %s | %s | %s | %d | %d |" % (
            name, rs[0].get("model_requested"), rs[0].get("long_side_px"), len(rs), share(len(done), len(rs)),
            fmt(pct(srv, .5), " s"), fmt(pct(srv, .9), " s"), fmt(pct(wall, .5), " s"), fmt(pct(wall, .9), " s"),
            share(sum(1 for r in done if r.get("json_valid")), len(done)),
            share(sum(1 for r in done if r.get("mplus_ok")), len(done)),
            fmt(statistics.mean(kw) if kw else None),
            share(sum(1 for k in kw if k >= 50), len(done)),
            sum(1 for r in done if r.get("has_think_tag")), sum(1 for r in done if r.get("cache_hit"))))

    print("\n## Server time by node (compare configs on the same node)\n")
    print("| Node | Config | n | p50 | p90 |")
    print("|---|---|---|---|---|")
    per_node: Dict[tuple, List[float]] = defaultdict(list)
    for r in rows:
        if r.get("status") == "completed" and r.get("server_elapsed_seconds"):
            per_node[(r.get("node") or "?", r["config"])].append(float(r["server_elapsed_seconds"]))
    for (node, name), vs in sorted(per_node.items()):
        print("| %s | %s | %d | %s | %s |" % (node, name, len(vs), fmt(pct(vs, .5), " s"), fmt(pct(vs, .9), " s")))

    failures = [r for r in rows if r.get("status") != "completed"]
    if failures:
        print("\n## Failures\n")
        reasons: Dict[tuple, int] = defaultdict(int)
        for r in failures:
            reasons[(r["config"], (r.get("error") or r.get("status") or "?")[:90])] += 1
        for (name, why), n in sorted(reasons.items()):
            print("- %s: %d × %s" % (name, n, why))

    if args.judge:
        verdicts = [json.loads(line) for line in open(args.judge, encoding="utf-8") if line.strip()]
        print("\n## Judge (blind pairwise)\n")
        print("| Pair | judged | candidate wins | baseline wins | ties | mean score base / cand "
              "| wrong kw base / cand | errors |")
        print("|---|---|---|---|---|---|---|---|")
        by_pair: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for v in verdicts:
            by_pair[v["pair"]].append(v)
        for pair, vs in sorted(by_pair.items()):
            base, cand = pair.split("-", 1)
            ok = [v for v in vs if not v.get("error") and v.get("winner_config")]
            sb = [v["score_" + base] for v in ok if isinstance(v.get("score_" + base), int)]
            sc = [v["score_" + cand] for v in ok if isinstance(v.get("score_" + cand), int)]
            wb = [v["wrong_keywords_" + base] for v in ok if isinstance(v.get("wrong_keywords_" + base), int)]
            wc = [v["wrong_keywords_" + cand] for v in ok if isinstance(v.get("wrong_keywords_" + cand), int)]
            print("| %s | %d | %d | %d | %d | %s / %s (%s) | %s / %s | %d |" % (
                pair, len(ok), sum(1 for v in ok if v["winner_config"] == cand),
                sum(1 for v in ok if v["winner_config"] == base), sum(1 for v in ok if v["winner_config"] == "tie"),
                fmt(statistics.mean(sb) if sb else None), fmt(statistics.mean(sc) if sc else None),
                ("cand = %.0f%% of base" % (100.0 * statistics.mean(sc) / statistics.mean(sb))) if sb and sc else "—",
                fmt(statistics.mean(wb) if wb else None), fmt(statistics.mean(wc) if wc else None),
                sum(1 for v in vs if v.get("error"))))


if __name__ == "__main__":
    raise SystemExit(main())
