#!/usr/bin/env python3
"""Safe edits of the prod regression corpus (2026-10-11).

Several agents copied whole corpus.json files over prod and silently dropped each other's cases, thresholds and
XFAILs. This tool is the only way to change it: under one lock it reads the CURRENT file, changes only the cases
you name (by id), keeps a timestamped backup, and refuses to drop cases.

  sudo corpus_edit.py upsert <case.json>          # add or replace one whole case (its "id")
  sudo corpus_edit.py xfail <case_id> <check_id> --reason R --owner O
  sudo corpus_edit.py unxfail <case_id> <check_id>
  sudo corpus_edit.py set <case_id> <check_id> <threshold_key> '<json value>'
  corpus_edit.py history                           # backups, newest first
"""
import argparse
import datetime as dt
import fcntl
import json
import os
import pathlib
import shutil
import sys

CORPUS = pathlib.Path(os.environ.get("AUTORIG_CORPUS", "/srv/autorig/autotests/corpus.json"))
HIST = CORPUS.parent / "corpus.history"
LOCK = "/run/lock/autorig-corpus.lock"


def cases_of(d):
    return d["cases"] if isinstance(d, dict) and "cases" in d else d


def find(cs, cid):
    for c in cs:
        if c.get("id") == cid:
            return c
    raise SystemExit(f"no case {cid}")


def check_of(c, chk):
    for ch in c.get("checks", []):
        if ch.get("id") == chk:
            return ch
    raise SystemExit(f"no check {chk} in {c.get('id')}")


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    u = sub.add_parser("upsert"); u.add_argument("file")
    x = sub.add_parser("xfail"); x.add_argument("case"); x.add_argument("check")
    x.add_argument("--reason", required=True); x.add_argument("--owner", required=True)
    ux = sub.add_parser("unxfail"); ux.add_argument("case"); ux.add_argument("check")
    s = sub.add_parser("set"); s.add_argument("case"); s.add_argument("check"); s.add_argument("key"); s.add_argument("value")
    sub.add_parser("history")
    a = ap.parse_args()
    if a.cmd == "history":
        for p in sorted(HIST.glob("corpus.*.json"), reverse=True)[:20]:
            print(p.name, p.stat().st_size)
        return
    with open(LOCK, "w") as lk:
        fcntl.flock(lk, fcntl.LOCK_EX)
        d = json.loads(CORPUS.read_text())
        cs = cases_of(d)
        before = {c.get("id") for c in cs}
        if a.cmd == "upsert":
            new = json.loads(pathlib.Path(a.file).read_text())
            idx = next((i for i, c in enumerate(cs) if c.get("id") == new.get("id")), None)
            if idx is None:
                cs.append(new)
            else:
                cs[idx] = new
        elif a.cmd == "xfail":
            check_of(find(cs, a.case), a.check)["xfail"] = {"reason": a.reason, "owner": a.owner}
        elif a.cmd == "unxfail":
            check_of(find(cs, a.case), a.check).pop("xfail", None)
        elif a.cmd == "set":
            check_of(find(cs, a.case), a.check).setdefault("thresholds", {})[a.key] = json.loads(a.value)
        if not before <= {c.get("id") for c in cs}:
            raise SystemExit("refused: cases would be dropped")
        HIST.mkdir(exist_ok=True)
        shutil.copy2(CORPUS, HIST / f"corpus.{dt.datetime.utcnow():%Y%m%dT%H%M%S}.json")
        tmp = CORPUS.with_suffix(".tmp")
        tmp.write_text(json.dumps(d, ensure_ascii=False, indent=1))
        os.replace(tmp, CORPUS)
        print(f"ok: {a.cmd} {getattr(a, 'case', '')} ({len(cs)} cases)")


if __name__ == "__main__":
    sys.exit(main())
