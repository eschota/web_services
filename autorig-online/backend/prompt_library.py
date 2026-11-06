"""Live system prompts (owner 2026-09-30): edit them on /system_prompts, and
the next task that uses one gets the new text - no deploy, no restart.

A graph keeps the system prompt it was saved with. The library does not rewrite
graphs: it keeps a table `sha1(original text) -> new text`, and every place a
system prompt is used asks `resolve()` first. So one edit reaches every graph,
every tab and every node that still carries that text, and a graph saved by an
older tab keeps working.

The server's own standing rules (the expression rule, the sound rule, the
default instructions) are entries too, under `rule:<name>`; `rule()` returns
the edited text or the code's default.

The store is one small JSON file, re-read when it changes on disk, so several
workers see the same table without talking to each other.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("autorig.prompt_library")

STORE = Path(os.getenv("AUTORIG_PROMPT_LIBRARY", "/srv/autorig/data/var/prompts/library.json"))
_lock = threading.Lock()
_cache: Dict[str, Any] = {"mtime": -1.0, "checked": 0.0, "entries": {}, "rules": {}}
RECHECK_SECONDS = 2.0
MAX_TEXT = 12000


def key_of(text: str) -> str:
    """The name of a prompt: its own text, whitespace-normalised."""
    compact = " ".join(str(text or "").split())
    return hashlib.sha1(compact.encode("utf-8")).hexdigest()[:16]


def _read() -> Dict[str, Any]:
    """The table, re-read only when the file changed (checked at most every 2 s)."""
    now = time.time()
    with _lock:
        if now - float(_cache["checked"]) < RECHECK_SECONDS:
            return _cache
        _cache["checked"] = now
        try:
            mtime = STORE.stat().st_mtime
        except OSError:
            _cache.update(mtime=-1.0, entries={}, rules={})
            return _cache
        if mtime == _cache["mtime"]:
            return _cache
        try:
            data = json.loads(STORE.read_text(encoding="utf-8"))
        except Exception as error:
            logger.info("prompt library unreadable, keeping the last table: %s", error)
            return _cache
        entries = {}
        rules = {}
        for row in data.get("entries") or []:
            if not isinstance(row, dict):
                continue
            text = str(row.get("text") or "")
            name = str(row.get("rule") or "")
            if name:
                rules[name] = text
            source = str(row.get("key") or "")
            if source and text:
                entries[source] = text
        _cache.update(mtime=mtime, entries=entries, rules=rules)
        logger.info("prompt library reloaded: %d prompts, %d rules", len(entries), len(rules))
        return _cache


def resolve(text: Optional[str]) -> str:
    """The text to actually use for this system prompt."""
    original = str(text or "")
    if not original.strip():
        return original
    return _read()["entries"].get(key_of(original), original)


def rule(name: str, default: str) -> str:
    """A server rule by name, or the code's own text when it was never edited."""
    return _read()["rules"].get(str(name)) or default


def stamp() -> str:
    """A short mark that changes whenever the table is saved.

    Requests whose answer depends on the library carry this in their cache key,
    so an edit on /system_prompts really renders again instead of returning the
    picture made under the old text.
    """
    data = _read()
    return f"{len(data['entries'])}.{len(data['rules'])}.{float(data['mtime']):.3f}"


def table() -> Dict[str, Any]:
    data = _read()
    return {"entries": dict(data["entries"]), "rules": dict(data["rules"])}


def save(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Write the whole table. A row is {key|rule, text, label?, original?}."""
    clean: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        text = str(row.get("text") or "")[:MAX_TEXT]
        name = str(row.get("rule") or "").strip()[:64]
        source = str(row.get("key") or "").strip()[:32]
        if not text.strip() or not (name or source):
            continue
        clean.append({k: v for k, v in (("key", source), ("rule", name), ("text", text),
                                        ("label", str(row.get("label") or "")[:200]),
                                        ("original", str(row.get("original") or "")[:MAX_TEXT])) if v})
    payload = {"version": 1, "saved_at": time.time(), "entries": clean}
    STORE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STORE.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(STORE)
    with _lock:
        _cache["checked"] = 0.0      # the next reader picks it up at once
    return {"saved_int": len(clean), "path_string": str(STORE)}
