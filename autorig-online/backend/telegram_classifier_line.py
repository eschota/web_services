"""The classifier's verdict as the FIRST line of every AutoRig task notice (owner 2026-10-20 ~14:45 UTC, register §15:
«сверху тут сначала пиши Классификатор как определил объект, потому что сейчас непонятно не ошибся ли он»).

``line(run_id, lang)`` -> one HTML line, e.g.
  «🧭 <b>Классификатор:</b> bird · yellow chick · конечности biped · уверенность 1.0 · голоса согласны
   (воксели 2 ноги · Vision 2 ноги · соседи bird 4/7)»
read from the V3 run's ``analysis/class.json`` (category, variant, confidence, agree, needs_review, the three voters
voxels / Vision / neighbours, limbs.verdict) and ``analysis/category.json`` (the final category, body, what).
⚠️ in front when the voters disagree, the run needs review, the confidence is under ``LOW_CONFIDENCE`` (0.7), fewer
than two voters voted (the others abstained), or the classifier's own category is unknown / differs from the final one.
No class.json (or no run): «🧭 Классификатор: нет данных». Never raw JSON; every value is HTML-escaped.
Languages: ru (the operators' broadcast chats) and en (fallback for every other customer language).
"""
from __future__ import annotations

import html
import json
import os
import re
from collections import Counter
from pathlib import Path

RUNS_DIR = Path(os.getenv("AUTORIG_MT_RUNS_DIR", "/srv/autorig/data/motion_transfer/runs"))
LOW_CONFIDENCE = float(os.getenv("AUTORIG_TG_CLASS_LOW_CONFIDENCE", "0.7"))
_RUN = re.compile(r"^[0-9a-f]{20}$")

TEXT = {
    "ru": {"title": "Классификатор", "none": "нет данных", "conf": "уверенность", "agree": "голоса согласны",
           "disagree": "голоса расходятся", "review": "на проверку", "limbs": "конечности", "body": "тело",
           "class": "классификатор", "voxels": "воксели", "vision": "Vision", "neigh": "соседи", "abstain": "нет голоса",
           "legs": {"none": "без ног", "zero": "без ног", "one": "1 нога", "two": "2 ноги", "three": "3 ноги",
                    "four": "4 ноги", "six": "6 ног", "eight": "8 ног", "many": "много ног"}},
    "en": {"title": "Classifier", "none": "no data", "conf": "confidence", "agree": "voters agree",
           "disagree": "voters disagree", "review": "needs review", "limbs": "limbs", "body": "body",
           "class": "classifier", "voxels": "voxels", "vision": "Vision", "neigh": "neighbours", "abstain": "abstain",
           "legs": {"none": "no legs", "zero": "no legs", "one": "1 leg", "two": "2 legs", "three": "3 legs",
                    "four": "4 legs", "six": "6 legs", "eight": "8 legs", "many": "many legs"}},
}


def lang_of(code) -> str:
    """The notice language for a language code: ru, else en."""
    c = str(code or "").strip().lower()[:2]
    return c if c in TEXT else "en"


def _load(path: Path):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except (OSError, ValueError):
        return None


def load(run_id, runs_dir: Path | None = None):
    """-> (class.json, category.json) of the run; (None, None) when the run is unknown."""
    if not run_id or not _RUN.match(str(run_id)):
        return None, None
    base = Path(runs_dir or RUNS_DIR) / str(run_id) / "analysis"
    return _load(base / "class.json"), _load(base / "category.json")


def _word(value, limit: int = 40) -> str:
    return html.escape(re.sub(r"\s+", " ", str(value or "")).strip()[:limit])


def _conf(value) -> str | None:
    try:
        c = float(value)
    except (TypeError, ValueError):
        return None
    s = f"{c:.2f}".rstrip("0")
    return s + "0" if s.endswith(".") else s


def _abstained(voter) -> bool:
    return not isinstance(voter, dict) or str(voter.get("vote") or "abstain").strip().lower() == "abstain"


def _legs(voter, t) -> str:
    if _abstained(voter):
        return t["abstain"]
    legs = str(voter.get("legs") or "").strip().lower()
    if legs:
        return t["legs"].get(legs, _word(legs, 16))
    return _word(voter.get("vote") or "-", 16)


def _neighbours(voter, t) -> str:
    if _abstained(voter):
        return t["abstain"]
    rows = [r for r in (voter.get("neighbours") or []) if isinstance(r, dict)]
    cats = Counter(str(r.get("category") or "").strip() for r in rows if str(r.get("category") or "").strip())
    if cats:
        cat, n = cats.most_common(1)[0]
        return f"{_word(cat, 20)} {n}/{len(rows)}"
    return _word(voter.get("vote") or "-", 16)


def render(cls, cat, lang: str = "ru") -> str:
    """The line from the two dicts (either may be None)."""
    t = TEXT[lang_of(lang)]
    if not isinstance(cls, dict):
        return f"🧭 <b>{t['title']}:</b> {t['none']}"
    cat = cat if isinstance(cat, dict) else {}
    cls_cat = str(cls.get("category") or "").strip()
    final = str(cat.get("category") or "").strip() or cls_cat
    variant = str(cls.get("variant") or cat.get("subcategory") or "").strip()
    head = _word(final or "?", 24) + (f"/{_word(variant, 24)}" if variant else "")
    parts = [head]
    if cls_cat and final and cls_cat != final:                       # the final category overrode the classifier
        parts.append(f"{t['class']} {_word(cls_cat, 24)}")
    what = cat.get("what") or ((cls.get("garments_evidence") or {}).get("vision") or {}).get("what")
    if what:
        parts.append(_word(what, 40))
    body = str(cat.get("body") or "").strip()
    if body and body != final:
        parts.append(f"{t['body']} {_word(body, 20)}")
    limbs = (cls.get("limbs") or {}).get("verdict") if isinstance(cls.get("limbs"), dict) else None
    if limbs:
        parts.append(f"{t['limbs']} {_word(limbs, 20)}")
    conf = _conf(cls.get("confidence"))
    if conf is not None:
        parts.append(f"{t['conf']} {conf}")
    agree = cls.get("agree")
    needs_review = bool(cls.get("needs_review"))
    parts.append(t["agree"] if agree is True else t["disagree"])
    if needs_review:
        parts.append(t["review"])
    try:
        low = float(cls.get("confidence")) < LOW_CONFIDENCE
    except (TypeError, ValueError):
        low = True
    voters = cls.get("voters") if isinstance(cls.get("voters"), dict) else {}
    voted = sum(1 for k in ("voxels", "vision", "neighbours") if not _abstained(voters.get(k)))
    warn = (agree is not True or needs_review or low or voted < 2
            or cls_cat.lower() in ("", "unknown") or (bool(final) and cls_cat != final))
    votes = (f"{t['voxels']} {_legs(voters.get('voxels'), t)} · {t['vision']} {_legs(voters.get('vision'), t)} · "
             f"{t['neigh']} {_neighbours(voters.get('neighbours'), t)}")
    return ("⚠️ " if warn else "") + f"🧭 <b>{t['title']}:</b> " + " · ".join(parts) + f" ({votes})"


def line(run_id, lang: str = "ru", runs_dir: Path | None = None) -> str:
    """The first line of the notice for this run; «нет данных» on any failure, never an exception."""
    try:
        cls, cat = load(run_id, runs_dir)
        return render(cls, cat, lang)
    except Exception as exc:                                         # noqa: BLE001 - the notice goes out anyway
        print(f"[Telegram] classifier line for run {run_id} failed: {type(exc).__name__}")
        return render(None, None, lang)


def prepend(text: str, first_line: str) -> str:
    return f"{first_line}\n{text}" if first_line else text
