"""Parsing and scoring of a keywording answer, the way M+ reads it.

Ported from MicrostockPlus src/libs/mpcore/renderfinvisionapi.ts
(parseAIMetaAnswer, normalizeKeywordsList, salvageTruncatedObject), so the
A/B counts what M+ would actually save. Standard library only.
"""
from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional

# A keyword is a word or a phrase of up to three words: letters, digits,
# hyphens inside words, single spaces (renderfinvisionapi.ts KEYWORD_PATTERN).
_WORD = r"[^\W_]+(?:-[^\W_]+)*"
KEYWORD_RE = re.compile(r"^%s(?: %s){0,2}$" % (_WORD, _WORD), re.UNICODE)
FENCE_RE = re.compile(r"```(?:json)?\s*([\s\S]*?)```", re.IGNORECASE)
THINK_RE = re.compile(r"<think>|</think>", re.IGNORECASE)


def strip_code_fences(text: str) -> str:
    m = FENCE_RE.search(text)
    if m:
        return m.group(1).strip()
    text = re.sub(r"^\s*```(?:json)?", "", text, flags=re.IGNORECASE)
    return re.sub(r"```\s*$", "", text).strip()


def extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    stripped = strip_code_fences(text)
    candidates = [stripped]
    first, last = stripped.find("{"), stripped.rfind("}")
    if first >= 0 and last > first:
        candidates.append(stripped[first:last + 1])
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except ValueError:
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


def salvage_truncated_object(text: str) -> Optional[Dict[str, Any]]:
    stripped = strip_code_fences(text)

    def read_string(field: str) -> str:
        m = re.search(r'"%s"\s*:\s*"((?:[^"\\]|\\.)*)"' % field, stripped)
        if not m:
            return ""
        try:
            return json.loads('"' + m.group(1) + '"')
        except ValueError:
            return m.group(1)

    start = re.search(r'"keywords"\s*:\s*\[', stripped)
    if not start:
        return None
    keywords: List[str] = []
    item = re.compile(r'\s*"((?:[^"\\]|\\.)*)"\s*(,|\])')
    pos = start.end()
    while True:
        m = item.match(stripped, pos)
        if not m:
            break
        keywords.append(m.group(1))
        pos = m.end()
        if m.group(2) == "]":
            break
    return {"title": read_string("title"), "description": read_string("description"), "keywords": keywords}


def normalize_keywords(value: Any, limit: int = 0) -> List[str]:
    if isinstance(value, list):
        raw = value
    elif isinstance(value, str):
        raw = re.split(r"[,;\n]", value)
    else:
        raw = []
    seen = set()
    out: List[str] = []
    for item in raw:
        if not isinstance(item, (str, int, float)) or isinstance(item, bool):
            continue
        kw = re.sub(r"\s+", " ", str(item).strip().lower())
        if not kw or kw in seen or not KEYWORD_RE.match(kw):
            continue
        seen.add(kw)
        out.append(kw)
    return out[:limit] if limit > 0 else out


def score_answer(answer: str, exact_keywords: int = 50) -> Dict[str, Any]:
    """Everything the report needs from one answer.

    json_valid: the answer (fences allowed) is one complete JSON object with
    the three fields - what the prompt asks for.
    mplus_ok: M+ would accept it (strict parse or salvage of a cut-off list),
    i.e. parseAIMetaAnswer() returns non-null. Since M+ 4f70345 (keywords
    first in the JSON) that needs keywords only; a missing title/description
    is topped up by a second call, so title_len/description_len show it.
    """
    text = str(answer or "")
    strict = extract_json_object(text) if text.strip() else None
    record = strict or (salvage_truncated_object(text) if text.strip() else None)
    raw_keywords = record.get("keywords") if isinstance(record, dict) else None
    raw_list = raw_keywords if isinstance(raw_keywords, list) else []
    title = record.get("title").strip() if isinstance(record, dict) and isinstance(record.get("title"), str) else ""
    description = (record.get("description").strip()
                   if isinstance(record, dict) and isinstance(record.get("description"), str) else "")
    all_norm = normalize_keywords(raw_keywords)
    saved = all_norm[:exact_keywords]
    lowered = [re.sub(r"\s+", " ", str(k).strip().lower()) for k in raw_list if isinstance(k, (str, int, float))]
    non_ascii = [k for k in saved if any(ord(ch) > 127 for ch in k)]
    return {
        "json_valid": bool(strict and isinstance(strict.get("keywords"), list)
                           and isinstance(strict.get("title"), str) and isinstance(strict.get("description"), str)),
        "mplus_ok": bool(saved),
        "title": title,
        "description": description,
        "keywords": saved,
        "kw_raw_count": len(raw_list),
        "kw_unique_valid_count": len(all_norm),
        "kw_saved_count": len(saved),
        "kw_duplicates": len(lowered) - len(set(lowered)),
        "kw_rejected_format": sum(1 for k in set(lowered) if k and not KEYWORD_RE.match(k)),
        "kw_non_ascii": len(non_ascii),
        "title_len": len(title),
        "description_len": len(description),
        "has_think_tag": bool(THINK_RE.search(text)),
    }
