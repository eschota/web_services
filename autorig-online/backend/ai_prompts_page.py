"""/system_prompts - every system prompt in one place, edited live.

GET  /api/ai/prompts        every system prompt the saved graphs use, with the
                            edited text when there is one, plus the server's
                            own standing rules.
PUT  /api/ai/prompts        save the table (prompt_library), which the next
                            task picks up without a restart.

Owner-only: the graphs and the server rules are the whole pipeline's wording.
"""
from __future__ import annotations

import json
import logging
import os
import pathlib
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

import prompt_library

logger = logging.getLogger("autorig.prompts_page")

GRAPH_DIR = pathlib.Path(os.getenv("AUTORIG_AI_GRAPH_DIR", "/srv/autorig/data/var/ai-graphs"))


def _server_rules() -> List[Dict[str, Any]]:
    """The standing rules the server adds to prompts, with their code defaults."""
    rows: List[Dict[str, Any]] = []
    try:
        import ai_prompt_edit as edit
        rows += [
            {"rule": "expression", "label": "Expression rule (added to every generator prompt)",
             "default": edit.EXPRESSION_RULE},
            {"rule": "sound", "label": "Sound rule (video with sound)", "default": edit.SOUND_RULE},
            {"rule": "edit", "label": "Prompt edit - system prompt", "default": edit.EDIT_SYSTEM},
            {"rule": "translate", "label": "Prompt translate - system prompt", "default": edit.TRANSLATE_SYSTEM},
        ]
    except Exception as error:
        logger.info("prompts page: rules of ai_prompt_edit unavailable: %s", error)
    try:
        import ai_qwen_image_api as qwen
        rows.append({"rule": "qwen_default", "label": "Image node - default system prompt",
                     "default": qwen.QWEN_SYSTEM_PROMPT_DEFAULT})
    except Exception:
        pass
    try:
        import ai_vision_api as vision
        rows.append({"rule": "animate_default", "label": "Video node - prompt when nothing is wired",
                     "default": vision.DEFAULT_ANIMATE_PROMPT})
    except Exception:
        pass
    return rows


def _graph_prompts() -> List[Dict[str, Any]]:
    """Every distinct system prompt of the saved graphs, newest graph first."""
    found: Dict[str, Dict[str, Any]] = {}
    try:
        paths = sorted(GRAPH_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)[:200]
    except Exception:
        paths = []
    for path in paths:
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        graph = doc.get("graph_object") or doc
        name = str(graph.get("name") or path.stem)
        for node in graph.get("nodes") or []:
            params = node.get("params") or {}
            text = params.get("_system_prompt")
            if not isinstance(text, str) or not text.strip():
                continue
            key = prompt_library.key_of(text)
            row = found.setdefault(key, {"key": key, "original": text, "uses_array": [], "services_array": []})
            if len(row["uses_array"]) < 12:
                row["uses_array"].append(f"{name} · {node.get('id')}" +
                                         (f" · {params.get('_label')}" if params.get("_label") else ""))
            service = str(node.get("service") or "")
            if service and service not in row["services_array"]:
                row["services_array"].append(service)
            row["count_int"] = int(row.get("count_int") or 0) + 1
    return sorted(found.values(), key=lambda row: -int(row.get("count_int") or 0))


class PromptRow(BaseModel):
    key: str = Field("", max_length=32)
    rule: str = Field("", max_length=64)
    text: str = Field(..., max_length=prompt_library.MAX_TEXT)
    label: str = Field("", max_length=200)
    original: str = Field("", max_length=prompt_library.MAX_TEXT)


class PromptsSave(BaseModel):
    rows_array: List[PromptRow] = Field(default_factory=list)


def build_prompts_router(require_admin) -> APIRouter:
    from fastapi import Depends

    router = APIRouter()

    @router.get("/api/ai/prompts")
    async def api_prompts(_admin=Depends(require_admin)):
        table = prompt_library.table()
        graphs = []
        for row in _graph_prompts():
            edited = table["entries"].get(row["key"])
            graphs.append(dict(row, text_string=edited or row["original"], edited_bool=bool(edited)))
        rules = []
        for row in _server_rules():
            edited = table["rules"].get(row["rule"])
            rules.append({"rule": row["rule"], "label": row["label"], "original": row["default"],
                          "text_string": edited or row["default"], "edited_bool": bool(edited)})
        return {"success_bool": True, "graphs_array": graphs, "rules_array": rules,
                "store_string": str(prompt_library.STORE)}

    @router.put("/api/ai/prompts")
    async def api_prompts_save(body: PromptsSave, _admin=Depends(require_admin)):
        rows = [row.dict() for row in body.rows_array]
        if len(rows) > 500:
            raise HTTPException(status_code=413, detail={"error_string": "too_many"})
        result = prompt_library.save(rows)
        logger.info("prompt library saved: %s rows", result.get("saved_int"))
        return dict(result, success_bool=True)

    return router
