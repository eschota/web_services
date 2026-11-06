"""Join texts - one prompt out of many small ones, word for word (owner 2026-09-30).

"описание сделай подробным в текстовых нодах, каждый элемент отдельно и всё
объединено в единый промпт внешности, так чтобы каждую деталь можно было
удалить или изменить в этом графе".

A Text node takes two wires and rewrites what it gets through a language
model, so chaining a dozen of them to build one description both costs a dozen
model calls and lets details drift away on the way. This node has no model at
all: it takes up to twenty text wires, in socket order, drops the empty ones
and joins the rest behind the node's own typed lead. Delete a text node and its
detail is gone from the prompt; edit it and the next run carries the new words.
"""
from __future__ import annotations

import time
from typing import Any, Dict, List

from fastapi import APIRouter
from pydantic import BaseModel, ConfigDict, Field

router = APIRouter()

SOCKETS = 20
MAX_TEXT = 12000


class TextJoinRequest(BaseModel):
    model_config = ConfigDict(extra="allow")    # text_1 .. text_20 arrive as extra fields

    prompt: str = Field("", max_length=MAX_TEXT, description="The node's own typed lead, put first")
    separator: str = Field("; ", max_length=20)
    end: str = Field(".", max_length=4)


def join(lead: str, parts: List[str], separator: str = "; ", end: str = ".") -> str:
    """Lead first, then every non-empty part, each without its own closing mark."""
    clean = [str(part).strip().rstrip(";.,").strip() for part in parts]
    clean = [part for part in clean if part]
    body = separator.join(clean)
    lead = str(lead or "").strip()
    if not body:
        return lead
    text = (lead + " " + body) if lead else body
    return text + (end if end and not text.endswith(end) else "")


@router.post("/api/ai/text-join")
async def api_text_join(body: TextJoinRequest) -> Dict[str, Any]:
    data = body.model_dump()
    extra = body.model_extra or {}
    parts = []
    for index in range(1, SOCKETS + 1):
        value = extra.get(f"text_{index}", data.get(f"text_{index}"))
        if isinstance(value, str) and value.strip():
            parts.append(value[:MAX_TEXT])
    text = join(body.prompt, parts, body.separator or "; ", body.end if body.end is not None else ".")
    # Answered at once: the editor's text runner takes answer_string straight
    # from the reply, so no task and no polling.
    return {"success_bool": True, "finished_bool": True, "status_string": "completed",
            "answer_string": text, "parts_int": len(parts), "chars_int": len(text),
            "server_time_unix_int": int(time.time())}
