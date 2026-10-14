"""Defect buttons on every AutoRig task notice (owner order 2026-10-14, AGENTS.md «Defect Buttons On Every Task Notice»):
«каждая модель авторига должна иметь кнопки при отправке в телеграм … запускать проверки соответственно нажатой кнопке
… снизу показывай ссылками топ 5 самых похожих по эмбеддингу 3д моделей».

* ``markup(task_id)``: the four buttons that replace «Коллекция ×15» / «Сабмитить» on the completion / needs-review
  notice; callback data ``rd:<kind>:<task uuid>`` (<= 49 bytes, the old ``rd:<kind>:<task8>`` form is accepted too).
* ``on_callback``: a press (in autorig-storage-telegram) -> the MT defect report (POST 127.0.0.1:8251/api/mt/
  defect-reports/<task>/<kind>/press, which starts the class checks), a toast, and the notice edited to show
  «🔎 разбор: <kind> · в работе». If MT does not answer, the report file is written here (checks start at the next
  GET /api/mt/defect-reports).
* ``similar_html(run_id)``: five lines «🔗 похожие: <score> <matched> · <task link>» from GET /api/mt/similar/<run>
  (the CLIP sheet and rest-pose embeddings), 3 s timeout, nothing on failure.
"""
from __future__ import annotations

import html
import json
import os
import re
import time
from pathlib import Path

MT_BASE = os.getenv("AUTORIG_MT_LOCAL_URL", "http://127.0.0.1:8251").rstrip("/")
SITE = "https://autorig.online"
REPORT_DIR = Path(os.getenv("MT_DEFECT_REPORTS_DIR", "/srv/autorig/data/var/rig_defects"))
KINDS = (
    ("transform", "Неправильный трансформ модели"),
    ("tear", "Разрыв меша после скининга"),
    ("elbows", "Слишком широкие локти"),
    ("unknown", "Непонятная ошибка"),
)
LABEL = dict(KINDS)
CALLBACK_PATTERN = r"^rd:(transform|tear|elbows|unknown):([0-9a-fA-F-]{8,36})$"
_RX = re.compile(CALLBACK_PATTERN)
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_RUN = re.compile(r"^[0-9a-f]{20}$")
SIMILAR_K = int(os.getenv("AUTORIG_TG_SIMILAR_K", "5"))
SIMILAR_TIMEOUT_S = float(os.getenv("AUTORIG_TG_SIMILAR_TIMEOUT_S", "3"))
CAPTION_MAX = 1024


def callback_data(kind: str, task_id: str) -> str:
    return f"rd:{kind}:{task_id}"


def markup(task_id: str, pressed=()):
    """One button per row (the labels are long); a pressed kind is marked 🔎."""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    return InlineKeyboardMarkup([[InlineKeyboardButton(("🔎 " if kind in pressed else "") + label,
                                                       callback_data=callback_data(kind, task_id))]
                                 for kind, label in KINDS])


def run_id_of_task(task) -> str | None:
    """The V3 run of a task: viewer_settings.v3.session.mt_run_id, else the run in viewer_prepared_glb_url."""
    try:
        vs = json.loads(getattr(task, "viewer_settings", None) or "{}")
        run = str((((vs.get("v3") or {}).get("session") or {}).get("mt_run_id")) or "") if isinstance(vs, dict) else ""
        if _RUN.match(run):
            return run
    except (TypeError, ValueError, AttributeError):
        pass
    m = re.search(r"/api/mt/(?:files|view)/([0-9a-f]{20})", str(getattr(task, "viewer_prepared_glb_url", "") or ""))
    return m.group(1) if m else None


# ------------------------------------------------------------------------------------------------ similar models
async def similar_rows(run_id: str | None, k: int = SIMILAR_K, timeout: float = SIMILAR_TIMEOUT_S) -> list:
    if not run_id or not _RUN.match(str(run_id)):
        return []
    try:
        import httpx
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.get(f"{MT_BASE}/api/mt/similar/{run_id}", params={"k": k})
        if r.status_code != 200:
            return []
        rows = (r.json() or {}).get("results") or []
        return [x for x in rows if isinstance(x, dict) and _UUID.match(str(x.get("task") or ""))][:k]
    except Exception as exc:                                     # noqa: BLE001 - the notice goes without the lines
        print(f"[Telegram] similar models for run {run_id} unavailable: {type(exc).__name__}")
        return []


def similar_line(row: dict) -> str:
    task = str(row["task"])
    url = f"{SITE}/task?id={task}"
    what = re.sub(r"\s+", " ", str(row.get("what") or "")).strip()[:28]
    detail = []
    if row.get("img") is not None:
        detail.append(f"img {float(row['img']):.2f}")
    if row.get("pose") is not None:
        detail.append(f"pose {float(row['pose']):.2f}")
    return (f"🔗 похожие: {float(row.get('score') or 0):.2f} {html.escape(str(row.get('matched') or ''))} · "
            f"<a href=\"{html.escape(url)}\">{task[:8]}</a>"
            + (f" {html.escape(what)}" if what else "") + (f" ({', '.join(detail)})" if detail else ""))


async def similar_html(run_id: str | None) -> str:
    rows = await similar_rows(run_id)
    return "\n".join(similar_line(r) for r in rows)


def visible_len(text_html: str) -> int:
    """What Telegram counts against the caption limit: the text after the HTML entities are parsed."""
    return len(html.unescape(re.sub(r"<[^>]+>", "", text_html or "")))


def with_lines(text: str, lines_html: str, limit: int = CAPTION_MAX) -> str:
    """text + as many of the lines as fit under the caption limit."""
    out = text
    for line in [x for x in (lines_html or "").split("\n") if x]:
        cand = out + "\n" + line
        if visible_len(cand) > limit:
            break
        out = cand
    return out


# ------------------------------------------------------------------------------------------------ the press
def _fallback_report(task_id, kind, run_id, by, chat_id, message_id, chat_type) -> None:
    """MT did not answer: the same report file (autorig.mt.defect-report/1), checks pending until MT lists it."""
    path = REPORT_DIR / f"{task_id}.{kind}.json"
    now = time.time()
    press = {"by": by, "at": round(now, 1), "chat_id": chat_id, "message_id": message_id, "chat_type": chat_type}
    try:
        rec = json.loads(path.read_text(encoding="utf-8"))
        rec.setdefault("presses", []).append(press)
        if rec.get("status") not in ("new", "in_work"):
            rec["status"] = "new"
    except (OSError, ValueError):
        rec = {"schema": "autorig.mt.defect-report/1", "task_id": task_id, "task8": task_id[:8], "kind": kind,
               "label": LABEL[kind], "run_id": run_id, "status": "new", "by": by, "at": round(now, 1),
               "at_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)), "message_id": message_id,
               "chat_id": chat_id, "chat_type": chat_type, "presses": [press], "evidence": {}, "notes": [],
               "replies": [], "links": {"task": f"{SITE}/task?id={task_id}"}}
    rec["checks"] = {"state": "pending_mt", "at": round(now, 1)}
    rec["updated_at"] = round(now, 1)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(rec, ensure_ascii=False, indent=1), encoding="utf-8")
    os.replace(tmp, path)


async def _resolve_task(ref: str):
    """-> (task_id, run_id) for a full id or an 8+ hex prefix (the compact callback form)."""
    from sqlalchemy import select

    from database import AsyncSessionLocal, Task
    ref = ref.lower()
    async with AsyncSessionLocal() as db:
        if _UUID.match(ref):
            task = await db.scalar(select(Task).where(Task.id == ref))
        else:
            rows = (await db.execute(select(Task).where(Task.id.like(f"{ref}%")).limit(2))).scalars().all()
            task = rows[0] if len(rows) == 1 else None
    if task is None:
        return None, None
    return str(task.id), run_id_of_task(task)


def _who(user) -> dict:
    if user is None:
        return {"id": None, "name": "", "username": ""}
    name = " ".join(x for x in (getattr(user, "first_name", "") or "", getattr(user, "last_name", "") or "") if x)
    return {"id": getattr(user, "id", None), "name": name[:80], "username": str(getattr(user, "username", "") or "")[:64]}


def _pressed_kinds(message) -> set:
    out = set()
    rm = getattr(message, "reply_markup", None)
    for row in getattr(rm, "inline_keyboard", None) or []:
        for b in row:
            m = _RX.match(str(getattr(b, "callback_data", "") or ""))
            if m and str(getattr(b, "text", "")).startswith("🔎"):
                out.add(m.group(1))
    return out


async def _mark_notice(bot, message, task_id: str, kind: str) -> bool:
    """The notice shows «🔎 разбор: <kind> · в работе» and the pressed button gets 🔎. -> False when it cannot be
    edited (then the caller replies in the chat instead)."""
    from telegram.constants import ParseMode
    line = f"🔎 разбор: {kind} · в работе"
    pressed = _pressed_kinds(message) | {kind}
    rm = markup(task_id, pressed)
    is_caption = getattr(message, "caption", None) is not None
    body = (message.caption_html if is_caption else getattr(message, "text_html", None)) or ""
    try:
        if line in body:
            await bot.edit_message_reply_markup(chat_id=message.chat.id, message_id=message.message_id, reply_markup=rm)
            return True
        new = body + "\n" + line
        if is_caption:
            if visible_len(new) > CAPTION_MAX:
                await bot.edit_message_reply_markup(chat_id=message.chat.id, message_id=message.message_id,
                                                    reply_markup=rm)
                return False
            await bot.edit_message_caption(chat_id=message.chat.id, message_id=message.message_id, caption=new,
                                           parse_mode=ParseMode.HTML, reply_markup=rm)
        else:
            await bot.edit_message_text(chat_id=message.chat.id, message_id=message.message_id, text=new,
                                        parse_mode=ParseMode.HTML, reply_markup=rm, disable_web_page_preview=True)
        return True
    except Exception as exc:                                     # noqa: BLE001 - the report is written already
        print(f"[Telegram] defect notice edit failed for {task_id}/{kind}: {type(exc).__name__}: {str(exc)[:160]}")
        return False


async def on_callback(update, context) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    m = _RX.match(query.data)
    if not m:
        await query.answer("Некорректные данные кнопки")
        return
    kind, ref = m.group(1), m.group(2)
    message = query.message
    chat = getattr(message, "chat", None)
    chat_id = int(chat.id) if chat is not None else None
    message_id = int(message.message_id) if message is not None else None
    chat_type = str(getattr(chat, "type", "") or "") if chat is not None else ""
    try:
        task_id, run_id = await _resolve_task(ref)
    except Exception as exc:                                     # noqa: BLE001
        print(f"[Telegram] defect press: task lookup failed for {ref}: {type(exc).__name__}: {exc}")
        task_id, run_id = (ref.lower(), None) if _UUID.match(ref.lower()) else (None, None)
    if not task_id:
        await query.answer("Задача не найдена")
        return
    by = _who(getattr(query, "from_user", None))
    payload = {"run_id": run_id, "by": by, "chat_id": chat_id, "message_id": message_id, "chat_type": chat_type}
    ok, started = False, None
    try:
        import httpx
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(f"{MT_BASE}/api/mt/defect-reports/{task_id}/{kind}/press", json=payload)
        ok = r.status_code == 200
        if ok:
            started = (r.json() or {}).get("started")
        else:
            print(f"[Telegram] defect press: MT answered {r.status_code} for {task_id}/{kind}")
    except Exception as exc:                                     # noqa: BLE001
        print(f"[Telegram] defect press: MT unavailable for {task_id}/{kind}: {type(exc).__name__}")
    if not ok:
        try:
            _fallback_report(task_id, kind, run_id, by, chat_id, message_id, chat_type)
        except Exception as exc:                                 # noqa: BLE001
            print(f"[Telegram] defect press: fallback report failed for {task_id}/{kind}: {exc}")
            await query.answer("Не удалось записать отчёт, попробуйте ещё раз")
            return
    print(f"[Telegram] defect report {task_id}/{kind} run={run_id} by={by.get('id')} chat={chat_id} "
          f"mt={'ok' if ok else 'fallback'} checks={started}")
    await query.answer(f"🔎 {LABEL[kind]}: принято, проверки запущены"[:190])
    if message is not None and not await _mark_notice(context.bot, message, task_id, kind):
        try:
            await context.bot.send_message(chat_id=chat_id, text=f"🔎 разбор: {kind} · в работе",
                                           reply_to_message_id=message_id, allow_sending_without_reply=True)
        except Exception as exc:                                 # noqa: BLE001
            print(f"[Telegram] defect ack reply failed for {task_id}/{kind}: {type(exc).__name__}")
