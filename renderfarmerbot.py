#!/usr/bin/env python3
"""Low-noise Telegram dashboard for the live AutoRig/Renderfin farm.

The monitor intentionally owns one status message per subscribed chat. It
polls every minute, but edits Telegram only when a semantic farm state changes.
It never emits automatic media groups, completion videos, or startup messages.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import html
import json
import logging
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import BadRequest, Forbidden, TelegramError
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes


VERSION = "v3.0.0"
DEFAULT_DATA_DIR = "/srv/autorig/data/var/renderfarmer-monitor"
DEFAULT_CONVERTERS = {
    "F1": "https://converter-f1.freestock.online/api-converter-glb",
    "F2": "https://converter-f2.freestock.online/api-converter-glb",
    "F11": "https://converter-f11.freestock.online/api-converter-glb",
    "F13": "https://converter-f13.freestock.online/api-converter-glb",
}
PARKED_CONVERTERS = ("F7",)


logging.basicConfig(
    level=os.getenv("RENDERFARMER_LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("renderfarmer-monitor")

# python-telegram-bot uses httpx internally. INFO request logs contain the bot
# token as part of the Telegram URL, so they must never reach journald.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("telegram.request").setLevel(logging.WARNING)


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _worker_sort_key(item: "ConverterStatus") -> tuple[bool, int, str]:
    suffix = item.name[1:] if item.name.upper().startswith("F") else ""
    number = int(suffix) if suffix.isdigit() else 1_000_000
    return (not item.enabled, number, item.name)


@dataclass
class ConverterStatus:
    name: str
    enabled: bool = True
    online: bool = False
    healthy: bool = False
    active: int = 0
    pending: int = 0
    queue: int = 0
    stuck: int = 0
    completed: int = 0
    hunyuan_state: str = ""
    hostname: str = ""
    error: str = ""


@dataclass
class FarmSnapshot:
    converter_queue: dict[str, Any]
    renderfin: dict[str, Any]
    converters: list[ConverterStatus]
    disk_free_gb: float
    disk_used_percent: float
    checked_at: str
    errors: list[str] = field(default_factory=list)

    def semantic_payload(self) -> dict[str, Any]:
        """Return stable state; deliberately exclude time and noisy host CPU."""
        disk_band = "critical" if self.disk_free_gb < 10 else "warning" if self.disk_free_gb < 50 else "ok"
        queue = self.converter_queue
        renderfin = self.renderfin
        pools = renderfin.get("hunyuan_pools") or {}
        return {
            "converter_queue": {
                "ok": bool(queue.get("ok")),
                "active": _int(queue.get("total_active")),
                "pending": _int(queue.get("total_pending")),
                "queued": _int(queue.get("total_queue")),
                "available": _int(queue.get("available_workers")),
                "total": _int(queue.get("total_workers")),
            },
            "renderfin": {
                "ok": bool(renderfin.get("ok")),
                "servers": _int(renderfin.get("servers")),
                "pending": _int(renderfin.get("pending")),
                "rendering": _int(renderfin.get("rendering")),
                "dedicated": sorted(str(v) for v in pools.get("dedicated", [])),
                "shared": sorted(str(v) for v in pools.get("shared_converter", [])),
                "reserved": _int(pools.get("shared_reserved")),
                "ordinary_waiting": bool(pools.get("ordinary_conversion_waiting")),
                "config_error": bool(renderfin.get("hunyuan_config_error")),
            },
            "converters": [
                {
                    "name": item.name,
                    "enabled": item.enabled,
                    "online": item.online,
                    "healthy": item.healthy,
                    "active": item.active,
                    "pending": item.pending,
                    "queue": item.queue,
                    "stuck": item.stuck,
                    "completed": item.completed,
                    "hunyuan_state": item.hunyuan_state,
                }
                for item in self.converters
            ],
            "disk_band": disk_band,
            "errors": sorted(self.errors),
        }

    def fingerprint(self) -> str:
        raw = json.dumps(self.semantic_payload(), ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class StateStore:
    """Atomic persistent subscriptions, message IDs, and delivered fingerprints."""

    def __init__(self, data_dir: str) -> None:
        self.data_dir = Path(data_dir)
        self.path = self.data_dir / "state.json"
        self.state: dict[str, Any] = {
            "schema": 3,
            "subscribed_chats": [],
            "status_messages": {},
            "fingerprints": {},
        }
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._load()

    def _load(self) -> None:
        if self.path.exists():
            try:
                loaded = json.loads(self.path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    self.state.update(loaded)
                    return
            except (OSError, ValueError) as exc:
                logger.error("State file is invalid; preserving it and starting with legacy import: %s", exc)
        self._import_legacy()

    def _import_legacy(self) -> None:
        """Import v2 files without deleting or recreating existing Telegram messages."""
        chats_file = self.data_dir / "chats.json"
        sessions_file = self.data_dir / "sessions.json"
        try:
            if chats_file.exists():
                chats = json.loads(chats_file.read_text(encoding="utf-8")).get("chats", [])
                self.state["subscribed_chats"] = [_int(chat) for chat in chats]
            if sessions_file.exists():
                messages = json.loads(sessions_file.read_text(encoding="utf-8")).get("messages", [])
                for message in messages:
                    if message.get("type") == "status":
                        self.state["status_messages"][str(_int(message.get("chat_id")))] = _int(
                            message.get("message_id")
                        )
            if chats_file.exists() or sessions_file.exists():
                logger.info(
                    "Imported legacy monitor state: %d subscriptions, %d dashboard IDs",
                    len(self.state["subscribed_chats"]),
                    len(self.state["status_messages"]),
                )
                self.save()
        except (OSError, ValueError, TypeError) as exc:
            logger.error("Legacy state import failed without modifying source files: %s", exc)

    @property
    def chats(self) -> list[int]:
        return list(dict.fromkeys(_int(chat) for chat in self.state.get("subscribed_chats", [])))

    def subscribe(self, chat_id: int) -> bool:
        if chat_id in self.chats:
            return False
        self.state["subscribed_chats"] = self.chats + [chat_id]
        self.save()
        return True

    def unsubscribe(self, chat_id: int) -> bool:
        if chat_id not in self.chats:
            return False
        self.state["subscribed_chats"] = [chat for chat in self.chats if chat != chat_id]
        self.state["status_messages"].pop(str(chat_id), None)
        self.state["fingerprints"].pop(str(chat_id), None)
        self.save()
        return True

    def message_id(self, chat_id: int) -> int | None:
        value = _int(self.state.get("status_messages", {}).get(str(chat_id)))
        return value or None

    def fingerprint(self, chat_id: int) -> str:
        return str(self.state.get("fingerprints", {}).get(str(chat_id), ""))

    def mark_delivered(self, chat_id: int, message_id: int, fingerprint: str) -> None:
        self.state.setdefault("status_messages", {})[str(chat_id)] = int(message_id)
        self.state.setdefault("fingerprints", {})[str(chat_id)] = fingerprint
        self.save()

    def forget_message(self, chat_id: int) -> None:
        self.state.setdefault("status_messages", {}).pop(str(chat_id), None)
        self.state.setdefault("fingerprints", {}).pop(str(chat_id), None)
        self.save()

    def save(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(self.state, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        tmp_name = ""
        try:
            with tempfile.NamedTemporaryFile(
                "w", encoding="utf-8", dir=self.data_dir, prefix=".state-", suffix=".tmp", delete=False
            ) as handle:
                tmp_name = handle.name
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(tmp_name, 0o600)
            os.replace(tmp_name, self.path)
        finally:
            if tmp_name and os.path.exists(tmp_name):
                with contextlib.suppress(OSError):
                    os.unlink(tmp_name)


class FarmPoller:
    def __init__(self) -> None:
        self.queue_url = os.getenv("RENDERFARMER_QUEUE_URL", "http://127.0.0.1:8200/api/queue/status")
        self.renderfin_url = os.getenv("RENDERFARMER_RENDERFIN_URL", "http://127.0.0.1:8210/renderfin/health")
        self.disk_path = os.getenv("RENDERFARMER_DISK_PATH", "/srv/autorig")
        self.converters = self._converter_config()

    @staticmethod
    def _converter_config() -> dict[str, str]:
        raw = os.getenv("RENDERFARMER_CONVERTERS_JSON", "").strip()
        if not raw:
            return dict(DEFAULT_CONVERTERS)
        try:
            parsed = json.loads(raw)
            if not isinstance(parsed, dict):
                raise ValueError("expected an object")
            return {str(name): str(url).rstrip("/") for name, url in parsed.items() if str(url).strip()}
        except (TypeError, ValueError) as exc:
            logger.error("Invalid RENDERFARMER_CONVERTERS_JSON; using defaults: %s", exc)
            return dict(DEFAULT_CONVERTERS)

    async def _fetch_json(self, client: httpx.AsyncClient, url: str, attempts: int = 2) -> dict[str, Any]:
        last_error = "unknown error"
        for attempt in range(attempts):
            try:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                if not isinstance(data, dict):
                    raise ValueError("JSON root is not an object")
                return data
            except (httpx.HTTPError, ValueError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                if attempt + 1 < attempts:
                    await asyncio.sleep(1)
        raise RuntimeError(last_error)

    async def _poll_converter(self, client: httpx.AsyncClient, name: str, base_url: str) -> ConverterStatus:
        item = ConverterStatus(name=name)
        try:
            data = await self._fetch_json(client, f"{base_url.rstrip('/')}/server-status")
            summary = data.get("tasks_summary") or {}
            item.online = True
            item.healthy = bool(data.get("healthy", True)) and not bool(data.get("issues"))
            item.active = _int(summary.get("processing", data.get("total_active")))
            item.pending = _int(summary.get("pending", data.get("total_pending")))
            item.queue = _int(summary.get("queue_size", data.get("queue_size")))
            item.stuck = _int(summary.get("stuck"))
            item.completed = _int(summary.get("completed", data.get("total_completed_tasks")))
            item.hostname = str(data.get("hostname") or "")
            item.hunyuan_state = str((data.get("hunyuan") or {}).get("service_state") or "")
        except RuntimeError as exc:
            item.error = str(exc)[:160]
        return item

    async def snapshot(self) -> FarmSnapshot:
        timeout = httpx.Timeout(12.0, connect=5.0)
        errors: list[str] = []
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            queue_task = asyncio.create_task(self._fetch_json(client, self.queue_url))
            renderfin_task = asyncio.create_task(self._fetch_json(client, self.renderfin_url))
            converter_tasks = [
                asyncio.create_task(self._poll_converter(client, name, url))
                for name, url in self.converters.items()
            ]

            try:
                queue = await queue_task
                queue["ok"] = True
            except RuntimeError as exc:
                queue = {"ok": False}
                errors.append(f"AutoRig queue: {exc}")
            try:
                renderfin = await renderfin_task
            except RuntimeError as exc:
                renderfin = {"ok": False}
                errors.append(f"Renderfin: {exc}")

            converter_results = await asyncio.gather(*converter_tasks)
            for item in converter_results:
                if not item.online:
                    errors.append(f"{item.name}: offline")

        for parked in PARKED_CONVERTERS:
            if parked not in self.converters:
                converter_results.append(ConverterStatus(name=parked, enabled=False))
        converter_results.sort(key=_worker_sort_key)

        try:
            disk = shutil.disk_usage(self.disk_path)
            disk_free_gb = disk.free / (1024**3)
            disk_used_percent = 100.0 * disk.used / disk.total if disk.total else 0.0
        except OSError as exc:
            disk_free_gb = 0.0
            disk_used_percent = 100.0
            errors.append(f"disk: {exc}")

        checked_at = datetime.now(ZoneInfo(os.getenv("RENDERFARMER_TIMEZONE", "Asia/Novosibirsk"))).isoformat(
            timespec="seconds"
        )
        return FarmSnapshot(
            converter_queue=queue,
            renderfin=renderfin,
            converters=converter_results,
            disk_free_gb=disk_free_gb,
            disk_used_percent=disk_used_percent,
            checked_at=checked_at,
            errors=errors,
        )


def _state_icon(ok: bool) -> str:
    return "🟢" if ok else "🔴"


def format_snapshot(snapshot: FarmSnapshot) -> str:
    queue = snapshot.converter_queue
    renderfin = snapshot.renderfin
    pools = renderfin.get("hunyuan_pools") or {}
    queue_ok = bool(queue.get("ok"))
    renderfin_ok = bool(renderfin.get("ok")) and not bool(renderfin.get("hunyuan_config_error"))
    disk_icon = "🟢" if snapshot.disk_free_gb >= 50 else "🟠" if snapshot.disk_free_gb >= 10 else "🔴"

    lines = [
        f"🖥 <b>RenderFarm Status {VERSION}</b>",
        "",
        (
            f"{_state_icon(queue_ok)} <b>AutoRig:</b> "
            f"{_int(queue.get('total_active'))} active · {_int(queue.get('total_pending'))} pending · "
            f"{_int(queue.get('total_queue'))} queued · {_int(queue.get('available_workers'))}/"
            f"{_int(queue.get('total_workers'))} workers free"
        ),
        (
            f"{_state_icon(renderfin_ok)} <b>Renderfin:</b> "
            f"{_int(renderfin.get('rendering'))} rendering · {_int(renderfin.get('pending'))} pending · "
            f"{_int(renderfin.get('servers'))} Comfy nodes"
        ),
        (
            f"{disk_icon} <b>way-fr disk:</b> {snapshot.disk_free_gb:.0f} GB free · "
            f"{snapshot.disk_used_percent:.0f}% used"
        ),
        "",
        "<b>Conversion workers</b>",
    ]

    enabled = [item for item in snapshot.converters if item.enabled]
    for item in snapshot.converters:
        name = html.escape(item.name)
        if not item.enabled:
            lines.append(f"⏸ <b>{name}:</b> parked / disabled")
        elif not item.online:
            lines.append(f"🔴 <b>{name}:</b> offline")
        elif item.active:
            extra = f" · {item.stuck} stuck" if item.stuck else ""
            lines.append(
                f"🟢 <b>{name}:</b> {item.active} active · {item.pending} pending · "
                f"{item.queue} queued{extra} · ✅ {item.completed} done"
            )
        else:
            icon = "🟡" if item.healthy else "🟠"
            extra = f" · {item.stuck} stuck" if item.stuck else ""
            lines.append(f"{icon} <b>{name}:</b> idle{extra} · ✅ {item.completed} done")

    online = sum(1 for item in enabled if item.online)
    lines.append(f"📊 <b>Converters:</b> {online}/{len(enabled)} enabled online")

    dedicated = [html.escape(str(name)) for name in pools.get("dedicated", [])]
    shared = [html.escape(str(name)) for name in pools.get("shared_converter", [])]
    lines.extend(
        [
            "",
            "<b>Hunyuan pool</b>",
            f"🎯 Dedicated: {', '.join(dedicated) if dedicated else 'none'}",
            f"↪️ Shared fallback: {', '.join(shared) if shared else 'none'}",
            f"🛡 Converter reserve: {_int(pools.get('shared_reserved'))}",
        ]
    )
    if pools.get("ordinary_conversion_waiting"):
        lines.append("⏳ Shared Hunyuan admission paused: ordinary conversion is waiting")
    if snapshot.errors:
        lines.extend(["", f"⚠️ <b>Issues:</b> {html.escape('; '.join(snapshot.errors[:4]))}"])

    stamp = datetime.fromisoformat(snapshot.checked_at).strftime("%d.%m %H:%M:%S")
    lines.extend(
        [
            "",
            f"🕒 <b>State snapshot:</b> {stamp}",
            "ℹ️ Unchanged state is not resent; polling continues every minute.",
        ]
    )
    return "\n".join(lines)


def dashboard_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🔄 Refresh", callback_data="refresh_status"),
                InlineKeyboardButton("📋 Tasks", url="https://autorig.online/gallery"),
            ],
            [
                InlineKeyboardButton("F1", url="https://converter-f1.freestock.online/api-converter-glb-ui"),
                InlineKeyboardButton("F2", url="https://converter-f2.freestock.online/api-converter-glb-ui"),
                InlineKeyboardButton("F11", url="https://converter-f11.freestock.online/api-converter-glb-ui"),
                InlineKeyboardButton("F13", url="https://converter-f13.freestock.online/api-converter-glb-ui"),
            ],
        ]
    )


class RenderFarmerMonitor:
    def __init__(self, token: str) -> None:
        self.store = StateStore(os.getenv("RENDERFARMER_DATA_DIR", DEFAULT_DATA_DIR))
        self.poller = FarmPoller()
        self.poll_interval = max(30, _int(os.getenv("RENDERFARMER_POLL_SECONDS", "60"), 60))
        self._poll_task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self.application = (
            Application.builder().token(token).post_init(self._post_init).post_shutdown(self._post_shutdown).build()
        )
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("stop", self.stop_command))
        self.application.add_handler(CommandHandler("status", self.status_command))
        self.application.add_handler(CommandHandler("version", self.version_command))
        self.application.add_handler(CallbackQueryHandler(self.callback_query))

    async def _post_init(self, application: Application) -> None:
        # There is no startup broadcast. Existing dashboard IDs are reused.
        await self.refresh_all(force=False)
        self._poll_task = asyncio.create_task(self._poll_loop(), name="renderfarmer-poll")
        logger.info("Monitor started with %d subscriptions; interval=%ds", len(self.store.chats), self.poll_interval)

    async def _post_shutdown(self, application: Application) -> None:
        self._stop.set()
        if self._poll_task:
            self._poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._poll_task

    async def _poll_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.poll_interval)
                break
            except asyncio.TimeoutError:
                pass
            try:
                await self.refresh_all(force=False)
            except Exception:
                logger.exception("Periodic refresh failed")

    async def refresh_all(self, force: bool = False) -> FarmSnapshot:
        snapshot = await self.poller.snapshot()
        results = await asyncio.gather(
            *(self._upsert_dashboard(chat_id, snapshot, force=force) for chat_id in self.store.chats),
            return_exceptions=True,
        )
        failures = sum(1 for result in results if isinstance(result, Exception))
        if failures:
            logger.error("Dashboard refresh had %d unexpected failures", failures)
        return snapshot

    async def _upsert_dashboard(self, chat_id: int, snapshot: FarmSnapshot, force: bool = False) -> str:
        fingerprint = snapshot.fingerprint()
        if not force and self.store.fingerprint(chat_id) == fingerprint:
            return "unchanged"

        text = format_snapshot(snapshot)
        message_id = self.store.message_id(chat_id)
        bot = self.application.bot
        try:
            if message_id:
                try:
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text=text,
                        parse_mode="HTML",
                        reply_markup=dashboard_keyboard(),
                        disable_web_page_preview=True,
                    )
                except BadRequest as exc:
                    message = str(exc).lower()
                    if "message is not modified" in message:
                        self.store.mark_delivered(chat_id, message_id, fingerprint)
                        return "unchanged"
                    if "message to edit not found" not in message and "message identifier is not specified" not in message:
                        raise
                    self.store.forget_message(chat_id)
                    message_id = None

            if not message_id:
                sent = await bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    parse_mode="HTML",
                    reply_markup=dashboard_keyboard(),
                    disable_web_page_preview=True,
                )
                message_id = sent.message_id

            self.store.mark_delivered(chat_id, message_id, fingerprint)
            return "updated"
        except Forbidden:
            # A blocked/deleted chat must not be retried every minute.
            self.store.unsubscribe(chat_id)
            logger.warning("Removed one unreachable Telegram subscription")
            return "unsubscribed"
        except TelegramError as exc:
            # Keep the old ID and fingerprint so a transient failure never creates
            # a replacement message on the next poll.
            logger.warning("Telegram dashboard update failed: %s", type(exc).__name__)
            return "failed"

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_chat or not update.effective_message:
            return
        chat_id = update.effective_chat.id
        created = self.store.subscribe(chat_id)
        snapshot = await self.poller.snapshot()
        await self._upsert_dashboard(chat_id, snapshot, force=True)
        await update.effective_message.reply_text(
            "✅ Monitoring enabled. One dashboard will be edited only when farm state changes."
            if created
            else "✅ Monitoring is already enabled; dashboard refreshed."
        )

    async def stop_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_chat or not update.effective_message:
            return
        removed = self.store.unsubscribe(update.effective_chat.id)
        await update.effective_message.reply_text(
            "✅ Monitoring disabled." if removed else "Monitoring was already disabled."
        )

    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_chat or not update.effective_message:
            return
        chat_id = update.effective_chat.id
        if chat_id not in self.store.chats:
            await update.effective_message.reply_text("Use /start once to enable the single farm dashboard.")
            return
        snapshot = await self.poller.snapshot()
        await self._upsert_dashboard(chat_id, snapshot, force=True)
        await update.effective_message.reply_text("✅ Dashboard refreshed.")

    async def version_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.effective_message:
            await update.effective_message.reply_text(
                f"RenderFarmer Monitor {VERSION}\nSemantic deduplication, persistent dashboard IDs, no automatic media."
            )

    async def callback_query(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if not query or not query.message:
            return
        await query.answer("Refreshing…")
        if query.data == "refresh_status":
            snapshot = await self.poller.snapshot()
            await self._upsert_dashboard(query.message.chat_id, snapshot, force=True)

    def run(self) -> None:
        self.application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


async def _check_once() -> int:
    snapshot = await FarmPoller().snapshot()
    print(format_snapshot(snapshot))
    print("fingerprint=" + snapshot.fingerprint())
    return 0 if not snapshot.errors else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="poll once and print without contacting Telegram")
    args = parser.parse_args()
    if args.check:
        return asyncio.run(_check_once())

    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        logger.error("TELEGRAM_BOT_TOKEN is required")
        return 2
    RenderFarmerMonitor(token).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
