"""Dedicated Telegram bot for human review of Animation Fitting videos.

This process intentionally does not use AutoRig's main ``TELEGRAM_BOT_TOKEN``.
It reads only ``ANIMATION_FITTING_TELEGRAM_BOT_TOKEN`` so @codexai_bot review
traffic cannot interfere with @autorigbot task notifications.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
import sys
from typing import Any

from animation_fitting_telegram_approval import (
    APPROVE,
    REJECT,
    ReviewError,
    UnauthorizedReviewer,
    callback_data,
    configured_owner,
    decision_caption,
    default_ledger_path,
    make_candidate_binding,
    record_candidate_markup_failure,
    record_decision,
    register_sent_candidate,
    sha256_file,
)


def _get_token() -> str:
    token = os.getenv("ANIMATION_FITTING_TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("ANIMATION_FITTING_TELEGRAM_BOT_TOKEN is not set")
    return token


def _private_owner() -> tuple[int, int]:
    chat_id, user_id = configured_owner()
    if chat_id != user_id:
        raise ReviewError("Animation Fitting reviewer must be a private owner chat")
    return chat_id, user_id


def _binding_from_video(
    *,
    video_path: str | os.PathLike[str],
    candidate_id: str,
    media_kind: str,
    machine_qa_sha256: str | None,
    expected_video_sha256: str | None,
):
    path = Path(video_path)
    if not path.is_file() or path.stat().st_size <= 0:
        raise FileNotFoundError(f"Animation Fitting video is missing or empty: {path}")
    actual_sha256 = sha256_file(path)
    if expected_video_sha256 is not None and actual_sha256 != expected_video_sha256.strip().lower():
        raise ReviewError("video SHA-256 does not match the immutable expected digest")
    binding = make_candidate_binding(
        candidate_id,
        actual_sha256,
        media_kind=media_kind,
        machine_qa_sha256=machine_qa_sha256,
    )
    return path, binding


def _telegram_markup(binding):
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Утвердить", callback_data=callback_data(binding, APPROVE)),
        InlineKeyboardButton("❌ Отклонить", callback_data=callback_data(binding, REJECT)),
    ]])


async def send_review_video(
    *,
    video_path: str | os.PathLike[str],
    candidate_id: str,
    caption: str,
    media_kind: str = "raw_reference",
    machine_qa_sha256: str | None = None,
    expected_video_sha256: str | None = None,
    ledger_path: str | os.PathLike[str] | None = None,
):
    """Send a new review video with mandatory approve/reject buttons."""
    from telegram import Bot

    owner_chat_id, _ = _private_owner()
    path, binding = _binding_from_video(
        video_path=video_path,
        candidate_id=candidate_id,
        media_kind=media_kind,
        machine_qa_sha256=machine_qa_sha256,
        expected_video_sha256=expected_video_sha256,
    )
    bot = Bot(token=_get_token())
    markup = _telegram_markup(binding)
    with path.open("rb") as handle:
        message = await bot.send_video(
            chat_id=owner_chat_id,
            video=handle,
            caption=str(caption or "")[:1024],
            supports_streaming=True,
            reply_markup=markup,
        )
    ledger = Path(ledger_path) if ledger_path is not None else default_ledger_path()
    try:
        register_sent_candidate(
            ledger,
            binding,
            chat_id=owner_chat_id,
            message_id=int(message.message_id),
        )
    except Exception:
        # A callback without its immutable ledger binding must fail visibly.
        try:
            await bot.delete_message(
                chat_id=owner_chat_id,
                message_id=int(message.message_id),
            )
        except Exception:
            try:
                await bot.edit_message_reply_markup(
                    chat_id=owner_chat_id,
                    message_id=int(message.message_id),
                    reply_markup=None,
                )
            except Exception:
                pass
        raise
    return message, binding


async def attach_review_buttons(
    *,
    video_path: str | os.PathLike[str],
    expected_video_sha256: str,
    candidate_id: str,
    message_id: int,
    media_kind: str = "raw_reference",
    machine_qa_sha256: str | None = None,
    caption: str | None = None,
    ledger_path: str | os.PathLike[str] | None = None,
):
    """Attach review buttons to an existing video without sending a duplicate."""
    from telegram import Bot

    owner_chat_id, _ = _private_owner()
    _, binding = _binding_from_video(
        video_path=video_path,
        candidate_id=candidate_id,
        media_kind=media_kind,
        machine_qa_sha256=machine_qa_sha256,
        expected_video_sha256=expected_video_sha256,
    )
    ledger = Path(ledger_path) if ledger_path is not None else default_ledger_path()
    # Registration precedes markup publication.  If Telegram rejects the edit,
    # the append-only ledger records an explicit non-actionable failure event.
    register_sent_candidate(
        ledger,
        binding,
        chat_id=owner_chat_id,
        message_id=int(message_id),
    )
    bot = Bot(token=_get_token())
    markup = _telegram_markup(binding)
    try:
        if caption is None:
            result = await bot.edit_message_reply_markup(
                chat_id=owner_chat_id,
                message_id=int(message_id),
                reply_markup=markup,
            )
        else:
            result = await bot.edit_message_caption(
                chat_id=owner_chat_id,
                message_id=int(message_id),
                caption=str(caption)[:1024],
                reply_markup=markup,
            )
    except Exception as exc:
        record_candidate_markup_failure(
            ledger,
            binding,
            chat_id=owner_chat_id,
            message_id=int(message_id),
            error_type=type(exc).__name__,
        )
        raise
    return result, binding


async def handle_review_callback(update, context) -> None:
    """Authenticate, persist and visibly acknowledge one Telegram callback."""
    query = getattr(update, "callback_query", None)
    if query is None:
        return
    message = getattr(query, "message", None)
    chat = getattr(message, "chat", None)
    user = getattr(query, "from_user", None)
    if message is None or chat is None or user is None:
        await query.answer("Недействительный запрос", show_alert=True)
        return
    try:
        owner_chat_id, owner_user_id = _private_owner()
        result = record_decision(
            default_ledger_path(),
            callback_data_value=query.data,
            callback_query_id=str(query.id),
            chat_id=int(chat.id),
            user_id=int(user.id),
            message_id=int(message.message_id),
            chat_type=str(getattr(chat, "type", "")),
            owner_chat_id=owner_chat_id,
            owner_user_id=owner_user_id,
        )
    except UnauthorizedReviewer:
        await query.answer("Только владелец может принимать решение", show_alert=True)
        return
    except (ReviewError, ValueError) as exc:
        print(f"[AnimationFittingTelegram] Review rejected: {type(exc).__name__}: {exc}")
        await query.answer("Решение не сохранено: кандидат не найден", show_alert=True)
        return

    approved = result.record.get("decision") == APPROVE
    if result.record.get("reviewStage") == "fitted_visual_qa":
        answer = "Visual QA: PASS" if approved else "Visual QA: FAIL"
    else:
        answer = "Референс утверждён" if approved else "Референс отклонён"
    if result.duplicate:
        answer += " (уже сохранено)"
    await query.answer(answer)
    caption = decision_caption(getattr(message, "caption", None), result.record)
    try:
        await query.edit_message_caption(
            caption=caption,
            reply_markup=getattr(message, "reply_markup", None),
        )
    except Exception as exc:
        print(f"[AnimationFittingTelegram] Caption edit failed: {type(exc).__name__}")
        try:
            await message.reply_text(f"{answer}. Решение записано в журнал.")
        except Exception:
            pass


async def _start_command(update, context) -> None:
    chat = getattr(update, "effective_chat", None)
    user = getattr(update, "effective_user", None)
    if chat is None or user is None:
        return
    try:
        owner_chat_id, owner_user_id = _private_owner()
        authorized = (
            str(getattr(chat, "type", "")) == "private"
            and int(chat.id) == owner_chat_id
            and int(user.id) == owner_user_id
        )
    except ReviewError:
        authorized = False
    text = (
        "✅ Проверка анимаций включена. Используйте кнопки под видео."
        if authorized
        else "Этот бот принимает решения только от настроенного владельца."
    )
    if getattr(update, "effective_message", None) is not None:
        await update.effective_message.reply_text(text)


async def discover_owner() -> dict[str, Any]:
    """Discover exactly one private /start sender without guessing an id."""
    from telegram import Bot

    bot = Bot(token=_get_token())
    updates = await bot.get_updates(timeout=0, limit=100, allowed_updates=["message"])
    owners: dict[tuple[int, int], dict[str, Any]] = {}
    for update in updates:
        chat = getattr(update, "effective_chat", None)
        user = getattr(update, "effective_user", None)
        message = getattr(update, "effective_message", None)
        text = str(getattr(message, "text", "") or "").strip()
        if not (text == "/start" or text.startswith("/start@")):
            continue
        if chat is None or user is None or str(getattr(chat, "type", "")) != "private":
            continue
        chat_id, user_id = int(chat.id), int(user.id)
        if chat_id != user_id:
            continue
        owners[(chat_id, user_id)] = {
            "chatId": chat_id,
            "userId": user_id,
            "username": getattr(user, "username", None),
        }
    if len(owners) != 1:
        raise ReviewError(f"discover-owner requires exactly one unique private /start user; found {len(owners)}")
    return next(iter(owners.values()))


async def run_polling() -> None:
    from telegram.ext import ApplicationBuilder, CallbackQueryHandler, CommandHandler

    _private_owner()  # Fail closed before starting long polling.
    app = ApplicationBuilder().token(_get_token()).build()
    app.add_handler(CommandHandler("start", _start_command))
    app.add_handler(
        CallbackQueryHandler(
            handle_review_callback,
            pattern=r"^af1:[ar]:[0-9a-f]{24}$",
        )
    )
    await app.initialize()
    await app.start()
    await app.updater.start_polling(
        drop_pending_updates=False,
        allowed_updates=["callback_query", "message"],
    )
    print("[AnimationFittingTelegram] Dedicated review bot started")
    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Dedicated Animation Fitting Telegram review bot")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("poll")
    sub.add_parser("discover-owner")
    for command in ("send", "attach"):
        item = sub.add_parser(command)
        item.add_argument("--video", required=True)
        item.add_argument("--candidate-id", required=True)
        item.add_argument("--media-kind", choices=("raw_reference", "fitted_preview"), default="raw_reference")
        item.add_argument("--machine-qa-sha256")
        item.add_argument("--expected-video-sha256", required=command == "attach")
        item.add_argument("--caption")
        item.add_argument("--ledger")
        if command == "attach":
            item.add_argument("--message-id", type=int, required=True)
    return parser


async def _run_cli(args) -> None:
    if args.command == "poll":
        await run_polling()
        return
    if args.command == "discover-owner":
        print(json.dumps(await discover_owner(), ensure_ascii=False, sort_keys=True))
        return
    common = dict(
        video_path=args.video,
        candidate_id=args.candidate_id,
        media_kind=args.media_kind,
        machine_qa_sha256=args.machine_qa_sha256,
        ledger_path=args.ledger,
    )
    if args.command == "send":
        message, binding = await send_review_video(
            **common,
            caption=args.caption or "",
            expected_video_sha256=args.expected_video_sha256,
        )
    else:
        message, binding = await attach_review_buttons(
            **common,
            message_id=args.message_id,
            caption=args.caption,
            expected_video_sha256=args.expected_video_sha256,
        )
    print(json.dumps({
        "messageId": int(message.message_id),
        "candidateId": binding.candidate_id,
        "videoSha256": binding.video_sha256,
        "callbackKey": binding.callback_key,
    }, ensure_ascii=False, sort_keys=True))


def main() -> None:
    try:
        asyncio.run(_run_cli(_parser().parse_args()))
    except Exception as exc:
        # Telegram's InvalidToken exception may include the rejected token in
        # its message.  Never let exception text or a traceback reach the
        # systemd journal; the exception class is sufficient for operations.
        print(
            f"[AnimationFittingTelegram] Fatal: {type(exc).__name__}",
            file=sys.stderr,
        )
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
