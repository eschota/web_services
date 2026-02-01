"""
Telegram notification service for CGTrader Submit Server.
Handles notifications, error logging, and bot commands.
"""
import asyncio
import traceback
from datetime import datetime
from typing import Optional
import json

from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
import database as db

# Global bot instance for sync notifications
_bot: Optional[Bot] = None


def get_bot() -> Bot:
    """Get or create bot instance."""
    global _bot
    if _bot is None:
        _bot = Bot(token=TELEGRAM_BOT_TOKEN)
    return _bot


def _truncate(text: str, max_len: int = 1000) -> str:
    """Truncate text to max length."""
    if len(text) <= max_len:
        return text
    return text[:max_len - 3] + "..."


# =============================================================================
# Synchronous notification functions (for use in worker)
# =============================================================================

def send_sync(text: str, reply_markup=None, parse_mode: str = "HTML"):
    """Send message synchronously."""
    try:
        bot = get_bot()
        asyncio.get_event_loop().run_until_complete(
            bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=text,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
                disable_web_page_preview=True
            )
        )
    except RuntimeError:
        # No event loop, create one
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(
                get_bot().send_message(
                    chat_id=TELEGRAM_CHAT_ID,
                    text=text,
                    parse_mode=parse_mode,
                    reply_markup=reply_markup,
                    disable_web_page_preview=True
                )
            )
        finally:
            loop.close()
    except Exception as e:
        print(f"[Telegram] Failed to send message: {e}")


def notify_service_start():
    """Notify that the service has started."""
    queue = db.get_queue_status()
    interrupted = len(db.get_interrupted_tasks())
    
    text = (
        "🚀 <b>CGTrader Submit Server Started</b>\n\n"
        f"📅 {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
        f"📊 Queue: {queue['queue_length']} pending\n"
        f"🔄 Interrupted tasks: {interrupted}\n"
        f"✅ Done: {queue['counts'].get('done', 0)}\n"
        f"❌ Errors: {queue['counts'].get('error', 0)}"
    )
    send_sync(text)


def notify_new_task(task_id: str, input_url: str):
    """Notify about a new task."""
    # Truncate URL if too long
    url_display = input_url if len(input_url) < 60 else input_url[:57] + "..."
    
    text = (
        f"🟢 <b>New CGTrader Task</b>\n\n"
        f"🆔 <code>{task_id}</code>\n"
        f"📦 {url_display}"
    )
    
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("📊 Status", callback_data=f"status:{task_id}")
    ]])
    
    send_sync(text, reply_markup=keyboard)


def notify_task_progress(task_id: str, step: str, message: str = ""):
    """Notify about task progress (optional, for debugging)."""
    step_emoji = {
        "downloading": "⬇️",
        "extracting": "📂",
        "analyzing": "🔍",
        "uploading": "⬆️",
        "filling_form": "📝",
        "publishing": "🚀",
    }
    
    emoji = step_emoji.get(step, "⏳")
    text = f"{emoji} Task <code>{task_id[:8]}...</code>: {step}"
    if message:
        text += f"\n{message}"
    
    send_sync(text)


def notify_task_done(task_id: str, product_url: Optional[str] = None):
    """Notify that a task completed successfully."""
    task = db.get_task(task_id)
    
    duration = ""
    if task and task.get("started_at") and task.get("completed_at"):
        try:
            start = datetime.fromisoformat(task["started_at"])
            end = datetime.fromisoformat(task["completed_at"])
            delta = end - start
            minutes = delta.seconds // 60
            seconds = delta.seconds % 60
            duration = f"\n⏱ Duration: {minutes}m {seconds}s"
        except:
            pass
    
    text = (
        f"✅ <b>Task Completed</b>\n\n"
        f"🆔 <code>{task_id}</code>{duration}"
    )
    
    if product_url:
        text += f"\n🔗 {product_url}"
    
    send_sync(text)


def notify_task_error(
    task_id: str,
    step: str,
    error: Exception,
    input_url: str = "",
    attempts: int = 0,
    max_attempts: int = 3
):
    """Send detailed error notification for debugging."""
    error_type = type(error).__name__
    error_msg = str(error)
    stack = traceback.format_exc()
    
    text = (
        f"🔴 <b>CGTRADER ERROR</b>\n\n"
        f"🆔 Task: <code>{task_id}</code>\n"
        f"📍 Step: {step}\n"
        f"🔄 Attempt: {attempts}/{max_attempts}\n"
    )
    
    if input_url:
        url_short = input_url if len(input_url) < 50 else input_url[:47] + "..."
        text += f"📦 URL: {url_short}\n"
    
    text += (
        f"\n❌ Error: {error_type}\n"
        f"<pre>{_truncate(error_msg, 500)}</pre>\n\n"
        f"<pre>{_truncate(stack, 800)}</pre>\n\n"
        f"📅 {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
    )
    
    # Add retry button if attempts remaining
    keyboard = None
    if attempts < max_attempts:
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔄 Retry", callback_data=f"retry:{task_id}")
        ]])
    
    send_sync(text, reply_markup=keyboard)


def notify_warning(task_id: str, message: str):
    """Send a warning notification."""
    text = (
        f"⚠️ <b>Warning</b>\n\n"
        f"🆔 <code>{task_id}</code>\n"
        f"📝 {message}"
    )
    send_sync(text)


# =============================================================================
# Bot command handlers
# =============================================================================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command."""
    await update.message.reply_text(
        "🤖 CGTrader Submit Bot\n\n"
        "Commands:\n"
        "/status - Queue status\n"
        "/task <id> - Task details\n"
        "/recent - Recent tasks"
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /status command."""
    queue = db.get_queue_status()
    
    text = (
        "📊 <b>Queue Status</b>\n\n"
        f"📝 Pending: {queue['counts'].get('created', 0)}\n"
        f"⏳ Processing: {sum(queue['counts'].get(s, 0) for s in ['downloading', 'extracting', 'analyzing', 'uploading', 'filling_form', 'publishing'])}\n"
        f"✅ Done: {queue['counts'].get('done', 0)}\n"
        f"❌ Errors: {queue['counts'].get('error', 0)}\n"
        f"📦 Total: {queue['total']}"
    )
    
    if queue.get("processing"):
        task = queue["processing"]
        text += (
            f"\n\n<b>Current task:</b>\n"
            f"🆔 <code>{task['id']}</code>\n"
            f"📍 Step: {task.get('status', 'unknown')}"
        )
    
    await update.message.reply_text(text, parse_mode="HTML")


async def cmd_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /task <id> command."""
    if not context.args:
        await update.message.reply_text("Usage: /task <task_id>")
        return
    
    task_id = context.args[0]
    task = db.get_task(task_id)
    
    if not task:
        await update.message.reply_text(f"❌ Task not found: {task_id}")
        return
    
    status_emoji = {
        "created": "📝",
        "downloading": "⬇️",
        "extracting": "📂",
        "analyzing": "🔍",
        "uploading": "⬆️",
        "filling_form": "📝",
        "publishing": "🚀",
        "done": "✅",
        "error": "❌",
    }
    
    emoji = status_emoji.get(task["status"], "❓")
    
    text = (
        f"📋 <b>Task Details</b>\n\n"
        f"🆔 <code>{task['id']}</code>\n"
        f"{emoji} Status: {task['status']}\n"
        f"📍 Step: {task.get('step') or '-'}\n"
        f"🔄 Attempts: {task['attempts']}/{task['max_attempts']}\n"
        f"📅 Created: {task['created_at']}\n"
    )
    
    if task.get("input_url"):
        url = task["input_url"]
        if len(url) > 50:
            url = url[:47] + "..."
        text += f"📦 URL: {url}\n"
    
    if task.get("error_message"):
        text += f"\n❌ Error: {_truncate(task['error_message'], 300)}"
    
    if task.get("cgtrader_product_url"):
        text += f"\n🔗 Product: {task['cgtrader_product_url']}"
    
    # Add retry button for failed tasks
    keyboard = None
    if task["status"] == "error":
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔄 Retry", callback_data=f"retry:{task_id}")
        ]])
    
    await update.message.reply_text(text, parse_mode="HTML", reply_markup=keyboard)


async def cmd_recent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /recent command."""
    tasks = db.get_recent_tasks(10)
    
    if not tasks:
        await update.message.reply_text("No tasks found.")
        return
    
    status_emoji = {
        "created": "📝", "downloading": "⬇️", "extracting": "📂",
        "analyzing": "🔍", "uploading": "⬆️", "filling_form": "📝",
        "publishing": "🚀", "done": "✅", "error": "❌",
    }
    
    lines = ["📋 <b>Recent Tasks</b>\n"]
    for task in tasks:
        emoji = status_emoji.get(task["status"], "❓")
        task_id_short = task["id"][:8]
        lines.append(f"{emoji} <code>{task_id_short}</code> - {task['status']}")
    
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle inline button callbacks."""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    if data.startswith("retry:"):
        task_id = data.split(":")[1]
        if db.retry_task(task_id):
            await query.edit_message_text(
                f"🔄 Task <code>{task_id}</code> queued for retry.",
                parse_mode="HTML"
            )
        else:
            await query.edit_message_text(
                f"❌ Cannot retry task {task_id}",
                parse_mode="HTML"
            )
    
    elif data.startswith("status:"):
        task_id = data.split(":")[1]
        task = db.get_task(task_id)
        
        if task:
            status_emoji = {
                "created": "📝", "downloading": "⬇️", "extracting": "📂",
                "analyzing": "🔍", "uploading": "⬆️", "filling_form": "📝",
                "publishing": "🚀", "done": "✅", "error": "❌",
            }
            emoji = status_emoji.get(task["status"], "❓")
            
            text = (
                f"{emoji} Task <code>{task_id[:8]}...</code>\n"
                f"Status: {task['status']}\n"
                f"Step: {task.get('step') or '-'}"
            )
            
            if task.get("error_message"):
                text += f"\n❌ {_truncate(task['error_message'], 200)}"
            
            await query.edit_message_text(text, parse_mode="HTML")
        else:
            await query.edit_message_text(f"❌ Task not found")


# =============================================================================
# Bot runner
# =============================================================================

def run_bot():
    """Run the Telegram bot (blocking)."""
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("task", cmd_task))
    app.add_handler(CommandHandler("recent", cmd_recent))
    app.add_handler(CallbackQueryHandler(callback_handler))
    
    print("[Telegram] Bot starting...")
    app.run_polling(drop_pending_updates=True)


def run_bot_async():
    """Run bot in a background thread."""
    import threading
    
    def _run():
        try:
            run_bot()
        except Exception as e:
            print(f"[Telegram] Bot error: {e}")
    
    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return thread


if __name__ == "__main__":
    run_bot()
