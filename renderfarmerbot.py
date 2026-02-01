#!/usr/bin/env python3
"""
RenderFarmer Telegram Bot

Monitors disk space and converter server status, sends updates to subscribed Telegram chats.
"""

import asyncio
import dataclasses
import json
import logging
import os
import shutil
import uuid
from dataclasses import dataclass
from typing import Dict, List, Optional
from collections import deque
import time

import aiohttp
import psutil
from telegram import Bot, InputMediaPhoto, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


@dataclass
class ConverterStatus:
    """Status information for a converter server."""
    name: str
    online: bool = False
    active_tasks: int = 0
    queue_size: int = 0
    server_version: str = ""
    total_completed: int = 0
    active_tasks_data: dict = dataclasses.field(default_factory=dict)
    task_durations: dict = dataclasses.field(default_factory=dict)  # {task_id: duration_seconds}


class SpyServer:
    """Monitors disk space and converter servers."""

    def __init__(self):
        self.converter_servers = {
            'F1': 'http://5.129.157.224:5132/api-converter-glb',
            'F2': 'http://5.129.157.224:5279/api-converter-glb',
            'F7': 'http://5.129.157.224:5131/api-converter-glb',
            'F11': 'http://5.129.157.224:5533/api-converter-glb',
            'F13': 'http://5.129.157.224:5267/api-converter-glb'
        }

        # CPU monitoring - store measurements for 10 minutes (60 measurements at 10-sec intervals)
        self.cpu_history = deque(maxlen=60)
        self.last_cpu_check = 0

        # Main API status
        self.api_status = {'online': False, 'servers_count': 0, 'tasks_count': 0, 'pending_count': 0}

        # Task timing - track when tasks started
        self.task_start_times = {}  # {server_name: {task_id: start_time}}

        # Completed tasks tracking - for sending completion videos
        self.completed_tasks = set()  # Track task_ids we've already processed

    async def poll_disk_space(self) -> int:
        """Get free disk space in GB, rounded."""
        try:
            stat = shutil.disk_usage('/')
            free_gb = round(stat.free / (1024 ** 3))
            return free_gb
        except Exception as e:
            logger.error(f"Failed to poll disk space: {e}")
            return 0

    async def poll_cpu_usage(self) -> float:
        """Get current CPU usage and maintain history for average calculation (10 minutes)."""
        try:
            current_time = time.time()

            # Check CPU every 10 seconds minimum
            if current_time - self.last_cpu_check >= 10:
                # Get CPU usage percentage (blocking call, but should be fast)
                cpu_percent = psutil.cpu_percent(interval=1)
                self.cpu_history.append(cpu_percent)
                self.last_cpu_check = current_time

                logger.debug(f"CPU usage measured: {cpu_percent}%")

            # Return average of available measurements
            if self.cpu_history:
                return round(sum(self.cpu_history) / len(self.cpu_history), 1)
            else:
                # Fallback to current measurement
                return round(psutil.cpu_percent(interval=0.1), 1)

        except Exception as e:
            logger.error(f"Failed to poll CPU usage: {e}")
            return 0.0

    async def poll_api_status(self):
        """Poll main API status from https://renderfin.com/api-render."""
        api_url = "https://renderfin.com/api-render"

        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
                'Accept': 'application/json, text/plain, */*'
            }
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10), headers=headers) as session:
                async with session.get(api_url) as response:
                    logger.debug(f"API status response: {response.status}")

                    if response.status == 200:
                        try:
                            data = await response.json()
                            servers = data.get('servers', [])
                            tasks = data.get('tasks', [])

                            # Count pending tasks
                            pending_count = sum(1 for task in tasks if hasattr(task, 'status') and task.get('status') == 'Pending')

                            self.api_status = {
                                'online': True,
                                'servers_count': len(servers),
                                'tasks_count': len(tasks),
                                'pending_count': pending_count
                            }
                            logger.info(f"✅ API online: {len(servers)} servers, {len(tasks)} tasks, {pending_count} pending")
                        except Exception as e:
                            logger.warning(f"Could not parse API JSON: {e}")
                            self.api_status = {'online': False, 'servers_count': 0, 'tasks_count': 0, 'pending_count': 0}
                    else:
                        logger.warning(f"API returned HTTP {response.status}")
                        self.api_status = {'online': False, 'servers_count': 0, 'tasks_count': 0, 'pending_count': 0}

        except Exception as e:
            logger.warning(f"Failed to poll API: {e}")
            self.api_status = {'online': False, 'servers_count': 0, 'tasks_count': 0, 'pending_count': 0}

    async def poll_converter(self, name: str, url: str) -> ConverterStatus:
        """Poll a converter server for status information."""
        status = ConverterStatus(name=name)
        # Use the base URL directly (according to TЗ, it's /api-converter-glb)
        full_url = url

        logger.debug(f"Polling converter {name} at {url}")

        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
                'Accept': 'application/json, text/plain, */*'
            }
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10), headers=headers) as session:
                async with session.get(url) as response:
                    logger.debug(f"Server {name} responded with status {response.status}")

                    if response.status == 200:
                        try:
                            data = await response.json()
                            logger.info(f"✅ {name}: active={data.get('total_active', 0)}, completed={data.get('total_completed_tasks', 0)}")
                            status.online = True
                            status.active_tasks = data.get('total_active', 0)
                            status.queue_size = data.get('queue_size', 0)
                            status.server_version = data.get('server_version', '')
                            status.total_completed = data.get('total_completed_tasks', 0)

                            # Store active tasks data for image processing
                            status.active_tasks_data = data.get('active_tasks', {})

                            # Track task start times for timing
                            current_active_task_ids = set(status.active_tasks_data.keys())

                            # Initialize server tracking if not exists
                            if name not in self.task_start_times:
                                self.task_start_times[name] = {}

                            # Remove completed tasks
                            completed_tasks = []
                            for task_id in self.task_start_times[name]:
                                if task_id not in current_active_task_ids:
                                    completed_tasks.append(task_id)

                            for task_id in completed_tasks:
                                del self.task_start_times[name][task_id]
                                logger.debug(f"Task {task_id} on {name} completed, removed timing")

                            # Add new tasks with start time and calculate durations
                            import time
                            current_time = time.time()

                            # Calculate durations for active tasks and store output URLs
                            status.task_durations = {}
                            for task_id in current_active_task_ids:
                                if task_id not in self.task_start_times[name]:
                                    self.task_start_times[name][task_id] = current_time
                                    logger.debug(f"Task {task_id} on {name} started at {current_time}")
                                    status.task_durations[task_id] = 0
                                else:
                                    duration = int(current_time - self.task_start_times[name][task_id])
                                    status.task_durations[task_id] = duration


                            # Check for completed tasks and send completion videos
                            for task_id in completed_tasks:
                                if task_id not in self.completed_tasks:
                                    # Check if task has video output and send completion message
                                    await self.check_and_send_completion_video(name, task_id, status.active_tasks_data.get(task_id, {}))
                                    self.completed_tasks.add(task_id)

                        except Exception as e:
                            logger.warning(f"Could not parse JSON from {name}: {e}")
                            status.online = False
                    else:
                        logger.warning(f"Server {name} returned HTTP {response.status}")
                        status.online = False
        except Exception as e:
            logger.warning(f"Failed to poll converter {name}: {e}")
            status.online = False

        logger.debug(f"Final status for {name}: online={status.online}")
        return status

    async def check_task_images(self, converter_statuses: Dict[str, ConverterStatus]) -> List[str]:
        """Check for available *_view.jpg images from active tasks on all servers."""
        available_images = []

        try:
            # Process active tasks from all online servers
            for status in converter_statuses.values():
                if status.online and status.active_tasks_data:
                    logger.debug(f"Checking images for server {status.name} with {len(status.active_tasks_data)} active tasks")

                    for task_id, task_data in status.active_tasks_data.items():
                        if 'output_urls' in task_data and task_data['output_urls']:
                            for url in task_data['output_urls']:
                                if url.endswith('_view.jpg'):
                                    try:
                                        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
                                            async with session.head(url) as response:
                                                if response.status == 200:
                                                    available_images.append(url)
                                                    logger.debug(f"✅ Found available image: {url}")
                                                else:
                                                    logger.debug(f"❌ Image not available (HTTP {response.status}): {url}")
                                    except Exception as e:
                                        logger.debug(f"Failed to check image {url}: {e}")

            # Limit to 10 images max
            available_images = available_images[:10]

            if available_images:
                logger.info(f"📸 Found {len(available_images)} active task images to display")
            else:
                logger.debug("No active task images found")

        except Exception as e:
            logger.error(f"Error checking task images: {e}")

        return available_images

    async def check_and_send_completion_video(self, server_name: str, task_id: str, task_data: dict):
        """Check for completion video and send to Telegram if found."""
        try:
            # Get server URL for API call
            server_url = self.converter_servers.get(server_name)
            if not server_url:
                logger.error(f"Unknown server {server_name} for task {task_id}")
                return

            # Query task status from server API
            task_status_url = f"{server_url}/status/{task_id}"
            logger.debug(f"Checking task status: {task_status_url}")

            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.get(task_status_url) as response:
                    if response.status != 200:
                        logger.debug(f"Task status not available: {task_status_url} (HTTP {response.status})")
                        return

                    task_info = await response.json()
                    logger.debug(f"Got task info for {task_id}: {task_info}")

            # Get output URLs from task info
            output_urls = task_info.get('output_urls', [])
            if not output_urls:
                logger.debug(f"No output URLs for completed task {task_id}")
                return

            # Find video files (.mp4 priority, then .mov)
            video_urls = []
            for url in output_urls:
                if isinstance(url, str):
                    if url.endswith('.mp4'):
                        video_urls.append(url)
                    elif url.endswith('.mov'):
                        video_urls.append(url)

            # Sort by priority (.mp4 first)
            video_urls.sort(key=lambda x: 0 if x.endswith('.mp4') else 1)

            if not video_urls:
                logger.debug(f"No video files found for completed task {task_id}")
                return

            # Check if video is accessible (HEAD request)
            video_url = video_urls[0]  # Use first (highest priority) video
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                    async with session.head(video_url) as response:
                        if response.status != 200:
                            logger.debug(f"Video not accessible: {video_url} (HTTP {response.status})")
                            return
            except Exception as e:
                logger.debug(f"Failed to check video accessibility: {video_url} - {e}")
                return

            # Send completion message with video
            await self.send_completion_video(server_name, task_id, video_url, task_info)

            logger.info(f"✅ Sent completion video for task {task_id} on {server_name}")

        except Exception as e:
            logger.error(f"Error checking completion video for task {task_id}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")

    async def send_completion_video(self, server_name: str, task_id: str, video_url: str, task_info: dict):
        """Send completion video to all subscribed chats."""
        try:
            # Get task duration if available
            duration_text = ""
            start_time = None
            for server_tasks in self.task_start_times.values():
                if task_id in server_tasks:
                    start_time = server_tasks[task_id]
                    break

            if start_time:
                import time
                total_duration = int(time.time() - start_time)
                minutes = total_duration // 60
                seconds = total_duration % 60
                duration_text = f" ⏱️ {minutes:02d}:{seconds:02d}"

            # Get additional task info
            task_type = "Unknown"
            if 'output_urls' in task_info:
                output_urls = task_info['output_urls']
                if any('.mp4' in url for url in output_urls if isinstance(url, str)):
                    task_type = "Video Render"
                elif any('.mov' in url for url in output_urls if isinstance(url, str)):
                    task_type = "Animation"
                elif any('.png' in url for url in output_urls if isinstance(url, str)):
                    task_type = "Image Render"
                elif any('.glb' in url for url in output_urls if isinstance(url, str)):
                    task_type = "3D Model"

            # Create completion message
            from datetime import datetime
            current_time = datetime.now().strftime("%H:%M")

            caption = f"""🎬 <b>Task Completed Successfully! 🎉</b>

📍 <b>Server:</b> {server_name}
🆔 <b>Task ID:</b> {task_id[:8]}...
🎯 <b>Type:</b> {task_type}
{duration_text}
✅ <b>Status:</b> Render Complete

📎 <b>Output:</b> {len(task_info.get('output_urls', []))} files generated

⏰ <b>Completed at {current_time}</b>"""

            # Send video to all subscribed chats
            tasks = []
            for chat_id in self.session_manager.subscribed_chats:
                task = self.bot.send_video(
                    chat_id=chat_id,
                    video=video_url,
                    caption=caption,
                    parse_mode='HTML',
                    supports_streaming=True
                )
                tasks.append(task)

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                success_count = sum(1 for r in results if not isinstance(r, Exception))
                logger.info(f"Sent completion video to {success_count}/{len(tasks)} chats")

                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.error(f"Failed to send completion video to chat {self.session_manager.subscribed_chats[i]}: {result}")

        except Exception as e:
            logger.error(f"Error sending completion video for task {task_id}: {e}")
            import traceback
            logger.error(f"Video send traceback: {traceback.format_exc()}")

    async def get_all_status(self) -> tuple[int, float, Dict[str, ConverterStatus], List[str]]:
        """Get comprehensive status: disk space, CPU usage, converter statuses, API status, and available images."""
        # Poll API status
        await self.poll_api_status()

        # Poll disk space and CPU usage in parallel
        disk_task = self.poll_disk_space()
        cpu_task = self.poll_cpu_usage()

        disk_free, cpu_usage = await asyncio.gather(disk_task, cpu_task)

        # Poll all converters in parallel
        tasks = []
        for name, url in self.converter_servers.items():
            tasks.append(self.poll_converter(name, url))

        converter_statuses = {}
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, result in enumerate(results):
            name = list(self.converter_servers.keys())[i]
            if isinstance(result, Exception):
                logger.error(f"Error polling {name}: {result}")
                converter_statuses[name] = ConverterStatus(name=name)
            else:
                converter_statuses[name] = result

        # Check for available images from active tasks
        available_images = await self.check_task_images(converter_statuses)

        return disk_free, cpu_usage, converter_statuses, available_images


class SessionManager:
    """Tracks messages for cleanup between sessions."""

    def __init__(self, data_dir: str = "renderfarmer_data"):
        self.data_dir = data_dir
        self.sessions_file = os.path.join(data_dir, "sessions.json")
        self.chats_file = os.path.join(data_dir, "chats.json")
        self.session_id = str(uuid.uuid4())
        self.sent_messages: List[Dict] = []
        self.subscribed_chats: List[int] = []

        # Ensure data directory exists
        os.makedirs(data_dir, exist_ok=True)

        # Load persisted data
        self._load_sessions()
        self._load_chats()

    def _load_sessions(self):
        """Load previous session data."""
        try:
            if os.path.exists(self.sessions_file):
                with open(self.sessions_file, 'r') as f:
                    data = json.load(f)
                    # Only load messages from previous sessions
                    self.sent_messages = [
                        msg for msg in data.get('messages', [])
                        if msg.get('session_id') != self.session_id
                    ]
        except Exception as e:
            logger.error(f"Failed to load sessions: {e}")

    def _load_chats(self):
        """Load subscribed chat IDs."""
        try:
            if os.path.exists(self.chats_file):
                with open(self.chats_file, 'r') as f:
                    data = json.load(f)
                    self.subscribed_chats = data.get('chats', [])
        except Exception as e:
            logger.error(f"Failed to load chats: {e}")

    def save_message(self, chat_id: int, message_id: int, message_type: str = "status"):
        """Save a sent message for later cleanup."""
        message_data = {
            'session_id': self.session_id,
            'chat_id': chat_id,
            'message_id': message_id,
            'type': message_type
        }
        self.sent_messages.append(message_data)
        self.persist_to_json()

    def subscribe_chat(self, chat_id: int):
        """Subscribe a chat to receive updates."""
        if chat_id not in self.subscribed_chats:
            self.subscribed_chats.append(chat_id)
            self._save_chats()

    def unsubscribe_chat(self, chat_id: int):
        """Unsubscribe a chat from updates."""
        if chat_id in self.subscribed_chats:
            self.subscribed_chats.remove(chat_id)
            self._save_chats()

    def _save_chats(self):
        """Save subscribed chats to JSON."""
        try:
            data = {'chats': self.subscribed_chats}
            with open(self.chats_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save chats: {e}")

    async def clear_previous_session(self, bot: Bot):
        """Delete all messages from previous sessions."""
        current_session_messages = [msg for msg in self.sent_messages if msg['session_id'] == self.session_id]

        for message in self.sent_messages:
            if message['session_id'] != self.session_id:
                try:
                    await bot.delete_message(
                        chat_id=message['chat_id'],
                        message_id=message['message_id']
                    )
                    logger.info(f"Deleted message {message['message_id']} from chat {message['chat_id']}")
                except BadRequest as e:
                    if "message to delete not found" in str(e):
                        logger.debug(f"Message {message['message_id']} already deleted")
                    else:
                        logger.warning(f"Failed to delete message {message['message_id']}: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error deleting message {message['message_id']}: {e}")

        # Keep only current session messages
        self.sent_messages = current_session_messages
        self.persist_to_json()

    def persist_to_json(self):
        """Save current session messages to JSON."""
        try:
            data = {'messages': self.sent_messages}
            with open(self.sessions_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to persist sessions: {e}")


class RenderFarmerBot:
    """Main bot class coordinating all functionality."""

    def __init__(self, token: str):
        self.token = token
        self.bot = Bot(token=token)
        self.spy_server = SpyServer()
        self.session_manager = SessionManager()
        self.application = Application.builder().token(token).build()

        # Load version info
        self.version = self.load_version()

        # Set up command handlers
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("stop", self.stop_command))
        self.application.add_handler(CommandHandler("status", self.status_command))
        self.application.add_handler(CommandHandler("version", self.version_command))

        # Set up callback query handler for inline keyboard buttons
        self.application.add_handler(CallbackQueryHandler(self.handle_callback))

        # Set up post-init function to schedule jobs after application starts
        self.application.post_init = self.post_init

        # Track the two permanent messages per chat
        self.status_messages: Dict[int, int] = {}  # status_message ID per chat
        self.results_messages: Dict[int, int] = {}  # last_results_message ID per chat

        # Track content to avoid unnecessary edits
        self.last_status_content: Dict[int, str] = {}
        self.last_results_content: Dict[int, str] = {}

    def load_version(self) -> str:
        """Load version information from file."""
        try:
            with open('/root/renderfarmerbot_version.txt', 'r') as f:
                return f.readline().strip()
        except:
            return "v2.1.0"

    def format_status_text(self, disk_free: int, cpu_usage: float, converter_statuses: Dict[str, ConverterStatus]) -> tuple[str, object]:
        """Format status information as HTML text with inline keyboard."""
        # API Status with better formatting
        api_emoji = "🟢" if self.spy_server.api_status['online'] else "🔴"
        api_status_line = f"{api_emoji} <b>API Status:</b> {self.spy_server.api_status['servers_count']} servers, {self.spy_server.api_status['tasks_count']} tasks"

        # System status
        disk_line = f"💾 <b>Disk:</b> {disk_free} GB free"
        cpu_line = f"⚡ <b>CPU:</b> {cpu_usage}% avg (10 min)"

        # Calculate totals for prominent display
        online_servers = [s for s in converter_statuses.values() if s.online]
        total_active = sum(s.active_tasks for s in online_servers)
        total_queue = sum(s.queue_size for s in online_servers)
        total_completed = sum(s.total_completed for s in online_servers)
        online_count = len(online_servers)

        # ACTIVE TASKS - make it very prominent
        if total_active > 0:
            active_header = f"\n🎯 <b>⚠️ ACTIVE RENDERING: {total_active} tasks running ⚠️</b>"
        else:
            active_header = f"\n🎯 <b>No active tasks</b>"

        # Queue status - make it prominent if there are queued tasks
        if total_queue > 0:
            queue_header = f"\n📋 <b>🔶 QUEUED: {total_queue} tasks waiting 🔶</b>"
        else:
            queue_header = ""

        # Server status lines with color coding
        server_lines = []
        for name in ['F1', 'F2', 'F7', 'F11', 'F13']:
            status = converter_statuses.get(name, ConverterStatus(name=name))
            if status.online:
                if status.active_tasks > 0:
                    # Show task durations for active servers
                    duration_text = ""
                    if status.task_durations:
                        durations = []
                        for task_id, duration in status.task_durations.items():
                            minutes = duration // 60
                            seconds = duration % 60
                            durations.append(f"{minutes:02d}:{seconds:02d}")
                        duration_text = f" ⏱️ {', '.join(durations)}"

                    # Green: actively rendering
                    server_lines.append(f"🟢 <b>{name}:</b> 🔥 <b>{status.active_tasks} ACTIVE</b> | {status.queue_size} queued | ✅ {status.total_completed} done{duration_text}")
                else:
                    # Yellow: online but idle
                    server_lines.append(f"🟡 <b>{name}:</b> idle | {status.queue_size} queued | ✅ {status.total_completed} done")
            else:
                # Red: offline
                server_lines.append(f"🔴 <b>{name}:</b> ❌ offline")

        servers_text = "\n".join(server_lines)

        # Summary
        summary_line = f"\n📊 <b>Summary:</b> {online_count}/5 servers online | Total completed: {total_completed}"

        # Add timestamp signature with alarm clock emoji
        from datetime import datetime
        current_time = datetime.now().strftime("%H:%M")
        timestamp_line = f"\n\n⏰ <b>Updated at {current_time}</b>"

        # Main message text
        message_text = f"🖥 <b>RenderFarm Status</b> {self.version}\n\n{api_status_line}\n{disk_line}\n{cpu_line}{active_header}{queue_header}\n\n{servers_text}{summary_line}{timestamp_line}"

        # Create inline keyboard
        keyboard = [
            # API and refresh buttons
            [
                InlineKeyboardButton("🌐 API", url="https://renderfin.com/api-render"),
                InlineKeyboardButton("🔄 Refresh", callback_data="refresh_status")
            ],
            # Server management buttons (first row)
            [
                InlineKeyboardButton("⚙️ F1", url="http://5.129.157.224:5132/api-converter-glb-ui"),
                InlineKeyboardButton("⚙️ F2", url="http://5.129.157.224:5279/api-converter-glb-ui"),
                InlineKeyboardButton("⚙️ F7", url="http://5.129.157.224:5131/api-converter-glb-ui")
            ],
            # Server management buttons (second row)
            [
                InlineKeyboardButton("⚙️ F11", url="http://5.129.157.224:5533/api-converter-glb-ui"),
                InlineKeyboardButton("⚙️ F13", url="http://5.129.157.224:5267/api-converter-glb-ui"),
                InlineKeyboardButton("📊 Tasks", callback_data="show_tasks")
            ],
            # Test links section
            [
                InlineKeyboardButton("🔄 Перезапуск F1", url="http://5.129.157.224:5132/api-converter-glb-restart-server"),
                InlineKeyboardButton("⚙️ Управление F2", url="http://5.129.157.224:5279/api-converter-glb-ui")
            ]
        ]

        # Restart buttons for offline servers (only show if there are offline servers)
        if online_count < 5:
            keyboard.extend([
                [
                    InlineKeyboardButton("🔄 F1", url="http://5.129.157.224:5132/api-converter-glb-restart-server"),
                    InlineKeyboardButton("🔄 F2", url="http://5.129.157.224:5279/api-converter-glb-restart-server"),
                    InlineKeyboardButton("🔄 F7", url="http://5.129.157.224:5131/api-converter-glb-restart-server")
                ],
                [
                    InlineKeyboardButton("🔄 F11", url="http://5.129.157.224:5533/api-converter-glb-restart-server"),
                    InlineKeyboardButton("🔄 F13", url="http://5.129.157.224:5267/api-converter-glb-restart-server")
                ]
            ])

        reply_markup = InlineKeyboardMarkup(keyboard)

        return message_text, reply_markup

    async def download_image(self, url: str, filename: str) -> Optional[bytes]:
        """Download an image from URL."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        return await response.read()
        except Exception as e:
            logger.error(f"Failed to download image {url}: {e}")
        return None

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command."""
        if not update.effective_chat:
            return

        chat_id = update.effective_chat.id
        logger.info(f"Received /start command from chat {chat_id}")

        was_already_subscribed = chat_id in self.session_manager.subscribed_chats

        if not was_already_subscribed:
            self.session_manager.subscribe_chat(chat_id)

            # Create permanent messages for new subscriber
            try:
                # Create status message
                status_text = "🖥 RenderFarm Status\n💾 Initializing..."
                status_msg = await self.bot.send_message(chat_id=chat_id, text=status_text)
                self.status_messages[chat_id] = status_msg.message_id
                self.last_status_content[chat_id] = status_text
                self.session_manager.save_message(chat_id, status_msg.message_id, "status")

                # Create results message
                results_text = "📊 Completed Tasks\n⏳ Loading..."
                results_msg = await self.bot.send_message(chat_id=chat_id, text=results_text)
                self.results_messages[chat_id] = results_msg.message_id
                self.last_results_content[chat_id] = results_text
                self.session_manager.save_message(chat_id, results_msg.message_id, "results")

                # Send immediate update
                disk_free, converter_statuses, _ = await self.spy_server.get_all_status()
                await self.update_status_message(chat_id, disk_free, converter_statuses)
                await self.update_results_message(chat_id, converter_statuses)

                await update.message.reply_text("✅ Subscribed! You now have 2 permanent status messages.")
                logger.info(f"Created permanent messages for new subscriber {chat_id}")

            except Exception as e:
                logger.error(f"Failed to create permanent messages for chat {chat_id}: {e}")
                await update.message.reply_text("❌ Failed to set up status messages. Please try again.")
        else:
            await update.message.reply_text("✅ You are already subscribed to RenderFarm updates!")

        logger.info(f"Total subscribed chats: {len(self.session_manager.subscribed_chats)}")

    async def stop_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /stop command."""
        if not update.effective_chat:
            return

        chat_id = update.effective_chat.id
        self.session_manager.unsubscribe_chat(chat_id)

        # Remove from last status messages
        self.last_status_messages.pop(chat_id, None)

        await update.message.reply_text("❌ Unsubscribed from RenderFarm status updates.")

    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /status command - force immediate status update."""
        if not update.effective_chat:
            return

        chat_id = update.effective_chat.id

        # Get current status
        disk_free, cpu_usage, converter_statuses, image_urls = await self.spy_server.get_all_status()

        # Force update by clearing cached content
        self.last_status_content.pop(chat_id, None)
        self.last_results_content.pop(chat_id, None)

        # Update both permanent messages
        await self.update_status_message(chat_id, disk_free, cpu_usage, converter_statuses, image_urls)
        await self.update_results_message(chat_id, converter_statuses)

        await update.message.reply_text("✅ Status updated!")

    async def version_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /version command - show bot version and info."""
        if not update.effective_chat:
            return

        try:
            with open('/root/renderfarmerbot_version.txt', 'r') as f:
                version_info = f.read()
        except:
            version_info = f"{self.version}\n\nVersion file not found."

        await update.message.reply_text(
            f"🤖 <b>RenderFarmer Bot</b>\n\n{version_info}",
            parse_mode='HTML'
        )

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle callback queries from inline keyboard buttons."""
        query = update.callback_query
        if not query:
            return

        await query.answer()  # Acknowledge the callback

        chat_id = query.message.chat_id
        data = query.data

        if data == "refresh_status":
            # Force refresh status
            logger.info(f"Manual refresh requested for chat {chat_id}")
            disk_free, cpu_usage, converter_statuses, image_urls = await self.spy_server.get_all_status()

            # Force update by clearing cached content
            self.last_status_content.pop(chat_id, None)
            self.last_results_content.pop(chat_id, None)

            # Update both permanent messages
            await self.update_status_message(chat_id, disk_free, cpu_usage, converter_statuses)
            await self.update_results_message(chat_id, converter_statuses)

        elif data == "show_tasks":
            # Show detailed task information
            disk_free, cpu_usage, converter_statuses, image_urls = await self.spy_server.get_all_status()

            task_info = ["📋 <b>Detailed Task Status:</b>"]
            for name in ['F1', 'F2', 'F7', 'F11', 'F13']:
                status = converter_statuses.get(name, ConverterStatus(name=name))
                if status.online:
                    task_info.append(f"🟢 <b>{name}:</b> {status.active_tasks} active, {status.queue_size} queued, {status.total_completed} total")
                else:
                    task_info.append(f"🔴 <b>{name}:</b> Server offline")

            await query.message.reply_text(
                "\n".join(task_info),
                parse_mode='HTML'
            )

    async def create_permanent_messages(self):
        """Create the two permanent messages for all subscribed chats."""
        logger.info(f"Creating permanent messages for {len(self.session_manager.subscribed_chats)} chats")

        for chat_id in self.session_manager.subscribed_chats:
            try:
                logger.info(f"Creating messages for chat {chat_id}")

                # Get initial status - ensure API status is polled first
                await self.spy_server.poll_api_status()
                disk_free, cpu_usage, converter_statuses, image_urls = await self.spy_server.get_all_status()
                logger.info(f"Got status data: disk={disk_free}, cpu={cpu_usage}, servers={len(converter_statuses)}, api_online={self.spy_server.api_status['online']}")

                # Create status message with inline keyboard
                status_text, reply_markup = self.format_status_text(disk_free, cpu_usage, converter_statuses)
                logger.info(f"Status text length: {len(status_text)}")

                status_msg = await self.bot.send_message(
                    chat_id=chat_id,
                    text=status_text,
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
                logger.info(f"Status message sent with ID {status_msg.message_id}")

                self.status_messages[chat_id] = status_msg.message_id
                self.last_status_content[chat_id] = status_text
                self.session_manager.save_message(chat_id, status_msg.message_id, "status")

                # Create results message
                await self.update_results_message(chat_id, converter_statuses)

                # Send media group if there are active task images
                if image_urls:
                    await self.send_media_group(chat_id, status_text, image_urls)

                logger.info(f"✅ Created permanent messages for chat {chat_id}")

            except Exception as e:
                logger.error(f"❌ Failed to create permanent messages for chat {chat_id}: {e}")
                import traceback
                logger.error(f"Full traceback: {traceback.format_exc()}")

    async def update_status_message(self, chat_id: int, disk_free: int, cpu_usage: float, converter_statuses: Dict[str, ConverterStatus], available_images: List[str] = None):
        """Update the status message for a chat with inline keyboard and optional media group."""
        status_text, reply_markup = self.format_status_text(disk_free, cpu_usage, converter_statuses)

        if available_images is None:
            available_images = []

        logger.debug(f"Status text for chat {chat_id}: {status_text[:100]}...")

        if chat_id in self.last_status_content and self.last_status_content[chat_id] == status_text:
            # Content hasn't changed, skip update
            logger.debug(f"Status content unchanged for chat {chat_id}, skipping update")
            return

        try:
            if chat_id in self.status_messages:
                # Edit existing message
                logger.info(f"Editing status message {self.status_messages[chat_id]} for chat {chat_id}")
                await self.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=self.status_messages[chat_id],
                    text=status_text,
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
                logger.info(f"✅ Updated status message for chat {chat_id}")
            else:
                # Create new message if it doesn't exist
                logger.info(f"Creating new status message for chat {chat_id}")
                msg = await self.bot.send_message(
                    chat_id=chat_id,
                    text=status_text,
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
                self.status_messages[chat_id] = msg.message_id
                self.session_manager.save_message(chat_id, msg.message_id, "status")
                logger.info(f"✅ Created status message {msg.message_id} for chat {chat_id}")

            self.last_status_content[chat_id] = status_text

            # Send media group if there are active task images
            if available_images:
                await self.send_media_group(chat_id, status_text, available_images)

        except Exception as e:
            logger.error(f"Failed to update status message for chat {chat_id}: {e}")

    async def send_media_group(self, chat_id: int, caption: str, image_urls: List[str]):
        """Send media group with active task images."""
        try:
            if not image_urls:
                return

            logger.info(f"📸 Sending media group with {len(image_urls)} images to chat {chat_id}")

            # Prepare media group with timestamp
            from telegram import InputMediaPhoto
            from datetime import datetime

            # Add timestamp to caption
            current_time = datetime.now().strftime("%H:%M")
            timestamped_caption = f"{caption}\n\n⏰ <b>Updated at {current_time}</b>"

            media = []
            for i, url in enumerate(image_urls):
                if i == 0:
                    # First image gets the timestamped caption
                    media.append(InputMediaPhoto(media=url, caption=timestamped_caption, parse_mode='HTML'))
                else:
                    media.append(InputMediaPhoto(media=url))

            # Send media group
            await self.bot.send_media_group(chat_id=chat_id, media=media)

            logger.info(f"✅ Sent media group with {len(media)} images to chat {chat_id}")

        except Exception as e:
            logger.error(f"❌ Failed to send media group to chat {chat_id}: {e}")
            import traceback
            logger.error(f"Media group traceback: {traceback.format_exc()}")

    async def update_results_message(self, chat_id: int, converter_statuses: Dict[str, ConverterStatus]):
        """Update the results message for a chat with HTML formatting."""
        # Show completed tasks count for each server
        lines = ["📊 <b>Completed Tasks by Server</b>"]
        total_completed = 0

        for name in ['F1', 'F2', 'F7', 'F11', 'F13']:
            status = converter_statuses.get(name, ConverterStatus(name=name))
            if status.online:
                lines.append(f"🟢 <b>{name}:</b> ✅ {status.total_completed} completed")
                total_completed += status.total_completed
            else:
                lines.append(f"🔴 <b>{name}:</b> ❌ offline")

        lines.append(f"\n🎯 <b>Total:</b> {total_completed} tasks completed")

        results_text = "\n".join(lines)

        if chat_id in self.last_results_content and self.last_results_content[chat_id] == results_text:
            # Content hasn't changed, skip update
            return

        try:
            if chat_id in self.results_messages:
                # Edit existing message
                await self.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=self.results_messages[chat_id],
                    text=results_text,
                    parse_mode='HTML'
                )
                logger.debug(f"Updated results message for chat {chat_id}")
            else:
                # Create new message if it doesn't exist
                msg = await self.bot.send_message(
                    chat_id=chat_id,
                    text=results_text,
                    parse_mode='HTML'
                )
                self.results_messages[chat_id] = msg.message_id
                self.session_manager.save_message(chat_id, msg.message_id, "results")

            self.last_results_content[chat_id] = results_text

        except Exception as e:
            logger.error(f"Failed to update results message for chat {chat_id}: {e}")

    async def send_test_message(self):
        """Send test message with HTML formatting and version info."""
        try:
            from datetime import datetime
            current_time = datetime.now().strftime("%H:%M:%S")

            # Test message with HTML formatting and timestamp
            timestamp = f"⏰ <b>Sent at {current_time}</b>"
            test_message = f"""🤖 <b>RenderFarmer Bot {self.version}</b>

✅ Бот запущен и работает!
📊 Система готова к мониторингу

🔗 <b>Тестовые ссылки:</b>
🔄 Перезапуск F1 (<a href="http://5.129.157.224:5132/api-converter-glb-restart-server">ссылка</a>)
⚙️ Управление F2 (<a href="http://5.129.157.224:5279/api-converter-glb-ui">ссылка</a>)

{timestamp}"""

            tasks = []
            for chat_id in self.session_manager.subscribed_chats:
                task = self.bot.send_message(
                    chat_id=chat_id,
                    text=test_message,
                    parse_mode='HTML',
                    disable_web_page_preview=True
                )
                tasks.append(task)

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                success_count = sum(1 for r in results if not isinstance(r, Exception))
                logger.info(f"Sent test message to {success_count}/{len(tasks)} chats")
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.error(f"Failed to send test message to chat {self.session_manager.subscribed_chats[i]}: {result}")
            else:
                logger.info("No subscribed chats for test message")

        except Exception as e:
            logger.error(f"Error sending test message: {e}")

    async def cleanup_old_messages(self, chat_id: int):
        """Remove old messages that are not the two permanent ones."""
        # This will be called periodically to clean up any stray messages
        pass  # For now, we'll rely on session cleanup

    async def status_update_job(self, context):
        """Job that runs every 60 seconds to update status and results messages."""
        try:
            logger.debug("Running status update job")

            # Get current status
            disk_free, cpu_usage, converter_statuses, image_urls = await self.spy_server.get_all_status()

            # Update both messages for all subscribed chats
            for chat_id in self.session_manager.subscribed_chats:
                await self.update_status_message(chat_id, disk_free, cpu_usage, converter_statuses, image_urls)
                await self.update_results_message(chat_id, converter_statuses)

            logger.debug(f"Updated messages for {len(self.session_manager.subscribed_chats)} chats")

        except Exception as e:
            logger.error(f"Error in status update job: {e}")

    async def post_init(self, application):
        """Called after application initialization."""
        # Schedule status updates every 60 seconds
        application.job_queue.run_repeating(self.status_update_job, interval=60, first=10)

        # Create permanent messages for all subscribed chats
        await self.create_permanent_messages()

        # Send test message with markdown links
        await self.send_test_message()

    async def run(self):
        """Start the bot."""
        # Clear previous session messages
        await self.session_manager.clear_previous_session(self.bot)

        # Start the bot
        logger.info("Starting RenderFarmer Telegram Bot...")
        try:
            await self.application.run_polling(
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=True
            )
        except RuntimeError as e:
            if "Cannot close a running event loop" in str(e):
                logger.info("Event loop closure prevented - this is normal in systemd environment")
                # Keep the application alive manually
                import signal
                import asyncio

                # Create a future that never completes to keep the event loop running
                stop_future = asyncio.Future()

                def signal_handler(signum, frame):
                    logger.info("Received signal, shutting down...")
                    stop_future.set_result(None)

                signal.signal(signal.SIGTERM, signal_handler)
                signal.signal(signal.SIGINT, signal_handler)

                await stop_future
            else:
                raise


async def main():
    """Main entry point."""
    token = os.getenv('TELEGRAM_BOT_TOKEN')
    if not token:
        logger.error("TELEGRAM_BOT_TOKEN environment variable not set!")
        return

    bot = RenderFarmerBot(token)
    await bot.run()


if __name__ == "__main__":
    # Fix nested event loop issues
    import nest_asyncio
    nest_asyncio.apply()

    asyncio.run(main())