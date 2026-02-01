# RenderFarmer Telegram Bot v2.1.0

## Overview

Advanced RenderFarm Monitor - monitors disk space, CPU usage, converter server status, and main API status, sending automatic updates to subscribed Telegram chats every 60 seconds with rich HTML formatting and interactive inline keyboards.

## Features

- **CPU Monitoring**: 10-minute average CPU usage tracking
- **Disk Monitoring**: Tracks available disk space in GB
- **API Status**: Monitors main renderfin.com API status
- **Converter Server Monitoring**: Polls 5 converter servers (F1, F2, F7, F11, F13) for status
- **Rich HTML Formatting**: Beautiful status messages with emojis and formatting
- **Interactive Inline Keyboards**: Clickable buttons for server management
- **Image Support**: Downloads and sends preview images when available
- **Message Management**: Two permanent messages that update automatically
- **Auto-restart Watchdogg**: Monitors code changes and restarts bot automatically
- **Version Control**: Built-in version tracking and changelog
- **Commands**: `/start`, `/stop`, `/status`, `/version`

## Setup Instructions

### 1. Telegram Bot Token

You need to obtain a bot token from [@BotFather](https://t.me/botfather) on Telegram.

1. Message `@BotFather` with `/newbot`
2. Follow the prompts to create your bot
3. Copy the token (format: `123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11`)

### 2. Environment Variable

Set the `TELEGRAM_BOT_TOKEN` environment variable:

```bash
export TELEGRAM_BOT_TOKEN="your_bot_token_here"
```

### 3. Install Dependencies

Dependencies are already installed:
- `python-telegram-bot`
- `aiohttp`

### 4. Run the Bot

#### Manual Testing
```bash
cd /root
python3 renderfarmerbot.py
```

#### Systemd Service

The service file is already created at `/etc/systemd/system/renderfarmerbot.service`.

**Important**: Update the bot token in the service file:
```bash
sudo nano /etc/systemd/system/renderfarmerbot.service
# Replace the TELEGRAM_BOT_TOKEN value with your actual token
```

To start the service:
```bash
sudo systemctl daemon-reload
sudo systemctl enable renderfarmerbot
sudo systemctl start renderfarmerbot
```

To check status:
```bash
sudo systemctl status renderfarmerbot
```

To view logs:
```bash
sudo journalctl -u renderfarmerbot -f
```

## File Structure

```
/root/
├── renderfarmerbot.py          # Main bot script
├── renderfarmer_data/
│   ├── sessions.json           # Session history (message IDs to delete)
│   └── chats.json              # Subscribed chat IDs
└── renderfarmerbot.service     # Systemd service file (in /etc/systemd/system/)
```

## Usage

1. Start a chat with your bot on Telegram
2. Send `/start` to subscribe to updates
3. The bot will send status updates every minute
4. Send `/status` for immediate update
5. Send `/stop` to unsubscribe

## Status Format

### Main Status Message (HTML formatted with inline keyboard):
```
🖥 RenderFarm Status v2.1.0

🟢 API Status: 5 servers, 12 tasks
💾 Disk: 37 GB free
⚡ CPU: 47.0% avg (10 min)

🟢 F1: 0 active | 0 queue | ✅ 5 done
🟢 F2: 0 active | 0 queue | ✅ 1 done
🟢 F7: 0 active | 0 queue | ✅ 1 done
🟢 F11: 0 active | 0 queue | ✅ 2 done
🟢 F13: 0 active | 0 queue | ✅ 0 done

📊 Summary: 5/5 servers online | Total: 9 tasks completed
```

### Results Message (completed tasks):
```
📊 Completed Tasks by Server
🟢 F1: ✅ 5 completed
🟢 F2: ✅ 1 completed
🟢 F7: ✅ 1 completed
🟢 F11: ✅ 2 completed
🟢 F13: ✅ 0 completed

🎯 Total: 9 tasks completed
```

### Interactive Buttons:
- 🌐 API - Direct link to API status
- 🔄 Refresh - Manual status update
- ⚙️ F1-F13 - Server management panels
- 🔄 Restart - Server restart links (for offline servers)
- 📊 Tasks - Detailed task information

## Technical Details

- **Polling Interval**: 60 seconds
- **Image Check**: HEAD requests to `*_view.jpg` URLs
- **Message Cleanup**: Deletes messages from previous bot sessions (48h Telegram limit)
- **Error Handling**: Graceful handling of network timeouts and API errors
- **Async Operations**: All network requests are asynchronous

## Watchdogg Auto-restart

Bot includes an automatic code monitoring system that detects changes in source files and restarts the bot automatically.

### Features:
- **File Monitoring**: Watches all `.py` files in `/root` directory
- **Auto-restart**: Instantly restarts bot when code changes are detected
- **Logging**: All actions logged to `/var/log/renderfarmer_watchdogg.log`
- **Safe Operation**: Ignores cache files and temporary files

### Control:
```bash
# Check watchdogg status
sudo systemctl status renderfarmer-watchdogg

# Restart watchdogg
sudo systemctl restart renderfarmer-watchdogg

# View watchdogg logs
sudo journalctl -u renderfarmer-watchdogg -f
```

## Version Control

Bot includes built-in version tracking with changelog:

- **Version File**: `/root/renderfarmerbot_version.txt`
- **Version Command**: `/version` shows current version and features
- **Automatic Updates**: Version displayed in status messages

## Troubleshooting

### Bot not responding:
1. Check if `TELEGRAM_BOT_TOKEN` is set correctly
2. Verify bot token is valid (test with a simple message)
3. Check systemd service status and logs

### Converter servers offline:
- This is normal if servers are down for maintenance
- The bot will show ❌ offline status

### Images not appearing:
- Images only appear when converter servers have active tasks
- Check if `output_urls` contain `*_view.jpg` patterns

## Logs

All bot activity is logged to systemd journal. View with:
```bash
journalctl -u renderfarmerbot -f
```