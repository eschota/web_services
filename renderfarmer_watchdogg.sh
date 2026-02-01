#!/bin/bash

# RenderFarmer Watchdogg - monitors code changes and restarts bot
# Usage: ./renderfarmer_watchdogg.sh

WATCH_DIR="/root"
BOT_SERVICE="renderfarmerbot"
LOG_FILE="/var/log/renderfarmer_watchdogg.log"

# Function to log messages
log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

# Function to restart bot
restart_bot() {
    log_message "Detected code changes. Restarting $BOT_SERVICE..."
    sudo systemctl restart "$BOT_SERVICE"

    # Wait a bit and check status
    sleep 3
    if sudo systemctl is-active --quiet "$BOT_SERVICE"; then
        log_message "✅ $BOT_SERVICE restarted successfully"
    else
        log_message "❌ Failed to restart $BOT_SERVICE"
        sudo systemctl status "$BOT_SERVICE" >> "$LOG_FILE" 2>&1
    fi
}

# Create log file if it doesn't exist
touch "$LOG_FILE"

log_message "🚀 Starting RenderFarmer Watchdogg"
log_message "Monitoring directory: $WATCH_DIR"
log_message "Bot service: $BOT_SERVICE"

# Check if bot is currently running
if sudo systemctl is-active --quiet "$BOT_SERVICE"; then
    log_message "✅ $BOT_SERVICE is currently active"
else
    log_message "⚠️  $BOT_SERVICE is not active, starting it..."
    sudo systemctl start "$BOT_SERVICE"
fi

# Monitor file changes using inotifywait
# Watch for: modify, create, delete, move events on Python files
inotifywait -m -r -e modify,create,delete,move "$WATCH_DIR" \
    --include '\.py$' \
    --exclude '/__pycache__/' \
    --exclude '/\.git/' \
    --format '%w%f %e' |
while read file event; do
    # Skip if it's a temporary file or pycache
    if [[ "$file" == *"/__pycache__/"* ]] || [[ "$file" == *".pyc" ]] || [[ "$file" == *".pyo" ]]; then
        continue
    fi

    log_message "📝 File changed: $file ($event)"
    restart_bot
done