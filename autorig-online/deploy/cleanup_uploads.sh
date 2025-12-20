#!/bin/bash
# Cleanup old uploads (older than 24 hours)
# Run via cron: 0 */6 * * * /opt/autorig-online/deploy/cleanup_uploads.sh

UPLOAD_DIR="/var/autorig/uploads"
MAX_AGE_MINUTES=1440  # 24 hours

echo "$(date): Starting upload cleanup..."

# Delete files older than MAX_AGE_MINUTES
find "$UPLOAD_DIR" -type f -mmin +$MAX_AGE_MINUTES -delete 2>/dev/null

# Delete empty directories
find "$UPLOAD_DIR" -type d -empty -delete 2>/dev/null

echo "$(date): Cleanup completed"

