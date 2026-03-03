#!/bin/bash
# Deploy task page and rig-editor.js (fixes animations + AO shader)
# Run from: /root/autorig-online

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
STATIC_DIR="${PROJECT_DIR}/static"

echo "Deploying task viewer (task.html + rig-editor.js)..."
echo "Source: $STATIC_DIR"

# Backend reads from same dir when running from /root/autorig-online
# Verify version before deploy
V=$(grep -o "rig-editor.js?v=[0-9]*" "$STATIC_DIR/task.html" | head -1)
echo "Version in task.html: $V"

# Backend STATIC_DIR = /root/autorig-online/static (sibling of backend/)
# No copy needed - we edit in place. Just verify:
test -f "$STATIC_DIR/task.html" && echo "✓ task.html exists"
test -f "$STATIC_DIR/js/rig-editor.js" && echo "✓ rig-editor.js exists"
grep -q "v=67" "$STATIC_DIR/task.html" && echo "✓ task.html has v67" || echo "⚠ task.html version mismatch"
echo ""
echo "Restart backend to pick up changes (if any): systemctl restart autorig"
echo "Verify: curl -s 'https://autorig.online/task?id=test' | grep -o 'rig-editor.js?v=[0-9]*'"
