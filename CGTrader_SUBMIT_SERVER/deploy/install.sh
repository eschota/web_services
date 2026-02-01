#!/bin/bash
# CGTrader Submit Server - Installation Script

set -e

echo "=== CGTrader Submit Server Installation ==="

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo "Please run as root"
    exit 1
fi

PROJECT_DIR="/root/CGTrader_SUBMIT_SERVER"

# Install system dependencies
echo "Installing system dependencies..."
apt-get update
apt-get install -y chromium-browser chromium-chromedriver xvfb

# Create virtual display for headless Chrome (if needed)
echo "Setting up virtual display..."
if ! pgrep -x "Xvfb" > /dev/null; then
    Xvfb :99 -screen 0 1920x1080x24 &
    echo "export DISPLAY=:99" >> /etc/environment
fi

# Install Python dependencies
echo "Installing Python dependencies..."
cd "$PROJECT_DIR"
if [ ! -d "venv" ]; then
    python3 -m venv venv
fi
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Initialize database
echo "Initializing database..."
python -c "import database; print('Database initialized:', database.DB_PATH)"

# Install systemd service
echo "Installing systemd service..."
cp deploy/cgtrader_submit.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable cgtrader-submit

# Update nginx configuration
echo "Updating nginx configuration..."
NGINX_CONF="/etc/nginx/sites-enabled/default"
if [ -f "$NGINX_CONF" ]; then
    # Check if our location already exists
    if ! grep -q "api-submit-cgtrader" "$NGINX_CONF"; then
        echo "Adding CGTrader location to nginx..."
        # Insert before the closing } of the server block
        # This is a simplified approach - may need manual adjustment
        cat deploy/nginx_location.conf
        echo ""
        echo "Please manually add the above location block to your nginx config"
        echo "Then run: nginx -t && systemctl reload nginx"
    else
        echo "Nginx location already configured"
    fi
fi

echo ""
echo "=== Installation Complete ==="
echo ""
echo "To start the service:"
echo "  systemctl start cgtrader-submit"
echo ""
echo "To check status:"
echo "  systemctl status cgtrader-submit"
echo ""
echo "To view logs:"
echo "  journalctl -u cgtrader-submit -f"
echo ""
echo "Don't forget to add the nginx location block!"
