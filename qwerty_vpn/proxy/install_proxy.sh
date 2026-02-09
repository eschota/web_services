#!/bin/bash
# =============================================================
# QwertyStock VPN - Proxy Node Installer
# Запусти на новом VPS:
#   bash <(curl -sL https://autorig.online/static/vpn/install_proxy.sh)
# =============================================================

set -e

PROXY_HTTP_PORT=49152
PROXY_SOCKS_PORT=49153
GATEWAY_URL="https://autorig.online/vpn"

echo "============================================"
echo "  QwertyStock VPN - Proxy Node Setup"
echo "============================================"
echo ""

# 1. Detect OS
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS=$ID
else
    echo "ERROR: Cannot detect OS"
    exit 1
fi

echo "[1/6] OS detected: $OS ($PRETTY_NAME)"

# 2. Install build tools
echo "[2/6] Installing build tools..."
if [[ "$OS" == "ubuntu" || "$OS" == "debian" ]]; then
    apt update -qq > /dev/null 2>&1
    apt install -y -qq build-essential git curl > /dev/null 2>&1
elif [[ "$OS" == "centos" || "$OS" == "rocky" || "$OS" == "almalinux" || "$OS" == "fedora" ]]; then
    yum install -y -q gcc make git curl > /dev/null 2>&1
else
    echo "WARNING: Unknown OS, trying apt..."
    apt update -qq > /dev/null 2>&1
    apt install -y -qq build-essential git curl > /dev/null 2>&1
fi
echo "    Done"

# 3. Compile 3proxy
echo "[3/6] Compiling 3proxy..."
cd /tmp
rm -rf 3proxy-build
git clone --depth 1 --branch 0.9.4 https://github.com/3proxy/3proxy.git 3proxy-build > /dev/null 2>&1
cd 3proxy-build
make -f Makefile.Linux -j$(nproc) > /dev/null 2>&1
cp bin/3proxy /usr/local/bin/3proxy
chmod +x /usr/local/bin/3proxy
rm -rf /tmp/3proxy-build
echo "    Done: $(/usr/local/bin/3proxy --version 2>&1 | grep '3proxy')"

# 4. Create config
echo "[4/6] Creating configuration..."
mkdir -p /var/log/3proxy /etc/3proxy

cat > /etc/3proxy/3proxy.cfg << 'EOF'
# QwertyStock VPN Proxy Node
daemon
pidfile /var/run/3proxy.pid

log /var/log/3proxy/3proxy.log D
logformat "L%t %N %p %E %C:%c %R:%r %O %I %h %T"
rotate 7

timeouts 1 5 30 60 180 1800 15 60
maxconn 1000

nscache 65536
nserver 8.8.8.8
nserver 1.1.1.1

auth none

proxy -p49152 -n
socks -p49153 -n
EOF
echo "    Done: /etc/3proxy/3proxy.cfg"

# 5. Create systemd service
echo "[5/6] Creating systemd service..."
cat > /etc/systemd/system/qwerty-3proxy.service << 'EOF'
[Unit]
Description=QwertyStock VPN 3proxy
After=network.target

[Service]
Type=forking
PIDFile=/var/run/3proxy.pid
ExecStart=/usr/local/bin/3proxy /etc/3proxy/3proxy.cfg
ExecReload=/bin/kill -HUP $MAINPID
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable qwerty-3proxy > /dev/null 2>&1
systemctl start qwerty-3proxy
echo "    Done: service started"

# 6. Verify
echo "[6/6] Verifying..."
sleep 1

PUBLIC_IP=$(curl -s ifconfig.me || curl -s icanhazip.com || echo "UNKNOWN")
COUNTRY=$(curl -s "https://ipinfo.io/${PUBLIC_IP}/country" 2>/dev/null || echo "??")

if systemctl is-active --quiet qwerty-3proxy; then
    echo "    3proxy is RUNNING"
else
    echo "    ERROR: 3proxy failed to start!"
    journalctl -u qwerty-3proxy --no-pager -n 10
    exit 1
fi

# Quick test
TEST=$(curl -s --proxy http://127.0.0.1:${PROXY_HTTP_PORT} --max-time 10 http://httpbin.org/ip 2>&1)
if echo "$TEST" | grep -q "origin"; then
    echo "    Proxy test: OK"
else
    echo "    WARNING: Proxy test failed (may need firewall rules)"
fi

echo ""
echo "============================================"
echo "  SETUP COMPLETE!"
echo "============================================"
echo ""
echo "  Server IP:    $PUBLIC_IP"
echo "  Country:      $COUNTRY"
echo "  HTTP Proxy:   $PUBLIC_IP:$PROXY_HTTP_PORT"
echo "  SOCKS5:       $PUBLIC_IP:$PROXY_SOCKS_PORT"
echo ""
echo "============================================"
echo "  NEXT STEP: Register this node on Gateway"
echo "============================================"
echo ""
echo "  Run this command to register:"
echo ""
echo "  curl -X POST -H 'Content-Type: application/json' \\"
echo "    -H 'X-API-Key: qvpn_master_key_2026' \\"
echo "    '${GATEWAY_URL}/api/add-vps' \\"
echo "    -d '{\"ip\": \"${PUBLIC_IP}\", \"country\": \"${COUNTRY}\", \"proxy_port\": ${PROXY_HTTP_PORT}, \"socks_port\": ${PROXY_SOCKS_PORT}}'"
echo ""
echo "  Or send me this info and I'll add manually:"
echo "  IP: $PUBLIC_IP | Country: $COUNTRY"
echo ""
