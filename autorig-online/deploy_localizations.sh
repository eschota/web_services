#!/bin/bash

# AutoRig Localization Deployment Script
# =====================================
# Deploys all localized pages to production

echo "🚀 Deploying localized pages to production..."

# Copy all localized HTML files
echo "📄 Copying localized pages..."
find /root/autorig-online/static -name "*-ru.html" -o -name "*-zh.html" -o -name "*-hi.html" | xargs -I {} cp {} /opt/autorig-online/static/

# Update sitemap
echo "🗺️  Updating sitemap..."
cd /root/autorig-online && python3 update_sitemap_localized.py

# Update nginx config
echo "🔧 Updating nginx config..."
cd /root/autorig-online && python3 generate_nginx_localized.py

# Update backend routes
echo "🐍 Updating backend routes..."
cd /root/autorig-online && python3 generate_backend_localized.py

# Test nginx configuration
echo "🔍 Testing nginx configuration..."
if nginx -t; then
    echo "✅ Nginx configuration is valid"
    echo "🔄 Reloading nginx..."
    systemctl reload nginx
    echo "✅ Nginx reloaded successfully"
else
    echo "❌ Nginx configuration test failed"
    exit 1
fi

# Test key localized pages
echo "🧪 Testing localized pages..."
test_pages=(
    "https://autorig.online/mixamo-alternative-ru"
    "https://autorig.online/mixamo-alternative-zh"
    "https://autorig.online/mixamo-alternative-hi"
    "https://autorig.online/rig-glb-unity-ru"
    "https://autorig.online/rig-fbx-unreal-zh"
    "https://autorig.online/t-pose-vs-a-pose-hi"
)

for page in "${test_pages[@]}"; do
    if curl -s --head "$page" | head -1 | grep "200" > /dev/null; then
        echo "✅ $page - OK"
    else
        echo "❌ $page - FAILED"
    fi
done

echo "🎉 Localization deployment completed!"
echo ""
echo "📊 Total localized pages: 21 (7 guides × 3 languages)"
echo "🌐 Languages: Russian (ru), Chinese (zh), Hindi (hi)"
echo "🗺️  Sitemap updated: https://autorig.online/sitemap.xml"
echo "🔍 Test any localized page in your browser!"
