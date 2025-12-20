#!/bin/bash

# AutoRig SEO Pages Deployment Script
# ==================================
# This script deploys SEO landing pages to production

echo "🚀 Deploying SEO pages to production..."

# Copy HTML files to production static directory
echo "📄 Copying HTML files..."
cp /root/autorig-online/static/glb-auto-rig.html /opt/autorig-online/static/
cp /root/autorig-online/static/fbx-auto-rig.html /opt/autorig-online/static/
cp /root/autorig-online/static/obj-auto-rig.html /opt/autorig-online/static/
cp /root/autorig-online/static/t-pose-rig.html /opt/autorig-online/static/
cp /root/autorig-online/static/how-it-works.html /opt/autorig-online/static/
cp /root/autorig-online/static/faq.html /opt/autorig-online/static/
cp /root/autorig-online/static/gallery.html /opt/autorig-online/static/
cp /root/autorig-online/static/g-template.html /opt/autorig-online/static/

# Update sitemap
echo "🗺️  Updating sitemap..."
cp /root/autorig-online/static/sitemap.xml /opt/autorig-online/static/

# Test nginx configuration
echo "🔧 Testing nginx configuration..."
if nginx -t; then
    echo "✅ Nginx configuration is valid"
    echo "🔄 Reloading nginx..."
    systemctl reload nginx
    echo "✅ Nginx reloaded successfully"
else
    echo "❌ Nginx configuration test failed"
    exit 1
fi

# Test key pages
echo "🧪 Testing page accessibility..."
pages=(
    "https://autorig.online/glb-auto-rig"
    "https://autorig.online/fbx-auto-rig"
    "https://autorig.online/faq"
    "https://autorig.online/gallery"
    "https://autorig.online/g/test"
)

for page in "${pages[@]}"; do
    if curl -s --head "$page" | head -1 | grep "200" > /dev/null; then
        echo "✅ $page - OK"
    else
        echo "❌ $page - FAILED"
    fi
done

echo "🎉 SEO pages deployment completed!"
echo ""
echo "📊 Sitemap updated: https://autorig.online/sitemap.xml"
echo "🔍 Test the pages in your browser to ensure they're working correctly."
