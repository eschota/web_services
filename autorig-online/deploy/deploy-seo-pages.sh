#!/bin/bash

# AutoRig SEO Pages Deployment Script
# ==================================
# This script deploys SEO landing pages to production

echo "🚀 Deploying SEO pages to production..."

# Copy HTML files to production static directory
echo "📄 Copying HTML files..."
cp /root/autorig-online/static/glb-auto-rig.html /root/autorig-online/static/
cp /root/autorig-online/static/fbx-auto-rig.html /root/autorig-online/static/
cp /root/autorig-online/static/obj-auto-rig.html /root/autorig-online/static/
cp /root/autorig-online/static/t-pose-rig.html /root/autorig-online/static/
cp /root/autorig-online/static/t-pose-vs-a-pose.html /root/autorig-online/static/
cp /root/autorig-online/static/t-pose-vs-a-pose-ru.html /root/autorig-online/static/
cp /root/autorig-online/static/t-pose-vs-a-pose-zh.html /root/autorig-online/static/
cp /root/autorig-online/static/t-pose-vs-a-pose-hi.html /root/autorig-online/static/
cp /root/autorig-online/static/how-it-works.html /root/autorig-online/static/
cp /root/autorig-online/static/faq.html /root/autorig-online/static/
cp /root/autorig-online/static/gallery.html /root/autorig-online/static/
cp /root/autorig-online/static/g-template.html /root/autorig-online/static/
cp /root/autorig-online/static/rig-glb-unity.html /root/autorig-online/static/
cp /root/autorig-online/static/rig-glb-unity-ru.html /root/autorig-online/static/
cp /root/autorig-online/static/rig-glb-unity-zh.html /root/autorig-online/static/
cp /root/autorig-online/static/rig-glb-unity-hi.html /root/autorig-online/static/
cp /root/autorig-online/static/rig-fbx-unreal.html /root/autorig-online/static/
cp /root/autorig-online/static/rig-fbx-unreal-ru.html /root/autorig-online/static/
cp /root/autorig-online/static/rig-fbx-unreal-zh.html /root/autorig-online/static/
cp /root/autorig-online/static/rig-fbx-unreal-hi.html /root/autorig-online/static/
cp /root/autorig-online/static/animation-retargeting.html /root/autorig-online/static/
cp /root/autorig-online/static/animation-retargeting-ru.html /root/autorig-online/static/
cp /root/autorig-online/static/animation-retargeting-zh.html /root/autorig-online/static/
cp /root/autorig-online/static/animation-retargeting-hi.html /root/autorig-online/static/
cp /root/autorig-online/static/glb-vs-fbx.html /root/autorig-online/static/
cp /root/autorig-online/static/glb-vs-fbx-ru.html /root/autorig-online/static/
cp /root/autorig-online/static/glb-vs-fbx-zh.html /root/autorig-online/static/
cp /root/autorig-online/static/glb-vs-fbx-hi.html /root/autorig-online/static/

# Update sitemap
echo "🗺️  Updating sitemap..."
cp /root/autorig-online/static/sitemap.xml /root/autorig-online/static/

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
