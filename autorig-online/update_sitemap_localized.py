#!/usr/bin/env python3
"""
Update sitemap.xml with all localized pages
"""

import os
from pathlib import Path
from datetime import datetime

def generate_sitemap_entries():
    """Generate sitemap entries for all localized pages"""

    base_pages = [
        'mixamo-alternative',
        'rig-glb-unity',
        'rig-fbx-unreal',
        't-pose-vs-a-pose',
        'glb-vs-fbx',
        'auto-rig-obj',
        'animation-retargeting'
    ]

    languages = ['ru', 'zh', 'hi']  # Skip 'en' as it's the base

    entries = []

    # Base pages (English)
    for page in base_pages:
        entries.append(f"""    <url>
        <loc>https://autorig.online/{page}</loc>
        <lastmod>2024-12-20</lastmod>
        <changefreq>monthly</changefreq>
        <priority>0.7</priority>
    </url>""")

    # Localized pages
    for page in base_pages:
        for lang in languages:
            entries.append(f"""    <url>
        <loc>https://autorig.online/{page}-{lang}</loc>
        <lastmod>2024-12-20</lastmod>
        <changefreq>monthly</changefreq>
        <priority>0.7</priority>
    </url>""")

    return '\n'.join(entries)

def update_sitemap():
    """Update the sitemap.xml file"""

    sitemap_path = Path('/opt/autorig-online/static/sitemap.xml')

    # Read current sitemap
    with open(sitemap_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Find the location to insert new entries (after the existing guide entries)
    insert_marker = '    <!-- Guide Pages -->'

    # Generate new entries
    new_entries = generate_sitemap_entries()

    # Insert new entries
    updated_content = content.replace(insert_marker, f'{insert_marker}\n{new_entries}')

    # Write back
    with open(sitemap_path, 'w', encoding='utf-8') as f:
        f.write(updated_content)

    print("Sitemap updated with localized pages!")

if __name__ == '__main__':
    update_sitemap()
