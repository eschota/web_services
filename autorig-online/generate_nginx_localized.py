#!/usr/bin/env python3
"""
Generate nginx configuration for all localized routes
"""

from pathlib import Path

def generate_nginx_locations():
    """Generate nginx location blocks for all localized pages"""

    base_pages = [
        'mixamo-alternative',
        'rig-glb-unity',
        'rig-fbx-unreal',
        't-pose-vs-a-pose',
        'glb-vs-fbx',
        'auto-rig-obj',
        'animation-retargeting'
    ]

    languages = ['ru', 'zh', 'hi']

    locations = []

    # Base pages (English)
    for page in base_pages:
        locations.append(f"""    location = /{page} {{
        alias /opt/autorig-online/static/{page}.html;
        add_header Content-Type "text/html";
    }}""")

    # Localized pages
    for page in base_pages:
        for lang in languages:
            locations.append(f"""    location = /{page}-{lang} {{
        alias /opt/autorig-online/static/{page}-{lang}.html;
        add_header Content-Type "text/html";
    }}""")

    return '\n'.join(locations)

def update_nginx_config():
    """Update nginx config with localized routes"""

    config_path = Path('/etc/nginx/sites-available/autorig.online')

    # Read current config
    with open(config_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Find the location to insert new entries
    insert_marker = '    # Guide Pages'

    # Generate new locations
    new_locations = generate_nginx_locations()

    # Insert new locations
    updated_content = content.replace(insert_marker, f'{insert_marker}\n{new_locations}')

    # Write back
    with open(config_path, 'w', encoding='utf-8') as f:
        f.write(updated_content)

    print("Nginx config updated with localized routes!")

if __name__ == '__main__':
    update_nginx_config()
