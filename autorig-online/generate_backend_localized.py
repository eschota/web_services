#!/usr/bin/env python3
"""
Generate backend routes for all localized pages
"""

from pathlib import Path

def generate_backend_routes():
    """Generate FastAPI route functions for all localized pages"""

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

    routes = []

    # Base pages (English)
    for page in base_pages:
        routes.append(f"""@app.get("/{page}")
async def {page.replace('-', '_')}():
    \"\"\"Serve {page} page\"\"\"
    return FileResponse(str(STATIC_DIR / "{page}.html"))""")

    # Localized pages
    for page in base_pages:
        for lang in languages:
            route_name = f"{page.replace('-', '_')}_{lang}"
            routes.append(f"""@app.get("/{page}-{lang}")
async def {route_name}():
    \"\"\"Serve {lang.upper()} {page} page\"\"\"
    return FileResponse(str(STATIC_DIR / "{page}-{lang}.html"))""")

    return '\n\n'.join(routes)

def update_backend_routes():
    """Update main.py with localized routes"""

    main_py_path = Path('/root/autorig-online/backend/main.py')

    # Read current main.py
    with open(main_py_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Find the location to insert new routes (after existing guide routes)
    insert_marker = '# Guide pages'

    # Generate new routes
    new_routes = generate_backend_routes()

    # Insert new routes
    updated_content = content.replace(insert_marker, f'{insert_marker}\n{new_routes}')

    # Write back
    with open(main_py_path, 'w', encoding='utf-8') as f:
        f.write(updated_content)

    print("Backend routes updated with localized pages!")

if __name__ == '__main__':
    update_backend_routes()
