#!/usr/bin/env python3
"""
AutoRig Localization Generator
Generates localized versions of guide pages for all supported languages.
"""

import os
import json
from pathlib import Path

# Supported languages
LANGUAGES = {
    'en': {'name': 'English', 'code': 'en'},
    'ru': {'name': 'Русский', 'code': 'ru'},
    'zh': {'name': '中文', 'code': 'zh'},
    'hi': {'name': 'हिंदी', 'code': 'hi'}
}

# Guide pages to localize
GUIDES = [
    'mixamo-alternative',
    'rig-glb-unity',
    'rig-fbx-unreal',
    't-pose-vs-a-pose',
    'glb-vs-fbx',
    'auto-rig-obj',
    'animation-retargeting'
]

# Translation mappings (simplified - in real implementation, use proper translation service)
TRANSLATIONS = {
    'ru': {
        'Mixamo Alternative': 'Альтернатива Mixamo',
        'Auto Rigging': 'Автоматический риггинг',
        'GLB/FBX/OBJ': 'GLB/FBX/OBJ',
        'Unity/Unreal': 'Unity/Unreal',
        'Start Auto Rigging': 'Начать автоматический риггинг',
        'Related Guides': 'Связанные руководства',
        'Services': 'Сервисы',
        'Guides': 'Руководства',
        'Home': 'Главная',
        'How It Works': 'Как работает',
        'FAQ': 'FAQ',
        'Gallery': 'Галерея'
    },
    'zh': {
        'Mixamo Alternative': 'Mixamo替代方案',
        'Auto Rigging': '自动绑定',
        'GLB/FBX/OBJ': 'GLB/FBX/OBJ',
        'Unity/Unreal': 'Unity/Unreal',
        'Start Auto Rigging': '开始自动绑定',
        'Related Guides': '相关指南',
        'Services': '服务',
        'Guides': '指南',
        'Home': '首页',
        'How It Works': '如何工作',
        'FAQ': 'FAQ',
        'Gallery': '画廊'
    },
    'hi': {
        'Mixamo Alternative': 'Mixamo विकल्प',
        'Auto Rigging': 'ऑटो रिगिंग',
        'GLB/FBX/OBJ': 'GLB/FBX/OBJ',
        'Unity/Unreal': 'Unity/Unreal',
        'Start Auto Rigging': 'ऑटो रिगिंग शुरू करें',
        'Related Guides': 'संबंधित गाइड',
        'Services': 'सर्विसेज',
        'Guides': 'गाइड',
        'Home': 'होम',
        'How It Works': 'कैसे काम करता है',
        'FAQ': 'FAQ',
        'Gallery': 'गैलरी'
    }
}

def translate_text(text, lang):
    """Simple translation function - in production, use proper translation service"""
    if lang not in TRANSLATIONS:
        return text

    translations = TRANSLATIONS[lang]
    for key, value in translations.items():
        text = text.replace(key, value)

    return text

def generate_localized_page(template_path, output_path, lang_code, lang_name):
    """Generate a localized version of a page"""
    try:
        with open(template_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Basic translations
        content = translate_text(content, lang_code)

        # Update HTML lang attribute
        content = content.replace('<html lang="en">', f'<html lang="{lang_code}">')

        # Update URLs for localized pages
        if lang_code != 'en':
            content = content.replace('href="/', f'href="/')
            # Add language suffix to internal links
            for guide in GUIDES:
                if f'href="/{guide}"' in content and not f'href="/{guide}-{lang_code}"' in content:
                    content = content.replace(f'href="/{guide}"', f'href="/{guide}-{lang_code}"')

        # Update meta tags
        content = content.replace('content="en"', f'content="{lang_code}"')

        # Create output directory if needed
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(content)

        print(f"Generated: {output_path}")

    except Exception as e:
        print(f"Error generating {output_path}: {e}")

def main():
    """Generate all localized pages"""
    static_dir = Path('/root/autorig-online/static')

    for guide in GUIDES:
        template_file = static_dir / f'{guide}.html'

        if not template_file.exists():
            print(f"Template not found: {template_file}")
            continue

        for lang_code, lang_info in LANGUAGES.items():
            if lang_code == 'en':
                continue  # Skip English, use as template

            output_file = static_dir / f'{guide}-{lang_code}.html'
            generate_localized_page(template_file, output_file, lang_code, lang_info['name'])

    print("Localization generation complete!")

if __name__ == '__main__':
    main()
