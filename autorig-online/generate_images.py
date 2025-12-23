#!/usr/bin/env python3
"""
RenderFin Image Generator for AutoRig Online
Генерирует все необходимые изображения через RenderFin API
"""

import requests
import time
import os
import json
from pathlib import Path
from typing import Dict, Optional

# Конфигурация
API_URL = "https://renderfin.com/api-render"
BASE_DIR = Path(__file__).parent
IMAGES_DIR = BASE_DIR / "static" / "images"
USER_NAME = "autorig_online"

# Список изображений для генерации
IMAGES_TO_GENERATE = [
    {
        "name": "og-image.png",
        "path": IMAGES_DIR / "og-image.png",
        "prompt": "Professional Open Graph image for AutoRig Online. Dark background with gradient (indigo to purple). Center: 3D character wireframe or skeleton in T-pose. Text overlay: 'AutoRig Online - Automatic 3D Character Rigging'. Modern, clean design with subtle tech elements. Suitable for social media previews.",
        "aspect_ratio": 1200 / 630,  # 1.904
        "negative_prompt": "text, watermark, low quality, blurry"
    },
    {
        "name": "hero-main.jpg",
        "path": IMAGES_DIR / "hero" / "hero-main.jpg",
        "prompt": "Hero image for 3D character rigging service. Abstract 3D wireframe character in T-pose, floating in dark space with indigo/purple gradient lighting. Modern, tech-forward aesthetic. Subtle particles or grid pattern. Professional, clean composition. Space for text overlay on left side.",
        "aspect_ratio": 1920 / 1080,  # 1.777
        "negative_prompt": "text, watermark, low quality, blurry, cluttered"
    },
    # Gallery images
    {
        "name": "fantasy-warrior.jpg",
        "path": IMAGES_DIR / "gallery" / "fantasy-warrior.jpg",
        "prompt": "Fantasy warrior 3D character in T-pose. Armor, weapons, detailed design. Professional game-ready model. Clean background, studio lighting. High quality render.",
        "aspect_ratio": 1200 / 900,  # 1.333
        "negative_prompt": "low quality, blurry, watermark, text"
    },
    {
        "name": "cyberpunk-character.jpg",
        "path": IMAGES_DIR / "gallery" / "cyberpunk-character.jpg",
        "prompt": "Cyberpunk character 3D model. Mix of mechanical and organic elements. Futuristic design. Clean background, professional render.",
        "aspect_ratio": 1200 / 900,  # 1.333
        "negative_prompt": "low quality, blurry, watermark, text"
    },
    {
        "name": "animal-companion.jpg",
        "path": IMAGES_DIR / "gallery" / "animal-companion.jpg",
        "prompt": "Quadruped animal companion 3D model. Dog or wolf-like creature in T-pose. Professional game character. Clean background.",
        "aspect_ratio": 1200 / 900,  # 1.333
        "negative_prompt": "low quality, blurry, watermark, text"
    },
    {
        "name": "mecha-robot.jpg",
        "path": IMAGES_DIR / "gallery" / "mecha-robot.jpg",
        "prompt": "Mecha robot 3D character. Mechanical design with visible joints and armor. Professional game-ready model. Clean background.",
        "aspect_ratio": 1200 / 900,  # 1.333
        "negative_prompt": "low quality, blurry, watermark, text"
    },
    {
        "name": "cartoon-character.jpg",
        "path": IMAGES_DIR / "gallery" / "cartoon-character.jpg",
        "prompt": "Stylized cartoon 3D character. Exaggerated proportions, friendly appearance. Game-ready model. Clean background.",
        "aspect_ratio": 1200 / 900,  # 1.333
        "negative_prompt": "low quality, blurry, watermark, text"
    },
    {
        "name": "animation-showcase-thumb.jpg",
        "path": IMAGES_DIR / "gallery" / "animation-showcase-thumb.jpg",
        "prompt": "Thumbnail for animation showcase video. Multiple character poses in sequence showing walk cycle or action. Dynamic composition. Indigo/purple gradient background.",
        "aspect_ratio": 1200 / 900,  # 1.333
        "negative_prompt": "low quality, blurry, watermark, text"
    },
    # Process images for How It Works page
    {
        "name": "hero-overview.jpg",
        "path": IMAGES_DIR / "process" / "hero-overview.jpg",
        "prompt": "Abstract dark-themed illustration: 3D character silhouette in T-pose morphing into skeleton wireframe, then into geometric shapes representing outputs, with vertical frame for preview. Pure visual flow, no UI, no interface, minimal style, soft neon glow, dark background.",
        "aspect_ratio": 21 / 9,  # 2.333
        "negative_prompt": "text, words, letters, numbers, labels, buttons, cards, UI, interface, dashboard, panel, readable, watermark, logo, brand, cluttered, low quality, blurry"
    },
    {
        "name": "upload-ui.jpg",
        "path": IMAGES_DIR / "process" / "upload-ui.jpg",
        "prompt": "Abstract dark background with geometric shapes: large rounded rectangle with upload icon, smaller rectangle with link icon, circular button shape. Pure visual composition, no UI elements, no interface, minimal icons only, soft glow, dark theme.",
        "aspect_ratio": 4 / 3,  # 1.333
        "negative_prompt": "text, words, letters, numbers, labels, buttons, cards, UI, interface, dashboard, panel, readable, watermark, logo, brand, cluttered, low quality, blurry"
    },
    {
        "name": "analysis-visualization.jpg",
        "path": IMAGES_DIR / "process" / "analysis-visualization.jpg",
        "prompt": "Futuristic but clean visualization of a 3D humanoid mesh being analyzed: subtle wireframe overlay, highlighted joint landmarks at shoulders/elbows/hips/knees, soft scanning lines. Dark background, premium tech style, no readable text, no watermark.",
        "aspect_ratio": 16 / 9,  # 1.777
        "negative_prompt": "low quality, blurry, watermark, text, cluttered"
    },
    {
        "name": "skeleton-overlay.jpg",
        "path": IMAGES_DIR / "process" / "skeleton-overlay.jpg",
        "prompt": "3D character silhouette with a clean skeleton rig overlay (spine, arms, legs) in a modern dark UI style. Subtle glow on bones, clean and professional, no watermark, no text.",
        "aspect_ratio": 3 / 2,  # 1.5
        "negative_prompt": "low quality, blurry, watermark, text, cluttered"
    },
    {
        "name": "skinning-deformation.jpg",
        "path": IMAGES_DIR / "process" / "skinning-deformation.jpg",
        "prompt": "Split-screen illustration: left shows a character arm bending with visible deformation artifacts, right shows smooth deformation. Optionally include subtle weight heatmap colors on the mesh. Dark background, modern technical style, no watermark, no text.",
        "aspect_ratio": 16 / 9,  # 1.777
        "negative_prompt": "low quality, blurry, watermark, text, cluttered"
    },
    {
        "name": "animation-preview.jpg",
        "path": IMAGES_DIR / "process" / "animation-preview.jpg",
        "prompt": "Vertical smartphone-like preview frame of a rigged 3D character performing a walk cycle, with small thumbnail strips below showing idle/run/jump poses. Dark UI, premium look, subtle glow, no watermark, no brand text.",
        "aspect_ratio": 9 / 16,  # 0.5625
        "negative_prompt": "low quality, blurry, watermark, text, cluttered"
    },
    {
        "name": "downloads-panel.jpg",
        "path": IMAGES_DIR / "process" / "downloads-panel.jpg",
        "prompt": "Abstract dark background with four large geometric shapes in grid layout, each containing distinct file type icon and download arrow icon. Pure visual composition, no UI elements, no interface, minimal design, soft glow, dark theme.",
        "aspect_ratio": 16 / 9,  # 1.777
        "negative_prompt": "text, words, letters, numbers, labels, buttons, cards, UI, interface, dashboard, panel, readable, watermark, logo, brand, cluttered, low quality, blurry"
    },
    {
        "name": "three-pillars.jpg",
        "path": IMAGES_DIR / "process" / "three-pillars.jpg",
        "prompt": "Abstract dark background with three large icons arranged horizontally: analysis icon (magnifying glass or scan), construction icon (gears or build), skinning icon (mesh or weight). Pure visual composition, no UI elements, no interface, minimal design, soft neon glow, dark theme.",
        "aspect_ratio": 3 / 1,  # 3.0
        "negative_prompt": "text, words, letters, numbers, labels, buttons, cards, UI, interface, dashboard, panel, readable, watermark, logo, brand, cluttered, low quality, blurry"
    },
    {
        "name": "comparison-table.jpg",
        "path": IMAGES_DIR / "process" / "comparison-table.jpg",
        "prompt": "Abstract dark background with two vertical columns of icons. Left column: clock icon, dollar icon, person icon. Right column: speed icon, accessibility icon, consistency icon. Pure visual comparison, no UI elements, no interface, minimal icons only, dark theme.",
        "aspect_ratio": 16 / 9,  # 1.777
        "negative_prompt": "text, words, letters, numbers, labels, buttons, cards, UI, interface, dashboard, panel, readable, watermark, logo, brand, cluttered, low quality, blurry"
    },
    {
        "name": "t-pose-dont.jpg",
        "path": IMAGES_DIR / "process" / "t-pose-dont.jpg",
        "prompt": "Square split illustration: left shows correct T-pose humanoid silhouette with a check icon, right shows incorrect pose (arms angled down / asymmetry) with a cross icon. Dark background, clean minimal style, no watermark.",
        "aspect_ratio": 1 / 1,  # 1.0
        "negative_prompt": "low quality, blurry, watermark, text, cluttered"
    },
    # New images for improved page
    {
        "name": "pipeline-hero.jpg",
        "path": IMAGES_DIR / "process" / "pipeline-hero.jpg",
        "prompt": "Abstract dark-themed illustration: 3D character silhouette in T-pose morphing into skeleton wireframe, then transforming into geometric shapes representing file outputs, with a vertical frame showing animation preview. Pure visual flow, no UI elements, no interface, minimal premium style, soft neon glow, dark background.",
        "aspect_ratio": 21 / 9,  # 2.333
        "negative_prompt": "text, words, letters, numbers, labels, buttons, cards, UI, interface, dashboard, panel, readable, watermark, logo, brand, cluttered, low quality, blurry"
    },
    {
        "name": "progress-dashboard.jpg",
        "path": IMAGES_DIR / "process" / "progress-dashboard.jpg",
        "prompt": "Abstract dark background with horizontal progress bar gradient and geometric shapes representing file states. Some shapes glow bright (ready), others dim (pending). Pure visual representation, no UI elements, no interface, minimal icons only, dark theme, soft neon accents.",
        "aspect_ratio": 4 / 3,  # 1.333
        "negative_prompt": "text, words, letters, numbers, labels, buttons, cards, UI, interface, dashboard, panel, readable, watermark, logo, brand, cluttered, low quality, blurry"
    },
    {
        "name": "download-panel-new.jpg",
        "path": IMAGES_DIR / "process" / "download-panel-new.jpg",
        "prompt": "Abstract dark background with four large geometric shapes arranged in grid, each containing a distinct icon representing different file types. Minimal design, pure icons only, soft glow effects, no UI elements, no interface, dark theme.",
        "aspect_ratio": 16 / 9,  # 1.777
        "negative_prompt": "text, words, letters, numbers, labels, buttons, cards, UI, interface, dashboard, panel, readable, watermark, logo, brand, cluttered, low quality, blurry"
    },
    # Screenshot
    {
        "name": "screenshot.png",
        "path": IMAGES_DIR / "screenshot.png",
        "prompt": "Screenshot mockup of AutoRig Online website. Show upload interface with 3D character preview. Modern, clean UI design. Browser window frame. Professional appearance.",
        "aspect_ratio": 1920 / 1080,  # 1.777
        "negative_prompt": "low quality, blurry, watermark"
    },
]


def create_directories():
    """Создает необходимые директории"""
    directories = [
        IMAGES_DIR,
        IMAGES_DIR / "hero",
        IMAGES_DIR / "gallery",
        IMAGES_DIR / "process",
    ]
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
    print(f"✓ Директории созданы")


def generate_image(image_config: Dict) -> Optional[str]:
    """
    Генерирует одно изображение через RenderFin API
    Возвращает URL готового изображения или None при ошибке
    """
    print(f"\n🔄 Генерация: {image_config['name']}")
    print(f"   Aspect ratio: {image_config['aspect_ratio']:.3f}")
    
    # Подготовка запроса
    payload = {
        "prompt": image_config["prompt"],
        "aspect_ratio": image_config["aspect_ratio"],
        "user_name": USER_NAME
    }
    
    if "negative_prompt" in image_config:
        payload["negative_prompt"] = image_config["negative_prompt"]
    
    try:
        # Отправка запроса на генерацию
        print(f"   📤 Отправка запроса на генерацию...")
        response = requests.post(API_URL, json=payload, timeout=30)
        
        if response.status_code != 200:
            print(f"   ❌ Ошибка API: {response.status_code}")
            print(f"   Ответ: {response.text}")
            return None
        
        result = response.json()
        
        if "output_url" not in result:
            print(f"   ❌ Неожиданный ответ API: {result}")
            return None
        
        output_url = result["output_url"]
        print(f"   ✅ Получен output_url: {output_url}")
        print(f"   ⚠️  ВАЖНО: Файл еще не готов! Начинаем опрос каждые 15 секунд...")
        
        # Поллинг результата - опрашиваем output_url пока файл не будет готов
        return poll_image_url(output_url)
                
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Ошибка запроса: {e}")
        return None
    except Exception as e:
        print(f"   ❌ Неожиданная ошибка: {e}")
        return None


def poll_image_url(url: str, max_attempts: int = 40, delay: int = 15) -> Optional[str]:
    """
    Поллит URL изображения до готовности
    Опрашивает output_url каждые 15 секунд пока файл не будет готов (статус 200)
    """
    print(f"   ⏳ Ожидание готовности файла (макс. {max_attempts * delay // 60} минут)...")
    print(f"   🔄 Опрос каждые {delay} секунд...")
    
    for attempt in range(1, max_attempts + 1):
        try:
            # Проверяем доступность файла через HEAD запрос
            response = requests.head(url, timeout=10, allow_redirects=True)
            
            if response.status_code == 200:
                # Файл готов!
                elapsed_min = (attempt - 1) * delay // 60
                elapsed_sec = (attempt - 1) * delay % 60
                print(f"   ✅ Изображение готово! (попытка {attempt}, прошло ~{elapsed_min}м {elapsed_sec}с)")
                return url
            elif response.status_code == 404:
                # Файл еще не готов (404 - не найден)
                if attempt < max_attempts:
                    elapsed_min = attempt * delay // 60
                    elapsed_sec = attempt * delay % 60
                    print(f"   ⏳ Попытка {attempt}/{max_attempts}... файл еще не готов (404), ждем {delay} сек (прошло ~{elapsed_min}м {elapsed_sec}с)")
                    time.sleep(delay)
                else:
                    print(f"   ⚠️  Превышено время ожидания (файл все еще не готов после {max_attempts} попыток)")
                    return None
            else:
                # Неожиданный статус
                print(f"   ⚠️  Неожиданный статус {response.status_code}, продолжаем опрос...")
                if attempt < max_attempts:
                    time.sleep(delay)
                else:
                    return None
                
        except requests.exceptions.RequestException as e:
            # Если ошибка сети, продолжаем попытки
            if attempt < max_attempts:
                print(f"   ⏳ Попытка {attempt}/{max_attempts}... ошибка сети ({type(e).__name__}), ждем {delay} сек")
                time.sleep(delay)
            else:
                print(f"   ⚠️  Превышено время ожидания (ошибки сети: {e})")
                return None
    
    return None


def download_image(url: str, save_path: Path) -> bool:
    """
    Скачивает изображение по URL и сохраняет в файл
    """
    try:
        print(f"   📥 Скачивание изображения...")
        response = requests.get(url, timeout=60, stream=True)
        response.raise_for_status()
        
        # Сохраняем файл
        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        file_size = save_path.stat().st_size / 1024  # KB
        print(f"   ✅ Сохранено: {save_path} ({file_size:.1f} KB)")
        return True
        
    except Exception as e:
        print(f"   ❌ Ошибка скачивания: {e}")
        return False


def main():
    """Основная функция"""
    print("=" * 70)
    print("RenderFin Image Generator для AutoRig Online")
    print("=" * 70)
    
    # Создаем директории
    create_directories()
    
    # Статистика
    total = len(IMAGES_TO_GENERATE)
    success = 0
    failed = 0
    skipped = 0
    
    # Генерируем каждое изображение
    for idx, image_config in enumerate(IMAGES_TO_GENERATE, 1):
        print(f"\n[{idx}/{total}] {image_config['name']}")
        
        # Пропускаем если файл уже существует
        if image_config['path'].exists():
            print(f"   ⏭️  Файл уже существует, пропускаем")
            skipped += 1
            continue
        
        # Генерируем изображение
        image_url = generate_image(image_config)
        
        if image_url:
            # Скачиваем изображение
            if download_image(image_url, image_config['path']):
                success += 1
            else:
                failed += 1
                # Сохраняем URL для повторной попытки
                print(f"   💾 URL сохранен для повторной попытки: {image_url}")
        else:
            failed += 1
        
        # Пауза между запросами (чтобы не перегружать API)
        if idx < total:
            print(f"   ⏸️  Пауза 3 секунды перед следующим изображением...")
            time.sleep(3)
    
    # Итоговая статистика
    print("\n" + "=" * 70)
    print("ИТОГИ ГЕНЕРАЦИИ")
    print("=" * 70)
    print(f"Всего изображений: {total}")
    print(f"✅ Успешно: {success}")
    print(f"⏭️  Пропущено (уже существуют): {skipped}")
    print(f"❌ Ошибок: {failed}")
    print(f"⏱️  Примерное время: ~{total * 1} минута(ы)")
    print("=" * 70)
    
    if failed > 0:
        print("\n⚠️  Некоторые изображения не были сгенерированы.")
        print("   Проверьте логи выше и повторите попытку для неудачных.")
    else:
        print("\n🎉 Все изображения успешно сгенерированы!")


if __name__ == "__main__":
    main()
