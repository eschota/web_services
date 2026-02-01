"""
Metadata extraction using OpenAI Vision API.
Analyzes 3D model preview images to generate CGTrader listing metadata.
"""
import os
import base64
import json
import re
from typing import Dict, Any, Optional
from pathlib import Path

from openai import OpenAI

from config import OPENAI_API_KEY, CGTRADER_DEFAULT_PRICE

# Initialize OpenAI client
client = OpenAI(api_key=OPENAI_API_KEY)


def find_preview_image(extract_path: str) -> Optional[str]:
    """
    Find the preview image in extracted archive.
    Expected pattern: {uuid}_100k/{uuid}_Unity_HDRP_Render_1_view.jpg
    """
    extract_dir = Path(extract_path)
    
    # Look for *_Unity_HDRP_Render_1_view.jpg
    patterns = [
        "*/*_Unity_HDRP_Render_1_view.jpg",
        "*/*_Unity_HDRP_Render_1_view.png",
        "*/*/*_Unity_HDRP_Render_1_view.jpg",
        "*/*/*_Unity_HDRP_Render_1_view.png",
        "**/*_view.jpg",
        "**/*_view.png",
        "**/*.jpg",
        "**/*.png",
    ]
    
    for pattern in patterns:
        matches = list(extract_dir.glob(pattern))
        if matches:
            # Prefer the one with "Render" or "view" in name
            for match in matches:
                if "render" in match.name.lower() or "view" in match.name.lower():
                    return str(match)
            return str(matches[0])
    
    return None


def encode_image_base64(image_path: str) -> str:
    """Encode image to base64 for API."""
    with open(image_path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def get_image_mime_type(image_path: str) -> str:
    """Get MIME type from image extension."""
    ext = Path(image_path).suffix.lower()
    mime_types = {
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".gif": "image/gif",
        ".webp": "image/webp",
    }
    return mime_types.get(ext, "image/jpeg")


def extract_metadata_from_image(image_path: str) -> Dict[str, Any]:
    """
    Use GPT-4 Vision to analyze the image and extract metadata.
    Returns metadata suitable for CGTrader listing.
    """
    if not os.path.exists(image_path):
        raise FileNotFoundError(f"Image not found: {image_path}")
    
    # Encode image
    base64_image = encode_image_base64(image_path)
    mime_type = get_image_mime_type(image_path)
    
    # Prepare the prompt
    prompt = """Analyze this 3D model preview image and provide metadata for a CGTrader listing.

Return a JSON object with these fields:
{
    "title": "descriptive title for the 3D model (2-5 words, capitalize each word)",
    "description": "detailed description of the model, its features, and use cases (50-150 words)",
    "tags": ["tag1", "tag2", ...],  // 5-15 relevant tags for search
    "category": "one of: Character, Vehicle, Architecture, Aircraft, Animal, Plant, Furniture, Electronics, Weapon, Food, Sport, Various",
    "subcategory": "relevant subcategory based on category",
    "is_human": true/false,  // whether the model depicts a human/humanoid
    "is_rigged": false,  // assume not rigged unless clearly visible
    "has_textures": true,  // assume has textures based on rendered image
    "suggested_price": 25-99  // USD price based on complexity
}

Be specific and accurate. Focus on what you can clearly see in the image.
For characters: describe clothing, pose, style (realistic/stylized/cartoon).
For objects: describe type, style, detail level.

Return ONLY the JSON object, no additional text."""

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:{mime_type};base64,{base64_image}",
                                "detail": "high"
                            }
                        }
                    ]
                }
            ],
            max_tokens=1000,
            temperature=0.3,
        )
        
        # Parse response
        content = response.choices[0].message.content.strip()
        
        # Extract JSON from response (handle markdown code blocks)
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()
        
        metadata = json.loads(content)
        
        # Validate and set defaults
        metadata = validate_metadata(metadata)
        
        return metadata
        
    except json.JSONDecodeError as e:
        print(f"[Metadata] Failed to parse GPT response: {e}")
        print(f"[Metadata] Raw response: {content}")
        return get_default_metadata()
    except Exception as e:
        print(f"[Metadata] OpenAI API error: {e}")
        raise


def validate_metadata(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """Validate and fix metadata fields."""
    
    # Valid categories for CGTrader
    valid_categories = [
        "Character", "Vehicle", "Architecture", "Aircraft", 
        "Animal", "Plant", "Furniture", "Electronics",
        "Weapon", "Food", "Sport", "Various"
    ]
    
    # Ensure required fields
    if "title" not in metadata or not metadata["title"]:
        metadata["title"] = "3D Model"
    
    if "description" not in metadata or not metadata["description"]:
        metadata["description"] = "High-quality 3D model ready for use in games, visualization, and animation projects."
    
    if "tags" not in metadata or not isinstance(metadata["tags"], list):
        metadata["tags"] = ["3d-model", "game-ready", "pbr"]
    else:
        # Clean tags: lowercase, replace spaces with hyphens
        metadata["tags"] = [
            tag.lower().replace(" ", "-").replace("_", "-")
            for tag in metadata["tags"]
            if isinstance(tag, str) and len(tag) > 1
        ][:15]  # Max 15 tags
    
    if "category" not in metadata or metadata["category"] not in valid_categories:
        metadata["category"] = "Character"  # Default based on our use case
    
    if "subcategory" not in metadata:
        # Default subcategories per category
        default_subcategories = {
            "Character": "Man",
            "Vehicle": "Car",
            "Architecture": "Building",
            "Aircraft": "Airplane",
            "Animal": "Mammal",
            "Plant": "Tree",
            "Furniture": "Chair",
            "Electronics": "Computer",
            "Weapon": "Gun",
            "Food": "Fruit",
            "Sport": "Ball",
            "Various": "Other",
        }
        metadata["subcategory"] = default_subcategories.get(metadata["category"], "Other")
    
    # Price validation
    if "suggested_price" not in metadata:
        metadata["suggested_price"] = CGTRADER_DEFAULT_PRICE
    else:
        try:
            price = int(metadata["suggested_price"])
            metadata["suggested_price"] = max(5, min(999, price))
        except (ValueError, TypeError):
            metadata["suggested_price"] = CGTRADER_DEFAULT_PRICE
    
    # Boolean fields
    metadata["is_human"] = metadata.get("is_human", True)
    metadata["is_rigged"] = metadata.get("is_rigged", False)
    metadata["has_textures"] = metadata.get("has_textures", True)
    
    return metadata


def get_default_metadata() -> Dict[str, Any]:
    """Return default metadata when extraction fails."""
    return {
        "title": "3D Character Model",
        "description": (
            "High-quality 3D character model with detailed textures. "
            "Perfect for games, animations, and visualization projects. "
            "Includes multiple file formats for easy integration."
        ),
        "tags": [
            "character", "3d-model", "game-ready", "pbr", 
            "textured", "low-poly", "human", "man"
        ],
        "category": "Character",
        "subcategory": "Man",
        "is_human": True,
        "is_rigged": False,
        "has_textures": True,
        "suggested_price": CGTRADER_DEFAULT_PRICE,
    }


def extract_polygon_info(extract_path: str) -> Dict[str, int]:
    """
    Try to extract polygon/vertex count from filename or metadata files.
    Pattern: *_100k* suggests 100,000 polygons
    """
    extract_dir = Path(extract_path)
    
    # Default values
    info = {
        "polygons": 100000,
        "vertices": 100000,
    }
    
    # Check directory name for polygon hint
    dir_name = extract_dir.name
    
    # Match patterns like "100k", "50k", "200000"
    patterns = [
        (r"(\d+)k", lambda m: int(m.group(1)) * 1000),
        (r"(\d{4,})", lambda m: int(m.group(1))),
    ]
    
    for pattern, extractor in patterns:
        match = re.search(pattern, dir_name, re.IGNORECASE)
        if match:
            count = extractor(match)
            if 1000 <= count <= 10000000:
                info["polygons"] = count
                info["vertices"] = int(count * 1.1)  # Rough estimate
                break
    
    # Look for metadata files
    for meta_file in extract_dir.glob("**/*.json"):
        try:
            with open(meta_file) as f:
                data = json.load(f)
                if "polygons" in data:
                    info["polygons"] = int(data["polygons"])
                if "vertices" in data:
                    info["vertices"] = int(data["vertices"])
                break
        except:
            pass
    
    return info


def generate_full_metadata(extract_path: str) -> Dict[str, Any]:
    """
    Generate complete metadata for CGTrader listing.
    Combines image analysis with file information.
    """
    # Find preview image
    image_path = find_preview_image(extract_path)
    
    if image_path:
        print(f"[Metadata] Found preview image: {image_path}")
        metadata = extract_metadata_from_image(image_path)
        metadata["preview_image"] = image_path
    else:
        print("[Metadata] No preview image found, using defaults")
        metadata = get_default_metadata()
        metadata["preview_image"] = None
    
    # Add polygon info
    poly_info = extract_polygon_info(extract_path)
    metadata["polygons"] = poly_info["polygons"]
    metadata["vertices"] = poly_info["vertices"]
    
    # Add technical details
    metadata["geometry"] = "Polygon mesh"
    metadata["unwrapped_uvs"] = True
    metadata["non_overlapping"] = True
    metadata["ai_generated"] = False
    metadata["license"] = "Royalty free"
    
    return metadata


if __name__ == "__main__":
    # Test with a sample image
    import sys
    if len(sys.argv) > 1:
        result = generate_full_metadata(sys.argv[1])
        print(json.dumps(result, indent=2, ensure_ascii=False))
