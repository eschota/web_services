# Image Generation Guide

This document contains all the prompts needed to generate the remaining raster images for the AutoRig Online website.

## Priority Images (Generate First)

### 1. Open Graph Image
**File**: `og-image.png`  
**Location**: `/static/images/og-image.png`  
**Size**: 1200x630px  
**Format**: PNG/JPG  
**Prompt**: 
```
Professional Open Graph image for AutoRig Online. Dark background with gradient (indigo to purple). Center: 3D character wireframe or skeleton in T-pose. Text overlay: 'AutoRig Online - Automatic 3D Character Rigging'. Modern, clean design with subtle tech elements. Suitable for social media previews.
```

### 2. Hero Image
**File**: `hero-main.jpg`  
**Location**: `/static/images/hero/hero-main.jpg`  
**Size**: 1920x1080px (desktop), 1200x800px (main)  
**Format**: JPG/WebP  
**Prompt**: 
```
Hero image for 3D character rigging service. Abstract 3D wireframe character in T-pose, floating in dark space with indigo/purple gradient lighting. Modern, tech-forward aesthetic. Subtle particles or grid pattern. Professional, clean composition. Space for text overlay on left side.
```

## Gallery Images (6 images)

All gallery images should be **1200x900px (4:3 aspect ratio)** in **JPG/WebP** format.

### 1. Fantasy Warrior
**File**: `fantasy-warrior.jpg`  
**Location**: `/static/images/gallery/fantasy-warrior.jpg`  
**Prompt**: 
```
Fantasy warrior 3D character in T-pose. Armor, weapons, detailed design. Professional game-ready model. Clean background, studio lighting. High quality render.
```

### 2. Cyberpunk Character
**File**: `cyberpunk-character.jpg`  
**Location**: `/static/images/gallery/cyberpunk-character.jpg`  
**Prompt**: 
```
Cyberpunk character 3D model. Mix of mechanical and organic elements. Futuristic design. Clean background, professional render.
```

### 3. Animal Companion
**File**: `animal-companion.jpg`  
**Location**: `/static/images/gallery/animal-companion.jpg`  
**Prompt**: 
```
Quadruped animal companion 3D model. Dog or wolf-like creature in T-pose. Professional game character. Clean background.
```

### 4. Mecha Robot
**File**: `mecha-robot.jpg`  
**Location**: `/static/images/gallery/mecha-robot.jpg`  
**Prompt**: 
```
Mecha robot 3D character. Mechanical design with visible joints and armor. Professional game-ready model. Clean background.
```

### 5. Cartoon Character
**File**: `cartoon-character.jpg`  
**Location**: `/static/images/gallery/cartoon-character.jpg`  
**Prompt**: 
```
Stylized cartoon 3D character. Exaggerated proportions, friendly appearance. Game-ready model. Clean background.
```

### 6. Animation Showcase Thumbnail
**File**: `animation-showcase-thumb.jpg`  
**Location**: `/static/images/gallery/animation-showcase-thumb.jpg`  
**Prompt**: 
```
Thumbnail for animation showcase video. Multiple character poses in sequence showing walk cycle or action. Dynamic composition. Indigo/purple gradient background.
```

## Process Images (How It Works Page)

### 1. Rigging Process
**File**: `rigging-process.jpg`  
**Location**: `/static/images/process/rigging-process.jpg`  
**Size**: 1200x800px  
**Format**: PNG/JPG  
**Prompt**: 
```
Infographic-style illustration showing 3D character rigging process. Before/after comparison: static model on left, rigged skeleton on right. Arrows showing transformation. Clean, modern design with indigo/purple color scheme. Professional, educational appearance.
```

### 2. Comparison (Traditional vs Auto)
**File**: `comparison.jpg`  
**Location**: `/static/images/process/comparison.jpg`  
**Size**: 1200x600px  
**Format**: PNG/JPG  
**Prompt**: 
```
Split-screen comparison illustration. Left side: traditional manual rigging (complex, time-consuming). Right side: automated AI rigging (fast, efficient). Use icons and simple graphics. Modern infographic style.
```

### 3. Technology Visualization
**File**: `technology.jpg`  
**Location**: `/static/images/process/technology.jpg`  
**Size**: 1000x600px  
**Format**: PNG/JPG  
**Prompt**: 
```
Abstract visualization of AI/neural network processing 3D geometry. Wireframe mesh being analyzed, nodes and connections. Modern, tech-forward design. Indigo/purple color scheme. Professional, scientific appearance.
```

## Additional Images

### Screenshot (for Schema.org)
**File**: `screenshot.png`  
**Location**: `/static/images/screenshot.png`  
**Size**: 1920x1080px  
**Format**: PNG  
**Prompt**: 
```
Screenshot mockup of AutoRig Online website. Show upload interface with 3D character preview. Modern, clean UI design. Browser window frame. Professional appearance.
```

## Generation Tips

1. **Use AI image generators** like DALL-E, Midjourney, Stable Diffusion, or similar
2. **Maintain consistency** in color scheme (indigo #6366f1 to purple #a855f7)
3. **Optimize after generation**: Convert to WebP format, compress for web
4. **Create @2x versions** for retina displays where appropriate
5. **Test in both dark and light themes** to ensure visibility

## After Generation

1. Place images in the correct directories as specified
2. Optimize using tools like:
   - ImageMagick: `convert input.jpg -quality 85 -resize 1200x output.webp`
   - Squoosh.app (online tool)
   - TinyPNG (for PNG/JPG)
3. Update file paths in HTML if needed
4. Test loading performance

