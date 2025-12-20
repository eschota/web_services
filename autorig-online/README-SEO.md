# AutoRig SEO Landing Pages

## Overview
This document describes the SEO landing pages created for AutoRig Online to improve search engine visibility and user targeting.

## Pages Created

### 1. Format-Specific Landing Pages
- **`/glb-auto-rig`** - GLB format auto rigging (Web/VR optimized)
- **`/fbx-auto-rig`** - FBX format auto rigging (Unity/Unreal Engine)
- **`/obj-auto-rig`** - OBJ format auto rigging (Sculpting/Static models)
- **`/t-pose-rig`** - T-pose rigging specialization

### 2. Informational Pages
- **`/how-it-works`** - Step-by-step process explanation
- **`/faq`** - Frequently asked questions with Schema.org FAQ markup

### 3. Gallery & Examples
- **`/gallery`** - Showcase of rigging results
- **`/g/{id}`** - Individual result pages (e.g., `/g/fantasy-warrior`)

## SEO Features Implemented

### Schema.org Markup
- **Main Page**: SoftwareApplication schema
- **FAQ Page**: FAQPage schema with Q&A pairs
- **Gallery Pages**: VideoObject and ImageGallery schemas
- **All Pages**: Breadcrumb navigation

### Technical SEO
- ✅ Unique meta titles and descriptions for each page
- ✅ Open Graph tags for social sharing
- ✅ Proper heading hierarchy (H1-H3)
- ✅ Semantic HTML structure
- ✅ Fast loading (direct nginx serving)
- ✅ Mobile responsive design

### Sitemap & Indexing
- ✅ Updated sitemap.xml with all new pages
- ✅ Lastmod dates for content freshness
- ✅ Proper priority and change frequency settings
- ✅ Gallery result pages included

## Nginx Configuration
All SEO pages are served directly by nginx (not proxied to backend) for maximum performance:

```nginx
location = /glb-auto-rig {
    alias /opt/autorig-online/static/glb-auto-rig.html;
    add_header Content-Type "text/html";
}
```

## Backend Routes
Backend routes have been updated to serve the appropriate HTML files:

```python
@app.get("/glb-auto-rig")
async def glb_auto_rig():
    return FileResponse(str(STATIC_DIR / "glb-auto-rig.html"))
```

## Deployment
Use the deployment script for updates:

```bash
./deploy/deploy-seo-pages.sh
```

This script:
- Copies HTML files to production
- Updates sitemap
- Tests nginx configuration
- Verifies page accessibility

## Target Keywords

### Primary Keywords
- "auto rig glb online"
- "rig glb t-pose"
- "fbx auto rig unity"
- "fbx rigging unreal"
- "obj auto rig"
- "t pose rigging"

### Long-tail Keywords
- "automatic 3d character rigging"
- "ai powered rigging service"
- "professional rigging software"
- "game engine character setup"

## Content Strategy

### Page-Specific Content
Each landing page focuses on:
- Specific use case (GLB for web, FBX for games, etc.)
- Target audience (web devs, game devs, 3D artists)
- Unique value propositions
- Clear calls-to-action

### Gallery Strategy
- Real examples build trust
- Before/after comparisons show value
- Video content for engagement
- Indexable result pages for additional traffic

## Monitoring & Optimization

### Analytics Setup
- Google Analytics tracking on all pages
- Event tracking for CTA clicks
- Conversion funnel monitoring

### Performance Monitoring
- Page load speed tracking
- Core Web Vitals monitoring
- Search console indexing status

## Future Enhancements

### Dynamic Gallery
- Real user-submitted examples
- Automated thumbnail generation
- Video processing pipeline

### Advanced SEO
- Internationalization (i18n)
- AMP versions for mobile
- Structured data enhancements

### Content Marketing
- Blog posts about rigging techniques
- Tutorial videos
- Case studies

## Files Structure
```
/opt/autorig-online/static/
├── glb-auto-rig.html
├── fbx-auto-rig.html
├── obj-auto-rig.html
├── t-pose-rig.html
├── how-it-works.html
├── faq.html
├── gallery.html
├── g-template.html
└── sitemap.xml
```

## Contact
For questions about the SEO implementation, refer to the development team.
