"""SEO · social previews and indexing rules (2026-10-11).

One place for what crawlers and link previews (Telegram, Facebook, X, Slack,
Discord) see on autorig.online:

* ``task_head``: the <head> of /task?id=… — robots, title, description,
  canonical, Open Graph, Twitter card and JSON-LD.  Only a finished, public,
  titled task is indexable; a failed, unfinished, untitled or adult task is
  ``noindex`` and never gets a status emoji in its title.
* ``complete_social_head``: fills the Open Graph / Twitter tags that a static
  page left out (og:url = canonical, site name, locale and its alternates,
  image size and alt, ``summary_large_image``) and swaps the old default
  picture, whose AI-drawn caption was misspelled, for the site card.
* ``/og/task/<id>.jpg``: a 1200×630 card made from the task's V3 viewer capture
  (or its classic poster): the render, sharp, over a blurred copy of itself.
* ``/og/site.jpg``: a 1200×630 card of the newest safe V3 captures under the
  AutoRig logo, used by the home page, the gallery and every page without its
  own picture.
* ``gallery_cards_html``: the first gallery page as plain links, so the
  gallery passes crawlers to the task pages without running JS.

Cards are written once into ``AUTORIG_OG_CARD_DIR`` (outside the release) by
temp file + rename and served from there.
"""
from __future__ import annotations

import asyncio
import html as _html
import io
import json
import os
import re
import struct
import time
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from fastapi import Request  # module level: the route annotations are strings (postponed evaluation)

BASE_URL = (os.getenv("APP_URL") or "https://autorig.online").rstrip("/")
SITE_NAME = "AutoRig.online"
OG_W, OG_H = 1200, 630
OG_CARD_DIR = Path(os.getenv("AUTORIG_OG_CARD_DIR", "/srv/autorig/data/static/og-cards"))
V3_POSTER_DIR = Path(os.getenv("AUTORIG_V3_POSTER_DIR", "/srv/autorig/data/static/posters-v3"))
SITE_CARD_PATH = "/og/site.jpg"
# V3 preview clip (gallery_poster.py `clip`): 4 s turntable, 540x960 H.264, served at /og/task/<id>.mp4 through
# the nginx internal location /_autorig_previews_v3/ (range requests for players and crawlers).
V3_CLIP_DIR = Path(os.getenv("AUTORIG_V3_PREVIEW_VIDEO_DIR", "/srv/autorig/data/static/previews-v3"))
V3_CLIP_SIZE = (540, 960)
SITE_CARD_TTL_SEC = 6 * 3600
SITE_CARD_ALT = "Rigged 3D models made on AutoRig.online"
SITE_CARD_SUBJECTS = re.compile(
    r"\b(character|warrior|knight|soldier|girl|boy|woman|man|hero|heroic|robot|creature|adventurer|player|"
    r"cowboy|detective|mermaid|businesswoman|child|fighter|wizard|mage|elf|princess|monster|dragon|animal)\b", re.I)
SITE_CARD_SKIP = re.compile(r"\b(abstract|base mesh|generic|head|bust|minimalist)\b", re.I)
# The old default og:image: an AI picture whose caption reads «Automatily 3-D Charracter Rigigse».
LEGACY_DEFAULT_IMAGES = ("/static/images/og-image.png",)
LOCALES = {"en": "en_US", "ru": "ru_RU", "zh": "zh_CN", "hi": "hi_IN", "fa": "fa_IR"}
INDEX_ROBOTS = "index, follow, max-image-preview:large, max-video-preview:-1"
NOINDEX_ROBOTS = "noindex, follow"
HIDDEN_ROBOTS = "noindex, nofollow"
TITLE_MAX = 65
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
GENERIC_DESCRIPTION = ("Auto-rig GLB, FBX and OBJ characters and animals online with AutoRig: "
                       "skeleton, skinning and animations in about a minute.")


# Link-preview and search crawlers. A page render for them must be side-effect free (no re-rig, no task
# creation, no queue work): the coordinator's rule of 2026-10-11 for the «current-version rig» re-rig.
CRAWLER_UA = re.compile(
    r"bot\b|crawler|spider|slurp|facebookexternalhit|facebookcatalog|meta-externalagent|twitterbot|telegrambot|"
    r"whatsapp|slackbot|slack-imgproxy|discordbot|linkedinbot|pinterest|vkshare|skypeuripreview|embedly|"
    r"googlebot|google-inspectiontool|googleother|bingbot|bingpreview|yandex|baiduspider|duckduckbot|applebot|"
    r"petalbot|semrush|ahrefs|mj12bot|gptbot|oai-searchbot|chatgpt-user|claudebot|claude-user|perplexity|"
    r"ccbot|bytespider|headlesschrome|lighthouse|preview", re.I)


def is_crawler(user_agent: Any) -> bool:
    """True for search and link-preview crawlers (and headless renderers); empty UA counts as a crawler."""
    ua = str(user_agent or "").strip()
    return not ua or CRAWLER_UA.search(ua) is not None


# Guides published as one URL per language (<slug>, <slug>-ru, -zh, -hi). Their pages carry no hreflang of
# their own, so complete_social_head links the family (Google needs every variant to list all of them).
ARTICLE_FAMILIES = ("mixamo-alternative", "rig-glb-unity", "rig-fbx-unreal", "glb-vs-fbx", "t-pose-vs-a-pose",
                    "animation-retargeting", "face-rig-animation", "auto-rig-obj", "image-to-rigged-3d-character")
ARTICLE_LANGS = ("en", "ru", "zh", "hi")


def article_alternates(canonical: Optional[str]) -> List[Tuple[str, str]]:
    if not canonical or not canonical.startswith(BASE_URL + "/"):
        return []
    slug = canonical[len(BASE_URL) + 1:].split("?")[0].strip("/")
    base = re.sub(r"-(ru|zh|hi)$", "", slug)
    if base not in ARTICLE_FAMILIES:
        return []
    out = [(lang, f"{BASE_URL}/{base}" + ("" if lang == "en" else f"-{lang}")) for lang in ARTICLE_LANGS]
    return out + [("x-default", f"{BASE_URL}/{base}")]


def esc(value: Any) -> str:
    return _html.escape(str(value if value is not None else ""), quote=True)


def is_task_id(value: Any) -> bool:
    return bool(_UUID.match(str(value or "")))


# --------------------------------------------------------------------------- task pages

def _clip_words(text: str, limit: int) -> str:
    text = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(text) <= limit:
        return text
    cut = text[: limit - 1].rsplit(" ", 1)[0].rstrip(" ,.;:-—–")
    return (cut or text[: limit - 1]) + "…"


def _strip_status_marks(text: str) -> str:
    """No status emoji (❌ ✅ ⏳ …) ever reaches a title."""
    return re.sub(r"^[\W_]+(?=\w)", "", str(text or ""), flags=re.UNICODE).strip()


def task_index_decision(task: Any, hidden: bool) -> Tuple[bool, str]:
    """(indexable, reason). Indexable = done, public, not adult on this host, with real LLM metadata."""
    if hidden:
        return False, "hidden"
    if str(getattr(task, "status", "") or "") != "done":
        return False, f"status_{getattr(task, 'status', None)}"
    if getattr(task, "is_public", True) is False:
        return False, "private"
    if str(getattr(task, "content_rating", "") or "").lower() == "adult":
        return False, "adult"
    try:
        from seo_gallery import seo_passes_indexing_gate

        ok, issues = seo_passes_indexing_gate(task)
    except Exception as exc:  # noqa: BLE001 - a missing helper never makes a page indexable
        return False, f"gate_error_{type(exc).__name__}"
    return (True, "ok") if ok else (False, "thin_" + "_".join(issues[:2]))


def poster_signature(task_id: str) -> Optional[str]:
    """mtime stamp of the task's V3 capture, used to version its card URL."""
    if not is_task_id(task_id):
        return None
    try:
        st = (V3_POSTER_DIR / f"{task_id}.jpg").stat()
    except OSError:
        return None
    return f"{int(st.st_mtime):x}" if st.st_size > 8000 else None


MT_RUNS_DIR = Path(os.getenv("AUTORIG_MT_RUNS_DIR", "/srv/autorig/data/motion_transfer/runs"))
_MT_RUN_RE = re.compile(r"/api/mt/files/([0-9a-f]{12,40})/")
CATEGORY_NAMES = {"hands": "Hand", "adult_human": "Human character", "humanoid": "Humanoid character",
                  "quadruped": "Four-legged animal", "bird": "Bird", "fish": "Fish", "insect": "Insect",
                  "snake": "Snake", "vehicle": "Vehicle", "weapon": "Weapon", "prop": "Prop"}


def _v3_run_id(task: Any) -> Optional[str]:
    urls = [str(getattr(task, "viewer_prepared_glb_url", None) or "")]
    for attr in ("ready_urls", "output_urls"):
        try:
            urls += [str(u) for u in (getattr(task, attr, None) or [])]
        except TypeError:
            pass
    for url in urls:
        m = _MT_RUN_RE.search(url)
        if m:
            return m.group(1)
    return None


def task_label(task: Any) -> Optional[str]:
    """A human name for the task: LLM title, else the V3 run's Vision label, else its category."""
    title = _strip_status_marks(str(getattr(task, "poster_llm_title", None) or "").strip())
    if len(title) > 2:
        return title
    run = _v3_run_id(task)
    if not run:
        return None
    try:
        doc = json.loads((MT_RUNS_DIR / run / "analysis" / "category.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    what = re.sub(r"\s+", " ", str(doc.get("what") or "")).strip(" .")
    if 2 < len(what) <= 80 and re.fullmatch(r"[A-Za-z0-9 ,'()/&+-]+", what):
        return what[0].upper() + what[1:]
    category = str(doc.get("category") or "").strip().lower()
    return CATEGORY_NAMES.get(category) or (category.replace("_", " ").capitalize() if category and category != "other" else None)


def v3_clip_signature(task_id: str) -> Optional[str]:
    if not is_task_id(task_id):
        return None
    try:
        st = (V3_CLIP_DIR / f"{task_id}.mp4").stat()
    except OSError:
        return None
    return f"{int(st.st_mtime):x}" if st.st_size > 20000 else None


def v3_clip_url(task_id: str, base_url: str = "") -> Optional[str]:
    sig = v3_clip_signature(task_id)
    return f"{base_url}/og/task/{task_id}.mp4?v={sig}" if sig else None


_POSTER_IDS: Dict[str, Any] = {"at": -1.0, "ids": frozenset()}


def v3_poster_ids(ttl: float = 60.0) -> frozenset:
    """Task ids that have a V3 capture (posters-v3/<id>.jpg > 8 KB), cached ttl seconds. The gallery lists a V3
    task only once its poster exists, so a card is never blank."""
    now = time.monotonic()
    if _POSTER_IDS["at"] >= 0 and now - _POSTER_IDS["at"] < ttl:
        return _POSTER_IDS["ids"]
    ids = set()
    try:
        with os.scandir(V3_POSTER_DIR) as it:
            for entry in it:
                name = entry.name
                if name.endswith(".jpg") and is_task_id(name[:-4]):
                    try:
                        if entry.stat().st_size > 8000:
                            ids.add(name[:-4])
                    except OSError:
                        continue
    except OSError:
        pass
    _POSTER_IDS.update(at=now, ids=frozenset(ids))
    return _POSTER_IDS["ids"]


def task_card_url(task_id: str, signature: Optional[str]) -> str:
    return f"{BASE_URL}/og/task/{task_id}.jpg?v={signature or 'c'}.{TASK_CARD_VERSION}"


def site_card_url() -> str:
    return f"{BASE_URL}{SITE_CARD_PATH}"


def task_head(task: Any, *, hidden: bool, has_video: bool, has_poster: bool,
              poster_sig: Optional[str] = None, video_dims: Optional[Tuple[int, int]] = None,
              base_url: str = BASE_URL) -> Dict[str, Any]:
    """Everything /task?id=… puts in its head. Returns robots, title, h1 and the head HTML."""
    task_id = str(getattr(task, "id", "") or "")
    task_url = f"{base_url}/task?id={task_id}"
    indexable, reason = task_index_decision(task, hidden)
    status = str(getattr(task, "status", "") or "")
    keywords: List[str] = []
    long_desc = ""
    label = None if hidden else task_label(task)
    if hidden:
        name = "AutoRig task"
        description = GENERIC_DESCRIPTION
    else:
        # Every public task has its own name: the LLM title, else the V3 Vision label («Low-poly human
        # hand»), else the category. The generic wording is only for a task nothing is known about.
        name = label or ("Rigging a 3D model" if status in ("created", "processing", "queued") else "Rigged 3D model")
        description = (f"{label} — rigged on AutoRig: skeleton, skinning and animations, open it in the 3D viewer."
                       if label else GENERIC_DESCRIPTION)
        if status == "done":
            try:
                from seo_gallery import enrich_seo_metadata

                _t, seo_desc, seo_keywords, _semantic = enrich_seo_metadata(task)
                if (getattr(task, "poster_llm_description", None) or "").strip():
                    long_desc = seo_desc
                    description = seo_desc
                keywords = list(seo_keywords or [])
            except Exception as exc:  # noqa: BLE001
                print(f"[seo] enrich_seo_metadata failed for {task_id}: {type(exc).__name__}: {exc}")

    meta_description = _clip_words(description, 170)
    if indexable:
        suffix = " — rigged 3D model | AutoRig"
        title = (f"{name}{suffix}" if len(name) + len(suffix) <= TITLE_MAX
                 else f"{_clip_words(name, TITLE_MAX - len(' | AutoRig'))} | AutoRig")
        robots = INDEX_ROBOTS
    else:
        title = f"{_clip_words(name, TITLE_MAX - len(' | AutoRig'))} | AutoRig"
        robots = HIDDEN_ROBOTS if hidden else NOINDEX_ROBOTS
    og_title = _clip_words(name, 90)

    # The preview picture is independent of indexing: every public SFW task shows its own V3 render,
    # whatever its status (done, re-rigging, needs_review). Only a hidden (18+) or private task does not.
    show_media = not hidden and getattr(task, "is_public", True) is not False
    if show_media and (has_poster or poster_sig):
        image, image_w, image_h, image_alt = task_card_url(task_id, poster_sig), OG_W, OG_H, f"{og_title} — rigged 3D model"
    else:
        image, image_w, image_h, image_alt = site_card_url(), OG_W, OG_H, SITE_CARD_ALT
    thumb = f"{base_url}/thumb/{task_id}" + (f"?v={poster_sig}" if poster_sig else "")
    video = f"{base_url}/api/video/{task_id}" if (show_media and has_video and status == "done") else None
    clip = v3_clip_url(task_id, base_url) if show_media else None
    if clip:                       # the V3 turntable is the task's preview video (and replaces the V2 clip)
        video, video_dims = clip, V3_CLIP_SIZE

    lines: List[str] = [f'<meta name="description" content="{esc(meta_description)}">']
    if keywords and indexable:
        lines.append(f'<meta name="keywords" content="{esc(", ".join(keywords[:24]))}">')
    if indexable:
        lines.extend(_task_json_ld(task, task_url, og_title, long_desc or description, keywords,
                                   thumb if (has_poster or poster_sig) else image, video, base_url))
    lines.append(f'<link rel="canonical" href="{esc(task_url)}">')
    lines += [
        "<!-- Open Graph / Telegram / Social (seo_social.task_head) -->",
        f'<meta property="og:type" content="{"video.other" if video else "website"}">',
        f'<meta property="og:site_name" content="{SITE_NAME}">',
        f'<meta property="og:url" content="{esc(task_url)}">',
        f'<meta property="og:title" content="{esc(og_title)}">',
        f'<meta property="og:description" content="{esc(meta_description)}">',
        '<meta property="og:locale" content="en_US">',
        f'<meta property="og:image" content="{esc(image)}">',
        f'<meta property="og:image:secure_url" content="{esc(image)}">',
        '<meta property="og:image:type" content="image/jpeg">',
        f'<meta property="og:image:width" content="{image_w}">',
        f'<meta property="og:image:height" content="{image_h}">',
        f'<meta property="og:image:alt" content="{esc(image_alt)}">',
    ]
    if video:
        lines += [
            f'<meta property="og:video" content="{esc(video)}">',
            f'<meta property="og:video:secure_url" content="{esc(video)}">',
            '<meta property="og:video:type" content="video/mp4">',
        ]
        if video_dims:
            lines += [f'<meta property="og:video:width" content="{int(video_dims[0])}">',
                      f'<meta property="og:video:height" content="{int(video_dims[1])}">']
    # A Twitter "player" card needs an HTML player page; a raw MP4 there voids the whole card.
    lines += [
        '<meta name="twitter:card" content="summary_large_image">',
        f'<meta name="twitter:title" content="{esc(og_title)}">',
        f'<meta name="twitter:description" content="{esc(meta_description)}">',
        f'<meta name="twitter:image" content="{esc(image)}">',
        f'<meta name="twitter:image:alt" content="{esc(image_alt)}">',
    ]
    return {
        "indexable": indexable,
        "reason": reason,
        "robots": robots,
        "title": title,
        "h1": og_title,
        "head_html": "\n    ".join(lines),
    }


def _iso(value: Any) -> Optional[str]:
    try:
        return value.isoformat() if value is not None else None
    except Exception:  # noqa: BLE001
        return None


def _task_json_ld(task: Any, task_url: str, name: str, description: str, keywords: Sequence[str],
                  image: str, video: Optional[str], base_url: str) -> List[str]:
    created = _iso(getattr(task, "created_at", None))
    updated = _iso(getattr(task, "updated_at", None)) or created
    # The V3 shell reads the description and keywords of this CreativeWork back into the page text
    # (task_page_live.fill_v3_description), so it stays a top-level "CreativeWork".
    work: Dict[str, Any] = {
        "@context": "https://schema.org",
        "@type": "CreativeWork",
        "name": name[:200],
        "headline": name[:110],
        "description": str(description or "")[:2000],
        "url": task_url,
        "mainEntityOfPage": task_url,
        "image": image,
        "thumbnailUrl": image,
        "genre": "Rigged 3D model",
        "creator": {"@type": "Organization", "name": SITE_NAME, "url": base_url + "/"},
        "isFamilyFriendly": True,
        "inLanguage": "en",
    }
    if created:
        work["dateCreated"] = created
    if updated:
        work["dateModified"] = updated
    if keywords:
        work["keywords"] = ", ".join(list(keywords)[:24])
    if video:
        work["associatedMedia"] = {
            "@type": "VideoObject",
            "name": name[:200],
            "description": str(description or "")[:2000] or name,
            "thumbnailUrl": [image],
            "contentUrl": video,
            "uploadDate": updated or created,
        }
    crumbs = {
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": [
            {"@type": "ListItem", "position": 1, "name": "AutoRig", "item": base_url + "/"},
            {"@type": "ListItem", "position": 2, "name": "Gallery", "item": base_url + "/gallery"},
            {"@type": "ListItem", "position": 3, "name": name[:110], "item": task_url},
        ],
    }
    return [f'<script type="application/ld+json">{_json_ld_dump(work)}</script>',
            f'<script type="application/ld+json">{_json_ld_dump(crumbs)}</script>']


def _json_ld_dump(doc: Dict[str, Any]) -> str:
    # "</" inside a string would close the script element early.
    return json.dumps(doc, ensure_ascii=False).replace("</", "<\\/")


# --------------------------------------------------------------------------- static pages

_META_RE = r'<meta\b[^>]*\b(?:property|name)="{key}"[^>]*>'


def _meta_content(doc: str, key: str) -> Optional[str]:
    m = re.search(r'<meta\b[^>]*\b(?:property|name)="' + re.escape(key) + r'"[^>]*\bcontent="([^"]*)"', doc, re.I)
    if m:
        return m.group(1)
    m = re.search(r'<meta\b[^>]*\bcontent="([^"]*)"[^>]*\b(?:property|name)="' + re.escape(key) + r'"', doc, re.I)
    return m.group(1) if m else None


def _has_meta(doc: str, key: str) -> bool:
    return re.search(_META_RE.format(key=re.escape(key)), doc, re.I) is not None


def _set_meta_content(doc: str, key: str, value: str) -> str:
    pattern = re.compile(r'(<meta\b[^>]*\b(?:property|name)="' + re.escape(key) + r'"[^>]*\bcontent=")([^"]*)(")', re.I)
    return pattern.sub(lambda m: m.group(1) + esc(value) + m.group(3), doc, count=1)


def _head_end(doc: str) -> Optional[int]:
    m = re.search(r"</head\s*>", doc, re.I)
    return m.start() if m else None


def complete_social_head(doc: str) -> str:
    """Fill the social tags a static page left out. Never removes or rewrites a page's own choice,
    except og:url (follows the canonical) and the legacy misspelled default picture. Never raises."""
    try:
        return _complete_social_head(doc)
    except Exception as exc:  # noqa: BLE001 - a page is never lost over its preview tags
        print(f"[seo] complete_social_head skipped: {type(exc).__name__}: {exc}")
        return doc


def _complete_social_head(doc: str) -> str:
    if not isinstance(doc, str) or "<head" not in doc[:4000].lower():
        return doc
    end = _head_end(doc)
    if end is None:
        return doc
    head = doc[:end]
    if re.search(r'<meta\b[^>]*name="robots"[^>]*content="[^"]*noindex', head, re.I) and not _has_meta(head, "og:title"):
        return doc  # private tools (/dev, /admin, …) keep their bare head
    title_m = re.search(r"<title[^>]*>(.*?)</title>", head, re.I | re.S)
    title = _html.unescape(re.sub(r"\s+", " ", title_m.group(1)).strip()) if title_m else SITE_NAME
    desc = _meta_content(head, "description")
    desc = _html.unescape(desc) if desc else None
    canon_m = re.search(r'<link\b[^>]*\brel="canonical"[^>]*\bhref="([^"]+)"', head, re.I)
    canonical = _html.unescape(canon_m.group(1)) if canon_m else None
    lang_m = re.search(r"<html\b[^>]*\blang=\"([a-zA-Z-]+)\"", doc[:2000])
    lang = (lang_m.group(1).split("-")[0].lower() if lang_m else "en")

    # The legacy default picture → the site card (1200×630, real V3 captures, correct spelling).
    for legacy in LEGACY_DEFAULT_IMAGES:
        for key in ("og:image", "og:image:secure_url", "twitter:image"):
            value = _meta_content(head, key)
            if value and value.split("?")[0].endswith(legacy):
                head = _set_meta_content(head, key, site_card_url())
                for dim_key, dim in (("og:image:width", OG_W), ("og:image:height", OG_H)):
                    if _has_meta(head, dim_key):
                        head = _set_meta_content(head, dim_key, str(dim))
    if canonical and _has_meta(head, "og:url"):
        head = _set_meta_content(head, "og:url", canonical)

    og_title = _meta_content(head, "og:title")
    og_title = _html.unescape(og_title) if og_title else title
    og_desc = _meta_content(head, "og:description")
    og_desc = _html.unescape(og_desc) if og_desc else desc
    image = _meta_content(head, "og:image")
    add: List[str] = []

    def need(key: str, value: Optional[str], attr: str = "property") -> None:
        if value and not _has_meta(head, key):
            add.append(f'<meta {attr}="{key}" content="{esc(value)}">')

    need("og:title", og_title)
    need("og:description", og_desc)
    need("og:type", "website")
    need("og:site_name", SITE_NAME)
    need("og:url", canonical)
    if not image:
        image = site_card_url()
        need("og:image", image)
        need("og:image:width", str(OG_W))
        need("og:image:height", str(OG_H))
    elif image.split("?")[0].endswith(SITE_CARD_PATH):
        need("og:image:width", str(OG_W))
        need("og:image:height", str(OG_H))
    need("og:image:alt", og_title if not image.endswith(SITE_CARD_PATH) else SITE_CARD_ALT)
    locale = LOCALES.get(lang, "en_US")
    need("og:locale", locale)
    if not re.search(r'<link\b[^>]*\brel="alternate"[^>]*\bhreflang=', head, re.I):
        alts_links = article_alternates(canonical)
        if alts_links:
            add.extend(f'<link rel="alternate" hreflang="{hl}" href="{esc(url)}">' for hl, url in alts_links)
            head = head.rstrip() + "\n    " + "\n    ".join(add) + "\n"
            add = []
    if not re.search(r'property="og:locale:alternate"', head):
        alts = []
        for hl in re.findall(r'<link\b[^>]*\brel="alternate"[^>]*\bhreflang="([a-zA-Z-]+)"', head, re.I):
            loc = LOCALES.get(hl.split("-")[0].lower())
            current = _meta_content(head, "og:locale") or locale
            if loc and loc != current and loc not in alts:
                alts.append(loc)
        add.extend(f'<meta property="og:locale:alternate" content="{loc}">' for loc in alts)
    need("twitter:card", "summary_large_image", "name")
    need("twitter:title", og_title, "name")
    need("twitter:description", og_desc, "name")
    need("twitter:image", _meta_content(head, "og:image") or image, "name")
    if add:
        head = head.rstrip() + "\n    <!-- social tags completed by seo_social -->\n    " + "\n    ".join(add) + "\n"
    return head + doc[end:]


# --------------------------------------------------------------------------- media facts

def mp4_dimensions(path: Path) -> Optional[Tuple[int, int]]:
    """Width and height of the first video track of an MP4/MOV, read from the tkhd box with seeks only."""
    containers = {b"moov", b"trak", b"mdia", b"minf", b"stbl", b"edts"}

    def walk(fh, start: int, end: int, depth: int) -> Optional[Tuple[int, int]]:
        pos = start
        while pos + 8 <= end and depth < 6:
            fh.seek(pos)
            header = fh.read(8)
            if len(header) < 8:
                return None
            size, kind = struct.unpack(">I4s", header)
            hdr = 8
            if size == 1:
                size = struct.unpack(">Q", fh.read(8))[0]
                hdr = 16
            elif size == 0:
                size = end - pos
            if size < hdr:
                return None
            if kind == b"tkhd":
                body = fh.read(min(size - hdr, 120))
                version = body[0] if body else 0
                offset = 76 + (12 if version == 1 else 0)  # width/height are the last 8 bytes
                if len(body) >= offset + 8:
                    w, h = struct.unpack(">II", body[offset: offset + 8])
                    w, h = w >> 16, h >> 16
                    if w > 0 and h > 0:
                        return w, h
            elif kind in containers:
                found = walk(fh, pos + hdr, pos + size, depth + 1)
                if found:
                    return found
            pos += size
        return None

    try:
        with open(path, "rb") as fh:
            fh.seek(0, 2)
            return walk(fh, 0, fh.tell(), 0)
    except (OSError, struct.error, ValueError):
        return None


_VIDEO_DIMS: Dict[str, Tuple[float, Optional[Tuple[int, int]]]] = {}


def video_dimensions_cached(key: str, path: Optional[Path]) -> Optional[Tuple[int, int]]:
    hit = _VIDEO_DIMS.get(key)
    if hit and time.monotonic() - hit[0] < 3600:
        return hit[1]
    dims = mp4_dimensions(path) if path else None
    if len(_VIDEO_DIMS) > 5000:
        _VIDEO_DIMS.clear()
    _VIDEO_DIMS[key] = (time.monotonic(), dims)
    return dims


# --------------------------------------------------------------------------- cards

def _font(size: int):
    from PIL import ImageFont

    for name in ("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", "C:/Windows/Fonts/segoeuib.ttf",
                 "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
                 "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf"):
        if os.path.exists(name):
            return ImageFont.truetype(name, size)
    return ImageFont.load_default()


def _cover(img, w: int, h: int):
    from PIL import Image

    scale = max(w / img.width, h / img.height)
    resized = img.resize((max(1, round(img.width * scale)), max(1, round(img.height * scale))), Image.LANCZOS)
    left, top = (resized.width - w) // 2, (resized.height - h) // 2
    return resized.crop((left, top, left + w, top + h))


def _contain(img, w: int, h: int):
    from PIL import Image

    scale = min(w / img.width, h / img.height)
    return img.resize((max(1, round(img.width * scale)), max(1, round(img.height * scale))), Image.LANCZOS)


def _rounded(img, radius: int):
    from PIL import Image, ImageDraw

    mask = Image.new("L", img.size, 0)
    ImageDraw.Draw(mask).rounded_rectangle((0, 0, img.width - 1, img.height - 1), radius=radius, fill=255)
    out = img.convert("RGBA")
    out.putalpha(mask)
    return out


def _logo(logo_path: Optional[Path], height: int):
    from PIL import Image

    if not logo_path or not Path(logo_path).is_file():
        return None
    try:
        logo = Image.open(logo_path).convert("RGBA")
    except OSError:
        return None
    scale = height / logo.height
    return logo.resize((max(1, round(logo.width * scale)), height), Image.LANCZOS)


def _wrap_lines(draw, text: str, font, width: int, max_lines: int) -> List[str]:
    words, lines, cur = str(text or "").split(), [], ""
    for i, word in enumerate(words):
        trial = f"{cur} {word}".strip()
        if draw.textlength(trial, font=font) <= width:
            cur = trial
            continue
        if cur:
            lines.append(cur)
        cur = word
        if len(lines) == max_lines:
            lines[-1] = lines[-1].rstrip(" ,.;:-") + "…"
            return lines
    if cur:
        lines.append(cur)
    return lines[:max_lines]


TASK_CARD_VERSION = "3"  # part of the cached card's file name: bump when the layout changes


def compose_task_card(source: bytes, logo_path: Optional[Path] = None, title: Optional[str] = None) -> bytes:
    """1200×630: the render at full height on the left over a blurred copy of itself; the model's
    name, «Rigged 3D model» and the AutoRig mark on the right. A wide render fills the card instead."""
    from PIL import Image, ImageDraw, ImageEnhance, ImageFilter

    src = Image.open(io.BytesIO(source)).convert("RGB")
    bg = _cover(src, OG_W // 4, OG_H // 4).filter(ImageFilter.GaussianBlur(6)).resize((OG_W, OG_H), Image.BILINEAR)
    bg = ImageEnhance.Brightness(bg).enhance(0.42)
    card = bg.convert("RGBA")
    portrait = src.height > src.width * 1.1
    if portrait:
        fg = _contain(src, OG_W, OG_H - 36)
        x = 90
    else:
        fg = _contain(src, OG_W - 60, OG_H - 36)
        x = (OG_W - fg.width) // 2
    card.alpha_composite(_rounded(fg, 18), (x, (OG_H - fg.height) // 2))
    draw = ImageDraw.Draw(card)
    if portrait:
        left = x + fg.width + 56
        width = OG_W - left - 48
        name = _strip_status_marks(re.sub(r"\s+low-poly 3d model$", "", str(title or ""), flags=re.I)) \
            or "Rigged 3D model"
        font = _font(50)
        lines = _wrap_lines(draw, name, font, width, 4)
        y = max(60, (OG_H - (len(lines) * 62 + 110)) // 2)
        for line in lines:
            draw.text((left, y), line, font=font, fill=(255, 255, 255, 255))
            y += 62
        draw.text((left, y + 18), "Rigged 3D model", font=_font(30), fill=(150, 200, 255, 255))
        logo = _logo(logo_path, 58)
        lx = left
        if logo is not None:
            card.alpha_composite(logo, (left, OG_H - 58 - 40))
            lx += logo.width + 16
        draw.text((lx, OG_H - 58 - 40 + 12), "AutoRig.online", font=_font(30), fill=(220, 228, 245, 255))
    else:
        logo = _logo(logo_path, 64)
        if logo is not None:
            card.alpha_composite(logo, (OG_W - logo.width - 28, OG_H - logo.height - 22))
    out = io.BytesIO()
    card.convert("RGB").save(out, "JPEG", quality=86, optimize=True, progressive=True)
    return out.getvalue()


def compose_site_card(posters: Sequence[Path], logo_path: Optional[Path] = None,
                      tagline: str = "Auto-rig any 3D character in about a minute") -> bytes:
    """1200×630: the AutoRig logo and tagline over a row of real V3 viewer captures."""
    from PIL import Image, ImageDraw, ImageEnhance, ImageFilter

    card = Image.new("RGB", (OG_W, OG_H), (11, 14, 26))
    shots = []
    for path in posters:
        try:
            shots.append(Image.open(path).convert("RGB"))
        except OSError:
            continue
    if shots:
        strip = Image.new("RGB", (OG_W, OG_H))
        w = OG_W // len(shots) + 1
        for i, shot in enumerate(shots):
            strip.paste(_cover(shot, w, OG_H), (i * (OG_W // len(shots)), 0))
        bg = strip.resize((OG_W // 8, OG_H // 8), Image.BILINEAR).filter(ImageFilter.GaussianBlur(3))
        card = ImageEnhance.Brightness(bg.resize((OG_W, OG_H), Image.BILINEAR)).enhance(0.35)
    card = card.convert("RGBA")
    draw = ImageDraw.Draw(card)
    top = 150
    if shots:
        n = len(shots)
        gap = 14
        tile_w = min(260, (OG_W - 2 * 30 - gap * (n - 1)) // n)
        tile_h = min(OG_H - top - 26, round(tile_w * 16 / 9))
        x0 = (OG_W - (tile_w * n + gap * (n - 1))) // 2
        y0 = top + max(0, (OG_H - top - tile_h) // 2 - 6)
        for i, shot in enumerate(shots):
            tile = _rounded(_cover(shot, tile_w, tile_h), 14)
            card.alpha_composite(tile, (x0 + i * (tile_w + gap), y0))
    logo = _logo(logo_path, 96)
    x = 36
    if logo is not None:
        card.alpha_composite(logo, (x, 26))
        x += logo.width + 24
    title_font, tag_font = _font(46), _font(28)
    draw.text((x, 34), "AutoRig.online", font=title_font, fill=(255, 255, 255, 255))
    draw.text((x, 92), tagline, font=tag_font, fill=(196, 206, 230, 255))
    out = io.BytesIO()
    card.convert("RGB").save(out, "JPEG", quality=86, optimize=True, progressive=True)
    return out.getvalue()


def write_atomic(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{time.monotonic_ns()}.tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


# --------------------------------------------------------------------------- gallery

def gallery_cards_html(items: Iterable[Any], titles: Dict[str, str]) -> str:
    """Plain links for the first gallery page. The page's JS replaces them with live cards."""
    cards = []
    for it in items:
        task_id = str(getattr(it, "task_id", "") or "")
        if not is_task_id(task_id):
            continue
        thumb = getattr(it, "thumbnail_url", None) or f"/thumb/{task_id}"
        title = titles.get(task_id) or "Rigged 3D model"
        cards.append(
            f'<a href="/task?id={task_id}" class="gallery-card" data-task-id="{task_id}" style="text-decoration:none;">'
            f'<div class="gallery-card-media"><img src="{esc(thumb)}" loading="lazy" decoding="async" '
            f'alt="{esc(title)}" width="270" height="480"></div>'
            f'<div class="gallery-card-meta"><span class="gallery-time">{esc(_clip_words(title, 48))}</span></div></a>'
        )
    return "\n".join(cards)


# --------------------------------------------------------------------------- routes

def install(app: Any, *, get_db: Callable, resolve_poster_url_for_task: Callable[[Any], Optional[str]],
            static_dir: Path) -> None:
    """Register /og/site.jpg and /og/task/<id>.jpg on the app."""
    from fastapi import Depends
    from fastapi.responses import FileResponse, Response

    logo_path = static_dir / "images" / "logo" / "autorig-logo.png"
    site_lock = asyncio.Lock()

    async def _site_card_file(db) -> Optional[Path]:
        path = OG_CARD_DIR / "site.jpg"
        try:
            fresh = path.is_file() and time.time() - path.stat().st_mtime < SITE_CARD_TTL_SEC
        except OSError:
            fresh = False
        if fresh:
            return path
        async with site_lock:
            if path.is_file() and time.time() - path.stat().st_mtime < SITE_CARD_TTL_SEC:
                return path
            try:
                from sqlalchemy import select
                from database import Task

                rows = (await db.execute(
                    select(Task.id, Task.poster_llm_title).where(Task.status == "done", Task.is_public.is_(True),
                                                                 Task.content_rating == "safe")
                    .order_by(Task.created_at.desc()).limit(400)
                )).all()
                posters = []
                for tid, title in rows:
                    if not SITE_CARD_SUBJECTS.search(str(title or "")) or SITE_CARD_SKIP.search(str(title or "")):
                        continue  # characters only: no props, parts or untitled uploads on the site card
                    candidate = V3_POSTER_DIR / f"{tid}.jpg"
                    try:
                        if candidate.stat().st_size > 8000:
                            posters.append(candidate)
                    except OSError:
                        continue
                    if len(posters) >= 5:
                        break
                data = await asyncio.to_thread(compose_site_card, posters, logo_path)
                await asyncio.to_thread(write_atomic, path, data)
            except Exception as exc:  # noqa: BLE001 - keep serving the previous card
                print(f"[seo] site card refresh failed: {type(exc).__name__}: {exc}")
            return path if path.is_file() else None

    async def _task_source(task) -> Optional[bytes]:
        v3 = V3_POSTER_DIR / f"{task.id}.jpg"
        try:
            if v3.stat().st_size > 8000:
                return await asyncio.to_thread(v3.read_bytes)
        except OSError:
            pass
        poster_url = resolve_poster_url_for_task(task)
        if not poster_url:
            return None
        try:
            from artifact_cache import lookup_cached_artifact

            durable = lookup_cached_artifact(task.id, source_url=poster_url)
            if durable and durable.get("path"):
                return await asyncio.to_thread(Path(durable["path"]).read_bytes)
        except Exception as exc:  # noqa: BLE001
            print(f"[seo] poster cache lookup failed for {task.id}: {exc}")
        try:
            from worker_transport import worker_http_client

            async with worker_http_client() as client:
                resp = await client.get(poster_url, timeout=20.0, follow_redirects=True)
            if resp.status_code == 200 and len(resp.content) > 2000:
                return resp.content
        except Exception as exc:  # noqa: BLE001
            print(f"[seo] poster fetch failed for {task.id}: {type(exc).__name__}: {exc}")
        return None

    def _jpeg(path: Path, max_age: int) -> FileResponse:
        return FileResponse(path, media_type="image/jpeg", headers={
            "Cache-Control": f"public, max-age={max_age}", "Access-Control-Allow-Origin": "*"})

    @app.get("/og/site.jpg", include_in_schema=False)
    @app.head("/og/site.jpg", include_in_schema=False)
    async def og_site_card(db=Depends(get_db)):
        path = await _site_card_file(db)
        if path is None:
            return Response(status_code=404)
        return _jpeg(path, 3600)

    @app.get("/og/task/{task_id}.mp4", include_in_schema=False)
    @app.head("/og/task/{task_id}.mp4", include_in_schema=False)
    async def og_task_clip(task_id: str, request: Request, db=Depends(get_db)):
        if not is_task_id(task_id) or v3_clip_signature(task_id) is None:
            return Response(status_code=404)
        from tasks import get_task_by_id
        import site_mode

        task = await get_task_by_id(db, task_id)
        if (task is None or getattr(task, "is_public", True) is False or site_mode.hides(request, task)
                or str(getattr(task, "content_rating", "") or "").lower() == "adult"):
            return Response(status_code=404)
        return Response(status_code=200, media_type="video/mp4", headers={
            "X-Accel-Redirect": f"/_autorig_previews_v3/{task_id}.mp4",
            "Cache-Control": "public, max-age=604800" if request.query_params.get("v") else "public, max-age=600",
            "Access-Control-Allow-Origin": "*"})

    @app.get("/og/task/{task_id}.jpg", include_in_schema=False)
    @app.head("/og/task/{task_id}.jpg", include_in_schema=False)
    async def og_task_card(task_id: str, request: Request, db=Depends(get_db)):
        if not is_task_id(task_id):
            return Response(status_code=404)
        from tasks import get_task_by_id
        import site_mode

        task = await get_task_by_id(db, task_id)
        if task is None:
            return Response(status_code=404)
        if (getattr(task, "is_public", True) is False or site_mode.hides(request, task)
                or str(getattr(task, "content_rating", "") or "").lower() == "adult"):
            path = await _site_card_file(db)
            return _jpeg(path, 3600) if path else Response(status_code=404)
        sig = poster_signature(task_id) or "c"
        path = OG_CARD_DIR / "task" / f"{task_id}-{sig}-l{TASK_CARD_VERSION}.jpg"
        if not path.is_file():
            source = await _task_source(task)
            if source is None:
                site = await _site_card_file(db)
                return _jpeg(site, 600) if site else Response(status_code=404)
            try:
                data = await asyncio.to_thread(compose_task_card, source, logo_path, task_label(task))
            except Exception as exc:  # noqa: BLE001 - a broken poster falls back to the site card
                print(f"[seo] task card failed for {task_id}: {type(exc).__name__}: {exc}")
                site = await _site_card_file(db)
                return _jpeg(site, 600) if site else Response(status_code=404)
            await asyncio.to_thread(write_atomic, path, data)
        return _jpeg(path, 604800 if request.query_params.get("v") else 86400)
