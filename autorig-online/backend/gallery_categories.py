"""V3 rig categories for the public gallery (site refresh to V3, 2026-10-16).

Before: every V3 task reported ``rig_icon_key = "humanoid"`` (the key only existed on the old V2 animal path) and
``/api/gallery?rig_type=`` filtered in Python after loading every row. Now:

* the V3 run writes ``<MT runs>/<run>/analysis/category.json`` (``category``, ``subcategory``). When a V3 task reaches
  done / needs_review, ``v3_runtime_mount.project_task`` stores both on the task row (``tasks.v3_category``,
  ``tasks.v3_subcategory``); ``backfill_v3_categories`` fills the tasks that were finished before;
* the gallery filters in SQL on ``v3_category`` (``rig_filter_condition``), so ``total`` and ``has_more`` come from the
  database and the page is not loaded in full;
* the old V2 animal slugs (dog, horse, ...) stay accepted: they match the old V2 rows and the V3 rows of the mapped
  category whose subcategory is one of that animal's names;
* nothing reads files per request: ``category_for_task`` uses the column, with a one-line fallback to "humanoid".

CLI (on the VPS, once, as the backend user):  py -3.11 gallery_categories.py --backfill [--dry-run] [--limit N]
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

MT_RUNS_DIR = Path(os.getenv("AUTORIG_MT_RUNS_DIR", "/srv/autorig/data/motion_transfer/runs"))
_RUN_ID = re.compile(r"^[A-Za-z0-9_-]{8,128}$")

# The gallery slugs of the V3 categories (= the values stored in tasks.v3_category, plus "other" which is stored
# but not offered as a filter).
V3_CATEGORIES: Tuple[str, ...] = ("humanoid", "quadruped", "equine", "bird", "serpent", "monster", "multilimb",
                                  "hands", "vehicle")
STORED_CATEGORIES: Tuple[str, ...] = V3_CATEGORIES + ("other",)

# Older analysis docs and the Vision labels use other words (seo_social.CATEGORY_NAMES knows some of them).
CATEGORY_ALIASES: Dict[str, str] = {
    "adult_human": "humanoid", "human": "humanoid", "biped": "humanoid", "primate": "humanoid",
    "snake": "serpent", "insect": "multilimb", "spider": "multilimb", "hand": "hands",
    "car": "vehicle", "fish": "other", "weapon": "other", "prop": "other",
    "anthro": "humanoid", "insect_multi_legged": "multilimb", "multi_legged": "multilimb", "arachnid": "multilimb",
}

# Old V2 animal presets -> the V3 category they belong to.
LEGACY_ANIMAL_CATEGORY: Dict[str, str] = {
    "dog": "quadruped", "bear": "quadruped", "cat": "quadruped", "cow": "quadruped", "elephant": "quadruped",
    "giraffe": "quadruped", "mouse": "quadruped", "pig": "quadruped", "rabbit": "quadruped", "turtle": "quadruped",
    "deer": "equine", "horse": "equine",
}
# Names a V3 subcategory may carry for each old slug (same words as the per-animal pages use to pick examples).
LEGACY_ANIMAL_NAMES: Dict[str, Tuple[str, ...]] = {
    "dog": ("dog", "dogs", "wolf", "wolves", "canine", "puppy", "fox"),
    "bear": ("bear", "bears", "cub", "grizzly", "panda"),
    "cat": ("cat", "cats", "kitten", "feline", "panther", "tiger", "lion"),
    "cow": ("cow", "cows", "bull", "cattle", "calf", "ox"),
    "deer": ("deer", "stag", "elk", "doe"),
    "elephant": ("elephant", "elephants", "mammoth"),
    "giraffe": ("giraffe", "giraffes"),
    "horse": ("horse", "horses", "pony", "ponies", "equine", "unicorn"),
    "mouse": ("mouse", "mice", "rat", "rats", "rodent", "hamster"),
    "pig": ("pig", "pigs", "piglet", "boar", "hog", "swine"),
    "rabbit": ("rabbit", "rabbits", "bunny", "bunnies", "hare"),
    "turtle": ("turtle", "turtles", "tortoise", "tortoises"),
}

# rig_icon_key for the gallery cards: the existing icon (static/Icons_png/<Name>.png) that fits best. The categories
# below have no icon of their own yet; they fall back to the Human icon (what rig-icons.js does for unknown keys).
ICON_KEY_FOR_CATEGORY: Dict[str, str] = {"humanoid": "humanoid", "quadruped": "dog", "equine": "horse", "bird": "bird",
                                         "serpent": "serpent", "monster": "monster", "multilimb": "multilimb",
                                         "hands": "hands", "vehicle": "vehicle", "other": "other"}
CATEGORIES_WITHOUT_ICON: Tuple[str, ...] = ()
# The subcategory words the analysis writes for a quadruped (registry v16): horses are quadruped / "equine".
QUADRUPED_SUBCATEGORY_FOR_SLUG: Dict[str, Tuple[str, ...]] = {
    "horse": ("equine",), "deer": ("cervine", "deer"), "dog": ("canine",), "cat": ("feline",), "cow": ("bovine",),
    "bear": ("ursine", "bear"), "pig": ("porcine", "pig"), "elephant": ("elephant",), "giraffe": ("giraffe",),
    "mouse": ("rodent",), "rabbit": ("lagomorph", "rabbit"), "turtle": ("turtle", "reptile"),
}


def normalize_category(value: Any) -> Optional[str]:
    """A category slug from a Vision / analysis value, or None when it says nothing usable."""
    text = re.sub(r"[\s-]+", "_", str(value or "").strip().lower())
    if not text:
        return None
    text = CATEGORY_ALIASES.get(text, text)
    return text if text in STORED_CATEGORIES else "other"


def normalize_subcategory(value: Any) -> Optional[str]:
    text = re.sub(r"\s+", " ", str(value or "").strip().lower())[:48]
    return text or None


def category_from_doc(doc: Any) -> Tuple[Optional[str], Optional[str]]:
    if not isinstance(doc, dict):
        return None, None
    return normalize_category(doc.get("category")), normalize_subcategory(doc.get("subcategory"))


def category_for_run(run_id: Any) -> Tuple[Optional[str], Optional[str]]:
    """(category, subcategory) from the run's analysis/category.json; (None, None) when it is not there (yet)."""
    run = str(run_id or "")
    if not _RUN_ID.fullmatch(run):
        return None, None
    try:
        doc = json.loads((MT_RUNS_DIR / run / "analysis" / "category.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None, None
    return category_from_doc(doc)


def mt_run_id_of_settings(settings: Dict[str, Any]) -> Optional[str]:
    """The MT run id (the runs/<id> directory) of a V3 task: viewer_settings.v3.session.mt_run_id."""
    v3 = settings.get("v3") if isinstance(settings, dict) else None
    session = (v3 or {}).get("session") if isinstance(v3, dict) else None
    run = str((session or {}).get("mt_run_id") or "") if isinstance(session, dict) else ""
    return run if _RUN_ID.fullmatch(run) else None


def _settings(task: Any) -> Dict[str, Any]:
    try:
        value = json.loads(getattr(task, "viewer_settings", None) or "{}")
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def legacy_animal_slug(task: Any) -> Optional[str]:
    """The old V2 animal preset of a task (only the V2 animal path sets it), else None."""
    if str(getattr(task, "input_type", "") or "").strip().lower() != "animal":
        return None
    det = _settings(task).get("rig_v2_animal_detection")
    if not isinstance(det, dict):
        return None
    slug = str(det.get("animal_type") or det.get("animal_type_string") or "").strip().lower()
    return slug if slug in LEGACY_ANIMAL_CATEGORY else None


def category_for_task(task: Any) -> str:
    """The V3 category shown on a gallery card; falls back to the old animal mapping, then to humanoid."""
    stored = normalize_category(getattr(task, "v3_category", None))
    if stored:
        return stored
    legacy = legacy_animal_slug(task)
    return LEGACY_ANIMAL_CATEGORY[legacy] if legacy else "humanoid"


def icon_key_for_task(task: Any) -> str:
    """rig_icon_key of a task that is not on the V2 animal path (every V3 task)."""
    return ICON_KEY_FOR_CATEGORY.get(category_for_task(task), "humanoid")


def _legacy_animal_condition(Task: Any, slug: str) -> Any:
    from sqlalchemy import and_, func, or_

    vs = func.coalesce(Task.viewer_settings, "")
    return and_(
        func.coalesce(Task.input_type, "") == "animal",
        or_(*[vs.like(f'%"{key}": "{slug}"%') for key in ("animal_type", "animal_type_string")],
            *[vs.like(f'%"{key}":"{slug}"%') for key in ("animal_type", "animal_type_string")]),
    )


def _any_legacy_animal_condition(Task: Any) -> Any:
    from sqlalchemy import and_, func

    return and_(func.coalesce(Task.input_type, "") == "animal",
                func.coalesce(Task.viewer_settings, "").like("%rig_v2_animal_detection%"))


def rig_filter_condition(Task: Any, slug: str) -> Optional[Any]:
    """SQL condition for ``/api/gallery?rig_type=<slug>``; None when the column does not exist yet (the caller then
    keeps its Python filter). Slugs: a V3 category, or an old V2 animal preset."""
    from sqlalchemy import and_, or_

    col = getattr(Task, "v3_category", None)
    sub = getattr(Task, "v3_subcategory", None)
    if col is None or sub is None:
        return None
    if slug == "humanoid":
        # stored humanoids, plus every row without a category that is not an old V2 animal (the fallback)
        return or_(col == "humanoid", and_(col.is_(None), ~_any_legacy_animal_condition(Task)))
    if slug in V3_CATEGORIES:
        legacy = [_legacy_animal_condition(Task, s) for s, c in LEGACY_ANIMAL_CATEGORY.items() if c == slug]
        if slug == "equine":                                   # horses are stored as quadruped / "equine"
            return or_(col == slug, and_(col == "quadruped", sub.in_(("equine",))), *legacy)
        return or_(col == slug, *legacy)
    if slug in LEGACY_ANIMAL_CATEGORY:
        names = [n.lower() for n in LEGACY_ANIMAL_NAMES.get(slug, (slug,))] + list(QUADRUPED_SUBCATEGORY_FOR_SLUG.get(slug, ()))
        return or_(_legacy_animal_condition(Task, slug),
                   and_(col.in_((LEGACY_ANIMAL_CATEGORY[slug], "quadruped")), sub.in_(names)))
    return None


async def backfill_v3_categories(db: Any, limit: int = 1000, dry_run: bool = False) -> Dict[str, int]:
    """Fill v3_category / v3_subcategory of finished V3 tasks that have none (run id from viewer_settings.v3)."""
    from sqlalchemy import select

    from database import Task

    stats = {"seen": 0, "filled": 0, "no_run": 0, "no_file": 0}
    rows = (await db.execute(
        select(Task).where(Task.pipeline_kind == "v3", Task.v3_category.is_(None),
                           Task.status.in_(("done", "needs_review"))).limit(limit))).scalars().all()
    for task in rows:
        stats["seen"] += 1
        v3 = _settings(task).get("v3")
        run = mt_run_id_of_settings(_settings(task)) or ((v3 or {}).get("dispatch_run_id") if isinstance(v3, dict) else None)
        if not run:
            stats["no_run"] += 1
            continue
        category, subcategory = category_for_run(run)
        if not category:
            stats["no_file"] += 1
            continue
        stats["filled"] += 1
        if not dry_run:
            task.v3_category, task.v3_subcategory = category, subcategory
    if not dry_run:
        await db.commit()
    return stats


async def _cli(argv: list) -> int:
    from database import AsyncSessionLocal

    limit = int(argv[argv.index("--limit") + 1]) if "--limit" in argv else 1000
    async with AsyncSessionLocal() as db:
        print(await backfill_v3_categories(db, limit=limit, dry_run="--dry-run" in argv))
    return 0


if __name__ == "__main__":
    import asyncio
    import sys

    if "--backfill" not in sys.argv:
        print(__doc__)
        sys.exit(2)
    sys.exit(asyncio.run(_cli(sys.argv[1:])))
