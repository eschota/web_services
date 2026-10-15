#!/usr/bin/env python3
"""Site refresh to V3 (2026-10-16): V3 rig categories in the public gallery.

    py -3.11 tools/patch_gallery_v3_categories.py <backend dir> [--dry-run] [--no-backup]

Patches backend/main.py, database.py, models.py and v3_runtime_mount.py and creates backend/gallery_categories.py:
  * tasks.v3_category / tasks.v3_subcategory (new nullable columns; migration lines for sqlite and PostgreSQL in
    init_db, run at the next start; an index on v3_category);
  * v3_runtime_mount.project_task stores the category from the run's analysis/category.json when the V3 run is
    terminal; `py -3.11 gallery_categories.py --backfill` fills the tasks finished before;
  * GALLERY_RIG_TYPES gets the V3 slugs (old animal slugs stay); /api/gallery?rig_type= filters in SQL when the
    column exists (exact total / has_more), keeps the Python filter otherwise;
  * GalleryItem.category; rig_icon_key of V3 tasks (quadruped -> dog, equine -> horse, others humanoid).

Every anchor is exact text and must occur exactly once in its file. All results are compiled (py_compile) before
any file is replaced; backups are <file>.pre_gallery_v3. A second run is a no-op.
"""
import argparse
import os
import py_compile
import shutil
import sys

MARKER_FILE = "gallery_categories.py"

MODULE_SOURCE = '"""V3 rig categories for the public gallery (site refresh to V3, 2026-10-16).\n\nBefore: every V3 task reported ``rig_icon_key = "humanoid"`` (the key only existed on the old V2 animal path) and\n``/api/gallery?rig_type=`` filtered in Python after loading every row. Now:\n\n* the V3 run writes ``<MT runs>/<run>/analysis/category.json`` (``category``, ``subcategory``). When a V3 task reaches\n  done / needs_review, ``v3_runtime_mount.project_task`` stores both on the task row (``tasks.v3_category``,\n  ``tasks.v3_subcategory``); ``backfill_v3_categories`` fills the tasks that were finished before;\n* the gallery filters in SQL on ``v3_category`` (``rig_filter_condition``), so ``total`` and ``has_more`` come from the\n  database and the page is not loaded in full;\n* the old V2 animal slugs (dog, horse, ...) stay accepted: they match the old V2 rows and the V3 rows of the mapped\n  category whose subcategory is one of that animal\'s names;\n* nothing reads files per request: ``category_for_task`` uses the column, with a one-line fallback to "humanoid".\n\nCLI (on the VPS, once, as the backend user):  py -3.11 gallery_categories.py --backfill [--dry-run] [--limit N]\n"""\nfrom __future__ import annotations\n\nimport json\nimport os\nimport re\nfrom pathlib import Path\nfrom typing import Any, Dict, Optional, Tuple\n\nMT_RUNS_DIR = Path(os.getenv("AUTORIG_MT_RUNS_DIR", "/srv/autorig/data/motion_transfer/runs"))\n_RUN_ID = re.compile(r"^[A-Za-z0-9_-]{8,128}$")\n\n# The gallery slugs of the V3 categories (= the values stored in tasks.v3_category, plus "other" which is stored\n# but not offered as a filter).\nV3_CATEGORIES: Tuple[str, ...] = ("humanoid", "quadruped", "equine", "bird", "serpent", "monster", "multilimb",\n                                  "hands", "vehicle")\nSTORED_CATEGORIES: Tuple[str, ...] = V3_CATEGORIES + ("other",)\n\n# Older analysis docs and the Vision labels use other words (seo_social.CATEGORY_NAMES knows some of them).\nCATEGORY_ALIASES: Dict[str, str] = {\n    "adult_human": "humanoid", "human": "humanoid", "biped": "humanoid", "primate": "humanoid",\n    "snake": "serpent", "insect": "multilimb", "spider": "multilimb", "hand": "hands",\n    "car": "vehicle", "fish": "other", "weapon": "other", "prop": "other",\n}\n\n# Old V2 animal presets -> the V3 category they belong to.\nLEGACY_ANIMAL_CATEGORY: Dict[str, str] = {\n    "dog": "quadruped", "bear": "quadruped", "cat": "quadruped", "cow": "quadruped", "elephant": "quadruped",\n    "giraffe": "quadruped", "mouse": "quadruped", "pig": "quadruped", "rabbit": "quadruped", "turtle": "quadruped",\n    "deer": "equine", "horse": "equine",\n}\n# Names a V3 subcategory may carry for each old slug (same words as the per-animal pages use to pick examples).\nLEGACY_ANIMAL_NAMES: Dict[str, Tuple[str, ...]] = {\n    "dog": ("dog", "dogs", "wolf", "wolves", "canine", "puppy", "fox"),\n    "bear": ("bear", "bears", "cub", "grizzly", "panda"),\n    "cat": ("cat", "cats", "kitten", "feline", "panther", "tiger", "lion"),\n    "cow": ("cow", "cows", "bull", "cattle", "calf", "ox"),\n    "deer": ("deer", "stag", "elk", "doe"),\n    "elephant": ("elephant", "elephants", "mammoth"),\n    "giraffe": ("giraffe", "giraffes"),\n    "horse": ("horse", "horses", "pony", "ponies", "equine", "unicorn"),\n    "mouse": ("mouse", "mice", "rat", "rats", "rodent", "hamster"),\n    "pig": ("pig", "pigs", "piglet", "boar", "hog", "swine"),\n    "rabbit": ("rabbit", "rabbits", "bunny", "bunnies", "hare"),\n    "turtle": ("turtle", "turtles", "tortoise", "tortoises"),\n}\n\n# rig_icon_key for the gallery cards: the existing icon (static/Icons_png/<Name>.png) that fits best. The categories\n# below have no icon of their own yet; they fall back to the Human icon (what rig-icons.js does for unknown keys).\nICON_KEY_FOR_CATEGORY: Dict[str, str] = {"humanoid": "humanoid", "quadruped": "dog", "equine": "horse"}\nCATEGORIES_WITHOUT_ICON: Tuple[str, ...] = ("bird", "serpent", "monster", "multilimb", "hands", "vehicle", "other")\n\n\ndef normalize_category(value: Any) -> Optional[str]:\n    """A category slug from a Vision / analysis value, or None when it says nothing usable."""\n    text = re.sub(r"[\\s-]+", "_", str(value or "").strip().lower())\n    if not text:\n        return None\n    text = CATEGORY_ALIASES.get(text, text)\n    return text if text in STORED_CATEGORIES else "other"\n\n\ndef normalize_subcategory(value: Any) -> Optional[str]:\n    text = re.sub(r"\\s+", " ", str(value or "").strip().lower())[:48]\n    return text or None\n\n\ndef category_from_doc(doc: Any) -> Tuple[Optional[str], Optional[str]]:\n    if not isinstance(doc, dict):\n        return None, None\n    return normalize_category(doc.get("category")), normalize_subcategory(doc.get("subcategory"))\n\n\ndef category_for_run(run_id: Any) -> Tuple[Optional[str], Optional[str]]:\n    """(category, subcategory) from the run\'s analysis/category.json; (None, None) when it is not there (yet)."""\n    run = str(run_id or "")\n    if not _RUN_ID.fullmatch(run):\n        return None, None\n    try:\n        doc = json.loads((MT_RUNS_DIR / run / "analysis" / "category.json").read_text(encoding="utf-8"))\n    except (OSError, ValueError):\n        return None, None\n    return category_from_doc(doc)\n\n\ndef _settings(task: Any) -> Dict[str, Any]:\n    try:\n        value = json.loads(getattr(task, "viewer_settings", None) or "{}")\n    except (TypeError, ValueError):\n        return {}\n    return value if isinstance(value, dict) else {}\n\n\ndef legacy_animal_slug(task: Any) -> Optional[str]:\n    """The old V2 animal preset of a task (only the V2 animal path sets it), else None."""\n    if str(getattr(task, "input_type", "") or "").strip().lower() != "animal":\n        return None\n    det = _settings(task).get("rig_v2_animal_detection")\n    if not isinstance(det, dict):\n        return None\n    slug = str(det.get("animal_type") or det.get("animal_type_string") or "").strip().lower()\n    return slug if slug in LEGACY_ANIMAL_CATEGORY else None\n\n\ndef category_for_task(task: Any) -> str:\n    """The V3 category shown on a gallery card; falls back to the old animal mapping, then to humanoid."""\n    stored = normalize_category(getattr(task, "v3_category", None))\n    if stored:\n        return stored\n    legacy = legacy_animal_slug(task)\n    return LEGACY_ANIMAL_CATEGORY[legacy] if legacy else "humanoid"\n\n\ndef icon_key_for_task(task: Any) -> str:\n    """rig_icon_key of a task that is not on the V2 animal path (every V3 task)."""\n    return ICON_KEY_FOR_CATEGORY.get(category_for_task(task), "humanoid")\n\n\ndef _legacy_animal_condition(Task: Any, slug: str) -> Any:\n    from sqlalchemy import and_, func, or_\n\n    vs = func.coalesce(Task.viewer_settings, "")\n    return and_(\n        func.coalesce(Task.input_type, "") == "animal",\n        or_(*[vs.like(f\'%"{key}": "{slug}"%\') for key in ("animal_type", "animal_type_string")],\n            *[vs.like(f\'%"{key}":"{slug}"%\') for key in ("animal_type", "animal_type_string")]),\n    )\n\n\ndef _any_legacy_animal_condition(Task: Any) -> Any:\n    from sqlalchemy import and_, func\n\n    return and_(func.coalesce(Task.input_type, "") == "animal",\n                func.coalesce(Task.viewer_settings, "").like("%rig_v2_animal_detection%"))\n\n\ndef rig_filter_condition(Task: Any, slug: str) -> Optional[Any]:\n    """SQL condition for ``/api/gallery?rig_type=<slug>``; None when the column does not exist yet (the caller then\n    keeps its Python filter). Slugs: a V3 category, or an old V2 animal preset."""\n    from sqlalchemy import and_, or_\n\n    col = getattr(Task, "v3_category", None)\n    sub = getattr(Task, "v3_subcategory", None)\n    if col is None or sub is None:\n        return None\n    if slug == "humanoid":\n        # stored humanoids, plus every row without a category that is not an old V2 animal (the fallback)\n        return or_(col == "humanoid", and_(col.is_(None), ~_any_legacy_animal_condition(Task)))\n    if slug in V3_CATEGORIES:\n        legacy = [_legacy_animal_condition(Task, s) for s, c in LEGACY_ANIMAL_CATEGORY.items() if c == slug]\n        return or_(col == slug, *legacy)\n    if slug in LEGACY_ANIMAL_CATEGORY:\n        names = [n.lower() for n in LEGACY_ANIMAL_NAMES.get(slug, (slug,))]\n        return or_(_legacy_animal_condition(Task, slug),\n                   and_(col == LEGACY_ANIMAL_CATEGORY[slug], sub.in_(names)))\n    return None\n\n\nasync def backfill_v3_categories(db: Any, limit: int = 1000, dry_run: bool = False) -> Dict[str, int]:\n    """Fill v3_category / v3_subcategory of finished V3 tasks that have none (run id from viewer_settings.v3)."""\n    from sqlalchemy import select\n\n    from database import Task\n\n    stats = {"seen": 0, "filled": 0, "no_run": 0, "no_file": 0}\n    rows = (await db.execute(\n        select(Task).where(Task.pipeline_kind == "v3", Task.v3_category.is_(None),\n                           Task.status.in_(("done", "needs_review"))).limit(limit))).scalars().all()\n    for task in rows:\n        stats["seen"] += 1\n        v3 = _settings(task).get("v3")\n        run = (v3 or {}).get("dispatch_run_id") if isinstance(v3, dict) else None\n        if not run:\n            stats["no_run"] += 1\n            continue\n        category, subcategory = category_for_run(run)\n        if not category:\n            stats["no_file"] += 1\n            continue\n        stats["filled"] += 1\n        if not dry_run:\n            task.v3_category, task.v3_subcategory = category, subcategory\n    if not dry_run:\n        await db.commit()\n    return stats\n\n\nasync def _cli(argv: list) -> int:\n    from database import AsyncSessionLocal\n\n    limit = int(argv[argv.index("--limit") + 1]) if "--limit" in argv else 1000\n    async with AsyncSessionLocal() as db:\n        print(await backfill_v3_categories(db, limit=limit, dry_run="--dry-run" in argv))\n    return 0\n\n\nif __name__ == "__main__":\n    import asyncio\n    import sys\n\n    if "--backfill" not in sys.argv:\n        print(__doc__)\n        sys.exit(2)\n    sys.exit(asyncio.run(_cli(sys.argv[1:])))\n'

PATCHES = {
    'main.py': [
        ('GALLERY_RIG_TYPES',
         'GALLERY_RIG_TYPES = (\n    "humanoid",\n    "dog",\n',
         '# V3 category slugs first (gallery_categories.V3_CATEGORIES), then the old V2 animal presets, which stay accepted.\nGALLERY_RIG_TYPES = (\n    "humanoid",\n    "quadruped",\n    "equine",\n    "bird",\n    "serpent",\n    "monster",\n    "multilimb",\n    "hands",\n    "vehicle",\n    "dog",\n'),
        ('rig_icon_key of a non-V2 task',
         '    input_type = str(getattr(task, "input_type", "") or "").strip().lower()\n    if input_type != "animal":\n        return "humanoid"\n',
         '    input_type = str(getattr(task, "input_type", "") or "").strip().lower()\n    if input_type != "animal":\n        import gallery_categories as _gallery_categories\n        return _gallery_categories.icon_key_for_task(task)  # V3: quadruped -> dog, equine -> horse, else humanoid\n'),
        ('filter in SQL',
         '    rig_filter = (rig_type or "all").strip().lower()\n    if rig_filter not in GALLERY_RIG_TYPES:\n        rig_filter = "all"\n    should_filter_rig = rig_filter != "all"\n',
         '    rig_filter = (rig_type or "all").strip().lower()\n    if rig_filter not in GALLERY_RIG_TYPES:\n        rig_filter = "all"\n    should_filter_rig = rig_filter != "all"\n    import gallery_categories as _gallery_categories\n    if should_filter_rig:\n        # V3 categories: filter in SQL on tasks.v3_category, so offset/limit, total and has_more are exact.\n        # (None = the column is not there yet: the Python filter below stays in charge.)\n        _rig_sql = _gallery_categories.rig_filter_condition(Task, rig_filter)\n        if _rig_sql is not None:\n            base_conditions.append(_rig_sql)\n            should_filter_rig = False\n'),
        ('GalleryItem.category',
         '            rig_icon_key=_gallery_rig_icon_key(t),\n        ))\n',
         '            rig_icon_key=_gallery_rig_icon_key(t),\n            category=_gallery_categories.category_for_task(t),\n        ))\n'),
    ],
    'database.py': [
        ('Task columns',
         '    viewer_animations_glb_url = Column(String(1024), nullable=True)\n',
         '    viewer_animations_glb_url = Column(String(1024), nullable=True)\n    # V3 rig category of the task (gallery filter): analysis/category.json of the V3 run, stored when the run\n    # finishes (v3_runtime_mount.project_task) or by gallery_categories.backfill_v3_categories.\n    v3_category = Column(String(16), nullable=True)\n    v3_subcategory = Column(String(48), nullable=True)\n'),
        ('sqlite migration',
         '            await _try_add_column("ALTER TABLE tasks ADD COLUMN viewer_animations_glb_url VARCHAR(1024)")\n',
         '            await _try_add_column("ALTER TABLE tasks ADD COLUMN viewer_animations_glb_url VARCHAR(1024)")\n            await _try_add_column("ALTER TABLE tasks ADD COLUMN v3_category VARCHAR(16)")\n            await _try_add_column("ALTER TABLE tasks ADD COLUMN v3_subcategory VARCHAR(48)")\n            await _try_add_column("CREATE INDEX IF NOT EXISTS ix_tasks_v3_category ON tasks (v3_category)")\n'),
        ('postgres migration',
         '            await _try_add_column_any(\n                "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS viewer_animations_glb_url VARCHAR(1024)"\n            )\n',
         '            await _try_add_column_any(\n                "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS viewer_animations_glb_url VARCHAR(1024)"\n            )\n            await _try_add_column_any("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS v3_category VARCHAR(16)")\n            await _try_add_column_any("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS v3_subcategory VARCHAR(48)")\n            await _try_add_column_any("CREATE INDEX IF NOT EXISTS ix_tasks_v3_category ON tasks (v3_category)")\n'),
    ],
    'models.py': [
        ('GalleryItem.category',
         '    rig_icon_key: str = "humanoid"',
         '    category: str = "humanoid"  # V3 rig category: humanoid | quadruped | equine | bird | serpent | monster | multilimb | hands | vehicle | other\n    rig_icon_key: str = "humanoid"'),
    ],
    'v3_runtime_mount.py': [
        ('store the category at completion',
         '    task.last_progress_at = now\n    task.updated_at = now\n',
         '    if patch.terminal:\n        # Gallery category (V3): analysis/category.json of this run; never fails the projection.\n        try:\n            import gallery_categories\n            _category, _subcategory = gallery_categories.category_for_run(record.run_id)\n            if _category:\n                task.v3_category, task.v3_subcategory = _category, _subcategory\n        except Exception as exc:                                 # noqa: BLE001\n            print(f"[V3] {binding.task_id} gallery category skipped: {type(exc).__name__}: {exc}")\n    task.last_progress_at = now\n    task.updated_at = now\n'),
    ],
}


def read(path):
    raw = open(path, "rb").read().decode("utf-8")
    return raw.replace("\r\n", "\n"), "\r\n" in raw


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("backend_dir")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-backup", action="store_true")
    args = ap.parse_args()
    d = args.backend_dir
    module_path = os.path.join(d, MARKER_FILE)
    if os.path.exists(module_path):
        current, _ = read(module_path)
        if current == MODULE_SOURCE:
            print("already patched, nothing to do")
            return 0
        print("FAIL: gallery_categories.py exists with different content")
        return 1
    staged = {}
    for fname, anchors in PATCHES.items():
        text, crlf = read(os.path.join(d, fname))
        for name, old, new in anchors:
            n = text.count(old)
            if n != 1:
                print(f"FAIL {fname} anchor {name!r}: found {n} times, expected exactly 1")
                return 1
            text = text.replace(old, new)
        staged[fname] = text.replace("\n", "\r\n") if crlf else text
    tmps = []
    try:
        for fname, text in list(staged.items()) + [(MARKER_FILE, MODULE_SOURCE)]:
            tmp = os.path.join(d, fname + ".gv3.tmp.py")
            with open(tmp, "wb") as f:
                f.write(text.encode("utf-8"))
            tmps.append((fname, tmp))
            py_compile.compile(tmp, cfile=tmp + "c", doraise=True)
            os.remove(tmp + "c")
    except py_compile.PyCompileError as exc:
        print("FAIL py_compile:", exc)
        for _, t in tmps:
            os.path.exists(t) and os.remove(t)
        return 1
    if args.dry_run:
        for _, t in tmps:
            os.remove(t)
        print(f"dry run ok: {sum(len(v) for v in PATCHES.values())} anchors in {len(PATCHES)} files, everything compiles")
        return 0
    for fname, t in tmps:
        target = os.path.join(d, fname)
        if os.path.exists(target) and not args.no_backup:
            shutil.copy2(target, target + ".pre_gallery_v3")
        os.replace(t, target)
    print(f"patched {len(PATCHES)} files and created {MARKER_FILE} in {d}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
