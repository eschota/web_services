#!/usr/bin/env python3
"""Site refresh to V3 (2026-10-16): wire the seven new rig-type icons.

    py -3.11 tools/patch_rig_icons_v3.py --static <static dir> [--backend <backend dir>] [--dry-run] [--no-backup]

The icons themselves (static/Icons_png/{Bird,Serpent,Monster,Multilimb,Hands,Vehicle,Other}.png) are drawn by
tools/draw_rig_icons_v3.py. This script changes the code that keys them:

* static/js/rig-icons.js: RIG_ICON_ANIMAL_KEYS gets bird, serpent, monster, multilimb, hands, vehicle, other
  (resolveRigIconUrl then returns /static/Icons_png/<TitleCase>.png for them; app.js, task-card.js and gallery.html
  already build the same URL from the key without that list). The result is checked with `node --check`.
* with --backend (after tools/patch_gallery_v3_categories.py was applied): backend/gallery_categories.py maps every V3
  category to its own icon key (bird -> bird, ...), CATEGORIES_WITHOUT_ICON becomes empty, and
  backend/tests/test_gallery_v3_categories.py expects the new keys.

Every anchor is exact text, must occur exactly once, all results are compiled / syntax-checked before any file is
replaced, backups are <file>.pre_rig_icons_v3, a second run is a no-op.
"""
import argparse
import os
import py_compile
import shutil
import subprocess
import sys

JS = [
    ("RIG_ICON_ANIMAL_KEYS list",
     "    const RIG_ICON_ANIMAL_KEYS = [\n"
     "        'dog', 'bear', 'cat', 'cow', 'deer', 'elephant', 'giraffe',\n"
     "        'horse', 'mouse', 'pig', 'rabbit', 'turtle',\n"
     "    ];\n",
     "    const RIG_ICON_ANIMAL_KEYS = [\n"
     "        'dog', 'bear', 'cat', 'cow', 'deer', 'elephant', 'giraffe',\n"
     "        'horse', 'mouse', 'pig', 'rabbit', 'turtle',\n"
     "        // V3 rig categories (2026-10-16): Bird.png, Serpent.png, Monster.png, Multilimb.png, Hands.png, Vehicle.png, Other.png\n"
     "        'bird', 'serpent', 'monster', 'multilimb', 'hands', 'vehicle', 'other',\n"
     "    ];\n"),
]

GC = [
    ("icon map comment and table",
     "# rig_icon_key for the gallery cards: the existing icon (static/Icons_png/<Name>.png) that fits best. The categories\n"
     "# below have no icon of their own yet; they fall back to the Human icon (what rig-icons.js does for unknown keys).\n"
     "ICON_KEY_FOR_CATEGORY: Dict[str, str] = {\"humanoid\": \"humanoid\", \"quadruped\": \"dog\", \"equine\": \"horse\"}\n"
     "CATEGORIES_WITHOUT_ICON: Tuple[str, ...] = (\"bird\", \"serpent\", \"monster\", \"multilimb\", \"hands\", \"vehicle\", \"other\")\n",
     "# rig_icon_key for the gallery cards = the icon file static/Icons_png/<TitleCase key>.png. Quadruped and equine reuse the\n"
     "# Dog and Horse icons; every other category has its own icon since tools/draw_rig_icons_v3.py.\n"
     "ICON_KEY_FOR_CATEGORY: Dict[str, str] = {\"humanoid\": \"humanoid\", \"quadruped\": \"dog\", \"equine\": \"horse\", \"bird\": \"bird\",\n"
     "                                         \"serpent\": \"serpent\", \"monster\": \"monster\", \"multilimb\": \"multilimb\",\n"
     "                                         \"hands\": \"hands\", \"vehicle\": \"vehicle\", \"other\": \"other\"}\n"
     "CATEGORIES_WITHOUT_ICON: Tuple[str, ...] = ()\n"),
]

TEST = [
    ("icon test",
     "        for c in gc.CATEGORIES_WITHOUT_ICON:\n"
     "            self.assertEqual(icon(c), \"humanoid\")\n",
     "        for c in (\"bird\", \"serpent\", \"monster\", \"multilimb\", \"hands\", \"vehicle\", \"other\"):\n"
     "            self.assertEqual(icon(c), c)\n"
     "        self.assertEqual(gc.CATEGORIES_WITHOUT_ICON, ())\n"),
    ("icon function test",
     "        self.assertEqual(f(SimpleNamespace(v3_category=\"vehicle\", input_type=\"t_pose\", viewer_settings=None)), \"humanoid\")\n",
     "        self.assertEqual(f(SimpleNamespace(v3_category=\"vehicle\", input_type=\"t_pose\", viewer_settings=None)), \"vehicle\")\n"),
]


def read(path):
    raw = open(path, "rb").read().decode("utf-8")
    return raw.replace("\r\n", "\n"), "\r\n" in raw


def stage(path, anchors, staged):
    text, crlf = read(path)
    if all(new in text and old not in text for _, old, new in anchors):
        return "done"
    for name, old, new in anchors:
        n = text.count(old)
        if n != 1:
            print(f"FAIL {path} anchor {name!r}: found {n} times, expected exactly 1")
            return "fail"
        text = text.replace(old, new)
    staged[path] = text.replace("\n", "\r\n") if crlf else text
    return "ok"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--static", required=True)
    ap.add_argument("--backend")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-backup", action="store_true")
    args = ap.parse_args()
    staged = {}
    plan = [(os.path.join(args.static, "js", "rig-icons.js"), JS)]
    if args.backend:
        gc = os.path.join(args.backend, "gallery_categories.py")
        if not os.path.exists(gc):
            print("FAIL: backend/gallery_categories.py is missing, apply tools/patch_gallery_v3_categories.py first")
            return 1
        plan += [(gc, GC), (os.path.join(args.backend, "tests", "test_gallery_v3_categories.py"), TEST)]
    states = []
    for path, anchors in plan:
        state = stage(path, anchors, staged)
        if state == "fail":
            return 1
        states.append(state)
    if not staged:
        print("already patched, nothing to do")
        return 0
    tmps = []
    for path, text in staged.items():
        tmp = path + ".ri3.tmp" + os.path.splitext(path)[1]
        with open(tmp, "wb") as f:
            f.write(text.encode("utf-8"))
        tmps.append((path, tmp))
        if path.endswith(".py"):
            try:
                py_compile.compile(tmp, cfile=tmp + "c", doraise=True)
            except py_compile.PyCompileError as exc:
                print("FAIL py_compile:", exc)
                [os.remove(t) for _, t in tmps]
                return 1
            finally:
                if os.path.exists(tmp + "c"):
                    os.remove(tmp + "c")
        else:
            r = subprocess.run(["node", "--check", tmp], capture_output=True, text=True)
            if r.returncode != 0:
                print("FAIL node --check:", r.stderr.strip())
                [os.remove(t) for _, t in tmps]
                return 1
    if args.dry_run:
        [os.remove(t) for _, t in tmps]
        print(f"dry run ok: {len(tmps)} file(s) would change, syntax checks passed")
        return 0
    for path, tmp in tmps:
        if not args.no_backup:
            shutil.copy2(path, path + ".pre_rig_icons_v3")
        os.replace(tmp, path)
    print("patched: " + ", ".join(p for p, _ in tmps))
    return 0


if __name__ == "__main__":
    sys.exit(main())
