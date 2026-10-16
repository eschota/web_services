#!/usr/bin/env python3
"""One-off (2026-10-16): point the LoRA manifests of a character at the datasets store.

Every renderfin / scratch URL in <char>/{dataset,plates,pack}.json and <char>/sura/*.json whose picture still exists
locally (pics/, plates/, plates/poses/, ref/) or still answers is kept in the store (set lora/<char>/pics for takes,
lora/<char>/plates for plates, lora/sura-poses for the shared Sura pose library;
recovered copies from worker-4090's ComfyUI input cache in /srv/autorig/data/datasets_incoming/4090-input count too; the same bytes are found again by sha256) and the JSON gets the permanent URL; the
old URL stays beside it (`origin` next to `url`, `<key>_origin` next to any other key). Backups <file>.bak-datasets-
20261016. anime.json / anime_accepted.json are left alone (their service keeps them in memory and rewrites them).

    sudo -u autorig /srv/autorig/venv/bin/python3 migrate_lora_manifests.py sola lina kira [--dry-run]
"""
import json
import os
import pathlib
import shutil
import sys

sys.path.insert(0, "/srv/autorig/datasets")
import datasets_client as DS  # noqa: E402

DATA = pathlib.Path("/srv/autorig/data")
SCRATCH_URL = "https://autorig.online/dev/api/scratch/"
EPHEMERAL = ("https://autorig.online/renderfin/render/", SCRATCH_URL, "https://autorig.online/nsfw/api/scratch/")
DRY = "--dry-run" in sys.argv
BAK = ".bak-datasets-20261016"


RECOVERED = DATA / "datasets_incoming" / "4090-input"   # worker-4090 ComfyUI input cache copies (by original name)


def local_by_name(root):
    names = {}
    for folder in [root / sub for sub in ("plates", "ref", "pics", "anime")] + [RECOVERED]:
        for p in folder.rglob("*"):
            if p.is_file():
                names.setdefault(p.name, p)
    return names


def build_map(char, root):
    """old URL -> (local path or None, set, name)."""
    m = {}
    st = json.loads((root / "dataset.json").read_text()) if (root / "dataset.json").exists() else {}
    for key, it in (st.get("items") or {}).items():
        for t in it.get("takes", []):
            if t.get("url") and t.get("png") and pathlib.Path(t["png"]).exists():
                m[t["url"]] = (t["png"], f"lora/{char}/pics", pathlib.Path(t["png"]).stem,
                               {"character": char, "key": key, "seed": t.get("seed"), "uid": t.get("uid"),
                                "rating": t.get("rating"), "kind": "dataset take"})
    pl = json.loads((root / "plates.json").read_text()) if (root / "plates.json").exists() else {}
    for s in pl.get("sent") or []:
        p = root / "plates" / f"r{s.get('round', pl.get('round'))}_{s.get('family')}_{s.get('seed')}.png"
        if s.get("url") and p.exists():
            m[s["url"]] = (str(p), f"lora/{char}/plates", p.stem, {"character": char, "kind": "plate round",
                                                                    "seed": s.get("seed"), "rating": s.get("rating")})
    return m


def resolve(url, char, m, names, cache, set_name):
    if url in cache:
        return cache[url]
    new = None
    if url in m:
        path, sname, name, meta = m[url]
        new = DS.keep_url(sname, path=path, name=name, meta=dict(meta, origin=url), fallback="")
    else:
        base = url.rsplit("/", 1)[-1].split("?")[0]
        cand = names.get(base) or names.get(base.split(f"{char}_pose_", 1)[-1])
        if cand:
            new = DS.keep_url(set_name, path=str(cand), name=cand.stem,
                              meta={"character": char, "kind": "pinned / recovered", "origin": url,
                                    "source_path": str(cand)}, fallback="")
    if not new:                                    # still on the server (under 24 h old) or nowhere
        new = DS.keep_url(set_name, url=url, meta={"character": char, "origin": url}, fallback="", timeout=60)
    cache[url] = new or None
    return cache[url]


def walk(node, fn, stats):
    if isinstance(node, dict):
        for k in list(node):
            v = node[k]
            if isinstance(v, str) and v.startswith(EPHEMERAL):
                new = fn(v)
                if new:
                    node[k] = new
                    okey = "origin" if k == "url" else f"{k}_origin"
                    node.setdefault(okey, v)
                    stats["moved"] += 1
                else:
                    stats["lost"] += 1
            else:
                walk(v, fn, stats)
    elif isinstance(node, list):
        for i, v in enumerate(node):
            if isinstance(v, str) and v.startswith(EPHEMERAL):
                new = fn(v)
                if new:
                    node[i] = new
                    stats["moved"] += 1
                else:
                    stats["lost"] += 1
            else:
                walk(v, fn, stats)


def save(path, data):
    if not DRY:
        bak = path.with_name(path.name + BAK)
        if not bak.exists():
            shutil.copy2(path, bak)
        tmp = path.with_name("." + path.name + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
        os.replace(tmp, path)


def main():
    for char in [a for a in sys.argv[1:] if not a.startswith("--")]:
        root = DATA / char
        m, names, cache = build_map(char, root), local_by_name(root), {}
        files = [root / f for f in ("dataset.json", "plates.json", "pack.json")] + sorted((root / "sura").glob("*.json"))
        total = {"moved": 0, "lost": 0}
        for f in files:
            if not f.exists():
                continue
            data = json.loads(f.read_text(encoding="utf-8"))
            stats = {"moved": 0, "lost": 0}
            set_name = "lora/sura-poses" if f.parent.name == "sura" else f"lora/{char}/plates"
            walk(data, lambda u: resolve(u, char, m, names, cache, set_name), stats)
            if stats["moved"]:
                save(f, data)
            total = {k: total[k] + stats[k] for k in total}
            print(char, f.name, stats, flush=True)
        print(char, "TOTAL", total, "distinct urls", len(cache), "kept", sum(1 for v in cache.values() if v),
              flush=True)


if __name__ == "__main__":
    main()
