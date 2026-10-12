"""multilimb: the child-limb label merge moves from trace_all into classify-time (merge_children: it needs the kinds):
a short twig or a child of an arm / wing / tail / leg joins its parent's region; a long child of a head appendage
(an arm whose path met a hanging ear) keeps its own label. An unpaired «wing» rooted high on the spine is a head
appendage. Appendage chains only when the monster branch (or appendages=on) asks."""
import pathlib
import sys

p = pathlib.Path(sys.argv[1]) / "multilimb.py"
s = p.read_text(encoding="utf-8")


def rep(s, old, new):
    assert s.count(old) == 1, (s.count(old), old[:100])
    return s.replace(old, new)


if "def merge_children" not in s:
    s = rep(s, '''    _, labels = _geodesic_labels(inside, dims, np.array(src), np.array(lab), depth, o["thin_cost"])
    for L in limbs:                                              # a child limb's cells belong to its parent chain
        par, seen = L["parent"], 0
        while par != "trunk" and seen < 8:
            top = limbs[par]
            par, seen = top["parent"], seen + 1
        if L["parent"] != "trunk":
            L["top"] = top["id"]
            labels[labels == L["id"] + 1] = top["id"] + 1
    for L in limbs:''', '''    _, labels = _geodesic_labels(inside, dims, np.array(src), np.array(lab), depth, o["thin_cost"])
    for L in limbs:                                              # the top-level ancestor of a child limb
        par, seen = L["parent"], 0
        top = None
        while par != "trunk" and seen < 8:
            top = limbs[par]
            par, seen = top["parent"], seen + 1
        if top is not None:
            L["top"] = top["id"]
    for L in limbs:''')
    s = rep(s, '''def classify(tr: dict, o: dict) -> dict:''', '''def merge_children(tr: dict, o: dict) -> int:
    """After classify: a child limb's cells join its top ancestor's region when the ancestor is a chain limb (arm,
    wing, tail, leg: a wing's second spar, a claw's second finger) or the child is a short twig; a long child of a
    head appendage (crest / hair / stub: an arm whose path met a hanging ear) keeps its own label. -> cells moved."""
    labels, H = tr["labels"], tr["H"]
    by_id = {L["id"]: L for L in tr["limbs"]}
    moved = 0
    for L in tr["limbs"]:
        if L.get("top") is None or L.get("merged"):
            continue
        top = by_id[L["top"]]
        if top.get("kind") in ("arm", "wing", "tail", "leg") or L["length"] < o["merge_child_pct_H"] / 100 * H:
            m = labels == L["id"] + 1
            moved += int(m.sum())
            labels[m] = top["id"] + 1
            L["merged"] = True
    for L in tr["limbs"]:
        L["cells_n"] = int((labels == L["id"] + 1).sum())
    return moved


def classify(tr: dict, o: dict) -> dict:''')
    s = rep(s, '''    "appendage_min_pct_H": {''', '''    "merge_child_pct_H": {"min": 0.0, "max": 60.0, "default": 25.0,
                          "doc": "a child branch shorter than this share of H joins its parent limb's region whatever the kind"},
    "wing_join_max": {"min": 0.2, "max": 1.0, "default": 0.45,
                      "doc": "an unpaired wing rooted above this share of the spine is a head appendage, not a wing"},
    "appendage_min_pct_H": {''')
    old_line = '            L["unsure"] = bool(1.0 <= ws < 2.0 and not L["below"])\n    for L in trunk:\n'
    assert s.count(old_line) == 1, s.count(old_line)
    s = s.replace(old_line, '            L["unsure"] = bool(1.0 <= ws < 2.0 and not L["below"])\n'
                  '            if L["kind"] == "wing" and L.get("join_frac", 0.0) > o["wing_join_max"]:\n'
                  '                L["kind"] = "crest"                              # a lone sheet rooted on the head / neck: an ear\n'
                  '    for L in trunk:\n', 1)
    s = rep(s, '''    tr = trace_all(P, faces, o)
    counts = classify(tr, o)
    H = tr["H"]
    leaks = fix_leaks(tr, o)''', '''    tr = trace_all(P, faces, o)
    counts = classify(tr, o)
    H = tr["H"]
    merged = merge_children(tr, o)
    leaks = fix_leaks(tr, o)''')
    s = rep(s, '''           "leak_cells_moved": leaks,''', '''           "leak_cells_moved": leaks, "merged_cells": merged,''')
    s = rep(s, '''    use_app = bool(appendages) and (o["appendages"] == "on" or (o["appendages"] == "auto" and use))''',
            '''    use_app = bool(appendages) and o["appendages"] == "on"      # auto: the monster branch switches it on''')
    p.write_text(s, encoding="utf-8", newline="\n")
    print("multilimb: merge_children + wing_join_max + appendages monster-only")
else:
    print("multilimb: already")
