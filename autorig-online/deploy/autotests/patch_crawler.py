"""Plan routing (Rig classes, 0a629e36 «multi-legged monarch caterpillar» rigged as a quadruped because category.json said
quadruped / feline): crawler words (caterpillar, larva, worm, centipede, ...) win over the body / category words.
python patch_crawler.py <mt dir>"""
import pathlib
import sys

p = pathlib.Path(sys.argv[1]) / "creature.py"
s = p.read_text(encoding="utf-8")
if "CRAWLER_WORDS" in s:
    print("creature: already patched")
    sys.exit(0)
old = '''    w = (words or "").lower()
    if re.search(PERSON_WORDS, w):
        return None
    if is_bird(w):
        return "bird"'''
new = '''    w = (words or "").lower()
    if re.search(PERSON_WORDS, w):
        return None
    if re.search(CRAWLER_WORDS, w):                              # a caterpillar / larva / worm / centipede is never a
        from . import serpent as _SP                             # quadruped, whatever category.json says (0a629e36:
        return "serpent" if re.search(_SP.SERPENT_WORDS, w) else "multileg"   # quadruped / feline over «caterpillar»)
    if is_bird(w):
        return "bird"'''
assert s.count(old) == 1, s.count(old)
s = s.replace(old, new)
old2 = '''QUAD_WORDS = r"quadruped|four-legged|four legged|on all fours|on four legs"'''
new2 = r'''CRAWLER_WORDS = (r"\b(caterpillars?|larva[el]?|maggots?|grubs?|centipedes?|millipedes?|worms?|earthworms?|slugs?|leech(es)?|"
                 r"inchworms?|silkworms?|glowworms?)\b|\b(гусениц|личинк|черв|многонож|сороконож|слизн|пиявк)\w*")
QUAD_WORDS = r"quadruped|four-legged|four legged|on all fours|on four legs"'''
assert s.count(old2) == 1
s = s.replace(old2, new2)
p.write_text(s, encoding="utf-8", newline="\n")
print("creature: crawler routing")
