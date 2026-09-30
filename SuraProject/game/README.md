# Sura — the game (working title)

Visual novel with interactive scenes, built on video generations from the
autorig.online engine (graphs on /nodes, approvals on /dev). Status 2026-09-30:
**concept stage.** Approved so far: the premise and the concept of level 1.

## Premise (approved)

A very rich young man, 18, flies on his father's private business jet to a
tropical island to meet his father and the father's new partner. He is used to
money opening every door; on this flight he has to interest people as himself.

**Sura** is the flight attendant on the jet — in truth a **secret agent**. She
appears at the start of the game and later schemes against the hero.
(The level-1 prototype still calls her by the working name "Mira"; the canon
name is Sura.)

## Hierarchy

```
game/
  README.md            this file: premise, status, structure
  DESIGN.md            pillars, format, rating split, open questions
  characters/          one file per character (hero, Sura, father, the partner)
  levels/
    01-jet/            level 1 "До острова": prototype, story.json, concept art
  art/                 shared art, style guides (interior style: dark futurism)
  pipeline/            how scenes are produced (graphs, depth, Blender block-out)
```

## Level list

| # | Folder | Place | Status |
|---|---|---|---|
| 1 | `levels/01-jet` | Cabin of the private jet, descent to the island | Prototype: 40 scenes, 6 endings |
| 2 | — | Arrival on the island, meeting the father and his partner | Not started |
