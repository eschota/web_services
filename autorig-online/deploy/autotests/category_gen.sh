#!/bin/bash
# Reference library per category (Fable, AGENTS.md «A Conveyor, Not One-Offs»): generate every gait / action of a
# category on the farm (H3 via renderfin) from one representative run, judge each take with mt/gait_gate.py (inside
# animal_video.run_path), retry with other seeds until the gate passes.  The library entry is keyed by the species
# (animal_video.species_key of WHAT); kin share it through animpick.VIDEO_KIN (zebra -> horse -> donkey ...).
#   usage: category_gen.sh <run id> "<what>" [clips...]          clips default: walk trot canter gallop idle rear look
#   e.g.   category_gen.sh 028d1acdb7187c532921 "white horse"             # the equine set
#          category_gen.sh eeb152fb0d700ce33e56 "fox" walk trot idle look  # canines
cd /srv/autorig/data/motion_transfer
RUN=runs/$1; WHAT="$2"; shift 2
CLIPS=${@:-walk trot canter gallop idle rear look}
SP=$(/srv/autorig/venv/bin/python3 -c "import sys; sys.path.insert(0,'.'); from mt import animal_video as AV; print(AV.species_key('$WHAT'))")
LIB=animal_motion_lib/$SP
echo "category $SP from $RUN ($WHAT): $CLIPS"
for c in $CLIPS; do
  for seed in 41 7 123; do
    echo "== $c seed=$seed $(date -u +%H:%M)"
    sudo -u autorig nice -n 19 env PYTHONPATH=. /srv/autorig/venv/bin/python3 -P -m mt.animal_video --run $RUN --what "$WHAT" --clip $c --model h3 --seed $seed 2>&1 | grep -E "period_s|quality|Traceback|Error" | head -3
    cp $LIB/$c.json $LIB/$c.seed$seed.json 2>/dev/null       # every take is kept; the best one becomes the entry
    q=$(python3 -c "import json; d=json.load(open('$LIB/$c.json')); print(d.get('quality'), '|', (d.get('quality_why') or {}).get('gait'))" 2>/dev/null)
    echo "   -> $q"
    case "$q" in ok*) break;; esac
  done
  /srv/autorig/venv/bin/python3 - "$LIB" "$c" <<PY
import json, glob, sys, os
lib, clip = sys.argv[1], sys.argv[2]
takes = []
for f in sorted(glob.glob(f"{lib}/{clip}.seed*.json")):
    e = json.load(open(f)); g = (e.get("quality_why") or {}).get("gait") or {}
    takes.append((0 if e.get("quality") == "ok" else 1, len(g.get("why") or []), f, e))
if takes:
    takes.sort(key=lambda t: (t[0], t[1]))
    best = takes[0]
    json.dump(best[3], open(f"{lib}/{clip}.json", "w"), indent=1)
    print("   best take:", os.path.basename(best[2]), best[3].get("quality"), (best[3].get("quality_why") or {}).get("gait", {}).get("why"))
PY
done
echo "CATEGORY_GEN_FINAL $SP $(date -u +%H:%M)"
