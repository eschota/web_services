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
    q=$(python3 -c "import json; d=json.load(open('$LIB/$c.json')); print(d.get('quality'), '|', (d.get('quality_why') or {}).get('gait'))" 2>/dev/null)
    echo "   -> $q"
    case "$q" in ok*) break;; esac
  done
done
echo "CATEGORY_GEN_FINAL $SP $(date -u +%H:%M)"
