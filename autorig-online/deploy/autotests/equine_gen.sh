#!/bin/bash
# Equine reference library (Fable, owner 2026-10-13 «конвейерное производство»): generate each gait on the farm (H3 via
# renderfin) from the white horse 31cc3ff3's run, judge it with mt/gait_gate.py (inside animal_video.run_path), retry
# with another seed until the gate passes (max 3 seeds). usage: equine_gen.sh [clips...]
cd /srv/autorig/data/motion_transfer
RUN=runs/028d1acdb7187c532921; WHAT="white horse"; LIB=animal_motion_lib/horse
CLIPS=${@:-trot canter gallop look}
for c in $CLIPS; do
  for seed in 41 7 123; do
    echo "== $c seed=$seed $(date -u +%H:%M)"
    sudo -u autorig nice -n 19 env PYTHONPATH=. /srv/autorig/venv/bin/python3 -P -m mt.animal_video --run $RUN --what "$WHAT" --clip $c --model h3 --seed $seed 2>&1 | grep -E "period_s|quality|Traceback|Error" | head -3
    q=$(python3 -c "import json; d=json.load(open('$LIB/$c.json')); print(d.get('quality'), '|', (d.get('quality_why') or {}).get('gait'))" 2>/dev/null)
    echo "   -> $q"
    case "$q" in ok*) break;; esac
  done
done
echo EQUINE_GEN_FINAL $(date -u +%H:%M)
