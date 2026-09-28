#!/usr/bin/env bash
# Post-switch smoke test for autorig-storage (owner rule, 2026-09-28).
#
#   deploy_smoke.sh <rollback-release-name> [graph-id]
#
# Run right after `current` is switched and autorig-storage restarted. Checks:
#   1. POST /api/qwen-image (tiny generate, wait_seconds 0) answers 200 with a task
#   2. GET  /api/ai/graphs/<graph-id> answers 200 with nodes
#   3. GET  /nodes answers 200
# If any fails, `current` goes back to <rollback-release-name>, autorig-storage
# restarts, and the script exits 1.
set -u
ROLLBACK="${1:?rollback release name}"
GRAPH="${2:-ba71804ece3f}"
BASE="http://127.0.0.1:8200"
RELEASES=/srv/autorig/releases
fail=""

for i in $(seq 1 30); do
  curl -fs -o /dev/null "$BASE/nodes" && break
  sleep 1
done

qwen=$(curl -s -m 60 -w '\n%{http_code}' -H 'Content-Type: application/json' \
  -d '{"prompt":"deploy smoke test: a small grey cube","mode":"generate","width":256,"height":256,"seed":1,"wait_seconds":0}' \
  "$BASE/api/qwen-image")
code=$(printf '%s' "$qwen" | tail -n1)
body=$(printf '%s' "$qwen" | sed '$d')
if [ "$code" != 200 ] || ! printf '%s' "$body" | grep -q '"success_bool": *true\|"task_id'; then
  fail="$fail qwen-image:$code:$(printf '%s' "$body" | head -c 200)"
fi

graph=$(curl -s -m 30 -w '\n%{http_code}' "$BASE/api/ai/graphs/$GRAPH")
code=$(printf '%s' "$graph" | tail -n1)
if [ "$code" != 200 ] || ! printf '%s' "$graph" | grep -q '"nodes'; then
  fail="$fail graph:$code"
fi

code=$(curl -s -o /dev/null -w '%{http_code}' -m 30 "$BASE/nodes")
[ "$code" = 200 ] || fail="$fail nodes:$code"

if [ -n "$fail" ]; then
  echo "SMOKE FAILED:$fail"
  echo "rolling back to $ROLLBACK"
  sudo ln -sfn "$RELEASES/$ROLLBACK" /srv/autorig/current.tmp && sudo mv -T /srv/autorig/current.tmp /srv/autorig/current
  sudo systemctl restart autorig-storage
  exit 1
fi
echo "SMOKE OK (qwen-image task accepted, graph $GRAPH, /nodes)"
