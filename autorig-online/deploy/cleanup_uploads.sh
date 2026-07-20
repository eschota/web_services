#!/usr/bin/env bash
set -euo pipefail

# Pressure-only cleanup for AutoRig upload originals.
# It does nothing while the root filesystem has enough free space.
# Public task cache, GLB cache, videos, posters, and DB rows are not touched.

UPLOAD_DIR="${UPLOAD_DIR:-/var/autorig/uploads}"
CHECK_PATH="${CHECK_PATH:-/}"
CRITICAL_FREE_GB="${CRITICAL_FREE_GB:-2.5}"
TARGET_FREE_GB="${TARGET_FREE_GB:-4.0}"
MIN_AGE_HOURS="${UPLOAD_PRESSURE_CLEANUP_MIN_AGE_HOURS:-1}"

gb_to_bytes() {
  awk -v gb="$1" 'BEGIN { printf "%.0f", gb * 1024 * 1024 * 1024 }'
}

free_bytes() {
  df -PB1 "$CHECK_PATH" | awk 'NR == 2 { print $4 }'
}

log() {
  printf '%s %s\n' "$(date -Is)" "$*"
}

if [ ! -d "$UPLOAD_DIR" ]; then
  log "upload dir missing: $UPLOAD_DIR"
  exit 0
fi

base="$(readlink -f "$UPLOAD_DIR")"
critical_bytes="$(gb_to_bytes "$CRITICAL_FREE_GB")"
target_bytes="$(gb_to_bytes "$TARGET_FREE_GB")"
current_free="$(free_bytes)"

if [ "$current_free" -ge "$critical_bytes" ]; then
  log "enough free space: $(awk -v b="$current_free" 'BEGIN { printf "%.2f", b/1024/1024/1024 }') GB >= ${CRITICAL_FREE_GB} GB; nothing to clean"
  exit 0
fi

min_age_minutes="$(awk -v h="$MIN_AGE_HOURS" 'BEGIN { printf "%d", h * 60 }')"
deleted=0
freed=0

log "low free space: $(awk -v b="$current_free" 'BEGIN { printf "%.2f", b/1024/1024/1024 }') GB < ${CRITICAL_FREE_GB} GB; cleaning upload originals older than ${MIN_AGE_HOURS}h toward ${TARGET_FREE_GB} GB"

while IFS= read -r -d '' record; do
  current_free="$(free_bytes)"
  if [ "$current_free" -ge "$target_bytes" ]; then
    break
  fi

  path="${record#* }"
  target="$(readlink -f "$path" 2>/dev/null || true)"
  case "$target" in
    "$base"/*) ;;
    *) log "skip unsafe path: $path"; continue ;;
  esac

  if [ ! -d "$target" ]; then
    continue
  fi

  size="$(du -sb "$target" 2>/dev/null | awk '{ print $1 }')"
  rm -rf -- "$target"
  deleted=$((deleted + 1))
  freed=$((freed + size))
  log "removed upload $(basename "$target") size=$(awk -v b="$size" 'BEGIN { printf "%.1f", b/1024/1024 }') MB"
done < <(find "$base" -mindepth 1 -maxdepth 1 -type d -mmin +"$min_age_minutes" -printf '%T@ %p\0' 2>/dev/null | sort -z -n)

find "$base" -mindepth 1 -type d -empty -mmin +"$min_age_minutes" -delete 2>/dev/null || true

current_free="$(free_bytes)"
log "done: removed=${deleted}, freed=$(awk -v b="$freed" 'BEGIN { printf "%.2f", b/1024/1024/1024 }') GB, free=$(awk -v b="$current_free" 'BEGIN { printf "%.2f", b/1024/1024/1024 }') GB"
