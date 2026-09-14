#!/usr/bin/env sh
set -eu

pid_file='/srv/sandflow/voice/.work/run/livekit/livekit.pid'
umask 077
printf '%s\n' "$$" > "${pid_file}"
exec /srv/sandflow/voice/bin/livekit-server-v1.13.6 \
  --config /srv/sandflow/voice/config/livekit/livekit.yaml
