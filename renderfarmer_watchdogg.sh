#!/bin/bash
# Retired in v3.0.0. systemd owns process restart policy; watching every Python
# file below /root caused restart storms and duplicate Telegram messages.
echo "renderfarmer_watchdogg is retired; use renderfarmer-monitor.service" >&2
exec sleep infinity
