# Changelog

## v0.01.004 - Default site rig orientation

- Defaulted browser-created rig tasks to an authoritative identity orientation when the user does not rotate the model manually.
- Preserved legacy worker orientation sweep for API payloads without `local_rotation_authoritative`.
- Ensured task restarts dispatch saved `rig_orientation` even when no separate model transform is present.

## v0.01.003 - Site-authoritative rig orientation

- Added manual face-direction controls to rig task creation and manual restart flows.
- Stored `rig_orientation` snapshots with authoritative `local_rotation` for site-created rig tasks.
- Passed authoritative orientation to converter workers so humanoid tasks can skip the worker orientation sweep.

## v0.01.002 - Worker viewer environment contract

- Added `viewer_environment` snapshots from selected viewer themes to task settings.
- Passed viewer environment snapshots to rig workers through worker payloads and `rig.json`.
- Preserved selected theme snapshots across task creation, viewer settings saves, auto-select, and restart flows.
