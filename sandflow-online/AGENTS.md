# SandFlow Online service

- Only this service directory is in scope. Do not edit or restart existing AutoRig backend/voice/farm jobs.
- Physics is client-only. The backend sequences and validates messages, manages leased client authority, and persists worlds.
- Deliver scoped tested checkpoints to eschota/web_services main, preserving unrelated branch history. Parent integration uses a clean main-based worktree.
- Maintain README and the canonical plan/log in eschota/Sand-Keeper/docs. Never mark infrastructure tests as Unity gameplay validation.
- All temporary files, SDK caches, compiled output, test data and deployment bundles live below `.work` or explicit ignored build folders in this directory.
- Voice agent owns `voice/`; parent owns backend, contracts, deployment and integration. No simultaneous edits to the same files.
- Feature flags default to closed public admission until real client validation. Do not invent a working multiplayer UI over an unconnected build.
