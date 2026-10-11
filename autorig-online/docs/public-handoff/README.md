# Public AutoRig development handoff

`autorig-online/static/handoff.md` is the source served at:

`https://www.autorig.online/handoff.md`

It is a public, sanitized coordination view. The root repository `handoff.md` remains the detailed internal checkpoint and source of truth for work ownership, production receipts and private operational context.

## Update rules

1. Read `AGENTS.md`, root `handoff.md`, `GET /api/fleet?format=text` and `GET /api/task-page/live`.
2. Re-read `autorig-online/static/handoff.md` immediately before editing it; another agent may have published meanwhile.
3. Keep only public-safe facts. Exclude secrets, customer identifiers, signed URLs, internal addresses, private asset paths and unpublished licensed material.
4. Add an `Updated` time in `Asia/Novosibirsk` and the UTC time of the production snapshot.
5. Distinguish `source-only`, `staged`, `live`, `accepted`, `blocked` and `TODO`. Do not infer completion from a commit or build alone.
6. Edit locally with `apply_patch`, then publish the one file atomically:

   ```bash
   scp autorig-online/static/handoff.md autorig-vps:/tmp/autorig-public-handoff.md
   ssh autorig-vps 'sudo python3 /srv/autorig/tools/live_static.py put handoff.md /tmp/autorig-public-handoff.md'
   ```

7. Verify the canonical URL and bytes:

   ```bash
   curl -fsS -A AutoRigAudit/1.0 -D - https://www.autorig.online/handoff.md
   ```

   The final response must be Markdown (`Content-Type: text/markdown; charset=utf-8`), `Cache-Control: no-cache`, and its SHA-256 must equal the local source.

8. Commit and push only the handoff payload, this procedure, and a route change when required. Never bundle unrelated working-tree changes.

The nginx route is an exact `/handoff.md` location. It reads the live-static overlay first and the current immutable release second, so later atomic publications do not need an application restart.
