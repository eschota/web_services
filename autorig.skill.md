# AutoRig Project Skill Index

Use this file as the project-level entrypoint for agents working in `R:\autorig`.

## Hierarchy

- Root skill: [skill.md](skill.md)
- XML hierarchy: [skill.xml](skill.xml)
- AutoRig.online service: [autorig-online/skill.md](autorig-online/skill.md)
- Project rules: [AGENTS.md](AGENTS.md)

## Service Map

- `autorig-online/`: production FastAPI backend, static frontend, viewer UI, task APIs, purchase/download flows.
- `autorig/`: AutoRig-related assets and tools.
- `qwerty_vpn/`: QwertyStock VPN gateway/proxy service.
- `CGTrader_SUBMIT_SERVER/`: CGTrader submit server.

## Current Download Bundle Contract

- Worker full bundle ZIP is resolved from task worker root and GUID.
- Worker sidecar metadata is `<guid>.zip.meta.json`.
- AutoRig backend exposes full bundle count through `bundle_file_count` only when `bundle_file_count_ready` is true.
- UI must not use direct cached-file `file_count` as the full bundle count.

## Current SEO URL Contract

- Public task URLs stay on `https://autorig.online/task?id={task_id}`.
- Do not create or index `/m` / `/m/...` task pages; they are not part of AutoRig.online.
