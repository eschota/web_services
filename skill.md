# AutoRig Root Skill

This repository contains AutoRig.online production services and adjacent helper services.

## Navigation

- Project node: `R:\autorig\autorig.skill.md`
- XML hierarchy: `R:\autorig\skill.xml`
- AutoRig.online service skill: `R:\autorig\autorig-online\skill.md`
- Project rules: `R:\autorig\AGENTS.md`

## Operational Rules

- Treat `R:\autorig\AGENTS.md` as the source of truth for deploy and production workflow.
- Keep AutoRig.online code changes scoped under `autorig-online/` unless a task explicitly names another service.
- Preserve dirty working-tree changes that are not part of the current task.
- Document new task transport, database, or API contracts in the relevant `skill.md` file.

