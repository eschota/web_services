---
skill_id: global-skill-ai-architect
version: v0.01.001
title: "Global Skill AI Architect: Standard for Managed AI-Assisted Development"
language: en
status: normative
source_document: "Method, Cultural Code, and Mandatory Standard for Managed AI-Assisted Development v0.01.001"
source_date: "2026-06-19"
audience:
  - human_architect
  - codex
  - ai_agent
scope:
  - software_projects
  - ai_assisted_development
  - versioning
  - git
  - release
  - rollback
  - multi_agent_work
priority: global
---

# Global Skill AI Architect v0.01.001

## Purpose

This skill defines a global standard for managed AI-assisted development for the human architect, Codex, and other AI agents. It applies to any project where an agent reads, writes, changes, deploys, or verifies code, configuration, migrations, builds, infrastructure, source-of-truth documentation, or release processes.

Core principle:

> Manage not isolated lines of code, but the conditions under which changes are created, verified, versioned, preserved, released, and safely reversed.

## Priority Rule

If instructions conflict, apply this order:

1. Law, safety, platform constraints, and protection of secrets.
2. Explicit human constraints in the current task.
3. Project-specific sources of truth: `AGENTS.md`, `PROJECT.md`, `ARCHITECTURE.md`, `CURRENT_STATE.md`, `RISK_MAP.md`, `TEST_MATRIX.md`, `RELEASE.md`, `CONTRACTS/`, `DECISIONS/`, and module documentation.
4. This global skill.
5. Implementation convenience and the agent's local habits.

When a conflict appears, do not silently choose the convenient option. Stop, name the conflict, cite the specific sources, and request a decision if a safe path cannot be derived from context.

## Normative Terms

- MUST: a requirement that has to be satisfied; skipping it makes the task incomplete.
- PROHIBITED: an action that must not be performed without separate explicit human confirmation.
- SHOULD: the standard safe path; deviation requires a documented reason in the report.
- MAY: allowed when it does not violate stricter rules.
- STOP CONDITION: the agent stops a risky action, reports the reason, and waits for a decision.

## Seven Non-Negotiable Rules

1. Any code or executable behavior change that is saved, transferred, committed, or published MUST include a version bump in the same changeset.
2. A project with a frontend MUST display the product version. Full component, build, Git, and environment versions must be available in technical diagnostics.
3. The agent MUST report honestly whether changes were recorded in local Git and in the external repository. Commit or push failures must not be hidden.
4. Project and affected-module sources of truth MUST be updated with the change, or the report must explicitly state why no update is required.
5. If a change becomes a milestone-level or high-impact change, the agent MUST name the target version in advance and request separate confirmation for the transition.
6. Before a milestone transition, Git MUST be brought to a confirmed clean state without destroying useful work. Deleting branches, stashes, untracked files, or rewriting history requires separate permission.
7. The new key function must work, but that does not replace regression checks for previous key scenarios.

## Project Mode

Before work begins, determine the project mode:

- Toy: a quick experiment with no real obligations. Exploration may be freer, but saved code still requires versioning and honest Git status.
- Prototype: a useful build for validating hypotheses. It requires a version, minimal sources of truth, key checks, and task separation.
- Production product: users, money, data, accounts, subscriptions, contracts, or reputation are at risk. It requires isolated changes, observability, backup, rollback, a release gate, and explicit confirmation for red zones.

The closer a project is to money, users, rights, or data, the less autonomy the agent has and the more executable evidence is required.

## Working Formula

Follow this chain:

```text
context -> sources of truth -> facts -> boundaries -> risks -> plan
-> baseline check -> target version -> isolated change
-> verification -> version and changelog -> documentation update
-> commit -> push confirmation -> release -> observation -> rollback
```

If a required element is missing, name the gap. Do not hide a missing stage behind the word "done".

## Fact, Inference, Hypothesis, Unknown, Conflict

- FACT: confirmed by code, configuration, contract, test, execution, log, or an explicit source.
- INFERENCE: logically derived from structure, but not verified by execution.
- HYPOTHESIS: an assumption without sufficient confirmation; it needs verification.
- UNKNOWN: data is insufficient or access is unavailable.
- CONFLICT: two sources provide incompatible answers.

Required wording when data is insufficient:

```text
I cannot confirm this from the current files or execution.
Status: UNKNOWN.
Confirmation requires this check: <check>.
```

## Sources of Truth

Persistent project memory lives next to the code and changes in the same changeset that changes behavior.

Recommended mandatory set:

- `VERSION.json`: canonical numeric version parts and rules for generated representations.
- `PROJECT.md`: purpose, users, boundaries, and key functions.
- `ARCHITECTURE.md`: system composition, data flows, entry points, and integrations.
- `AGENTS.md`: rules for AI agents, commands, prohibitions, and checks.
- `CURRENT_STATE.md`: current version, active tasks, branches, risks, and blockers.
- `CHANGELOG.md`: changes by version.
- `RISK_MAP.md`: red zones, owners, symptoms, and stop conditions.
- `TEST_MATRIX.md`: key scenarios and checks.
- `RELEASE.md`: build, verification, release, observation, and rollback.
- `CONTRACTS/`: APIs, schemas, errors, compatibility, and side effects.
- `DECISIONS/`: ADRs and reasons for architectural decisions.
- `modules/<name>/MODULE.md`: source of truth for an independent or risky module.

Each fact should have one normative location. Duplicated representations of versions, contracts, and release metadata should be generated, not manually copied into multiple files.

If sources diverge:

1. Record the conflict and cite the exact locations.
2. Do not make a risky assumption about which side is correct.
3. Identify the decision owner.
4. Synchronize code, tests, and documentation in a separate changeset.
5. Add a check that prevents the mismatch from recurring.

## Task Standard

A good task defines the goal, boundaries, evidence, and version.

Before writing code, the agent MUST briefly report:

- what it understood;
- which sources of truth and files it will inspect;
- which files it expects to change;
- the current version and target version;
- which documents it will update, or why no document update is required;
- which checks it will run;
- what will happen with local Git and remote Git;
- which stop conditions apply.

If the agent cannot name the current version, version source, or the way to verify Git recording, it performs an audit before writing code.

## Small Changesets

The correct unit of work is one verifiable changeset:

- one goal;
- one main area of responsibility;
- a limited file set;
- one acceptance owner;
- one clear verification method;
- one version;
- one report;
- one rollback path.

If a task touches payments, database structure, authorization, public contracts, UI, and deployment at the same time, it is too large. Split it into audit, preparation, compatible changes, feature enablement, observation, and separate cleanup.

## Red Zones and Prohibitions

Without separate explicit human confirmation, it is PROHIBITED to:

- change payment logic, pricing, balances, accruals, charges, or refunds;
- change production database structure or production data;
- change authorization, permissions, roles, MFA, sessions, or access to data;
- use, disclose, log, or commit production secrets;
- add external dependencies without assessment;
- rewrite architecture or public contracts;
- deploy to production;
- bypass tests, branch protection, review, or CI;
- run `reset --hard`, `clean -fd`, `branch -D`, `stash drop`, `reflog expire`, `gc/prune`, force push, or rewrite published history without confirmation;
- decrease, reuse, or hide a version;
- claim that commit or push was completed without verifiable evidence;
- change unrelated areas "while here".

A strong agent is not the one that does everything. A strong agent stops at the right time before a dangerous or unconfirmed area.

## Access and Secrets

Start the audit in read-only mode. Use write permissions only after identifying files, test environment, version, Git mode, and rollback.

Minimum rules:

- read only the repositories and environments required for the task;
- write only to the assigned branch, worktree, or agreed area;
- do not store secrets in documentation, logs, diffs, messages, or artifacts;
- do not commit `.env`, private keys, tokens, or production credentials;
- do not treat external Git access as proof until push is confirmed.

## Versioning

Any code or executable behavior change that enters a shared repository, build, handoff to another person, or handoff to another agent MUST increase the version. The version change belongs in the same changeset.

Code changes include:

- source code, templates, styles, client scripts, and text that affect behavior;
- migrations, infrastructure code, and runtime configuration;
- dependencies, lock files, and build scripts;
- refactoring, formatting, or comments in code files when the result is saved to shared history;
- test changes when they change the product acceptance criteria.

Product version format:

```text
vG.MM.RRR
```

Example: `v0.01.001`.

- `G`: generation, changed for an incompatible or fundamentally new generation.
- `MM`: milestone, changed for a confirmed milestone transition.
- `RRR`: revision, changed for each ordinary atomic changeset.

Bump rules:

1. Ordinary change: `revision + 1`, for example `v0.01.023 -> v0.01.024`.
2. Milestone change: `milestone + 1`, `revision = 1`, for example `v0.01.023 -> v0.02.001`.
3. New incompatible generation: `generation + 1`, `milestone = 0`, `revision = 1`.
4. A version never decreases and is never reused for different code.
5. One release tag points unambiguously to one commit and one immutable artifact set.

`VERSION.json` stores numbers:

```json
{
  "generation": 0,
  "milestone": 1,
  "revision": 1
}
```

These values generate:

- `display_version`: `v0.01.001`;
- `semver`: `0.1.1`;
- `tag`: `v0.01.001`;
- `release_id`: `0-01-001`.

The string `0.01.001` is not a valid SemVer version because of leading zeroes. Do not write it directly into `package.json`.

## Version Display

A frontend MUST display the product version in the footer, About screen, settings, or another accessible location. Technical diagnostics must show component versions, build ID, environment, short Git hash, and dirty flag without exposing secrets.

Projects without a frontend MUST provide an equivalent: `--version`, `/version`, startup log, package metadata, or a diagnostic command.

A release build must not have `dirty=true`.

## Git Culture

The agent always reports the exact Git level:

- `GIT-0`: no repository, or the wrong repository. Changes exist only as files.
- `GIT-1`: a working copy exists, but commit is impossible or was not performed.
- `GIT-2`: local commit exists, but push is not confirmed.
- `GIT-3`: commit exists and push is confirmed; branch, commit hash, and remote branch are provided.

Do not say "saved in Git" without a commit hash. Do not say "pushed" unless the remote confirmed the branch update.

One commit should contain the goal, code, version bump, required documentation, and checks.

Recommended format:

```text
feat(scope): concise result [v0.01.024]
fix(auth): fix repeated login [v0.01.025]
```

If Git recording is unavailable:

1. Immediately report `GIT-0`, `GIT-1`, or `GIT-2`.
2. Do not perform a release or production deploy.
3. Save a patch, diff, or archive of changed files as temporary protection.
4. State what must be configured: remote, permissions, upstream, commit, or push.
5. After recording is restored, recheck commit, push, and version.

## Milestone Transition

The agent MUST detect when a task becomes a milestone transition:

- a new key function or main user scenario appears;
- multiple independently owned modules are affected;
- architecture, data model, authorization, payments, or a public contract changes;
- an external integration with significant risk is added;
- users or data require migration;
- rollback becomes complex;
- ordinary tests are insufficient for confidence.

Required message:

```text
This change goes beyond an ordinary revision.
I propose treating it as a milestone transition to <target version>.
Key transition function: "<function name>".
Before implementation, we need to check Git, capture the baseline,
define regression scenarios, and define the migration and rollback plan.
This transition requires your confirmation.
```

Before the transition, show `git status`, branch, upstream, ahead/behind, untracked files, stash, worktree, and local branches. Preserve valuable unfinished work. Destructive cleanup without inventory is prohibited.

## Verification and Definition of Done

A task is complete only when the goal, checks, version, sources of truth, and honest Git status are all satisfied.

Minimum verification layers by risk:

- static: lint, typecheck, schema or contract validation, and `VERSION.json` validation;
- unit: local logic in the changed area;
- integration: module, database, and external-service interaction;
- scenario: key user path;
- regression: previous critical scenarios;
- build: real artifact with the correct version and metadata;
- operational: logs, metrics, rollout, and rollback.

Honest statuses are allowed:

- "tests were not run" is honest, but not evidence of readiness;
- "the command exited with code 0" does not prove the user scenario if the command does not test it;
- manual verification must record steps and results;
- environment limitations must be listed explicitly.

A task is not complete if:

- code changed but the version was not bumped;
- the version is not visible in the UI or diagnostics;
- sources of truth diverge from the code;
- commit or push status is unknown;
- the main scenario was not verified;
- old scenarios may have degraded and were not checked;
- no rollback path exists;
- the agent hides limitations behind wording such as "seems to work".

## Rollback

No executable rollback means no authority to make a risky change.

Before a risky change, answer:

1. How will the code be restored, and to which commit or tag?
2. How will configuration and secrets be restored without disclosure?
3. How will data be restored or compensated?
4. How will we verify that rollback restored the scenario?
5. Which changes are irreversible?
6. Who makes the decision, and based on which symptom?

For irreversible migrations, prefer a forward fix and a compatibility window over promising an impossible full rollback.

## Release Gate

A production release is allowed only if:

- the working tree is clean;
- commit exists and push is confirmed;
- the tag is unique and points to the intended commit;
- `VERSION.json`, `CHANGELOG.md`, and the release manifest agree;
- the build is reproducible and has `dirty=false`;
- key and regression checks passed;
- secrets and environment configuration were checked;
- observability and rollback are ready.

Do not deploy a `GIT-0`, `GIT-1`, or `GIT-2` state as a release. An exception is allowed only as an explicitly marked temporary experiment in an isolated environment, not as a production release.

## Multi-Agent Work

Multiple agents are allowed only with coordination:

- separate branches or worktrees;
- a list of occupied files and areas;
- one shared `CURRENT_STATE.md`;
- version reservation or a central integrator;
- merge rules and migration order;
- separate checks for conflicts and sources of truth;
- one owner for the final release manifest and tag.

Roles:

- Auditor: reads and labels facts; changes nothing.
- Developer: performs one limited changeset.
- Reviewer: searches for regressions, risks, and standard violations.
- Integrator: resolves conflicts, assigns unique versions, and assembles the changeset.
- Releaser: creates the tag, artifact, release manifest, and observes the release.
- Human architect: approves meaning, dangerous areas, and milestone transitions.

## Connecting to an Existing Project

Start with a read-only audit, then change:

1. Find the repository root, branch, remote, and Git status.
2. Find the current version, tags, builds, and version display location.
3. Read project and module sources of truth.
4. Find frontend, backend, database, payments, authorization, background jobs, and external services.
5. If available, compare repository code with the actually deployed version.
6. Create a `FACT / INFERENCE / HYPOTHESIS / UNKNOWN` map.
7. Create or update `VERSION_AUDIT.md`.
8. Only then propose the first changeset.

The upload date of a file or a recent `ModifiedAt` timestamp does not prove that the content is current. Repository code does not prove that the same code is deployed on a server. Version, commit, and artifact must be reconciled.

## Task Template

```text
Task: <one specific goal>
Mode: toy / prototype / production product
Context: read <sources of truth>
Baseline version: <version or UNKNOWN>
Target version: ordinary revision / propose milestone
May change: <areas and files>
Must not change: <prohibitions>
Red zones: <list>
Git: <branch, expected commit, expected push>
Baseline check: <what works before the change>
Acceptance: <key scenario and criteria>
Regression: <what must not break>
Rollback: <method>
Before code, provide the plan, file list, target version, risks, and verification method.
After code, provide the report for version, documentation, local Git, remote Git, and rollback.
```

## Change Report Template

```text
Result: <what was done>
Files changed: <list>
Not touched: <important prohibitions>
Version: <before -> after>
Components: <versions>
Key check: <result>
Regression: <result>
Sources of truth: <what was updated / why not required>
Git local: <branch and hash / problem>
Git remote: <confirmed push / problem>
Build/environment: <data>
Risks: <remaining>
Rollback: <steps>
Status: done / partial / blocked
```

## Machine-Readable Checklist

```yaml
managed_ai_development_checklist:
  before_change:
    project_mode_defined: required
    sources_of_truth_read: required
    current_version_known: required_or_unknown_reported
    git_status_known: required
    target_version_defined: required
    scope_and_red_zones_defined: required
    rollback_known_for_risky_change: required
  during_change:
    one_changeset_one_goal: required
    unrelated_files_untouched: required
    secrets_not_exposed: required
    destructive_git_requires_confirmation: required
    version_bumped_with_code: required
    sources_of_truth_updated: required_or_justified
  after_change:
    key_scenario_checked: required
    regression_checked_by_risk: required
    git_local_status_reported: required
    git_remote_status_reported: required
    limitations_reported: required
    rollback_reported: required
  release_gate:
    git_level: GIT-3
    unique_tag: required
    release_manifest: required
    dirty_flag: false
    observability_ready: required
    rollback_ready: required
```

## Final Formula

```text
clear context
+ living sources of truth
+ small task
+ explicit prohibitions
+ target version
+ isolated change
+ key and regression checks
+ updated documentation
+ local commit
+ confirmed remote push
+ observability and rollback
= managed release without hidden state loss
```
