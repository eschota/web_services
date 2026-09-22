# Node preview, duplication, A/B and history — production QA, 2026-09-22

Scope: bounded production-browser checks introduced against release `K1` and
carried into current release `K2`. This evidence proves the listed interactions
only. It does not qualify model output quality or complete the Video Avatar
story.

## Verified interactions

- A real Canny extraction/generation chain, task prefixes `7a207…` → `a9de6…`,
  completed through the graph. Its previous result remained in node history
  after reload.
- A compact (`Small`) node displayed five bounded history thumbnails.
- The selected global A comparison anchor survived graph copy and reload.
- Duplicating the graph created a distinct deep link while the current canvas
  and already accepted task/result state remained available.
- Main video preview played continuously in the overlay, and sharing the
  current result used the current durable media URL.
- A real Bonsai graph could be renamed and moved; its output and bounded result
  history remained attached afterward.
- The production UI exposed incremental execution, exact selected model data,
  task ETA/status, per-node share, graph duplication, A/B selection and result
  history in the tested flows.

## Limits

- These were targeted interaction checks, not an exhaustive cross-browser or
  accessibility pass.
- History thumbnails and overlay playback prove persistence and presentation,
  not anatomical, facial, identity or temporal quality of the generated media.
- Active-job continuity was exercised in the tested graph state, but longer
  interruption/reconnect cases remain part of continuing production QA.
- No two-Avatar story was rendered or accepted by these checks.
