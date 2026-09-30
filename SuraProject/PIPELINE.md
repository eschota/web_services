# Pipeline — where Sura is made

## autorig.online

| What | Where |
|---|---|
| Graph editor | https://autorig.online/nodes?g=<id> |
| Pose library (adult) | graph `5a7b0c19e4d2`: 14 appearance text nodes → Join texts → tattoo plates → 48 pose nodes |
| Video graphs (scene split, Sura swap) | `c9c5ba39f49f`, `aed316256aea`, `67f9cdc54c65`, `65ce6ea7e054` |
| Phone gallery of the farm | https://autorig.online/queue |
| System prompts (live, no restart) | https://autorig.online/system_prompts → `/srv/autorig/data/var/prompts/library.json` |
| Agent skill for graphs | https://autorig.online/dev/skills/nodes-graph-editing |
| Owner approvals | https://autorig.online/dev |

## Models on the fleet (frozen: no new base models unasked)

- Qwen-Image 2.1 turbo edit — identity swaps and edits (`/api/qwen-image`).
- Z-Image Turbo, PornMaster Z-image (adult), FLUX.2 klein, Krea 2 — text-to-image
  (`/api/image`).
- Sura LoRA `sura_zit_A.safetensors` (Z-Image, trigger `sura`, training preview);
  more bases in training on f5/f15/f12.
- Video: LTX-10Eros, MiniMax H3 (`/api/video`); 10Eros is silent unless the
  prompt names the sounds.
- Vision / Text: `bonsai2-27b`, `qwen35-9b-uncensored`.

## Proven recipes

- **Sura swap:** `<image1>` scene frame, `<image2>` Sura reference, `<image3>`
  depth of the scene. Face only when the scene shows it.
- **Join texts node** (`text_join`, 2026-09-30): up to 20 text wires joined word
  for word, no model — one detail per node.
- **Summary join** at native speed (`fit: native`), never stretched.
- A `/system_prompts` edit invalidates the render cache of the nodes using it.

## Upload to YouTube

- Uploader: `.agents/skills/asset-store-submission/scripts/youtube_upload.py`
  in the ASStore26 repo (Data API v3, OAuth, scope `youtube.upload`).
- Sura Games is its **own** channel: it needs its own OAuth consent and refresh
  token, never the AutoRig or U3D credentials (see `AGENTS.md` → YouTube).
  Status 2026-09-30: not yet authorised — the owner signs in once.
- A new API project may be limited to private uploads until Google's audit;
  check the actual `privacyStatus` after each upload.
- Tokens live outside Git (`/srv/autorig/secrets/` or the local user profile).
