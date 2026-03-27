---
name: autorig-online-agents
version: 1.1.0
description: AutoRig Online — API access for AI agents (3D model rigging) without Google sign-in.
homepage: https://autorig.online
metadata: {"autorig":{"api_base":"https://autorig.online","docs_skill_url":"https://autorig.online/skill.md"}}
---

# AutoRig Online (AI agents)

AutoRig Online converts uploaded or linked 3D models (GLB, FBX, OBJ) through a cloud rigging pipeline. This skill describes how **software agents** authenticate and call the HTTP API **without Google OAuth**.

## Skill files

| File | URL |
|------|-----|
| **SKILL.md** (this file) | `https://autorig.online/skill.md` |

**Install locally (same idea as Moltbook):**

```bash
mkdir -p ~/.config/autorig-agent
curl -sS -o ~/.config/autorig-agent/SKILL.md https://autorig.online/skill.md
```

**Base URL:** `https://autorig.online` (override with env `APP_URL` on self-hosted installs).

## Security

- **Treat your API key like a password.** Anyone with the key can create tasks and access resources owned by that agent id.
- **Send the key only to AutoRig Online** — the same hostname you use in browser (e.g. `https://autorig.online`). Do not paste it into third-party tools, chat logs, or other APIs.
- Prefer **`Authorization: Bearer <api_key>`** or header **`X-Api-Key: <api_key>`**.

## Register an agent (no Google)

Creates an anonymous session plus one API key. The response returns the **plaintext key once**.

```bash
curl -sS -X POST https://autorig.online/api/agents/register \
  -H "Content-Type: application/json" \
  -d '{"name":"MyAgent","description":"Batch-rigs GLB assets from our pipeline"}'
```

Example response:

```json
{
  "api_key": "ar_xxxxxxxx_....",
  "agent_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

- **`agent_id`** — stable UUID for this agent (internal anonymous owner id).
- **`api_key`** — use on every authenticated request until revoked (revocation is currently only via the website for cookie-bound keys; keep the key safe).

Registration is rate-limited per IP (see HTTP `429`).

## Who am I?

```bash
curl -sS https://autorig.online/api/agents/me \
  -H "Authorization: Bearer YOUR_API_KEY"
```

Returns display name, description, free-tier counters, and a short note on how **credits** relate to Google accounts.

## Buy credits with cryptocurrency (~20% off)

The website **Buy Credits** page has a **Buy with Crypto** tab with the same packs as Gumroad at a discount. **Credits are not minted automatically from the blockchain** in this flow: after you send **USDT on the correct network** (or **BTC**), you must **submit the transaction id**. Operators verify on-chain and credit your account manually.

### Config (tiers, addresses, amounts)

```bash
curl -sS "https://autorig.online/api/buy-credits/crypto-config"
```

Response includes:

- **`tiers`:** `tier_key` (`autorig-100`, `autorig-500`, `autorig-1000`), `credits`, `usd_standard`, `usd_discounted`, `usdt_amount`, `btc_amount_approx`, `usd_per_credit_discounted`
- **`networks`:** `id`, `label`, `asset`, `address`, `warning` — use the **`id`** in submit and send only the asset/network the row describes.

### Report payment (after your tx confirms)

```bash
curl -sS -X POST "https://autorig.online/api/buy-credits/crypto-submit" \
  -H "Authorization: Bearer YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "tier": "autorig-500",
    "network_id": "usdt_trc20",
    "tx_id": "YOUR_TRANSACTION_ID_OR_HASH",
    "contact_note": "optional; include agent_id if not using Bearer"
  }'
```

- **`tier`:** `autorig-100` | `autorig-500` | `autorig-1000`
- **`network_id`:** `usdt_trc20` | `usdt_ton` | `usdt_sol` | `usdt_erc20` | `btc`
- **`tx_id`:** on-chain id/hash (8–256 characters)
- **`contact_note`:** optional when the request is authenticated (Bearer agent key or browser Google session); **required** (min 5 characters) when unauthenticated — put **email** or **`agent_id`** from registration so support can match payment to an account.

This endpoint is rate-limited per client IP (see `429`; override default with env `RATE_LIMIT_CRYPTO_SUBMIT` on self-hosted installs).

## Authentication on API calls

```bash
curl -sS https://autorig.online/api/agents/me \
  -H "Authorization: Bearer ar_xxx_yyy"
# equivalent:
curl -sS https://autorig.online/api/agents/me \
  -H "X-Api-Key: ar_xxx_yyy"
```

Session cookies from the browser are unrelated; agents can be fully headless.

## Main API flows

### Create a rig task (JSON, URL input)

```bash
curl -sS -X POST https://autorig.online/api/task/create \
  -H "Authorization: Bearer YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"input_url":"https://example.com/model.glb","type":"t_pose","pipeline":"rig"}'
```

- **`pipeline`:** `rig` (default) or `convert` (GLB-only pipeline; input must be `.glb`).
- **`source`:** `link` (default) or `upload` (multipart — same endpoint, use form fields + file).

### Poll status

```bash
curl -sS "https://autorig.online/api/task/TASK_ID" \
  -H "Authorization: Bearer YOUR_API_KEY"
```

When `status` is `done`, use `ready_urls`, `guid`, and task-scoped download routes as needed.

### Auto Convert from a completed rig task

```bash
curl -sS -X POST "https://autorig.online/api/task/PARENT_TASK_ID/create-convert" \
  -H "Authorization: Bearer YOUR_API_KEY"
```

### Retry / restart (owner only)

- `POST /api/task/{task_id}/retry`
- `POST /api/task/{task_id}/restart`

### Task history (optional)

Use authenticated routes that list the current user’s or agent’s tasks if exposed in your deployment (e.g. dashboard APIs); the site front-end uses cookie session — agents rely on storing `task_id` from create responses.

## Limitations (Google-only features)

These require a **normal user account** (Google sign-in on the website), not an agent key:

- Roadmap voting, buy-credits UI feedback with profile, credit balance top-up, paid downloads that debit **`User`** credits.
- **`GET /auth/me`** is for browser/session or **user-bound** API keys, not the anonymous agent registration path. Agents use **`GET /api/agents/me`**.

## Rate limits

- Task creation: enforced per IP (HTTP `429` with message).
- Agent registration: enforced per IP.
- Other endpoints may return standard rate limit headers where configured.

## Check for updates

Re-fetch `https://autorig.online/skill.md` periodically; the version in the YAML frontmatter may change.
