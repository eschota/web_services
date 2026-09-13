# LiveKit control-plane integration contract

This contract covers backend-only LiveKit administration and webhook verification. It does not expose LiveKit API credentials to Unity clients and does not claim that production event enforcement is instantaneous.

## Administration client

Construct one `LiveKitAdminClient` with explicit `LiveKitAdminOptions`, an injected long-lived `HttpClient`, and `TimeProvider.System`. Production may use the loopback endpoint `http://127.0.0.1:7880`; any non-loopback endpoint must use HTTPS. Configuration is disabled by default and validation fails closed.

The backend must construct `VoiceControlTarget` only from authenticated membership state:

```csharp
var target = new VoiceControlTarget(
    sandbox.Id,
    authenticatedPlayer.Id,
    sandbox.IsPvp ? VoiceMode.Pvp : VoiceMode.Coop,
    serverAssignedTeamId);

await admin.MutePublishedTrackAsync(target, microphoneTrackSid, muted: true, cancellationToken);
await admin.SetMicrophonePermissionAsync(target, canPublish: false, cancellationToken);
```

`VoiceControlTarget` accepts a sandbox ID, not an arbitrary LiveKit room name. The client derives exactly `sf:{id}:all` or `sf:{id}:team:{0|1}` and signs a one-minute, room-scoped `roomAdmin` JWT for each call. It supports only the official Twirp operations:

- `UpdateParticipant`: preserves subscribe permission, disables data publication, and restricts any restored publication to `MICROPHONE`.
- `MutePublishedTrack`: requires a validated LiveKit `TR_...` track SID.
- `RemoveParticipant`: disconnects the exact trusted participant from the exact derived room.

Recommended mute order is track mute first, then `canPublish=false`; a kick is optional for membership revocation. Always persist authoritative admission/mute state before issuing control calls. Retry failed idempotent mute/permission operations with a bounded policy in the parent backend.

**Self-hosted safety boundary:** `RemoveParticipant` and permission changes must not be treated as permanent revocation of an already-issued join JWT. LiveKit documents that self-hosted deployments do not invalidate the existing token. The SandFlow backend must refuse fresh publishing tokens while muted/revoked, use short token TTL, and recheck authoritative state on every join, reconnect, token refresh, team change, and `track_published` event. A stale token may still create a brief enforcement window; the current unbuffered low-latency architecture cannot honestly promise zero leakage.

## Webhook verifier

The HTTP endpoint must pass the untouched request bytes and the full LiveKit authorization-header value to `LiveKitWebhookVerifier.Verify`. Do not parse and reserialize JSON before verification.

The verifier requires:

- HS256 with the configured LiveKit API key and secret;
- valid `iat`, `nbf`, and `exp` within the bounded age/skew policy;
- the signed standard-base64 `sha256` claim matching SHA-256 of the exact raw body;
- a valid LiveKit event envelope containing `id`, `event`, and `createdAt`;
- a non-replayed JWT (`jti`, or token digest when `jti` is absent).

It accepts the raw JWT format used by LiveKit SDK receivers and also tolerates a conventional `Bearer ` prefix. Bodies are capped at 256 KiB by default. Replay state is in-memory, bounded, and valid only for the life of the process; if the cache fills with unexpired entries, verification fails closed instead of evicting an entry and reopening a replay window.

LiveKit retries webhooks and does not guarantee delivery. The parent endpoint therefore needs durable idempotency by webhook event `id`: persist/claim the event ID before applying a membership transition, return success for an already-completed event, and distinguish a legitimate retry from a new event. The in-memory JWT replay cache is an additional request-authentication defense, not a substitute for durable event idempotency.

For enforcement webhooks:

1. Verify signature and raw-body hash.
2. Check durable event-ID idempotency.
3. Resolve participant identity and room back to authoritative SandFlow membership; never trust webhook metadata as the sole team/admission source.
4. On `participant_joined` and `track_published`, check mute/revocation state and apply `MutePublishedTrack` plus `UpdateParticipant(canPublish=false)` when required.
5. Commit event processing and respond successfully.

If authoritative membership/mute storage is unavailable, permission activation must fail closed: issue `canPublish=false` initially and enable microphone publication only after the backend confirms admission and mute state. This narrows but does not eliminate the asynchronous interval between media events and a later administrative call. Strict zero-window prevention would require a different pre-publication media architecture.

## Configuration and ownership

Admin and webhook credentials remain server-only. Use the same configured LiveKit key pair or a dedicated server-side key registered in LiveKit; never log JWTs, authorization headers, API secrets, or raw webhook bodies. The parent service owns HTTP endpoint routing, durable idempotency, authoritative state, retries, metrics, and shutdown/disposal. This library owns signing, target validation, Twirp request shape, signature/body verification, expiry checks, and bounded in-process replay rejection.
