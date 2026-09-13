# SandFlow communication moderation analysis

Status: approved product direction for implementation planning. This document changes no runtime, tests, deployment, or existing AutoRig service.

## Decision and guarantee boundary

SandFlow will use **reactive moderation with fast, unbuffered voice**. Player microphone audio continues to travel through the normal team/co-op LiveKit room while a trusted moderation participant subscribes to the same microphone track for VAD and ASR. When an accepted Russian or English violation is detected, the account loses both voice publication and chat sending for 60 seconds. Gameplay and receiving team voice continue.

This mode cannot satisfy a literal “nobody hears the profanity” guarantee: audio is published immediately, so the triggering word or phrase can reach listeners before ASR returns a decision. The chosen product trade-off is low conversational latency rather than pre-publication censorship.

The only architecture capable of guaranteeing that rejected audio is never heard would be a mandatory moderation relay:

1. Players publish into private ingress rooms that no player may subscribe to.
2. A trusted worker decodes and buffers every track, performs VAD/ASR, and republishes only accepted audio under a trusted identity into the team output room.
3. Rejected buffered segments are discarded or replaced with a tone.

That alternative adds perceptible utterance delay, decode/ASR/re-encode cost, speaker remapping, another media failure point, and substantially more CPU/bandwidth. It is **not selected** for the MVP and must not be implied by UI or marketing.

## Recommended CPU-only MVP

### Recognition pipeline

- Run a dedicated moderation worker as a hidden, subscribe-only LiveKit participant. Game clients never receive its credentials and never receive `roomAdmin` grants.
- Subscribe only to `microphone` tracks. Decode to mono 16 kHz PCM in memory and process per stable account identity, never by display name or transient participant SID.
- Use a lightweight VAD before ASR to avoid transcribing silence. `whisper.cpp` officially supports Silero VAD and exposes thresholds, minimum speech/silence duration, padding, and overlap; its streaming example is explicitly described as a basic/naive real-time example, so production latency and accuracy must be measured rather than assumed. Sources: [whisper.cpp VAD](https://github.com/ggml-org/whisper.cpp#voice-activity-detection-vad), [streaming example](https://github.com/ggml-org/whisper.cpp/blob/master/examples/stream/README.md).
- Recommended first benchmark: `whisper.cpp` with a pinned multilingual `base` or `small` model, CPU-only, short overlapping speech windows. It offers one multilingual decoder for Russian/English and code-switching, avoiding two completely separate recognizers. Do not use an English-only `.en` model.
- Benchmark fallback: Vosk with pinned small Russian and US-English models. Vosk is offline, exposes a streaming API and reconfigurable vocabulary, and publishes Russian and English models, but a dual-recognizer route doubles inference work and code-switched/clipped speech still needs corpus testing. Sources: [Vosk API](https://github.com/alphacep/vosk-api), [official model catalogue](https://alphacephei.com/vosk/models).
- `faster-whisper` is a useful comparison implementation because it provides word timestamps and integrated Silero VAD, but its usual strength is CTranslate2 GPU/batched inference. It should not be chosen for this host without a CPU benchmark against the same clips. Source: [faster-whisper README](https://github.com/SYSTRAN/faster-whisper/blob/master/README.md).
- No paid speech API and no new GPU is required for the MVP. The existing host has 24 logical CPUs and 31 GB RAM, but these are shared with existing services; no concurrency or latency promise is valid until isolated load tests establish a safe CPU quota.

### Detection policy

ASR output is evidence, not a direct substring trigger. The detector should:

1. Unicode-normalize and case-fold text, normalize `ё/е` only in the matching representation, collapse deliberate separator repetition, and tokenize by language-aware word boundaries.
2. Match whole normalized tokens and curated multi-token patterns. Never use unrestricted substring matching: a banned fragment inside an innocent word must not trigger punishment.
3. Maintain explicit Russian and English lexicons containing inflections and common obfuscations. Keep exact-match high-confidence terms separate from ambiguous context-dependent terms.
4. Accept an automatic violation only when both ASR confidence/stability and rule confidence meet a conservative threshold. Partial/unstable hypotheses may start a candidate but must not punish until stabilized or repeated.
5. Evaluate an overlapping context window so clipped words across ASR chunks are detectable. Deduplicate the same acoustic occurrence across overlapping windows using account ID plus word timestamps/audio interval.
6. Treat Russian/English code-switching as normal. Do not force one language for a whole session; test mixed phrases, accents, game terminology, packet loss, laughter, music, and clipped push-to-talk edges.
7. Do not create an automatic “quotation exception.” ASR cannot reliably determine quotation, irony, reclaimed language, or who is being addressed. Ambiguous/context-sensitive items should log a non-punitive candidate or require repetition; only the narrow high-confidence list should auto-mute.
8. Version the rules/model and include those versions in minimal violation metadata so regressions can be investigated without retaining speech.

False positives are more damaging than occasional misses for a 60-second automatic sanction. Initial production rules should therefore be deliberately narrow and expanded only from an opt-in/private validation corpus with labelled Russian, English, and mixed-language game chat.

### Chat moderation and anti-spam

Chat must be moderated synchronously **before broadcast**, so rejected text is never delivered. Apply the same token-aware profanity rules to the canonical server-side message representation; clients cannot submit a pre-approved flag.

Use separate server-side anti-spam signals:

- Per-account token buckets for message attempts and accepted messages, with a small burst allowance and a minute window.
- Duplicate/near-duplicate detection after normalization, including repeated punctuation/emoji and copy-pasted messages.
- Room-level fan-out accounting so switching team/world/private channels does not reset limits.
- Link and mention bursts as independent signals rather than assuming every short message is spam.
- Reject oversized messages before normalization and bound all moderation work.

Exact limits must be calibrated from playtests; suggested starting candidates, not promises, are a burst of 5 messages per 10 seconds, 20 accepted messages per minute, and a violation after 3 near-duplicates within 15 seconds. A single rate-limit overflow may be dropped with feedback; repeated/obvious flooding becomes an accepted communication violation and starts the shared 60-second mute.

Voice anti-spam should use VAD-derived speech duty cycle and repeated reconnect/publish churn, not raw packet rate (the codec naturally emits packets continuously). Suggested test candidates are warning after unusually long uninterrupted transmission and sanction only after sustained/repeated abuse. Normal conversation, accessibility use, background noise, and push-to-talk key bounce must be represented in the validation set before thresholds ship.

## Enforcement contract

Maintain one authoritative moderation record keyed by immutable SandFlow account/Steam identity, not room, participant SID, connection, device, IP, or channel:

```text
CommunicationMuteState {
  accountId
  mutedUntilUtc
  reasonCode              // PROFANITY_VOICE, PROFANITY_CHAT, SPAM_VOICE, SPAM_CHAT
  ruleVersion
  modelVersion?           // present for ASR decisions
  occurredAtUtc
  source                  // voice or chat
}
```

Raw audio and transcripts remain ephemeral in memory and are discarded after the decision window. Persist only the minimal violation record above; do not store the matched utterance by default. Operational metrics should contain counts and timing distributions, not transcript fragments.

On each **accepted** violation, atomically set:

```text
mutedUntilUtc = max(currentMutedUntilUtc, nowUtc + 60 seconds)
```

Thus a new accepted offence during a mute extends it to 60 seconds from the new offence. Duplicate ASR hypotheses for the same acoustic interval are one offence and must not repeatedly extend the timer.

While muted:

- Chat API rejects sends before broadcast.
- Voice-token issuance returns `canPublish=false`.
- The moderation/control worker immediately calls LiveKit `MutePublishedTrack` and then `UpdateParticipant` with `canPublish=false`; revoking `CanPublish` automatically unpublishes current tracks. LiveKit documents both operations and requires a server-held `roomAdmin` grant. Source: [LiveKit participant management](https://docs.livekit.io/intro/basics/rooms-participants-tracks/participants/).
- Keep `canSubscribe=true`: the player still hears teammates unless separately blocked for another reason.
- Never enable remote unmute. A client mute button cannot override authoritative state.
- Every join, reconnect, room/team/world-channel change, token refresh, and track-published event rechecks the account-scoped mute before allowing publication.

Self-hosted LiveKit does not invalidate the already-issued JWT when a participant is removed or permissions change. Short token TTL helps but is not sufficient for a 60-second rule because a still-valid token can reconnect. LiveKit explicitly requires the application backend to stop issuing tokens for removed participants in self-hosted deployments. Source: [LiveKit tokens and grants](https://docs.livekit.io/frontends/reference/tokens-grants/). Therefore defense in depth is mandatory:

1. backend token denial/read-only token during the mute;
2. server-side permission revocation and track mute for connected participants;
3. room event enforcement that immediately revokes any newly published track from a muted account;
4. the moderation subscriber discards muted-account audio even if a stale token briefly republishes.

The fourth layer prevents the ASR worker from processing abuse but, in the selected unbuffered architecture, other clients might receive a very short stale-token bypass before server enforcement. If even that transient leak is unacceptable, only the unselected pre-publication relay provides the required hard boundary.

## Failure policy and capacity

- **Voice ASR unavailable or overloaded:** voice remains available (fail-open), because fast unbuffered voice is the selected product mode. Emit a health alert and expose moderation-degraded state to operators; do not falsely present the channel as profanity-free.
- **Authoritative mute store/control path unavailable:** fail closed for chat send and new voice publication/token issuance, because otherwise a known muted identity can bypass punishment. Existing voice may remain until the control worker acts; alert immediately.
- **Chat moderation unavailable:** fail closed for public/team chat send with a temporary-unavailable response; gameplay continues.
- Bound per-track queues. Drop stale analysis frames rather than letting ASR latency grow without limit. Queue drops increment degraded-mode metrics.
- Isolate worker CPU/memory and lower its scheduling priority so it cannot starve the game/lobby, LiveKit, nginx, or existing AutoRig workloads. No deployment is justified until a production-like offline load test produces a safe concurrency cap.

Required measurements include real-time factor per concurrent speaker, detection latency from end of matched word, false-positive/false-negative rates by language and code-switch category, VAD speech/noise errors, memory per stream, CPU saturation behavior, queue age/drop rate, and time from accepted violation to track unpublish. Report percentiles, not averages alone.

## Acceptance scenarios

- High-confidence English and Russian violations trigger one shared 60-second voice/chat mute.
- Mixed Russian/English, inflected Russian, common obfuscation, and a word split across two ASR windows are detected without double sanction.
- Innocent words containing banned character sequences do not match; quoted or ambiguous phrases from the context list do not auto-sanction.
- The first triggering voice phrase may be audible; subsequent publication is revoked promptly and this limitation is visible in product documentation.
- A muted player cannot send chat or publish voice by reconnecting, refreshing a token, changing team/room/channel, creating a new participant SID, or toggling local mute.
- A muted player can keep playing and listening to allowed team voice.
- A second distinct accepted offence updates the deadline to 60 seconds from that offence; duplicate partial/final ASR results do not.
- Chat profanity is blocked before any recipient receives it.
- Voice spam uses VAD/speech behavior; chat spam uses message rate and normalized duplicates.
- Restarted moderation workers recover active mute state from the authoritative store without raw audio or transcripts.
- ASR outage follows the documented fail-open voice policy; mute-store outage follows fail-closed publication/chat policy.

## Implementation boundary

This analysis authorizes a later moderation implementation but does not itself add ASR packages, models, lexicons, runtime services, LiveKit admin credentials, database migrations, player reports/appeals UI, or deployment changes. Model files and profanity corpora require separate provenance/licence review and must not be downloaded into production implicitly.
