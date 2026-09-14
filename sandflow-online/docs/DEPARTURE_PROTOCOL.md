# Transactional room departure (capability 1)

Physics stays on clients. The server freezes ordering and stores validated client state; it never simulates a final step.

1. A ready authority sends `depart_begin` on its existing control WSS with its current epoch/tick/confirmed sequence and a new operation ID. This is ordered after that socket's commits. Server captures the final accepted input sequence and permits only the bounded final step range (at most 256 commands/step).
2. New admissions are rejected with `world_handoff`; autojoin skips the room. Existing participants receive `depart_pending`. Already-in-flight input receives an explicit, rate-limited `input_rejected`, not a forced disconnect. The authority drains accepted commands and captures a fresh snapshot; followers can receive it through the state WSS.
3. Authority POSTs the snapshot to `/api/v1/worlds/{world}/departures/{operation}`. The server validates role, epoch, fence, expected revision, encoded payload hash and full existing snapshot codec. Snapshot version and identity-bound completion receipt are committed in the same SQLite transaction, after the immutable blob is durable.
4. Only then is the departing authority removed. A ready follower may take over; otherwise remaining participants recover the newly saved cloud checkpoint. The same control socket can remain connected. Receipt GET/identical POST retry still works after departure or process restart; a mismatched replay or another identity is rejected.

The final durable `saved` record is broadcast BEFORE authority changes. `host` and `depart_prepared` also include an explicit known revision. A stale autosave is never accepted blindly: it receives the existing durable revision and a conflict response, allowing a correct retry. Real browser handoff exposed this requirement when the next host initially retained the preceding revision and its later autosaves failed.

The receipt contains no credentials. Retention is at most eight receipts/world and 24 hours. Departure preparation has a 60-second deadline, a 10-second attempt cooldown and the normal eight-second renewable authority lease. Abort/timeout clears the freeze. Corruption, stale epoch/fence or revision conflict cannot promote a new version. The legacy `X-SF-Final` bypass is rejected with `departure_required`; normal minute-limited autosaves remain supported.

Snapshot HTTP processing is restricted to two in-flight bodies through storage completion, each capped at 64 MiB and a 30-second read deadline. Readers allocate one announced-length body, reject truncation/overflow and release capacity exactly once. Unauthorized followers are rejected before body allocation. These limits are not a measured 100-player capacity claim.

`POST /api/v1/worlds/{world}/leave` removes an ordinary follower, but refuses to drop the current host without the final-save workflow. A sole unsynchronized peer is not presented as safely saved.

## Client contract

The new client checks `welcome.data.departureProtocol`. In-game save/leave and world switching await confirmation; Windows normal close uses `Application.wantsToQuit`. Failure keeps the client open with an error and a separately held force-leave action. Uncertain completion is retained in memory and queried again on retry even when WSS has already closed. New world operations and abandoned HTTP/capture callbacks are generation-fenced.

Browser tab close, OS termination and forced exit remain abrupt; they cannot be advertised as an asynchronous final-save guarantee. They may lose changes after the last successful save, as in the approved plan. Existing capture coverage is still incomplete for all prototype settings/actors; a successful departure does not prove full-schema persistence.

## Evidence

In-process tests cover pending-input fencing (including 600 commands), freeze admission, soft input rejection, corruption, idempotent completion after disconnect/restart, follower takeover, abort/timeout, body size/capacity and legacy bypass rejection. Unity build checks bind completion receipt identity/cursor/revision/hash. `tools/DepartureLiveProbe` exercises production HTTP and both WSS channels with an isolated private **CPU fixture**, not Unity gameplay. Actual Windows close/menu, mixed-client recovery, complete schema and voice continuity still require live client acceptance.
