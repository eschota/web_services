# Gated external coturn candidate for TLS 5349

Status: **source-supported candidate, not installed or deployment-validated**.

This avoids the embedded TURN `:443` advertisement limitation in LiveKit `v1.13.6`. Embedded `turn.enabled` stays false. Instead, `rtc.turn_servers` advertises a separate coturn service explicitly as:

```text
turns:autorig.online:5349?transport=tcp
```

## Verified compatibility

LiveKit `v1.13.6` official source defines external TURN entries with `host`, `port`, `protocol`, `secret_file`, and `ttl`. Its room manager constructs the ICE URL with the configured port and, when a shared secret is present, generates:

```text
username   = expiryUnix:participantId
credential = base64(HMAC-SHA1(sharedSecret, username))
```

Coturn's official `use-auth-secret` mechanism documents the same timestamp/user and HMAC-SHA1 format. Therefore the two components are protocol-compatible without putting static usernames/passwords in client or source configuration.

Sources: [LiveKit v1.13.6 config](https://github.com/livekit/livekit/blob/v1.13.6/pkg/config/config.go), [LiveKit external TURN ICE generation](https://github.com/livekit/livekit/blob/v1.13.6/pkg/service/roommanager.go), [coturn authentication and quota manual](https://github.com/coturn/coturn/blob/master/man/man1/turnserver.1), and [official coturn example config](https://github.com/coturn/coturn/blob/master/examples/etc/turnserver.conf).

TLS uses `autorig.online:5349`. The already observed public certificate SAN includes `autorig.online`, so a new TURN hostname/certificate is not mandatory. The runtime certificate/key must be copied into the dedicated service directory with least privilege and refreshed by a reviewed renewal hook; neither service should edit Certbot state directly.

## Preventing a general public relay

The candidate combines independent controls:

1. TLS-only client listener on 5349; anonymous/no-auth mode is not enabled.
2. Time-limited REST credentials issued by LiveKit only after a participant joins with a valid SandFlow token.
3. A shared secret supplied out of band to both components; no secret value exists in Git or service arguments.
4. `denied-peer-ip=0.0.0.0-255.255.255.255` plus `allowed-peer-ip=37.187.57.177`. Coturn documents that a specific allow overrides a denied range. Coturn ACLs are IP-only and cannot restrict the destination port, so this is not sufficient by itself.
5. A dedicated `sandflow-turn` Unix user and owner-matched OUTPUT chain restrict relay-source UDP 40000–40127 to exactly `37.187.57.177:7882`; every other IPv4 peer port is rejected. The different `sandflow-voice` UID is not matched, so LiveKit egress is unaffected.
6. IPv6 peers are explicitly covered by `denied-peer-ip=::-ffff:...:ffff`, while the owner-matched ip6tables rule rejects every coturn relay-range datagram. The kernel rule is authoritative: upstream coturn advisories document IPv4-mapped/native IPv6 ACL bypasses in affected versions, so absence of an IPv6 listener and coturn ACLs alone are not accepted controls.
7. Loopback peers remain disallowed by coturn's secure default; multicast peers are explicitly disabled.
8. Per-user allocation quota 4 and total quota 128. These are conservative candidate limits, not measured capacity; dual-peer/reconnect tests must prove 4 is sufficient before release.
9. Relay UDP ports are bounded to 40000–40127 and TCP relay endpoints are disabled. Do not generally expose this relay range at the public firewall: the only permitted peer is the co-hosted SFU. Verify the host's local route to its public IPv4 works before relying on this assumption.
10. No TURN CLI and no software banner. Both native units use a dedicated journald namespace with a candidate hard volatile cap of 64 MiB plus rate limits.

This restriction intentionally makes the coturn instance SandFlow-specific. It cannot be reused as a generic TURN service for unrelated products without a separate reviewed configuration.

## Credential lifetime

LiveKit's v1.13.6 external-TURN default is 14,400 seconds (4 hours); the candidate sets that value explicitly. Coturn's allocation lifetime candidate is 3,600 seconds and allocations are refreshed during an active WebRTC session. Reducing the credential TTL to match the five-minute join JWT could break long sessions when allocations refresh, because no tested client-side ICE credential rotation exists yet.

Four hours is a security trade-off: removing a participant does not instantly revoke an already issued TURN credential. Peer restriction and quotas limit what it can relay, but cannot make it individually revocable. Before production, test session lengths beyond one hour and decide whether backend-driven ICE restart/credential refresh supports a shorter TTL. Secret rotation invalidates all outstanding credentials and is an incident operation, not a per-user moderation tool.

## Required deployment gates

1. Install no package until the exact source/package choice is approved. `way-fr` currently has no coturn; Debian Bookworm reports candidate `4.6.1-1`. Pin the package version and verify its signed repository provenance and installed `turnserver --version` before enabling the unit.
2. Create separate locked `sandflow-voice` and `sandflow-turn` users, then use the reviewed layout templates. Generate one high-entropy shared secret out of band. Store the LiveKit copy at `/srv/sandflow/voice/secrets/turn-shared-secret` mode 0600; render an ignored `/srv/sandflow/voice/config/coturn/coturn.conf` mode 0600 owned by `sandflow-turn` containing the same value. Never print either.
3. Verify strict LiveKit config parsing with the exact pinned binary and verify coturn config syntax/version support without binding production ports.
4. Copy the matching `autorig.online` cert/key to `/srv/sandflow/voice/certs/coturn` with root-controlled renewal and `root:sandflow-turn` least privilege. Verify TLS hostname and expiry publicly without reading key content. Shared `certs/` remains a traversable `root:root 0755` parent and contains no files directly.
5. Keep nginx on 443 unchanged. Add only the reviewed 5349/TCP ingress rule plus existing LiveKit 7881/TCP and 7882/UDP rules; keep 7880 publicly denied.
6. Install the authoritative IPv4/IPv6 owner firewall rules before starting coturn. Verify the relay range is not generally exposed and that `sandflow-turn` can emit relay UDP only to `37.187.57.177:7882`; the same VPS on another UDP port, another public IP, RFC1918, loopback, multicast, IPv4-mapped IPv6, native IPv6, and translated IPv6 paths must fail.
7. Decode a real LiveKit JoinResponse and require an ICE URL exactly equal to `turns:autorig.online:5349?transport=tcp`, with a temporary `expiry:participantId` username. Reject any `:443` result.
8. Use `turnutils_uclient` and real Chrome/Firefox/native clients from an external UDP-blocked network; require relay candidates and two-way audio, then repeat after allocation refresh.
9. Load-test concurrent allocations and bandwidth while observing LiveKit, coturn, existing host CPU/network, packet loss, and file-descriptor use. Adjust quotas only from evidence.
10. Confirm game-host departure removes only that identity: remaining users retain their room and TURN allocations/audio.
11. Verify expired credentials cannot create/refresh allocations and that a non-admitted client never receives TURN credentials.
12. Keep the feature flag closed and embedded TURN disabled until every gate passes.

## Remaining limitations

- No coturn package or binary is installed; exact runtime behavior on `way-fr` is unverified.
- 5349 remains less reachable than TURN/TLS 443 on restrictive networks, as already accepted.
- The same-host public-IP relay path and firewall ordering require real packet tests.
- Coturn has no native peer-port ACL. Security depends on the separate UID plus correctly ordered kernel OUTPUT rules; these were syntax-checked read-only but not applied or behavior-tested.
- Coturn IPv6 peer ACL handling has affected-version advisories; the candidate therefore rejects relay-range IPv6 at the kernel layer. A package without a reviewed security-fix status remains blocked.
- Four-hour credentials are not individually revocable. Admission, mute, kick, peer restrictions, and quotas are separate controls.
- The current web/native clients have not been tested with this external ICE server configuration.
