# Pinned binary deployment alternative

This source-only alternative runs the official LiveKit `v1.13.6` linux-amd64 release directly under `/srv/sandflow/voice`. It has no Docker dependency and therefore does not depend on the unrelated broken `docker.service` unit observed during preflight.

Nothing in this directory has been installed or run on `way-fr`.

Both native service examples use project-local `.work/tmp` and `.work/run` paths. Conservative candidate hard bounds are 200% CPU / 2 GiB / 512 tasks for LiveKit and 100% CPU / 512 MiB / 256 tasks for coturn. These are containment defaults, not capacity proof. Both write to the `sandflow-voice` journald namespace; the example namespace config uses volatile storage capped at 64 MiB with rate limits. All values require load verification.

## Pin and provenance

- Release: [LiveKit v1.13.6](https://github.com/livekit/livekit/releases/tag/v1.13.6), signed release commit `3cfbd12`.
- Official archive: `livekit_1.13.6_linux_amd64.tar.gz`.
- Official release SHA-256: `2b61abef2b9ba14b4b8ca38b37de9a37ffc682b9931d5fc03ceca2f0b77d3e33` from the release's `checksums.txt`.
- Installed versioned path: `/srv/sandflow/voice/bin/livekit-server-v1.13.6`.

The installation template downloads over HTTPS, checks the exact archive checksum before extraction, installs a versioned binary, and prints `--version`. It does not create credentials, copy certificates, install a unit, change firewall/nginx, or start LiveKit unless an operator deliberately runs and extends the reviewed steps.

## API and RTC binding

LiveKit's `bind_addresses` is global. Binding it only to `127.0.0.1` also binds RTC listeners to loopback, so the corrected base config omits it. The binary listens on the host; ordered IPv4 and IPv6 firewall rules must keep `7880/tcp` private while exposing `7881/tcp` and `7882/udp`. nginx may then proxy the existing `/sandflow/voice` WSS path to `127.0.0.1:7880` without giving the public internet direct RoomService access.

`rtc.node_ip` is pinned to the verified host address `37.187.57.177` with `use_external_ip: false`. Revalidate the host address immediately before deployment; a mismatch is a hard stop.

## TURN hostname and port finding

The embedded TURN listener does not technically require a distinct hostname. LiveKit `v1.13.6` validates that `turn.domain` is a valid domain, loads the configured TLS keypair, and listens on `turn.tls_port`; a certificate must match the configured domain. The already deployed public certificate includes `autorig.online`, so the same hostname can cryptographically serve `autorig.online:5349` while signaling remains `wss://autorig.online/sandflow/voice`. No new TURN subdomain is mandatory for that topology. A deployment copy of the certificate/key would need secure root-controlled renewal into the owning service's restricted child below `/srv/sandflow/voice/certs`; the service must never read or modify the Certbot private-key file directly without an explicit permissions design.

The layout deliberately keeps shared `config/` and `certs/` parents `root:root 0755` for traversal only. LiveKit files belong under `config/livekit` and `certs/livekit` (`root:sandflow-voice 0750`); coturn files belong under `config/coturn` and `certs/coturn` (`root:sandflow-turn 0750`). Neither installer changes the other service's child directory, so rerunning them in either order cannot revoke traversal or merge secret access.

However, the pinned official `v1.13.6` source constructs its **embedded** TURN ICE URL as `turns:{domain}:443?transport=tcp` even when `tls_port` is 5349. Therefore its embedded listener cannot satisfy this topology. The base config keeps embedded TURN disabled rather than pretend it works.

An alternative is now source-verified: `rtc.turn_servers` does honor explicit port 5349 and generates coturn-compatible time-limited HMAC credentials. See [the gated external coturn candidate](EXTERNAL_COTURN_5349.md) and its complete LiveKit/coturn example configs. This path does not wait for a future LiveKit release, but remains feature-closed until installation, peer-restriction, JoinResponse, relay, refresh, and load tests pass. Port 443 remains owned by nginx and is not changed.

This limitation reduces connectivity for restrictive TCP-443-only networks. The user-approved 5349 policy accepts that coverage limitation, but it does not waive the requirement that the configured port actually be advertised and tested.

Relevant official sources: [deployment and TURN guidance](https://docs.livekit.io/transport/self-hosting/deployment/), [ports and firewall](https://docs.livekit.io/transport/self-hosting/ports-firewall/), [v1.13.6 room-manager source](https://github.com/livekit/livekit/blob/v1.13.6/pkg/service/roommanager.go), and [v1.13.6 TURN listener source](https://github.com/livekit/livekit/blob/v1.13.6/pkg/service/turn.go).

## Pre-install validation checklist

1. Confirm architecture is `x86_64` and public IPv4 is still `37.187.57.177`.
2. Re-download official `checksums.txt` and require the archive hash above to match both the checked-in pin and upstream.
3. Inspect archive members before extraction; require exactly the expected `livekit-server` executable and no absolute/traversal paths.
4. Create a dedicated locked `sandflow-voice` system user with no shell/home and `/srv/sandflow/voice` ownership scoped to required paths.
   If external coturn is selected, create a different locked `sandflow-turn` user and run `prepare-coturn-layout.example.sh`; never share the LiveKit process UID.
5. Create `/srv/sandflow/voice/secrets/keys.yaml` out of band with mode `0600`, owned by `sandflow-voice`; never place credentials in the service unit, Git, process arguments, or logs.
6. Copy only the public certificate and required private key into the dedicated cert directory with least privilege and a reviewed Certbot renewal hook. Never print key contents.
7. Produce the runtime `livekit.yaml` from the base example and keep it out of Git if it contains paths or environment-specific values.
8. Validate strict parsing with the exact pinned binary on a staging namespace/host or temporary non-public ports. `v1.13.6` has no documented parse-only command, so do not run a production-port “validation” process alongside live services.
9. Inspect the startup log for exact API/RTC bind addresses, advertised node IP, version, and zero config errors; never log key values.
10. Prove public `7880` denial before enabling the nginx signaling route. Prove nginx loopback access separately.
11. Prove external ICE/UDP 7882 and ICE/TCP 7881 with real clients; a listening socket alone is insufficient.
12. Keep embedded TURN disabled. Enable the external coturn candidate only after explicit 5349 advertisement and authenticated relay candidates pass from an external UDP-blocked client.
13. Install the reviewed unit under a final name, run `systemd-analyze verify` on it, then start it only during an authorized deployment window.
    Also install and validate the namespaced journald cap before either native service can emit logs.
14. Set CPU/memory/log monitoring and abort thresholds before concurrency tests; the preflight resource snapshot was not a capacity proof.

## Service lifecycle and game-host departure

The dedicated unit owns only the SFU process. It must not require or restart Docker, AutoRig, rendering, database, or game backend services. A LiveKit process restart affects all voice rooms and therefore requires an explicit maintenance window.

Game-authority host departure is application state, not SFU lifecycle. Remove only the departed participant, transfer the game lease independently, and never stop this service or delete the LiveKit room while other participants remain.
