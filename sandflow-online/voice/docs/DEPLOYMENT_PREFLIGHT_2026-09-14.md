# SandFlow LiveKit read-only deployment preflight — 2026-09-14

Status: **not ready to deploy**. Evidence was collected read-only over `ssh -o BatchMode=yes way-fr`; no files, firewall rules, DNS, certificates, containers, services, credentials, nginx configuration, or running processes were changed.

Post-preflight source remediation: the later binary template under `deploy/binary/` removes Docker as a dependency, removes the unsafe global loopback bind, and pins the official archive/checksum. Embedded TURN remains disabled. A separately gated coturn candidate uses v1.13.6 `rtc.turn_servers`, which source review confirmed advertises explicit port 5349 with time-limited shared-secret credentials. This does not change the host readiness status; no coturn package is installed and no relay test has run.

## Verified host state

Snapshot time: 2026-09-14 00:42 UTC.

| Item | Verified result |
|---|---|
| SSH target identity | `way.qwertystock.com`, public IPv4 `37.187.57.177`; `autorig.online` resolves to that IPv4 |
| Kernel / CPU | Debian-family Linux kernel `6.1.0-21-amd64`, x86_64, 24 logical CPUs |
| Load snapshot | load average `5.85 / 5.94 / 5.65`; several existing Python/PostgreSQL workers were consuming substantial CPU, so spare realtime capacity is not established |
| Memory | 33.5 GB total, 20.1 GB reported available; 13.6 GB of 18.3 GB swap was in use |
| Root disk | 941.7 GB total, 835.1 GB used, 97.0 GB available, 90% used; inode use only 7% |
| Docker | daemon `27.3.1`, Compose `2.29.7`, 8 containers / 6 running; LiveKit `v1.13.6` image is not present |
| Docker service health | daemon currently runs, but systemd reports `docker.service` as `Loaded: bad-setting`; its unit has an unknown lowercase `[service]` section and is reported without a valid `ExecStart` |
| Existing HTTPS | nginx owns TCP 443 on both IPv4 and IPv6 |
| Existing certificate | public certificate for `autorig.online` exists and is valid 2026-08-24 through 2026-11-22; SANs are only `autorig.online` and `www.autorig.online` |
| Certificate renewal | `certbot.timer` is active and enabled |
| TURN identity | `turn.sandflow.autorig.online` has no observed A record/certificate, but later source review confirmed it is not mandatory: the existing certificate covers `autorig.online`, which can be the TURN domain on a distinct port |
| Candidate checkout | `/root/sandflow-online` and the example unit's `/root/sandflow-online/voice/deploy` working directory are absent; `/srv/autorig` exists |

No secret values, private keys, tokens, environment values, or certificate key contents were read.

## Port and firewall evidence

`ss -lntup` showed no listener on any candidate LiveKit port:

| Port | Listener state | Firewall state |
|---|---|---|
| `7880/tcp` API/signaling | free | must remain non-public or explicitly rejected externally |
| `7881/tcp` ICE fallback | free | no IPv4 ACCEPT rule |
| `7882/udp` ICE UDP mux | free | no IPv4 ACCEPT rule |
| `5349/tcp` TURN/TLS candidate | free | no IPv4 ACCEPT rule |
| `3478/udp` optional TURN/UDP | free | not present in the current SandFlow template |
| `443/tcp` | occupied by nginx | existing production web traffic; must not be rebound by LiveKit |

IPv4 iptables has a default `INPUT DROP` policy. Explicit read-only `iptables -C` checks confirmed missing ACCEPT rules for `7881/tcp`, `7882/udp`, and `5349/tcp`. UFW is unavailable and no relevant nftables output was observed. Provider/upstream firewall state was not inspectable from the machine and remains unverified.

LiveKit's current official port reference describes `7881/tcp` as ICE/TCP fallback, optional UDP mux on `7882`, and TURN/TLS on `5349`; exposed media ports must be admitted by the firewall: [LiveKit ports and firewall](https://docs.livekit.io/transport/self-hosting/ports-firewall/).

## Candidate configuration review

The checked-in examples are a useful starting point but are **not deployment-ready**:

1. `bind_addresses: [127.0.0.1]` is a global LiveKit bind setting, not an API-only bind. With the current host-network Compose template it would also keep ICE/TCP and ICE/UDP on loopback, contradicting the requirement that `7881/tcp` and `7882/udp` be publicly reachable. The production design must either:
   - omit the global loopback bind, bind LiveKit on the host, and enforce non-public `7880` with IPv4/IPv6 firewall rules while nginx proxies it locally; or
   - provide a separately validated network architecture that exposes media directly while keeping the API private.
2. The example `sandflow-voice.service` points to a working directory that does not exist on this host.
3. The pinned image has not been downloaded or inspected on the host. Image retrieval, digest verification, config validation, and an offline container smoke test remain undone.
4. No real `livekit.yaml`, API key/secret, TURN certificate, or webhook URL exists. Creating any of these was outside this preflight.
5. The Docker daemon is currently usable, but the broken systemd unit is a deployment blocker: a machine reboot or Docker restart cannot be assumed safe. This must be repaired and validated separately without disrupting the six running containers.
6. The host's UDP receive/send maxima are both 64 MiB, above the value mentioned in common LiveKit warnings, but no LiveKit-specific socket-buffer, packet-loss, NIC, or sustained bandwidth test has been run.
7. Root storage has 97 GB free but is already at 90% use. LiveKit itself is not storage-heavy without recording, yet image/log growth must have explicit rotation and disk-pressure monitoring. No Egress/recording service is proposed.

The official deployment guide recommends host networking for Docker, requires a trusted TLS domain for signaling and a separate domain/certificate for TURN, and states that capacity is CPU- and bandwidth-bound: [Deploying LiveKit](https://docs.livekit.io/transport/self-hosting/deployment/). The present memory snapshot is therefore not proof of safe concurrent voice capacity; a bounded load test under existing CPU workloads is mandatory.

## TURN/TLS without taking port 443

The existing certificate SAN permits `autorig.online:5349`; TLS identity is hostname-based, not port-based. LiveKit's `v1.13.6` TURN source requires a valid configured domain and loads the configured certificate/keypair, but does not require a distinct signaling/TURN hostname. Therefore signaling can remain `wss://autorig.online/sandflow/voice` and no new TURN subdomain is inherently required.

This still does **not** provide the broadest corporate-firewall fallback. LiveKit's official deployment guide states that without a load balancer, TURN/TLS must be advertised on port 443; 5349 may be blocked by restrictive networks: [TURN/TLS deployment guidance](https://docs.livekit.io/transport/self-hosting/deployment/#turn-tls).

More importantly, source verification found that pinned official `v1.13.6` starts its embedded TURN listener on configured 5349 but hard-codes the JoinResponse ICE URL to `turns:{domain}:443`. Embedded TURN on 5349 must remain disabled. The separately reviewed external-coturn candidate uses `rtc.turn_servers`, whose v1.13.6 source does include the configured port in the ICE URL; it remains feature-closed until real authentication, peer restriction, refresh, and relay tests pass.

On this single existing IP, nginx already owns `0.0.0.0:443` and `[::]:443`. The exact choices are therefore:

- after resolving the `v1.13.6` advertisement blocker, accept TURN/TLS on `autorig.online:5349` with reduced restrictive-network coverage; or
- provision a separate public IP/L4 endpoint that can offer `turn.sandflow.autorig.online:443`; or
- deliberately redesign the existing 443 front door around validated L4 SNI routing, which is high-risk and must not be attempted as an incidental SandFlow change.

The current requirement says not to occupy 443, so the bounded existing-IP candidate is `autorig.online:5349` with its limitations documented. The existing certificate can be securely copied/renewed into the dedicated service path; it still requires inbound firewall/provider rules, a compatible advertised ICE URL, and real external ICE/TURN tests from UDP-blocked networks.

## Exact prerequisites before any deployment

1. Confirm the intended deployment path on `way-fr`; update the example unit only after that path exists.
2. Repair and safely validate `docker.service` while preserving existing containers; do not restart it blindly.
3. Use `autorig.online` as the bounded 5349 TURN domain with a least-privilege copy/renewal design for its existing certificate; a new subdomain is optional, not mandatory.
4. Keep embedded TURN disabled on pinned `v1.13.6`. The external coturn 5349 candidate may proceed only after its exact package, shared-secret handling, peer restrictions, JoinResponse URL, allocation refresh, and relay behavior are validated. The already-approved 5349 policy accepts reduced corporate-firewall reach.
5. Correct the LiveKit bind/API isolation design; prove 7880 is unreachable publicly while nginx can reach it locally, and prove 7881/TCP plus 7882/UDP bind to the public interface.
6. Add narrowly scoped IPv4 and IPv6/provider firewall rules for the chosen ports. Current IPv4 rules block all three required public ports.
7. Create server-only API credentials and ignored runtime config with safe permissions; do not commit or log values.
8. Add the signed webhook endpoint and durable event-id idempotency before relying on kick/mute/reconnect enforcement.
9. Pull and verify the pinned image digest, validate configuration syntax against that exact image, and run a non-public smoke check before exposing ports.
10. Establish CPU/network capacity with bounded synthetic rooms while monitoring existing services. Define abort thresholds for CPU, load, packet loss, memory, and latency.
11. Configure log limits and resource limits; verify no Egress/recording or raw voice persistence is enabled.
12. Run real cross-client ICE tests: direct UDP, ICE/TCP, TURN/TLS 5349, UDP-blocked network, reconnect, and certificate renewal/reload.

## Game-host departure invariant

Voice-room lifetime must not be tied to the current game-authority host. When the game host leaves:

- remove/disconnect only that participant's voice identity;
- transfer the game simulation lease through the parent backend independently;
- do not call LiveKit `DeleteRoom`, do not rotate the team room name, and do not disconnect remaining participants;
- allow the LiveKit room to remain until its last participant leaves.

LiveKit documents that a room is the shared container for its participants and closes after the last participant leaves, not when an application-designated game host departs: [LiveKit room management](https://docs.livekit.io/intro/basics/rooms-participants-tracks/rooms/). This invariant still needs an integration test with at least three participants; it was not exercised during this infrastructure preflight.

## Verification boundary

Verified here: host identity, snapshot resources/load, listeners, local IPv4 firewall rules, Docker/Compose presence and broken unit state, pinned-image absence, certificate names/public SANs, DNS absence for the TURN name, and missing example working directory.

Not verified: provider firewall, sustained bandwidth, IPv6 exposure policy, LiveKit startup/config parsing, DNS propagation after changes, certificate issuance for TURN, nginx WebSocket proxy behavior, ICE candidate advertisement, TURN relay, browser/native audio, concurrency, failover, or host-departure behavior. No readiness or gameplay claim should be made from this report.
