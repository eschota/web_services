# Firewall isolation requirements

These are design requirements, not an executable firewall script. Preserve the host's existing ordered policy and use its established firewall management source; do not append rules blindly.

For public IPv4 `37.187.57.177` and the host's public IPv6:

- accept established/related traffic and loopback first;
- allow `7881/tcp` to the public interface for ICE/TCP fallback;
- allow `7882/udp` to the public interface for the bounded single-port ICE UDP mux;
- deny new public traffic to `7880/tcp` on every public interface/address;
- allow local nginx to reach `127.0.0.1:7880`;
- keep existing nginx ownership of `443/tcp` unchanged;
- allow `5349/tcp` only when the gated external coturn configuration is enabled;
- do not generally expose coturn relay UDP 40000–40127: the candidate permits only the co-hosted SFU peer, and the same-host route must be proven first;
- apply the reviewed owner-matched OUTPUT policy for the dedicated `sandflow-turn` UID: relay source ports 40000–40127 may reach only `37.187.57.177:7882`; reject all other IPv4 destinations/ports and all IPv6 relay traffic;
- do not expose Prometheus, debug, pprof, API credentials, or admin endpoints publicly.

Required post-change checks from both the host and an external machine:

```text
127.0.0.1:7880        reachable by nginx/backend only
37.187.57.177:7880    rejected or timed out
37.187.57.177:7881    reachable while LiveKit runs
37.187.57.177:7882/udp produces a real ICE candidate in WebRTC tests
autorig.online:443    still serves the existing website and signaling path
autorig.online:5349   tested only after compatible TURN advertisement is proven
```

IPv4 and IPv6 must be tested separately. A public `7880` denial is a release gate, not a best-effort recommendation.
