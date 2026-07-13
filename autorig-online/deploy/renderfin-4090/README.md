# AutoRig Renderfin RTX 4090 edge

The authoritative ComfyUI origin is `127.0.0.1:8188` on `DESKTOP-QTG6T29`.
The Windows tunnel exposes it only as Way loopback `127.0.0.1:19409`; nginx is
the authenticated public edge at `worker-4090.renderfin.qwertystock.com`.

The SSH key is restricted to `permitlisten="localhost:19409"` and has no shell.
Stateful jobs remain pinned to this worker URL, workflow fingerprint and prompt
ID. The root Renderfin health pool is not used for prompt submission or polling.

Deployment order:

1. Install the restricted public key with `install_restricted_tunnel_key.py`.
2. Copy `tunnel-4090.ps1` beside the runtime key and start its scheduled task.
3. Add the DNS A record through `qwertystock_domain_api` using
   read → backup → preview → matching-zone-hash apply → re-read.
4. Install the HTTP-only nginx site and run `nginx -t`/reload.
5. Expand `renderfin-workers.qwertystock.com` with the new SAN using the
   existing webroot challenge.
6. Replace the HTTP-only site with `worker-4090.nginx.conf`, validate/reload,
   then smoke `/edge-health`, authenticated `/health/4090`, upload and prompt.
