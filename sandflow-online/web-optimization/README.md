# SandFlow WebGPU bootstrap candidate

This directory is an isolated integration candidate. It is not deployed and does not prove that the currently public client is multiplayer-capable.

## Build facts verified from the actual source

`OnlineClientBuild.Web()` currently builds `Assets/SandFlowOnline/Scenes/OnlineBeach.unity` to `Builds/online-web`, forces WebGPU, Brotli, decompression fallback **on**, threads **off**, and Unity data caching **on**. The candidate therefore uses Unity filename macros and serves `.unityweb` fallback payloads verbatim without `Content-Encoding`. It does not assume the older `web.*` filenames.

## Parent integration contract

1. Copy `template/` to `Game/Assets/WebGLTemplates/SandFlowOnline/` and set `PlayerSettings.WebGL.template = "PROJECT:SandFlowOnline"` inside the Web branch of `OnlineClientBuild` before building.
2. Keep `decompressionFallback=true` unless a separate browser-tested build disables it. With the current setting, do not add `Content-Encoding` to `.unityweb`; the Unity loader performs Brotli fallback decompression. Consequently the fallback container can remain `application/octet-stream` rather than pretending the compressed bytes are directly stream-compilable wasm. Use the revalidation rules in `delivery-rules.nginx.conf` and replace its release-root placeholder with the verified new release path.
3. The template passes only these Unity arguments:
   - `-sandflowWorldId <32-hex-guid>` for `/sandflow/s/{id}`.
   - `-sandflowPreview` when `mode: "preview"`.
4. `SandFlowRoomClient` must parse `-sandflowWorldId` using `Guid.TryParseExact(value, "N", ...)` into `requestedWorld`. It must treat `-sandflowPreview` as non-networked preview and must not create a session, join a room, or open sockets. Unknown/missing/duplicate argument values must fail closed.
5. Never pass room passwords, bearer tokens, Steam IDs, or auth data through the URL or Unity arguments. Passwords remain POST bodies entered in the in-game UI. The bootstrap refuses URLs containing password/token/auth-like query or fragment keys and does not log the URL.
6. Default candidate mode is `preview`, visibly labelled “Preview — not connected to a live room”. Change to `live` only after the new client/backend admission and browser flow are independently validated.
7. `/sandflow/s/{id}` must serve this same index with HTTP 200. The page has `<base href="/sandflow/">`, so Build/TemplateData assets remain rooted correctly.

Unity's official documentation describes `.unityweb` as the decompression-fallback naming/delivery path and warns that fallback is less efficient than native browser decompression. A future native-Brotli candidate must be a separately built/browser-tested change; do not add `Content-Encoding: br` opportunistically to this fallback contract: https://docs.unity3d.com/6000.0/Documentation/Manual/webgl-deploying.html

Run deterministic checks with:

```powershell
pwsh -File tests/Test-Candidate.ps1
```

The checks validate deep-link parsing, sensitive URL rejection, configuration bounds, macro wiring and delivery invariants. They do not launch a browser or HTTP server.

Asset configuration is deliberately narrower than generic relative URLs: only the four generated `Build/` filename shapes and literal `StreamingAssets` are accepted. Schemes (including `javascript:`), backslashes, percent encoding, query/fragment suffixes, network paths and traversal are rejected. WebGPU adapter detection has its own 3–30 second bound. The Unity load state clears timers once; after the main timeout it will not begin loader initialization, and a Unity instance that resolves late is immediately asked to `Quit()` and is never presented as loaded.
