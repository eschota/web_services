# SandFlow WebGPU bootstrap candidate

This directory is an isolated integration candidate. It is not deployed and does not prove that the currently public client is multiplayer-capable.

## Build facts verified from the actual source

`OnlineClientBuild.Web()` currently builds `Assets/SandFlowOnline/Scenes/OnlineBeach.unity` to `Builds/online-web`, forces WebGPU, Brotli, decompression fallback **on**, threads **off**, and Unity data caching **on**. The candidate therefore uses Unity filename macros and serves `.unityweb` fallback payloads verbatim without `Content-Encoding`. It does not assume the older `web.*` filenames.

## Parent integration contract

1. Copy `template/` to `Game/Assets/WebGLTemplates/SandFlowOnlinePreview/` and set `PlayerSettings.WebGL.template = "PROJECT:SandFlowOnlinePreview"` inside the normal Web branch before building. For the separate `WebQa` build profile, create `Game/Assets/WebGLTemplates/SandFlowOnlineQa/` from `template-qa/index.html` plus the exact shared `template/TemplateData/` directory, then select `PROJECT:SandFlowOnlineQa`. This keeps one guarded JS/CSS source while giving QA its own index/base/mode. `WebQa` must also set `PlayerSettings.WebGL.nameFilesAsHashes=true`; the generated hashed names satisfy the strict filename contract.
2. Keep `decompressionFallback=true` unless a separate browser-tested build disables it. With the current setting, do not add `Content-Encoding` to `.unityweb`; the Unity loader performs Brotli fallback decompression. Consequently the fallback container can remain `application/octet-stream` rather than pretending the compressed bytes are directly stream-compilable wasm. Use the revalidation rules in `delivery-rules.nginx.conf` and replace its release-root placeholder with the verified new release path.
3. The template passes only these Unity arguments:
   - `-sandflowWorldId <32-hex-guid>` for `/sandflow/s/{id}`.
   - `-sandflowPreview` when `mode: "preview"`.
4. `SandFlowRoomClient` must parse `-sandflowWorldId` using `Guid.TryParseExact(value, "N", ...)` into `requestedWorld`. It must treat `-sandflowPreview` as non-networked preview and must not create a session, join a room, or open sockets. Unknown/missing/duplicate argument values must fail closed.
5. Never pass room passwords, bearer tokens, Steam IDs, or auth data through the URL or Unity arguments. Passwords remain POST bodies entered in the in-game UI. The bootstrap refuses URLs containing password/token/auth-like query or fragment keys and does not log the URL.
6. The normal candidate remains `preview`, visibly labelled “Preview — not connected to a live room”. The separate QA template is `live` but always displays “Online QA / not public release”. Do not convert the normal template to live during QA.
7. The parser accepts exactly `/sandflow/`, `/sandflow/s/{32hex}`, `/sandflow/qa/`, and `/sandflow/qa/s/{32hex}`. It rejects missing-slash roots, trailing slashes on world routes, nested/arbitrary paths and a template whose configured `routeBase` does not match the actual surface.
8. Parent deploys only `delivery-rules-qa.nginx.conf` for the QA release, replacing `__SANDFLOW_QA_RELEASE_ROOT__` with its verified immutable release path. The exact `/sandflow/qa/` location deliberately uses a directory `alias .../;` plus `index index.html`; a file alias there produced HTTP 500 on GET/HEAD. The quoted 32-hex world regex must remain quoted for nginx brace parsing. The fragment defines only `/sandflow/qa...`, uses `no-store` and `X-Robots-Tag`, and must coexist with—not replace or redefine—the old public root, API, WS and health locations.
9. The QA-only badge class places “Online QA / not public release” at the top-right with an 84 px reserve for the existing debug button and `pointer-events:none`. The shared default `#mode-badge` rule remains top-left for the normal preview template.
10. `webqa-build-contract.json` is the machine-readable WebQA integration gate. Hash-named Unity files are required because `dataCaching=true` stores the data payload in Unity's Cache API: changing only the nginx alias while keeping stable asset URLs can otherwise depend on weak Last-Modified/ETag validators. The QA HTML and HTTP payloads remain `no-store`; each rebuilt index points at new content-addressed filenames, so switching the immutable release root cannot reuse an older Unity data object under the same URL. Parent must upload the complete hashed release before switching the alias and verify every macro-referenced file exists.

Unity's official documentation describes `.unityweb` as the decompression-fallback naming/delivery path and warns that fallback is less efficient than native browser decompression. A future native-Brotli candidate must be a separately built/browser-tested change; do not add `Content-Encoding: br` opportunistically to this fallback contract: https://docs.unity3d.com/6000.0/Documentation/Manual/webgl-deploying.html

Run deterministic checks with:

```powershell
pwsh -File tests/Test-Candidate.ps1
```

The checks validate deep-link parsing, sensitive URL rejection, configuration bounds, macro wiring and delivery invariants. They do not launch a browser or HTTP server.

Asset configuration is deliberately narrower than generic relative URLs: only the four generated `Build/` filename shapes and literal `StreamingAssets` are accepted. Schemes (including `javascript:`), backslashes, percent encoding, query/fragment suffixes, network paths and traversal are rejected. WebGPU adapter detection has its own 3–30 second bound. The Unity load state clears timers once; after the main timeout it will not begin loader initialization, and a Unity instance that resolves late is immediately asked to `Quit()` and is never presented as loaded.
