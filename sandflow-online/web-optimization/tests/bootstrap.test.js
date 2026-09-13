"use strict";
const assert = require("assert");
const core = require("../template/TemplateData/sandflow-bootstrap.js");

assert.deepStrictEqual(core.parseLocation("/sandflow/", "", ""),
  { ok: true, worldId: null, routeBase: "/sandflow/", qa: false });
assert.deepStrictEqual(core.parseLocation("/sandflow/s/0123456789abcdef0123456789ABCDEF", "", ""),
  { ok: true, worldId: "0123456789abcdef0123456789abcdef", routeBase: "/sandflow/", qa: false });
assert.deepStrictEqual(core.parseLocation("/sandflow/qa/", "", ""),
  { ok: true, worldId: null, routeBase: "/sandflow/qa/", qa: true });
assert.deepStrictEqual(core.parseLocation("/sandflow/qa/s/0123456789ABCDEF0123456789abcdef", "", ""),
  { ok: true, worldId: "0123456789abcdef0123456789abcdef", routeBase: "/sandflow/qa/", qa: true });
assert.strictEqual(core.parseLocation("/sandflow", "", "").ok, false);
assert.strictEqual(core.parseLocation("/sandflow/qa", "", "").ok, false);
assert.strictEqual(core.parseLocation("/sandflow/s/0123456789abcdef0123456789abcdef/", "", "").ok, false);
assert.strictEqual(core.parseLocation("/sandflow/qa/s/0123456789abcdef0123456789abcdef/", "", "").ok, false);
assert.strictEqual(core.parseLocation("/sandflow/s/not-a-guid", "", "").ok, false);
assert.strictEqual(core.parseLocation("/sandflow/s/0123456789abcdef0123456789abcdef/extra", "", "").ok, false);
for (const invalid of ["/sandflow/qa/extra", "/sandflow/qa/s/not-a-guid", "/sandflow/qa/s/0123456789abcdef0123456789abcdef/extra",
  "/sandflow/qa/qa/", "/sandflow/s/s/0123456789abcdef0123456789abcdef"])
  assert.strictEqual(core.parseLocation(invalid, "", "").ok, false, invalid);
for (const key of ["password", "pass", "pwd", "token", "auth", "authorization", "bearer", "steamId"])
  assert.strictEqual(core.parseLocation("/sandflow/", `?${key}=secret`, "").ok, false, key);
for (const suffix of ["?password=secret", "?token=secret", "#auth=secret", "#steamId=secret"])
  assert.strictEqual(core.parseLocation("/sandflow/qa/", suffix[0] === "?" ? suffix : "", suffix[0] === "#" ? suffix : "").ok, false, suffix);
assert.strictEqual(core.parseLocation("/sandflow/", "?lang=en", "").ok, true);

const config = { schemaVersion: 1, mode: "preview", loaderUrl: "Build/x.loader.js",
  surface: "public-preview", routeBase: "/sandflow/",
  unityConfig: { dataUrl: "Build/x.data.unityweb", frameworkUrl: "Build/x.framework.js.unityweb",
    codeUrl: "Build/x.wasm.unityweb", streamingAssetsUrl: "StreamingAssets" },
  stallWarningSeconds: 30, capabilityTimeoutSeconds: 10, loadTimeoutSeconds: 180 };
assert.deepStrictEqual(core.validateConfig(config), { ok: true });
assert.strictEqual(core.validateConfig({ ...config, mode: "public-multiplayer" }).ok, false);
assert.strictEqual(core.validateConfig({ ...config, mode: "live" }).ok, false);
assert.strictEqual(core.validateConfig({ ...config, routeBase: "/sandflow/arbitrary/" }).ok, false);
assert.strictEqual(core.validateConfig({ ...config, surface: "online-qa" }).ok, false);
const qaConfig = { ...config, mode: "live", surface: "online-qa", routeBase: "/sandflow/qa/",
  loaderUrl: "Build/online-web-qa.loader.js",
  unityConfig: { ...config.unityConfig, dataUrl: "Build/online-web-qa.data.unityweb",
    frameworkUrl: "Build/online-web-qa.framework.js.unityweb", codeUrl: "Build/online-web-qa.wasm.unityweb" } };
assert.deepStrictEqual(core.validateConfig(qaConfig), { ok: true });
assert.strictEqual(core.validateConfig({ ...config, loadTimeoutSeconds: 20 }).ok, false);
assert.strictEqual(core.validateConfig({ ...config, loaderUrl: "https://cdn.example/x.js" }).ok, false);
assert.strictEqual(core.validateConfig({ ...config, unityConfig: { ...config.unityConfig, dataUrl: "../private.data" } }).ok, false);
for (const path of ["javascript:alert(1)", "Build\\x.loader.js", "Build/%2e%2e/x.loader.js",
  "Build/x.loader.js?token=x", "Build/x.loader.js#fragment", "//host/x.loader.js", "Build/..loader.js",
  "Build/x.y.loader.js"])
  assert.strictEqual(core.validateConfig({ ...config, loaderUrl: path }).ok, false, path);
assert.deepStrictEqual(core.unityArguments("preview", null), ["-sandflowPreview"]);
assert.deepStrictEqual(core.unityArguments("live", "0123456789abcdef0123456789abcdef"),
  ["-sandflowWorldId", "0123456789abcdef0123456789abcdef"]);
(async function () {
  assert.strictEqual(await core.capabilityError({ isSecureContext: false }),
    "SandFlow WebGPU requires a secure HTTPS context.");
  assert.strictEqual(await core.capabilityError({ isSecureContext: true, WebAssembly: {}, WebSocket: function(){}, navigator: {} }),
    "This client requires WebGPU.");
  assert.strictEqual(await core.capabilityError({ isSecureContext: true, WebAssembly: {}, WebSocket: function(){},
    navigator: { gpu: { requestAdapter: async () => null } } }), "No compatible WebGPU adapter is available.");
  assert.strictEqual(await core.capabilityError({ isSecureContext: true, WebAssembly: {}, WebSocket: function(){},
    navigator: { gpu: { requestAdapter: async () => ({}) } } }), null);
  let capabilityTimerCleared = 0;
  assert.strictEqual(await core.capabilityError({ isSecureContext: true, WebAssembly: {}, WebSocket: function(){},
    navigator: { gpu: { requestAdapter: () => new Promise(function(){}) } },
    setTimeout: function (callback) { callback(); return 7; }, clearTimeout: function () { capabilityTimerCleared++; } }, 5),
    "WebGPU capability detection timed out.");
  assert.strictEqual(capabilityTimerCleared, 1);

  let intervalClears = 0, timeoutClears = 0, quitCalls = 0;
  const state = core.createLoadState(function (id) { assert.strictEqual(id, 11); intervalClears++; },
    function (id) { assert.strictEqual(id, 12); timeoutClears++; });
  state.setTimers(11, 12);
  assert.strictEqual(state.abort(), true);
  assert.strictEqual(state.abort(), false);
  assert.strictEqual(state.acceptUnity({ Quit: function () { quitCalls++; } }), false);
  assert.strictEqual(intervalClears, 1); assert.strictEqual(timeoutClears, 1); assert.strictEqual(quitCalls, 1);

  let settledClears = 0;
  const settled = core.createLoadState(function () { settledClears++; }, function () { settledClears++; });
  settled.setTimers(21, 22);
  assert.strictEqual(settled.acceptUnity({ Quit: function () { throw new Error("must not quit accepted instance"); } }), true);
  assert.strictEqual(settled.acceptUnity({ Quit: function () { quitCalls++; } }), false);
  assert.strictEqual(settledClears, 2); assert.strictEqual(quitCalls, 2);
  console.log("PASS bootstrap path/config/security/capability tests");
})().catch(function (error) { console.error(error); process.exitCode = 1; });
