(function (root, factory) {
  var api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  else root.SandFlowBootstrap = api;
})(typeof globalThis !== "undefined" ? globalThis : this, function () {
  "use strict";
  var PUBLIC_WORLD_PATH = /^\/sandflow\/s\/([0-9a-fA-F]{32})$/;
  var QA_WORLD_PATH = /^\/sandflow\/qa\/s\/([0-9a-fA-F]{32})$/;
  var PUBLIC_ROOT_PATH = /^\/sandflow\/$/;
  var QA_ROOT_PATH = /^\/sandflow\/qa\/$/;
  var SENSITIVE_KEY = /^(?:pass(?:word)?|pwd|token|auth(?:orization)?|bearer|steam(?:id)?)$/i;

  function parseLocation(pathname, search, hash) {
    pathname = pathname || ""; search = search || ""; hash = hash || "";
    if (hasSensitiveKeys(search) || hasSensitiveKeys(hash.replace(/^#/, "?")))
      return { ok: false, error: "Sensitive credentials are not allowed in SandFlow URLs." };
    var match = QA_WORLD_PATH.exec(pathname);
    if (match) return { ok: true, worldId: match[1].toLowerCase(), routeBase: "/sandflow/qa/", qa: true };
    match = PUBLIC_WORLD_PATH.exec(pathname);
    if (match) return { ok: true, worldId: match[1].toLowerCase(), routeBase: "/sandflow/", qa: false };
    if (QA_ROOT_PATH.test(pathname)) return { ok: true, worldId: null, routeBase: "/sandflow/qa/", qa: true };
    if (PUBLIC_ROOT_PATH.test(pathname)) return { ok: true, worldId: null, routeBase: "/sandflow/", qa: false };
    return { ok: false, error: "Invalid SandFlow path." };
  }

  function hasSensitiveKeys(value) {
    if (!value) return false;
    var query = value.charAt(0) === "?" ? value.substring(1) : value;
    return query.split("&").some(function (part) {
      var raw = part.split("=", 1)[0];
      try { raw = decodeURIComponent(raw.replace(/\+/g, " ")); } catch (_) { return true; }
      return SENSITIVE_KEY.test(raw);
    });
  }

  function validateConfig(config) {
    if (!config || config.schemaVersion !== 1) return { ok: false, error: "Unsupported bootstrap configuration." };
    if (config.mode !== "preview" && config.mode !== "live") return { ok: false, error: "Invalid client mode." };
    if (config.routeBase !== "/sandflow/" && config.routeBase !== "/sandflow/qa/")
      return { ok: false, error: "Invalid route base." };
    if ((config.surface !== "public-preview" && config.surface !== "online-qa")
        || (config.surface === "public-preview" && (config.routeBase !== "/sandflow/" || config.mode !== "preview"))
        || (config.surface === "online-qa" && (config.routeBase !== "/sandflow/qa/" || config.mode !== "live")))
      return { ok: false, error: "Invalid deployment surface." };
    if (!config.loaderUrl || !config.unityConfig) return { ok: false, error: "Unity build configuration is incomplete." };
    if (!/^Build\/[A-Za-z0-9][A-Za-z0-9_-]{0,127}\.loader\.js$/.test(config.loaderUrl)
        || !/^Build\/[A-Za-z0-9][A-Za-z0-9_-]{0,127}\.data\.unityweb$/.test(config.unityConfig.dataUrl)
        || !/^Build\/[A-Za-z0-9][A-Za-z0-9_-]{0,127}\.framework\.js\.unityweb$/.test(config.unityConfig.frameworkUrl)
        || !/^Build\/[A-Za-z0-9][A-Za-z0-9_-]{0,127}\.wasm\.unityweb$/.test(config.unityConfig.codeUrl)
        || config.unityConfig.streamingAssetsUrl !== "StreamingAssets")
      return { ok: false, error: "Unity assets must use project-relative paths." };
    if (!Number.isFinite(config.stallWarningSeconds) || config.stallWarningSeconds < 10 || config.stallWarningSeconds > 120)
      return { ok: false, error: "Invalid stall warning bound." };
    if (!Number.isFinite(config.capabilityTimeoutSeconds) || config.capabilityTimeoutSeconds < 3
        || config.capabilityTimeoutSeconds > 30)
      return { ok: false, error: "Invalid capability timeout bound." };
    if (!Number.isFinite(config.loadTimeoutSeconds) || config.loadTimeoutSeconds < 60 || config.loadTimeoutSeconds > 600
        || config.loadTimeoutSeconds <= config.stallWarningSeconds)
      return { ok: false, error: "Invalid loading timeout bound." };
    return { ok: true };
  }

  function unityArguments(mode, worldId) {
    var args = [];
    if (mode === "preview") args.push("-sandflowPreview");
    if (worldId) args.push("-sandflowWorldId", worldId);
    return args;
  }

  function fail(elements, message) {
    elements.loading.hidden = true; elements.error.hidden = false;
    elements.error.textContent = message; elements.shell.setAttribute("aria-busy", "false");
  }

  async function capabilityError(environment, timeoutMilliseconds) {
    if (!environment.isSecureContext) return "SandFlow WebGPU requires a secure HTTPS context.";
    if (typeof environment.WebAssembly !== "object") return "This browser does not support WebAssembly.";
    if (typeof environment.WebSocket !== "function") return "This browser does not support WebSocket connections.";
    if (!environment.navigator || !environment.navigator.gpu) return "This client requires WebGPU.";
    var schedule = environment.setTimeout || setTimeout;
    var cancel = environment.clearTimeout || clearTimeout;
    var timer = null;
    try {
      var adapter = await Promise.race([
        environment.navigator.gpu.requestAdapter({ powerPreference: "high-performance" }),
        new Promise(function (_, reject) {
          timer = schedule(function () { reject(new Error("capability-timeout")); }, timeoutMilliseconds || 10000);
        })
      ]);
      if (!adapter) return "No compatible WebGPU adapter is available.";
    } catch (error) {
      return error && error.message === "capability-timeout"
        ? "WebGPU capability detection timed out."
        : "WebGPU initialization was refused by the browser or graphics driver.";
    } finally { if (timer !== null) cancel(timer); }
    return null;
  }

  function createLoadState(clearIntervalFunction, clearTimeoutFunction) {
    var interval = null, timeout = null, cleared = false;
    var state = {
      aborted: false, settled: false,
      setTimers: function (intervalId, timeoutId) { interval = intervalId; timeout = timeoutId; },
      clearTimers: function () {
        if (cleared) return; cleared = true;
        if (interval !== null) clearIntervalFunction(interval);
        if (timeout !== null) clearTimeoutFunction(timeout);
      },
      abort: function () { if (state.settled || state.aborted) return false; state.aborted = true; state.clearTimers(); return true; },
      acceptUnity: function (instance) {
        if (state.aborted || state.settled) {
          if (instance && typeof instance.Quit === "function") instance.Quit();
          return false;
        }
        state.settled = true; state.clearTimers(); return true;
      }
    };
    return state;
  }

  async function start(config) {
    var elements = {
      shell: document.getElementById("shell"), canvas: document.getElementById("unity-canvas"),
      loading: document.getElementById("loading"), status: document.getElementById("status"),
      progress: document.getElementById("progress"), detail: document.getElementById("detail"),
      badge: document.getElementById("mode-badge"), error: document.getElementById("error")
    };
    var valid = validateConfig(config); if (!valid.ok) { fail(elements, valid.error); return; }
    var route = parseLocation(location.pathname, location.search, location.hash);
    if (!route.ok) { fail(elements, route.error); return; }
    if (route.routeBase !== config.routeBase) { fail(elements, "This client template cannot run on this route."); return; }
    var capability = await capabilityError(window, config.capabilityTimeoutSeconds * 1000);
    if (capability) { fail(elements, capability); return; }
    if (config.surface === "online-qa") {
      elements.badge.hidden = false;
      elements.badge.textContent = "Online QA / not public release";
    } else if (config.mode === "preview") {
      elements.badge.hidden = false;
      elements.badge.textContent = "Preview — not connected to a live room";
    }
    elements.canvas.addEventListener("contextmenu", function (event) { event.preventDefault(); });
    config.unityConfig.arguments = unityArguments(config.mode, route.worldId);
    config.unityConfig.showBanner = function (message, type) {
      if (type === "error") fail(elements, String(message));
      else elements.status.textContent = String(message).replace(/<[^>]*>/g, "");
    };
    elements.status.textContent = "Loading SandFlow…";
    var lastProgressAt = Date.now();
    var loadState = createLoadState(clearInterval, clearTimeout);
    var stallTimer = setInterval(function () {
      if (!loadState.aborted && !loadState.settled && Date.now() - lastProgressAt >= config.stallWarningSeconds * 1000)
        elements.status.textContent = "Loading is taking longer than expected…";
    }, 1000);
    var timeout = setTimeout(function () {
      if (loadState.abort()) fail(elements, "SandFlow did not finish loading within the configured time limit. Reload to try again.");
    }, config.loadTimeoutSeconds * 1000);
    loadState.setTimers(stallTimer, timeout);
    var script = document.createElement("script"); script.src = config.loaderUrl;
    script.onerror = function () { if (loadState.abort()) fail(elements, "The Unity loader could not be downloaded."); };
    script.onload = function () {
      if (loadState.aborted || loadState.settled) return;
      if (typeof createUnityInstance !== "function") {
        loadState.abort(); fail(elements, "The Unity loader is invalid."); return;
      }
      createUnityInstance(elements.canvas, config.unityConfig, function (value) {
        var progress = Math.max(0, Math.min(1, Number(value) || 0)); lastProgressAt = Date.now();
        elements.progress.value = progress; elements.detail.textContent = Math.round(progress * 100) + "%";
      }).then(function (unityInstance) {
        if (!loadState.acceptUnity(unityInstance)) return;
        elements.loading.hidden = true; elements.shell.setAttribute("aria-busy", "false");
      }).catch(function () {
        if (loadState.abort()) fail(elements, "The SandFlow client could not start.");
      });
    };
    document.body.appendChild(script);
  }

  return { parseLocation: parseLocation, validateConfig: validateConfig, unityArguments: unityArguments,
    hasSensitiveKeys: hasSensitiveKeys, capabilityError: capabilityError, createLoadState: createLoadState, start: start };
});
