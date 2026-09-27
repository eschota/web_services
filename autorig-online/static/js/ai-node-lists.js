/*
 * For each shot (2026-09-27): list sockets in the /nodes editor.
 *
 * A node whose result carries `items` (Scene split, and every node run per
 * item) is a list. A node wired to a per-item output of a list runs once per
 * item — in parallel, a few at a time — and shows its items as a strip like X9.
 * Nodes wired only to single values (the avatar build, the director's brief)
 * run once and are shared by every item. A list sink (Concat shots) receives
 * the whole list as an array. `params._when` gates a node per item:
 *   {"node": "<stored id>", "is": "MOTION", "options": ["MOTION", "SCENE"]}
 * runs item i only when that node's item i decides for `is` (the option named
 * alone in its text; otherwise the first option), and {"is": "FAILED"} runs an
 * item only when that node's item i failed (a fallback branch).
 */
(function () {
  'use strict';
  const ITEM_TASKS = new Map();
  let api = null;
  const PER_ITEM_PARALLEL = 3;
  const LIST_SINKS = new Set(['video_concat', 'video_summary']);
  const TRANSIENT = /server restarted|unreachable|10054|10053|reset|ECONN|timed? ?out|HTTP 50[234]|Bad Gateway|did not accept/i;

  function install(host) { api = host; }

  const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

  /* ----------------------------------------------------------- runners */

  async function pollVideoTool(accepted, runner, report) {
    let data = accepted;
    for (let attempt = 0; attempt < 1200; attempt += 1) {
      if (data && data.status_string === 'completed') return data;
      if (data && data.status_string === 'failed') throw new Error(data.error_string || 'video tool failed');
      await sleep(attempt < 10 ? 1500 : 3000);
      const response = await fetch('/api/ai/video-tools/status/' + encodeURIComponent(accepted.task_id_string));
      if (response.status === 404) throw new Error('the server no longer knows this job — Render again');
      data = await response.json().catch(() => null);
      if (report && data) {
        try { report({status_string: data.status_string === 'processing' ? 'processing' : data.status_string}); } catch (e) { /* display only */ }
      }
    }
    throw new Error('the video job did not finish in time');
  }

  function shotItems(data) {
    return (data.shots_array || []).map(shot => ({
      status: 'done', type: 'video', value: shot.clip_url, error: '',
      outputs: {shot_clip_url_string: shot.clip_url, first_frame_url_string: shot.first_frame_url,
                middle_frame_url_string: shot.middle_frame_url || shot.first_frame_url,
                shot_info_string: shot.label, last_frame_url_string: shot.last_frame_url || ''},
      meta: {frames: shot.frames, label: shot.label, index: shot.index, shot: shot.shot, part: shot.part,
             chain_frames: shot.chain_frames || shot.frames, next_same_scene: !!shot.next_same_scene}
    }));
  }

  const AVATAR_FIELDS = ['avatar_string', 'front_url_string', 'face_closeup_url_string', 'full_body_url_string',
    'three_quarter_left_url_string', 'three_quarter_right_url_string', 'profile_left_url_string',
    'profile_right_url_string', 'back_url_string', 'sheet_url_string', 'source_frame_url_string', 'description_string'];

  const RUNNERS = {
    scene_split: {api: '/api/ai/video-tools/scene-split', field: 'storyboard_url_string', type: 'image',
      finish: async (accepted, runner, report) => {
        const data = await pollVideoTool(accepted, runner, report);
        return {value: data.storyboard_url_string,
                outputs: {storyboard_url_string: data.storyboard_url_string,
                          scenes_text_string: data.scenes_text_string,
                          shots_json_string: data.shots_json_string,
                          source_video_url_string: data.source_video_url_string},
                items: shotItems(data),
                summary: data.scenes_int + ' scene' + (data.scenes_int === 1 ? '' : 's') + ' · ' + data.count_int +
                         ' shot' + (data.count_int === 1 ? '' : 's') + ' · ' + data.frames_int + ' frames · ' +
                         data.duration_float + ' s' + (data.cuts_array && data.cuts_array.length ? ' · cuts at ' + data.cuts_array.join(', ') + ' s' : '')};
      }},
    // Extract Frames (replaces "Video first frame" under the id video_frame).
    video_frame: {api: '/api/ai/video-tools/extract-frames', field: 'first_url_string', type: 'image',
      finish: async (accepted, runner, report) => {
        const data = await pollVideoTool(accepted, runner, report);
        const frames = data.frames_array || [];
        return {value: data.first_url_string,
                outputs: {image_url_string: data.first_url_string, frames_text_string: data.frames_text_string},
                items: frames.map((f, i) => ({status: 'done', type: 'image', value: f.url, error: '',
                  outputs: {frame_url_string: f.url, frame_info_string: f.text},
                  meta: {label: f.label, index: i, scene: f.scene, frame: f.frame}})),
                summary: data.scenes_int + ' scene' + (data.scenes_int === 1 ? '' : 's') + ' · ' + data.count_int +
                         ' picture' + (data.count_int === 1 ? '' : 's') + ' · ' + data.frames_int + ' frames'};
      }},
    video_storyboard: {api: '/api/ai/video-tools/scene-split', field: 'storyboard_url_string', type: 'image',
      finish: async (accepted, runner, report) => {
        const data = await pollVideoTool(accepted, runner, report);
        return {value: data.storyboard_url_string,
                outputs: {image_url_string: data.storyboard_url_string, scenes_text_string: data.scenes_text_string},
                summary: data.scenes_int + ' scene' + (data.scenes_int === 1 ? '' : 's') + ' · ' + data.frames_int +
                         ' frames · ' + data.fps_int + ' fps · ' + data.duration_float + ' s'};
      }},
    // Avatar (ready or build): a saved Avatar answers at once; otherwise the
    // server started an Avatar build and this follows it like the builder node.
    avatar_ready: {api: '/api/ai/avatar-ready', field: 'avatar_string', type: 'avatar',
      finish: async (accepted) => {
        let data = accepted;
        const id = String(accepted.task_id_string || '');
        for (let attempt = 0; attempt < 1500; attempt += 1) {
          if (data && data.finished_bool) {
            if (data.success_bool === false || data.status_string === 'failed') throw new Error(data.error_string || 'the Avatar build failed');
            if (!data.avatar_string) throw new Error('no Avatar came back');
            const outputs = {};
            AVATAR_FIELDS.forEach(field => { if (data[field]) outputs[field] = String(data[field]); });
            return {value: data.avatar_string, outputs};
          }
          if (!/^avb_[a-f0-9]{24}$/.test(id)) throw new Error('the Avatar builder returned no job');
          await sleep(Math.max(2, Math.min(10, Number(data && data.retry_after_seconds_float) || 4)) * 1000);
          const response = await fetch('/api/ai/avatar-build/status/' + encodeURIComponent(id));
          data = await response.json().catch(() => null);
          if (!response.ok) throw new Error((data && data.detail && data.detail.message_string) || ('HTTP ' + response.status));
        }
        throw new Error('the Avatar build did not finish in time');
      }},
    // Normal map had no runner in the page (pose/depth/canny only).
    control_normal: {api: '/api/controlnet', field: 'image_url_string', type: 'control_normal',
      finish: async (accepted) => {
        const url = accepted.image_url_string;
        for (let attempt = 0; attempt < 400; attempt += 1) {
          const status = accepted.task_id_string ? await fetch('/api/ai/render-status/' + encodeURIComponent(accepted.task_id_string))
            .then(r => r.ok ? r.json() : null).catch(() => null) : null;
          if (status && (status.status_string === 'failed' || status.status_string === 'cancelled')) throw new Error(status.error_string || status.status_string);
          if (status && status.status_string === 'completed') return status.output_url_string || url;
          const probe = await fetch(url, {method: 'HEAD'}).catch(() => null);
          if (probe && probe.ok) return url;
          await sleep(2500);
        }
        throw new Error('the normal map did not land in time');
      }},
    wan_image: {api: '/api/ai/wan-animate', field: 'video_url_string', type: 'video',
      finish: async (accepted) => {
        const url = accepted.video_url_string;
        for (let attempt = 0; attempt < 900; attempt += 1) {
          const status = await fetch('/api/ai/render-status/' + encodeURIComponent(accepted.task_id_string))
            .then(r => r.ok ? r.json() : null).catch(() => null);
          if (status && (status.status_string === 'failed' || status.status_string === 'cancelled')) throw new Error(status.error_string || status.status_string);
          if (status && status.status_string === 'completed') return status.output_url_string || url;
          const probe = await fetch(url, {method: 'HEAD'}).catch(() => null);
          if (probe && probe.ok) return url;
          await sleep(3000);
        }
        throw new Error('Wan-Animate did not finish in time');
      }},
    // Summary (keyframe chain): the same join job in chain mode (adjustBody).
    video_summary: {api: '/api/ai/video-tools/concat', field: 'video_url_string', type: 'video',
      finish: async (accepted, runner, report) => (await pollVideoTool(accepted, runner, report)).video_url_string},
    video_concat: {api: '/api/ai/video-tools/concat', field: 'video_url_string', type: 'video',
      finish: async (accepted, runner, report) => (await pollVideoTool(accepted, runner, report)).video_url_string},
    audio_from_source: {api: '/api/ai/video-tools/audio-mux', field: 'video_url_string', type: 'video',
      finish: async (accepted, runner, report) => (await pollVideoTool(accepted, runner, report)).video_url_string}
  };

  /** Last-step changes to a request body (called at the end of bodyFor). */
  function adjustBody(serviceId, body) {
    if (serviceId === 'video_storyboard' || serviceId === 'scene_split') delete body.view;
    if (serviceId === 'video_summary') body.chain = true;
    if (serviceId === 'video_frame') {
      delete body.view;
      body.template = body.template || 'start_only';  // a migrated First frame node
      body.detect_scenes = body.detect_scenes !== 'off';
    }
    if (serviceId === 'vision' && typeof body.context === 'string') {
      const context = body.context.trim();
      if (context) body.prompt = String(body.prompt || '').trim() + '\n\nContext (facts about what you see):\n' + context;
      delete body.context;
    }
    return body;
  }

  /* ------------------------------------------------------------- gates */

  function whenOf(params) {
    const when = params && params._when;
    return when && typeof when === 'object' && when.node ? when : null;
  }

  function canvasIdFor(storedId) {
    const found = api.findByStoredId(String(storedId));
    return found == null ? null : String(found);
  }

  /** Extra ordering edges: a gated node runs after the node it reads. */
  function gateLinks(graph) {
    const extra = [];
    (graph.nodes || []).forEach(node => {
      const chainFrom = node.params && node.params._chain_next ? canvasIdFor(node.params._chain_next) : null;
      if (chainFrom && chainFrom !== String(node.id)) extra.push({from: chainFrom, to: String(node.id), output: '_chain', input: '_chain'});
      const when = whenOf(node.params);
      if (!when) return;
      const from = canvasIdFor(when.node);
      if (from && from !== String(node.id)) extra.push({from, to: String(node.id), output: '_gate', input: '_gate'});
    });
    return extra;
  }

  function decide(text, when) {
    const options = (when.options && when.options.length ? when.options : [when.is]).map(o => String(o).toUpperCase());
    const upper = String(text || '').toUpperCase();
    const named = options.filter(option => upper.indexOf(option) !== -1);
    return named.length === 1 ? named[0] : options[0];
  }

  function gatePasses(when, gateResult, index) {
    if (!when) return {ok: true};
    if (!gateResult) return {ok: false, why: 'gate node did not run'};
    const want = String(when.is || '').toUpperCase();
    let item = Array.isArray(gateResult.items) ? gateResult.items[index] : {status: 'done', value: gateResult.value};
    // Parts of one long scene follow the decision made for its first part
    // (unless `per: "item"`), so a continuous scene never switches subgraph.
    if (want !== 'FAILED' && when.per !== 'item' && item && item.meta && item.meta.part > 0 && Array.isArray(gateResult.items)) {
      const head = gateResult.items.find(other => other && other.meta && other.meta.shot === item.meta.shot && other.meta.part === 0);
      if (head) item = head;
    }
    if (want === 'FAILED') {
      return item && item.status === 'failed' ? {ok: true} : {ok: false, why: 'not needed (' + (item ? item.status : 'no item') + ')'};
    }
    if (!item || item.status !== 'done') return {ok: false, why: 'route did not arrive'};
    const decision = decide(item.value, when);
    return decision === want ? {ok: true} : {ok: false, why: 'route: ' + decision};
  }

  /* ------------------------------------------------------------- lists */

  function isList(result) { return !!(result && Array.isArray(result.items) && result.items.length); }

  /** Does this wire carry item i of a list (true) or one shared value (false)? */
  function perItem(upstream, field) {
    if (!isList(upstream)) return false;
    // A field the node also keeps once for the whole list (Scene split's shot
    // list, storyboard, 24 fps reference) is shared; everything else is per item.
    const top = upstream.outputs;
    if (field && top && Object.prototype.hasOwnProperty.call(top, field) &&
        !upstream.items.some(item => item && item.outputs && Object.prototype.hasOwnProperty.call(item.outputs, field))) return false;
    return true;
  }

  function itemValue(item, field) {
    if (!item || item.status !== 'done') return null;
    if (item.outputs && field && Object.prototype.hasOwnProperty.call(item.outputs, field)) return item.outputs[field];
    return item.value;
  }

  function frameCountFor(frames, cap) {
    const n = Math.max(9, Math.ceil(Number(frames) || 0));
    const eight = 8 * Math.ceil((n - 1) / 8) + 1;
    return Math.min(eight, Number(cap) || eight);
  }

  /**
   * Called by runGraph for every service node. Returns a run record
   * ({ok, result} / {ok:false, error}) when this node is a list sink, runs per
   * item or is gated; returns null to let the ordinary path run it.
   */
  // Outputs of Scene split that only exist per shot: a wire from one of them
  // must never fall back to the node's single value (the storyboard PNG went
  // to Wan/LTX as a "control video" when a cached Scsplit lost its list).
  const PER_ITEM_FIELDS = new Set(['shot_clip_url_string', 'first_frame_url_string', 'middle_frame_url_string',
    'shot_info_string']);
  const GENERATORS = new Set(['video', 'video_control']);

  /** Model / LoRA set on a Concat shots node, for the generators wired into it. */
  function concatOverride(id) {
    if (!api.graphFromCanvas) return null;
    const graph = api.graphFromCanvas();
    const byId = new Map(graph.nodes.map(node => [String(node.id), node]));
    const out = {};
    graph.links.filter(link => String(link.from) === String(id)).forEach(link => {
      const target = byId.get(String(link.to));
      if (!target || target.service !== 'video_concat') return;
      const p = target.params || {};
      if (String(p.checkpoint || '').trim() && !out.checkpoint) out.checkpoint = String(p.checkpoint).trim();
      if ((String(p.loras || '').trim() || String(p.lora || '').trim()) && !out.lora_set) {
        out.lora_set = true; out.lora = String(p.lora || '').trim(); out.loras = String(p.loras || '').trim();
        out.lora_strength = Number(p.lora_strength) || 0;
      }
    });
    return Object.keys(out).length ? out : null;
  }

  /** Badges: which generators a Concat drives, and on them where the model comes from. */
  function paintOverrides() {
    if (!api || !api.graphFromCanvas) return;
    const graph = api.graphFromCanvas();
    const byId = new Map(graph.nodes.map(node => [String(node.id), node]));
    const label = node => ((node.params || {})._label || node.service || node.id).slice(0, 40);
    graph.nodes.filter(node => node.service === 'video_concat').forEach(concat => {
      const feeders = graph.links.filter(link => String(link.to) === String(concat.id))
        .map(link => byId.get(String(link.from))).filter(Boolean);
      const driven = feeders.filter(node => GENERATORS.has(node.service));
      const fixed = feeders.filter(node => node.service !== 'video_concat' && !GENERATORS.has(node.service) &&
        /video|wan|avatar/.test(node.service));
      const p = concat.params || {};
      const set = String(p.checkpoint || '').trim() || String(p.loras || '').trim() || String(p.lora || '').trim();
      const el = api.nodeElement(concat.id);
      const head = el && el.querySelector('.nhead');
      if (head) {
        let badge = head.querySelector('.noverride');
        if (!badge) { badge = document.createElement('span'); badge.className = 'noverride'; head.appendChild(badge); }
        badge.textContent = set ? ' ⇧ model/LoRA → ' + driven.length : ' ⇧ inherit';
        badge.title = (set ? 'Model / LoRAs set here override: ' : 'Model / LoRAs: inherit (empty) — set them here to override: ') +
          (driven.map(label).join(', ') || 'no LTX/MiniMax generator wired') +
          (fixed.length ? '. Not affected (own fixed model): ' + fixed.map(label).join(', ') : '');
        badge.style.cssText = 'margin-left:4px;font:600 10px system-ui;color:' + (set ? '#22d3ee' : 'rgba(255,255,255,.55)');
      }
      driven.forEach(node => {
        const gel = api.nodeElement(node.id);
        const ghead = gel && gel.querySelector('.nhead');
        if (!ghead) return;
        let b = ghead.querySelector('.nfromconcat');
        if (!set) { if (b) b.remove(); return; }
        if (!b) { b = document.createElement('span'); b.className = 'nfromconcat'; ghead.appendChild(b); }
        b.textContent = ' ⇩ model from Concat';
        b.title = 'Model / LoRAs of this node are overridden by "' + label(concat) + '" for every shot';
        b.style.cssText = 'margin-left:4px;font:600 10px system-ui;color:#22d3ee';
      });
    });
  }
  if (typeof setInterval !== 'undefined' && typeof document !== 'undefined' && document.addEventListener) {
    setInterval(() => { try { paintOverrides(); } catch (e) { /* display only */ } }, 3000);
  }

  async function maybeRun(ctx) {
    const {id, node, feeds, upstreamRecords, pending, epoch, keepDone} = ctx;
    const params = Object.assign({}, node.params || {});
    const when = whenOf(params);
    const lists = feeds.map((link, index) => perItem(upstreamRecords[index] && upstreamRecords[index].result, link.output));
    const lost = feeds.find((link, index) => PER_ITEM_FIELDS.has(link.output) && !lists[index]);
    if (lost) {
      return {ok: false, error: 'Scene split came back without its shot list (' + lost.output + ') — press Render again'};
    }
    const sink = LIST_SINKS.has(node.service);
    if (!sink && !when && !lists.some(Boolean)) return null;
    const shared = {};
    for (let index = 0; index < feeds.length; index += 1) {
      if (lists[index]) continue;
      const link = feeds[index];
      const upstream = upstreamRecords[index].result;
      let value = api.outputValue(upstream, link.output);
      if (upstream.media) value = await api.adaptMediaValue(value, node.service, link.input);
      shared[link.input] = value;
    }
    if (sink) {
      // A sink needs every item: wait for the streamed lists to finish.
      await Promise.all(feeds.map((link, index) => lists[index] ? settled(upstreamRecords[index].result) : null));
      const resolved = Object.assign({}, shared);
      feeds.forEach((link, index) => {
        if (!lists[index]) return;
        resolved[link.input] = upstreamRecords[index].result.items.map(item => itemValue(item, link.output) || '');
      });
      await api.followInputSizeAtRun(node, resolved, params);
      const body = api.bodyFor(node.service, resolved, params);
      const signature = api.stableJson({service: node.service, body});
      try {
        const result = await api.startIncrementalService(id, node, resolved, params, signature, epoch, keepDone, ctx.graph);
        return {ok: true, result};
      } catch (error) {
        return {ok: false, error: String(error.message || error)};
      }
    }
    // Keyframe chain: the end frame of segment i is the start frame of segment
    // i+1 of the same scene, so it is taken from that node, never drawn twice.
    let chainResult = null;
    if (params._chain_next) {
      const chainId = canvasIdFor(params._chain_next);
      const chainRecord = chainId ? await pending.get(chainId) : null;
      chainResult = chainRecord && chainRecord.ok ? chainRecord.result : null;
    }
    let gateResult = null;
    if (when) {
      const gateId = canvasIdFor(when.node);
      const gateRecord = gateId ? await pending.get(gateId) : null;
      gateResult = gateRecord && gateRecord.ok ? gateRecord.result : null;
    }
    const count = Math.max(1, ...feeds.map((link, index) => lists[index] ? upstreamRecords[index].result.items.length : 0),
      isList(gateResult) ? gateResult.items.length : 0);
    try {
      // Streamed: the node answers at once and each item resolves on its own,
      // so shot 1 goes on to the next node while shot 2 is still rendering.
      const {result, done} = runEach(id, node, feeds, upstreamRecords, lists, shared, params, when, gateResult, count, epoch, chainResult);
      return {ok: true, result, whenDone: done};
    } catch (error) {
      return {ok: false, error: String(error.message || error)};
    }
  }

  function settled(result) {
    return Promise.all((result && result.itemPromises) || []);
  }

  function runEach(id, node, feeds, upstreamRecords, lists, shared, params, when, gateResult, count, epoch, chainResult) {
    const runner = api.runnerFor(node.service);
    const previous = api.runState.get(String(id));
    const oldItems = previous && Array.isArray(previous.items) ? previous.items : [];
    const items = [];
    for (let i = 0; i < count; i += 1) items.push(Object.assign({status: 'queued', type: runner.type, value: '', error: '', outputs: null, meta: null},
      oldItems[i] && oldItems[i].seed_override ? {seed_override: oldItems[i].seed_override} : {}));
    const record = {status: 'running', type: api.runnerType(runner, ''), value: '', items, started_at: Date.now() / 1000};
    api.runState.set(String(id), record);
    const state = () => { const el = api.nodeElement(id); return el && el.querySelector('.nstate'); };
    const report = () => {
      if (epoch !== api.epoch() || !api.nodeElement(id)) return;
      const counts = {};
      items.forEach(item => { counts[item.status] = (counts[item.status] || 0) + 1; });
      const text = count + ' shot' + (count === 1 ? '' : 's') + ': ' + Object.keys(counts).map(k => counts[k] + ' ' + k).join(' · ');
      const box = state();
      if (box) { box.textContent = text; box.className = 'nstate ' + (items.some(x => ['queued', 'running'].includes(x.status)) ? 'running' : 'done'); }
      paint(id);
    };
    report();
    const resolvers = [];
    const itemPromises = items.map((item, i) => new Promise(resolve => { resolvers[i] = resolve; }));
    let next = 0;
    const work = async () => {
      while (next < count) {
        const i = next; next += 1;
        try {
          await processItem(i);
          // One retry for a transient farm error (model unreachable, reset,
          // gateway) so a single hiccup does not drop a shot.
          if (items[i].status === 'failed' && TRANSIENT.test(items[i].error || '')) {
            await sleep(10000);
            items[i].status = 'queued'; items[i].error = '';
            await processItem(i);
          }
        } catch (error) {
          items[i].status = 'failed';
          items[i].error = String(error.message || error).slice(0, 400);
          report();
        }
        resolvers[i](items[i]);
      }
    };
    const processItem = async i => {
        const item = items[i];
        // Wait only for item i upstream (and the route decisions it depends on).
        await Promise.all(feeds.map((link, index) => {
          const upstream = lists[index] && upstreamRecords[index].result;
          return upstream && upstream.itemPromises ? upstream.itemPromises[i] : null;
        }));
        if (gateResult && gateResult.itemPromises) await Promise.all(gateResult.itemPromises.slice(0, i + 1));
        const resolved = Object.assign({}, shared);
        let missing = '';
        feeds.forEach((link, index) => {
          if (!lists[index]) return;
          const upstreamItem = upstreamRecords[index].result.items[i];
          if (!item.meta && upstreamItem && upstreamItem.meta) item.meta = upstreamItem.meta;
          const value = itemValue(upstreamItem, link.output);
          if (value == null || value === '') missing = missing || (link.input + ' (' + (upstreamItem ? upstreamItem.status : 'no item') + ')');
          else resolved[link.input] = value;
        });
        if (missing) { item.status = 'skipped'; item.error = 'no input: ' + missing; report(); return; }
        const gate = gatePasses(when, gateResult, i);
        if (!gate.ok) { item.status = 'skipped'; item.error = gate.why; report(); return; }
        if (chainResult && item.meta && item.meta.next_same_scene && chainResult.itemPromises) {
          const shared = await chainResult.itemPromises[i + 1];
          if (shared && shared.status === 'done') {
            Object.assign(item, {status: 'done', value: shared.value, outputs: shared.outputs || null,
                                 type: shared.type || item.type, sig: 'chain:' + shared.value, chained: true});
            report();
            return;
          }
        }
        const itemParams = Object.assign({}, params);
        // One seed per item (base + i): shots never share a seed, so similar
        // inputs cannot collapse to the same result; a locked/rerolled item
        // keeps its own seed.
        if (Number(itemParams.seed) > 0) itemParams.seed = Number(itemParams.seed) + i;
        const previousItem = oldItems[i];
        if (previousItem && previousItem.seed_override) {
          itemParams.seed = previousItem.seed_override;
          item.seed_override = previousItem.seed_override;
        }
        if (GENERATORS.has(node.service)) {
          const override = concatOverride(id);
          if (override && override.checkpoint) itemParams.checkpoint = override.checkpoint;
          // The Concat's LoRA stack (same component as a video node: slot 1 =
          // lora + lora_strength, slots 2.. = loras) replaces the node's own.
          if (override && override.lora_set) {
            itemParams.lora = override.lora; itemParams.loras = override.loras;
            if (override.lora_strength) itemParams.lora_strength = override.lora_strength; else delete itemParams.lora_strength;
          }
        }
        if (item.meta && item.meta.frames && Number(itemParams.frame_count) > 0 && itemParams._frames_from_shot !== false) {
          // Keyframe chain: a segment includes its shared end frame.
          itemParams.frame_count = frameCountFor(item.meta.chain_frames || item.meta.frames, itemParams.frame_count);
        }
        try {
          await api.followInputSizeAtRun(node, resolved, itemParams);
          let body = api.bodyFor(node.service, resolved, itemParams);
          const sig = api.stableJson(body);
          const old = oldItems[i];
          if (old && old.status === 'done' && old.sig === sig && old.value) {
            Object.assign(item, {status: 'done', value: old.value, outputs: old.outputs || null, type: old.type || item.type, sig, cached: true,
                                 seed: old.seed || Number(itemParams.seed) || 0});
            report();
            return;
          }
          item.status = 'running';
          report();
          const post = body._post_upscale;
          delete body._post_upscale;
          let finished;
          // Supersession per shot: a newer body for the same node and shot
          // stands down the older one while it is still queued.
          const itemKey = String(id) + ':' + i;
          const previousTask = ITEM_TASKS.get(itemKey);
          if (previousTask && previousTask.sig !== sig && api.supersedeTasks) api.supersedeTasks([previousTask.taskId]);
          const mineTask = {sig, taskId: ''};
          ITEM_TASKS.set(itemKey, mineTask);
          try {
            const accepted = await api.submitJson(runner.api, body);
            mineTask.taskId = accepted.task_id_string || '';
            finished = api.splitMulti(await runner.finish(accepted, runner, null));
            if (ITEM_TASKS.get(itemKey) !== mineTask) throw new Error('replaced by a newer render');
          } catch (error) {
            if (String(error.message || '').indexOf(api.BUDGET_EXHAUSTED) === -1) throw error;
            body = Object.assign({}, body, {max_output_tokens: Math.min(8192, (Number(body.max_output_tokens) || 1024) * 2)});
            const accepted = await api.submitJson(runner.api, body);
            finished = api.splitMulti(await runner.finish(accepted, runner, null));
          }
          let value = finished.value;
          if (post && value) value = await api.upscaleClip2x(value, null);
          if (!value) throw new Error('no result');
          Object.assign(item, {status: 'done', value, outputs: finished.outputs || null, type: api.runnerType(runner, value), sig,
                               seed: Number(itemParams.seed) || 0});
        } catch (error) {
          item.status = 'failed';
          item.error = String(error.message || error).slice(0, 400);
        }
        report();
    };
    const done = Promise.all(Array.from({length: Math.min(PER_ITEM_PARALLEL, count)}, work)).then(() => {
    const first = items.find(item => item.status === 'done');
    record.status = 'done';
    record.value = first ? first.value : '';
    record.type = first ? first.type : record.type;
    if (!first && items.every(item => item.status === 'failed')) {
      record.status = 'failed';
      record.error = 'every shot failed: ' + (items[0].error || '');
    }
    api.recordResult(id, record);
    report();
    // Even when every shot failed the list goes on: a fallback branch gated on
    // FAILED and Concat (which takes the next candidate per shot) need it.
    result.value = record.value;
    result.type = record.type;
    return result;
    });
    const result = {type: record.type, value: '', outputs: null, items, itemPromises, fromEach: true};
    return {result, done};
  }

  /* ------------------------------------------------------------ display */

  function cellMedia(value, type) {
    if (type === 'text' || !/^https?:/.test(String(value || ''))) {
      const text = document.createElement('div');
      text.textContent = String(value || '').slice(0, 140);
      text.title = String(value || '');
      text.style.cssText = 'font:10px/1.25 system-ui;padding:3px;height:100%;overflow:hidden;white-space:pre-wrap;opacity:.9';
      return text;
    }
    const video = api.looksLikeVideo(value);
    const media = document.createElement(video ? 'video' : 'img');
    media.src = value;
    media.style.cssText = 'width:100%;height:100%;object-fit:contain;display:block;background:#0b0c18';
    if (video) { media.muted = true; media.loop = true; media.preload = 'metadata'; media.playsInline = true;
      media.addEventListener('mouseenter', () => media.play().catch(() => {}));
      media.addEventListener('mouseleave', () => media.pause()); }
    else { media.loading = 'lazy'; media.decoding = 'async'; }
    return media;
  }

  function paint(id) {
    const element = api.nodeElement(id);
    const record = api.runState.get(String(id));
    if (!element || !record || !Array.isArray(record.items) || !record.items.length) return;
    const host = element.querySelector('.nout');
    if (!host) return;
    let strip = host.querySelector(':scope > .nlist');
    if (!strip) {
      strip = document.createElement('div');
      strip.className = 'nlist';
      host.appendChild(strip);
    }
    const node = api.meta(id) || {};
    const scene = node.service === 'scene_split';
    // A grid like X9: cells follow the frame's aspect, rows wrap, no scrollbar.
    const probe = record.items.find(it => it.status === 'done');
    const portrait = !probe || !probe.outputs || true;
    const aspect = listAspect(id, record);
    // Landscape -> fewer, wider cells; portrait -> more, narrower (like X9).
    const minCell = aspect >= 1.2 ? 120 : aspect <= 0.8 ? 58 : 84;
    strip.style.cssText = 'display:grid;grid-template-columns:repeat(auto-fill,minmax(' + minCell + 'px,1fr));gap:3px;margin-top:4px;max-width:100%';
    strip.innerHTML = '';
    if (record.summary) {
      const head = document.createElement('div');
      head.textContent = record.summary;
      head.style.cssText = 'flex:0 0 100%;font:700 12px system-ui;color:#f59e0b;margin-bottom:2px';
      head.style.gridColumn = '1 / -1';
      strip.appendChild(head);
    }
    record.items.forEach((item, index) => {
      const box = document.createElement('div');
      box.style.cssText = 'position:relative;aspect-ratio:' + aspect.toFixed(4) + ';border-radius:4px;overflow:hidden;cursor:zoom-in;' +
        'background:rgba(255,255,255,.06);outline:1px solid ' + (item.status === 'failed' ? '#fb7185' : item.status === 'done' ? 'rgba(255,255,255,.18)' : 'rgba(255,255,255,.08)');
      if (node.service === 'video_frame' && item.meta && item.meta.label) {
        const cap = document.createElement('i');
        cap.textContent = item.meta.label;
        cap.style.cssText = 'position:absolute;z-index:1;left:2px;bottom:1px;font:600 8px system-ui;font-style:normal;color:#fff;text-shadow:0 0 3px #000';
        box.appendChild(cap);
      }
      box.title = 'Shot ' + (index + 1) + ' · ' + item.status + (item.error ? ': ' + item.error : '') + (item.meta && item.meta.label ? '\n' + item.meta.label : '');
      if (item.status === 'done') {
        const shown = scene && item.outputs ? item.outputs.first_frame_url_string : item.value;
        box.appendChild(cellMedia(shown, scene ? 'image' : item.type));
      } else {
        const label = document.createElement('span');
        label.textContent = item.status === 'failed' ? '⚠ failed' : item.status === 'skipped' ? ('— ' + (item.error || 'skipped')).slice(0, 40) : item.status;
        label.style.cssText = 'position:absolute;inset:0;display:flex;align-items:center;justify-content:center;text-align:center;font-size:9px;padding:2px;opacity:.85;' +
          (item.status === 'failed' ? 'color:#fb7185' : '');
        box.appendChild(label);
      }
      const tag = document.createElement('i');
      tag.textContent = String(index + 1);
      tag.style.cssText = 'position:absolute;left:3px;top:1px;font:700 9px system-ui;font-style:normal;color:#fff;text-shadow:0 0 3px #000';
      box.appendChild(tag);
      if (rerollable(node.service) && !scene) box.appendChild(cellTools(id, index, item));
      ['mousedown', 'pointerdown'].forEach(type => box.addEventListener(type, event => event.stopPropagation()));
      box.addEventListener('contextmenu', event => { event.preventDefault(); event.stopPropagation(); segmentMenu(id, index, box); });
      box.addEventListener('click', event => {
        event.stopPropagation();
        if ((item.status === 'done' && (item.type !== 'text' || scene)) || (rerollable(node.service) && !scene)) {
          openListLightbox(id, index); return;
        }
        if (item.status !== 'done') { api.toast('Shot ' + (index + 1) + ': ' + item.status + (item.error ? ' — ' + item.error : '')); return; }
        if (item.type === 'text' && !scene) { api.toast('Shot ' + (index + 1) + ': ' + String(item.value).slice(0, 400)); return; }
        const url = scene ? item.value : item.value;
        api.openPreview(api.looksLikeVideo(url) ? 'video' : 'image', url);
      });
      strip.appendChild(box);
    });
  }

  const REROLLABLE = new Set(['video', 'video_control', 'wan_image', 'qwen_image', 'image', 'avatar_video',
    'control_pose', 'control_depth', 'control_canny', 'control_normal']);
  function rerollable(service) { return REROLLABLE.has(service); }

  // Real width/height of a list's outputs: measured from the loaded media and
  // kept on the record; until then the node's size fields.
  const ASPECTS = new Map();
  function listAspect(id, record) {
    const known = ASPECTS.get(String(id));
    if (known) return known;
    const done = (record.items || []).find(it => it.status === 'done' && /^https?:/.test(String(it.value || '')));
    if (done && typeof document !== 'undefined') {
      const url = String(done.value);
      const probe = document.createElement(api.looksLikeVideo(url) ? 'video' : 'img');
      const settle = (w, h) => {
        if (!(w > 0 && h > 0)) return;
        const a = w / h;
        if (Math.abs((ASPECTS.get(String(id)) || 0) - a) > 0.01) { ASPECTS.set(String(id), a); paint(id); }
      };
      if (probe.tagName === 'VIDEO') { probe.preload = 'metadata'; probe.muted = true;
        probe.addEventListener('loadedmetadata', () => settle(probe.videoWidth, probe.videoHeight)); }
      else probe.addEventListener('load', () => settle(probe.naturalWidth, probe.naturalHeight));
      probe.src = url;
    }
    const [w, h] = cellAspect(id).split('/').map(Number);
    return w > 0 && h > 0 ? w / h : 9 / 16;
  }

  function cellAspect(id) {
    const el = api.nodeElement(id);
    const w = Number((el && el.querySelector('[data-param="width"]') || {}).value) || 9;
    const h = Number((el && el.querySelector('[data-param="height"]') || {}).value) || 16;
    return w + '/' + h;
  }

  function newSeed() { return Math.floor(Math.random() * 2147483000) + 1; }

  function toggleLock(id, index) {
    const record = api.runState.get(String(id));
    const item = record && record.items && record.items[index];
    if (!item) return;
    if (item.seed_override) { delete item.seed_override; api.toast('Segment ' + (index + 1) + ': seed follows the node again.'); }
    else { item.seed_override = item.seed || newSeed(); api.toast('Segment ' + (index + 1) + ': seed ' + item.seed_override + ' locked.'); }
    api.recordResult(id, record);
    paint(id);
  }

  /**
   * "Use this" (X9 parity): keep the take on screen by locking the seed it was
   * rendered with, so R (new node seed) and re-renders skip this segment.
   */
  function useTake(id, index) {
    const record = api.runState.get(String(id));
    const item = record && record.items && record.items[index];
    if (!item || item.status !== 'done') return;
    item.seed_override = item.seed_override || item.seed || newSeed();
    api.recordResult(id, record);
    paint(id);
    api.toast('Segment ' + (index + 1) + ': this take is kept (seed ' + item.seed_override + ' locked; R skips it).');
  }

  /** 🎲 / 🔒 overlay on a strip cell, with the seed. */
  function cellTools(id, index, item) {
    const bar = document.createElement('div');
    bar.style.cssText = 'position:absolute;right:2px;bottom:2px;display:flex;align-items:center;gap:1px;padding:1px;' +
      'border-radius:4px;background:rgba(0,0,0,.55);font:600 8px system-ui;color:#fff';
    const seed = document.createElement('span');
    seed.textContent = (item.seed_override || item.seed) ? String(item.seed_override || item.seed) : '';
    // The seed shows on hover only, so the chip stays a small corner.
    seed.style.cssText = 'display:none;padding:0 2px;white-space:nowrap;opacity:.9';
    bar.addEventListener('mouseenter', () => { if (seed.textContent) seed.style.display = ''; });
    bar.addEventListener('mouseleave', () => { seed.style.display = 'none'; });
    bar.title = (item.seed_override || item.seed) ? 'Seed ' + (item.seed_override || item.seed) : '';
    const btn = (glyph, title, fn, on) => {
      const b = document.createElement('button');
      b.type = 'button'; b.textContent = glyph; b.title = title;
      b.style.cssText = 'border:0;border-radius:3px;padding:0 2px;font:11px system-ui;cursor:pointer;line-height:14px;' +
        (on ? 'background:#f59e0b;color:#111' : 'background:rgba(255,255,255,.18);color:#fff');
      ['mousedown', 'pointerdown'].forEach(type => b.addEventListener(type, e => e.stopPropagation()));
      b.addEventListener('click', e => { e.stopPropagation(); fn(); });
      return b;
    };
    bar.appendChild(seed);
    bar.appendChild(btn('🎲', 'New seed & re-render this segment', () => rerollSegment(id, index, newSeed())));
    bar.appendChild(btn('🔒', item.seed_override ? 'Seed locked (' + item.seed_override + ') — click to unlock' : 'Lock this seed',
                        () => toggleLock(id, index), !!item.seed_override));
    return bar;
  }

  /** The X9-style lightbox for list outputs: zoom, pan, ←/→, Esc, video plays, 🎲 / 🔒. */
  function openListLightbox(id, start) {
    let dialog = document.getElementById('list-lightbox');
    if (!dialog) {
      dialog = document.createElement('dialog');
      dialog.id = 'list-lightbox';
      dialog.style.cssText = 'width:96vw;height:94vh;max-width:96vw;max-height:94vh;padding:10px;border:0;border-radius:12px;background:#0d0e1c;color:#fff;overflow:hidden';
      dialog.innerHTML = '<div class="llstage" style="position:relative;width:100%;height:calc(100% - 44px);overflow:hidden;cursor:grab;display:flex;align-items:center;justify-content:center"></div>' +
        '<div style="display:flex;gap:8px;align-items:center;justify-content:center;margin-top:8px;font:13px system-ui">' +
        '<button type="button" data-ll="prev" title="Previous (←)">←</button><span class="llcap"></span>' +
        '<button type="button" data-ll="next" title="Next (→)">→</button>' +
        '<button type="button" data-ll="reroll" title="New seed & re-render this segment">🎲 New seed & re-render</button>' +
        '<button type="button" data-ll="use" title="Keep this take: lock its seed (R and re-renders leave it alone)">Use this</button>' +
        '<button type="button" data-ll="lock" title="Lock / unlock this segment\'s seed">🔒 Lock seed</button>' +
        '<button type="button" data-ll="zoom0" title="Fit (0)">Fit</button>' +
        '<button type="button" data-ll="close" title="Close (Esc)">✕</button></div>';
      document.body.appendChild(dialog);
      const stage = dialog.querySelector('.llstage');
      const view = {z: 1, x: 0, y: 0, drag: null};
      const apply = () => { const m = stage.firstChild; if (m) m.style.transform = `translate(${view.x}px,${view.y}px) scale(${view.z})`; };
      dialog._reset = () => { view.z = 1; view.x = 0; view.y = 0; apply(); };
      stage.addEventListener('wheel', e => {
        e.preventDefault();
        const k = e.deltaY < 0 ? 1.15 : 1 / 1.15;
        view.z = Math.min(12, Math.max(0.5, view.z * k)); apply();
      }, {passive: false});
      stage.addEventListener('pointerdown', e => { view.drag = {x: e.clientX - view.x, y: e.clientY - view.y}; stage.style.cursor = 'grabbing'; stage.setPointerCapture(e.pointerId); });
      stage.addEventListener('pointermove', e => { if (!view.drag) return; view.x = e.clientX - view.drag.x; view.y = e.clientY - view.drag.y; apply(); });
      stage.addEventListener('pointerup', () => { view.drag = null; stage.style.cursor = 'grab'; });
      // pinch
      const touches = new Map();
      let pinch = 0;
      stage.addEventListener('touchmove', e => {
        if (e.touches.length !== 2) return;
        e.preventDefault();
        const d = Math.hypot(e.touches[0].clientX - e.touches[1].clientX, e.touches[0].clientY - e.touches[1].clientY);
        if (pinch) { view.z = Math.min(12, Math.max(0.5, view.z * d / pinch)); apply(); }
        pinch = d;
      }, {passive: false});
      stage.addEventListener('touchend', () => { pinch = 0; touches.clear(); });
      dialog.addEventListener('click', event => {
        const action = event.target && event.target.dataset && event.target.dataset.ll;
        if (action === 'prev') dialog._show(dialog._index - 1);
        if (action === 'next') dialog._show(dialog._index + 1);
        if (action === 'zoom0') dialog._reset();
        if (action === 'reroll') { rerollSegment(dialog._node, dialog._index, newSeed()); dialog._show(dialog._index); }
        if (action === 'lock') { toggleLock(dialog._node, dialog._index); dialog._show(dialog._index); }
        if (action === 'use') { useTake(dialog._node, dialog._index); dialog._show(dialog._index); }
        if (action === 'close') dialog.close();
      });
      dialog.addEventListener('keydown', event => {
        if (event.key === 'ArrowLeft') { event.preventDefault(); dialog._show(dialog._index - 1); }
        if (event.key === 'ArrowRight') { event.preventDefault(); dialog._show(dialog._index + 1); }
        if (event.key === '0') dialog._reset();
      });
      dialog.addEventListener('close', () => { const clip = dialog.querySelector('video'); if (clip) clip.pause(); });
    }
    dialog._node = String(id);
    dialog._show = index => {
      const record = api.runState.get(dialog._node);
      if (!record || !record.items || !record.items.length) return;
      const count = record.items.length;
      index = ((index % count) + count) % count;
      dialog._index = index;
      const item = record.items[index];
      const stage = dialog.querySelector('.llstage');
      stage.innerHTML = '';
      const node = api.meta(dialog._node) || {};
      const scene = node.service === 'scene_split';
      const url = scene ? item.value : item.value;
      if (item.status === 'done' && url && /^https?:/.test(url)) {
        const media = document.createElement(api.looksLikeVideo(url) ? 'video' : 'img');
        media.src = url;
        media.style.cssText = 'max-width:100%;max-height:100%;display:block;transform-origin:center center;user-select:none;pointer-events:none';
        if (media.tagName === 'VIDEO') { media.controls = false; media.autoplay = true; media.loop = true; media.muted = true; media.playsInline = true; }
        media.draggable = false;
        stage.appendChild(media);
      } else {
        stage.textContent = 'Segment ' + (index + 1) + ': ' + item.status + (item.error ? ' — ' + item.error : '');
      }
      dialog._reset();
      const tools = rerollable(node.service) && !scene;
      dialog.querySelector('[data-ll="reroll"]').hidden = !tools;
      dialog.querySelector('[data-ll="use"]').hidden = !tools || item.status !== 'done';
      const lock = dialog.querySelector('[data-ll="lock"]');
      lock.hidden = !tools;
      lock.textContent = item.seed_override ? '🔓 Unlock seed ' + item.seed_override : '🔒 Lock seed';
      dialog.querySelector('.llcap').textContent = 'Segment ' + (index + 1) + ' / ' + count +
        ((item.seed_override || item.seed) ? ' · seed ' + (item.seed_override || item.seed) : '') +
        (item.meta && item.meta.label ? ' · ' + item.meta.label : '');
    };
    if (!dialog.open) dialog.showModal();
    dialog._show(start);
  }

  /**
   * Per-segment menu (owner, 2026-09-27): open it, re-render it alone with a
   * new seed, or lock its seed. The seed lives on the item (seed_override), so
   * the rest of the list keeps its results and only nodes after this one
   * (Summary / Concat) join again.
   */
  function segmentMenu(id, index, anchor) {
    const record = api.runState.get(String(id));
    const item = record && record.items && record.items[index];
    if (!item) return;
    document.querySelectorAll('.nsegmenu').forEach(el => el.remove());
    const menu = document.createElement('div');
    menu.className = 'nsegmenu';
    const rect = anchor.getBoundingClientRect();
    menu.style.cssText = 'position:fixed;z-index:9999;left:' + Math.round(rect.left) + 'px;top:' + Math.round(rect.bottom + 4) +
      'px;background:#161827;border:1px solid rgba(255,255,255,.2);border-radius:8px;padding:4px;display:flex;flex-direction:column;gap:2px;font:12px system-ui';
    const add = (text, fn, enabled) => {
      const b = document.createElement('button');
      b.type = 'button'; b.textContent = text; b.disabled = enabled === false;
      b.style.cssText = 'text-align:left;background:transparent;color:#fff;border:0;padding:5px 9px;border-radius:5px;cursor:pointer';
      b.addEventListener('mouseenter', () => { b.style.background = 'rgba(255,255,255,.08)'; });
      b.addEventListener('mouseleave', () => { b.style.background = 'transparent'; });
      b.addEventListener('click', event => { event.stopPropagation(); menu.remove(); fn(); });
      menu.appendChild(b);
    };
    const seedNow = item.seed_override || Number(((api.meta(id) || {}).params || {}).seed) || 0;
    add('Open segment ' + (index + 1), () => {
      if (item.value) api.openPreview(api.looksLikeVideo(item.value) ? 'video' : 'image', item.value);
    }, item.status === 'done' && !!item.value);
    add('New seed & re-render this segment', () => rerollSegment(id, index, Math.floor(Math.random() * 2147483000) + 1));
    add(item.seed_override ? 'Seed locked: ' + item.seed_override + ' (unlock)' : 'Lock this segment\'s seed', () => {
      if (item.seed_override) { delete item.seed_override; api.toast('Segment ' + (index + 1) + ': seed follows the node again.'); }
      else { item.seed_override = seedNow || Math.floor(Math.random() * 2147483000) + 1; api.toast('Segment ' + (index + 1) + ': seed ' + item.seed_override + ' locked.'); }
      api.recordResult(id, record);
    });
    document.body.appendChild(menu);
    setTimeout(() => document.addEventListener('click', () => menu.remove(), {once: true}), 0);
  }

  function rerollSegment(id, index, seed) {
    const record = api.runState.get(String(id));
    const item = record && record.items && record.items[index];
    if (!item || !api.runGraph) return;
    item.seed_override = seed;
    item.status = 'stale';
    item.sig = '';
    api.recordResult(id, record);
    // Everything after this node joins again; everything before is reused.
    api.invalidate(id);
    api.toast('Segment ' + (index + 1) + ': new seed ' + seed + ' — re-rendering only this segment, then re-joining.');
    api.runGraph(true);
  }

  /** A saved list result reopened from a link. Returns true when handled. */
  function restore(id, record, state) {
    if (!record || !Array.isArray(record.items) || !record.items.length) return false;
    if (record.status === 'running') {
      record.status = record.items.some(item => item.status === 'done') ? 'stale' : 'failed';
      record.error = 'interrupted — Render again (finished shots are reused)';
    }
    if (state) {
      state.textContent = record.status === 'done' ? 'done · ' + record.items.length + ' shots' : (record.error || record.status);
      state.className = record.status === 'done' ? 'nstate done' : 'nstate';
    }
    requestAnimationFrame(() => paint(id));
    return true;
  }

  /** The ⎇ badge of a gated node; click to change or clear the condition. */
  function decorate(id, params) {
    const element = api.nodeElement(id);
    const head = element && element.querySelector('.nhead');
    const item = api.meta(id);
    if (!head || !item) return;
    let badge = head.querySelector('.nwhen');
    if (!item.when) { if (badge) badge.remove(); return; }
    if (!badge) {
      badge = document.createElement('button');
      badge.type = 'button';
      badge.className = 'nwhen';
      ['mousedown', 'pointerdown', 'touchstart', 'dblclick'].forEach(type => badge.addEventListener(type, event => event.stopPropagation()));
      badge.addEventListener('click', event => {
        event.stopPropagation();
        const current = api.meta(id).when;
        const answer = window.prompt('Run this node per shot only when node "' + current.node + '" says (e.g. MOTION, SCENE, FAILED). Empty = always.', current.is || '');
        if (answer === null) return;
        if (!answer.trim()) api.meta(id).when = null; else api.meta(id).when = Object.assign({}, current, {is: answer.trim().toUpperCase()});
        decorate(id);
        api.invalidate(id);
      });
      head.appendChild(badge);
    }
    badge.textContent = '⎇ ' + item.when.is;
    badge.title = 'Runs per shot only when "' + item.when.node + '" = ' + item.when.is + ' (click to change)';
    badge.style.cssText = 'margin-left:4px;border:1px solid #22d3ee;border-radius:6px;cursor:pointer;font:700 10px system-ui;padding:1px 5px;background:transparent;color:#22d3ee';
  }

  window.AINodeLists = {install, RUNNERS, adjustBody, gateLinks, maybeRun, paint, restore, decorate, isList,
                        LIST_SINKS, pollVideoTool};
})();
