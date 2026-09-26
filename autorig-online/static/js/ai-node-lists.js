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
  let api = null;
  const PER_ITEM_PARALLEL = 3;
  const LIST_SINKS = new Set(['video_concat']);

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
                shot_info_string: shot.label},
      meta: {frames: shot.frames, label: shot.label, index: shot.index, shot: shot.shot, part: shot.part}
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
    video_concat: {api: '/api/ai/video-tools/concat', field: 'video_url_string', type: 'video',
      finish: async (accepted, runner, report) => (await pollVideoTool(accepted, runner, report)).video_url_string},
    audio_from_source: {api: '/api/ai/video-tools/audio-mux', field: 'video_url_string', type: 'video',
      finish: async (accepted, runner, report) => (await pollVideoTool(accepted, runner, report)).video_url_string}
  };

  /** Last-step changes to a request body (called at the end of bodyFor). */
  function adjustBody(serviceId, body) {
    if (serviceId === 'video_storyboard' || serviceId === 'scene_split') delete body.view;
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
  async function maybeRun(ctx) {
    const {id, node, feeds, upstreamRecords, pending, epoch, keepDone} = ctx;
    const params = Object.assign({}, node.params || {});
    const when = whenOf(params);
    const lists = feeds.map((link, index) => perItem(upstreamRecords[index] && upstreamRecords[index].result, link.output));
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
      const {result, done} = runEach(id, node, feeds, upstreamRecords, lists, shared, params, when, gateResult, count, epoch);
      return {ok: true, result, whenDone: done};
    } catch (error) {
      return {ok: false, error: String(error.message || error)};
    }
  }

  function settled(result) {
    return Promise.all((result && result.itemPromises) || []);
  }

  function runEach(id, node, feeds, upstreamRecords, lists, shared, params, when, gateResult, count, epoch) {
    const runner = api.runnerFor(node.service);
    const previous = api.runState.get(String(id));
    const oldItems = previous && Array.isArray(previous.items) ? previous.items : [];
    const items = [];
    for (let i = 0; i < count; i += 1) items.push({status: 'queued', type: runner.type, value: '', error: '', outputs: null, meta: null});
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
        const itemParams = Object.assign({}, params);
        if (item.meta && item.meta.frames && Number(itemParams.frame_count) > 0 && itemParams._frames_from_shot !== false) {
          itemParams.frame_count = frameCountFor(item.meta.frames, itemParams.frame_count);
        }
        try {
          await api.followInputSizeAtRun(node, resolved, itemParams);
          let body = api.bodyFor(node.service, resolved, itemParams);
          const sig = api.stableJson(body);
          const old = oldItems[i];
          if (old && old.status === 'done' && old.sig === sig && old.value) {
            Object.assign(item, {status: 'done', value: old.value, outputs: old.outputs || null, type: old.type || item.type, sig, cached: true});
            report();
            return;
          }
          item.status = 'running';
          report();
          const post = body._post_upscale;
          delete body._post_upscale;
          let finished;
          try {
            const accepted = await api.submitJson(runner.api, body);
            finished = api.splitMulti(await runner.finish(accepted, runner, null));
          } catch (error) {
            if (String(error.message || '').indexOf(api.BUDGET_EXHAUSTED) === -1) throw error;
            body = Object.assign({}, body, {max_output_tokens: Math.min(8192, (Number(body.max_output_tokens) || 1024) * 2)});
            const accepted = await api.submitJson(runner.api, body);
            finished = api.splitMulti(await runner.finish(accepted, runner, null));
          }
          let value = finished.value;
          if (post && value) value = await api.upscaleClip2x(value, null);
          if (!value) throw new Error('no result');
          Object.assign(item, {status: 'done', value, outputs: finished.outputs || null, type: api.runnerType(runner, value), sig});
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
    media.style.cssText = 'width:100%;height:100%;object-fit:cover;display:block';
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
    strip.style.cssText = 'display:flex;gap:3px;overflow-x:auto;margin-top:4px;padding-bottom:2px;max-width:100%';
    strip.innerHTML = '';
    if (record.summary) {
      const head = document.createElement('div');
      head.textContent = record.summary;
      head.style.cssText = 'flex:0 0 100%;font:700 12px system-ui;color:#f59e0b;margin-bottom:2px';
      strip.style.flexWrap = 'wrap';
      strip.appendChild(head);
    }
    record.items.forEach((item, index) => {
      const box = document.createElement('div');
      box.style.cssText = 'position:relative;flex:0 0 74px;height:74px;border-radius:4px;overflow:hidden;cursor:pointer;' +
        'background:rgba(255,255,255,.06);outline:1px solid ' + (item.status === 'failed' ? '#fb7185' : item.status === 'done' ? 'rgba(255,255,255,.18)' : 'rgba(255,255,255,.08)');
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
      ['mousedown', 'pointerdown'].forEach(type => box.addEventListener(type, event => event.stopPropagation()));
      box.addEventListener('click', event => {
        event.stopPropagation();
        if (item.status !== 'done') { api.toast('Shot ' + (index + 1) + ': ' + item.status + (item.error ? ' — ' + item.error : '')); return; }
        if (item.type === 'text' && !scene) { api.toast('Shot ' + (index + 1) + ': ' + String(item.value).slice(0, 400)); return; }
        const url = scene ? item.value : item.value;
        api.openPreview(api.looksLikeVideo(url) ? 'video' : 'image', url);
      });
      strip.appendChild(box);
    });
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
