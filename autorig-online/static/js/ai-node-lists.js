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

  function install(host) { api = host; watchCatalogue(); }

  /* ------------------------------------------- stale catalogue guard */

  // A tab opened before a deploy keeps the old socket lists: a wire drawn to
  // "Frame 3" was read as whatever the old output 3 was (Extract Frames' text
  // info), Vision got text instead of a picture and failed "Provide image_url"
  // (2026-09-28), and the save was refused so the wire never reached the
  // server. The page now notices a changed catalogue, says so, and holds Render
  // until it is reloaded.
  function socketSignature(data) {
    return JSON.stringify((data.services_array || []).map(sv => [sv.id,
      (sv.inputs || []).map(i => i.field), (sv.outputs || []).map(o => o.field)]));
  }
  let catalogueSig = null;
  function watchCatalogue() {
    if (typeof fetch === 'undefined' || typeof document === 'undefined' || !document.addEventListener) return;
    const check = () => fetch('/api/ai/services', {cache: 'no-store'}).then(r => r.json()).then(data => {
      const sig = socketSignature(data);
      if (catalogueSig === null) { catalogueSig = sig; return; }
      if (sig !== catalogueSig) staleBanner();
    }).catch(() => {});
    check();
    setInterval(check, 60000);
  }
  function staleBanner() {
    if (document.getElementById('stale-catalogue')) return;
    const bar = document.createElement('div');
    bar.id = 'stale-catalogue';
    bar.style.cssText = 'position:fixed;z-index:10000;left:50%;top:8px;transform:translateX(-50%);padding:8px 14px;border-radius:8px;' +
      'background:#b45309;color:#fff;font:600 13px system-ui;box-shadow:0 4px 18px rgba(0,0,0,.4);cursor:pointer';
    bar.textContent = 'Nodes were updated on the server — reload this page before rendering or wiring (sockets changed). Click to reload.';
    bar.addEventListener('click', () => location.reload());
    document.body.appendChild(bar);
    const run = document.getElementById('run');
    if (run) { run.disabled = true; run.title = 'Reload the page first: the node catalogue changed on the server'; }
  }

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

  /* ------------------------------------------- ControlNet map adjust */

  // Contrast / levels / gamma / blur / invert on the map nodes: the server makes
  // the map and applies them (/api/ai/video-tools/map), so the adjusted map is
  // the node's output. While a slider moves, the node previews the change on the
  // unadjusted map with CSS filters; "tint" only colours that preview.
  const MAP_KEYS = ['contrast', 'black', 'white', 'gamma', 'blur', 'invert'];
  function mapSig(body) { return MAP_KEYS.map(k => k + '=' + body[k]).join('&'); }
  const mapRunnerFor = channel => ({api: '/api/ai/video-tools/map', field: 'image_url_string', type: 'control_' + channel,
    finish: async (accepted, runner, report) => {
      const data = await pollVideoTool(accepted, runner, report);
      return {value: data.image_url_string,
              outputs: {image_url_string: data.image_url_string, source_map_url_string: data.source_map_url_string || data.image_url_string,
                        adjust_string: data.applied_object ? mapSig(data.applied_object) : ''}};
    }});

  function mapControls(element) {
    const read = name => { const c = element.querySelector('[data-param="' + name + '"]'); return c ? c.value : ''; };
    return {contrast: Number(read('contrast') || 1), black: Number(read('black') || 0), white: Number(read('white') || 255),
            gamma: Number(read('gamma') || 1), blur: Number(read('blur') || 0), invert: read('invert') === 'on', tint: read('_tint') || 'none'};
  }

  function paintMapPreview(id) {
    const element = api.nodeElement(id);
    if (!element) return;
    const img = element.querySelector('.nout > img');
    if (!img) return;
    const record = api.runState.get(String(id)) || {};
    const outputs = record.outputs || {};
    const c = mapControls(element);
    const sig = mapSig({contrast: c.contrast, black: c.black, white: c.white, gamma: c.gamma, blur: c.blur, invert: c.invert});
    const rendered = outputs.adjust_string === sig;
    const source = outputs.source_map_url_string;
    // Rendered with these settings: show the real output. Otherwise preview the
    // settings on the unadjusted map (approximate: levels/gamma as brightness).
    if (!rendered && source && img.dataset.preview !== source) { img.dataset.preview = source; img.src = source; }
    if (rendered && outputs.image_url_string && img.dataset.preview !== outputs.image_url_string) {
      img.dataset.preview = outputs.image_url_string; img.src = outputs.image_url_string;
    }
    const filters = [];
    if (!rendered) {
      const span = Math.max(1, c.white - c.black) / 255;
      filters.push('contrast(' + (c.contrast / span).toFixed(3) + ')');
      filters.push('brightness(' + (Math.pow(0.5, 1 / c.gamma) / 0.5 - (c.black - (255 - c.white)) / 510).toFixed(3) + ')');
      if (c.blur > 0) filters.push('blur(' + (c.blur / 100 * Math.max(img.clientWidth, img.clientHeight)).toFixed(1) + 'px)');
      if (c.invert) filters.push('invert(1)');
    }
    if (c.tint === 'warm') filters.push('sepia(0.8) saturate(2)');
    if (c.tint === 'cool') filters.push('sepia(0.6) hue-rotate(170deg) saturate(2)');
    if (c.tint === 'false') filters.push('sepia(1) saturate(6) hue-rotate(-40deg)');
    img.style.filter = filters.join(' ');
    let note = element.querySelector('.nmapnote');
    if (!note) { note = document.createElement('small'); note.className = 'nmapnote'; img.parentNode.insertBefore(note, img.nextSibling); }
    note.textContent = rendered || !source ? '' : 'preview — Render to apply these settings';
    note.style.cssText = 'display:block;font:600 10px system-ui;color:#f59e0b;margin-top:2px';
  }

  if (typeof setInterval !== 'undefined' && typeof document !== 'undefined' && document.addEventListener) {
    document.addEventListener('input', event => {
      const node = event.target && event.target.closest && event.target.closest('.drawflow-node');
      if (!node || !api) return;
      const id = node.id.replace(/^node-/, '');
      if (/^control_/.test((api.meta(id) || {}).service || '')) paintMapPreview(id);
    }, true);
    document.addEventListener('change', event => {
      const node = event.target && event.target.closest && event.target.closest('.drawflow-node');
      if (!node || !api) return;
      const id = node.id.replace(/^node-/, '');
      if (/^control_/.test((api.meta(id) || {}).service || '')) paintMapPreview(id);
    }, true);
    setInterval(() => {
      try {
        if (!api || !api.graphFromCanvas) return;
        api.graphFromCanvas().nodes.filter(n => /^control_/.test(n.service || '')).forEach(n => paintMapPreview(n.id));
      } catch (e) { /* display only */ }
    }, 2000);
  }

  const RUNNERS = {
    control_pose: mapRunnerFor('pose'), control_depth: mapRunnerFor('depth'), control_canny: mapRunnerFor('canny'),
    control_normal: mapRunnerFor('normal'),
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
    // Search · Civitai (2026-09-28): answers at once (server cache 10 min).
    civitai_search: {api: '/api/ai/civitai-search', field: 'image_url_string', type: 'image',
      finish: async (accepted) => {
        const data = accepted || {};
        if (!data.success_bool) throw new Error((data.detail && data.detail.message_string) || 'Civitai search failed');
        const found = data.items_array || [];
        const kind = data.kind_string === 'video' ? 'video' : 'image';
        const outputs = {image_url_string: found[0] ? found[0].url_string : '', items_text_string: data.items_text_string || ''};
        found.slice(0, FRAME_SOCKETS).forEach((f, i) => { outputs['frame_' + (i + 1) + '_url_string'] = f.url_string; });
        return {value: outputs.image_url_string, type: kind, outputs,
                frameLabels: found.map(f => ({label: '@' + f.author_string, url: f.url_string})),
                items: found.map((f, i) => ({status: 'done', type: kind, value: f.url_string, error: '',
                  outputs: {media_url_string: f.url_string, media_info_string: f.info_string},
                  meta: {label: '@' + f.author_string + ' · ♥ ' + f.reactions_int, index: i, link: f.link_string,
                         author: f.author_string, reactions: f.reactions_int, prompt: f.prompt_string, info: f.info_string,
                         width: f.width_int, height: f.height_int}})),
                summary: data.summary_string || ''};
      }},
    // Extract Frames (replaces "Video first frame" under the id video_frame).
    video_frame: {api: '/api/ai/video-tools/extract-frames', field: 'first_url_string', type: 'image',
      finish: async (accepted, runner, report) => {
        const data = await pollVideoTool(accepted, runner, report);
        const frames = data.frames_array || [];
        const outputs = {image_url_string: data.first_url_string, frames_text_string: data.frames_text_string};
        frames.slice(0, FRAME_SOCKETS).forEach((f, i) => { outputs['frame_' + (i + 1) + '_url_string'] = f.url; });
        return {value: data.first_url_string, outputs,
                frameLabels: frames.map(f => ({label: f.label, url: f.url})),
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
    control_normal_legacy: {api: '/api/controlnet', field: 'image_url_string', type: 'control_normal',
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
  /* ---------------------------------------- Qwen per-picture strength */

  // A Strength slider (0..1) beside each connected picture socket of a
  // Qwen-Image node. The value rides as param rs_1..rs_3 (socket order); the
  // server lets a weaker picture join only for the last part of the sampling
  // steps (the layout stays free), a weak control
  // map is also softened, and the prompt is told to follow it loosely.
  const RS_FIELDS = {image: 1, reference_2: 2, reference_3: 3};

  function applyRefStrength(serviceId, body, resolved, params) {
    if (serviceId !== 'qwen_image' || !resolved || !params) return;
    const channels = api.mapChannels ? api.mapChannels() : new Map();
    const byUrl = new Map();
    Object.keys(RS_FIELDS).forEach(field => {
      const url = resolved[field];
      if (!url) return;
      const v = Number(params['rs_' + RS_FIELDS[field]]);
      byUrl.set(String(url), Number.isFinite(v) ? Math.max(0, Math.min(1, v)) : 1);
    });
    Object.keys(body).filter(k => /^rs_\d$/.test(k)).forEach(k => delete body[k]);
    if (!body.image_url && !body.image_base64) return;
    let order = [body.image_url || body.image_base64 || ''].concat(body.reference_image_urls || []).filter(Boolean);
    // Control maps last, the character (the first non-map picture) first, and
    // the prompt's "image N" renumbered to match: with a map in slot 1 Qwen
    // redrew the map's subject and ignored the character (2026-09-28).
    if (order.some(url => channels.get(String(url))) && order.some(url => !channels.get(String(url)))) {
      const sorted = order.filter(url => !channels.get(String(url))).concat(order.filter(url => channels.get(String(url))));
      if (sorted.some((url, i) => url !== order[i])) {
        const renumber = new Map(order.map((url, i) => [i + 1, sorted.indexOf(url) + 1]));
        body.prompt = String(body.prompt || '').replace(/\b([Ii]mage|[Pp]icture) (\d)\b/g,
          (m, word, n) => word + ' ' + (renumber.get(Number(n)) || n));
        if (body.image_base64) { delete body.image_base64; }
        body.image_url = sorted[0];
        body.reference_image_urls = sorted.slice(1);
        order = sorted;
      }
    }
    if (![...byUrl.values()].some(v => v < 0.999)) return;
    const strengths = order.map(url => byUrl.has(String(url)) ? byUrl.get(String(url)) : 1);
    body.reference_strengths = strengths;
    body.reference_attenuate = order.map((url, i) => !!channels.get(String(url)) && strengths[i] < 0.999);
    const hints = [];
    order.forEach((url, i) => {
      const ch = channels.get(String(url));
      if (strengths[i] <= 0) { hints.push('Ignore image ' + (i + 1) + ' (disabled).'); return; }
      if (strengths[i] < 0.8) {
        hints.push(ch ? 'Image ' + (i + 1) + ' is a ' + ch + ' map: use it only as a loose layout guide; draw the character from image 1.'
                      : 'Follow image ' + (i + 1) + ' only loosely.');
      }
    });
    if (hints.length) body.prompt = [String(body.prompt || '').trim(), hints.join(' ')].filter(Boolean).join(' ');
  }

  function paintRefStrength(id) {
    const node = api.meta(id) || {};
    if (node.service !== 'qwen_image') return;
    const element = api.nodeElement(id);
    if (!element || !api.graphFromCanvas) return;
    const wired = new Set(api.graphFromCanvas().links.filter(l => String(l.to) === String(id)).map(l => l.input));
    const rows = [...element.querySelectorAll('.nports .pin')];
    (node.inFields || []).forEach((field, index) => {
      const k = RS_FIELDS[field];
      const row = rows[index];
      if (!k || !row) return;
      let box = element.querySelector('.nrs-' + k);
      if (!box) {
        box = document.createElement('span');
        box.className = 'nrs nrs-' + k;
        box.style.cssText = 'display:inline-flex;align-items:center;gap:2px;margin-left:6px;font:600 9px system-ui;opacity:.9';
        const input = document.createElement('input');
        input.type = 'range'; input.min = '0'; input.max = '1'; input.step = '0.05'; input.value = '1';
        input.dataset.param = 'rs_' + k;
        input.title = 'Strength of image ' + k + ' (1 = full; lower = it shapes only the early steps; a map is also softened)';
        input.style.cssText = 'width:56px;height:10px';
        const out = document.createElement('b');
        out.textContent = '1.00';
        input.addEventListener('input', () => { out.textContent = Number(input.value).toFixed(2); });
        input.addEventListener('change', () => { out.textContent = Number(input.value).toFixed(2); api.invalidate(id); });
        ['mousedown', 'pointerdown', 'touchstart'].forEach(t => input.addEventListener(t, e => e.stopPropagation()));
        box.appendChild(input); box.appendChild(out);
        box._out = out; box._input = input;
        row.appendChild(box);
      }
      box.style.display = wired.has(field) ? 'inline-flex' : 'none';
      box._out.textContent = Number(box._input.value).toFixed(2);
    });
  }

  if (typeof setInterval !== 'undefined' && typeof document !== 'undefined' && document.addEventListener) {
    setInterval(() => {
      try {
        if (!api || !api.graphFromCanvas) return;
        api.graphFromCanvas().nodes.filter(n => n.service === 'qwen_image').forEach(n => paintRefStrength(n.id));
      } catch (e) { /* display only */ }
    }, 1500);
  }

  function adjustBody(serviceId, body, resolved, params) {
    if (/^control_/.test(serviceId)) {
      body.invert = body.invert === 'on' || body.invert === true;
      ['contrast', 'black', 'white', 'gamma', 'blur'].forEach(k => { if (body[k] === undefined) delete body[k]; });
      if (body.black === 0) body.black = 0;

    }
    applyRefStrength(serviceId, body, resolved, params);
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

  /* ------------------------------------------- Extract Frames sockets */

  // Extract Frames exposes each frame on its own plain socket (frame_1 .. 12):
  // a node wired there receives that one picture and runs once. The socket
  // rows show "k · S1 start" + a thumbnail; unused rows are hidden, and a wired
  // socket past the current count stays visible as "missing" (the link is kept).
  const FRAME_SOCKETS = 20;  // Extract Frames uses 12, Search up to 20
  const PROBES = new Map();   // node id -> {key, frames}

  function frameLabelsOf(id) {
    const record = api.runState.get(String(id));
    if (record && Array.isArray(record.items) && record.items.length) {
      return record.items.map(it => ({label: (it.meta && it.meta.label) || '', url: it.value}));
    }
    const probe = PROBES.get(String(id));
    return probe && probe.frames ? probe.frames : null;
  }

  function wiredFrameSockets(id) {
    const wired = new Set();
    if (!api.graphFromCanvas) return wired;
    api.graphFromCanvas().links.forEach(link => {
      const m = /^frame_(\d+)_url_string$/.exec(String(link.from) === String(id) ? link.output : '');
      if (m) wired.add(Number(m[1]));
    });
    return wired;
  }

  function paintFrameSockets(id) {
    const element = api.nodeElement(id);
    if (!element) return;
    const rows = [...element.querySelectorAll('.nports .pout')];
    const ports = [...element.querySelectorAll('.outputs .output')];
    const frames = frameLabelsOf(id);
    const wired = wiredFrameSockets(id);
    const search = (api.meta(id) || {}).service === 'civitai_search';
    const count = frames ? frames.length : 0;
    let changed = false;
    for (let k = 1; k <= FRAME_SOCKETS; k += 1) {
      const row = rows[k - 1], port = ports[k - 1];
      if (!row || !port) continue;
      const have = k <= count;
      const show = have || wired.has(k) || (!frames && k === 1);
      const key = (show ? '1' : '0') + (have ? frames[k - 1].label + frames[k - 1].url : (wired.has(k) ? 'missing' : ''));
      if (row.dataset.fkey === key) continue;
      row.dataset.fkey = key;
      changed = true;
      row.style.display = show ? '' : 'none';
      port.style.display = show ? '' : 'none';
      row.textContent = '';
      if (have) {
        const clip = api.looksLikeVideo(frames[k - 1].url);
        const img = document.createElement(clip ? 'video' : 'img');
        if (clip) { img.muted = true; img.preload = 'metadata'; img.src = frames[k - 1].url + '#t=0.1'; }
        else img.src = /^https:[/][/]image[.]civitai[.]com[/]/.test(frames[k - 1].url)
          ? '/api/ai/thumb?w=64&url=' + encodeURIComponent(frames[k - 1].url) : frames[k - 1].url;
        img.style.cssText = 'width:16px;height:16px;object-fit:cover;border-radius:3px;vertical-align:middle;margin-right:4px';
        row.appendChild(img);
        row.appendChild(document.createTextNode(k + ' · ' + frames[k - 1].label));
        row.title = (search ? 'Item ' : 'Frame ') + k + ': ' + frames[k - 1].label + ' — a node wired here gets this one ' + (search ? 'item' : 'picture');
      } else if (wired.has(k)) {
        row.appendChild(document.createTextNode(k + ' · missing'));
        row.style.color = '#fb7185';
        row.title = 'Frame ' + k + ' is not produced by the current template/clip; the wire is kept';
      } else {
        row.appendChild(document.createTextNode((search ? 'Item ' : 'Frame ') + k));
      }
      if (have) row.style.color = '';
    }
    if (changed && api.updatePorts) api.updatePorts(id);
  }

  /** Quick probe: when the wired clip or template changes, fetch the frame list
   *  (cached server-side) so sockets can be wired before rendering. */
  function probeExtract(id) {
    if (!api.graphFromCanvas) return;
    const graph = api.graphFromCanvas();
    const node = graph.nodes.find(n => String(n.id) === String(id));
    const link = graph.links.find(l => String(l.to) === String(id) && l.input === 'video_url');
    const src = link && graph.nodes.find(n => String(n.id) === String(link.from));
    if (!node || !src || src.kind !== 'input' || !/^https?:/.test(String(src.value || ''))) return;
    const p = node.params || {};
    const body = adjustBody('video_frame', {video_url: src.value, template: p.template, detect_scenes: p.detect_scenes,
                                            n: Number(p.n) || 4, offset: Number(p.offset) || 0});
    const key = JSON.stringify(body);
    const prior = PROBES.get(String(id));
    if (prior && prior.key === key) return;
    PROBES.set(String(id), {key, frames: prior ? prior.frames : null});
    fetch('/api/ai/video-tools/extract-frames', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: key})
      .then(r => r.json()).then(accepted => pollVideoTool(accepted, RUNNERS.video_frame, null))
      .then(data => {
        const cur = PROBES.get(String(id));
        if (!cur || cur.key !== key) return;
        cur.frames = (data.frames_array || []).map(f => ({label: f.label, url: f.url}));
        paintFrameSockets(id);
      }).catch(() => {});
  }

  if (typeof setInterval !== 'undefined' && typeof document !== 'undefined' && document.addEventListener) {
    setInterval(() => {
      try {
        if (!api || !api.graphFromCanvas) return;
        api.graphFromCanvas().nodes.filter(n => n.service === 'video_frame').forEach(n => {
          probeExtract(n.id);
          paintFrameSockets(n.id);
        });
        api.graphFromCanvas().nodes.filter(n => n.service === 'civitai_search').forEach(n => paintFrameSockets(n.id));
      } catch (e) { /* display only */ }
    }, 2500);
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
    // Civitai originals are several MB each: the grid shows a cached thumbnail.
    media.src = !video && /^https:[/][/]image[.]civitai[.]com[/]/.test(String(value))
      ? '/api/ai/thumb?w=240&url=' + encodeURIComponent(value) : value;
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
      box.title = (item.meta && item.meta.info) ? (index + 1) + '. ' + item.meta.info :
        'Shot ' + (index + 1) + ' · ' + item.status + (item.error ? ': ' + item.error : '') + (item.meta && item.meta.label ? '\n' + item.meta.label : '');
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
      // One corner chip: the index, plus the picture's label for Extract Frames.
      tag.textContent = String(index + 1) + ((node.service === 'video_frame' || node.service === 'civitai_search') && item.meta && item.meta.label ? ' · ' + item.meta.label : '');
      tag.style.cssText = 'position:absolute;z-index:1;left:2px;top:2px;padding:0 3px;border-radius:3px;background:rgba(0,0,0,.55);font:700 8px system-ui;font-style:normal;color:#fff;max-width:calc(100% - 4px);overflow:hidden;text-overflow:ellipsis;white-space:nowrap';
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

  function openListLightbox(id, start) {
    const node = String(id);
    const items = () => { const r = api.runState.get(node); return r && Array.isArray(r.items) ? r.items : []; };
    const info = () => api.meta(node) || {};
    const tools = () => rerollable(info().service) && info().service !== 'scene_split';
    window.AILightbox.open({
      kind: 'list', start, aspect: ASPECTS.get(node) || 0,
      count: () => items().length,
      item: index => {
        const item = items()[index];
        if (!item) return {};
        const scene = info().service === 'scene_split';
        const status = item.status === 'done' ? 'done' : item.status === 'failed' ? 'error' : item.status === 'skipped' ? 'error' : item.status;
        const url = String(item.value || '');
        const text = item.type === 'text' || (url && !/^https?:/.test(url));
        return {url: text ? '' : url, text: text ? url : '', kind: text ? 'text' : '', status,
                thumb: scene && item.outputs ? item.outputs.first_frame_url_string : '',
                seed: item.seed_override || item.seed, locked: !!item.seed_override, used: !!item.seed_override && item.status === 'done',
                error: item.status === 'skipped' ? 'skipped' + (item.error ? ': ' + item.error : '') : item.error};
      },
      title: index => {
        const item = items()[index] || {};
        const word = info().service === 'video_frame' ? 'Frame' : info().service === 'scene_split' ? 'Shot' :
          info().service === 'civitai_search' ? 'Civitai' : 'Segment';
        return word + ' ' + (index + 1) + '/' + items().length + (item.meta && item.meta.label ? ' · ' + item.meta.label : '');
      },
      actions: {
        reseed: index => rerollSegment(node, index, newSeed()), reseedHidden: () => !tools(),
        use: tools() ? index => useTake(node, index) : null,
        useTip: 'Keep this take: lock its seed (R and re-renders leave it alone)',
        lock: tools() ? index => toggleLock(node, index) : null,
        extra: [{glyph: '↗', label: 'Open on Civitai', key: 'O',
                 hidden: index => !((items()[index] || {}).meta || {}).link,
                 run: index => { const link = ((items()[index] || {}).meta || {}).link; if (link) window.open(link, '_blank', 'noopener'); }}]
      }
    });
  }

  /** Search · Civitai: "↻ Refresh" skips the 10-minute server cache once. */
  function searchRefreshButton(id) {
    if ((api.meta(id) || {}).service !== 'civitai_search') return;
    const element = api.nodeElement(id);
    const head = element && element.querySelector('.nhead');
    if (!head || head.querySelector('.nsearch-refresh')) return;
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'nsearch-refresh';
    button.textContent = '↻';
    button.title = 'Refresh: fetch Civitai again now (results are otherwise cached for 10 minutes)';
    button.style.cssText = 'border:1px solid rgba(255,255,255,.18);background:transparent;color:inherit;border-radius:6px;cursor:pointer;padding:0 6px;margin-left:4px';
    ['mousedown', 'pointerdown', 'touchstart', 'dblclick'].forEach(type => button.addEventListener(type, event => event.stopPropagation()));
    button.addEventListener('click', event => {
      event.stopPropagation();
      const graph = api.graphFromCanvas();
      const node = graph.nodes.find(n => String(n.id) === String(id));
      const body = Object.assign({}, (node && node.params) || {}, {refresh: true});
      Object.keys(body).forEach(key => { if (key.startsWith('_')) delete body[key]; });
      button.disabled = true;
      fetch('/api/ai/civitai-search', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)})
        .then(r => r.json()).then(() => { api.invalidate(id); api.toast('Civitai refreshed — Render to use the new results.'); })
        .catch(() => api.toast('Civitai did not answer.'))
        .finally(() => { button.disabled = false; });
    });
    head.appendChild(button);
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
    try { paintRefStrength(id); } catch (e) { /* not drawn yet */ }
    try { searchRefreshButton(id); } catch (e) { /* not drawn yet */ }
    // A migrated "First frame" node (saved without a template) stays one picture.
    const meta0 = api.meta(id) || {};
    if (meta0.service === 'video_frame' && params && !params.template) {
      const field = api.nodeElement(id) && api.nodeElement(id).querySelector('[data-param="template"]');
      if (field) field.value = 'start_only';
      // ...and the whole clip as one scene: exactly the old single first frame.
      const scenes = api.nodeElement(id) && api.nodeElement(id).querySelector('[data-param="detect_scenes"]');
      if (scenes && !params.detect_scenes) scenes.value = 'off';
    }
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
