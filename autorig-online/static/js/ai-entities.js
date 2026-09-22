/* Typed entities shared by the AI service pages.
 *
 * An entity is { type, value, mime, label, origin }. `value` is the string for
 * `text` and a public URL for image/video/model3d, so handing one service's
 * result to another copies nothing — the next service is given the address.
 *
 * The handoff rides in sessionStorage rather than the URL: a prompt can be
 * long, a data URL longer, and a page reload should not lose what was sent.
 */
(function (global) {
  'use strict';

  const SLOT = 'autorig.ai.handoff';
  const HISTORY = 'autorig.ai.history';
  const HISTORY_LIMIT = 12;

  let catalogue = null;

  function entity(type, value, extra) {
    return Object.assign({ type: type, value: value, mime: '', label: '', origin: null },
                         extra || {});
  }

  async function loadCatalogue() {
    if (catalogue) return catalogue;
    const response = await fetch('/api/ai/services');
    if (!response.ok) throw new Error('service catalogue unavailable');
    catalogue = await response.json();
    return catalogue;
  }

  /* ----------------------------------------------------------- handoff slot */

  function send(target, ent) {
    sessionStorage.setItem(SLOT, JSON.stringify({ target: target, entity: ent }));
  }

  /** Take whatever was handed to this page, once. */
  function take(serviceId) {
    let parcel;
    try {
      parcel = JSON.parse(sessionStorage.getItem(SLOT) || 'null');
    } catch (err) {
      parcel = null;
    }
    if (!parcel || parcel.target !== serviceId) return null;
    sessionStorage.removeItem(SLOT);
    return parcel.entity;
  }

  /* --------------------------------------------------------------- history */

  function remember(ent) {
    let items = [];
    try {
      items = JSON.parse(sessionStorage.getItem(HISTORY) || '[]');
    } catch (err) {
      items = [];
    }
    items.unshift(Object.assign({ at: Date.now() }, ent));
    sessionStorage.setItem(HISTORY, JSON.stringify(items.slice(0, HISTORY_LIMIT)));
  }

  function history(type) {
    let items = [];
    try {
      items = JSON.parse(sessionStorage.getItem(HISTORY) || '[]');
    } catch (err) {
      items = [];
    }
    return type ? items.filter(item => item.type === type) : items;
  }

  /* ------------------------------------------------------------ handoff UI */

  /**
   * Render "send this result onward" controls under a result.
   * Only services that accept the entity's type appear; a service that is
   * declared but not wired yet is shown disabled, because knowing where a
   * result is meant to go is useful before the destination works.
   */
  async function renderHandoff(container, ent, options) {
    const opts = options || {};
    container.innerHTML = '';
    if (!ent || !ent.value) return;

    const data = await loadCatalogue();
    const targets = (data.handoff_object || {})[ent.type] || [];
    const usable = targets.filter(t => t.service_id !== opts.from);
    if (!usable.length) return;

    const row = document.createElement('div');
    row.className = 'handoff-row';

    const label = document.createElement('span');
    label.className = 'handoff-label';
    label.textContent = 'Send this ' + ent.type + ' to';
    row.appendChild(label);

    usable.forEach(target => {
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'handoff-btn' + (target.status === 'planned' ? ' planned' : '');
      button.textContent = target.title;
      if (target.status === 'planned') {
        button.disabled = true;
        button.title = target.title + ' is not wired up yet';
      } else {
        button.title = 'Use it as: ' + target.input_title;
        button.onclick = () => {
          send(target.service_id, ent);
          global.location.href = target.path;
        };
      }
      row.appendChild(button);
    });

    const copy = document.createElement('button');
    copy.type = 'button';
    copy.className = 'handoff-btn ghost';
    copy.textContent = 'Copy';
    copy.onclick = async () => {
      try {
        await navigator.clipboard.writeText(ent.value);
        copy.textContent = 'Copied';
        setTimeout(() => { copy.textContent = 'Copy'; }, 1500);
      } catch (err) {
        copy.textContent = 'Copy failed';
      }
    };
    row.appendChild(copy);

    container.appendChild(row);
  }



  /* ------------------------------------------------------------- auto start */

  /**
   * Whether a service has everything it needs to run right now.
   *
   * Derived from the catalogue's required inputs rather than hard-coded per
   * page, so adding a service or changing what it needs does not leave five
   * pages disagreeing about when a run can begin.
   */
  async function canAutoStart(serviceId, providedTypes) {
    let data;
    try { data = await loadCatalogue(); } catch (err) { return false; }
    const entry = (data.services_array || []).find(s => s.id === serviceId);
    if (!entry) return false;
    const required = (entry.inputs || []).filter(i => i.required);
    if (!required.length) return false;
    return required.every(i => providedTypes.indexOf(i.type) !== -1);
  }

  /**
   * Arriving with an entity is already a decision to generate, so the run
   * starts on its own. The short delay lets the page paint what it received
   * first, and the note says it is happening rather than leaving it to guess.
   */
  async function autoStart(serviceId, providedTypes, run, noticeHost) {
    if (!(await canAutoStart(serviceId, providedTypes))) return false;
    if (noticeHost) {
      const note = document.createElement('div');
      note.className = 'auto-note';
      note.textContent = 'Starting automatically — edit and send again any time';
      noticeHost.appendChild(note);
    }
    setTimeout(run, 400);
    return true;
  }

  /* ------------------------------------------------- fleet + task indicator */

  let fleet = null;
  let fleetAt = 0;

  async function getFleet(force) {
    const fresh = Date.now() - fleetAt < 5000;
    if (fleet && fresh && !force) return fleet;
    const response = await fetch('/api/ai/fleet');
    fleet = await response.json();
    fleetAt = Date.now();
    return fleet;
  }

  function human(seconds) {
    const s = Math.max(0, Math.round(seconds));
    if (s < 60) return s + 's';
    const m = Math.floor(s / 60);
    if (m < 60) return m + 'm' + (s % 60 ? ' ' + (s % 60) + 's' : '');
    return Math.floor(m / 60) + 'h ' + (m % 60) + 'm';
  }

  /**
   * Where a job sits in the farm's queue, when the farm can say.
   *
   * "Queued" on its own answers none of the question somebody waiting is
   * actually asking. A rank does: seventh of twenty-one is a wait worth going
   * away from, second of two is not.
   */
  function queuePlaceLabel(position, length) {
    const place = Number(position) || 0;
    const total = Number(length) || 0;
    if (place <= 0) return '';
    return '#' + place + (total >= place ? ' of ' + total : '');
  }

  /**
   * The one control in the strip that changes anything.
   *
   * Only an admin is offered it, because it stands down work that belongs to
   * everybody; the server checks that again and is the one that decides. On
   * the editor page a signed-out owner is told where the button went, because
   * "there is no button" and "you are not signed in" look identical.
   */
  function queueAdminMarkup(data) {
    if (data && data.admin_bool === true) {
      const waiting = Number((data.queue_object || {}).queued_int) || 0;
      return '<div class="fleet-admin">' +
        '<button type="button" class="fleet-clear"' + (waiting ? '' : ' disabled') + '>' +
        'Clear queue' + (waiting ? ' (' + waiting + ')' : '') + '</button>' +
        '<i>Cancels everything that has not started. Jobs already on a card finish.</i></div>';
    }
    if (global.location && global.location.pathname === '/nodes') {
      return '<div class="fleet-admin"><a href="/auth/login?next=%2Fnodes">' +
        'Sign in as admin to clear the queue</a></div>';
    }
    return '';
  }

  /**
   * What each kind of work looks like in the strip. A busy dot says which
   * kind of job is on that card, because "something is running" is not worth
   * a colour of its own when six different things can be running.
   *
   * `conversion` is the farm's own rig and GLB work: not one of these
   * services, but it occupies the same cards and so has to be visible.
   */
  const ACTIVITY = {
    vision: { colour: '#38bdf8', title: 'Vision' },
    text: { colour: '#a78bfa', title: 'Text' },
    // A node that does not report which of the two it is running.
    ai: { colour: '#818cf8', title: 'Vision / text' },
    image: { colour: '#facc15', title: 'Image' },
    control: { colour: '#c084fc', title: 'ControlNet' },
    video: { colour: '#fb7185', title: 'Video' },
    model3d: { colour: '#34d399', title: '3D model' },
    '3dmodel': { colour: '#34d399', title: '3D model' },
    conversion: { colour: '#94a3b8', title: 'Rig / GLB conversion' }
  };

  /**
   * A compact fleet dot-strip: one dot per node, dim when offline, green when
   * free, and coloured by the kind of work it is doing when busy. Detail only
   * on hover, because the strip itself is the status.
   */
  function mountFleet(host, serviceId) {
    host.className = 'fleet';
    host.innerHTML = '<div class="fleet-dots"></div><div class="fleet-pop"></div>';
    const dots = host.querySelector('.fleet-dots');
    const pop = host.querySelector('.fleet-pop');

    async function paint() {
      let data;
      try { data = await getFleet(); } catch (err) { return; }
      const nodes = data.nodes_array || [];
      dots.innerHTML = '';
      const seen = [];
      nodes.forEach(node => {
        const dot = document.createElement('i');
        dot.className = 'fleet-dot' +
          (!node.online ? ' off' : node.busy ? ' busy' : ' free') +
          (node.kind === 'render' ? ' render' : '');
        const work = node.online && node.busy ? ACTIVITY[node.activity] : null;
        if (work) {
          dot.style.background = work.colour;
          if (seen.indexOf(node.activity) === -1) seen.push(node.activity);
        }
        dot.title = node.id + (work ? ' — ' + work.title
                                    : node.online ? ' — free' : ' — offline');
        dots.appendChild(dot);
      });
      const service = (data.services_object || {})[serviceId] || {};
      const busy = data.nodes_busy_int || 0;
      const online = data.nodes_online_int || 0;
      const typical = service.median_seconds_float || 0;

      const summary = document.createElement('span');
      summary.className = 'fleet-sum';
      summary.textContent = online + '/' + (data.nodes_total_int || 0);
      dots.appendChild(summary);

      const queue = data.queue_object || {};
      if (Number.isFinite(Number(queue.running_int)) && Number.isFinite(Number(queue.queued_int))) {
        const queueSummary = document.createElement('span');
        queueSummary.className = 'fleet-queue';
        const eta = queue.eta_seconds_float == null ? '' : ' · ~' + human(queue.eta_seconds_float);
        // The total alone hides which kind of work is waiting; one video queue
        // can hold the whole farm while image and text are idle.
        const perType = Object.entries(data.services_object || {})
          .map(([id, row]) => [id, Number(row.running_int) || 0, Number(row.queued_int) || 0, row])
          .filter(([, running, queued]) => running + queued > 0);
        queueSummary.textContent = Number(queue.running_int) + ' running · ' +
          Number(queue.queued_int) + ' queued' + eta +
          (queue.blocked_int ? ' · ' + Number(queue.blocked_int) + ' blocked' : '') +
          (perType.length ? ' · ' + perType.map(([id, running, queued]) =>
            id + ' ' + running + '▶ ' + queued + '⏳').join(' · ') : '');
        queueSummary.title = Number(queue.running_int) + ' running, ' +
          Number(queue.queued_int) + ' queued, ' + Number(queue.blocked_int || 0) +
          ' blocked' + (queue.eta_seconds_float == null ? '' :
            '; estimated wait ' + human(queue.eta_seconds_float)) +
          (queue.estimate_kind_string ? '; ' + queue.estimate_kind_string : '') +
          (queue.sample_count_int ? '; ' + Number(queue.sample_count_int) + ' measured samples' : '') +
          (perType.length ? '\nBy type: ' + perType.map(([id, running, queued, row]) =>
            id + ' — ' + running + ' running, ' + queued + ' queued' +
            (row.median_seconds_float ? ', typical ' + human(row.median_seconds_float) : '')).join('; ') : '');
        dots.appendChild(queueSummary);
      }

      pop.innerHTML =
        '<div class="fleet-pop-head">' + busy + ' of ' + online + ' busy</div>' +
        '<div class="fleet-pop-row"><span>typical ' + serviceId + '</span><b>' +
          (typical ? human(typical) : '—') + '</b></div>' +
        (service.measured_bool
          ? '<div class="fleet-pop-note">measured over ' + (service.samples_int || 0) + ' recent jobs</div>'
          : '<div class="fleet-pop-note">no recent jobs, estimate</div>') +
        '<div class="fleet-pop-nodes">' + nodes.map(n => {
            const work = n.online && n.busy ? ACTIVITY[n.activity] : null;
            const style = work ? ' style="border-color:' + work.colour +
                                 ';color:' + work.colour + '"' : '';
            return '<span class="' + (!n.online ? 'off' : n.busy ? 'busy' : 'free') +
                   '"' + style + '>' + n.id +
                   (work ? ' · ' + work.title : '') + '</span>';
          }).join('') + '</div>' +
        (seen.length
          ? '<div class="fleet-key">' + seen.map(id =>
              '<span><i style="background:' + ACTIVITY[id].colour + '"></i>' +
              ACTIVITY[id].title + '</span>').join('') + '</div>'
          : '') +
        queueAdminMarkup(data);

      const clear = pop.querySelector('.fleet-clear');
      if (clear) clear.addEventListener('click', event => {
        // The strip itself toggles pinning on click; a button inside it means
        // the button, not the strip.
        event.stopPropagation();
        clearQueue(clear).catch(() => {});
      });
    }

    async function clearQueue(button) {
      const waiting = Number(((fleet || {}).queue_object || {}).queued_int) || 0;
      if (!global.confirm('Cancel every job on the farm that has not started yet' +
          (waiting ? ' (' + waiting + ' waiting)' : '') +
          '?\n\nAnything already rendering is left to finish.')) return;
      button.disabled = true;
      button.textContent = 'Clearing…';
      try {
        const response = await fetch('/api/ai/queue/clear', {method: 'POST'});
        const data = await response.json().catch(() => ({}));
        if (!response.ok || data.success_bool === false) {
          throw new Error(data.detail || data.error_string || ('HTTP ' + response.status));
        }
        button.textContent = (Number(data.cancelled_int) || 0) + ' stood down · ' +
          (Number(data.running_untouched_int) || 0) + ' still rendering';
      } catch (error) {
        button.textContent = 'Could not clear: ' + (error.message || error);
      }
      // Long enough to read what happened before the strip repaints itself.
      setTimeout(() => { getFleet(true).then(paint).catch(() => {}); }, 2500);
    }

    paint();
    const timer = setInterval(paint, 2500);
    let lastRefresh = 0;
    document.addEventListener('ai-task-status', () => {
      if (Date.now() - lastRefresh < 2000) return;
      lastRefresh = Date.now();
      getFleet(true).then(paint).catch(() => {});
    });
    host.addEventListener('click', () => host.classList.toggle('pinned'));
    host.addEventListener('mouseenter', () => { getFleet(true).then(paint).catch(() => {}); });
    return { stop: () => clearInterval(timer), refresh: paint };
  }

  /**
   * Progress for one request, started the instant the user asks for it.
   *
   * Driven by the service's measured median rather than by server events: the
   * first useful event arrives long after somebody starts waiting. It eases
   * toward 95% and holds there until the real result lands, so it never claims
   * to be finished before it is.
   */
  function startTask(host, serviceId) {
    const timingService = serviceId === 'avatar_video' || serviceId === 'video_control'
      ? 'video'
      : serviceId === 'avatar_image'
        ? 'image'
        : String(serviceId || '').startsWith('control_') ? 'control' : serviceId;
    function setPhase(phase) {
      host.classList.add('task-prog');
      host.classList.remove('running', 'queued', 'done', 'failed');
      if (phase) host.classList.add(phase);
    }
    setPhase('running');
    host.innerHTML = '<div class="task-bar"><i></i></div><div class="task-eta"></div>';
    const fill = host.querySelector('.task-bar i');
    const eta = host.querySelector('.task-eta');
    const started = Date.now();
    let typical = 0;
    // How many jobs of this kind the farm is getting through at once, so a
    // rank in the queue can be turned into a wait rather than a number.
    let parallel = 1;
    let stopped = false;
    let active = false;
    let worker = '';
    let activeSince = 0;
    let queuePosition = 0;
    let queueLength = 0;

    getFleet().then(data => {
      const service = (data.services_object || {})[timingService] || {};
      typical = service.median_seconds_float || 0;
      parallel = Math.max(1, Number(service.running_int) || 0);
    }).catch(() => {});

    function tick() {
      if (stopped) return;
      const elapsed = (Date.now() - started) / 1000;
      if (!active) {
        fill.style.width = '3%';
        const place = queuePlaceLabel(queuePosition, queueLength);
        // With a rank and a measured median, the wait is worth more than the
        // stopwatch; without one, the stopwatch is all there is to show.
        const wait = place && typical
          ? '~' + human(queuePosition * typical / parallel)
          : human(elapsed);
        eta.textContent = 'Queued' + (worker ? ' · ' + worker : '') +
          (place ? ' · ' + place : '') + ' · ' + wait;
        setTimeout(tick, 500);
        return;
      }
      // Without a measurement yet, creep on a neutral curve rather than stall.
      const scale = typical || 30;
      const renderingElapsed = (Date.now() - activeSince) / 1000;
      const ratio = 1 - Math.exp(-renderingElapsed / (scale * 0.65));
      fill.style.width = Math.min(95, ratio * 95).toFixed(1) + '%';
      const left = typical ? Math.max(0, typical - renderingElapsed) : 0;
      eta.textContent = typical
        ? ((worker ? worker + ' · ' : '') + (left > 0 ? '~' + human(left) + ' estimated' : 'rendering · ' + human(renderingElapsed)))
        : (worker ? worker + ' · ' : '') + human(renderingElapsed);
      setTimeout(tick, 400);
    }
    tick();

    return {
      setState(info) {
        if (info.active && !active) activeSince = info.startedAt ? info.startedAt * 1000 : Date.now();
        active = !!info.active;
        worker = info.worker || '';
        queuePosition = Number(info.queuePosition) || 0;
        queueLength = Number(info.queueLength) || 0;
        setPhase(active ? 'running' : 'queued');
      },
      async pollRender(taskId) {
        const response = await fetch('/api/ai/render-status/' + encodeURIComponent(taskId));
        if (!response.ok) return null;
        const data = await response.json();
        this.setState({active: data.status_string === 'rendering', worker: data.node_string,
          startedAt: data.started_at_unix_float,
          queuePosition: data.queue_position_int, queueLength: data.queue_length_int});
        document.dispatchEvent(new CustomEvent('ai-task-status', {detail: data}));
        if (['failed', 'cancelled'].includes(data.status_string)) throw new Error(data.error_string || data.status_string);
        return data;
      },
      finish(ok) {
        stopped = true;
        fill.style.width = '100%';
        setPhase(ok === false ? 'failed' : 'done');
        eta.textContent = human((Date.now() - started) / 1000);
      },
      clear() { stopped = true; setPhase(''); host.innerHTML = ''; }
    };
  }

  /** Shared stylesheet for the handoff controls, injected once. */
  function injectStyles() {
    if (document.getElementById('ai-entities-style')) return;
    const style = document.createElement('style');
    style.id = 'ai-entities-style';
    style.textContent = `
      .handoff-row { display:flex; gap:8px; align-items:center; flex-wrap:wrap;
                     margin-top:16px; padding-top:14px;
                     border-top:1px solid rgba(255,255,255,.1); }
      .handoff-label { font-size:12px; color:var(--text-secondary,#9aa0b5); }
      .handoff-btn { font:inherit; font-size:12px; padding:6px 12px; border-radius:999px;
                     cursor:pointer; color:#dfe6ff;
                     background:rgba(123,92,255,.16); border:1px solid rgba(123,92,255,.45); }
      .handoff-btn:hover:not(:disabled) { background:rgba(123,92,255,.3); }
      .handoff-btn.ghost { background:rgba(255,255,255,.06);
                           border-color:rgba(255,255,255,.16); color:var(--text-secondary,#9aa0b5); }
      .handoff-btn.planned { opacity:.4; cursor:not-allowed; }
      .received { display:flex; gap:10px; align-items:center; margin-bottom:14px;
                  padding:10px 12px; border-radius:10px; font-size:13px;
                  background:rgba(56,189,248,.1); border:1px solid rgba(56,189,248,.35); }
      .received img { max-height:46px; border-radius:6px; }
      .received .from { color:var(--text-secondary,#9aa0b5); font-size:12px; }
      .service-nav { display:flex; gap:8px; justify-content:center; flex-wrap:wrap; margin:22px 0 0; }
      .service-nav a { font-size:13px; padding:7px 14px; border-radius:999px; text-decoration:none;
                       color:var(--text-secondary,#9aa0b5);
                       background:rgba(255,255,255,.05); border:1px solid rgba(255,255,255,.12); }
      .service-nav a:hover { color:#fff; border-color:rgba(123,92,255,.5); }
      .service-nav a.current { color:#fff; background:rgba(123,92,255,.2);
                               border-color:rgba(123,92,255,.55); }
      .service-nav a.planned { opacity:.45; pointer-events:none; }
      .fleet { position:relative; display:inline-flex; align-items:center; }
      .fleet-dots { display:inline-flex; align-items:center; gap:4px; padding:5px 10px;
                    border-radius:999px; cursor:default;
                    background:rgba(255,255,255,.05); border:1px solid rgba(255,255,255,.12); }
      .fleet-dot { width:7px; height:7px; border-radius:50%; background:#3a4050; display:block; }
      .fleet-dot.free { background:#38d996; }
      .fleet-dot.busy { background:#ffc857; }
      .fleet-dot.off { background:#3a4050; }
      .fleet-dot.render { border-radius:2px; }
      .fleet-sum { font-size:11px; color:var(--text-secondary,#9aa0b5); margin-left:4px; }
      .fleet-queue { font-size:11px; color:var(--text-secondary,#9aa0b5); margin-left:8px;
                     white-space:nowrap; }
      .fleet-key { display:flex; flex-wrap:wrap; gap:10px; margin-top:9px; padding-top:8px;
                   border-top:1px solid rgba(255,255,255,.1); font-size:11px;
                   color:var(--text-secondary,#9aa0b5); }
      .fleet-key span { display:inline-flex; align-items:center; gap:5px; }
      .fleet-key i { width:7px; height:7px; border-radius:50%; display:block; }

      /* ---- model picker: a dropdown whose options are pictures */
      .mpick { position:relative; display:block; width:100%; }
      .mpick-open { position:relative; z-index:60; }
      .mpick-button { display:flex; align-items:center; gap:8px; width:100%;
        background:rgba(10,11,22,.85); border:1px solid rgba(255,255,255,.14);
        border-radius:9px; color:inherit; padding:5px 8px; cursor:pointer;
        font:inherit; font-size:12px; text-align:left; }
      .mpick-button:hover { border-color:#7b5cff; }
      .mpick-thumb { width:30px; height:30px; flex:0 0 30px; border-radius:6px;
        background:#1a1b30 center/cover no-repeat; display:block; }
      .mpick-thumb.empty { background:
        repeating-linear-gradient(45deg,#23243d 0 5px,#1a1b30 5px 10px); }
      .mpick-choice { flex:1; min-width:0; display:block; overflow:hidden; }
      .mpick-label, .mpick-file { display:block; min-width:0; overflow:hidden;
        text-overflow:ellipsis; white-space:nowrap; }
      .mpick-file { margin-top:1px; color:var(--text-secondary,#9aa0b5); font-size:10px; }
      .mpick-caret { opacity:.6; font-size:10px; }
      .mpick-panel { position:absolute; z-index:40; top:calc(100% + 5px); left:0;
        width:max(100%, 330px); max-width:calc(100vw - 32px); box-sizing:border-box;
        max-height:330px; overflow-y:auto; background:rgba(18,19,38,.99);
        border:1px solid rgba(255,255,255,.16); border-radius:11px; padding:5px;
        box-shadow:0 16px 40px rgba(0,0,0,.55); }
      .mpick-item { display:flex; gap:9px; align-items:flex-start; width:100%;
        background:none; border:0; border-radius:8px; padding:6px; cursor:pointer;
        color:inherit; font:inherit; text-align:left; }
      .mpick-item:hover:not(:disabled) { background:rgba(123,92,255,.18); }
      .mpick-item.chosen { background:rgba(123,92,255,.26); }
      .mpick-item.blocked { opacity:.45; cursor:not-allowed; }
      .mpick-item .mpick-thumb { width:52px; height:52px; flex:0 0 52px; }
      .mpick-text { min-width:0; overflow-wrap:anywhere; }
      .mpick-text b { display:block; font-size:12.5px; font-weight:600; }
      .mpick-text i, .mpick-text u, .mpick-text s {
        display:block; font-style:normal; text-decoration:none; font-size:11px;
        color:var(--text-secondary,#9aa0b5); margin-top:1px; }
      .mpick-text u { color:#c7b9ff; }
      .mpick-rec { display:block; font-style:normal; font-size:10.5px; color:#4ade80;
                   margin-top:2px; }
      .mpick-text s { color:#ff8a9b; }
      .fleet-pop { position:absolute; top:calc(100% + 8px); left:50%; transform:translateX(-50%);
                   min-width:210px; padding:12px 14px; border-radius:12px; z-index:40;
                   background:rgba(12,13,26,.97); border:1px solid rgba(255,255,255,.14);
                   opacity:0; pointer-events:none; transition:opacity .12s; text-align:left; }
      .fleet:hover .fleet-pop, .fleet.pinned .fleet-pop { opacity:1; pointer-events:auto; }
      .fleet-admin { margin-top:10px; padding-top:9px; display:grid; gap:5px;
                     border-top:1px solid rgba(255,255,255,.12); }
      .fleet-admin a { font-size:11px; color:#8ab4ff; }
      .fleet-admin i { font-style:normal; font-size:10.5px; line-height:1.35;
                       color:var(--text-secondary,#9aa0b5); }
      .fleet-clear { font:inherit; font-size:11.5px; padding:5px 9px; border-radius:7px;
                     cursor:pointer; color:#fda4af; background:rgba(251,113,133,.12);
                     border:1px solid rgba(251,113,133,.45); }
      .fleet-clear:hover:not(:disabled) { background:rgba(251,113,133,.24); }
      .fleet-clear:disabled { opacity:.45; cursor:not-allowed; }
      .fleet-dot.busy { outline:1px solid currentColor; outline-offset:2px; animation:fleet-working 1s infinite alternate; }
      @keyframes fleet-working { to { opacity:.45; } }
      .fleet-pop-head { font-size:13px; margin-bottom:8px; }
      .fleet-pop-row { display:flex; justify-content:space-between; gap:12px; font-size:12px;
                       color:var(--text-secondary,#9aa0b5); }
      .fleet-pop-row b { color:#fff; }
      .fleet-pop-note { font-size:11px; color:var(--text-secondary,#9aa0b5); margin-top:6px; opacity:.75; }
      .fleet-pop-nodes { display:flex; flex-wrap:wrap; gap:4px; margin-top:10px; }
      .fleet-pop-nodes span { font-size:10px; padding:2px 6px; border-radius:5px;
                              background:rgba(255,255,255,.06); color:var(--text-secondary,#9aa0b5); }
      .fleet-pop-nodes span.free { color:#38d996; }
      .fleet-pop-nodes span.busy { color:#ffc857; }
      .fleet-pop-nodes span.off { opacity:.4; text-decoration:line-through; }
      .task-prog { margin-top:14px; display:none; }
      .task-prog.running, .task-prog.queued, .task-prog.done, .task-prog.failed { display:block; }
      .task-bar { height:4px; border-radius:999px; overflow:hidden; background:rgba(255,255,255,.08); }
      .task-bar i { display:block; height:100%; width:0;
                    background:linear-gradient(90deg,#7b5cff,#38bdf8); transition:width .4s linear; }
      .task-prog.done .task-bar i { background:#38d996; }
      .task-prog.failed .task-bar i { background:#ff6474; }
      .task-eta { margin-top:6px; font-size:11px; color:var(--text-secondary,#9aa0b5); }
      .auto-note { margin-top:8px; font-size:12px; color:#8ab4ff; }
    `;
    document.head.appendChild(style);
  }

  /** The row of links to the other services, so every page is one click apart. */
  async function renderNav(container, currentId) {
    const data = await loadCatalogue();
    container.className = 'service-nav';
    container.innerHTML = '';
    (data.services_array || []).forEach(entry => {
      const link = document.createElement('a');
      link.href = entry.path;
      link.textContent = entry.title;
      if (entry.id === currentId) link.className = 'current';
      else if (entry.status === 'planned') link.className = 'planned';
      container.appendChild(link);
    });
  }

  /** Show what arrived from another service, so the input is not a mystery. */
  function renderReceived(container, ent) {
    if (!ent) { container.innerHTML = ''; return; }
    container.innerHTML = '';
    const box = document.createElement('div');
    box.className = 'received';
    if (ent.type === 'image') {
      const img = document.createElement('img');
      img.src = ent.value;
      img.alt = '';
      box.appendChild(img);
    }
    const text = document.createElement('div');
    const from = ent.origin && ent.origin.service ? ent.origin.service : 'another service';
    text.innerHTML = '<div>Received a ' + ent.type + ' from <b>' + from + '</b></div>' +
      '<div class="from">' + (ent.type === 'text'
        ? String(ent.value).slice(0, 90) + (String(ent.value).length > 90 ? '…' : '')
        : ent.value) + '</div>';
    box.appendChild(text);
    container.appendChild(box);
  }


  /* --------------------------------------------------------- model picker */

  /**
   * A dropdown of models shown as pictures.
   *
   * A checkpoint or a LoRA is recognised by what it produces; `Pixar_Toon.safetensors`
   * tells a person nothing and `skin texture style v5` barely more. The preview
   * Civitai shows on the model page is the thing that actually identifies it,
   * so that is what the list is made of.
   *
   * Built as a button plus a panel rather than a `<select>`, because a native
   * option cannot hold an image.
   */
  let modelCatalogue = {};

  function samplingPolicy(entry) {
    return (entry && (entry.sampling_policy_object || entry.sampling_policy)) || {};
  }

  function samplingPresentation(entry, kind) {
    const policy = samplingPolicy(entry);
    const parts = [];
    if (Number(policy.fixed_steps) > 0) parts.push('Fixed ' + Number(policy.fixed_steps) + ' steps');
    else if (Number(policy.auto_steps) > 0) parts.push('Auto preset · ' + Number(policy.auto_steps) + ' steps');
    if (policy.cfg_mode === 'fixed') {
      const cfg = Number(policy.cfg_value);
      parts.push('CFG ' + (Number.isFinite(cfg) ? cfg : 1) + (cfg === 1 ? ' (no extra guidance)' : ''));
    }
    if (policy.scheduler_mode === 'native') parts.push(policy.scheduler_label || 'Native scheduler');
    const recommended = entry && entry.recommended || {};
    if (kind === 'checkpoints' && Object.keys(policy).length) {
      if (policy.cfg_mode !== 'fixed' && recommended.cfg != null) parts.push('CFG ' + recommended.cfg);
      if (recommended.sampler) parts.push('sampler ' + recommended.sampler);
      if (policy.scheduler_mode !== 'native' && recommended.scheduler) parts.push('scheduler ' + recommended.scheduler);
    }
    const compatible = !entry || entry.sampling_recommendations_compatible !== false;
    const visibleRecommended = {};
    const authorSampling = [];
    Object.keys(recommended).forEach(key => {
      const samplingKey = ['steps', 'cfg', 'sampler', 'scheduler'].includes(key);
      if (samplingKey) authorSampling.push(key + ' ' + recommended[key]);
      if ((compatible || ['strength', 'lora_strength'].includes(key)) &&
          !(kind === 'checkpoints' && Object.keys(policy).length && samplingKey)) {
        visibleRecommended[key] = recommended[key];
      }
    });
    return {
      policy_text: parts.join(' · '),
      recommended: visibleRecommended,
      note: kind === 'loras' && !compatible ? 'Sampling inherits the selected base model.' : '',
      author_sampling_text: authorSampling.join(' · ')
    };
  }

  async function loadModels(serviceId) {
    if (modelCatalogue[serviceId]) return modelCatalogue[serviceId];
    const response = await fetch('/api/ai/model-catalogue?service=' + encodeURIComponent(serviceId));
    const data = await response.json();
    modelCatalogue[serviceId] = data;
    return data;
  }

  function modelPicker(host, serviceId, kind, options) {
    const settings = options || {};
    const state = { value: settings.value || '', entries: [], checkpoints: [] };
    // Added, not assigned: the node editor marks its slots with `mpick-slot`
    // and looks them up again to restore a saved choice, so overwriting the
    // class list quietly broke reopening a graph with a model on it.
    host.classList.add('mpick');
    host.innerHTML =
      '<button type="button" class="mpick-button">' +
        '<span class="mpick-thumb"></span>' +
        '<span class="mpick-choice"><span class="mpick-label">Loading…</span>' +
          '<small class="mpick-file"></small></span>' +
        '<span class="mpick-caret">▾</span>' +
      '</button>' +
      '<div class="mpick-panel" hidden></div>';

    const button = host.querySelector('.mpick-button');
    const panel = host.querySelector('.mpick-panel');
    const thumb = host.querySelector('.mpick-thumb');
    const label = host.querySelector('.mpick-label');
    const fileLabel = host.querySelector('.mpick-file');

    function paintButton() {
      // A saved node may still name a file the catalogue replaced; the entry
      // lists those names in `legacy_files` and the backend renders them on it.
      const entry = state.entries.find(e => e.file === state.value) ||
        state.entries.find(e => (e.legacy_files || []).includes(state.value));
      if (!entry) {
        thumb.style.backgroundImage = '';
        thumb.className = 'mpick-thumb empty';
        label.textContent = state.value
          ? state.value
          : (kind === 'loras' ? 'No LoRA' : 'Choose a model');
        fileLabel.textContent = state.value ? 'Selected file' :
          (kind === 'loras' ? 'Optional adapter: none' : 'Select a checkpoint');
        button.title = state.value || label.textContent;
        return;
      }
      thumb.className = 'mpick-thumb';
      thumb.style.backgroundImage = entry.preview ? 'url(' + entry.preview + ')' : '';
      label.textContent = entry.title || entry.file;
      fileLabel.textContent = entry.file;
      const sampling = samplingPresentation(entry, kind);
      button.title = (entry.title || entry.file) + ' — ' + entry.file +
        (sampling.policy_text ? '. ' + sampling.policy_text : '') +
        (sampling.author_sampling_text ? '. Author examples: ' + sampling.author_sampling_text : '') +
        (sampling.note ? '. ' + sampling.note : '');
    }

    // The cards on these pages use `backdrop-filter`, which makes each one its
    // own stacking context, so a later card paints over the open panel no
    // matter how high its z-index is. Lifting the card that owns the picker is
    // the only thing that actually works.
    function setOpen(open) {
      if (open && kind === 'loras') {
        panel.querySelectorAll('.mpick-item[data-model-file]').forEach(item => {
          const entry = state.entries.find(value => value.file === item.dataset.modelFile);
          if (!entry) return;
          const reason = loraCompatibilityReason(entry);
          item.disabled = entry.usable === false || !!reason;
          item.classList.toggle('blocked', item.disabled);
          item.title = reason || (entry.title || entry.file) + ' — ' + entry.file + '. ' + (entry.recommended_from || '');
        });
      }
      panel.hidden = !open;
      const card = host.closest('.ai-card') || host.closest('.drawflow-node');
      if (card) card.classList.toggle('mpick-open', open);
    }

    function loraCompatibilityReason(entry) {
      if (kind !== 'loras') return '';
      const scope = host.closest('.drawflow-node, .ai-card') || document;
      const field = scope.querySelector('[data-param="checkpoint"], [name="checkpoint"], #checkpoint');
      const base = state.checkpoints.find(item => item.file === field?.value);
      const left = base?.family, right = entry?.family;
      if (!left || !right || left === right || (['pony','sdxl'].includes(left) && ['pony','sdxl'].includes(right))) return '';
      return 'This LoRA requires ' + (entry.base || right) + '. Choose a compatible checkpoint first.';
    }

    function choose(value, materialized = false) {
      const candidate = state.entries.find(entry => entry.file === value);
      if (candidate && loraCompatibilityReason(candidate)) return;
      state.value = value;
      panel.querySelectorAll('.mpick-item').forEach(item =>
        item.classList.toggle('chosen', item.dataset.modelFile === value));
      paintButton();
      setOpen(false);
      // The entry goes with the value: the caller wants the model's own
      // recommended settings, and it should not have to fetch them again.
      const entry = state.entries.find(e => e.file === value) || null;
      if (settings.onChange) settings.onChange(value, entry, { materialized: materialized === true });
    }

    function row(entry) {
      const item = document.createElement('button');
      item.type = 'button';
      item.dataset.modelFile = entry.file;
      item.className = 'mpick-item' + (entry.usable === false ? ' blocked' : '') +
                       (entry.file === state.value ? ' chosen' : '');
      const triggers = (entry.triggers || []).slice(0, 2).join(', ');
      const sampling = samplingPresentation(entry, kind);
      const rec = sampling.recommended;
      const recText = Object.keys(rec).length
        ? Object.keys(rec).sort().map(k => k + ' ' + rec[k]).join(' · ')
        : '';
      item.title = (entry.title || entry.file) + ' — ' + entry.file + '. ' +
        (entry.recommended_from || 'No author settings published.') +
        (sampling.policy_text ? ' Runtime preset: ' + sampling.policy_text + '.' : '') +
        (sampling.author_sampling_text ? ' Author examples: ' + sampling.author_sampling_text + '.' : '') +
        (sampling.note ? ' ' + sampling.note : '');
      item.innerHTML =
        '<span class="mpick-thumb"' +
          (entry.preview ? ' style="background-image:url(' + entry.preview + ')"' : '') + '></span>' +
        '<span class="mpick-text">' +
          '<b>' + escapeHtml(entry.title || entry.file) + '</b>' +
          '<i>' + escapeHtml(entry.file) +
            (entry.base ? ' · ' + escapeHtml(entry.base) : '') +
            (entry.size_mb ? ' · ' + Math.round(entry.size_mb) + ' MB' : '') +
            (entry.nsfw ? ' · 18+' : '') + '</i>' +
          (triggers ? '<u>' + escapeHtml(triggers) + '</u>' : '') +
          (sampling.policy_text ? '<em class="mpick-rec">' + escapeHtml(sampling.policy_text) + '</em>' : '') +
          (recText ? '<em class="mpick-rec">' + escapeHtml(recText) + '</em>' : '') +
          (sampling.note ? '<i>' + escapeHtml(sampling.note) + '</i>' : '') +
          (entry.usable === false
            ? '<s>' + escapeHtml(entry.unusable_reason || 'not runnable here') + '</s>' : '') +
        '</span>';
      if (entry.usable === false) {
        item.disabled = true;
      } else {
        item.addEventListener('click', () => choose(entry.file));
      }
      return item;
    }

    loadModels(serviceId).then(data => {
      state.checkpoints = data.checkpoints_array || [];
      state.entries = (kind === 'checkpoints' ? data.checkpoints_array : data.loras_array) || [];
      panel.innerHTML = '';
      if (kind === 'loras') {
        const none = document.createElement('button');
        none.type = 'button';
        none.dataset.modelFile = '';
        none.className = 'mpick-item' + (state.value ? '' : ' chosen');
        none.title = 'Use the selected checkpoint without an optional LoRA adapter.';
        none.innerHTML = '<span class="mpick-thumb empty"></span><span class="mpick-text">' +
                         '<b>No LoRA</b><i>Optional adapter: none</i></span>';
        none.addEventListener('click', () => choose(''));
        panel.appendChild(none);
      }
      state.entries.forEach(entry => panel.appendChild(row(entry)));
      if (kind === 'checkpoints' && !state.value) {
        // Saved graphs apply their hidden checkpoint and LoRA values in the
        // same turn that mounts this picker, before this promise callback.
        // A legacy blank checkpoint paired with a Flux1 LoRA must be resolved
        // by the family-aware backend; choosing the image service's FLUX2
        // default here would create an invalid pair.
        const scope = host.closest('.drawflow-node, .ai-card') || document;
        const siblingLora = scope.querySelector && scope.querySelector('[data-param="lora"]');
        const hasExplicitLora = siblingLora && String(siblingLora.value || '').trim();
        if (!hasExplicitLora) {
          const defaultEntry = state.entries.find(entry => entry.usable !== false &&
            ((entry.default_for_services || []).indexOf(serviceId) !== -1 || entry.default === true));
          if (defaultEntry) choose(defaultEntry.file, true);
        }
      }
      paintButton();
      if (settings.onReady) settings.onReady(state.value,
        state.entries.find(entry => entry.file === state.value) || null);
    }).catch(() => { label.textContent = 'catalogue unavailable'; });

    button.addEventListener('click', event => {
      event.stopPropagation();
      setOpen(panel.hidden);
    });
    document.addEventListener('click', () => setOpen(false));
    panel.addEventListener('click', event => event.stopPropagation());

    return {
      get value() { return state.value; },
      get entry() { return state.entries.find(entry => entry.file === state.value) || null; },
      set value(v) {
        state.value = v;
        panel.querySelectorAll('.mpick-item').forEach(item => item.classList.toggle('chosen', item.dataset.modelFile === v));
        paintButton();
      }
    };
  }

  function escapeHtml(value) {
    return String(value == null ? '' : value)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  injectStyles();

  global.AIEntities = {
    TEXT: 'text', IMAGE: 'image', VIDEO: 'video', MODEL3D: 'model3d',
    queuePlaceLabel: queuePlaceLabel,
    queueAdminMarkup: queueAdminMarkup,
    entity: entity,
    loadCatalogue: loadCatalogue,
    send: send,
    take: take,
    remember: remember,
    history: history,
    renderHandoff: renderHandoff,
    renderNav: renderNav,
    renderReceived: renderReceived,
    getFleet: getFleet,
    mountFleet: mountFleet,
    startTask: startTask,
    human: human,
    canAutoStart: canAutoStart,
    autoStart: autoStart,
    modelPicker: modelPicker,
    loadModels: loadModels,
    samplingPolicy: samplingPolicy,
    samplingPresentation: samplingPresentation
  };
})(window);
