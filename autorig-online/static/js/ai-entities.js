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
   * A compact fleet dot-strip: one dot per node, lit when online, amber when
   * busy. Detail only on hover, because the strip itself is the status.
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
      nodes.forEach(node => {
        const dot = document.createElement('i');
        dot.className = 'fleet-dot' +
          (!node.online ? ' off' : node.busy ? ' busy' : ' free') +
          (node.kind === 'render' ? ' render' : '');
        dot.title = node.id;
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

      pop.innerHTML =
        '<div class="fleet-pop-head">' + busy + ' of ' + online + ' busy</div>' +
        '<div class="fleet-pop-row"><span>typical ' + serviceId + '</span><b>' +
          (typical ? human(typical) : '—') + '</b></div>' +
        (service.measured_bool
          ? '<div class="fleet-pop-note">measured over ' + (service.samples_int || 0) + ' recent jobs</div>'
          : '<div class="fleet-pop-note">no recent jobs, estimate</div>') +
        '<div class="fleet-pop-nodes">' + nodes.map(n =>
            '<span class="' + (!n.online ? 'off' : n.busy ? 'busy' : 'free') + '">' +
            n.id + '</span>').join('') + '</div>';
    }

    paint();
    const timer = setInterval(paint, 7000);
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
    host.className = 'task-prog running';
    host.innerHTML = '<div class="task-bar"><i></i></div><div class="task-eta"></div>';
    const fill = host.querySelector('.task-bar i');
    const eta = host.querySelector('.task-eta');
    const started = Date.now();
    let typical = 0;
    let stopped = false;

    getFleet().then(data => {
      const service = (data.services_object || {})[serviceId] || {};
      typical = service.median_seconds_float || 0;
    }).catch(() => {});

    function tick() {
      if (stopped) return;
      const elapsed = (Date.now() - started) / 1000;
      // Without a measurement yet, creep on a neutral curve rather than stall.
      const scale = typical || 30;
      const ratio = 1 - Math.exp(-elapsed / (scale * 0.65));
      fill.style.width = Math.min(95, ratio * 95).toFixed(1) + '%';
      const left = typical ? Math.max(0, typical - elapsed) : 0;
      eta.textContent = typical
        ? (left > 0 ? '~' + human(left) + ' left' : 'any moment now · ' + human(elapsed))
        : human(elapsed);
      setTimeout(tick, 400);
    }
    tick();

    return {
      finish(ok) {
        stopped = true;
        fill.style.width = '100%';
        host.className = 'task-prog ' + (ok === false ? 'failed' : 'done');
        eta.textContent = human((Date.now() - started) / 1000);
      },
      clear() { stopped = true; host.className = 'task-prog'; host.innerHTML = ''; }
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
      .fleet-pop { position:absolute; top:calc(100% + 8px); left:50%; transform:translateX(-50%);
                   min-width:210px; padding:12px 14px; border-radius:12px; z-index:40;
                   background:rgba(12,13,26,.97); border:1px solid rgba(255,255,255,.14);
                   opacity:0; pointer-events:none; transition:opacity .12s; text-align:left; }
      .fleet:hover .fleet-pop { opacity:1; }
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
      .task-prog.running, .task-prog.done, .task-prog.failed { display:block; }
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

  injectStyles();

  global.AIEntities = {
    TEXT: 'text', IMAGE: 'image', VIDEO: 'video', MODEL3D: 'model3d',
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
    autoStart: autoStart
  };
})(window);
