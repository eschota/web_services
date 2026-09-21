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
    renderReceived: renderReceived
  };
})(window);
