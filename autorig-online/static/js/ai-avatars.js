/* /avatars — drop one picture or video, get a reusable character.
 *
 * The page is a thin face over two APIs: POST /api/ai/avatar-build/upload
 * (the source never becomes a public URL) with its status poll, and the
 * owner-scoped Avatar library at /api/ai/avatars. Everything is icons with
 * tooltips; the only words are the Avatar names.
 */
(() => {
  'use strict';

  const $ = id => document.getElementById(id);
  const SLOTS = ['front', 'face_closeup', 'full_body', 'three_quarter_left', 'three_quarter_right',
                 'profile_left', 'profile_right', 'back'];
  const SLOT_TIPS = {front: 'Front', face_closeup: 'Face', full_body: 'Full body',
                     three_quarter_left: '3/4 left', three_quarter_right: '3/4 right',
                     profile_left: 'Profile left', profile_right: 'Profile right', back: 'Back'};
  const STAGES = [
    ['source', 'Source', '<path d="M4 6h16v12H4z"/><path d="M10 9.5v5l4-2.5z"/>'],
    ['describe', 'Describe', '<path d="M2 12s3.6-6 10-6 10 6 10 6-3.6 6-10 6S2 12 2 12z"/><circle cx="12" cy="12" r="2.6"/>'],
    ['front', 'Front view', '<circle cx="12" cy="8" r="3.2"/><path d="M5.5 20c.8-3.6 3.4-5.5 6.5-5.5s5.7 1.9 6.5 5.5"/>'],
    ['views', 'All views', '<rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/>'],
    ['sheet', 'Sheet', '<path d="M4 4h16v16H4z"/><path d="M4 12h16M12 4v16"/>'],
    ['save', 'Saved', '<path d="M5 12.5 10 17l9-10"/>'],
  ];
  const JOB_KEY = 'autorig.avatarBuild.job';
  const state = {file: null, polling: null, avatars: [], current: null};

  const svg = (body, cls) => `<svg class="${cls || ''}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round">${body}</svg>`;
  const escapeHtml = value => String(value == null ? '' : value).replace(/[&<>"']/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]));
  const SLOT_ICON = svg('<circle cx="12" cy="8" r="3"/><path d="M6 20c.7-3.3 3.1-5 6-5s5.3 1.7 6 5"/>', 'slot-icon');
  const QA_MARK = {passed: '✓', accepted_with_warnings: '!', failed: '✗', unchecked: '?'};

  function store(key, value) {
    try { value == null ? localStorage.removeItem(key) : localStorage.setItem(key, value); } catch (_) { /* private mode */ }
  }
  function recall(key) {
    try { return localStorage.getItem(key); } catch (_) { return null; }
  }

  function message(text, error) {
    const box = $('av-msg');
    box.textContent = text || '';
    box.className = 'av-msg' + (error ? ' error' : '');
  }

  async function api(url, options) {
    const response = await fetch(url, options);
    let data = null;
    try { data = await response.json(); } catch (_) { data = null; }
    if (!response.ok) {
      const detail = data && data.detail;
      const text = typeof detail === 'string' ? detail
        : Array.isArray(detail) ? detail.map(item => item.msg).join('; ')
        : (detail && (detail.message_string || detail.error_string)) || ('HTTP ' + response.status);
      throw new Error(text);
    }
    return data || {};
  }

  /* ------------------------------------------------------------- source */

  function chooseFile(file) {
    if (!file) return;
    const video = /^video\//.test(file.type) || /\.(mp4|mov|webm|m4v|mkv)$/i.test(file.name || '');
    if (!video && !/^image\/(png|jpeg|webp)$/.test(file.type)) {
      message('PNG, JPEG, WebP, MP4, WebM or MOV', true);
      return;
    }
    state.file = file;
    const drop = $('av-drop');
    drop.querySelectorAll('img,video').forEach(node => node.remove());
    const url = URL.createObjectURL(file);
    const media = document.createElement(video ? 'video' : 'img');
    media.src = url;
    if (video) { media.muted = true; media.loop = true; media.autoplay = true; media.playsInline = true; }
    drop.appendChild(media);
    $('av-build').disabled = false;
    message('');
  }

  function bindDrop() {
    const drop = $('av-drop');
    const input = $('av-file');
    drop.addEventListener('click', () => input.click());
    drop.addEventListener('keydown', event => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); input.click(); } });
    input.addEventListener('change', () => chooseFile(input.files && input.files[0]));
    ['dragenter', 'dragover'].forEach(name => drop.addEventListener(name, event => { event.preventDefault(); drop.classList.add('dragging'); }));
    ['dragleave', 'drop'].forEach(name => drop.addEventListener(name, event => { event.preventDefault(); drop.classList.remove('dragging'); }));
    drop.addEventListener('drop', event => chooseFile(event.dataTransfer.files && event.dataTransfer.files[0]));
    document.addEventListener('paste', event => {
      const item = [...(event.clipboardData ? event.clipboardData.items : [])].find(entry => entry.kind === 'file');
      if (item) chooseFile(item.getAsFile());
    });
  }

  /* -------------------------------------------------------------- build */

  function renderStages(job) {
    const host = $('av-stages');
    const stage = job ? job.stage_string : '';
    const failed = job && job.finished_bool && job.success_bool === false;
    const index = job && job.finished_bool && !failed ? STAGES.length : STAGES.findIndex(item => item[0] === stage);
    host.innerHTML = STAGES.map((item, number) => {
      const cls = failed && number === index ? 'failed' : number < index ? 'done' : number === index ? 'active' : '';
      return (number ? '<span class="av-stage-line"></span>' : '') +
        `<span class="av-stage ${cls}" data-tip="${escapeHtml(item[1])}">${svg(item[2])}</span>`;
    }).join('');
  }

  function slotHtml(slot, view, pending) {
    const tip = SLOT_TIPS[slot] || slot;
    if (!view || !view.url) {
      return `<div class="av-slot ${pending ? 'pending' : ''}" data-tip="${escapeHtml(tip)}">${SLOT_ICON}</div>`;
    }
    const status = view.status || 'unchecked';
    const score = view.score != null ? ' · ' + Math.round(view.score * 100) + '%' : '';
    return `<div class="av-slot" data-tip="${escapeHtml(tip + score)}"><img src="${escapeHtml(view.url)}" alt="${escapeHtml(tip)}" loading="lazy" data-full="${escapeHtml(view.url)}">` +
      `<span class="av-badge ${escapeHtml(status)}">${QA_MARK[status] || '?'}</span></div>`;
  }

  function renderLiveViews(job) {
    const views = (job && job.views_object) || {};
    const running = job && !job.finished_bool;
    $('av-live-views').innerHTML = SLOTS.map(slot => {
      const view = views[slot];
      return slotHtml(slot, view ? {url: view.url_string, status: view.status_string, score: view.identity_score_float} : null, running);
    }).join('');
  }

  async function build() {
    if (!state.file) return;
    const form = new FormData();
    form.append('file', state.file);
    form.append('outfit', $('av-outfit').value.trim());
    form.append('display_name', $('av-name').value.trim());
    $('av-build').disabled = true;
    message('');
    try {
      const job = await api('/api/ai/avatar-build/upload', {method: 'POST', body: form});
      store(JOB_KEY, job.task_id_string);
      follow(job);
    } catch (error) {
      message(error.message, true);
      $('av-build').disabled = false;
    }
  }

  function follow(job) {
    clearTimeout(state.polling);
    renderStages(job);
    renderLiveViews(job);
    if (job.finished_bool) {
      store(JOB_KEY, null);
      $('av-build').disabled = !state.file;
      if (job.success_bool === false) message(job.error_string || 'failed', true);
      else { message(''); loadLibrary(); }
      return;
    }
    state.polling = setTimeout(async () => {
      try {
        follow(await api('/api/ai/avatar-build/status/' + encodeURIComponent(job.task_id_string)));
      } catch (error) {
        message(error.message, true);
        state.polling = setTimeout(() => follow(job), 8000);
      }
    }, Math.max(2, Math.min(10, Number(job.retry_after_seconds_float) || 4)) * 1000);
  }

  /* ------------------------------------------------------------ library */

  async function loadLibrary() {
    const host = $('av-library');
    try {
      const data = await api('/api/ai/avatars');
      state.avatars = data.avatars_array || [];
    } catch (error) {
      host.innerHTML = `<div class="av-empty">${escapeHtml(error.message)}</div>`;
      return;
    }
    if (!state.avatars.length) {
      host.innerHTML = `<div class="av-empty" data-tip="No Avatars yet">${svg('<circle cx="12" cy="8" r="3.2"/><path d="M5.5 20c.8-3.6 3.4-5.5 6.5-5.5s5.7 1.9 6.5 5.5"/>')}</div>`;
      return;
    }
    host.innerHTML = state.avatars.map(item => `
      <article class="av-card" data-id="${escapeHtml(item.avatar_id)}" tabindex="0">
        <div class="av-card-media">${item.cover_url ? `<img src="${escapeHtml(item.cover_url)}" alt="" loading="lazy">` : SLOT_ICON}
          <div class="av-card-meta">
            <span class="av-pill" data-tip="Version">v${escapeHtml(item.current_version)}</span>
            ${item.view_count ? `<span class="av-pill" data-tip="Views">${svg('<rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/>')}${escapeHtml(item.view_count)}</span>` : ''}
          </div>
        </div>
        <div class="av-card-name">${escapeHtml(item.display_name)}</div>
      </article>`).join('');
  }

  async function openAvatar(id) {
    let profile;
    try {
      profile = (await api('/api/ai/avatars/' + encodeURIComponent(id))).avatar_object;
    } catch (error) {
      message(error.message, true);
      return;
    }
    state.current = profile;
    $('av-modal-name').textContent = profile.display_name;
    $('av-modal-version').textContent = 'v' + profile.version;
    const info = [profile.identity_prompt, profile.appearance, profile.body, profile.wardrobe].filter(Boolean).join(' · ');
    $('av-modal-info').dataset.tip = info.length > 140 ? info.slice(0, 137) + '…' : info;
    $('av-modal-info').title = info;
    const sheet = profile.sheet && profile.sheet.canonical_url;
    $('av-modal-sheet').hidden = !sheet;
    if (sheet) $('av-modal-sheet').href = sheet;
    const views = profile.views || {};
    const slots = SLOTS.filter(slot => views[slot]).concat(Object.keys(views).filter(slot => !SLOTS.includes(slot)));
    const refs = (profile.references || []).filter(ref => ref.media_type === 'image');
    $('av-modal-views').innerHTML = slots.length
      ? slots.map(slot => slotHtml(slot, {url: views[slot].canonical_url, status: (views[slot].qa || {}).status,
                                          score: (views[slot].qa || {}).identity_score})).join('')
      : refs.map(ref => slotHtml(ref.role, {url: ref.canonical_url, status: 'unchecked'})).join('');
    $('av-modal').hidden = false;
  }

  function bindLibrary() {
    $('av-library').addEventListener('click', event => {
      const card = event.target.closest('.av-card');
      if (card) openAvatar(card.dataset.id);
    });
    $('av-library').addEventListener('keydown', event => {
      const card = event.target.closest('.av-card');
      if (card && (event.key === 'Enter' || event.key === ' ')) { event.preventDefault(); openAvatar(card.dataset.id); }
    });
    $('av-refresh').addEventListener('click', loadLibrary);
    $('av-modal-close').addEventListener('click', () => { $('av-modal').hidden = true; });
    $('av-modal').addEventListener('click', event => { if (event.target === $('av-modal')) $('av-modal').hidden = true; });
    $('av-modal-copy').addEventListener('click', async () => {
      if (!state.current) return;
      const value = state.current.avatar_id + '@' + state.current.version;
      try { await navigator.clipboard.writeText(value); $('av-modal-copy').dataset.tip = 'Copied'; }
      catch (_) { $('av-modal-copy').dataset.tip = value; }
      setTimeout(() => { $('av-modal-copy').dataset.tip = 'Copy Avatar id'; }, 1600);
    });
    document.addEventListener('click', event => {
      const picture = event.target.closest('img[data-full]');
      if (!picture) return;
      const box = $('av-lightbox');
      box.querySelector('img').src = picture.dataset.full;
      box.hidden = false;
    });
    $('av-lightbox').addEventListener('click', () => { $('av-lightbox').hidden = true; });
    document.addEventListener('keydown', event => {
      if (event.key === 'Escape') { $('av-lightbox').hidden = true; $('av-modal').hidden = true; }
    });
  }

  async function resume() {
    const id = recall(JOB_KEY);
    renderStages(null);
    renderLiveViews(null);
    if (!id) return;
    try { follow(await api('/api/ai/avatar-build/status/' + encodeURIComponent(id))); }
    catch (_) { store(JOB_KEY, null); }
  }

  document.addEventListener('DOMContentLoaded', () => {
    bindDrop();
    bindLibrary();
    $('av-build').addEventListener('click', build);
    resume();
    loadLibrary();
  });
})();
