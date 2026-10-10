// Task page V3 shell: one task, one Unity viewer (Task page · V3, 2026-10-10).
// The task id is the only input. Queue, stage, progress and the viewer to open
// come from /api/task/<id>/v3-view, resolved on the server; a run id is never
// read from this page's URL. Strings go through I18n.t() (Localization · V3
// owns the dictionaries); the built-in English/Russian lines are only fallbacks.
const BUILD = 'tv3-20261010.14';
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const MT_RUN = /^[0-9a-f]{20}$/;
const UNITY_PAGE = '/api/mt/unity/test/index.html';
const $ = (id) => document.getElementById(id);
const els = {
  stage: $('tv3-stage'), viewer: $('tv3-viewer'), card: $('tv3-card'), steps: $('tv3-steps'),
  bar: document.querySelector('#tv3-card .tv3-bar'), fill: $('tv3-fill'), line: $('tv3-line'),
  pct: $('tv3-pct'), chip: $('tv3-chip'), classic: $('tv3-classic'), share: $('tv3-share'),
  full: $('tv3-full'), build: $('tv3-build'),
};
const params = new URLSearchParams(location.search);
const taskId = String(params.get('id') || '').trim().toLowerCase();

const FALLBACK = {
  en: {
    taskv3_queued: 'In queue', taskv3_ahead: '{count} ahead', taskv3_next: 'next up',
    taskv3_processing: 'Processing', taskv3_rigging: 'Rig and animations', taskv3_ready: 'Ready',
    taskv3_failed: 'Processing failed', taskv3_missing: 'Task not found or private',
    taskv3_offline: 'Offline, retrying…', taskv3_copied: 'Link copied', taskv3_review: 'Needs review',
    taskv3_preparing: 'Preparing the model for the viewer',
    taskv3_unavailable: '3D model unavailable',
    live_stage_cl_prepare: 'Preparing the model', live_stage_cl_openpose: 'Finding arms, legs and fingers',
    live_stage_cl_pose: 'Straightening the pose', live_stage_cl_rig: 'Rigging', live_stage_cl_retarget: 'Animations',
    live_stage_cl_export: 'Unity package', live_stage_cl_preview: 'Preview video', live_stage_cl_finish: 'Packing the results',
  },
  ru: {
    taskv3_queued: 'В очереди', taskv3_ahead: 'впереди {count}', taskv3_next: 'следующая',
    taskv3_processing: 'Обработка', taskv3_rigging: 'Риг и анимации', taskv3_ready: 'Готово',
    taskv3_failed: 'Не удалось обработать', taskv3_missing: 'Задача не найдена или закрыта',
    taskv3_offline: 'Нет связи, повторяем…', taskv3_copied: 'Ссылка скопирована', taskv3_review: 'Нужна проверка',
    taskv3_preparing: 'Готовим модель для вьювера',
    taskv3_unavailable: '3D-модель недоступна',
    live_stage_cl_prepare: 'Готовлю модель', live_stage_cl_openpose: 'Ищу руки, ноги и пальцы',
    live_stage_cl_pose: 'Выравниваю позу', live_stage_cl_rig: 'Ригаю', live_stage_cl_retarget: 'Анимации',
    live_stage_cl_export: 'Пакет для Unity', live_stage_cl_preview: 'Превью-видео', live_stage_cl_finish: 'Упаковываю результат',
  },
};

function uiLang() {
  let lang = window.I18n && window.I18n.currentLang;
  if (!lang) { try { lang = localStorage.getItem('autorig_lang'); } catch (e) { lang = ''; } }
  return String(lang || navigator.language || 'en').slice(0, 2).toLowerCase();
}

function tr(key, replacements) {
  const i18n = window.I18n;
  if (i18n && typeof i18n.has === 'function' && i18n.has(key)) return i18n.t(key, replacements);
  let text = (FALLBACK[uiLang()] || FALLBACK.en)[key] || FALLBACK.en[key] || key;
  for (const [name, value] of Object.entries(replacements || {})) text = text.split(`{${name}}`).join(String(value));
  return text;
}

let timer = 0;
let shown = null; // {kind, run, revision}
let lastState = null;
let unavailable = false; // a finished task whose model could not be fetched stays labelled so

function setSteps(map) {
  for (const li of els.steps.children) {
    li.className = map[li.dataset.step] || '';
  }
}

function setProgress(value, waiting) {
  const pct = Math.max(0, Math.min(100, Math.round(Number(value || 0) * 100)));
  els.fill.style.width = `${pct}%`;
  els.bar.classList.toggle('wait', !!waiting);
  els.bar.setAttribute('aria-valuenow', String(waiting ? 0 : pct));
  els.pct.textContent = waiting ? '' : `${pct}%`;
}

function viewerUrl(viewer) {
  if (!viewer || viewer.page !== UNITY_PAGE) return null;
  const url = new URL(UNITY_PAGE, location.origin);
  const p = viewer.params || {};
  if (viewer.kind === 'mt-run') {
    if (!MT_RUN.test(String(p.run || ''))) return null;
    url.searchParams.set('run', p.run);
  } else if (viewer.kind === 'legacy') {
    const api = String(viewer.api_path || '');
    if (api !== `/api/task-viewer/${taskId}` || p.run !== 'task') return null;
    url.searchParams.set('api', location.origin + api);
    url.searchParams.set('run', 'task');
  } else {
    return null;
  }
  // Session agent V3: every task has its public session agent; the viewer asks /api/mt/task-agent/<id> for its run
  // (a classic task gets a lightweight one) and greets in the page's language.
  url.searchParams.set('agent_task', taskId);
  url.searchParams.set('agent_lang', agentLang());
  return url;
}

function agentLang() {
  const lang = (window.I18n && window.I18n.currentLang) || document.documentElement.lang || uiLang();
  return String(lang || 'en').slice(0, 2).toLowerCase();
}

// Session agent V3 (owner 2026-10-10: «вот это всё должен аватар показывать, а не отдельная строчка какая то»): the
// task's stage and progress go to the session agent in the viewer: a ring around its call window, the percent in its
// header, each new stage told by it as a short line. The pill stays hidden while the agent shows them.
function agentStatus(state, status, finished, stageTitle) {
  let bridge = null;
  try { bridge = els.viewer.contentWindow && els.viewer.contentWindow.autorigUnity; } catch (e) { return false; }
  if (!bridge || typeof bridge.agentStatus !== 'function') return false;
  const active = !finished && status !== 'error';
  let line = '';
  // Session agent V3 (browser language): the queue position is told again whenever it moves
  if (status === 'created') line = `${tr('taskv3_queued')} · ${state.queue && state.queue.ahead > 0 ? tr('taskv3_ahead', { count: state.queue.ahead }) : tr('taskv3_next')}`;
  else if (status === 'processing') line = stageTitle || tr('taskv3_rigging');
  else if (status === 'needs_review') line = tr('taskv3_review');
  else if (status === 'error') line = tr('taskv3_failed');
  else if (status === 'done') line = tr('taskv3_ready');
  try {
    return bridge.agentStatus({
      active, progress: Number(state.progress || 0), status, stage: state.stage, queue: state.queue || null,
      key: `${status}:${state.stage || ''}:${status === 'created' && state.queue ? state.queue.ahead : ''}`,
      line, mood: status === 'error' ? 'oops' : (active ? 'think' : 'excited'),
    }) !== false;
  } catch (e) { return false; }
}

function showViewer(viewer) {
  const url = viewerUrl(viewer);
  if (!url) return false;
  const run = url.searchParams.get('run');
  if (shown && shown.kind === viewer.kind && shown.run === run) {
    if (shown.revision !== viewer.revision) {
      shown.revision = viewer.revision;
      const bridge = els.viewer.contentWindow && els.viewer.contentWindow.autorigUnity;
      if (bridge && typeof bridge.send === 'function') bridge.send('LoadRun', run);
      else els.viewer.src = url.toString();
    }
    return true;
  }
  shown = { kind: viewer.kind, run, revision: viewer.revision };
  els.viewer.src = url.toString();
  els.viewer.hidden = false;
  return true;
}

// A finished task whose model cannot be loaded still shows what the server
// published for it: the preview video, else the poster (the same og: media the
// server put in this page's head). Same-origin paths only.
function headMedia(property) {
  const meta = document.querySelector(`meta[property="${property}"]`);
  if (!meta || !meta.content) return '';
  try {
    const url = new URL(meta.content, location.origin);
    return url.pathname.startsWith(`/api/video/${taskId}`) || url.pathname.startsWith(`/api/thumb/${taskId}`)
      ? url.pathname : '';
  } catch (e) { return ''; }
}

let mediaShown = false;
const mediaFailed = new Set();
function showMedia() {
  if (mediaShown) return true;
  const video = mediaFailed.has('video') ? '' : headMedia('og:video');
  const poster = mediaFailed.has('image') ? '' : headMedia('og:image');
  if (!video && !poster) return false;
  const box = document.createElement('div');
  box.className = 'tv3-media';
  const el = document.createElement(video ? 'video' : 'img');
  if (video) {
    Object.assign(el, { src: video, muted: true, autoplay: true, loop: true, playsInline: true, controls: true });
    if (poster) el.poster = poster;
  } else {
    el.src = poster;
    el.alt = '';
  }
  // A media file that is gone too must not leave a broken frame: fall back to the next one, then to the card.
  el.addEventListener('error', () => {
    mediaFailed.add(video ? 'video' : 'image');
    box.remove();
    mediaShown = false;
    if (lastState) render(lastState);
  }, { once: true });
  box.appendChild(el);
  els.viewer.parentNode.insertBefore(box, els.viewer);
  mediaShown = true;
  return true;
}

function render(state) {
  if (!state || state.schema !== 'autorig.task-page-v3/1') throw new Error('contract');
  lastState = state;
  const status = state.status;
  const viewer = state.viewer;
  const queue = state.queue;
  const v3 = state.v3;
  const progress = Number(state.progress || 0);
  if (state.admin || params.get('live') === '1') {
    els.build.hidden = false;
    els.build.textContent = `${BUILD}${state.pipeline === 'v3' ? ' · V3' : ''}`;
  }
  els.card.classList.toggle('failed', status === 'error');

  const finished = status === 'done' || status === 'needs_review';
  const hasViewer = !!viewer && showViewer(viewer);
  if (hasViewer) unavailable = false;
  else if (state.model && state.model.state === 'unavailable') unavailable = true;
  // the classic page carries downloads and the restart button: point at it when this page cannot help
  els.classic.classList.toggle('ok', unavailable || status === 'error');
  const rigged = hasViewer && viewer.rigged;
  setSteps({
    queue: status === 'created' ? 'run' : 'done',
    process: status === 'created' ? '' : status === 'processing' ? 'run' : status === 'error' ? 'fail' : 'done',
    model: hasViewer ? 'done' : (state.model && state.model.state === 'warming' ? 'run' : ''),
    rig: rigged ? (status === 'needs_review' ? 'fail' : 'done') : (hasViewer && status !== 'error' ? 'run' : ''),
  });

  // V3 tasks name their stage (Intake's titles); classic tasks show the queue and a percent.
  const stageTitle = state.pipeline === 'v3' && state.stage_title ? `V3 · ${state.stage_title}`
    : (state.live_stage ? tr(`live_stage_${state.live_stage}`) : '');
  let line = '';
  let waiting = false;
  if (status === 'created') {
    line = stageTitle || `${tr('taskv3_queued')} · ${queue && queue.ahead > 0 ? tr('taskv3_ahead', { count: queue.ahead }) : tr('taskv3_next')}`;
    waiting = true;
  } else if (status === 'processing') {
    line = stageTitle || tr('taskv3_processing');
  } else if (status === 'error') {
    line = tr('taskv3_failed');
  } else if (status === 'needs_review') {
    line = tr('taskv3_review');
  } else if (!hasViewer && unavailable) {
    line = tr('taskv3_unavailable');
  } else if (!hasViewer) {
    line = tr('taskv3_preparing');
    waiting = true;
  } else {
    line = tr('taskv3_ready');
  }
  els.line.textContent = line;
  els.card.title = (v3 && v3.message) || '';
  setProgress(finished ? 1 : progress, waiting);

  // The viewer owns the screen as soon as any model exists; progress moves to a chip.
  const media = !hasViewer && unavailable && finished && showMedia();
  els.card.hidden = hasViewer || media;
  let chip = '';
  if (media) chip = tr('taskv3_unavailable');
  else if (hasViewer && status === 'needs_review') chip = tr('taskv3_review');
  else if (hasViewer && status === 'error') chip = tr('taskv3_failed');
  else if (hasViewer && !finished) chip = `${stageTitle || (status === 'created' ? tr('taskv3_queued') : tr('taskv3_rigging'))} · ${Math.round(progress * 100)}%`;
  else if (hasViewer && !rigged && !finished && state.model && state.model.state === 'warming') chip = tr('taskv3_rigging');
  els.chip.hidden = !chip;
  els.chip.textContent = chip;
  els.chip.title = (v3 && v3.message) || '';
  // Session agent V3: the agent tells the stage and shows the progress; no separate pill while it does
  if (hasViewer && agentStatus(state, status, finished, stageTitle)) els.chip.hidden = true;

  if ((finished && rigged) || status === 'error') return 30000;
  return status === 'created' ? 4000 : 2500;
}

function showMissing() {
  els.line.textContent = tr('taskv3_missing');
  els.card.classList.add('failed');
  setProgress(0, false);
}

// Live processing V3: a classic task reports outputs late, so its real stage and progress come from the converter's
// own log, followed by the MT service (mt/live_classic.py). The same request starts the voxelization and the first
// skeleton for the viewer while the task is still in the queue. Without it the clock-based progress stays.
async function mergeLive(state) {
  if (state.pipeline !== 'classic' || (state.status !== 'processing' && state.status !== 'created')) return;
  const ctl = new AbortController();
  const abort = setTimeout(() => ctl.abort(), 4000);
  try {
    const r = await fetch(`/api/mt/classic/${encodeURIComponent(taskId)}/state`, {
      credentials: 'same-origin', cache: 'no-store', signal: ctl.signal,
    });
    if (!r.ok || state.status !== 'processing') return;
    const st = (await r.json()).state || {};
    const rows = st.stages || [];
    const current = rows.find((x) => x.status === 'running');
    if (!current && !rows.some((x) => x.status === 'done')) return; // no worker line yet
    state.progress = Math.max(0.01, Math.min(0.99, Number(st.progress) || 0));
    state.progress_basis = 'stages';
    if (current) state.live_stage = current.id;
    if (st.eta_s) state.eta_s = Math.round(Number(st.eta_s));
  } catch (e) { /* the clock-based progress stays */ } finally { clearTimeout(abort); }
}

async function poll() {
  clearTimeout(timer);
  if (!UUID.test(taskId)) { showMissing(); return; }
  try {
    const response = await fetch(`/api/task/${encodeURIComponent(taskId)}/v3-view`, {
      credentials: 'same-origin', cache: 'no-store', headers: { Accept: 'application/json' },
    });
    if (response.status === 404) { showMissing(); return; }
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const doc = await response.json();
    await mergeLive(doc);
    const next = render(doc);
    if (next > 0) timer = setTimeout(poll, next);
  } catch (error) {
    els.line.textContent = tr('taskv3_offline');
    timer = setTimeout(poll, 6000);
  }
}

function measureHeader() {
  const header = document.getElementById('site-header');
  const h = header ? Math.round(header.getBoundingClientRect().height) : 0;
  document.body.style.setProperty('--tv3-hdr', `${h || 72}px`);
}

function wireTools() {
  els.classic.href = UUID.test(taskId) ? `/task?id=${encodeURIComponent(taskId)}&classic=1` : '/';
  els.share.addEventListener('click', async () => {
    const url = `${location.origin}/task?id=${encodeURIComponent(taskId)}`;
    try {
      if (navigator.share && matchMedia('(pointer:coarse)').matches) await navigator.share({ url });
      else await navigator.clipboard.writeText(url);
      els.share.classList.add('ok');
      els.share.title = tr('taskv3_copied');
      setTimeout(() => els.share.classList.remove('ok'), 1600);
    } catch (e) { /* the user closed the share sheet */ }
  });
  els.full.addEventListener('click', () => {
    if (document.fullscreenElement) document.exitFullscreen().catch(() => {});
    else els.stage.requestFullscreen?.().catch(() => {});
  });
}

measureHeader();
addEventListener('resize', measureHeader, { passive: true });
addEventListener('load', measureHeader, { once: true });
addEventListener('pagehide', () => clearTimeout(timer));
wireTools();
try {
  const ready = window.I18n && typeof window.I18n.init === 'function' ? window.I18n.init() : null;
  if (ready && typeof ready.then === 'function') ready.then(() => { if (lastState) render(lastState); }, () => {});
} catch (e) { /* the header stays in its server language */ }
addEventListener('languageChanged', () => { if (lastState) render(lastState); });
window.autorigTaskPage = { build: BUILD, taskId, poll };
poll();
