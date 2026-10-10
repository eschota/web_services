// Task page V3 shell: one task, one Unity viewer (Task page · V3, 2026-10-10).
// The task id is the only input. Queue, stage, progress and the viewer to open
// come from /api/task/<id>/v3-view, resolved on the server; a run id is never
// read from this page's URL. Strings go through I18n.t() (Localization · V3
// owns the dictionaries); the built-in English/Russian lines are only fallbacks.
const BUILD = 'tv3-20261010.5';
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
  },
  ru: {
    taskv3_queued: 'В очереди', taskv3_ahead: 'впереди {count}', taskv3_next: 'следующая',
    taskv3_processing: 'Обработка', taskv3_rigging: 'Риг и анимации', taskv3_ready: 'Готово',
    taskv3_failed: 'Не удалось обработать', taskv3_missing: 'Задача не найдена или закрыта',
    taskv3_offline: 'Нет связи, повторяем…', taskv3_copied: 'Ссылка скопирована', taskv3_review: 'Нужна проверка',
    taskv3_preparing: 'Готовим модель для вьювера',
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
    url.searchParams.set('agent', '0');
  } else {
    return null;
  }
  return url;
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
  const rigged = hasViewer && viewer.rigged;
  setSteps({
    queue: status === 'created' ? 'run' : 'done',
    process: status === 'created' ? '' : status === 'processing' ? 'run' : status === 'error' ? 'fail' : 'done',
    model: hasViewer ? 'done' : (state.model && state.model.state === 'warming' ? 'run' : ''),
    rig: rigged ? (status === 'needs_review' ? 'fail' : 'done') : (hasViewer && status !== 'error' ? 'run' : ''),
  });

  // V3 tasks name their stage (Intake's titles); classic tasks show the queue and a percent.
  const stageTitle = state.pipeline === 'v3' && state.stage_title ? `V3 · ${state.stage_title}` : '';
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
  els.card.hidden = hasViewer;
  let chip = '';
  if (hasViewer && status === 'needs_review') chip = tr('taskv3_review');
  else if (hasViewer && status === 'error') chip = tr('taskv3_failed');
  else if (hasViewer && !finished) chip = `${stageTitle || (status === 'created' ? tr('taskv3_queued') : tr('taskv3_rigging'))} · ${Math.round(progress * 100)}%`;
  else if (hasViewer && !rigged && state.model && state.model.state === 'warming') chip = tr('taskv3_rigging');
  els.chip.hidden = !chip;
  els.chip.textContent = chip;
  els.chip.title = (v3 && v3.message) || '';

  if ((finished && rigged) || status === 'error') return 30000;
  return status === 'created' ? 4000 : 2500;
}

function showMissing() {
  els.line.textContent = tr('taskv3_missing');
  els.card.classList.add('failed');
  setProgress(0, false);
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
    const next = render(await response.json());
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
