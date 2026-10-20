// Task page V3 shell: one task, one Unity viewer (Task page · V3, 2026-10-10).
// The task id is the only input. Queue, stage, progress and the viewer to open
// come from /api/task/<id>/v3-view, resolved on the server; a run id is never
// read from this page's URL. Strings go through I18n.t() (Localization · V3
// owns the dictionaries); the built-in English/Russian lines are only fallbacks.
const BUILD = 'tv3-20261020.3';
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const MT_RUN = /^[0-9a-f]{20}$/;
const UNITY_PAGE = '/api/mt/unity/test/index.html';
const $ = (id) => document.getElementById(id);
const els = {
  stage: $('tv3-stage'), viewer: $('tv3-viewer'), card: $('tv3-card'), steps: $('tv3-steps'),
  bar: document.querySelector('#tv3-card .tv3-bar'), fill: $('tv3-fill'), line: $('tv3-line'),
  pct: $('tv3-pct'), chip: $('tv3-chip'), classic: $('tv3-classic'), share: $('tv3-share'),
  full: $('tv3-full'), build: $('tv3-build'), rerig: $('tv3-rerig'),
  status: $('tv3-status'), statusTitle: $('tv3-status-title'), statusSub: $('tv3-status-sub'),
  statusPct: $('tv3-status-pct'), statusFill: $('tv3-status-fill'), statusSteps: $('tv3-status-steps'),
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
    taskv3_step_queue: 'Queue', taskv3_step_source: 'Source', taskv3_step_analysis: 'Analysis', taskv3_step_rig: 'Rig',
    taskv3_step_retarget: 'Animations', taskv3_step_qa: 'Check', taskv3_step_publish: 'Publish',
    taskv3_status_step: 'step {n} of {total}', taskv3_status_ready_sub: 'The rig is in the viewer, animations are playing',
    taskv3_status_review_sub: 'The rig is published; the quality check asks for a look', taskv3_status_fail_sub: 'Tap for the classic page and a restart',
    taskv3_status_details: 'tap for details',
    taskv3_tool_rerig: 'Re-rig with the new pipeline', taskv3_rerig_queued: 'Re-rig queued',
    taskv3_rerig_running: 'Re-rigging with the new pipeline…', taskv3_rerig_failed: 'Re-rig failed; tap to try again',
    taskv3_rerig_done: 'Re-rigged with the current pipeline', taskv3_rerig_later: 'Re-rig unavailable right now',
    live_stage_cl_prepare: 'Preparing the model', live_stage_cl_openpose: 'Finding arms, legs and fingers',
    live_stage_cl_pose: 'Straightening the pose', live_stage_cl_rig: 'Rigging', live_stage_cl_retarget: 'Animations',
    live_stage_cl_export: 'Unity package', live_stage_cl_preview: 'Preview video', live_stage_cl_finish: 'Packing the results',
    viewer_canvas_tab_preview: 'Preview: the animated model · Tab', viewer_canvas_tab_blueprint: 'Blueprint: edit the skeleton · Tab',
    viewer_canvas_tab_canvas: 'Canvas: the parts in colour, 3D by parts · Tab', viewer_parts_children: 'Parts as 3D models',
    viewer_parts_of: 'Part «{part}» of', viewer_parts_open: 'Open the original task',
  },
  ru: {
    taskv3_queued: 'В очереди', taskv3_ahead: 'впереди {count}', taskv3_next: 'следующая',
    taskv3_processing: 'Обработка', taskv3_rigging: 'Риг и анимации', taskv3_ready: 'Готово',
    taskv3_failed: 'Не удалось обработать', taskv3_missing: 'Задача не найдена или закрыта',
    taskv3_offline: 'Нет связи, повторяем…', taskv3_copied: 'Ссылка скопирована', taskv3_review: 'Нужна проверка',
    taskv3_preparing: 'Готовим модель для вьювера',
    taskv3_unavailable: '3D-модель недоступна',
    taskv3_step_queue: 'Очередь', taskv3_step_source: 'Исходник', taskv3_step_analysis: 'Анализ', taskv3_step_rig: 'Риг',
    taskv3_step_retarget: 'Анимации', taskv3_step_qa: 'Проверка', taskv3_step_publish: 'Публикация',
    taskv3_status_step: 'шаг {n} из {total}', taskv3_status_ready_sub: 'Риг во вьювере, анимации играют',
    taskv3_status_review_sub: 'Риг опубликован; проверка качества просит взглянуть', taskv3_status_fail_sub: 'Нажмите: классическая страница и перезапуск',
    taskv3_status_details: 'нажмите для подробностей',
    taskv3_tool_rerig: 'Переригать новой версией пайплайна', taskv3_rerig_queued: 'Перериг в очереди',
    taskv3_rerig_running: 'Переригаю новой версией пайплайна…', taskv3_rerig_failed: 'Перериг не удался; нажмите, чтобы повторить',
    taskv3_rerig_done: 'Переригано текущей версией пайплайна', taskv3_rerig_later: 'Перериг сейчас недоступен',
    live_stage_cl_prepare: 'Готовлю модель', live_stage_cl_openpose: 'Ищу руки, ноги и пальцы',
    live_stage_cl_pose: 'Выравниваю позу', live_stage_cl_rig: 'Ригаю', live_stage_cl_retarget: 'Анимации',
    live_stage_cl_export: 'Пакет для Unity', live_stage_cl_preview: 'Превью-видео', live_stage_cl_finish: 'Упаковываю результат',
    viewer_canvas_tab_preview: 'Превью: анимированная модель · Tab', viewer_canvas_tab_blueprint: 'Блюпринт: править скелет · Tab',
    viewer_canvas_tab_canvas: 'Канвас: части в цвете, 3D по частям · Tab', viewer_parts_children: 'Части как 3D-модели',
    viewer_parts_of: 'Часть «{part}» модели', viewer_parts_open: 'Открыть исходную задачу',
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

// Top status banner (owner 2026-10-16: «большую кнопку сверху с прогрессбаром и статусом текущего рига, заметную
// сверху, пока ждём непонятно что происходит»): the stage, the percent, the bar and the pipeline steps over the viewer.
// It stays while the task is queued or processing, turns amber for needs_review, red for a failure, shows «Ready»
// for a few seconds when the rig is done and then hides. A tap toggles the step strip (remembered per browser).
const STEPS = ['queue', 'source', 'analysis', 'rig', 'retarget', 'qa', 'publish'];
const STEP_OF = {
  intake: 0, normalization: 0, dispatch: 0, pending_register: 0, pending_submit: 0, queued: 0, starting: 0, resuming: 0,
  retrying: 0, source: 1, source_registered: 1, model_validation: 1, generation: 1, running: 1, analysis: 2, rig: 3,
  retarget: 4, qa: 5, qa_review: 5, publish: 6, complete: 6,
};
let statusHide = 0;
// Tab opens the blueprint from the page as well (owner 2026-10-19: «редактор у нас на Tab, куда он исчез со страницы»): a click on
// the presentation cards or the status bar moves the focus out of the viewer iframe, and Tab then walks the page's links.
// Not while typing; the viewer's own handler takes the key when the iframe has the focus.
document.addEventListener('keydown', function (e) {
  if (e.key !== 'Tab' || e.altKey || e.ctrlKey || e.metaKey) return;
  const a = document.activeElement;
  if (a && (a.tagName === 'INPUT' || a.tagName === 'TEXTAREA' || a.tagName === 'SELECT' || a.isContentEditable)) return;
  const fr = els.viewer;
  if (!fr || a === fr) return;
  let w, doc;
  try { w = fr.contentWindow; doc = w && w.document; } catch (x) { return; }
  if (!doc || !w.autorigBlueprint) return;
  e.preventDefault(); e.stopImmediatePropagation();
  try { const c = doc.getElementById('unity-canvas'); if (c) c.focus(); } catch (x) { /* ignore */ }
  try { w.dispatchEvent(new KeyboardEvent('keydown', {key: 'Tab', code: 'Tab', bubbles: true, cancelable: true, shiftKey: e.shiftKey})); } catch (x) { /* ignore */ }
}, true);

let statusLingered = '';   // the finished banner (done / review) shows once per page load, then the viewer owns the screen
let statusCompact = true;   // the step strip is closed by default (owner 2026-10-19: compact); a click opens it
try { statusCompact = localStorage.getItem('tv3_status_compact') !== '0'; } catch (e) { /* private mode */ }
function topStatus(state, status, finished, hasViewer, stageTitle, progress, waiting) {
  const el = els.status;
  if (!el) return false;
  const active = status === 'created' || status === 'processing' || (!finished && status !== 'error' && hasViewer === false);
  let kind = active ? (waiting ? 'wait' : 'run') : status === 'error' ? 'fail' : status === 'needs_review' ? 'review' : finished ? 'done' : '';
  if (!kind) { el.hidden = true; return false; }
  const step = active ? (STEP_OF[String(state.stage_key || '')] ?? (status === 'created' ? 0 : 3)) : STEPS.length;
  let title = stageTitle ? stageTitle.replace(/^V3 · /, '') : '';
  let sub = '';
  if (kind === 'wait' || status === 'created') {
    title = title || tr('taskv3_queued');
    sub = state.queue && state.queue.ahead > 0 ? tr('taskv3_ahead', { count: state.queue.ahead }) : tr('taskv3_next');
  } else if (kind === 'run') {
    title = title || tr('taskv3_rigging');
    sub = `${tr('taskv3_status_step', { n: Math.min(step + 1, STEPS.length), total: STEPS.length })}${state.eta_s ? ` · ~${Math.max(1, Math.round(state.eta_s / 60))} min` : ''}`;
  } else if (kind === 'review') { title = tr('taskv3_review'); sub = tr('taskv3_status_review_sub'); }
  else if (kind === 'fail') { title = tr('taskv3_failed'); sub = tr('taskv3_status_fail_sub'); }
  else { title = tr('taskv3_ready'); sub = tr('taskv3_status_ready_sub'); }
  const pct = Math.max(0, Math.min(100, Math.round((kind === 'done' || kind === 'review' ? 1 : Number(progress || 0)) * 100)));
  els.statusTitle.textContent = title;
  els.statusSub.textContent = sub;
  els.statusPct.textContent = kind === 'wait' ? '' : `${pct}%`;
  els.statusFill.style.width = `${kind === 'wait' ? 0 : pct}%`;
  if (els.statusSteps.children.length !== STEPS.length) {
    els.statusSteps.textContent = '';
    for (const s of STEPS) { const li = document.createElement('li'); li.dataset.step = s; li.textContent = tr(`taskv3_step_${s}`); els.statusSteps.appendChild(li); }
  }
  Array.from(els.statusSteps.children).forEach((li, i) => { li.className = i < step ? 'done' : (i === step && active) ? 'run' : ''; });
  el.className = `tv3-status ${kind}${statusCompact ? ' compact' : ''}`;
  el.title = kind === 'fail' ? tr('taskv3_status_fail_sub') : tr('taskv3_status_details');
  if ((kind === 'done' || kind === 'review') && statusLingered === kind) return true;   // already shown and faded: every poll must not bring it back
  el.hidden = false;
  clearTimeout(statusHide);
  if (kind === 'done' || kind === 'review') {
    // the finished states linger, then fade: the viewer owns the screen
    statusLingered = kind;
    statusHide = setTimeout(() => { el.classList.add('fading'); setTimeout(() => { el.hidden = true; el.classList.remove('fading'); }, 400); }, kind === 'done' ? 7000 : 15000);
  }
  return true;
}
if (els.status) {
  els.status.addEventListener('click', () => {
    if (els.status.classList.contains('fail')) { location.href = els.classic.href || `/task?id=${encodeURIComponent(taskId)}&classic=1`; return; }
    statusCompact = !statusCompact;
    els.status.classList.toggle('compact', statusCompact);
    els.status.setAttribute('aria-expanded', statusCompact ? 'false' : 'true');
    try { localStorage.setItem('tv3_status_compact', statusCompact ? '1' : '0'); } catch (e) { /* private mode */ }
  });
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
  } else if (viewer.kind === 'picture') {
    // Picture intake V3 (owner 2026-10-16: «вьювер V3 сразу должен открываться с пустой сценой и показывать картинку»):
    // the viewer opens on the black scene with the picture (and its cutout when it exists) while the model is generated
    const pic = String(p.picture || '');
    if (!/^https?:\/\//.test(pic) && !pic.startsWith('/')) return null;
    url.searchParams.set('picture', pic);
    if (p.cutout) url.searchParams.set('cutout', String(p.cutout));
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
  // the viewer's UI language = the language this page is rendered in (owner 2026-10-16: «у пользователя по дефолту
  // должен быть английский язык, а не русский»): the server's <html lang> first, then the i18n module, then the
  // browser; never a Russian default
  const htmlLang = String(document.documentElement.lang || '').slice(0, 2).toLowerCase();
  const lang = (/^[a-z]{2}$/.test(htmlLang) && htmlLang) || (window.I18n && window.I18n.currentLang) || uiLang();
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
  const run = url.searchParams.get('run') || (viewer.kind === 'picture' ? 'picture' : null);
  if (shown && shown.kind === viewer.kind && shown.run === run) {
    if (shown.revision !== viewer.revision) {
      shown.revision = viewer.revision;
      const swap = () => {
        const bridge = els.viewer.contentWindow && els.viewer.contentWindow.autorigUnity;
        if (bridge && typeof bridge.send === 'function' && viewer.kind !== 'picture') bridge.send('LoadRun', run);
        else els.viewer.src = url.toString();
      };
      // Scene gate (owner 2026-10-17 «полноценная сцена грузилась только после того как риг готов»): a new model version
      // of a rig already on screen (a re-skin, the full mesh after the light one) waits behind the presentation; the
      // first rigged model never waits, the gate needs it to open (task-v31-stages.js owns window.autorigSceneGate)
      const gate = window.autorigSceneGate;
      if (gate && !gate.open && shown.rigged && viewer.rigged) gate.whenOpen(swap, `LoadRun ${run} (model swap)`);
      else swap();
    }
    shown.rigged = !!viewer.rigged;
    return true;
  }
  shown = { kind: viewer.kind, run, revision: viewer.revision, rigged: !!viewer.rigged };
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
  const stageTitle = state.pipeline === 'v3' ? v3StageTitle(state)
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
  els.card.title = line;   // never the server's own (Russian) message
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
  els.chip.title = chip;
  // Session agent V3: the agent tells the stage and shows the progress; no separate pill while it does
  if (hasViewer && agentStatus(state, status, finished, stageTitle)) els.chip.hidden = true;
  // Picture intake V3: the stage into the iframe (picture-plate.js shows it under the picture while the model is made)
  if (hasViewer) { try { els.viewer.contentWindow && els.viewer.contentWindow.postMessage({ type: 'autorig.v3.stage', stage_key: state.stage_key || null, sub_key: state.sub_key || null, progress: Number(progress || 0), title: stageTitle || line, status }, location.origin); } catch (e) { /* ignore */ } }
  // the big top banner (owner 2026-10-16) while the rig is in the making; the small chip steps aside for it
  if (hasViewer && topStatus(state, status, finished, hasViewer, stageTitle, progress, waiting)) els.chip.hidden = true;
  else if (!hasViewer && els.status) els.status.hidden = true;

  renderParts(state.parts);

  // Re-rig button (owner 2026-10-19): the pipeline on the server is newer than this rig (a version bump, or an
  // install under the same version: the rig-path sha) -> the button; its own request's state while it runs.
  const rerigBusy = syncRerig(state.rig_version, status);
  if (rerigBusy) return 4000;
  if ((finished && rigged) || status === 'error') return 30000;
  return status === 'created' ? 4000 : 2500;
}

// ---------------------------------------------------------------- the viewer's three modes on the page (owner 2026-10-20)
// «режим Canvas … доступен только когда есть сегментация и так же на таб переключается и в UX»: the viewer (blueprint.js
// mode3) owns the state and posts {type: 'autorig.v3.mode', mode, canvas_ok, rigged, modes}; this strip mirrors it in the
// tool row and switches it with autorigBlueprint.setMode3(). Canvas shows only when the run has its parts.
const MODE_ICON = {
  preview: '<circle cx="8" cy="5" r="2.2"/><path d="M8 7.2v6.5M3.5 10.5h9M8 13.7l-3 6.3M8 13.7l3 6.3" opacity=".75"/><path d="M14.5 8.5v10l7.5-5z" fill="currentColor"/>',
  blueprint: '<circle cx="9" cy="5" r="2.2"/><path d="M9 7.2v7M4 10.5h10M9 14.2l-3 6M9 14.2l3 6"/><path d="M14.5 17.5l5-5 2.2 2.2-5 5-3 .8z"/>',
  canvas: '<path d="M5 4.5h7.5l-1 5.5H6z" opacity=".9"/><path d="M12.5 4.5H19l-.8 5.5h-6.7z" opacity=".55"/><path d="M6 11.5h5.6l-.6 8H6.8z" opacity=".55"/><path d="M12.4 11.5H18l-.8 8h-4.4z" opacity=".9"/>',
};
const modeEls = {};
let modeState = null;
let pageAnims = [];
let pageAnimTimer = 0;
function pageAnimHold(on) {
  if (!on) {
    clearInterval(pageAnimTimer); pageAnimTimer = 0;
    for (const a of pageAnims) { try { a.play(); } catch (e) { /* ignore */ } }
    pageAnims = [];
    return;
  }
  const hold = () => {
    try {
      for (const a of document.getAnimations()) {
        try {
          if (a.playState === 'running' && a.effect && a.effect.getTiming().iterations === Infinity) { a.pause(); pageAnims.push(a); }
        } catch (e) { /* ignore */ }
      }
    } catch (e) { /* ignore */ }
  };
  hold();
  if (!pageAnimTimer) pageAnimTimer = setInterval(hold, 1000);
}
function buildModes() {
  const tools = document.getElementById('tv3-tools');
  if (!tools || modeEls.box) return;
  const box = document.createElement('span');
  box.className = 'tv3-modes';
  box.hidden = true;
  box.setAttribute('role', 'tablist');
  for (const m of ['blueprint', 'preview', 'canvas']) {
    const b = document.createElement('button');
    b.type = 'button';
    b.className = 'tv3-tool tv3-mode';
    b.dataset.mode = m;
    b.setAttribute('role', 'tab');
    b.innerHTML = `<svg viewBox="0 0 24 24" aria-hidden="true">${MODE_ICON[m]}</svg>`;
    b.addEventListener('click', () => {
      let bp = null;
      try { bp = els.viewer.contentWindow && els.viewer.contentWindow.autorigBlueprint; } catch (e) { bp = null; }
      if (bp && typeof bp.setMode3 === 'function') bp.setMode3(m);
      try { els.viewer.focus(); } catch (e) { /* ignore */ }
    });
    box.appendChild(b);
    modeEls[m] = b;
  }
  const anchor = els.build || null;
  tools.insertBefore(box, anchor && anchor.parentNode === tools ? anchor : null);
  modeEls.box = box;
}
function paintModes() {
  buildModes();
  if (!modeEls.box) return;
  const d = modeState;
  modeEls.box.hidden = !(d && d.rigged);
  const canvasOn = !!(d && d.mode === 'canvas');
  // the Canvas is the result: a stage replay still playing over it (task-v31-stages.js) is skipped, as its own ⏭ does
  if (canvasOn && !els.stage.classList.contains('tv3-canvas')) {
    const scrub = document.querySelector('.v31-scrub');
    const skip = document.querySelector('.v31-skipbtn');
    if (scrub && !scrub.hidden && skip) { try { skip.click(); } catch (e) { /* ignore */ } }
  }
  els.stage.classList.toggle('tv3-canvas', canvasOn);
  pageAnimHold(canvasOn);
  if (!d) return;
  for (const m of ['blueprint', 'preview', 'canvas']) {
    const b = modeEls[m];
    const on = d.mode === m;
    b.hidden = m === 'canvas' ? !d.canvas_ok : false;
    b.classList.toggle('on', on);
    b.setAttribute('aria-selected', on ? 'true' : 'false');
    const t = tr(`viewer_canvas_tab_${m}`);
    b.title = t;
    b.setAttribute('aria-label', t);
  }
}
addEventListener('message', (e) => {
  if (e.origin !== location.origin || !e.data || e.data.type !== 'autorig.v3.mode') return;
  try { if (e.source !== els.viewer.contentWindow) return; } catch (x) { return; }
  modeState = { mode: e.data.mode, canvas_ok: !!e.data.canvas_ok, rigged: !!e.data.rigged };
  paintModes();
});
els.viewer && els.viewer.addEventListener('load', () => { modeState = null; paintModes(); });
// the message is sent on every change; this also catches a viewer that changed before the page listened
setInterval(() => {
  let bp = null;
  try { bp = els.viewer.contentWindow && els.viewer.contentWindow.autorigBlueprint; } catch (e) { return; }
  if (!bp || typeof bp.mode3 !== 'function') return;
  const d = bp.mode3();
  const next = { mode: d.mode, canvas_ok: !!d.canvas_ok, rigged: !!d.rigged };
  if (!modeState || modeState.mode !== next.mode || modeState.canvas_ok !== next.canvas_ok || modeState.rigged !== next.rigged) { modeState = next; paintModes(); }
}, 1500);

// ---------------------------------------------------------------- parts as tasks (owner 2026-10-20 «каждая модель —
// полноценный таск со окном навигации к оригиналу»): v3-view.parts {parent, children[]} -> a strip under the viewer:
// a child task links back to its original and its sibling parts; an original lists its parts' tasks.
const PART_EN = { hat: 'hat', beard: 'beard', hair: 'hair', coat: 'coat', cloak: 'cloak', skirt: 'skirt', trousers: 'trousers', boots: 'boots', mittens: 'mittens', belt: 'belt', head: 'head' };
const PROV_ICON = { tripo: '🔺', hunyuan_cloud: '☁️', farm_full: '🏭', farm_fast: '⚡' };
function partName(id, label) {
  const k = String(id || '').replace(/^prop:.*/, 'prop');
  const t = tr(`viewer_parts_label_${k}`);
  if (t !== `viewer_parts_label_${k}`) return t;
  return label || PART_EN[k] || String(id || '');
}
let partsKey = '';
// a child task's siblings are its parent's children (GET /api/task/<parent>/v3-view, once per parent)
const parentKids = {};
function loadParentKids(ptask) {
  if (parentKids[ptask]) return;
  parentKids[ptask] = { kids: [], title: '' };
  fetch(`/api/task/${encodeURIComponent(ptask)}/v3-view`, { credentials: 'same-origin', cache: 'no-store' })
    .then((r) => (r.ok ? r.json() : null))
    .then((d) => {
      if (!d) return;
      parentKids[ptask] = { kids: (d.parts && Array.isArray(d.parts.children)) ? d.parts.children : [], title: String(d.title || '') };
      partsKey = '';
      if (lastState) renderParts(lastState.parts);
    })
    .catch(() => {});
}
function renderParts(parts) {
  let strip = document.getElementById('tv3-parts');
  // v3-view.parts (backend parts_build_routes.parts_view): {parent {parent_task, parent_run, part, label, provider}, children[]}
  let parent = parts && parts.parent ? (typeof parts.parent === 'string' ? { task: parts.parent } : parts.parent) : null;
  if (parent && !parent.task && parent.parent_task) parent = { task: parent.parent_task, run: parent.parent_run, part: parent.part, label: parent.label, title: parent.title };
  if (parent && UUID.test(String(parent.task || ''))) {
    loadParentKids(parent.task);
    const pk = parentKids[parent.task];
    if (pk && pk.title && !parent.title) parent = { ...parent, title: pk.title };
  }
  const kids = (parts && Array.isArray(parts.children) ? parts.children : []).filter((c) => c && UUID.test(String(c.task || '')));
  const sibs = (parts && Array.isArray(parts.siblings) ? parts.siblings
    : parent && parentKids[parent.task] ? parentKids[parent.task].kids : []).filter((c) => c && UUID.test(String(c.task || '')));
  const show = !!(parent && UUID.test(String(parent.task || ''))) || kids.length > 0;
  const key = show ? JSON.stringify([parent, kids, sibs, uiLang()]) : '';
  if (key === partsKey) return;
  partsKey = key;
  if (!show) { if (strip) strip.hidden = true; return; }
  if (!strip) {
    strip = document.createElement('nav');
    strip.id = 'tv3-parts';
    strip.className = 'tv3-parts';
    const vp = els.viewer.closest('.tv3-viewport') || els.viewer.parentNode;
    vp.parentNode.insertBefore(strip, vp.nextSibling);
  }
  strip.hidden = false;
  strip.innerHTML = '';
  const chip = (task, text, color, cls) => {
    const a = document.createElement('a');
    a.className = `tv3-part${cls ? ` ${cls}` : ''}`;
    a.href = `/task?id=${encodeURIComponent(task)}`;
    const dot = document.createElement('i');
    dot.style.background = /^#[0-9a-f]{3,8}$/i.test(String(color || '')) ? color : '#9fd2ff';
    a.appendChild(dot);
    a.appendChild(document.createTextNode(text));
    return a;
  };
  if (parent) {
    const me = parts.part || parent.part || '';
    if (!parts.part_label && parent.label) parts = { ...parts, part_label: parent.label };
    const lab = document.createElement('span');
    lab.className = 'tv3-parts-lab';
    lab.textContent = tr('viewer_parts_of', { part: partName(me, parts.part_label) });
    strip.appendChild(lab);
    const up = chip(parent.task, `↖ ${parent.title || parent.task.slice(0, 8)}`, '#9fd2ff', 'up');
    up.title = tr('viewer_parts_open');
    strip.appendChild(up);
    for (const s of (sibs.length ? sibs : kids)) strip.appendChild(chip(s.task, `${partName(s.part, s.label)}${PROV_ICON[s.provider] ? ` ${PROV_ICON[s.provider]}` : ''}`, s.color, s.task === taskId ? 'me' : ''));
  } else {
    const lab = document.createElement('span');
    lab.className = 'tv3-parts-lab';
    lab.textContent = tr('viewer_parts_children');
    strip.appendChild(lab);
    for (const c of kids) strip.appendChild(chip(c.task, `${partName(c.part, c.label)}${PROV_ICON[c.provider] ? ` ${PROV_ICON[c.provider]}` : ''}`, c.color, ''));
  }
}

let rerigPressed = false;
function syncRerig(rv, status) {
  const b = els.rerig;
  if (!b) return false;
  const btn = (rv && rv.button) || {};
  const st = String(btn.state || 'none');
  const newer = !!(rv && rv.new_pipeline);
  const busy = st === 'queued' || st === 'running';
  const show = newer && status !== 'created' && status !== 'processing' && st !== 'forbidden' && st !== 'not_for_vehicles';
  b.hidden = !(show || busy);
  b.classList.toggle('busy', busy);
  b.classList.toggle('fail', st === 'failed');
  b.classList.toggle('ok', st === 'done' && !newer);
  b.disabled = busy;
  let title = tr('taskv3_tool_rerig');
  if (st === 'queued') title = tr('taskv3_rerig_queued');
  else if (st === 'running') title = tr('taskv3_rerig_running');
  else if (st === 'failed') title = tr('taskv3_rerig_failed');
  else if (st === 'deferred' || st === 'not_now') title = tr('taskv3_rerig_later');
  else if (st === 'done' && !newer) title = tr('taskv3_rerig_done');
  b.title = title;
  b.setAttribute('aria-label', title);
  if (rv && rv.current) b.dataset.version = `${rv.task || '-'}/${rv.task_sha || '-'} -> ${rv.current}/${rv.current_sha || '-'}`;
  if (st === 'done' && rerigPressed && !newer) { rerigPressed = false; b.hidden = false; setTimeout(() => { if (lastState) render(lastState); }, 6000); }
  return busy;
}
async function pressRerig() {
  const b = els.rerig;
  if (!b || b.disabled || !UUID.test(taskId)) return;
  b.disabled = true;
  b.classList.add('busy');
  rerigPressed = true;
  try {
    const r = await fetch(`/api/task/${encodeURIComponent(taskId)}/rerig`, {
      method: 'POST', credentials: 'same-origin', cache: 'no-store', headers: { Accept: 'application/json' },
    });
    const rv = await r.json().catch(() => null);
    if (rv && lastState) { lastState.rig_version = rv; render(lastState); }
  } catch (e) { b.disabled = false; b.classList.remove('busy'); }
  clearTimeout(timer);
  timer = setTimeout(poll, 1500);
}

// The V3 stage comes as keys (stage_key, sub_key, stage_seconds) and is translated here (i18n v3_stage_* /
// v3_sub_*), English when a key has no translation yet. The server's Russian stage_title is never shown.
const V3_EN = {
  intake: 'Intake', normalization: 'Normalizing', dispatch: 'Queuing in V3', pending_register: 'Registering the source',
  pending_submit: 'Queuing in V3', queued: 'In the V3 queue', starting: 'Starting', source: 'Source', analysis: 'Analysis',
  rig: 'Rig', retarget: 'Retarget', qa: 'Quality check', publish: 'Publishing', complete: 'Done', qa_review: 'Needs review',
  generation: 'Generating the model', running: 'Processing', resuming: 'Resuming', retrying: 'Retrying after a failure',
  source_registered: 'Source accepted', model_validation: 'Checking the model', failed: 'Failed',
};
const V3_SUB_EN = {
  projections: 'projections', vision: 'Vision analysis', isolate: 'isolating the character', bones_and_weights: 'bones and weights', library_clips: 'animations',
  numeric: 'numeric check', scale: 'scale', detect: 'reading the picture', waiting_3d_worker: 'waiting for a 3D worker',
  hunyuan: 'Hunyuan 3D', tripo: 'Tripo', turntable: 'turntable', ready: 'model ready', killed_retry: 'retry after a failure',
};
function keyText(prefix, key, en) {
  const k = `${prefix}${key}`;
  const t = tr(k);
  return t !== k ? t : (en[key] || String(key).replace(/_/g, ' '));
}
function v3StageTitle(state) {
  if (!state.stage_key) return '';
  const parts = [keyText('v3_stage_', state.stage_key, V3_EN)];
  if (state.sub_key) parts.push(keyText('v3_sub_', state.sub_key, V3_SUB_EN));
  if (Number.isFinite(state.stage_seconds)) parts.push(tr('v3_seconds', { n: state.stage_seconds }).replace('v3_seconds', `${state.stage_seconds} s`));
  return `V3 · ${parts.join(' · ')}`;
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

// A failed poll backs off 2 s -> 60 s (Vehicles V3, 2026-10-16: a vehicle task answered 404 here and the page hammered
// the backend); «not found» only when the task itself is gone, never while /api/task/<id> answers.
let failStreak = 0;
async function taskExists() {
  try {
    const r = await fetch(`/api/task/${encodeURIComponent(taskId)}`, { credentials: 'same-origin', cache: 'no-store' });
    return r.ok;
  } catch (e) { return true; }
}

async function poll() {
  clearTimeout(timer);
  if (!UUID.test(taskId)) { showMissing(); return; }
  try {
    const response = await fetch(`/api/task/${encodeURIComponent(taskId)}/v3-view`, {
      credentials: 'same-origin', cache: 'no-store', headers: { Accept: 'application/json' },
    });
    if (response.status === 404 && !(await taskExists())) { showMissing(); return; }
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const doc = await response.json();
    failStreak = 0;
    await mergeLive(doc);
    const next = render(doc);
    if (next > 0) timer = setTimeout(poll, next);
  } catch (error) {
    failStreak += 1;
    els.line.textContent = tr('taskv3_offline');
    timer = setTimeout(poll, Math.min(60000, 2000 * 2 ** Math.min(5, failStreak - 1)));
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
  if (els.rerig) els.rerig.addEventListener('click', pressRerig);
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
window.autorigTaskPage = { build: BUILD, taskId, poll, renderParts, modes: () => modeState };
poll();
