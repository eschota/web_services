// AutoRig task page (2026-11-10): one screen - the model, its state, its clips, its downloads.
// Server contract (unchanged): GET /api/task/<id>/v3-view, /downloads-v3 (+ custom export, unlock), /auth/me,
// POST /api/task/create, /api/generate/from-image, /api/task/<id>/purchase-intent, window.autorigFunnel.
import { createViewer } from './viewer.js';

const $ = (id) => document.getElementById(id);
const params = new URLSearchParams(location.search);
const taskId = String(params.get('id') || '').trim().toLowerCase();
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const RUN = /^[0-9a-f]{20}$/;
const PREVIEW = /task4\.html$/.test(location.pathname);
const taskHref = (id) => (PREVIEW ? `${location.pathname}?id=${encodeURIComponent(id)}` : `/task?id=${encodeURIComponent(id)}`);

// The page language is the server's decision (profile, /ru/ url ...; English by default), never the browser's alone
const SRV_LANG = String((window.__AUTORIG_LANG__ && window.__AUTORIG_LANG__.lang) || '').toLowerCase();
const RU = /^\/ru(\/|$)/.test(location.pathname) || SRV_LANG === 'ru';
const T = {
  queued: ['In queue', 'В очереди'], ahead: ['{n} ahead', 'впереди {n}'], next: ['next', 'следующая'],
  loading: ['Loading model', 'Загрузка модели'], ready: ['Rig ready', 'Риг готов'], failed: ['Processing failed', 'Не удалось обработать'],
  missing: ['Task not found', 'Задача не найдена'], rerig: ['Improving the rig', 'Улучшаем риг'], picture: ['Generating the 3D model from the picture', 'Генерация 3D-модели по картинке'],
  s_intake: ['Intake', 'Приём'], s_normalization: ['Normalizing', 'Нормализация'], s_dispatch: ['Queuing', 'Постановка в очередь'],
  s_queued: ['In queue', 'В очереди'], s_starting: ['Starting', 'Запуск'], s_source: ['Reading the model', 'Чтение модели'],
  s_analysis: ['Analysing: category and parts', 'Анализ: категория и части'], s_rig: ['Rigging: bones and weights', 'Риг: кости и веса'],
  s_retarget: ['Animations', 'Анимации'], s_qa: ['Quality check', 'Проверка качества'], s_publish: ['Publishing', 'Публикация'],
  s_generation: ['Generating the model', 'Генерация модели'], s_running: ['Processing', 'Обработка'],
  m_pbr: ['Textured', 'Текстуры'], m_clay: ['Clay', 'Глина'], m_weights: ['Weights', 'Веса'], m_bones: ['Bones', 'Кости'],
  replay: ['Replay the rig build', 'Повторить сборку рига'], reset: ['Reset view', 'Сбросить вид'],
  b_new: ['New', 'Новая'], b_share: ['Share', 'Ссылка'], b_download: ['Download', 'Скачать'], signin: ['Sign in', 'Войти'],
  copied: ['Link copied', 'Ссылка скопирована'], uploading: ['Uploading…', 'Загрузка…'],
  pick: ['Pick a 3D model (GLB, FBX, OBJ, ZIP) or a picture', 'Выберите 3D-модель (GLB, FBX, OBJ, ZIP) или картинку'],
  dl_title: ['Download', 'Скачать'], dl_classic: ['Open the download page', 'Открыть страницу скачивания'], dl_notready: ['The rig is not ready yet.', 'Риг ещё не готов.'],
  dl_anim: ['With animations', 'С анимациями'], dl_mixamo: ['Mixamo bone names', 'Имена костей Mixamo'],
  dl_signin: ['Sign in to download', 'Войдите, чтобы скачать'], dl_signin_go: ['Sign in with Google', 'Войти через Google'],
  dl_notowner: ['Only the owner of this task can download it. Upload your own model — it takes a few minutes.', 'Скачать может только владелец задачи. Загрузите свою модель — это несколько минут.'],
  dl_sub: ['Unlimited downloads', 'Безлимитные скачивания'], dl_month: ['/ month', '/ месяц'],
  dl_pack: ['{n} tasks', '{n} задач'], dl_unlock: ['Unlock this task · {n} credits', 'Открыть эту задачу · {n} кредитов'],
  dl_wait: ['Waiting for the payment… the download starts by itself.', 'Ждём оплату… скачивание начнётся само.'],
  dl_queued: ['In the export queue', 'В очереди экспорта'], dl_running: ['Exporting', 'Экспорт'], dl_ready: ['Done', 'Готово'],
  dl_failed: ['Export failed. Try again.', 'Экспорт не удался. Повторите.'], dl_offline: ['Exporters are busy, this can take a few minutes.', 'Экспортёры заняты, это может занять несколько минут.'],
  dl_free: ['Viewing is free. Downloads need a plan.', 'Просмотр бесплатный. Для скачивания нужен тариф.'],
  stats: ['{v} verts · {t} tris · {b} bones', '{v} вершин · {t} треуг. · {b} костей'],
};
const tr = (k, vars) => {
  let s = (T[k] || [k, k])[RU ? 1 : 0];
  if (vars) for (const [a, b] of Object.entries(vars)) s = s.replace(`{${a}}`, b);
  return s;
};
document.querySelectorAll('[data-t]').forEach((el) => { el.textContent = tr(el.dataset.t); });
document.querySelectorAll('[data-t-title]').forEach((el) => { el.title = tr(el.dataset.tTitle); el.setAttribute('aria-label', el.title); });

const funnel = (ev, data) => { try { window.autorigFunnel && window.autorigFunnel.track(ev, { task_id: taskId, ...data }); } catch (e) { /* ignore */ } };
async function api(path, init) {
  const opt = { credentials: 'same-origin', cache: 'no-store', headers: { Accept: 'application/json' }, ...(init || {}) };
  if (opt.body && typeof opt.body === 'string') opt.headers['Content-Type'] = 'application/json';
  try {
    const r = await fetch(path, opt);
    let body = null;
    try { body = await r.json(); } catch (e) { /* not json */ }
    return { ok: r.ok, status: r.status, body };
  } catch (e) { return { ok: false, status: 0, body: null }; }
}
let toastTimer = 0;
function toast(text, ms = 2600) {
  const el = $('t4-toast');
  el.textContent = text; el.hidden = !text;
  clearTimeout(toastTimer);
  if (text && ms) toastTimer = setTimeout(() => { el.hidden = true; }, ms);
}
const fmtN = (n) => (n >= 1e6 ? `${(n / 1e6).toFixed(2)}M` : n >= 1e3 ? `${Math.round(n / 1e3)}k` : String(n));

// ------------------------------------------------------------------ viewer
const viewer = createViewer($('t4-view'));
window.__t4viewer = viewer;                              // for support and lab checks from the console
let modelKey = '', modelBusy = false, lastState = null, manifest = null;

viewer.on('model', (info) => {
  $('t4-stats').textContent = tr('stats', { v: fmtN(info.verts), t: fmtN(info.tris), b: String(info.bones) });
  const box = $('t4-clips');
  box.innerHTML = '';
  if (info.rigged && info.clips.length) {
    const pp = document.createElement('button');
    pp.className = 't4-chipbtn t4-pp'; pp.id = 't4-pp'; pp.textContent = '❚❚'; pp.title = 'Space';
    pp.onclick = () => viewer.togglePause();
    box.appendChild(pp);
    for (const c of info.clips) {
      const b = document.createElement('button');
      b.className = 't4-chipbtn'; b.dataset.clip = c.name; b.textContent = c.name.replace(/_/g, ' ');
      b.onclick = () => { viewer.skip(); viewer.playClip(c.name); };
      box.appendChild(b);
    }
  }
  $('t4-dock').hidden = false;
  $('t4-modes').classList.toggle('norig', !info.rigged || !info.bones);
});
viewer.on('clip', ({ name, paused }) => {
  document.querySelectorAll('#t4-clips [data-clip]').forEach((b) => b.classList.toggle('on', b.dataset.clip === name));
  const pp = $('t4-pp');
  if (pp) pp.textContent = paused ? '▶' : '❚❚';
});
$('t4-view').addEventListener('pointerdown', () => viewer.skip());

function setMode(mode) {
  viewer.setMode(mode);
  document.querySelectorAll('#t4-modes [data-mode]').forEach((b) => b.classList.toggle('on', b.dataset.mode === mode));
}
document.querySelectorAll('#t4-modes [data-mode]').forEach((b) => { b.onclick = () => setMode(b.dataset.mode); });
let bonesOn = false;
$('t4-bones').onclick = () => { bonesOn = !bonesOn; viewer.setBones(bonesOn); $('t4-bones').classList.toggle('on', bonesOn); };
$('t4-replay').onclick = () => viewer.replay();
$('t4-reset').onclick = () => viewer.resetView();

function modelOf(v, stamp) {
  if (!v) return null;
  const p = v.params || {};
  const bust = stamp ? `&rr=${encodeURIComponent(stamp)}` : '';
  if (v.kind === 'mt-run' && RUN.test(String(p.run || ''))) {
    return v.rigged
      ? { url: `/api/mt/files/${p.run}/rig/rigged.glb?view=2${bust}`, rigged: true, key: `${p.run}:rig:${v.revision || ''}:${stamp || ''}`, rig: `/api/mt/files/${p.run}/rig/rig.json${bust ? `?${bust.slice(1)}` : ''}` }
      : { url: `/api/mt/files/${p.run}/proj/model.glb?view=2`, rigged: false, key: `${p.run}:src` };
  }
  if (v.kind === 'legacy') return { url: `/api/task-viewer/${taskId}/files/task/rig/rigged.glb`, rigged: true, key: `legacy:${v.revision || ''}` };
  return null;
}
async function showModel(v, finished, stamp) {
  const m = modelOf(v, stamp);
  const pic = $('t4-picture');
  if (v && v.kind === 'picture' && v.params && /^(https?:\/\/|\/)/.test(String(v.params.picture || ''))) {
    if (pic.dataset.src !== v.params.picture) { pic.dataset.src = v.params.picture; pic.src = v.params.cutout || v.params.picture; }
    pic.hidden = false;
  } else if (m) pic.hidden = true;
  if (!m || m.key === modelKey || modelBusy) return;
  modelBusy = true;
  const seenKey = `t4seen:${m.key}`;
  let seen = false;
  try { seen = sessionStorage.getItem(seenKey) === '1'; } catch (e) { /* private mode */ }
  const reduce = window.matchMedia && matchMedia('(prefers-reduced-motion: reduce)').matches;
  const wasSource = modelKey.endsWith(':src');
  const intro = !m.rigged ? 'none' : (seen || reduce) ? 'none' : (wasSource ? 'full' : 'fast');
  const bar = $('t4-load');
  try {
    if (!viewer.hasModel) { bar.hidden = false; bar.firstElementChild.style.width = '4%'; }
    if (m.rig) {                                          // the rig says where the character faces: the camera stands in front
      const doc = await Promise.race([api(m.rig), new Promise((res) => setTimeout(() => res(null), 2500))]);
      if (doc && doc.body && doc.body.forward_axis) viewer.setFront(String(doc.body.forward_axis));
    }
    await viewer.load(m.url, { rigged: m.rigged, intro, onProgress: (f) => {
      bar.firstElementChild.style.width = `${Math.round(4 + f * 96)}%`;
      if (finished) setStatus('work', tr('loading'), `${Math.round(f * 100)}%`, null);
    } });
    if (finished && lastState) { lastStatus = ''; render(lastState); }
    modelKey = m.key;
    try { if (m.rigged) sessionStorage.setItem(seenKey, '1'); } catch (e) { /* private mode */ }
  } catch (e) {
    if (finished && m.rigged) console.warn('model load failed', e);
  } finally { bar.hidden = true; modelBusy = false; }
}

// ------------------------------------------------------------------ state
let timer = 0, fails = 0, lastStatus = '';
function setStatus(kind, title, sub, pct) {
  const el = $('t4-status');
  el.hidden = !title;
  el.dataset.kind = kind;
  $('t4-st-title').textContent = title || '';
  $('t4-st-sub').textContent = sub || '';
  const bar = $('t4-st-bar');
  bar.hidden = pct == null;
  if (pct != null) bar.firstElementChild.style.width = `${Math.max(2, Math.min(100, Math.round(pct * 100)))}%`;
}
function stageText(s) {
  const k = `s_${s.stage_key || ''}`;
  return T[k] ? tr(k) : tr('s_running');
}
function render(s) {
  lastState = s;
  const status = String(s.status || '');
  const finished = status === 'done' || status === 'needs_review';
  if (!document.getElementById('task-seo-heading') && s.title) $('t4-name').textContent = s.title;
  if (status === 'created') setStatus('work', tr('queued'), s.queue && s.queue.ahead > 0 ? tr('ahead', { n: String(s.queue.ahead) }) : tr('next'), 0.02);
  else if (status === 'processing') {
    const eta = Number(s.eta_s) > 0 ? ` · ~${Math.ceil(Number(s.eta_s) / 60)} min` : '';
    setStatus('work', stageText(s), `${Math.round(Number(s.progress || 0) * 100)}%${eta}`, Number(s.progress || 0));
  } else if (status === 'error') setStatus('fail', tr('failed'), '', null);
  else if (finished) {                                   // done or needs_review: the customer sees a ready rig either way
    setStatus('ok', tr('ready'), '', null); setTimeout(() => { if ($('t4-status').dataset.kind === 'ok') $('t4-status').hidden = true; }, 4000); }
  if (s.viewer && s.viewer.kind === 'picture' && !finished && status !== 'error') setStatus('work', tr('picture'), `${Math.round(Number(s.progress || 0) * 100)}%`, Number(s.progress || 0));
  // the rig is rebuilt with today's pipeline when its version is behind (re-rig on open): say so, then show the new rig
  const rr = (s.rig_version && s.rig_version.rerig) || {};
  const rerigBusy = finished && (rr.state === 'queued' || rr.state === 'running');
  if (rerigBusy) setStatus('work', tr('rerig'), rr.state === 'queued' ? tr('queued') : '', null);
  showModel(s.viewer, finished, rr.state === 'done' ? String(rr.finished_at || rr.version || '') : '');
  if (finished && lastStatus !== status) loadManifest();
  lastStatus = status;
  return (status === 'created' || status === 'processing') ? 2500 : (status === 'error' ? 0 : (rerigBusy ? 6000 : 45000));
}
async function poll() {
  clearTimeout(timer);
  const r = await api(`/api/task/${encodeURIComponent(taskId)}/v3-view`);
  if (r.status === 404) { setStatus('fail', tr('missing'), '', null); return; }
  let next = 0;
  if (r.ok && r.body) { fails = 0; next = render(r.body); } else { fails += 1; next = Math.min(60000, 2000 * 2 ** Math.min(fails, 5)); }
  if (next > 0) timer = setTimeout(poll, next);
}

// ------------------------------------------------------------------ downloads
const TARGETS = [
  { id: 'unity', name: 'Unity', ext: 'FBX' }, { id: 'unreal', name: 'Unreal Engine', ext: 'FBX' },
  { id: 'blender', name: 'Blender', ext: 'BLEND' }, { id: 'glb', name: 'glTF', ext: 'GLB' },
];
let drawerOpen = false, pending = null, job = null, payWatch = 0, paywallSeen = false;
const intentSent = new Set();
function el(tag, props, ...kids) {
  const n = document.createElement(tag);
  for (const [k, v] of Object.entries(props || {})) { if (k === 'class') n.className = v; else if (k.startsWith('on')) n[k] = v; else if (v != null && v !== false) n.setAttribute(k, v === true ? '' : v); }
  for (const kid of kids) if (kid != null) n.append(kid);
  return n;
}
async function loadManifest() {
  if (!UUID.test(taskId)) return null;
  const r = await api(`/api/task/${encodeURIComponent(taskId)}/downloads-v3`);
  if (r.ok && r.body) {
    manifest = r.body;
    const cat = manifest.category || {};
    if (cat.what && !document.getElementById('task-seo-heading') && !(lastState && lastState.title)) $('t4-name').textContent = cat.what;
    const chip = $('t4-cat');
    chip.hidden = !cat.category || cat.category === 'unknown';
    chip.textContent = cat.category || '';
    if (drawerOpen) renderDrawer();
  }
  return manifest;
}
function openDrawer() {
  drawerOpen = true;
  $('t4-drawer').hidden = false;
  document.body.classList.add('t4-drawer-open');
  renderDrawer();
  loadManifest();
}
function closeDrawer() { drawerOpen = false; $('t4-drawer').hidden = true; document.body.classList.remove('t4-drawer-open'); }
function notifyIntent(target, locked) {
  const k = `${target}:${locked}`;
  if (intentSent.has(k)) return;
  intentSent.add(k);
  api(`/api/task/${encodeURIComponent(taskId)}/purchase-intent`, { method: 'POST', body: JSON.stringify({ source: `t4 ${target}${locked ? ' · paywall' : ' · download'}` }) });
}
function watchPayment() {
  clearInterval(payWatch);
  const check = async () => {
    const m = await loadManifest();
    if (m && m.access && m.access.allowed) {
      clearInterval(payWatch); payWatch = 0;
      funnel('purchase', { value: Number(m.plan && m.plan.price_usd) || 20, currency: 'USD', props: { plan: 'unlimited_monthly' } });
      renderDrawer();
      if (pending) startExport(pending);
    }
  };
  payWatch = setInterval(check, 5000);
  addEventListener('focus', check, { once: true });
}
function paywall(access, plan) {
  const box = el('div', { class: 't4-pay' });
  if (!paywallSeen) { paywallSeen = true; funnel('paywall_view', { once: access.reason || 'paywall', props: { reason: access.reason || '' } }); }
  if (access.reason === 'signin_required') {
    box.append(el('p', {}, tr('dl_signin')),
      el('a', { class: 't4-btn primary wide', href: plan.login_url, onclick: () => funnel('sign_up_prompt', {}) }, tr('dl_signin_go')));
    return box;
  }
  if (access.reason === 'not_owner') {
    box.append(el('p', {}, tr('dl_notowner')), el('button', { class: 't4-btn primary wide', onclick: () => $('t4-file').click() }, `⬆ ${tr('b_new')}`));
    return box;
  }
  const wait = el('p', { class: 't4-wait', hidden: true }, tr('dl_wait'));
  const price = `$${Number(plan.price_usd).toFixed(plan.price_usd % 1 ? 2 : 0)}`;
  const sub = el('a', { class: 't4-btn primary wide t4-plan', href: plan.checkout_url, target: '_blank', rel: 'noopener',
    onclick: () => { wait.hidden = false; funnel('checkout_start', { props: { plan: 'unlimited_monthly' } }); watchPayment(); } },
    el('b', {}, tr('dl_sub')),
    el('span', {}, plan.offer && plan.offer.active ? el('s', {}, `$${Number(plan.list_price_usd).toFixed(0)}`) : null, ` ${price} ${tr('dl_month')}`));
  box.append(el('p', {}, tr('dl_free')), sub);
  if (plan.pack) {
    box.append(el('a', { class: 't4-btn wide t4-plan', href: plan.pack.checkout_url, target: '_blank', rel: 'noopener',
      onclick: () => { wait.hidden = false; funnel('checkout_start', { props: { plan: 'task_pack' } }); watchPayment(); } },
      el('b', {}, tr('dl_pack', { n: String(plan.pack.tasks) })), el('span', {}, `$${Number(plan.pack.price_usd).toFixed(0)}`)));
  }
  if (access.can_unlock) {
    box.append(el('button', { class: 't4-btn wide', onclick: async (e) => {
      e.currentTarget.disabled = true;
      const r = await api(`/api/task/${encodeURIComponent(taskId)}/unlock-v3`, { method: 'POST', body: '{}' });
      if (r.ok && r.body && r.body.access && r.body.access.allowed) {
        funnel('purchase', { value: 0, currency: 'USD', props: { plan: 'task_unlock' } });
        manifest = r.body; renderDrawer(); if (pending) startExport(pending);
      } else e.currentTarget.disabled = false;
    } }, tr('dl_unlock', { n: String(access.unlock_cost || 10) })));
  }
  box.append(wait);
  return box;
}
function renderDrawer() {
  const body = $('t4-drawer-body');
  body.innerHTML = '';
  if (!manifest) { body.append(el('p', { class: 't4-muted' }, '…')); return; }
  if (!manifest.available && manifest.legacy_url) {       // a task of the old pipeline: its files are on the classic page
    body.append(el('a', { class: 't4-btn primary wide', href: manifest.legacy_url, onclick: () => funnel('download_click', { props: { target: 'classic' } }) }, tr('dl_classic')));
    return;
  }
  if (!manifest.available) { body.append(el('p', { class: 't4-muted' }, tr('dl_notready'))); return; }
  const access = manifest.access || {}, opts = manifest.options || {};
  const grid = el('div', { class: 't4-targets' });
  for (const t of TARGETS) {
    if (opts.targets && !opts.targets.includes(t.id)) continue;
    grid.append(el('button', { class: `t4-target${access.allowed ? '' : ' locked'}`, 'data-target': t.id, onclick: () => chooseTarget(t.id) },
      el('b', {}, t.name), el('span', {}, t.ext)));
  }
  body.append(grid);
  const row = el('div', { class: 't4-opts' });
  if (opts.clips) row.append(el('label', {}, el('input', { type: 'checkbox', id: 't4-o-anim', checked: true }), ` ${tr('dl_anim')}`));
  if (opts.mixamo) row.append(el('label', {}, el('input', { type: 'checkbox', id: 't4-o-mixamo' }), ` ${tr('dl_mixamo')}`));
  if (row.childNodes.length) body.append(row);
  body.append(el('div', { class: 't4-job', id: 't4-job', hidden: true }, el('span', { id: 't4-job-text' }), el('i', {}, el('i', { id: 't4-job-fill' }))));
  if (!access.allowed) body.append(paywall(access, manifest.plan || {}));
  else if (manifest.exporter && !manifest.exporter.online) body.append(el('p', { class: 't4-muted' }, tr('dl_offline')));
  if (job) showJob(job.view);
}
function showJob(view) {
  const box = $('t4-job');
  if (!box) return;
  box.hidden = false;
  const failed = view.state === 'failed';
  box.classList.toggle('failed', failed);
  const pct = Math.round(Math.max(0, Math.min(1, Number(view.progress || 0))) * 100);
  $('t4-job-fill').style.width = `${failed || view.state === 'ready' ? 100 : Math.max(3, pct)}%`;
  $('t4-job-text').textContent = failed ? tr('dl_failed') : view.state === 'ready' ? tr('dl_ready') : view.state === 'queued' ? tr('dl_queued') : `${tr('dl_running')} ${pct}%`;
}
function chooseTarget(target) {
  if (!manifest) return;
  const anim = $('t4-o-anim'), mix = $('t4-o-mixamo');
  const req = { target, clips: anim && !anim.checked ? [] : 'all', mesh: true, mixamo: !!(mix && mix.checked) };
  const locked = !(manifest.access && manifest.access.allowed);
  notifyIntent(target, locked);
  funnel('download_click', { props: { target, ...(locked ? { locked: true } : {}) } });
  if (locked) {
    pending = req;
    const pay = document.querySelector('.t4-pay');
    if (pay) { pay.classList.remove('pulse'); void pay.offsetWidth; pay.classList.add('pulse'); pay.scrollIntoView({ block: 'nearest', behavior: 'smooth' }); }
    return;
  }
  startExport(req);
}
async function startExport(req) {
  pending = null;
  if (job) clearTimeout(job.timer);
  job = { req, view: { state: 'queued', progress: 0.02 }, timer: 0 };
  showJob(job.view);
  const base = `/api/task/${encodeURIComponent(taskId)}`;
  let fmt = null;
  const mine = job;
  const step = async (first) => {
    if (job !== mine) return;
    const r = first ? await api(`${base}/downloads-v3-custom`, { method: 'POST', body: JSON.stringify(req) })
      : await api(`${base}/downloads-v3/${encodeURIComponent(fmt)}`);
    if (job !== mine) return;
    if (r.status === 401 || r.status === 402 || r.status === 403) { job = null; pending = req; await loadManifest(); renderDrawer(); return; }
    if (!r.ok || !r.body) {
      mine.errors = (mine.errors || 0) + 1;
      if (first || mine.errors > 5) { mine.view = { state: 'failed' }; showJob(mine.view); return; }
      mine.timer = setTimeout(() => step(false), 3000);
      return;
    }
    mine.errors = 0;
    fmt = r.body.format || fmt;
    mine.view = r.body;
    showJob(r.body);
    if (r.body.state === 'ready' && r.body.url) {
      const a = el('a', { href: r.body.url, download: '', hidden: true });
      document.body.appendChild(a); a.click(); setTimeout(() => a.remove(), 1000);
      return;
    }
    if (r.body.state === 'failed') return;
    mine.timer = setTimeout(() => step(false), 1500);
  };
  step(true);
}
$('t4-dl').onclick = () => (drawerOpen ? closeDrawer() : openDrawer());
$('t4-drawer-close').onclick = closeDrawer;

// ------------------------------------------------------------------ upload, share, account
const MODEL_EXT = ['glb', 'gltf', 'fbx', 'obj', 'zip'], IMAGE_EXT = ['png', 'jpg', 'jpeg', 'webp'];
let uploading = false;
async function upload(files) {
  if (uploading || !files.length) return;
  const ext = (f) => String(f.name || '').split('.').pop().toLowerCase();
  const model = files.find((f) => MODEL_EXT.includes(ext(f)));
  const image = files.find((f) => IMAGE_EXT.includes(ext(f)));
  if (!model && !image) { toast(tr('pick'), 4000); return; }
  uploading = true;
  toast(tr('uploading'), 0);
  funnel('upload_start', {});
  try {
    const body = new FormData();
    let url = '/api/generate/from-image';
    if (model) { body.append('source', 'upload'); body.append('file', model); body.append('type', 't_pose'); body.append('pipeline', 'v3'); url = '/api/task/create'; }
    else body.append('file', image);
    const resp = await fetch(url, { method: 'POST', body, credentials: 'same-origin' });
    const data = await resp.json().catch(() => ({}));
    if (resp.status === 401) { location.href = '/auth/login'; return; }
    if (resp.status === 402) { location.href = '/buy-credits'; return; }
    if (!resp.ok || !data.task_id) throw new Error((data && data.detail && (data.detail.message_string || (typeof data.detail === 'string' && data.detail))) || `HTTP ${resp.status}`);
    funnel('task_created', { task_id: data.task_id });
    location.href = taskHref(data.task_id);
  } catch (e) { toast(String((e && e.message) || e), 6000); funnel('task_create_failed', {}); } finally { uploading = false; }
}
$('t4-new').onclick = () => $('t4-file').click();
$('t4-file').onchange = () => { const f = Array.from($('t4-file').files || []); $('t4-file').value = ''; upload(f); };
const stage = $('t4-stage');
stage.addEventListener('dragover', (e) => { e.preventDefault(); stage.classList.add('drop'); });
stage.addEventListener('dragleave', () => stage.classList.remove('drop'));
stage.addEventListener('drop', (e) => { e.preventDefault(); stage.classList.remove('drop'); upload(Array.from((e.dataTransfer && e.dataTransfer.files) || [])); });
$('t4-share').onclick = async () => {
  const url = `${location.origin}/task?id=${encodeURIComponent(taskId)}`;
  try { await navigator.clipboard.writeText(url); toast(tr('copied')); } catch (e) { prompt('', url); }
};
api('/auth/me').then((r) => {
  const u = r.body && r.body.authenticated && r.body.user;
  const a = $('t4-user');
  if (u) {
    a.href = '/dashboard'; a.textContent = '';
    if (u.picture) a.append(el('img', { src: u.picture, alt: '', referrerpolicy: 'no-referrer' })); else a.textContent = String(u.name || u.email || '?').slice(0, 1).toUpperCase();
    a.classList.add('in'); a.title = u.name || '';
  } else { a.href = `/auth/login?next=${encodeURIComponent(location.pathname + location.search)}`; a.textContent = tr('signin'); }
});

addEventListener('keydown', (e) => {
  const tag = (e.target && e.target.tagName || '').toLowerCase();
  if (tag === 'input' || tag === 'textarea' || e.metaKey || e.ctrlKey || e.altKey) return;
  if (e.key === ' ') { e.preventDefault(); viewer.skip(); viewer.togglePause(); }
  else if (e.key === '1') setMode('pbr'); else if (e.key === '2') setMode('clay'); else if (e.key === '3') setMode('weights');
  else if (e.key === 'b' || e.key === 'B') $('t4-bones').click();
  else if (e.key === 'd' || e.key === 'D') $('t4-dl').click();
  else if (e.key === 'r' || e.key === 'R') viewer.resetView();
  else if (e.key === 'Escape') closeDrawer();
});

const labGlb = String(params.get('glb') || '');
if (/^\/static\/lab\/[\w.-]+\.glb$/.test(labGlb)) {      // a lab build for review: /task?glb=/static/lab/<name>.glb
  $('t4-name').textContent = labGlb.split('/').pop();
  viewer.load(labGlb, { rigged: true, intro: 'none' }).catch((e) => setStatus('fail', String(e.message || e), '', null));
} else if (!UUID.test(taskId)) setStatus('fail', tr('missing'), '', null);
else {
  poll();
  loadManifest().then(() => { if (params.get('dl') === '1') openDrawer(); });
}
