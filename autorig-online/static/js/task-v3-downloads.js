// Downloads · V3 (owner, 2026-10-10): «скачивание делай только новых файлов, если их нет то нужно их по запросу
// экспортировать, пользователю показывать прогрессбар … все скачивания только с безлимитной подпиской».
// Round 2 (owner, 2026-10-11): «когда риг готов надо показать большую зелёную анимированную, зацикленно но
// разноритмично, кнопку Download Rig, с опциями скачивания как в предыдущей версии было, выбор анимаций и т.п.»
// The big green «Download Rig» button appears in the task bar once the V3 rig exists; it and the download tool open
// this panel: the target (Unity / Unreal FBX, Blender .blend, GLB, ZIP), the animations (all or chosen), with the
// model or animations only, Mixamo bone names for humanoids, the rig version. The export runs on request with a real
// percent and stage, behind the Unlimited paywall. The server decides everything (/api/task/<id>/downloads-v3); the
// clips are the run's current ones, so clips added in the viewport show up here on the next open.
const BUILD = 'dlv3-20261011.1';
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const params = new URLSearchParams(location.search);
const taskId = String(params.get('id') || '').trim().toLowerCase();
const PENDING_KEY = `dlv3:${taskId}`;
const SEL_KEY = `dlv3sel:${taskId}`;
const REDUCED = typeof matchMedia === 'function' && matchMedia('(prefers-reduced-motion: reduce)').matches;

const FALLBACK = {
  en: {
    dlv3_title: 'Download', dlv3_hero: 'Download Rig', dlv3_hero_tip: 'The rig is ready: download it with its animations',
    dlv3_close: 'Close', dlv3_clips: 'Animations', dlv3_all: 'All', dlv3_count: '{n} of {total}',
    dlv3_target_unity: 'FBX for Unity', dlv3_target_unreal: 'FBX for Unreal Engine', dlv3_target_blender: '.blend for Blender',
    dlv3_target_glb: 'GLB · three.js, Godot, web', dlv3_target_zip: 'ZIP · GLB + FBX + every animation',
    dlv3_with_model: 'Model and animations', dlv3_anim_only: 'Animations only (skeleton)',
    dlv3_mixamo: 'Mixamo bone names (mixamorig:)', dlv3_versions: 'Rig version', dlv3_version_current: 'current',
    dlv3_download: 'Download', dlv3_pick_clip: 'Choose at least one animation',
    dlv3_version: 'Rig {version}', dlv3_state_queued: 'In the export queue', dlv3_state_waiting_worker: 'Waiting for an exporter',
    dlv3_stage_taken: 'Export started', dlv3_stage_download: 'Fetching the model', dlv3_stage_import: 'Reading the rig',
    dlv3_stage_fbx: 'Writing FBX', dlv3_stage_glb: 'Writing GLB', dlv3_stage_blend: 'Writing .blend',
    dlv3_stage_verify: 'Checking the file', dlv3_stage_upload: 'Sending the file',
    dlv3_stage_clips: 'Cutting animations', dlv3_stage_zip: 'Packing ZIP', dlv3_ready: 'Your download is starting',
    dlv3_failed: 'Export failed · tap to retry', dlv3_not_ready: 'The rig is not ready yet', dlv3_legacy: 'Files of this task',
    dlv3_pay_title: 'Downloads come with Unlimited', dlv3_pay_files: 'GLB, FBX and ZIP with every animation',
    dlv3_pay_engines: 'Unity, Unreal, Blender, Godot', dlv3_pay_unlimited: 'Unlimited models and exports',
    dlv3_pay_price: '{price} / month', dlv3_pay_subscribe: 'Subscribe', dlv3_pay_signin: 'Sign in with Google',
    dlv3_pay_signin_first: 'Sign in first, then subscribe', dlv3_pay_waiting: 'Waiting for the payment · the download starts by itself',
    dlv3_pay_not_owner: 'Only the owner of this task can download its files', dlv3_view_as_free: 'View as a free user (admin)',
    dlv3_offline: 'No connection, retrying…',
  },
  ru: {
    dlv3_title: 'Скачать', dlv3_hero: 'Скачать риг', dlv3_hero_tip: 'Риг готов: скачайте его с анимациями',
    dlv3_close: 'Закрыть', dlv3_clips: 'Анимации', dlv3_all: 'Все', dlv3_count: '{n} из {total}',
    dlv3_target_unity: 'FBX для Unity', dlv3_target_unreal: 'FBX для Unreal Engine', dlv3_target_blender: '.blend для Blender',
    dlv3_target_glb: 'GLB · three.js, Godot, веб', dlv3_target_zip: 'ZIP · GLB + FBX + каждая анимация',
    dlv3_with_model: 'Модель и анимации', dlv3_anim_only: 'Только анимации (скелет)',
    dlv3_mixamo: 'Имена костей Mixamo (mixamorig:)', dlv3_versions: 'Версия рига', dlv3_version_current: 'текущая',
    dlv3_download: 'Скачать', dlv3_pick_clip: 'Выберите хотя бы одну анимацию',
    dlv3_version: 'Риг {version}', dlv3_state_queued: 'В очереди на экспорт', dlv3_state_waiting_worker: 'Ждём экспортёр',
    dlv3_stage_taken: 'Экспорт начался', dlv3_stage_download: 'Получаю модель', dlv3_stage_import: 'Читаю риг',
    dlv3_stage_fbx: 'Пишу FBX', dlv3_stage_glb: 'Пишу GLB', dlv3_stage_blend: 'Пишу .blend',
    dlv3_stage_verify: 'Проверяю файл', dlv3_stage_upload: 'Передаю файл',
    dlv3_stage_clips: 'Нарезаю анимации', dlv3_stage_zip: 'Упаковываю ZIP', dlv3_ready: 'Скачивание начинается',
    dlv3_failed: 'Экспорт не удался · нажмите, чтобы повторить', dlv3_not_ready: 'Риг ещё не готов', dlv3_legacy: 'Файлы этой задачи',
    dlv3_pay_title: 'Скачивание — в подписке Unlimited', dlv3_pay_files: 'GLB, FBX и ZIP со всеми анимациями',
    dlv3_pay_engines: 'Unity, Unreal, Blender, Godot', dlv3_pay_unlimited: 'Безлимитные модели и экспорты',
    dlv3_pay_price: '{price} / месяц', dlv3_pay_subscribe: 'Подписаться', dlv3_pay_signin: 'Войти через Google',
    dlv3_pay_signin_first: 'Сначала войдите, потом подписка', dlv3_pay_waiting: 'Ждём оплату · скачивание начнётся само',
    dlv3_pay_not_owner: 'Скачать файлы может только владелец задачи', dlv3_view_as_free: 'Смотреть как бесплатный (админ)',
    dlv3_offline: 'Нет связи, повторяем…',
  },
};

function uiLang() {
  let lang = window.I18n && window.I18n.currentLang;
  if (!lang) { try { lang = localStorage.getItem('autorig_lang'); } catch (e) { lang = ''; } }
  return String(lang || navigator.language || 'en').slice(0, 2).toLowerCase();
}

function tr(key, replacements) {
  const i18n = window.I18n;
  let text;
  if (i18n && typeof i18n.has === 'function' && i18n.has(key)) text = i18n.t(key, replacements);
  else {
    text = (FALLBACK[uiLang()] || FALLBACK.en)[key] || FALLBACK.en[key] || key;
    for (const [name, value] of Object.entries(replacements || {})) text = text.split(`{${name}}`).join(String(value));
  }
  return text;
}

const NS = 'http://www.w3.org/2000/svg';
const ICONS = {
  // animated on hover / while busy through CSS (.dl3-ico-*)
  glb: '<path d="M12 3l8 4.5v9L12 21l-8-4.5v-9z"/><path class="dl3-spin-y" d="M12 12l8-4.5M12 12v9M12 12L4 7.5"/>',
  fbx: '<circle cx="12" cy="4.5" r="1.8"/><path class="dl3-wave" d="M12 6.5v6M12 12.5l-4 7M12 12.5l4 7M7 9h10"/>',
  unity: '<path d="M12 2.8l8 4.6v9.2l-8 4.6-8-4.6V7.4z"/><path class="dl3-spin-y" d="M12 7.5v4.5l4 2.3M12 12l-4 2.3"/>',
  unreal: '<circle cx="12" cy="12" r="9"/><path class="dl3-wave" d="M8.5 8v5.2a3.5 3.5 0 0 0 7 0V8"/>',
  blender: '<circle cx="13.5" cy="13" r="6.5"/><circle class="dl3-drop" cx="13.5" cy="13" r="2.4"/><path d="M3 9.5l7-4.2M5 13h4"/>',
  zip: '<rect x="4" y="3" width="16" height="18" rx="2"/><path class="dl3-zip" d="M12 3v2M12 7v2M12 11v2"/><rect x="10" y="14" width="4" height="4" rx="1"/>',
  model: '<circle cx="12" cy="4.5" r="1.8"/><path d="M12 6.5v6M12 12.5l-4 7M12 12.5l4 7M7 9h10"/><path class="dl3-wave" d="M18 4c1.5 1 1.5 3 0 4"/>',
  bones: '<circle cx="12" cy="4.5" r="1.4"/><circle cx="12" cy="12" r="1.4"/><circle cx="7.5" cy="19.5" r="1.4"/><circle cx="16.5" cy="19.5" r="1.4"/><path class="dl3-wave" d="M12 6v4.5M11.2 13.3l-3 5M12.8 13.3l3 5"/>',
  mixamo: '<path d="M4 18V6l8 7 8-7v12"/>',
  rig: '<path d="M12 3v11"/><path class="dl3-hero-arrow" d="M7.5 9.5L12 14l4.5-4.5"/><path d="M5 20h14"/><circle cx="12" cy="3.2" r="1.2"/>',
  lock: '<rect x="5" y="11" width="14" height="10" rx="2"/><path class="dl3-shackle" d="M8 11V8a4 4 0 0 1 8 0v3"/><circle cx="12" cy="16" r="1.4"/>',
  files: '<path d="M7 3h7l5 5v13H7z"/><path d="M14 3v5h5"/><path class="dl3-drop" d="M12 11v6M9.5 14.5L12 17l2.5-2.5"/>',
  engines: '<rect x="3" y="4" width="18" height="13" rx="2"/><path d="M8 21h8M12 17v4"/><path class="dl3-wave" d="M7 13l3-4 3 3 4-5"/>',
  infinite: '<path class="dl3-loop" d="M7.5 15.5c-2 0-3.5-1.6-3.5-3.5s1.5-3.5 3.5-3.5c3.5 0 5.5 7 9 7 2 0 3.5-1.6 3.5-3.5S18.5 8.5 16.5 8.5c-3.5 0-5.5 7-9 7z"/>',
  google: '<path d="M20 12.2c0-.6-.1-1.2-.2-1.7H12v3.3h4.5a3.9 3.9 0 0 1-1.7 2.5v2h2.7c1.6-1.5 2.5-3.6 2.5-6.1z"/><path d="M12 20.5c2.3 0 4.2-.8 5.5-2.1l-2.7-2a5 5 0 0 1-7.5-2.7H4.6v2.1A8.5 8.5 0 0 0 12 20.5z"/><path d="M7.3 13.7a5 5 0 0 1 0-3.4V8.2H4.6a8.5 8.5 0 0 0 0 7.6z"/><path d="M12 6.8c1.3 0 2.5.5 3.4 1.3l2.4-2.4A8.5 8.5 0 0 0 4.6 8.2l2.7 2.1A5 5 0 0 1 12 6.8z"/>',
  close: '<path d="M6 6l12 12M18 6L6 18"/>',
  eye: '<path d="M2.5 12S6 5.5 12 5.5 21.5 12 21.5 12 18 18.5 12 18.5 2.5 12 2.5 12z"/><circle cx="12" cy="12" r="3"/>',
  legacy: '<path d="M4 5h16v14H4z"/><path d="M8 9h8M8 13h5"/>',
  check: '<path d="M5 12.5l4.5 4.5L19 7.5"/>',
};

function icon(name, cls) {
  const svg = document.createElementNS(NS, 'svg');
  svg.setAttribute('viewBox', '0 0 24 24');
  svg.setAttribute('aria-hidden', 'true');
  svg.setAttribute('class', `dl3-ico dl3-ico-${name}${cls ? ` ${cls}` : ''}`);
  svg.innerHTML = ICONS[name] || '';
  return svg;
}

function el(tag, attrs, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v === null || v === undefined || v === false) continue;
    if (k === 'class') node.className = v;
    else if (k === 'text') node.textContent = v;
    else if (k.startsWith('on')) node.addEventListener(k.slice(2), v);
    else node.setAttribute(k, v === true ? '' : String(v));
  }
  for (const child of children) if (child) node.appendChild(child);
  return node;
}

let panel = null;
let manifest = null;
let busy = null;            // {key, timer, view}
let payWatch = 0;
let watchTimer = 0;
let hero = null;
const tool = document.getElementById('tv3-classic');

// ?as=free (admins only, honoured by the server): see the page as a signed-in owner without a subscription
const AS_FREE = params.get('as') === 'free';
function withAs(path) { return AS_FREE ? `${path}${path.includes('?') ? '&' : '?'}as=free` : path; }

async function api(path, opts) {
  path = withAs(path);
  const r = await fetch(path, { credentials: 'same-origin', cache: 'no-store', headers: { Accept: 'application/json', ...(opts && opts.body ? { 'Content-Type': 'application/json' } : {}) }, ...(opts || {}) });
  let body = null;
  try { body = await r.json(); } catch (e) { body = null; }
  return { status: r.status, ok: r.ok, body };
}

function store(key, value) { try { if (value === null) sessionStorage.removeItem(key); else sessionStorage.setItem(key, JSON.stringify(value)); } catch (e) { /* private mode */ } }
function load(key) { try { return JSON.parse(sessionStorage.getItem(key) || 'null'); } catch (e) { return null; } }
function remember(req) { store(PENDING_KEY, req); }
function pending() { return load(PENDING_KEY); }
function forget() { store(PENDING_KEY, null); }

function priceText(plan) {
  const price = Number(plan && plan.price_usd) || 20;
  return tr('dlv3_pay_price', { price: `$${Number.isInteger(price) ? price : price.toFixed(2)}` });
}

// ---------------------------------------------------------------- the choice: target × clips × model × names × version
const sel = { target: 'unity', clips: null, mesh: true, mixamo: false, version: null };
(function restore() {
  const saved = load(SEL_KEY);
  if (saved && typeof saved === 'object') Object.assign(sel, saved);
})();
function saveSel() { store(SEL_KEY, sel); }

function clipsOf() { return (manifest && manifest.rig && manifest.rig.clips) || []; }
function opts() { return (manifest && manifest.options) || { targets: ['unity', 'unreal', 'blender', 'glb', 'zip'], clips: false, animation_only: false, mixamo: false }; }
function chosenClips() {
  const all = clipsOf();
  if (!Array.isArray(sel.clips)) return all.slice();
  return all.filter((c) => sel.clips.includes(c));      // clips gone from the run drop out by themselves
}
function activeVersion() {
  const rows = (manifest && manifest.versions) || [];
  const a = rows.find((v) => v.active);
  return a ? a.id : null;
}
function zipAllowed() { return !sel.version || sel.version === activeVersion(); }

function request() {
  // ZIP = the whole package of the current rig; everything else is a custom export of exactly what was chosen
  if (sel.target === 'zip') return { fmt: 'zip' };
  const o = opts();
  const all = clipsOf();
  const chosen = chosenClips();
  return {
    custom: {
      target: sel.target,
      clips: chosen.length === all.length ? 'all' : chosen,
      mesh: o.animation_only ? !!sel.mesh : true,
      mixamo: o.mixamo ? !!sel.mixamo : false,
      version: sel.version && sel.version !== activeVersion() ? sel.version : null,
    },
  };
}
function requestKey(req) { return JSON.stringify(req); }

// ---------------------------------------------------------------- panel
function build() {
  panel = el('div', { class: 'dl3', id: 'dl3', role: 'dialog', 'aria-label': tr('dlv3_title'), hidden: true });
  panel.addEventListener('contextmenu', (e) => e.stopPropagation());
  const stage = document.getElementById('tv3-stage') || document.body;
  stage.appendChild(panel);
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape' && panel && !panel.hidden) close(); });
  document.addEventListener('pointerdown', (e) => {
    if (panel && !panel.hidden && !panel.contains(e.target) && !(tool && tool.contains(e.target)) && !(hero && hero.contains(e.target))) close();
  });
}

function close() {
  if (panel) panel.hidden = true;
  if (tool) tool.classList.remove('dl3-open');
  if (hero) hero.classList.remove('open');
  clearInterval(payWatch);
  payWatch = 0;
}

let prefetch = null;      // the manifest is asked for at page load, before the viewer's engine fills the connections

async function open(resume) {
  if (!panel) build();
  panel.hidden = false;
  if (tool) tool.classList.add('dl3-open');
  if (hero) hero.classList.add('open');
  if (!manifest) loading();
  if (prefetch) { await prefetch; prefetch = null; render(); }
  await refresh();                                      // the run's current clips (the viewport may have added some)
  if (resume && manifest && manifest.available) choose(resume);
}

async function refresh() {
  if (!panel) build();
  try {
    const r = await api(`/api/task/${encodeURIComponent(taskId)}/downloads-v3`);
    if (!r.ok || !r.body || r.body.schema !== 'autorig.task-downloads-v3/1') throw new Error(String(r.status));
    manifest = r.body;
  } catch (e) {
    if (!manifest) manifest = null;
  }
  showHero();
  if (!panel.hidden) render();
}

function loading() {
  panel.textContent = '';
  panel.appendChild(head());
  panel.appendChild(el('div', { class: 'dl3-loading' }, el('i'), el('i'), el('i')));
}

function head() {
  const row = el('div', { class: 'dl3-head' },
    icon('files', 'dl3-head-ico'),
    el('b', { text: tr('dlv3_title') }));
  if (manifest && manifest.rig && manifest.rig.version) {
    row.appendChild(el('span', { class: 'dl3-ver', text: tr('dlv3_version', { version: manifest.rig.version }) }));
  }
  row.appendChild(el('i', { class: 'dl3-sp' }));
  if (manifest && manifest.access && manifest.access.can_view_as) {
    const on = !!manifest.access.view_as_free;
    row.appendChild(el('button', {
      type: 'button', class: `dl3-icobtn${on ? ' on' : ''}`, title: tr('dlv3_view_as_free'), 'aria-pressed': on ? 'true' : 'false',
      'aria-label': tr('dlv3_view_as_free'),
      onclick: async () => { await api('/api/admin/view-as', { method: 'POST', body: JSON.stringify({ mode: on ? 'off' : 'free' }) }); await refresh(); },
    }, icon('eye')));
  }
  row.appendChild(el('button', { type: 'button', class: 'dl3-icobtn', title: tr('dlv3_close'), 'aria-label': tr('dlv3_close'), onclick: close }, icon('close')));
  return row;
}

const TARGET_LABEL = { unity: 'Unity', unreal: 'Unreal', blender: 'Blender', glb: 'GLB', zip: 'ZIP' };
const TARGET_ICON = { unity: 'unity', unreal: 'unreal', blender: 'blender', glb: 'glb', zip: 'zip' };

function seg(name, value, choices) {
  // a row of icon toggles with tooltips; choices: [{value, icon, tip, label?}]
  const row = el('div', { class: 'dl3-seg', role: 'radiogroup' });
  for (const c of choices) {
    const on = sel[name] === c.value;
    row.appendChild(el('button', {
      type: 'button', role: 'radio', 'aria-checked': on ? 'true' : 'false', class: `dl3-segbtn${on ? ' on' : ''}`,
      title: tr(c.tip), 'aria-label': tr(c.tip),
      onclick: () => { sel[name] = c.value; saveSel(); render(); },
    }, icon(c.icon), c.label ? el('span', { text: c.label }) : null));
  }
  return row;
}

function render() {
  if (!panel) return;
  panel.textContent = '';
  panel.appendChild(head());
  if (!manifest) {
    panel.appendChild(el('p', { class: 'dl3-note', text: tr('dlv3_offline') }));
    return;
  }
  if (!manifest.available) {
    if (manifest.legacy_url) {
      panel.appendChild(el('a', { class: 'dl3-legacy', href: manifest.legacy_url, title: tr('dlv3_legacy') }, icon('legacy'), el('span', { text: tr('dlv3_legacy') })));
    } else {
      panel.appendChild(el('p', { class: 'dl3-note', text: tr('dlv3_not_ready') }));
    }
    return;
  }
  const o = opts();
  const versions = manifest.versions || [];
  if (sel.version && !versions.some((v) => v.id === sel.version)) sel.version = null;
  if (sel.target === 'zip' && !zipAllowed()) sel.target = 'unity';
  if (!o.targets.includes(sel.target)) sel.target = o.targets[0];

  // targets: big icon tiles
  const grid = el('div', { class: 'dl3-grid dl3-targets' });
  for (const t of o.targets) {
    if (t === 'zip' && !zipAllowed()) continue;
    grid.appendChild(el('button', {
      type: 'button', class: `dl3-big${sel.target === t ? ' on' : ''}`, title: tr(`dlv3_target_${t}`), 'aria-label': tr(`dlv3_target_${t}`),
      'aria-pressed': sel.target === t ? 'true' : 'false',
      onclick: () => { sel.target = t; saveSel(); render(); },
    }, icon(TARGET_ICON[t]), el('span', { text: TARGET_LABEL[t] })));
  }
  panel.appendChild(grid);

  // version, model / animations only, Mixamo names: only what this model and target make sensible
  const row = el('div', { class: 'dl3-opts' });
  if (versions.length > 1) {
    const box = el('div', { class: 'dl3-vers', title: tr('dlv3_versions') });
    for (const v of versions) {
      const on = (sel.version || activeVersion()) === v.id;
      box.appendChild(el('button', {
        type: 'button', class: `dl3-verbtn${on ? ' on' : ''}${v.active ? ' cur' : ''}`,
        title: `${v.id}${v.active ? ` · ${tr('dlv3_version_current')}` : ''}${v.note ? ` · ${v.note}` : ''}`,
        onclick: () => { sel.version = v.active ? null : v.id; saveSel(); render(); },
      }, el('span', { text: v.id })));
    }
    row.appendChild(box);
  }
  if (sel.target !== 'zip') {
    if (o.animation_only) {
      row.appendChild(seg('mesh', true, [
        { value: true, icon: 'model', tip: 'dlv3_with_model' },
        { value: false, icon: 'bones', tip: 'dlv3_anim_only' },
      ]));
    }
    if (o.mixamo && sel.target !== 'glb') {
      row.appendChild(el('button', {
        type: 'button', class: `dl3-segbtn dl3-mx${sel.mixamo ? ' on' : ''}`, 'aria-pressed': sel.mixamo ? 'true' : 'false',
        title: tr('dlv3_mixamo'), 'aria-label': tr('dlv3_mixamo'),
        onclick: () => { sel.mixamo = !sel.mixamo; saveSel(); render(); },
      }, icon('mixamo')));
    }
  }
  if (row.childNodes.length) panel.appendChild(row);

  // animations: the run's current clips, all or chosen
  const clips = clipsOf();
  if (clips.length && sel.target !== 'zip') {
    const chosen = chosenClips();
    const all = chosen.length === clips.length;
    const list = el('ul', { class: 'dl3-clips', 'aria-label': tr('dlv3_clips') });
    clips.forEach((clip) => {
      const on = chosen.includes(clip);
      list.appendChild(el('li', { class: on ? 'on' : '' },
        el('label', { class: 'dl3-clip', title: clip },
          el('input', { type: 'checkbox', checked: on, onchange: (e) => {
            const set = new Set(chosenClips());
            if (e.target.checked) set.add(clip); else set.delete(clip);
            sel.clips = set.size === clips.length ? null : clips.filter((c) => set.has(c));
            saveSel(); render();
          } }),
          el('span', { text: clip }))));
    });
    panel.appendChild(el('div', { class: 'dl3-sec' },
      el('div', { class: 'dl3-sechead' },
        el('h4', { text: tr('dlv3_clips') }),
        el('span', { class: 'dl3-count', text: tr('dlv3_count', { n: chosen.length, total: clips.length }) }),
        el('button', { type: 'button', class: `dl3-all${all ? ' on' : ''}`, 'aria-pressed': all ? 'true' : 'false', title: tr('dlv3_all'),
          onclick: () => { sel.clips = all ? [] : null; saveSel(); render(); } }, icon('check'), el('span', { text: tr('dlv3_all') }))),
      list));
  }

  // the one green button
  const req = request();
  const empty = req.custom && req.custom.mesh === false && chosenClips().length === 0;
  const label = sel.target === 'zip' ? 'ZIP' : `${TARGET_LABEL[sel.target]} · ${{ unity: 'FBX', unreal: 'FBX', blender: '.blend', glb: 'GLB' }[sel.target]}`;
  panel.appendChild(el('button', {
    type: 'button', class: 'dl3-go', disabled: empty, title: empty ? tr('dlv3_pick_clip') : `${tr('dlv3_download')} ${label}`,
    onclick: () => choose(request()),
  }, icon('rig'), el('span', { text: tr('dlv3_download') }), el('small', { text: label })));

  panel.appendChild(el('div', { class: 'dl3-prog', id: 'dl3-prog', hidden: true },
    el('div', { class: 'dl3-bar', role: 'progressbar', 'aria-valuemin': '0', 'aria-valuemax': '100', 'aria-valuenow': '0' }, el('i', { id: 'dl3-fill' })),
    el('p', { class: 'dl3-line' }, el('span', { id: 'dl3-stage' }), el('b', { id: 'dl3-pct' }))));
  panel.classList.toggle('locked', !!(manifest.access && !manifest.access.allowed));
  if (busy) progress(busy.view || { state: 'queued', progress: 0.02 });
}

function paywall(access, plan, req) {
  remember(req);
  const box = el('div', { class: 'dl3-pay' });
  box.appendChild(el('div', { class: 'dl3-pay-lock' }, icon('lock')));
  if (access.reason === 'not_owner') {
    box.appendChild(el('p', { class: 'dl3-pay-title', text: tr('dlv3_pay_not_owner') }));
    forget();
  } else {
    box.appendChild(el('p', { class: 'dl3-pay-title', text: tr('dlv3_pay_title') }));
    box.appendChild(el('ul', { class: 'dl3-pay-list' },
      el('li', { title: tr('dlv3_pay_files') }, icon('files'), el('span', { text: tr('dlv3_pay_files') })),
      el('li', { title: tr('dlv3_pay_engines') }, icon('engines'), el('span', { text: tr('dlv3_pay_engines') })),
      el('li', { title: tr('dlv3_pay_unlimited') }, icon('infinite'), el('span', { text: tr('dlv3_pay_unlimited') }))));
    box.appendChild(el('p', { class: 'dl3-pay-price', text: priceText(plan) }));
    if (!access.signed_in) {
      box.appendChild(el('a', { class: 'dl3-cta', id: 'dl3-signin', href: plan.login_url, title: tr('dlv3_pay_signin_first') },
        icon('google'), el('span', { text: tr('dlv3_pay_signin') })));
      box.appendChild(el('p', { class: 'dl3-pay-hint', text: tr('dlv3_pay_signin_first') }));
    } else {
      const wait = el('p', { class: 'dl3-pay-hint', id: 'dl3-wait', hidden: true, text: tr('dlv3_pay_waiting') });
      box.appendChild(el('a', {
        class: 'dl3-cta', id: 'dl3-subscribe', href: plan.checkout_url, target: '_blank', rel: 'noopener', title: priceText(plan),
        onclick: () => { wait.hidden = false; watchPayment(req); },
      }, icon('infinite'), el('span', { text: tr('dlv3_pay_subscribe') })));
      box.appendChild(wait);
    }
  }
  const old = panel.querySelector('.dl3-pay');
  if (old) old.remove();
  panel.appendChild(box);
  box.scrollIntoView({ block: 'nearest' });
}

// After the checkout tab opens, this tab keeps asking the server whether the subscription is active; the Gumroad
// webhook switches it on, then the remembered download continues here by itself.
function watchPayment(req) {
  clearInterval(payWatch);
  const check = async () => {
    const r = await api(`/api/task/${encodeURIComponent(taskId)}/downloads-v3`);
    if (r.ok && r.body && r.body.access && r.body.access.allowed) {
      clearInterval(payWatch);
      payWatch = 0;
      manifest = r.body;
      render();
      choose(req);
    }
  };
  payWatch = setInterval(check, 5000);
  addEventListener('focus', check, { once: true });
}

function progress(view) {
  const box = panel && panel.querySelector('#dl3-prog');
  if (!box) return;
  box.hidden = false;
  const pct = Math.max(0, Math.min(100, Math.round(Number(view.progress || 0) * 100)));
  const failed = view.state === 'failed';
  box.classList.toggle('failed', failed);
  box.querySelector('#dl3-fill').style.width = `${failed ? 100 : pct}%`;
  box.querySelector('.dl3-bar').setAttribute('aria-valuenow', String(pct));
  let stage = '';
  if (failed) stage = tr('dlv3_failed');
  else if (view.state === 'ready') stage = tr('dlv3_ready');
  else if (view.stage === 'waiting_worker') stage = tr('dlv3_state_waiting_worker');
  else if (view.state === 'queued') stage = tr('dlv3_state_queued');
  else {
    const key = String(view.stage || '').split(' ')[0];
    stage = tr(`dlv3_stage_${key}`);
    if (stage === `dlv3_stage_${key}`) stage = tr('dlv3_stage_taken');
  }
  box.querySelector('#dl3-stage').textContent = stage;
  box.querySelector('#dl3-pct').textContent = failed ? '' : `${pct}%`;
  const go = panel.querySelector('.dl3-go');
  if (go) go.classList.toggle('busy', !!busy && !failed && view.state !== 'ready');
}

function save(url) {
  const a = el('a', { href: withAs(url), download: '', hidden: true });
  document.body.appendChild(a);
  a.click();
  setTimeout(() => a.remove(), 1000);
}

async function choose(req) {
  if (!manifest || !req) return;
  if (typeof req === 'string') req = { fmt: req };          // an older remembered download
  const key = requestKey(req);
  if (busy && busy.key === key && busy.view && busy.view.state !== 'failed') return;
  if (manifest.access && !manifest.access.allowed) {
    paywall(manifest.access, manifest.plan, req);
    return;
  }
  if (busy) clearTimeout(busy.timer);
  busy = { key, req, timer: 0, view: { state: 'queued', progress: 0.02 } };
  progress(busy.view);
  const base = `/api/task/${encodeURIComponent(taskId)}`;
  let fmt = req.fmt || null;
  const step = async (first) => {
    let r;
    try {
      if (first && req.custom) r = await api(`${base}/downloads-v3-custom`, { method: 'POST', body: JSON.stringify(req.custom) });
      else r = await api(`${base}/downloads-v3/${encodeURIComponent(fmt)}`, first ? { method: 'POST' } : undefined);
    } catch (e) { r = { status: 0 }; }
    if (r.status === 401 || r.status === 402 || r.status === 403) {
      const detail = (r.body && r.body.detail) || {};
      busy = null;
      const p = panel.querySelector('#dl3-prog'); if (p) p.hidden = true;
      paywall({ ...(manifest.access || {}), reason: (detail.access && detail.access.reason) || 'subscription_required', signed_in: !!(detail.access && detail.access.signed_in) }, detail.plan || manifest.plan, req);
      return;
    }
    if (!r.ok || !r.body) {
      if (r.status === 0 || r.status >= 500) { busy.timer = setTimeout(() => step(first && !fmt), 3000); return; }
      busy.view = { state: 'failed', progress: 0 };
      busy.failedReq = req;
      progress(busy.view);
      return;
    }
    if (r.body.format) fmt = r.body.format;
    busy.view = r.body;
    progress(r.body);
    if (r.body.state === 'ready' && r.body.url) {
      forget();
      save(r.body.url);
      const done = busy;
      setTimeout(() => { if (busy === done) { busy = null; const p = panel.querySelector('#dl3-prog'); if (p) p.hidden = true; refresh(); } }, 2500);
      return;
    }
    if (r.body.state === 'failed') { forget(); busy.failedReq = req; return; }
    busy.timer = setTimeout(() => step(r.body.state === 'missing'), 1000);
  };
  step(true);
}

// a failed export is retried by tapping the progress line
document.addEventListener('click', (e) => {
  const box = e.target && e.target.closest && e.target.closest('#dl3-prog.failed');
  if (box && busy && busy.failedReq) { const req = busy.failedReq; busy = null; choose(req); }
});

// ---------------------------------------------------------------- the big green «Download Rig» button
function buildHero() {
  const bar = document.getElementById('tv3-tools');
  if (!bar || hero) return;
  hero = el('button', { type: 'button', class: 'dl3-hero', id: 'dl3-hero', hidden: true, title: tr('dlv3_hero_tip'), 'aria-label': tr('dlv3_hero'),
    onclick: (e) => { e.preventDefault(); if (panel && !panel.hidden) close(); else open(); } },
  el('i', { class: 'dl3-hero-shine', 'aria-hidden': 'true' }), icon('rig'), el('span', { text: tr('dlv3_hero') }));
  bar.appendChild(hero);
  if (REDUCED) return;
  // alive, not metronomic: a short burst at an uneven interval, on top of CSS loops of unequal lengths
  const burst = () => {
    if (hero && !hero.hidden && !document.hidden) {
      hero.classList.remove('burst');
      void hero.offsetWidth;
      hero.classList.add('burst');
    }
    setTimeout(burst, 2200 + Math.random() * 5600);
  };
  setTimeout(burst, 1500 + Math.random() * 2000);
}

function showHero() {
  if (!hero) buildHero();
  if (!hero) return;
  const ready = !!(manifest && manifest.available);
  if (ready && hero.hidden) {
    hero.hidden = false;
    hero.classList.add('enter');
  }
  if (!ready) hero.hidden = true;
  if (ready) { clearInterval(watchTimer); watchTimer = 0; }
}

function watchRig() {
  // the rig appears while the page is open: ask until it does (a classic task has its own files, no need)
  clearInterval(watchTimer);
  watchTimer = setInterval(() => {
    if (manifest && (manifest.available || manifest.reason === 'legacy')) { clearInterval(watchTimer); watchTimer = 0; return; }
    if (!document.hidden) refresh();
  }, 6000);
}

function wire() {
  if (!tool || !UUID.test(taskId)) return;
  tool.setAttribute('title', tr('dlv3_title'));
  tool.setAttribute('aria-label', tr('dlv3_title'));
  tool.setAttribute('aria-haspopup', 'dialog');
  tool.addEventListener('click', (e) => {
    e.preventDefault();
    e.stopImmediatePropagation();
    if (panel && !panel.hidden) close();
    else open();
  }, true);
  buildHero();
  prefetch = refresh().catch(() => {});
  watchRig();
  // back from sign-in (/auth/login?next=…&dl=1): reopen and continue the remembered download
  if (params.get('dl') === '1') open(pending() || null);
}

addEventListener('languageChanged', () => {
  if (tool) { tool.setAttribute('title', tr('dlv3_title')); tool.setAttribute('aria-label', tr('dlv3_title')); }
  if (hero) {
    hero.title = tr('dlv3_hero_tip');
    hero.setAttribute('aria-label', tr('dlv3_hero'));
    const span = hero.querySelector('span'); if (span) span.textContent = tr('dlv3_hero');
  }
  if (panel && !panel.hidden) render();
});
wire();
window.autorigDownloads = { build: BUILD, open, refresh, sel };
