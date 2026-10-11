// Texturing · V3 (owner, 2026-10-11): «я уже просил делать обязательное текстурирование … почему тогда до сих пор
// картошка без материалов PBR?» An upload whose textures / material library were not uploaded (an OBJ without its
// .mtl and images, an FBX with external textures) gets a texture tool in the task bar: the missing file names and one
// button to add them (or a ZIP) to this same task; the source is prepared again with them and the task runs a new
// attempt. Without them the model is textured automatically on our farm; the tool then shows that. Icon first, short
// lines (owner: icons, minimum text). The server decides everything: GET /api/task/<id>/v3-textures.
const BUILD = 'tex-20261011.1';
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const taskId = String(new URLSearchParams(location.search).get('id') || '').trim().toLowerCase();

const FALLBACK = {
  en: {
    tex_title: 'Textures', tex_missing: 'Missing files: {files}', tex_add: 'Add files or a ZIP',
    tex_hint: 'Without them the model gets free automatic textures.', tex_uploading: 'Uploading…',
    tex_done: 'Added: the model is being prepared again with them', tex_nothing: 'Nothing new was added',
    tex_auto: 'Textured automatically (free, on our farm)', tex_own: 'The model\'s own textures',
    error_source_files_not_owner: 'Only the owner of this task can add files',
    error_source_files_not_v3: 'Files can be added to new (V3) tasks only',
    error_source_files_no_upload: 'This task has no uploaded model to add files to',
    error_source_files_empty: 'Choose at least one file', error_source_files_too_large: 'The files are larger than 512 MB',
    error_source_files_unreadable: 'The model could not be read with these files',
    error_source_files_busy: 'The model is still being processed, try again in a minute',
  },
  ru: {
    tex_title: 'Текстуры', tex_missing: 'Не хватает файлов: {files}', tex_add: 'Добавить файлы или ZIP',
    tex_hint: 'Без них модель получит бесплатные автотекстуры.', tex_uploading: 'Загрузка…',
    tex_done: 'Добавлено: модель готовится заново с ними', tex_nothing: 'Ничего нового не добавлено',
    tex_auto: 'Текстуры сделаны автоматически (бесплатно, на нашей ферме)', tex_own: 'Собственные текстуры модели',
    error_source_files_not_owner: 'Добавить файлы может только владелец задачи',
    error_source_files_not_v3: 'Файлы можно добавить только в новые (V3) задачи',
    error_source_files_no_upload: 'В этой задаче нет загруженной модели',
    error_source_files_empty: 'Выберите хотя бы один файл', error_source_files_too_large: 'Файлы больше 512 МБ',
    error_source_files_unreadable: 'С этими файлами модель не читается',
    error_source_files_busy: 'Модель ещё обрабатывается, попробуйте через минуту',
  },
};

function uiLang() {
  let lang = window.I18n && window.I18n.currentLang;
  if (!lang) { try { lang = localStorage.getItem('autorig_lang'); } catch (e) { lang = ''; } }
  return String(lang || navigator.language || 'en').slice(0, 2).toLowerCase();
}

function tr(key, repl) {
  const i18n = window.I18n;
  let text;
  if (i18n && typeof i18n.has === 'function' && i18n.has(key)) text = i18n.t(key, repl);
  else {
    text = (FALLBACK[uiLang()] || FALLBACK.en)[key] || FALLBACK.en[key] || key;
    for (const [k, v] of Object.entries(repl || {})) text = text.split(`{${k}}`).join(String(v));
  }
  return text;
}

const CSS = `
.tv3-tex{position:relative}
.tv3-tex.warn{border-color:#f59e0b;color:#f59e0b;animation:tv3texPulse 2.4s ease-in-out infinite}
.tv3-tex.auto{border-color:#22c55e;color:#22c55e}
@keyframes tv3texPulse{0%,100%{box-shadow:0 0 0 0 rgba(245,158,11,.0)}50%{box-shadow:0 0 0 4px rgba(245,158,11,.28)}}
@media (prefers-reduced-motion: reduce){.tv3-tex.warn{animation:none}}
.tv3-texpop{position:absolute;z-index:40;top:44px;left:0;width:min(320px,calc(100vw - 32px));padding:12px;border-radius:14px;
  border:1px solid var(--tv3-line,rgba(255,255,255,.14));background:rgba(15,18,30,.94);color:#e8ecff;font:13px/1.45 system-ui,sans-serif;
  backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px);box-shadow:0 10px 30px rgba(0,0,0,.45)}
html[dir="rtl"] .tv3-texpop{left:auto;right:0}
.tv3-texpop[hidden]{display:none}
.tv3-texpop .row{display:flex;gap:8px;align-items:flex-start;margin:0 0 8px}
.tv3-texpop .row svg{flex:0 0 18px;width:18px;height:18px;fill:none;stroke:currentColor;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round}
.tv3-texpop code{font:12px ui-monospace,Menlo,Consolas,monospace;color:#fcd34d;word-break:break-all}
.tv3-texpop .hint{opacity:.72;font-size:12px}
.tv3-texpop button.add{display:flex;align-items:center;gap:8px;width:100%;justify-content:center;margin-top:6px;padding:9px 12px;
  border-radius:10px;border:0;background:#6366f1;color:#fff;font-weight:600;cursor:pointer}
.tv3-texpop button.add:disabled{opacity:.6;cursor:wait}
.tv3-texpop .msg{margin-top:8px;font-size:12px}
.tv3-texpop .msg.err{color:#fca5a5}
.tv3-texpop .msg.ok{color:#86efac}
`;

const ICON = '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="3.5" y="3.5" width="17" height="17" rx="3"/>'
  + '<path d="M3.5 15l4.5-4.5 4 4 3-3 5.5 5.5"/><circle cx="15.5" cy="8.5" r="1.6"/></svg>';
const ICON_FILE = '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M14 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8z"/>'
  + '<path d="M14 3v5h5M12 11v6M9 14l3-3 3 3"/></svg>';
const ICON_SPARK = '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3l1.8 4.6L18.5 9l-4.7 1.4L12 15l-1.8-4.6L5.5 9l4.7-1.4z"/>'
  + '<path d="M18 15l.8 2 2 .8-2 .8-.8 2-.8-2-2-.8 2-.8z"/></svg>';

let state = null;
let tool = null;
let pop = null;

function esc(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

function errorText(body) {
  const d = body && body.detail;
  if (d && d.error_string) return tr(`error_${d.error_string}`) || d.message_string || '';
  return '';
}

function ensureTool() {
  if (tool) return tool;
  const bar = document.getElementById('tv3-tools');
  if (!bar) return null;
  if (!document.getElementById('tv3-tex-css')) {
    const st = document.createElement('style');
    st.id = 'tv3-tex-css';
    st.textContent = CSS;
    document.head.appendChild(st);
  }
  tool = document.createElement('button');
  tool.type = 'button';
  tool.className = 'tv3-tool tv3-tex';
  tool.id = 'tv3-tex';
  tool.innerHTML = ICON;
  tool.dataset.build = BUILD;
  const anchor = document.getElementById('tv3-full');
  bar.insertBefore(tool, anchor ? anchor.nextSibling : null);
  pop = document.createElement('div');
  pop.className = 'tv3-texpop';
  pop.hidden = true;
  pop.setAttribute('role', 'dialog');
  tool.appendChild(pop);
  tool.addEventListener('click', (ev) => {
    if (pop.contains(ev.target)) return;
    pop.hidden = !pop.hidden;
    if (!pop.hidden) render();
  });
  document.addEventListener('click', (ev) => { if (tool && !tool.contains(ev.target)) pop.hidden = true; });
  return tool;
}

function render() {
  if (!state || !ensureTool()) return;
  const missing = state.missing || [];
  const auto = state.autotex && state.autotex.live !== false;
  const warn = state.status === 'missing_files' && missing.length;
  tool.classList.toggle('warn', Boolean(warn) && !auto);
  tool.classList.toggle('auto', Boolean(auto));
  const label = tr('tex_title');
  tool.title = label;
  tool.setAttribute('aria-label', label);
  const parts = [];
  if (warn) {
    parts.push(`<p class="row">${ICON}<span>${esc(tr('tex_missing', { files: '' }))}<br><code>${missing.slice(0, 12).map(esc).join(', ')}</code></span></p>`);
  }
  if (auto) parts.push(`<p class="row">${ICON_SPARK}<span>${esc(tr('tex_auto'))}</span></p>`);
  else if (state.status === 'source_ok') parts.push(`<p class="row">${ICON}<span>${esc(tr('tex_own'))}</span></p>`);
  if (warn && state.can_add_files) {
    parts.push(`<button type="button" class="add">${ICON_FILE}<span>${esc(tr('tex_add'))}</span></button>`);
    if (!auto) parts.push(`<p class="hint">${esc(tr('tex_hint'))}</p>`);
    parts.push('<p class="msg" aria-live="polite"></p>');
  }
  pop.innerHTML = parts.join('');
  const btn = pop.querySelector('button.add');
  if (btn) btn.addEventListener('click', pick);
}

function pick() {
  const input = document.createElement('input');
  input.type = 'file';
  input.multiple = true;
  input.accept = (state.accept || []).join(',');
  input.addEventListener('change', () => upload(Array.from(input.files || [])));
  input.click();
}

async function upload(files) {
  if (!files.length) return;
  const btn = pop.querySelector('button.add');
  const msg = pop.querySelector('.msg');
  if (btn) btn.disabled = true;
  if (msg) { msg.className = 'msg'; msg.textContent = tr('tex_uploading'); }
  const form = new FormData();
  for (const f of files) form.append('files', f, f.name);
  try {
    const r = await fetch(state.add_url, { method: 'POST', body: form, credentials: 'same-origin' });
    const body = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(errorText(body) || `HTTP ${r.status}`);
    if (msg) {
      msg.className = 'msg ok';
      msg.textContent = body.changed ? tr('tex_done') : tr('tex_nothing');
    }
    setTimeout(load, 1500);
  } catch (e) {
    if (msg) { msg.className = 'msg err'; msg.textContent = String(e.message || e); }
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function load() {
  if (!UUID.test(taskId)) return;
  try {
    const r = await fetch(`/api/task/${taskId}/v3-textures`, { credentials: 'same-origin', cache: 'no-store' });
    if (!r.ok) return;
    state = await r.json();
  } catch (e) { return; }
  const show = (state.status === 'missing_files' && (state.missing || []).length) || (state.autotex && state.autotex.live);
  if (!show) { if (tool) tool.hidden = true; return; }
  ensureTool();
  if (tool) tool.hidden = false;
  render();
}

addEventListener('languageChanged', render);
load();
setInterval(load, 60000);
