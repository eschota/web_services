// Task page V3: the Upload tool beside Download (owner 2026-10-16: «кнопку Upload рядом с довнлоад, чтобы можно было
// запустить диалог сразу о создании новой таски»). A click opens the file dialog at once; a picture goes to
// generation (/api/generate/from-image), a model to the rig (/api/task/create, pipeline v3); Ctrl+V with a file in the
// clipboard does the same; the page then opens the new task.
const MODEL_EXTS = ['.glb', '.gltf', '.fbx', '.obj', '.zip'];
const IMAGE_EXTS = ['.png', '.jpg', '.jpeg', '.webp'];
const ext = (f) => '.' + String(f.name || '').split('.').pop().toLowerCase();
const tr = (k, fb) => { try { const v = window.I18n && window.I18n.t ? window.I18n.t(k) : null; return v && v !== k ? v : fb; } catch (e) { return fb; } };
let busy = false;

function chip(text) {
  const c = document.getElementById('tv3-chip');
  if (!c) return;
  c.hidden = !text;
  c.textContent = text || '';
  c.title = text || '';
}

async function createModelTask(file) {
  const body = new FormData();
  body.append('source', 'upload');
  body.append('file', file);
  body.append('type', 't_pose');
  body.append('pipeline', 'v3');
  const resp = await fetch('/api/task/create', { method: 'POST', body, credentials: 'same-origin' });
  const data = await resp.json().catch(() => ({}));
  if (resp.status === 401) { window.location.href = '/auth/login'; return null; }
  if (resp.status === 402) { window.location.href = '/buy-credits'; return null; }
  if (!resp.ok) throw new Error((data && data.detail && (data.detail.message_string || data.detail)) || `HTTP ${resp.status}`);
  return data.task_id;
}

async function createImageTask(file) {
  const body = new FormData();
  body.append('file', file);
  const resp = await fetch('/api/generate/from-image', { method: 'POST', body, credentials: 'same-origin' });
  const data = await resp.json().catch(() => ({}));
  if (resp.status === 401) { window.location.href = '/auth/login'; return null; }
  if (resp.status === 402) { window.location.href = '/buy-credits'; return null; }
  if (!resp.ok) throw new Error((data && data.detail && (data.detail.message_string || data.detail)) || `HTTP ${resp.status}`);
  return data.task_id;
}

async function submit(files) {
  if (busy || !files.length) return;
  const models = files.filter((f) => MODEL_EXTS.includes(ext(f)));
  const images = files.filter((f) => IMAGE_EXTS.includes(ext(f)));
  if (!models.length && !images.length) { chip(tr('error_select_model_file', 'Pick a 3D model or a picture')); setTimeout(() => chip(''), 4000); return; }
  if (models.length > 1) { chip(tr('error_one_model_per_task', 'One model per task')); setTimeout(() => chip(''), 4000); return; }
  busy = true;
  chip(tr('upload_progress_uploading', 'Uploading…'));
  try {
    let id = null;
    if (models.length) id = await createModelTask(models[0]);
    else { for (const img of images) { const one = await createImageTask(img); if (one && !id) id = one; } }
    if (id) window.location.href = `/task?id=${encodeURIComponent(id)}`;
  } catch (err) {
    chip(String(err && err.message || err));
    setTimeout(() => chip(''), 6000);
  } finally { busy = false; }
}

const btn = document.getElementById('tv3-upload');
const input = document.getElementById('tv3-upload-input');
if (btn && input) {
  btn.addEventListener('click', () => { if (!busy) input.click(); });
  input.addEventListener('change', () => { const files = Array.from(input.files || []); input.value = ''; submit(files); });
  document.addEventListener('paste', (e) => {
    const tag = (e.target && e.target.tagName || '').toLowerCase();
    if (tag === 'input' || tag === 'textarea' || (e.target && e.target.isContentEditable)) return;
    const dt = e.clipboardData;
    if (!dt) return;
    let files = Array.from(dt.files || []);
    if (!files.length && dt.items) files = Array.from(dt.items).filter((it) => it.kind === 'file').map((it) => it.getAsFile()).filter(Boolean);
    if (!files.length) return;
    e.preventDefault();
    files = files.map((f, i) => (f.name && f.name !== 'image.png') ? f
      : new File([f], `pasted-${Date.now()}-${i + 1}.${(f.type.split('/')[1] || 'png').replace('jpeg', 'jpg')}`, { type: f.type }));
    submit(files);
  });
}
