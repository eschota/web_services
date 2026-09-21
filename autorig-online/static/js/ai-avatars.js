(function () {
  'use strict';

  const MAX_IMAGE_BYTES = 12 * 1024 * 1024;
  const state = { avatars: [], editing: null, references: [], upload: null };
  const $ = id => document.getElementById(id);
  const form = $('avatar-form');
  const library = $('avatar-library');
  const message = $('form-message');
  const preview = $('avatar-preview');
  const drop = $('avatar-drop');
  const fileInput = $('avatar-file');
  const referenceInput = $('avatar-reference-url');

  function text(value) { return value == null ? '' : String(value); }

  function setMessage(value, kind) {
    message.textContent = value || '';
    message.className = 'av-message' + (kind ? ' ' + kind : '');
  }

  function errorText(payload, fallback) {
    const detail = payload && payload.detail;
    if (typeof detail === 'string') return detail;
    if (detail && typeof detail.message_string === 'string') return detail.message_string;
    if (detail && typeof detail.message === 'string') return detail.message;
    if (detail && typeof detail.error_string === 'string') return detail.error_string;
    if (payload && typeof payload.message === 'string') return payload.message;
    return fallback;
  }

  async function api(path, options) {
    const response = await fetch(path, options);
    let payload = null;
    try { payload = await response.json(); } catch (_) {}
    if (!response.ok) {
      const error = new Error(errorText(payload, 'Request failed (' + response.status + ')'));
      error.status = response.status;
      throw error;
    }
    return payload || {};
  }

  function listFrom(payload) {
    if (Array.isArray(payload)) return payload;
    return payload.avatars_array || payload.avatars || payload.items || [];
  }

  function latestVersion(avatar) {
    const value = avatar.latest_version || avatar.current_version || avatar.version || {};
    if (typeof value === 'number' || typeof value === 'string') return { version: value };
    return value || {};
  }

  function avatarView(avatar) {
    const version = latestVersion(avatar);
    const references = version.references || avatar.references || [];
    const primary = references.find(item => item.role === 'face') || references.find(item => item.media_type === 'image') || {};
    const provenance = version.provenance || avatar.provenance || {};
    return {
      id: avatar.id || avatar.avatar_id,
      name: avatar.display_name || version.display_name || avatar.name || version.name || 'Untitled Avatar',
      version: version.version || version.number || version.version_number || avatar.version_number || 1,
      reference_url: primary.canonical_url || version.reference_url || version.image_url || avatar.reference_url || avatar.image_url || '',
      reference: primary,
      identity_prompt: version.identity_prompt || avatar.identity_prompt || '',
      appearance: version.appearance || avatar.appearance || '',
      wardrobe: version.wardrobe || avatar.wardrobe || '',
      negative_identity_prompt: version.negative_identity_prompt || avatar.negative_identity_prompt || '',
      notes: provenance.note || version.notes || avatar.notes || '',
      provenance: provenance,
      adapter: version.adapter || avatar.adapter || null
    };
  }

  function showPreview(url) {
    const value = text(url).trim();
    if (!value) {
      preview.hidden = true;
      preview.removeAttribute('src');
      return;
    }
    preview.classList.remove('broken');
    preview.src = value;
    preview.hidden = false;
  }

  preview.addEventListener('error', () => {
    preview.hidden = true;
    setMessage('The reference image could not be displayed. Check that the URL points directly to an image.', 'error');
  });

  async function acceptFile(file) {
    if (!file || !file.type.startsWith('image/')) {
      setMessage('Choose a PNG, JPEG or WebP image.', 'error'); return;
    }
    if (file.size > MAX_IMAGE_BYTES) {
      setMessage('The reference image must be 12 MB or smaller.', 'error'); return;
    }
    if (state.upload) return;
    showPreview(URL.createObjectURL(file));
    setMessage('Uploading reference image…');
    const data = new FormData();
    data.append('file', file, file.name || 'avatar-reference.png');
    state.upload = api('/api/ai/avatar-assets', { method: 'POST', body: data });
    try {
      const uploaded = await state.upload;
      const reference = uploaded.reference_object || uploaded.reference || uploaded;
      if (!reference.canonical_url || !reference.sha256) throw new Error('The image upload returned no durable reference.');
      state.references = [reference].concat(state.references.filter(item => item.role !== 'face'));
      referenceInput.value = reference.canonical_url;
      showPreview(reference.canonical_url);
      setMessage('Reference image ready.', 'success');
    } catch (error) {
      showPreview('');
      setMessage(error.message, 'error');
    } finally {
      state.upload = null;
      fileInput.value = '';
    }
  }

  drop.addEventListener('click', () => fileInput.click());
  drop.addEventListener('keydown', event => {
    if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); fileInput.click(); }
  });
  fileInput.addEventListener('change', () => acceptFile(fileInput.files[0]));
  drop.addEventListener('dragover', event => { event.preventDefault(); drop.classList.add('dragging'); });
  drop.addEventListener('dragleave', () => drop.classList.remove('dragging'));
  drop.addEventListener('drop', event => {
    event.preventDefault(); drop.classList.remove('dragging');
    acceptFile(Array.from(event.dataTransfer.files).find(file => file.type.startsWith('image/')));
  });
  document.addEventListener('paste', event => {
    if (/^(INPUT|TEXTAREA)$/.test(document.activeElement && document.activeElement.tagName)) return;
    const file = Array.from(event.clipboardData && event.clipboardData.files || []).find(item => item.type.startsWith('image/'));
    if (file) { event.preventDefault(); acceptFile(file); }
  });
  referenceInput.addEventListener('input', () => {
    const face = state.references.find(item => item.role === 'face');
    if (face && referenceInput.value.trim() !== face.canonical_url) {
      state.references = state.references.filter(item => item.role !== 'face');
    }
  });
  $('preview-url').addEventListener('click', () => {
    const url = referenceInput.value.trim();
    if (!/^https?:\/\//i.test(url)) { setMessage('Enter a complete http:// or https:// image URL.', 'error'); return; }
    importReference(url);
  });

  async function importReference(url) {
    const button = $('preview-url');
    button.disabled = true;
    setMessage('Importing a durable copy of the reference…');
    try {
      const uploaded = await api('/api/ai/avatar-assets/import', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ url: url })
      });
      const reference = uploaded.reference_object || uploaded.reference || uploaded;
      if (!reference.canonical_url || !reference.sha256) throw new Error('The import returned no durable reference.');
      state.references = [reference].concat(state.references.filter(item => item.role !== 'face'));
      referenceInput.value = reference.canonical_url;
      showPreview(reference.canonical_url);
      setMessage('Reference imported and ready.', 'success');
    } catch (error) {
      setMessage(error.status === 400 || error.status === 422
        ? 'That URL cannot be imported. Download the image and upload the file here.'
        : error.message, 'error');
    } finally { button.disabled = false; }
  }

  function resetForm() {
    state.editing = null;
    state.references = [];
    form.reset();
    $('avatar-id').value = '';
    $('editor-title').textContent = 'Create an Avatar';
    $('editor-lead').textContent = 'Start with a clear reference image. Front-facing, even light and an unobstructed face usually transfer best.';
    $('save-avatar').textContent = 'Save Avatar';
    $('cancel-edit').hidden = true;
    showPreview('');
    setMessage('');
  }

  function editAvatar(avatar) {
    const item = avatarView(avatar);
    state.editing = avatar;
    $('avatar-id').value = item.id;
    $('avatar-name').value = item.name;
    $('avatar-identity').value = item.identity_prompt;
    referenceInput.value = item.reference_url;
    $('avatar-appearance').value = item.appearance;
    $('avatar-wardrobe').value = item.wardrobe;
    $('avatar-negative').value = item.negative_identity_prompt;
    $('avatar-notes').value = item.notes;
    state.references = Array.isArray(state.editing.references) ? state.editing.references.slice() : (item.reference && item.reference.canonical_url ? [item.reference] : []);
    showPreview(item.reference_url);
    $('editor-title').textContent = 'Create a new version';
    $('editor-lead').textContent = item.name + ' · current version v' + item.version + '. Saving preserves that version and creates the next one.';
    $('save-avatar').textContent = 'Save new version';
    $('cancel-edit').hidden = false;
    setMessage('');
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }

  $('cancel-edit').addEventListener('click', resetForm);

  function fieldPayload() {
    const existing = state.editing ? avatarView(state.editing) : null;
    const sourceUrls = state.references.map(item => text(item.source_url || item.canonical_url));
    const inferredKind = sourceUrls.some(url => /(?:renderfin|\/render\/|\/tasks\/)/i.test(url)) ? 'generated' : 'uploaded';
    const sourceKind = state.references.length > 1 ? 'mixed' : ((existing && existing.provenance.source_kind) || inferredKind);
    return {
      display_name: $('avatar-name').value.trim(),
      identity_prompt: $('avatar-identity').value.trim(),
      appearance: $('avatar-appearance').value.trim(),
      wardrobe: $('avatar-wardrobe').value.trim(),
      negative_identity_prompt: $('avatar-negative').value.trim(),
      references: state.references.slice(),
      provenance: Object.assign({
        source_kind: sourceKind, source_task_ids: [], source_model: '', source_workflow: '', parameters_sha256: null
      }, (existing && existing.provenance) || {}, { note: $('avatar-notes').value.trim() }),
      adapter: (existing && existing.adapter) || {
        status: 'not_requested', pipeline_family: null, artifact_url: null,
        sha256: null, trigger_token: null, note: ''
      }
    };
  }

  form.addEventListener('submit', async event => {
    event.preventDefault();
    if (state.upload) { setMessage('Wait for the reference image upload to finish.', 'error'); return; }
    const payload = fieldPayload();
    if (!payload.display_name) { $('avatar-name').focus(); setMessage('Give this Avatar a name.', 'error'); return; }
    if (!payload.identity_prompt) { $('avatar-identity').focus(); setMessage('Describe the stable identity traits for this Avatar.', 'error'); return; }
    if (!payload.references.length) { referenceInput.focus(); setMessage('Upload an image or import an AutoRig scratch/render URL first.', 'error'); return; }
    const button = $('save-avatar');
    button.disabled = true;
    setMessage(state.editing ? 'Saving an immutable new version…' : 'Saving Avatar…');
    try {
      if (state.editing) {
        const id = avatarView(state.editing).id;
        await api('/api/ai/avatars/' + encodeURIComponent(id), {
          method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload)
        });
      } else {
        await api('/api/ai/avatars', {
          method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload)
        });
      }
      resetForm();
      await loadAvatars();
    } catch (error) {
      setMessage(error.status === 401 ? 'Sign in to save Avatars.' : error.message, 'error');
    } finally { button.disabled = false; }
  });

  function emptyState(title, copy) {
    library.innerHTML = '';
    const box = document.createElement('div');
    box.className = 'av-state';
    const content = document.createElement('div');
    const strong = document.createElement('strong'); strong.textContent = title;
    const line = document.createElement('span'); line.textContent = copy;
    content.append(strong, line); box.appendChild(content); library.appendChild(box);
  }

  function renderLibrary() {
    $('avatar-count').textContent = state.avatars.length ? state.avatars.length + (state.avatars.length === 1 ? ' saved character' : ' saved characters') : '';
    if (!state.avatars.length) {
      emptyState('No Avatars yet', 'Create one from a reference image. It will appear here for reuse in Nodes.'); return;
    }
    library.innerHTML = '';
    const grid = document.createElement('div'); grid.className = 'av-grid';
    state.avatars.forEach(avatar => {
      const item = avatarView(avatar);
      const card = document.createElement('article'); card.className = 'av-card';
      const media = document.createElement('div'); media.className = 'av-card-media';
      if (item.reference_url) {
        const image = document.createElement('img'); image.src = item.reference_url; image.alt = item.name + ' reference'; image.loading = 'lazy';
        image.addEventListener('error', () => { image.classList.add('broken'); media.textContent = 'Reference unavailable'; });
        media.appendChild(image);
      } else media.textContent = 'No reference image';
      const body = document.createElement('div'); body.className = 'av-card-body';
      const title = document.createElement('div'); title.className = 'av-card-title';
      const heading = document.createElement('h3'); heading.textContent = item.name;
      const version = document.createElement('span'); version.className = 'av-version'; version.textContent = 'v' + item.version; version.title = 'Immutable Avatar version';
      title.append(heading, version);
      const notes = document.createElement('p'); notes.className = 'av-card-notes'; notes.textContent = item.appearance || item.wardrobe || item.notes || 'Reference profile ready for a production graph.';
      const actions = document.createElement('div'); actions.className = 'av-card-actions';
      const use = document.createElement('a'); use.className = 'av-primary'; use.textContent = 'Use in Nodes';
      use.href = '/nodes?avatar=' + encodeURIComponent(item.id) + '@' + encodeURIComponent(item.version);
      const edit = document.createElement('button'); edit.type = 'button'; edit.className = 'av-secondary'; edit.textContent = 'New version'; edit.addEventListener('click', () => editAvatar(avatar));
      actions.append(use, edit); body.append(title, notes, actions); card.append(media, body); grid.appendChild(card);
    });
    library.appendChild(grid);
  }

  async function loadAvatars() {
    $('refresh-avatars').disabled = true;
    try {
      const payload = await api('/api/ai/avatars');
      const summaries = listFrom(payload);
      state.avatars = await Promise.all(summaries.map(async summary => {
        const id = summary.avatar_id || summary.id;
        const version = summary.current_version || summary.version || 1;
        try {
          const detail = await api('/api/ai/avatars/' + encodeURIComponent(id) + '?version=' + encodeURIComponent(version));
          return detail.avatar_object || detail.avatar || detail;
        } catch (_) { return summary; }
      }));
      renderLibrary();
    } catch (error) {
      if (error.status === 401) emptyState('Sign in to use Avatars', 'Your Avatar library is private to your account.');
      else emptyState('Could not load the Avatar library', error.message);
    } finally { $('refresh-avatars').disabled = false; }
  }

  $('refresh-avatars').addEventListener('click', loadAvatars);
  loadAvatars();
})();
