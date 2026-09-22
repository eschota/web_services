/** Explicit, local sharing controls for node inputs and outputs. */
(function (global) {
  'use strict';

  function install(options) {
    options = options || {};
    const canvas = options.canvas;
    const getMeta = options.getMeta || function () { return null; };
    const toast = typeof options.toast === 'function' ? options.toast : function () {};
    if (!canvas) throw new Error('AINodeShare.install requires canvas');

    installStyles();
    let dialog = null;
    let observer = null;
    let stopped = false;

    function nodeId(element) {
      return String(element && element.id || '').replace(/^node-/, '');
    }

    function httpUrl(value) {
      try {
        value = String(value || '').trim();
        if (!/^https?:\/\//i.test(value)) return '';
        const url = new URL(value);
        return /^https?:$/.test(url.protocol) && !url.username && !url.password ? url.href : '';
      } catch (_) { return ''; }
    }

    function inputShare(element, meta) {
      const field = element.querySelector('.ninput [data-value]');
      if (!field) return null;
      const type = String(meta.entityType || meta.entity_type || '');
      if (type === 'avatar') {
        const preview = element.querySelector('.ninput img[data-preview]');
        const reference = preview && !preview.hidden ? httpUrl(preview.currentSrc || preview.src) : '';
        return reference ? {
          kind: 'image', value: reference, label: 'Reference image',
          note: 'This shares only the Avatar reference image. The private Avatar profile and version are not shared.'
        } : null;
      }
      const value = String(field.value || '');
      if (type === 'text') return value ? { kind: 'text', value: value, label: 'Input text' } : null;
      const url = httpUrl(value);
      if (url) return { kind: type === 'video' ? 'video' : 'image', value: url, label: type === 'video' ? 'Input video' : 'Input image' };
      if (/^(data:|blob:)/i.test(value) || (element.querySelector('.ninput [data-preview]:not([hidden])') && !url)) {
        return { kind: 'pending', value: '', label: 'Input media', note: 'Wait for the upload to finish before sharing.' };
      }
      return null;
    }

    function outputShare(element) {
      const host = element.querySelector('.nout');
      if (!host) return null;
      const text = host.querySelector('.ntext');
      if (text && text.textContent) return { kind: 'text', value: text.textContent, label: 'Output text' };
      const video = host.querySelector('video.preview-expandable');
      const videoUrl = video && httpUrl(video.currentSrc || video.src);
      if (videoUrl) return { kind: 'video', value: videoUrl, label: 'Output video' };
      const image = host.querySelector('img.preview-expandable');
      const imageUrl = image && httpUrl(image.currentSrc || image.src);
      if (imageUrl) return { kind: 'image', value: imageUrl, label: 'Output image' };
      const link = host.querySelector('a[href]');
      const linkUrl = link && httpUrl(link.href);
      if (linkUrl) return { kind: 'file', value: linkUrl, label: 'Output file' };
      if ((video && video.src) || (image && image.src)) {
        return { kind: 'pending', value: '', label: 'Output media', note: 'Wait until the output has a shareable web address.' };
      }
      return null;
    }

    function shareValue(element) {
      const meta = getMeta(nodeId(element)) || {};
      return meta.kind === 'input' || element.classList.contains('input-node')
        ? inputShare(element, meta)
        : outputShare(element);
    }

    function updateButton(element) {
      const button = element.querySelector('.node-share-button');
      if (!button) return;
      const item = shareValue(element);
      button.disabled = !item || item.kind === 'pending';
      button.title = item
        ? (item.kind === 'pending' ? item.note : 'Share ' + item.label.toLowerCase())
        : (element.classList.contains('input-node') ? 'Add an input to share' : 'Share becomes available when this node has output');
      button.setAttribute('aria-label', button.title);
    }

    function prepare(element) {
      if (!element || !element.matches('.drawflow-node[id^="node-"]')) return;
      const header = element.querySelector('.nhead');
      if (!header) return;
      let button = header.querySelector('.node-share-button');
      if (!button) {
        button = document.createElement('button');
        button.type = 'button';
        button.className = 'node-share-button';
        button.textContent = '↗';
        button.addEventListener('pointerdown', event => { event.preventDefault(); event.stopPropagation(); });
        button.addEventListener('mousedown', event => { event.preventDefault(); event.stopPropagation(); });
        button.addEventListener('dblclick', event => { event.preventDefault(); event.stopPropagation(); });
        button.addEventListener('click', event => {
          event.preventDefault(); event.stopPropagation();
          const item = shareValue(element);
          if (item && item.kind !== 'pending') openDialog(item);
        });
        header.appendChild(button);
      }
      const modeButton = header.querySelector('.node-display-mode');
      if (modeButton && modeButton.nextElementSibling !== button) modeButton.after(button);
      updateButton(element);
    }

    function createDialog() {
      const panel = document.createElement('dialog');
      panel.className = 'ai-node-share-dialog';
      panel.setAttribute('aria-labelledby', 'ai-node-share-title');
      panel.innerHTML = '<div class="node-share-card">' +
        '<div class="node-share-head"><strong id="ai-node-share-title">Share</strong><button type="button" data-close aria-label="Close">×</button></div>' +
        '<div class="node-share-preview" data-preview></div>' +
        '<label class="node-share-value-label" for="ai-node-share-value">What will be shared</label>' +
        '<textarea id="ai-node-share-value" data-value readonly></textarea>' +
        '<p class="node-share-note" data-note></p>' +
        '<div class="node-share-actions"><button type="button" data-copy>Copy</button><a data-open target="_blank" rel="noopener">Open</a><a data-download download>Download</a><button type="button" data-native>System share</button></div>' +
        '</div>';
      panel.querySelector('[data-close]').addEventListener('click', () => panel.close());
      panel.addEventListener('click', event => { if (event.target === panel) panel.close(); });
      document.body.appendChild(panel);
      return panel;
    }

    function previewItem(host, item) {
      host.innerHTML = '';
      if (item.kind === 'image') {
        const image = document.createElement('img'); image.src = item.value; image.alt = item.label; host.appendChild(image);
      } else if (item.kind === 'video') {
        const video = document.createElement('video'); video.src = item.value; video.controls = true; video.loop = true; video.playsInline = true; host.appendChild(video);
      } else if (item.kind === 'text') {
        const block = document.createElement('div'); block.className = 'node-share-text'; block.textContent = item.value; host.appendChild(block);
      } else {
        const link = document.createElement('span'); link.textContent = 'Shareable file link'; host.appendChild(link);
      }
    }

    async function copyExplicitly(value, field) {
      if (!navigator.clipboard || !navigator.clipboard.writeText) {
        field.focus(); field.select();
        toast('Clipboard access is unavailable. Press Ctrl+C to copy the selected value.');
        return;
      }
      try {
        await navigator.clipboard.writeText(value);
        toast('Copied to clipboard.');
      } catch (_) {
        // Do not try a second write path after refusal: the existing clipboard
        // must remain untouched. Selection gives the user an explicit fallback.
        field.focus(); field.select();
        toast('Copy was refused. Press Ctrl+C to copy the selected value.');
      }
    }

    function openDialog(item) {
      if (!dialog) dialog = createDialog();
      dialog.querySelector('#ai-node-share-title').textContent = 'Share ' + item.label.toLowerCase();
      const field = dialog.querySelector('[data-value]');
      field.value = item.value;
      field.rows = item.kind === 'text' ? 7 : 3;
      const note = dialog.querySelector('[data-note]');
      note.textContent = item.note || 'Nothing is sent until you choose an action below.';
      previewItem(dialog.querySelector('[data-preview]'), item);
      const copy = dialog.querySelector('[data-copy]');
      copy.textContent = item.kind === 'text' ? 'Copy text' : 'Copy link';
      copy.onclick = () => copyExplicitly(item.value, field);
      const open = dialog.querySelector('[data-open]');
      const download = dialog.querySelector('[data-download]');
      const isUrl = item.kind !== 'text';
      open.hidden = !isUrl; download.hidden = !isUrl;
      if (isUrl) { open.href = item.value; download.href = item.value; }
      else { open.removeAttribute('href'); download.removeAttribute('href'); }
      const native = dialog.querySelector('[data-native]');
      native.hidden = typeof navigator.share !== 'function';
      native.onclick = async () => {
        try {
          const payload = item.kind === 'text'
            ? { title: item.label, text: item.value }
            : { title: item.label, url: item.value };
          await navigator.share(payload);
        } catch (error) {
          if (!error || error.name !== 'AbortError') toast('System sharing was unavailable. The share panel is still open.');
        }
      };
      if (!dialog.open) dialog.showModal();
    }

    const onValueChange = event => {
      const node = event.target.closest && event.target.closest('.drawflow-node[id^="node-"]');
      if (node) updateButton(node);
    };
    canvas.addEventListener('input', onValueChange, true);
    canvas.addEventListener('change', onValueChange, true);

    observer = new MutationObserver(records => {
      if (stopped) return;
      const affected = new Set();
      records.forEach(record => {
        const owner = record.target.nodeType === 1 && record.target.closest('.drawflow-node[id^="node-"]');
        if (owner) affected.add(owner);
        record.addedNodes.forEach(added => {
          if (added.nodeType !== 1) return;
          if (added.matches && added.matches('.drawflow-node[id^="node-"]')) affected.add(added);
          else if (added.querySelectorAll) added.querySelectorAll('.drawflow-node[id^="node-"]').forEach(node => affected.add(node));
        });
      });
      affected.forEach(prepare);
    });
    observer.observe(canvas, { childList: true, subtree: true, attributes: true, attributeFilter: ['src', 'href', 'hidden'] });
    canvas.querySelectorAll('.drawflow-node[id^="node-"]').forEach(prepare);

    return {
      refresh: function (id) {
        const node = id == null ? null : canvas.querySelector('#node-' + CSS.escape(String(id).replace(/^node-/, '')));
        if (node) prepare(node); else canvas.querySelectorAll('.drawflow-node[id^="node-"]').forEach(prepare);
      },
      destroy: function () {
        stopped = true;
        if (observer) observer.disconnect();
        canvas.removeEventListener('input', onValueChange, true);
        canvas.removeEventListener('change', onValueChange, true);
        if (dialog) { dialog.remove(); dialog = null; }
      }
    };
  }

  function installStyles() {
    if (document.getElementById('ai-node-share-style')) return;
    const style = document.createElement('style');
    style.id = 'ai-node-share-style';
    style.textContent = [
      '.node-share-button{flex:0 0 auto;width:24px;height:22px;margin-left:3px;padding:0;border:1px solid rgba(255,255,255,.18);border-radius:6px;background:rgba(255,255,255,.06);color:#67e8f9;font:700 14px/20px Inter,sans-serif;cursor:pointer}',
      '.node-share-button:hover:not(:disabled),.node-share-button:focus-visible{background:rgba(56,189,248,.18);border-color:#38bdf8;outline:none}',
      '.node-share-button:disabled{opacity:.3;cursor:not-allowed}',
      '.ai-node-share-dialog{width:min(560px,calc(100vw - 28px));max-height:calc(100vh - 36px);padding:0;border:1px solid #334155;border-radius:14px;background:#0f1020;color:#eef;box-shadow:0 24px 80px rgba(0,0,0,.65)}',
      '.ai-node-share-dialog::backdrop{background:rgba(4,5,10,.78);backdrop-filter:blur(3px)}',
      '.node-share-card{padding:16px;display:grid;gap:12px}',
      '.node-share-head{display:flex;align-items:center;justify-content:space-between;gap:12px}',
      '.node-share-head strong{font-size:16px}.node-share-head button{border:0;background:transparent;color:#cbd5e1;font-size:25px;cursor:pointer}',
      '.node-share-preview{min-height:72px;max-height:42vh;display:flex;align-items:center;justify-content:center;overflow:auto;border-radius:10px;background:#080914;color:#94a3b8}',
      '.node-share-preview img,.node-share-preview video{display:block;max-width:100%;max-height:42vh;object-fit:contain}',
      '.node-share-text{width:100%;padding:14px;white-space:pre-wrap;overflow-wrap:anywhere;line-height:1.45}',
      '.node-share-value-label{font-size:12px;color:#9aa0b5}',
      '.ai-node-share-dialog textarea{width:100%;resize:vertical;box-sizing:border-box;border:1px solid #334155;border-radius:8px;padding:9px 10px;background:#080914;color:#e2e8f0;font:12px/1.4 ui-monospace,SFMono-Regular,Consolas,monospace}',
      '.node-share-note{min-height:18px;margin:0;color:#9aa0b5;font-size:12px;line-height:1.45}',
      '.node-share-actions{display:flex;flex-wrap:wrap;gap:8px;justify-content:flex-end}',
      '.node-share-actions button,.node-share-actions a{border:1px solid #475569;border-radius:8px;background:#1e293b;color:#e2e8f0;padding:8px 11px;text-decoration:none;font-size:13px;cursor:pointer}',
      '.node-share-actions button:hover,.node-share-actions a:hover{background:#334155}'
    ].join('\n');
    document.head.appendChild(style);
  }

  global.AINodeShare = Object.freeze({ install: install });
})(window);
