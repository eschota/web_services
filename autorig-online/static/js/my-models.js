/**
 * «My models» (owner 2026-10-17, docs/my-gallery-privacy-spec-20261017.md): the owner's own cards with quick
 * controls: 🔒/🌐 private-public, 🗑 delete (10 s undo), ✓ select for bulk actions. Icons + tooltips only.
 * API: GET /api/my/tasks, PATCH /api/my/tasks/{id}, DELETE /api/my/tasks/{id}, POST /api/my/tasks/{id}/restore,
 *      POST /api/my/tasks/bulk, GET/POST /api/my/settings.
 */
(function () {
    'use strict';
    const $ = (id) => document.getElementById(id);
    const t = (key, fallback) => {
        try { return (window.I18n && I18n.has && I18n.has(key)) ? I18n.t(key) : fallback; } catch (_) { return fallback; }
    };
    const state = { items: [], page: 1, total: 0, perPage: 48, canPrivate: false, signedIn: false,
                    selected: new Set(), undoSeconds: 10, toastTimer: null, undoIds: [] };

    async function api(method, url, body) {
        const resp = await fetch(url, {
            method, credentials: 'same-origin',
            headers: body ? { 'Content-Type': 'application/json' } : {},
            body: body ? JSON.stringify(body) : undefined,
        });
        let data = null;
        try { data = await resp.json(); } catch (_) { data = null; }
        if (!resp.ok) {
            const code = data && data.detail && data.detail.error_string;
            const err = new Error(code || ('http_' + resp.status));
            err.code = code; err.status = resp.status;
            throw err;
        }
        return data;
    }

    function escapeAttr(v) { return String(v == null ? '' : v).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])); }

    function cardHtml(item) {
        const pub = item.is_public !== false;
        const lockTitle = pub ? t('my_make_private', 'Make private') : t('my_make_public', 'Make public');
        const lockDisabled = pub && !state.canPrivate;
        const stateTitle = pub ? t('my_public', 'Public: in the gallery') : t('my_private', 'Private: only you can see it');
        const busy = item.status === 'created' || item.status === 'processing';
        const sel = state.selected.has(item.task_id);
        const inner = (window.TaskCard ? TaskCard.render(item, {}) : `<a class="tc-card" href="/task?id=${item.task_id}"></a>`);
        return `<div class="my-card${sel ? ' is-selected' : ''}${item.deleting ? ' is-deleting' : ''}" data-id="${escapeAttr(item.task_id)}">`
            + inner
            + `<input type="checkbox" class="my-sel" ${sel ? 'checked' : ''} title="${escapeAttr(t('my_select', 'Select'))}" aria-label="${escapeAttr(t('my_select', 'Select'))}">`
            + `<div class="my-ctl">`
            + `<button type="button" class="my-ibtn my-vis" data-public="${pub ? 1 : 0}" ${lockDisabled ? 'aria-disabled="true"' : ''} title="${escapeAttr(lockDisabled ? t('my_private_locked', 'Private models come with the subscription') : lockTitle)}" aria-label="${escapeAttr(lockTitle)}">${pub ? (lockDisabled ? '🔐' : '🔒') : '🌐'}</button>`
            + `<button type="button" class="my-ibtn my-del" ${busy ? 'disabled' : ''} title="${escapeAttr(busy ? t('my_busy', 'Still processing') : t('my_delete', 'Delete for good'))}" aria-label="${escapeAttr(t('my_delete', 'Delete for good'))}">🗑</button>`
            + `</div>`
            + `<span class="my-state" title="${escapeAttr(stateTitle)}" aria-label="${escapeAttr(stateTitle)}">${pub ? '🌐' : '🔒'}</span>`
            + `</div>`;
    }

    function render() {
        const grid = $('my-grid');
        grid.setAttribute('aria-busy', 'false');
        const visible = state.items.filter((i) => !i.gone);
        if (!visible.length) {
            grid.innerHTML = `<div class="my-empty" style="grid-column:1/-1"><div aria-hidden="true">📦</div>`
                + `<div style="font-size:1rem">${escapeAttr(t('my_empty', 'No models yet'))}</div>`
                + `<a class="btn btn-primary" href="/">${escapeAttr(t('my_upload', 'Upload a model'))}</a></div>`;
        } else {
            grid.innerHTML = visible.map(cardHtml).join('');
            if (window.TaskCard) TaskCard.setupAutoplay(grid);
        }
        $('my-more').classList.toggle('hidden', state.items.length >= state.total);
        syncBulk();
    }

    function syncBulk() {
        const n = state.selected.size;
        $('my-sel-count').textContent = n ? String(n) : '';
        $('my-bulk-private').disabled = !n || !state.canPrivate;
        $('my-bulk-public').disabled = !n;
        $('my-bulk-delete').disabled = !n;
        const all = state.items.filter((i) => !i.gone);
        $('my-select-all').checked = !!all.length && all.every((i) => state.selected.has(i.task_id));
    }

    function toast(text, undoIds) {
        $('my-toast-text').textContent = text;
        state.undoIds = undoIds || [];
        $('my-toast-undo').classList.toggle('hidden', !state.undoIds.length);
        $('my-toast').hidden = false;
        clearTimeout(state.toastTimer);
        let left = state.undoSeconds;
        const tick = () => {
            if (state.undoIds.length) $('my-toast-text').textContent = `${text} · ${left}`;
            if (left-- <= 0) { $('my-toast').hidden = true; finishDeletes(state.undoIds); state.undoIds = []; return; }
            state.toastTimer = setTimeout(tick, 1000);
        };
        if (state.undoIds.length) tick(); else state.toastTimer = setTimeout(() => { $('my-toast').hidden = true; }, 3500);
    }

    function finishDeletes(ids) {
        ids.forEach((id) => {
            const it = state.items.find((x) => x.task_id === id);
            if (it && it.deleting) { it.gone = true; state.total = Math.max(0, state.total - 1); }
        });
        render();
    }

    function item(id) { return state.items.find((x) => x.task_id === id); }

    async function setVisibility(ids, makePublic) {
        if (!makePublic && !state.canPrivate) { window.location.href = '/buy-credits'; return; }
        const before = ids.map((id) => [id, item(id) && item(id).is_public]);
        ids.forEach((id) => { const it = item(id); if (it) it.is_public = makePublic; });   // optimistic
        render();
        try {
            if (ids.length === 1) await api('PATCH', `/api/my/tasks/${ids[0]}`, { is_public: makePublic });
            else {
                const res = await api('POST', '/api/my/tasks/bulk', { ids, action: makePublic ? 'public' : 'private' });
                (res.results || []).forEach((r) => { if (!r.ok) { const b = before.find((x) => x[0] === r.task_id); const it = item(r.task_id); if (it && b) it.is_public = b[1]; } });
            }
        } catch (e) {
            before.forEach(([id, v]) => { const it = item(id); if (it) it.is_public = v; });
            toast(e.code === 'subscription_required' ? t('my_private_locked', 'Private models come with the subscription') : t('my_error', 'Could not save, try again'));
        }
        render();
    }

    async function remove(ids) {
        ids.forEach((id) => { const it = item(id); if (it) it.deleting = true; state.selected.delete(id); });
        render();
        try {
            if (ids.length === 1) await api('DELETE', `/api/my/tasks/${ids[0]}`);
            else {
                const res = await api('POST', '/api/my/tasks/bulk', { ids, action: 'delete' });
                (res.results || []).forEach((r) => { if (!r.ok) { const it = item(r.task_id); if (it) it.deleting = false; } });
            }
            const done = ids.filter((id) => item(id) && item(id).deleting);
            render();
            toast(t('my_deleted', 'Deleted'), done);
        } catch (e) {
            ids.forEach((id) => { const it = item(id); if (it) it.deleting = false; });
            render();
            toast(e.code === 'task_busy' ? t('my_busy', 'Still processing') : t('my_error', 'Could not save, try again'));
        }
    }

    async function undo() {
        const ids = state.undoIds.slice();
        state.undoIds = [];
        clearTimeout(state.toastTimer);
        $('my-toast').hidden = true;
        for (const id of ids) {
            try {
                const res = await api('POST', `/api/my/tasks/${id}/restore`);
                const it = item(id);
                if (it) { it.deleting = false; it.is_public = res.is_public !== false; }
            } catch (_) { /* already erased */ }
        }
        render();
    }

    async function load(page) {
        const data = await api('GET', `/api/my/tasks?page=${page}&per_page=${state.perPage}`);
        state.page = page;
        state.total = data.total || 0;
        state.canPrivate = !!data.can_private;
        state.signedIn = !!data.signed_in;
        state.undoSeconds = data.undo_seconds || 10;
        state.items = page === 1 ? data.items : state.items.concat(data.items);
        $('my-plan-link').classList.toggle('hidden', state.canPrivate);
        $('my-signin').classList.toggle('hidden', state.signedIn);
        const dp = $('my-default-private');
        dp.classList.toggle('hidden', !(state.signedIn && state.canPrivate));
        dp.setAttribute('aria-pressed', data.private_by_default ? 'true' : 'false');
        render();
    }

    function bind() {
        const grid = $('my-grid');
        grid.addEventListener('click', (e) => {
            const card = e.target.closest('.my-card');
            if (!card) return;
            const id = card.getAttribute('data-id');
            if (e.target.closest('.my-vis')) {
                e.preventDefault(); e.stopPropagation();
                const it = item(id);
                setVisibility([id], !(it && it.is_public !== false));
            } else if (e.target.closest('.my-del')) {
                e.preventDefault(); e.stopPropagation();
                remove([id]);
            }
        });
        grid.addEventListener('change', (e) => {
            if (!e.target.classList.contains('my-sel')) return;
            const id = e.target.closest('.my-card').getAttribute('data-id');
            if (e.target.checked) state.selected.add(id); else state.selected.delete(id);
            e.target.closest('.my-card').classList.toggle('is-selected', e.target.checked);
            syncBulk();
        });
        $('my-select-all').addEventListener('change', (e) => {
            state.selected.clear();
            if (e.target.checked) state.items.filter((i) => !i.gone && !i.deleting).forEach((i) => state.selected.add(i.task_id));
            render();
        });
        $('my-bulk-private').addEventListener('click', () => setVisibility([...state.selected], false));
        $('my-bulk-public').addEventListener('click', () => setVisibility([...state.selected], true));
        $('my-bulk-delete').addEventListener('click', () => remove([...state.selected]));
        $('my-toast-undo').addEventListener('click', undo);
        $('my-more-btn').addEventListener('click', () => load(state.page + 1).catch(() => toast(t('my_error', 'Could not save, try again'))));
        $('my-default-private').addEventListener('click', async (e) => {
            const btn = e.currentTarget;
            const next = btn.getAttribute('aria-pressed') !== 'true';
            btn.setAttribute('aria-pressed', next ? 'true' : 'false');
            try { await api('POST', '/api/my/settings', { private_by_default: next }); }
            catch (_) { btn.setAttribute('aria-pressed', next ? 'false' : 'true'); toast(t('my_error', 'Could not save, try again')); }
        });
        window.addEventListener('beforeunload', () => { /* pending deletes finish on the server by themselves */ });
    }

    function setupChrome() {
        try {
            const saved = localStorage.getItem('autorig_theme') || 'dark';
            document.documentElement.setAttribute('data-theme', saved);
            const toggle = $('theme-toggle');
            if (toggle) {
                toggle.textContent = saved === 'dark' ? '☀️' : '🌙';
                toggle.addEventListener('click', () => {
                    const cur = document.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
                    document.documentElement.setAttribute('data-theme', cur);
                    try { localStorage.setItem('autorig_theme', cur); } catch (_) {}
                    toggle.textContent = cur === 'dark' ? '☀️' : '🌙';
                });
            }
        } catch (_) {}
        const btn = document.querySelector('.lang-btn');
        const dropdown = document.querySelector('.lang-dropdown');
        btn && btn.addEventListener('click', () => dropdown && dropdown.classList.toggle('open'));
        document.querySelectorAll('.lang-option').forEach((opt) => opt.addEventListener('click', async () => {
            await I18n.setLanguage(opt.getAttribute('data-lang'));
            dropdown && dropdown.classList.remove('open');
            render();
        }));
    }

    document.addEventListener('DOMContentLoaded', async () => {
        try { if (window.I18n && I18n.init) await I18n.init(); } catch (_) {}
        setupChrome();
        bind();
        try { await load(1); }
        catch (_) { $('my-grid').innerHTML = `<div class="my-empty" style="grid-column:1/-1">⚠️</div>`; }
    });
})();
