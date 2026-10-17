/**
 * Task page V3 · owner controls (owner 2026-10-17, docs/my-gallery-privacy-spec-20261017.md): the same 🔒/🌐 and 🗑
 * as the «My models» cards, shown in the tool strip only to the task's owner (or an admin). Icons + tooltips only.
 */
(function () {
    'use strict';
    const id = new URLSearchParams(location.search).get('id') || '';
    if (!/^[0-9a-f-]{36}$/i.test(id)) return;
    const tools = document.getElementById('tv3-tools');
    if (!tools) return;
    const t = (k, f) => { try { return (window.I18n && I18n.has && I18n.has(k)) ? I18n.t(k) : f; } catch (_) { return f; } };
    const mk = (cls, icon) => {
        const b = document.createElement('button');
        b.type = 'button'; b.className = 'tv3-tool ' + cls; b.hidden = true;
        b.style.fontSize = '1.05rem'; b.textContent = icon;
        return b;
    };
    const vis = mk('tv3-own-vis', '🔒');
    const del = mk('tv3-own-del', '🗑');
    const anchor = document.getElementById('tv3-full');
    tools.insertBefore(vis, anchor ? anchor.nextSibling : null);
    tools.insertBefore(del, vis.nextSibling);
    let task = null;
    let pendingDelete = null;

    function paint() {
        const pub = task.is_public !== false;
        const locked = pub && !task.can_manage_visibility;
        const label = pub ? (locked ? t('my_private_locked', 'Private models come with the subscription') : t('my_make_private', 'Make private'))
                          : t('my_make_public', 'Make public');
        vis.textContent = pub ? (locked ? '🔐' : '🔒') : '🌐';
        vis.title = label; vis.setAttribute('aria-label', label);
        del.title = t('my_delete', 'Delete for good'); del.setAttribute('aria-label', del.title);
        vis.hidden = false; del.hidden = false;
    }

    vis.addEventListener('click', async () => {
        const makePublic = task.is_public === false;
        if (!makePublic && !task.can_manage_visibility) { location.href = '/buy-credits'; return; }
        vis.disabled = true;
        try {
            const r = await fetch(`/api/my/tasks/${id}`, { method: 'PATCH', credentials: 'same-origin',
                headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ is_public: makePublic }) });
            if (r.status === 402) { location.href = '/buy-credits'; return; }
            if (!r.ok) throw new Error(String(r.status));
            task.is_public = makePublic;
            paint();
        } catch (_) { vis.title = t('my_error', 'Could not save, try again'); }
        finally { vis.disabled = false; }
    });

    del.addEventListener('click', async () => {
        if (pendingDelete) {                       // second press inside the window = undo
            clearTimeout(pendingDelete);
            pendingDelete = null;
            await fetch(`/api/my/tasks/${id}/restore`, { method: 'POST', credentials: 'same-origin' }).catch(() => {});
            del.textContent = '🗑'; del.title = t('my_delete', 'Delete for good');
            return;
        }
        const r = await fetch(`/api/my/tasks/${id}`, { method: 'DELETE', credentials: 'same-origin' }).catch(() => null);
        if (!r || !r.ok) { del.title = t('my_error', 'Could not save, try again'); return; }
        const data = await r.json().catch(() => ({}));
        let left = data.undo_seconds || 10;
        del.textContent = '↶';
        const tick = () => {
            del.title = `${t('my_undo', 'Undo')} · ${left}`;
            del.setAttribute('aria-label', del.title);
            if (left-- <= 0) { location.href = '/my'; return; }
            pendingDelete = setTimeout(tick, 1000);
        };
        tick();
    });

    fetch(`/api/task/${id}`, { credentials: 'same-origin', cache: 'no-store' })
        .then((r) => (r.ok ? r.json() : null))
        .then((data) => { if (data && data.is_owner) { task = data; paint(); } })
        .catch(() => {});
})();
